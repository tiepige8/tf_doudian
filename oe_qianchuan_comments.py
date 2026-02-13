#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OceanEngine / Qianchuan - 评论拉取 + 入库 + 负向评论自动隐藏 + 飞书汇总通知

你需要准备：
1) 你现有的 OAuth2 token 缓存文件（默认 /root/oe_token_cache.json）
2) 环境变量（建议统一放到 /root/oe_env.sh，然后 cron 里 source 一下）：
   - OE_APP_ID / OE_APP_SECRET
   - OE_TOKEN_CACHE  (可选，默认 /root/oe_token_cache.json)
   - PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD PGSSLMODE  (或 PG_DSN)
   - FEISHU_WEBHOOK_URL  (通知用；可为空，空则只入库不通知)

能力：
- run --once：增量拉取（默认回看 1 天），入库到 oe.fact_comment；把 emotion_type=NEGATIVE 的未隐藏评论调用 hide 接口隐藏；
              hide 行为会写到 oe.fact_comment_action（action='hide'）。
- notify：把“尚未通知”的隐藏成功记录汇总成一条飞书消息，并把 notified_at 写回（避免重复通知）。
- backfill：一次性历史回灌（按日期窗口分页拉取），可选同时隐藏负向未隐藏评论。

接口依据：
- 获取评论列表：GET https://api.oceanengine.com/open_api/v3.0/tools/comment/get/
  start_time/end_time 格式 yyyy-MM-dd，跨度 <= 90 天
- 隐藏评论：POST https://api.oceanengine.com/open_api/v3.0/tools/comment/hide/  comment_ids <= 20

注意：
- comment/get 有 page * page_size <= 10000 的限制。backfill 默认按 7 天窗口抓取，尽量避免触顶。
- 系统级限流（code=40100）出现时会自动退避重试（指数退避 + 抖动）。
"""

import argparse
import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


TZ_CN = timezone(timedelta(hours=8))
OE_NO_PERMISSION_CODE = 40002


class OeApiError(RuntimeError):
    """Structured OceanEngine API error with parsed code for caller handling."""

    def __init__(
        self,
        api_name: str,
        code: Optional[int],
        msg: str,
        help_msg: str = "",
        request_id: str = "",
        resp: Optional[Dict[str, Any]] = None,
    ):
        self.api_name = api_name
        self.code = code
        self.msg = msg
        self.help_msg = help_msg
        self.request_id = request_id
        self.resp = resp or {}
        super().__init__(
            f"[{api_name}] API失败 code={code}, msg={msg}, help={help_msg}, request_id={request_id}, resp={self.resp}"
        )


def _safe_bigint(x):
    """Best-effort cast to int for DB BIGINT columns.
    Returns None for empty / non-numeric / non-integer values.
    """
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x) if x.is_integer() else None
    s = str(x).strip()
    if s == "":
        return None
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return int(s)
        except Exception:
            return None
    # tolerate '123.0'
    try:
        f = float(s)
        return int(f) if f.is_integer() else None
    except Exception:
        return None


def _parse_comment_time(create_time):
    """create_time can be epoch seconds OR 'YYYY-MM-DD HH:MM:SS'."""
    if create_time is None or create_time == "":
        return None
    s = str(create_time).strip()
    if s.isdigit():
        try:
            return datetime.fromtimestamp(int(s), tz=TZ_CN)
        except Exception:
            return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=TZ_CN)
    except Exception:
        return None

API_BASE = "https://api.oceanengine.com"
API_V3 = API_BASE + "/open_api/v3.0"

OAUTH_BASE_DEFAULT = "https://ad.oceanengine.com"  # oauth2 access_token / refresh_token


# -------------------------
# Logging helpers
# -------------------------

def log(level: str, msg: str):
    ts = datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


# -------------------------
# Token cache
# -------------------------

@dataclass
class TokenCache:
    access_token: str
    refresh_token: str
    expires_at_epoch: int
    refresh_expires_at_epoch: int
    base_url_ad: str = OAUTH_BASE_DEFAULT
    updated_at: str = ""

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TokenCache":
        return TokenCache(
            access_token=str(d.get("access_token") or "").strip(),
            refresh_token=str(d.get("refresh_token") or "").strip(),
            expires_at_epoch=int(d.get("expires_at_epoch") or 0),
            refresh_expires_at_epoch=int(d.get("refresh_expires_at_epoch") or 0),
            base_url_ad=str(d.get("base_url_ad") or OAUTH_BASE_DEFAULT).strip(),
            updated_at=str(d.get("updated_at") or ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at_epoch": self.expires_at_epoch,
            "refresh_expires_at_epoch": self.refresh_expires_at_epoch,
            "base_url_ad": self.base_url_ad,
            "updated_at": self.updated_at or datetime.now(tz=TZ_CN).isoformat(),
        }


def load_token_cache(path: Path) -> Optional[TokenCache]:
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return TokenCache.from_dict(d)
    except Exception as e:
        log("ERROR", f"读取 token cache 失败 path={path} err={e!r}")
        return None


def save_token_cache(path: Path, cache: TokenCache):
    path.parent.mkdir(parents=True, exist_ok=True)
    cache.updated_at = datetime.now(tz=TZ_CN).isoformat()
    path.write_text(json.dumps(cache.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def now_epoch() -> int:
    return int(time.time())


def ensure_access_token(token_cache_path: Path, refresh_token_override: Optional[str] = None) -> TokenCache:
    """
    只要 refresh_token 没过期，就可以自动刷新拿到 access_token。
    你只需要“给一次 refresh_token”，后续脚本会自动维护 access_token。
    """
    app_id = os.getenv("OE_APP_ID", "").strip()
    app_secret = os.getenv("OE_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("缺少环境变量 OE_APP_ID / OE_APP_SECRET（用于 refresh_token 刷新）")

    cache = load_token_cache(token_cache_path)
    if cache is None:
        if not refresh_token_override:
            raise RuntimeError(f"token cache 不存在且未提供 refresh_token：{token_cache_path}")
        # initialize minimal cache (access_token unknown yet)
        cache = TokenCache(
            access_token="",
            refresh_token=refresh_token_override.strip(),
            expires_at_epoch=0,
            refresh_expires_at_epoch=0,
            base_url_ad=OAUTH_BASE_DEFAULT,
        )
        save_token_cache(token_cache_path, cache)

    if refresh_token_override:
        cache.refresh_token = refresh_token_override.strip()

    # access_token still valid?
    if cache.access_token and cache.expires_at_epoch and now_epoch() < cache.expires_at_epoch - 120:
        return cache

    # refresh_token expired?
    if cache.refresh_expires_at_epoch and now_epoch() > cache.refresh_expires_at_epoch - 120:
        raise RuntimeError("refresh_token 已过期：需要重新从浏览器授权拿到新的 refresh_token 再更新缓存")

    url = cache.base_url_ad.rstrip("/") + "/open_api/oauth2/refresh_token/"
    payload = {
        "app_id": app_id,
        "secret": app_secret,
        "grant_type": "refresh_token",
        "refresh_token": cache.refresh_token,
    }

    data = request_json_with_retry(
        method="POST",
        url=url,
        headers={"Content-Type": "application/json"},
        json_payload=payload,
        api_name="oauth2_refresh_token",
        max_attempts=6,
        retry_codes={40100, 51010},
    )

    # response structure: {"code":0,"data":{"access_token":...,"expires_in":...,"refresh_token":...,"refresh_token_expires_in":...}}
    d = data.get("data") or {}
    access_token = str(d.get("access_token") or "").strip()
    expires_in = int(d.get("expires_in") or 0)
    refresh_token = str(d.get("refresh_token") or "").strip() or cache.refresh_token
    refresh_expires_in = int(d.get("refresh_token_expires_in") or 0)

    if not access_token or expires_in <= 0:
        raise RuntimeError(f"refresh_token 刷新返回异常：{data}")

    cache.access_token = access_token
    cache.expires_at_epoch = now_epoch() + expires_in
    cache.refresh_token = refresh_token
    if refresh_expires_in > 0:
        cache.refresh_expires_at_epoch = now_epoch() + refresh_expires_in

    save_token_cache(token_cache_path, cache)
    return cache


# -------------------------
# HTTP helpers
# -------------------------

def oe_check_ok(resp_json: Dict[str, Any], api_name: str) -> Dict[str, Any]:
    code = resp_json.get("code")
    if code == 0 or code == "0":
        return resp_json
    msg = resp_json.get("message") or resp_json.get("msg") or ""
    help_msg = resp_json.get("help_message") or resp_json.get("help") or ""
    request_id = resp_json.get("request_id") or ""
    parsed_code: Optional[int]
    try:
        parsed_code = int(code)
    except Exception:
        parsed_code = None
    raise OeApiError(
        api_name=api_name,
        code=parsed_code,
        msg=str(msg),
        help_msg=str(help_msg),
        request_id=str(request_id),
        resp=resp_json,
    )


def request_json_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    api_name: str = "api",
    max_attempts: int = 6,
    retry_codes: Optional[set] = None,
) -> Dict[str, Any]:
    retry_codes = retry_codes or {40100, 51010, 50000}
    last_err: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            if method.upper() == "GET":
                r = requests.get(url, headers=headers, params=params, timeout=30)
            elif method.upper() == "POST":
                r = requests.post(url, headers=headers, params=params, json=json_payload, timeout=30)
            else:
                raise ValueError(f"unsupported method: {method}")

            # sometimes non-200 still has json
            try:
                j = r.json()
            except Exception:
                raise RuntimeError(f"[{api_name}] 非JSON响应 status={r.status_code} text={r.text[:200]}")

            if j.get("code") in (0, "0"):
                return j

            code_raw = j.get("code")
            try:
                code = int(code_raw)
            except Exception:
                code = -1

            if code in retry_codes and attempt < max_attempts:
                sleep = min(60.0, (0.6 * (2 ** (attempt - 1))) + random.random() * 0.8)
                log("WARNING", f"请求失败，重试 api={api_name} attempt={attempt}/{max_attempts} sleep={sleep:.1f}s code={code} msg={j.get('message') or j.get('msg')}")
                time.sleep(sleep)
                continue

            # not retryable or maxed
            oe_check_ok(j, api_name)
            return j
        except OeApiError as e:
            # OeApiError has already been retried when code is in retry_codes above.
            last_err = e
            break
        except Exception as e:
            last_err = e
            if attempt >= max_attempts:
                break
            sleep = min(60.0, (0.6 * (2 ** (attempt - 1))) + random.random() * 0.8)
            log("WARNING", f"请求异常，重试 api={api_name} attempt={attempt}/{max_attempts} sleep={sleep:.1f}s err={e!r}")
            time.sleep(sleep)

    if isinstance(last_err, OeApiError):
        raise last_err
    raise RuntimeError(f"[{api_name}] 请求最终失败：{last_err!r}")


# -------------------------
# Postgres helpers
# -------------------------

def get_conn():
    dsn = os.getenv("PG_DSN", "").strip()
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST", "").strip()
    db = os.getenv("PGDATABASE", "").strip()
    user = os.getenv("PGUSER", "").strip()
    pwd = os.getenv("PGPASSWORD", "").strip()
    port = os.getenv("PGPORT", "5432").strip()
    sslmode = os.getenv("PGSSLMODE", "require").strip()

    missing = [k for k, v in [("PGHOST", host), ("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", pwd)] if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)} (or set PG_DSN)")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, sslmode=sslmode)


def get_advertisers(cur) -> List[int]:
    cur.execute("SELECT advertiser_id FROM oe.dim_advertiser ORDER BY advertiser_id")
    return [int(r[0]) for r in cur.fetchall()]


def upsert_comments(cur, rows: List[Dict[str, Any]], seen_ts: datetime) -> int:
    """
    rows: each row is the comment object from API (dict).
    """
    if not rows:
        return 0

    values = []
    for c in rows:
        adv_id = int(c.get("advertiser_id") or 0)
        cid = int(c.get("comment_id") or 0)
        if not adv_id or not cid:
            continue

        create_time = c.get("create_time")
        comment_time = _parse_comment_time(create_time)

        values.append((
            adv_id,
            cid,
            comment_time,
            c.get("text") or c.get("content") or "",
            c.get("emotion_type"),
            c.get("hide_status"),
            c.get("level_type"),
            bool(c.get("is_replied")) if c.get("is_replied") is not None else None,
            int(float(c.get("reply_count") or 0)),
            int(float(c.get("like_count") or 0)),
            c.get("user_id") or c.get("author_id"),
            c.get("user_name") or c.get("author_name"),
            c.get("aweme_id"),
            c.get("aweme_name"),
            c.get("ad_id"),
            c.get("ad_name"),
            c.get("creative_id"),
            c.get("item_id"),
            c.get("item_title"),
            psycopg2.extras.Json(c),
            seen_ts,
        ))

    if not values:
        return 0

    sql = """
    INSERT INTO oe.fact_comment (
      advertiser_id, comment_id, comment_time, comment_text,
      emotion_type, hide_status, level_type, is_replied, reply_count, like_count,
      user_id, user_name, aweme_id, aweme_name, ad_id, ad_name, creative_id,
      item_id, item_title, raw, last_seen_at
    )
    VALUES %s
    ON CONFLICT (advertiser_id, comment_id) DO UPDATE SET
      comment_time = COALESCE(EXCLUDED.comment_time, oe.fact_comment.comment_time),
      comment_text = COALESCE(NULLIF(EXCLUDED.comment_text,''), oe.fact_comment.comment_text),
      emotion_type = COALESCE(EXCLUDED.emotion_type, oe.fact_comment.emotion_type),
      hide_status = COALESCE(EXCLUDED.hide_status, oe.fact_comment.hide_status),
      level_type = COALESCE(EXCLUDED.level_type, oe.fact_comment.level_type),
      is_replied = COALESCE(EXCLUDED.is_replied, oe.fact_comment.is_replied),
      reply_count = COALESCE(EXCLUDED.reply_count, oe.fact_comment.reply_count),
      like_count = COALESCE(EXCLUDED.like_count, oe.fact_comment.like_count),
      user_id = COALESCE(EXCLUDED.user_id, oe.fact_comment.user_id),
      user_name = COALESCE(EXCLUDED.user_name, oe.fact_comment.user_name),
      aweme_id = COALESCE(EXCLUDED.aweme_id, oe.fact_comment.aweme_id),
      aweme_name = COALESCE(EXCLUDED.aweme_name, oe.fact_comment.aweme_name),
      ad_id = COALESCE(EXCLUDED.ad_id, oe.fact_comment.ad_id),
      ad_name = COALESCE(EXCLUDED.ad_name, oe.fact_comment.ad_name),
      creative_id = COALESCE(EXCLUDED.creative_id, oe.fact_comment.creative_id),
      item_id = COALESCE(EXCLUDED.item_id, oe.fact_comment.item_id),
      item_title = COALESCE(EXCLUDED.item_title, oe.fact_comment.item_title),
      raw = EXCLUDED.raw,
      last_seen_at = EXCLUDED.last_seen_at
    """
    psycopg2.extras.execute_values(cur, sql, values, page_size=200)
    return len(values)


def upsert_action(cur, advertiser_id: int, comment_id: int, action: str, status: str,
                 request_id: Optional[str], error_code: Optional[int], error_message: Optional[str],
                 raw: Optional[Dict[str, Any]] = None) -> int:
    cur.execute(
        """
        INSERT INTO oe.fact_comment_action (
          advertiser_id, comment_id, action, action_ts, status, request_id, error_code, error_message, raw
        )
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        ON CONFLICT (advertiser_id, comment_id, action) DO UPDATE SET
          action_ts = EXCLUDED.action_ts,
          status = EXCLUDED.status,
          request_id = EXCLUDED.request_id,
          error_code = EXCLUDED.error_code,
          error_message = EXCLUDED.error_message,
          raw = COALESCE(EXCLUDED.raw, oe.fact_comment_action.raw)
        """,
        (advertiser_id, comment_id, action, status, request_id, error_code, error_message,
         psycopg2.extras.Json(raw) if raw is not None else None),
    )
    return cur.rowcount


def mark_hidden_in_fact(cur, advertiser_id: int, comment_ids: List[int]):
    if not comment_ids:
        return
    cur.execute(
        """
        UPDATE oe.fact_comment
        SET hide_status='HIDE', hidden_at=COALESCE(hidden_at, NOW()), last_seen_at=NOW()
        WHERE advertiser_id=%s AND comment_id = ANY(%s)
        """,
        (advertiser_id, comment_ids),
    )


def select_unnotified_hides(cur, since_hours: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          a.advertiser_id,
          d.advertiser_name,
          a.comment_id,
          a.action_ts,
          c.comment_text,
          c.emotion_type,
          c.aweme_name,
          c.ad_name
        FROM oe.fact_comment_action a
        LEFT JOIN oe.fact_comment c
          ON c.advertiser_id=a.advertiser_id AND c.comment_id=a.comment_id
        LEFT JOIN oe.dim_advertiser d
          ON d.advertiser_id=a.advertiser_id
        WHERE a.action='hide'
          AND a.status='success'
          AND a.notified_at IS NULL
          AND a.action_ts >= NOW() - (%s || ' hours')::interval
        ORDER BY a.action_ts DESC
        """,
        (since_hours,),
    )
    rows = []
    for r in cur.fetchall():
        rows.append({
            "advertiser_id": int(r[0]),
            "advertiser_name": r[1] or str(r[0]),
            "comment_id": int(r[2]),
            "action_ts": r[3],
            "comment_text": (r[4] or "").strip(),
            "emotion_type": r[5],
            "aweme_name": r[6],
            "ad_name": r[7],
        })
    return rows


def mark_notified(cur, keys: List[Tuple[int, int]]):
    if not keys:
        return
    # keys: (advertiser_id, comment_id)
    psycopg2.extras.execute_values(
        cur,
        """
        UPDATE oe.fact_comment_action a
        SET notified_at=NOW()
        FROM (VALUES %s) AS v(advertiser_id, comment_id)
        WHERE a.advertiser_id=v.advertiser_id AND a.comment_id=v.comment_id AND a.action='hide' AND a.status='success'
        """,
        keys,
        template="(%s,%s)",
        page_size=200,
    )


# -------------------------
# OceanEngine API calls
# -------------------------

def api_get_comments(access_token: str, advertiser_id: int, start_d: date, end_d: date,
                     page: int, page_size: int, hide_status: str = "NOT_HIDE") -> Dict[str, Any]:
    """
    GET /tools/comment/get/
    """
    url = API_V3 + "/tools/comment/get/"
    params = {
        "advertiser_id": advertiser_id,
        "start_time": start_d.isoformat(),  # yyyy-MM-dd
        "end_time": end_d.isoformat(),
        "order_field": "CREATE_TIME",
        "order_type": "DESC",
        "hide_status": hide_status,  # NOT_HIDE / HIDE / ALL
        "page": page,
        "page_size": page_size,
    }
    headers = {"Access-Token": access_token}
    j = request_json_with_retry("GET", url, headers=headers, params=params, api_name="qc_comment_get")
    oe_check_ok(j, "qc_comment_get")
    return j


def api_hide_comments(access_token: str, advertiser_id: int, comment_ids: List[int]) -> Dict[str, Any]:
    """
    POST /tools/comment/hide/
    """
    url = API_V3 + "/tools/comment/hide/"
    payload = {"advertiser_id": advertiser_id, "comment_ids": comment_ids}
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    j = request_json_with_retry("POST", url, headers=headers, json_payload=payload, api_name="qc_comment_hide")
    oe_check_ok(j, "qc_comment_hide")
    return j


# -------------------------
# Feishu notify
# -------------------------

def feishu_send_text(webhook: str, text: str):
    if not webhook:
        log("WARNING", "FEISHU_WEBHOOK_URL 为空，跳过通知")
        return
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        r = requests.post(webhook, json=payload, timeout=15)
        if r.status_code // 100 != 2:
            raise RuntimeError(f"HTTP {r.status_code} {r.text[:200]}")
    except Exception as e:
        raise RuntimeError(f"飞书 webhook 发送失败：{e!r}")


def build_notify_text(rows: List[Dict[str, Any]], window_hours: int) -> str:
    now_str = datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    total = len(rows)
    # group by advertiser
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        groups.setdefault(r["advertiser_name"], []).append(r)

    lines = []
    lines.append(f"【千川负向评论已隐藏汇总】{now_str}")
    lines.append(f"统计窗口：最近 {window_hours} 小时；本次新增隐藏：{total} 条")
    lines.append("")
    if total == 0:
        lines.append("本次无新增隐藏记录。")
        return "\n".join(lines)

    # show top 10 advertisers
    for adv_name, lst in sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True)[:10]:
        lines.append(f"- {adv_name}：{len(lst)} 条")
        # sample up to 3 comments
        for s in lst[:3]:
            txt = s.get("comment_text") or ""
            txt = txt.replace("\n", " ").strip()
            if len(txt) > 60:
                txt = txt[:60] + "…"
            extra = []
            if s.get("aweme_name"):
                extra.append(f"视频:{s['aweme_name']}")
            if s.get("ad_name"):
                extra.append(f"广告:{s['ad_name']}")
            suffix = ("（" + " ".join(extra) + "）") if extra else ""
            lines.append(f"    · {txt}{suffix}")
    if len(groups) > 10:
        lines.append(f"（其余 {len(groups)-10} 个账户略）")
    return "\n".join(lines)


# -------------------------
# Main logic
# -------------------------

def run_once(lookback_days: int, page_size: int, sleep_between_adv: float):
    token_cache_path = Path(os.getenv("OE_TOKEN_CACHE", "/root/oe_token_cache.json"))
    refresh_override = os.getenv("OE_REFRESH_TOKEN", "").strip() or None
    cache = ensure_access_token(token_cache_path, refresh_override)
    access_token = cache.access_token

    now_cn = datetime.now(tz=TZ_CN)
    start_d = (now_cn.date() - timedelta(days=lookback_days))
    end_d = now_cn.date()

    conn = get_conn()
    conn.autocommit = False
    total_upsert = 0
    total_hide_success = 0
    total_hide_fail = 0
    skipped_no_permission = 0
    try:
        with conn.cursor() as cur:
            advertiser_ids = get_advertisers(cur)
            log("INFO", f"识别广告账户数 advertiser_id={len(advertiser_ids)} lookback_days={lookback_days} start={start_d} end={end_d}")

            for idx, adv_id in enumerate(advertiser_ids, start=1):
                fetched: List[Dict[str, Any]] = []
                to_hide: List[int] = []

                page = 1
                max_pages = max(1, int(10000 / page_size))
                try:
                    while page <= max_pages:
                        j = api_get_comments(access_token, adv_id, start_d, end_d, page, page_size, hide_status="NOT_HIDE")
                        data = (j.get("data") or {})
                        clist = data.get("comment_list") or []
                        if not clist:
                            break

                        for c in clist:
                            if isinstance(c, dict):
                                c["advertiser_id"] = adv_id
                                fetched.append(c)
                                if (c.get("emotion_type") == "NEGATIVE") and (c.get("hide_status") != "HIDE"):
                                    try:
                                        to_hide.append(int(c.get("comment_id")))
                                    except Exception:
                                        pass

                        # next page?
                        if len(clist) < page_size:
                            break
                        page += 1

                        # gentle sleep to avoid system-wide throttle
                        time.sleep(0.15 + random.random() * 0.2)
                except OeApiError as e:
                    if e.code == OE_NO_PERMISSION_CODE:
                        skipped_no_permission += 1
                        log("WARNING", f"跳过无权限账户 advertiser_id={adv_id} code={e.code} request_id={e.request_id}")
                        continue
                    raise

                # upsert into DB
                total_upsert += upsert_comments(cur, fetched, now_cn)

                # hide negative
                if to_hide:
                    # de-dup
                    to_hide = sorted(set(to_hide))
                    # API requires <=20 ids per call
                    for i in range(0, len(to_hide), 20):
                        batch = to_hide[i:i+20]
                        try:
                            resp = api_hide_comments(access_token, adv_id, batch)
                            req_id = resp.get("request_id")
                            success_ids = ((resp.get("data") or {}).get("success_comment_ids") or [])
                            success_ids = [int(x) for x in success_ids if str(x).isdigit()]
                            fail_ids = [x for x in batch if x not in success_ids]

                            if success_ids:
                                mark_hidden_in_fact(cur, adv_id, success_ids)
                                for cid in success_ids:
                                    upsert_action(cur, adv_id, cid, "hide", "success", req_id, None, None, raw={"resp": resp})
                                total_hide_success += len(success_ids)

                            if fail_ids:
                                for cid in fail_ids:
                                    upsert_action(cur, adv_id, cid, "hide", "failed", req_id, None, "hide failed", raw={"resp": resp})
                                total_hide_fail += len(fail_ids)

                        except Exception as e:
                            # mark all failed
                            for cid in batch:
                                upsert_action(cur, adv_id, cid, "hide", "failed", None, None, str(e), raw={"error": repr(e)})
                            total_hide_fail += len(batch)
                            log("WARNING", f"隐藏失败 advertiser_id={adv_id} batch={len(batch)} err={e!r}")

                        time.sleep(0.2 + random.random() * 0.3)

                if idx % 10 == 0 or idx == len(advertiser_ids):
                    log("INFO", f"进度：{idx}/{len(advertiser_ids)} upsert={total_upsert} hide_ok={total_hide_success} hide_fail={total_hide_fail}")

                if sleep_between_adv > 0:
                    time.sleep(sleep_between_adv)

        conn.commit()
        log("INFO", f"DONE upsert={total_upsert} hide_ok={total_hide_success} hide_fail={total_hide_fail} skip_no_permission={skipped_no_permission}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_notify(window_hours: int):
    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            rows = select_unnotified_hides(cur, since_hours=window_hours)
            text = build_notify_text(rows, window_hours=window_hours)
            feishu_send_text(webhook, text)

            keys = [(r["advertiser_id"], r["comment_id"]) for r in rows]
            mark_notified(cur, keys)
        conn.commit()
        log("INFO", f"notify：已发送并标记 notified rows={len(rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def backfill(start_d: date, end_d: date, window_days: int, do_hide: bool, page_size: int, sleep_between_adv: float):
    token_cache_path = Path(os.getenv("OE_TOKEN_CACHE", "/root/oe_token_cache.json"))
    refresh_override = os.getenv("OE_REFRESH_TOKEN", "").strip() or None
    cache = ensure_access_token(token_cache_path, refresh_override)
    access_token = cache.access_token

    conn = get_conn()
    conn.autocommit = False
    now_cn = datetime.now(tz=TZ_CN)

    total_upsert = 0
    total_hide_success = 0
    total_hide_fail = 0
    skipped_no_permission = 0

    def daterange_chunks(s: date, e: date, days: int) -> List[Tuple[date, date]]:
        chunks = []
        cur = s
        while cur <= e:
            nxt = min(e, cur + timedelta(days=days - 1))
            chunks.append((cur, nxt))
            cur = nxt + timedelta(days=1)
        return chunks

    chunks = daterange_chunks(start_d, end_d, window_days)

    try:
        with conn.cursor() as cur:
            advertiser_ids = get_advertisers(cur)
            log("INFO", f"backfill：advertisers={len(advertiser_ids)} chunks={len(chunks)} range={start_d}..{end_d} window_days={window_days} do_hide={do_hide}")

            for idx, adv_id in enumerate(advertiser_ids, start=1):
                adv_no_permission = False
                for (s, e) in chunks:
                    fetched: List[Dict[str, Any]] = []
                    to_hide: List[int] = []
                    page = 1
                    max_pages = max(1, int(10000 / page_size))
                    try:
                        while page <= max_pages:
                            j = api_get_comments(access_token, adv_id, s, e, page, page_size, hide_status="ALL")
                            data = (j.get("data") or {})
                            clist = data.get("comment_list") or []
                            if not clist:
                                break
                            for c in clist:
                                if isinstance(c, dict):
                                    c["advertiser_id"] = adv_id
                                    fetched.append(c)
                                    if do_hide and (c.get("emotion_type") == "NEGATIVE") and (c.get("hide_status") != "HIDE"):
                                        try:
                                            to_hide.append(int(c.get("comment_id")))
                                        except Exception:
                                            pass
                            if len(clist) < page_size:
                                break
                            page += 1
                            time.sleep(0.15 + random.random() * 0.2)
                    except OeApiError as e2:
                        if e2.code == OE_NO_PERMISSION_CODE:
                            skipped_no_permission += 1
                            adv_no_permission = True
                            log("WARNING", f"backfill跳过无权限账户 advertiser_id={adv_id} code={e2.code} request_id={e2.request_id}")
                            break
                        raise

                    total_upsert += upsert_comments(cur, fetched, now_cn)

                    if do_hide and to_hide:
                        to_hide = sorted(set(to_hide))
                        for i in range(0, len(to_hide), 20):
                            batch = to_hide[i:i+20]
                            try:
                                resp = api_hide_comments(access_token, adv_id, batch)
                                req_id = resp.get("request_id")
                                success_ids = ((resp.get("data") or {}).get("success_comment_ids") or [])
                                success_ids = [int(x) for x in success_ids if str(x).isdigit()]
                                fail_ids = [x for x in batch if x not in success_ids]

                                if success_ids:
                                    mark_hidden_in_fact(cur, adv_id, success_ids)
                                    for cid in success_ids:
                                        upsert_action(cur, adv_id, cid, "hide", "success", req_id, None, None, raw={"resp": resp})
                                    total_hide_success += len(success_ids)

                                if fail_ids:
                                    for cid in fail_ids:
                                        upsert_action(cur, adv_id, cid, "hide", "failed", req_id, None, "hide failed", raw={"resp": resp})
                                    total_hide_fail += len(fail_ids)

                            except Exception as hide_err:
                                for cid in batch:
                                    upsert_action(cur, adv_id, cid, "hide", "failed", None, None, str(hide_err), raw={"error": repr(hide_err)})
                                total_hide_fail += len(batch)
                                log("WARNING", f"backfill隐藏失败 advertiser_id={adv_id} batch={len(batch)} err={hide_err!r}")
                            time.sleep(0.2 + random.random() * 0.3)

                    log("INFO", f"backfill adv={adv_id} window={s}..{e} fetched={len(fetched)} to_hide={len(to_hide) if do_hide else 0}")

                if adv_no_permission:
                    continue

                if idx % 5 == 0 or idx == len(advertiser_ids):
                    log("INFO", f"backfill 进度：{idx}/{len(advertiser_ids)} upsert={total_upsert} hide_ok={total_hide_success} hide_fail={total_hide_fail}")

                if sleep_between_adv > 0:
                    time.sleep(sleep_between_adv)

        conn.commit()
        log("INFO", f"backfill DONE upsert={total_upsert} hide_ok={total_hide_success} hide_fail={total_hide_fail} skip_no_permission={skipped_no_permission}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_run = sub.add_parser("run", help="增量拉取（默认回看 1 天）并隐藏负向评论")
    ap_run.add_argument("--once", action="store_true", help="只跑一次（推荐用于 cron）")
    ap_run.add_argument("--lookback-days", type=int, default=int(os.getenv("COMMENT_LOOKBACK_DAYS", "1")))
    ap_run.add_argument("--page-size", type=int, default=int(os.getenv("COMMENT_PAGE_SIZE", "100")))
    ap_run.add_argument("--sleep-between-adv", type=float, default=float(os.getenv("COMMENT_SLEEP_BETWEEN_ADV", "0.05")))

    ap_notify = sub.add_parser("notify", help="汇总发送飞书通知（默认最近 24h 未通知的隐藏成功记录）")
    ap_notify.add_argument("--window-hours", type=int, default=int(os.getenv("COMMENT_NOTIFY_WINDOW_HOURS", "24")))

    ap_back = sub.add_parser("backfill", help="一次性历史回灌")
    ap_back.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap_back.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap_back.add_argument("--window-days", type=int, default=int(os.getenv("COMMENT_BACKFILL_WINDOW_DAYS", "7")))
    ap_back.add_argument("--do-hide", action="store_true", help="回灌时也把负向未隐藏的评论隐藏掉")
    ap_back.add_argument("--page-size", type=int, default=int(os.getenv("COMMENT_PAGE_SIZE", "100")))
    ap_back.add_argument("--sleep-between-adv", type=float, default=float(os.getenv("COMMENT_SLEEP_BETWEEN_ADV", "0.05")))

    args = ap.parse_args()

    if args.cmd == "run":
        run_once(lookback_days=args.lookback_days, page_size=args.page_size, sleep_between_adv=args.sleep_between_adv)
        return

    if args.cmd == "notify":
        run_notify(window_hours=args.window_hours)
        return

    if args.cmd == "backfill":
        start_d = parse_date(args.start)
        end_d = parse_date(args.end)
        if end_d < start_d:
            raise SystemExit("end < start")
        if args.window_days <= 0:
            raise SystemExit("window-days must be positive")
        if args.window_days > 90:
            log("WARNING", "window-days > 90，接口要求 start_time/end_time 跨度<=90，已自动改为 90")
            args.window_days = 90
        backfill(start_d, end_d, window_days=args.window_days, do_hide=bool(args.do_hide),
                 page_size=args.page_size, sleep_between_adv=args.sleep_between_adv)
        return


if __name__ == "__main__":
    main()
