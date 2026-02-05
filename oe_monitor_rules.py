#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Evaluate balance/spend alert rules and write to oe.fact_alert_event (dedup built-in).
#
# Rules implemented:
# - RULE_00: daily 00:00 check -> account_valid < 2 * yesterday_cost
# - RULE_30M: every 30 min -> account_valid < 1 * yesterday_cost
# - RULE_1H: every hour -> account_valid < 4 * last_hour_spend (requires oe.fact_spend_hourly; otherwise skipped)
#
# Optional: send Feishu webhook notification when new alerts are inserted.
# - Enable by setting env FEISHU_WEBHOOK_URL (recommended) and passing --notify.
# - If your Feishu bot has “关键词” protection enabled, set FEISHU_KEYWORD and it
#   will be prepended to every message.
# - If your Feishu bot has “签名校验” enabled, set FEISHU_SECRET and the script will
#   attach (timestamp, sign) per Feishu custom-bot convention.

import argparse
import base64
import hashlib
import hmac
import json
import requests
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Optional, Iterable, Tuple

import psycopg2
import psycopg2.extras


TZ_CN = timezone(timedelta(hours=8))

# Per advertiser + rule, max notifications per day (use existing oe.fact_alert_event rows; no extra table).
ALERT_MAX_NOTIFY_PER_DAY = int(os.getenv("ALERT_MAX_NOTIFY_PER_DAY", "3"))


def money_to_yuan(x: Optional[float], unit_mult: float, digits: int) -> Optional[float]:
    """Convert OE money units to CNY yuan.

    In many OceanEngine finance APIs, amounts are returned in 1/100000 yuan.
    If so, unit_mult should be 0.00001.
    """
    if x is None:
        return None
    try:
        return round(float(x) * float(unit_mult), int(digits))
    except Exception:
        return None


def fmt_money(x: Optional[float], unit_mult: float, digits: int) -> str:
    """Format money to yuan string with fixed decimals."""
    v = money_to_yuan(x, unit_mult, digits)
    if v is None:
        v = 0.0
    try:
        return f"{float(v):.{int(digits)}f}"
    except Exception:
        return str(v)


def feishu_sign(secret: str, timestamp: str) -> str:
    """Feishu custom bot signature.

    Common convention:
      string_to_sign = f"{timestamp}\\n{secret}"
      sign = base64( hmac_sha256(secret, string_to_sign) )
    """
    s = f"{timestamp}\\n{secret}".encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), s, digestmod=hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def feishu_send_text(webhook_url: str, text: str, *, secret: Optional[str] = None, timeout: int = 10) -> Tuple[bool, str]:
    """Send a text message to Feishu custom-bot webhook.

    Returns (ok, response_text).
    """
    payload: Dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": text},
    }
    if secret:
        ts = str(int(time.time()))
        payload["timestamp"] = ts
        payload["sign"] = feishu_sign(secret, ts)

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            # Feishu/Lark bot typically returns JSON like {"StatusCode":0,...} or {"code":0,...}
            try:
                j = json.loads(body)
                if ("code" in j and int(j.get("code") or 0) != 0) or ("StatusCode" in j and int(j.get("StatusCode") or 0) != 0):
                    return False, body
            except Exception:
                pass
            return True, body
    except urllib.error.HTTPError as e:
        return False, f"HTTPError {e.code}: {e.read().decode('utf-8', errors='replace')}"
    except Exception as e:
        return False, repr(e)


def feishu_send_card(webhook_url: str, card: Dict[str, Any], secret: str = None):
    """Send Feishu interactive card."""
    payload = {"msg_type": "interactive", "card": card}
    r = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=20)
    r.raise_for_status()
    return r.text


def _shorten_name(s: str, max_len: int = 18) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def build_balance_daily_card(
    report_date: str,
    status_md: str,
    rows: List[Dict[str, Any]],
    max_rows: int = 80,
    header_template: str = "green",
) -> Dict[str, Any]:
    """
    Daily balance report card:
      - status/alert markdown block (status_md)
      - clean table (column_set) with numeric columns right-aligned
    """
    # Column weights
    w_name, w_bal, w_yc, w_c7, w_days, w_ratio = 6, 3, 3, 3, 2, 2

    def mk_col(content: str, weight: int, *, bold: bool = False, align: str = "left") -> Dict[str, Any]:
        # Single-line cells keep row heights consistent.
        if bold:
            text = {"tag": "lark_md", "content": f"**{content}**", "lines": 1, "text_align": align}
        else:
            text = {"tag": "plain_text", "content": content, "lines": 1, "text_align": align}
        return {
            "tag": "column",
            "width": "weighted",
            "weight": weight,
            "horizontal_align": align,  # left/center/right
            "elements": [{"tag": "div", "text": text}],
        }

    def mk_row(cols: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {"tag": "column_set", "columns": cols}

    # Header row
    header_cols = [
        mk_col("账户", w_name, bold=True, align="left"),
        mk_col("余额", w_bal, bold=True, align="right"),
        mk_col("昨日消耗", w_yc, bold=True, align="right"),
        mk_col("7日消耗", w_c7, bold=True, align="right"),
        mk_col("可用天数", w_days, bold=True, align="right"),
        mk_col("倍率", w_ratio, bold=True, align="right"),
    ]

    # Body rows
    body_rows: List[Dict[str, Any]] = []
    for r in rows[:max_rows]:
        body_rows.append(
            mk_row(
                [
                    mk_col(str(r.get("name", "")), w_name, align="left"),
                    mk_col(str(r.get("balance", "")), w_bal, align="right"),
                    mk_col(str(r.get("y_cost", "")), w_yc, align="right"),
                    mk_col(str(r.get("cost_7d", "")), w_c7, align="right"),
                    mk_col(str(r.get("days", "")), w_days, align="right"),
                    mk_col(str(r.get("ratio", "")), w_ratio, align="right"),
                ]
            )
        )

    elements: List[Dict[str, Any]] = []

    if status_md:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": status_md}})
        elements.append({"tag": "hr"})

    elements.append(mk_row(header_cols))
    elements.append({"tag": "hr"})
    elements.extend(body_rows)

    title_color = {"green": "green", "orange": "orange", "red": "red"}.get(header_template, "green")

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": title_color,
            "title": {"tag": "plain_text", "content": f"账户资金日报（{report_date}）"},
        },
        "elements": elements,
    }


def build_daily_balance_rows(
    *,
    adv_ids_in_report: List[int],
    name_map: Dict[int, str],
    balance_map: Dict[int, float],
    y_cost_map: Dict[int, float],
    cost7_map: Dict[int, float],
    unit_mult: float,
    digits: int,
) -> List[Dict[str, Any]]:
    def fmt(v: float) -> str:
        return f"{money_to_yuan(v, unit_mult, digits):.{digits}f}"

    rows: List[Dict[str, Any]] = []
    for adv_id in adv_ids_in_report:
        adv_id = int(adv_id)
        name = name_map.get(adv_id, str(adv_id))
        bal = float(balance_map.get(adv_id, 0.0))
        y_cost = float(y_cost_map.get(adv_id, 0.0))
        c7 = float(cost7_map.get(adv_id, 0.0))
        avg7 = c7 / 7.0 if c7 > 0 else 0.0
        days_left = (bal / avg7) if avg7 > 0 else None
        ratio = (bal / y_cost) if y_cost > 0 else 0.0
        rows.append(
            {
                "name": name,
                "balance": fmt(bal),
                "y_cost": fmt(y_cost),
                "cost_7d": fmt(c7),
                "days": f"{days_left:.1f}" if days_left is not None else "-",
                "ratio": f"{ratio:.2f}",
            }
        )
    return rows


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
        raise SystemExit(f"Missing env vars: {', '.join(missing)} (or set PG_DSN)")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, sslmode=sslmode)


def latest_balance_per_adv(cur) -> List[Dict[str, Any]]:
    cur.execute(
        '''
        SELECT DISTINCT ON (b.advertiser_id)
          b.advertiser_id, b.snapshot_ts, b.account_valid
        FROM oe.fact_balance_snapshot b
        ORDER BY b.advertiser_id, b.snapshot_ts DESC
        '''
    )
    rows = cur.fetchall()
    return [{"advertiser_id": r[0], "snapshot_ts": r[1], "account_valid": r[2]} for r in rows]


def yesterday_cost_map(cur, y: date) -> Dict[int, float]:
    cur.execute(
        '''
        SELECT advertiser_id, COALESCE(cost,0)
        FROM oe.fact_finance_daily
        WHERE date = %s
        ''',
        (y,),
    )
    return {int(adv_id): float(cost or 0) for adv_id, cost in cur.fetchall()}


def cost_7d_map(cur, end_day: date) -> Dict[int, float]:
    """Sum cost over last 7 days ending at end_day (inclusive)."""
    start_day = end_day - timedelta(days=6)
    cur.execute(
        '''
        SELECT advertiser_id, COALESCE(SUM(cost),0)
        FROM oe.fact_finance_daily
        WHERE date >= %s AND date <= %s
        GROUP BY advertiser_id
        ''',
        (start_day, end_day),
    )
    return {int(adv_id): float(cost or 0) for adv_id, cost in cur.fetchall()}


def last_hour_spend_map(cur, hour_ts: datetime) -> Dict[int, float]:
    cur.execute(
        '''
        SELECT advertiser_id, spend
        FROM oe.fact_spend_hourly
        WHERE hour_ts = %s
        ''',
        (hour_ts,),
    )
    return {int(adv_id): float(spend or 0) for adv_id, spend in cur.fetchall()}


def insert_alert(cur, adv_id: int, rule_id: str, severity: str,
                 balance_valid: float, baseline_spend: float, mult: float,
                 snapshot_ts: datetime, baseline_ts: Optional[datetime], dedup_key: str, detail: Dict[str, Any]) -> int:
    ratio = (balance_valid / baseline_spend) if baseline_spend > 0 else 0.0
    cur.execute(
        '''
        INSERT INTO oe.fact_alert_event (
          alert_ts, advertiser_id, rule_id, severity,
          balance_valid, baseline_spend, threshold_multiplier, ratio,
          snapshot_ts, baseline_ts, dedup_key, detail
        )
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dedup_key) DO NOTHING
        ''',
        (adv_id, rule_id, severity, balance_valid, baseline_spend, mult, ratio, snapshot_ts, baseline_ts, dedup_key, psycopg2.extras.Json(detail)),
    )
    return cur.rowcount


def fetch_adv_name_map(cur, adv_ids: Iterable[int]) -> Dict[int, str]:
    ids = sorted({int(x) for x in adv_ids if x is not None})
    if not ids:
        return {}
    cur.execute(
        '''
        SELECT advertiser_id, advertiser_name
        FROM oe.dim_advertiser
        WHERE advertiser_id = ANY(%s)
        ''',
        (ids,),
    )
    return {int(a): (n or "") for a, n in cur.fetchall()}


def alert_count_today_map(cur, *, rule_id: str, adv_ids: Iterable[int], day_cn: date) -> Dict[int, int]:
    """Count alert rows for (rule_id, advertiser_id) on the given China day.

    Uses existing oe.fact_alert_event rows; no new table.
    """
    ids = sorted({int(x) for x in adv_ids if x is not None})
    if not ids:
        return {}
    cur.execute(
        '''
        SELECT advertiser_id, COUNT(*)::int AS cnt
        FROM oe.fact_alert_event
        WHERE rule_id = %s
          AND advertiser_id = ANY(%s)
          AND (alert_ts AT TIME ZONE 'Asia/Shanghai')::date = %s
        GROUP BY advertiser_id
        ''',
        (rule_id, ids, day_cn),
    )
    return {int(a): int(c) for a, c in cur.fetchall()}


def build_feishu_text(
    rule_id: str,
    now_cn: datetime,
    alerts: List[Dict[str, Any]],
    name_map: Dict[int, str],
    unit_mult: float,
    digits: int,
    max_items: int,
    keyword: str,
) -> str:
    rule_title = {
        "RULE_00": "余额预警·日检(00:05)",
        "RULE_30M": "余额预警·30分钟",
        "RULE_1H": "余额预警·每小时",
    }.get(rule_id, rule_id)

    hdr = [
        f"【{rule_title}】触发 {len(alerts)} 个账户",
        f"时间: {now_cn.strftime('%Y-%m-%d %H:%M:%S')} (Asia/Shanghai)",
        "说明: 余额/消耗单位已换算为‘元’。",
        "",
    ]

    lines = []
    shown = alerts[: max_items]
    for i, a in enumerate(shown, start=1):
        adv_id = int(a["advertiser_id"])
        name = name_map.get(adv_id) or "(无名称)"
        bal = money_to_yuan(a["balance_valid"], unit_mult, digits)
        base = money_to_yuan(a["baseline_spend"], unit_mult, digits)
        thr = money_to_yuan(a["threshold"], unit_mult, digits)
        ratio = a.get("ratio", 0.0)
        snap_ts = a.get("snapshot_ts")
        snap_s = snap_ts.strftime('%m-%d %H:%M:%S') if isinstance(snap_ts, datetime) else ""
        sev = a.get("severity", "")
        lines.append(
            f"{i}. {name} | {adv_id} | 严重度={sev} | 可用余额={bal}元 | 基准消耗={base}元 | 阈值={thr}元 | 倍数={ratio:.2f} | 快照={snap_s}"
        )

    if len(alerts) > len(shown):
        lines.append(f"... 还有 {len(alerts) - len(shown)} 个账户未展示(为避免刷屏)。")

    text = "\n".join(hdr + lines)
    if keyword:
        # If the bot enables “关键词” protection, the keyword must appear in the message.
        text = f"{keyword}\n" + text
    return text


def build_daily_balance_text(
    now_cn: datetime,
    *,
    multiplier: float,
    alerted_adv_ids: List[int],
    adv_ids_in_report: List[int],
    name_map: Dict[int, str],
    balance_map: Dict[int, float],
    y_cost_map: Dict[int, float],
    cost7_map: Dict[int, float],
    unit_mult: float,
    digits: int,
    report_max_items: int,
    keyword: str,
) -> str:
    """Daily report for RULE_00: always list yesterday spenders; top section indicates whether alerts triggered."""
    def fmt_yuan(v: float) -> str:
        return f"{money_to_yuan(v, unit_mult, digits):.{digits}f}"

    lines: List[str] = []
    if alerted_adv_ids:
        lines.append(f"【余额预警·每日】⚠️ 触发 {len(alerted_adv_ids)} 个账户：余额 < 昨日消耗 × {multiplier:g}")
        for adv_id in alerted_adv_ids[:50]:
            adv_id = int(adv_id)
            name = name_map.get(adv_id, str(adv_id))
            bal = float(balance_map.get(adv_id, 0.0))
            y_cost = float(y_cost_map.get(adv_id, 0.0))
            ratio = (bal / y_cost) if y_cost > 0 else 0.0
            lines.append(f"- {name}｜余额 {fmt_yuan(bal)}｜昨日消耗 {fmt_yuan(y_cost)}｜倍率 {ratio:.2f}")
    else:
        lines.append("【余额预警·每日】✅ 余额充足，未触发预警")

    lines.append("--------------------")
    yday = (now_cn.date() - timedelta(days=1)).isoformat()
    lines.append(f"【账户资金日报】日期: {yday} (昨日)")
    lines.append("字段：余额｜昨日消耗｜7日消耗｜可用天数(余额/7日均消)｜倍率(余额/昨日)")
    lines.append("")

    shown = adv_ids_in_report[:report_max_items]
    for adv_id in shown:
        adv_id = int(adv_id)
        name = name_map.get(adv_id, str(adv_id))
        bal = float(balance_map.get(adv_id, 0.0))
        y_cost = float(y_cost_map.get(adv_id, 0.0))
        c7 = float(cost7_map.get(adv_id, 0.0))
        avg7 = c7 / 7.0 if c7 > 0 else 0.0
        days_left = (bal / avg7) if avg7 > 0 else None
        ratio = (bal / y_cost) if y_cost > 0 else 0.0
        days_s = f"{days_left:.1f}" if days_left is not None else "-"
        lines.append(f"{name}｜{fmt_yuan(bal)}｜{fmt_yuan(y_cost)}｜{fmt_yuan(c7)}｜{days_s}｜{ratio:.2f}")

    if len(adv_ids_in_report) > len(shown):
        lines.append(f"... 还有 {len(adv_ids_in_report) - len(shown)} 个账户未展示(为避免刷屏)。")

    text = "\n".join(lines)
    if keyword:
        text = f"{keyword}\n" + text
    return text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rule", required=True, choices=["RULE_00", "RULE_30M", "RULE_1H"])
    ap.add_argument("--as-of", help="As-of datetime in Asia/Shanghai, e.g. 2025-12-19 00:00:00 (optional)")
    ap.add_argument("--notify", action="store_true", help="Send Feishu webhook notification for NEW alerts")
    ap.add_argument("--feishu-webhook", default="", help="Override env FEISHU_WEBHOOK_URL")
    ap.add_argument("--feishu-secret", default="", help="Override env FEISHU_SECRET (only if you enabled signature)")
    ap.add_argument("--unit-mult", type=float, default=float(os.getenv("MONEY_TO_YUAN_MULT", "0.00001")), help="Convert unit to CNY yuan (default 0.00001)")
    ap.add_argument("--digits", type=int, default=int(os.getenv("MONEY_TO_YUAN_DIGITS", "2")), help="Decimal digits for yuan")
    ap.add_argument("--max-items", type=int, default=int(os.getenv("FEISHU_MAX_ITEMS", "30")), help="Max accounts per message")
    ap.add_argument("--always-notify", action="store_true", help="(RULE_00) Send daily balance report even if no alerts")
    ap.add_argument("--report-max-items", type=int, default=int(os.getenv("BALANCE_REPORT_MAX_ITEMS", "80")), help="(RULE_00) Max accounts in daily report")
    ap.add_argument("--notify-test", action="store_true", help="Send a test message to Feishu and exit")
    args = ap.parse_args()

    if args.as_of:
        now_cn = datetime.strptime(args.as_of, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_CN)
    else:
        now_cn = datetime.now(tz=TZ_CN)

    # Feishu config (optional)
    webhook_url = (args.feishu_webhook or os.getenv("FEISHU_WEBHOOK_URL", "")).strip()
    feishu_secret = (args.feishu_secret or os.getenv("FEISHU_SECRET", "")).strip() or None
    feishu_keyword = os.getenv("FEISHU_KEYWORD", "").strip()

    if args.notify_test:
        if not webhook_url:
            raise SystemExit("Missing FEISHU_WEBHOOK_URL (or pass --feishu-webhook) for --notify-test")
        ts = now_cn.strftime("%Y-%m-%d %H:%M:%S")
        prefix = (feishu_keyword + "\n") if feishu_keyword else ""
        feishu_send_text(webhook_url, f"{prefix}【OE监控】飞书Webhook测试 {ts}", secret=feishu_secret)
        print("OK: sent feishu test message")
        return

    y = now_cn.date() - timedelta(days=1)
    prev_hour = now_cn.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    conn = get_conn()
    conn.autocommit = False
    inserted = 0
    new_alerts: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            balances = latest_balance_per_adv(cur)
            y_cost = yesterday_cost_map(cur, y)

            last_hour = {}
            if args.rule == "RULE_1H":
                last_hour = last_hour_spend_map(cur, prev_hour)

            for b in balances:
                adv_id = int(b["advertiser_id"])
                # If balance fetch failed (NULL in DB), skip to avoid false positives.
                if b["account_valid"] is None:
                    continue
                bal = float(b["account_valid"])
                snap_ts = b["snapshot_ts"]

                if args.rule in ("RULE_00", "RULE_30M"):
                    base = float(y_cost.get(adv_id, 0.0))
                    mult = 2.0 if args.rule == "RULE_00" else 1.0
                    if base <= 0:
                        continue
                    if bal < mult * base:
                        sev = "warn" if args.rule == "RULE_00" else "crit"
                        dedup_key = (
                            f"{adv_id}|{args.rule}|{now_cn.date().isoformat()}"
                            if args.rule == "RULE_00"
                            else f"{adv_id}|{args.rule}|{now_cn.strftime('%Y-%m-%dT%H')}"
                        )
                        detail = {"yesterday": str(y), "yesterday_cost": base, "balance_valid": bal, "threshold": mult * base}
                        rc = insert_alert(cur, adv_id, args.rule, sev, bal, base, mult, snap_ts, None, dedup_key, detail)
                        inserted += rc
                        if rc:
                            new_alerts.append({
                                "advertiser_id": adv_id,
                                "rule_id": args.rule,
                                "severity": sev,
                                "balance_valid": bal,
                                "baseline_spend": base,
                                "threshold": mult * base,
                                "ratio": (bal / base) if base > 0 else 0.0,
                                "snapshot_ts": snap_ts,
                            })

                elif args.rule == "RULE_1H":
                    base = float(last_hour.get(adv_id, 0.0))
                    mult = 4.0
                    if base <= 0:
                        continue
                    if bal < mult * base:
                        sev = "crit"
                        dedup_key = f"{adv_id}|{args.rule}|{prev_hour.strftime('%Y-%m-%dT%H')}"
                        detail = {"last_hour": prev_hour.isoformat(), "last_hour_spend": base, "balance_valid": bal, "threshold": mult * base}
                        rc = insert_alert(cur, adv_id, args.rule, sev, bal, base, mult, snap_ts, prev_hour, dedup_key, detail)
                        inserted += rc
                        if rc:
                            new_alerts.append({
                                "advertiser_id": adv_id,
                                "rule_id": args.rule,
                                "severity": sev,
                                "balance_valid": bal,
                                "baseline_spend": base,
                                "threshold": mult * base,
                                "ratio": (bal / base) if base > 0 else 0.0,
                                "snapshot_ts": snap_ts,
                                "baseline_ts": prev_hour,
                            })

        conn.commit()
        print(f"OK: inserted_alerts={inserted} rule={args.rule} as_of={now_cn.isoformat()}")
        name_map = {}
        if new_alerts:
            # For notifications, names only needed for the accounts we may send.
            try:
                with conn.cursor() as cur_name:
                    name_map = fetch_adv_name_map(cur_name, [a["advertiser_id"] for a in new_alerts])
            except Exception:
                name_map = {}

        # Notify:
        # - Default (all rules): only for newly inserted alerts (dedup_key guarantees idempotency in DB).
        # - RULE_00: if --always-notify is set, send daily report even when there is NO alert.
        if args.notify:
            webhook = (args.feishu_webhook or os.getenv("FEISHU_WEBHOOK_URL", "")).strip()
            if not webhook:
                print("WARNING: --notify enabled but missing FEISHU_WEBHOOK_URL; skip sending")
            else:
                secret = (args.feishu_secret or os.getenv("FEISHU_SECRET", "")).strip() or None
                keyword = os.getenv("FEISHU_KEYWORD", "").strip()

                if args.rule == "RULE_00":
                    # Daily report (always list yesterday spenders). Multiplier is hard-coded: 2.0
                    try:
                        conn2 = get_conn()
                        cur2 = conn2.cursor()
                        balances2 = latest_balance_per_adv(cur2)
                        balance_map = {int(r["advertiser_id"]): float(r["account_valid"] or 0) for r in balances2}
                        adv_ids_in_report = sorted([int(a) for a, c in y_cost.items() if float(c) > 0])
                        name_map_r = fetch_adv_name_map(cur2, adv_ids_in_report)
                        cost7 = cost_7d_map(cur2, y)
                    finally:
                        try:
                            cur2.close()
                        except Exception:
                            pass
                        try:
                            conn2.close()
                        except Exception:
                            pass

                    alerted_ids: List[int] = []
                    for adv_id in adv_ids_in_report:
                        adv_id = int(adv_id)
                        bal_v = float(balance_map.get(adv_id, 0.0))
                        y_cost_v = float(y_cost.get(adv_id, 0.0))
                        if y_cost_v <= 0:
                            continue
                        if bal_v < (y_cost_v * 2.0):
                            alerted_ids.append(adv_id)

                    if new_alerts or args.always_notify:
                        try:
                            daily_rows = build_daily_balance_rows(
                                adv_ids_in_report=adv_ids_in_report,
                                name_map=name_map_r,
                                balance_map=balance_map,
                                y_cost_map=y_cost,
                                cost7_map=cost7,
                                unit_mult=args.unit_mult,
                                digits=args.digits,
                            )
                            status_lines: List[str] = []
                            if alerted_ids:
                                status_lines.append(f"【余额预警·每日】⚠️ 触发 {len(alerted_ids)} 个账户：余额 < 昨日消耗 × 2")
                                for adv_id in alerted_ids[:20]:
                                    adv_id = int(adv_id)
                                    nm = name_map_r.get(adv_id, str(adv_id))
                                    bal_v = float(balance_map.get(adv_id, 0.0))
                                    yc_v = float(y_cost.get(adv_id, 0.0))
                                    ratio_v = (bal_v / yc_v) if yc_v > 0 else 0.0
                                    status_lines.append(
                                        f"- {nm}｜余额 {fmt_money(bal_v, args.unit_mult, args.digits)}｜昨日消耗 {fmt_money(yc_v, args.unit_mult, args.digits)}｜倍率 {ratio_v:.2f}"
                                    )
                            else:
                                status_lines.append("【余额预警·每日】✅ 余额充足，未触发预警")

                            status_lines.append("--------------------")
                            status_lines.append(f"【账户资金日报】日期: {str(y)} (昨日)")
                            status_lines.append("字段：余额｜昨日消耗｜7日消耗｜可用天数(余额/7日均消)｜倍率(余额/昨日)")
                            status_md = "\n".join(status_lines)

                            card = build_balance_daily_card(
                                report_date=str(y),
                                status_md=status_md,
                                rows=daily_rows,
                                max_rows=args.report_max_items,
                                header_template='orange' if alerted_ids else 'green',
                            )
                            feishu_send_card(webhook, card, secret=secret)
                            print(f"OK: feishu_notified daily_report(card) alerts={len(alerted_ids)} rule={args.rule}")
                        except Exception as e:
                            print(f"WARNING: feishu_notify_failed err={e!r}")
                    else:
                        print("INFO: no new alerts; RULE_00 without --always-notify; skip sending")
                else:
                    if new_alerts:
                        # --- NEW: limit per-adv daily reminders (default 3) using existing fact_alert_event ---
                        try:
                            conn3 = get_conn()
                            cur3 = conn3.cursor()
                            cnt_map = alert_count_today_map(cur3, rule_id=args.rule, adv_ids=[a["advertiser_id"] for a in new_alerts], day_cn=now_cn.date())
                        finally:
                            try:
                                cur3.close()
                            except Exception:
                                pass
                            try:
                                conn3.close()
                            except Exception:
                                pass

                        send_alerts = [a for a in new_alerts if int(cnt_map.get(int(a["advertiser_id"]), 0)) <= ALERT_MAX_NOTIFY_PER_DAY]
                        suppressed = len(new_alerts) - len(send_alerts)

                        if not send_alerts:
                            print(f"INFO: all alerts suppressed by ALERT_MAX_NOTIFY_PER_DAY={ALERT_MAX_NOTIFY_PER_DAY} rule={args.rule}")
                        else:
                            # Rebuild name_map for only send_alerts (optional)
                            name_map_send = {int(k): v for k, v in name_map.items()} if name_map else {}
                            text = build_feishu_text(
                                rule_id=args.rule,
                                now_cn=now_cn,
                                alerts=sorted(send_alerts, key=lambda x: (x.get("ratio", 0.0))),
                                name_map=name_map_send,
                                unit_mult=args.unit_mult,
                                digits=args.digits,
                                max_items=args.max_items,
                                keyword=keyword,
                            )
                            if suppressed > 0:
                                text += f"\n\n（已对单账户每日提醒次数做上限：{ALERT_MAX_NOTIFY_PER_DAY} 次；本次静默 {suppressed} 条。）"
                            try:
                                ok, resp = feishu_send_text(webhook, text, secret=secret)
                                if ok:
                                    print(f"OK: feishu_notified alerts={len(send_alerts)} suppressed={suppressed} rule={args.rule}")
                                else:
                                    print(f"WARNING: feishu_notify_failed resp={resp}")
                            except Exception as e:
                                print(f"WARNING: feishu_notify_failed err={e!r}")
                    else:
                        print("INFO: no new alerts; skip sending")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
