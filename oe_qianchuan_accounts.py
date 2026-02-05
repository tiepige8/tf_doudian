#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v10：把 OAuth2 “获取Token + 刷新Token”也内置到脚本里，做到全自动维护 access_token/refresh_token（写入本地缓存）。

你只需要做一次“授权拿 auth_code”（浏览器登录同意授权，这是无法完全自动化的），之后脚本会：
- 自动用 auth_code 换取 access_token/refresh_token（并缓存）
- 自动在 access_token 过期前刷新
- 刷新后自动更新 refresh_token（并缓存）

对应文档（你上传的 PDF）：
- 获取Token：POST https://ad.oceanengine.com/open_api/oauth2/access_token/ （grant_type="auth_code" + auth_code）
- 刷新Token：POST https://ad.oceanengine.com/open_api/oauth2/refresh_token/ （grant_type="refresh_token" + refresh_token）

必须环境变量：
  OE_APP_ID
  OE_APP_SECRET

一次性（或 refresh_token 失效时）你需要提供 auth_code：
  - CLI: --auth-code
  - 或环境变量：OE_AUTH_CODE

refresh_token 来源优先级（高->低）：
  1) CLI: --refresh-token
  2) 环境变量 OE_REFRESH_TOKEN
  3) token 文件缓存（默认 ./oe_token_cache.json）

可选环境变量：
  OE_OAUTH_BASE_URL 默认 https://ad.oceanengine.com         （OAuth2 获取/刷新Token）
  OE_BASE_URL_API   默认 https://api.oceanengine.com        （qianchuan 资金/店铺接口等）
  OE_BASE_URL_AD    默认 https://ad.oceanengine.com         （open_api/2/* 以及 finance/detail/get）
  OE_TOKEN_FILE     默认 ./oe_token_cache.json
  OE_OUTPUT_DIR     默认 ./output
  OE_TIMEOUT_SEC    默认 20
  OE_LOG_LEVEL      默认 INFO

功能仍然保持 v9：
- 千川广告账户 advertiser_id 维度：基础信息 + 余额 + 昨天消耗 + 近N天消耗（不含查询当日）
"""

import argparse
import csv
import json
import os
import time
import random
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
import urllib.request
import urllib.error


# ---------------------------
# Config / IO
# ---------------------------

@dataclass
class Config:
    oauth_base_url: str      # OAuth2 token endpoints
    base_url_api: str        # api.oceanengine.com (most v1.0 qianchuan)
    base_url_ad: str         # ad.oceanengine.com (open_api/2 + finance detail)
    app_id: str
    app_secret: str
    token_file: Path
    output_dir: Path
    timeout_sec: int = 20
    # 为降低“系统级别限流”(40100) 触发概率：
    # - request_spacing_ms: 单次 HTTP 请求之间的最小间隔（毫秒）
    # - retry_*: 对 40100 / 网络异常做指数退避重试
    request_spacing_ms: int = 250
    retry_max_attempts: int = 6
    retry_base_sleep_sec: float = 1.0
    retry_max_sleep_sec: float = 20.0


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_date() -> datetime:
    return datetime.now()


def fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"缺少环境变量：{key}")
    return v


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------
# HTTP helpers (stdlib only)
# ---------------------------

def _read_json_resp(resp) -> Dict[str, Any]:
    raw = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return {"code": -1, "message": "non-json response", "raw": raw[:2000]}


def http_get(url: str, params: Dict[str, Any], timeout: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    qs = urlencode(filtered, doseq=True)
    full = url + ("?" + qs if qs else "")
    req = urllib.request.Request(full, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _read_json_resp(resp)
    except urllib.error.HTTPError as e:
        try:
            return _read_json_resp(e)
        except Exception:
            raise


def http_get_with_json_body(url: str, data: Optional[Dict[str, Any]] = None, timeout: int = 20, headers: Optional[Dict[str, str]] = None, **kwargs) -> Dict[str, Any]:
    # compatibility: allow callers to pass json_payload keyword
    if data is None and 'json_payload' in kwargs:
        data = kwargs.get('json_payload')
    if data is None:
        data = {}
    body = json.dumps(data).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=body, headers=req_headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _read_json_resp(resp)
    except urllib.error.HTTPError as e:
        try:
            return _read_json_resp(e)
        except Exception:
            raise


def http_post_json(url: str, data: Dict[str, Any], timeout: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    body = json.dumps(data).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=body, headers=req_headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _read_json_resp(resp)
    except urllib.error.HTTPError as e:
        try:
            return _read_json_resp(e)
        except Exception:
            raise


def oe_check_ok(resp: Dict[str, Any], action: str) -> Any:
    code = resp.get("code", 0)
    msg = resp.get("message") or resp.get("msg") or ""
    help_msg = resp.get("help_message") or resp.get("help") or ""
    rid = resp.get("request_id") or ""
    if code != 0:
        raise RuntimeError(
            f"[{action}] API失败 code={code}, msg={msg}, help={help_msg}, request_id={rid}, resp={str(resp)[:1200]}"
        )
    return resp.get("data", resp)


def _dig_data(obj: Any) -> Any:
    if isinstance(obj, dict) and isinstance(obj.get("data"), (dict, list)):
        return obj["data"]
    return obj


def _as_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def deep_get_list(data: Any) -> List[Dict[str, Any]]:
    d = _dig_data(data)
    if isinstance(d, dict):
        for k in ("list", "items", "advertiser_list", "shop_list", "data_list"):
            v = d.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        if isinstance(d.get("data"), dict):
            return deep_get_list(d["data"])
    if isinstance(d, list):
        return [x for x in d if isinstance(x, dict)]
    return []


def parse_number_list(data: Any) -> List[int]:
    d = _dig_data(data)
    if isinstance(d, dict):
        v = d.get("list")
        if isinstance(v, list):
            out = []
            for x in v:
                xi = _as_int(x)
                if xi is not None:
                    out.append(xi)
            return out
        if isinstance(d.get("data"), dict):
            return parse_number_list(d["data"])
    return []


def parse_shop_adv_list(data: Any) -> Tuple[List[int], List[Dict[str, Any]]]:
    d = _dig_data(data)
    adv_ids: List[int] = []
    adv_items: List[Dict[str, Any]] = []
    if isinstance(d, dict):
        adv_ids.extend(parse_number_list(d))
        v = d.get("adv_id_list")
        if isinstance(v, list):
            for it in v:
                if not isinstance(it, dict):
                    continue
                adv_items.append(it)
                xi = _as_int(it.get("adv_id") or it.get("id") or it.get("advertiser_id"))
                if xi is not None:
                    adv_ids.append(xi)
        if not adv_ids and isinstance(d.get("data"), dict):
            return parse_shop_adv_list(d["data"])
    adv_ids = sorted({x for x in adv_ids if isinstance(x, int)})
    return adv_ids, adv_items


# ---------------------------
# OceanEngine Client
# ---------------------------


class OEClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # 用于控制请求节奏，降低系统级限流(40100)概率
        self._last_req_mono = 0.0

    def _sleep_spacing(self) -> None:
        """在每次HTTP请求前做最小间隔控制。"""
        spacing = max(0.0, float(getattr(self.cfg, "request_spacing_ms", 0)) / 1000.0)
        if spacing <= 0:
            return
        now = time.monotonic()
        delta = now - self._last_req_mono
        if delta < spacing:
            # 给一点轻微抖动，避免多进程/多机同时撞峰
            time.sleep((spacing - delta) + random.uniform(0.0, min(0.05, spacing * 0.3)))
        self._last_req_mono = time.monotonic()

    def _call_with_retry(self, do_call, api_name: str) -> Dict[str, Any]:
        """对系统级限流(40100)与网络异常做指数退避重试。"""
        max_attempts = int(getattr(self.cfg, "retry_max_attempts", 1) or 1)
        base_sleep = float(getattr(self.cfg, "retry_base_sleep_sec", 1.0) or 1.0)
        max_sleep = float(getattr(self.cfg, "retry_max_sleep_sec", 20.0) or 20.0)

        last_resp: Optional[Dict[str, Any]] = None
        for attempt in range(1, max_attempts + 1):
            self._sleep_spacing()
            try:
                resp = do_call()
                last_resp = resp if isinstance(resp, dict) else {"code": -1, "message": "non-dict response"}
            except Exception as e:
                if attempt >= max_attempts:
                    raise
                wait = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
                wait *= random.uniform(0.7, 1.3)
                logging.warning("请求异常，重试 api=%s attempt=%d/%d sleep=%.1fs err=%s", api_name, attempt, max_attempts, wait, repr(e))
                time.sleep(wait)
                continue

            code = None
            try:
                code = int(last_resp.get("code")) if isinstance(last_resp, dict) and last_resp.get("code") is not None else None
            except Exception:
                code = None

            # 正常返回
            if code == 0:
                return last_resp

            # 系统级限流：40100
            if code == 40100 and attempt < max_attempts:
                wait = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
                wait *= random.uniform(0.7, 1.3)
                msg = last_resp.get("message") if isinstance(last_resp, dict) else ""
                logging.warning("系统限流，重试 api=%s attempt=%d/%d sleep=%.1fs code=%s msg=%s", api_name, attempt, max_attempts, wait, code, msg)
                time.sleep(wait)
                continue

            return last_resp

        return last_resp or {"code": -1, "message": "empty response"}

    def _http_get_retry(self, url: str, params: Dict[str, Any], headers: Dict[str, str], api_name: str) -> Dict[str, Any]:
        return self._call_with_retry(
            lambda: http_get(url, params=params, headers=headers, timeout=self.cfg.timeout_sec),
            api_name=api_name,
        )

    def _http_get_with_json_body_retry(self, url: str, json_payload: Dict[str, Any], headers: Dict[str, str], api_name: str) -> Dict[str, Any]:
        return self._call_with_retry(
            lambda: http_get_with_json_body(url, data=json_payload, headers=headers, timeout=self.cfg.timeout_sec),
            api_name=api_name,
        )

    # OAuth2 endpoints (per docs, hosted on ad.oceanengine.com)
    @property
    def url_access_token(self) -> str:
        return f"{self.cfg.oauth_base_url}/open_api/oauth2/access_token/"

    @property
    def url_refresh_token(self) -> str:
        return f"{self.cfg.oauth_base_url}/open_api/oauth2/refresh_token/"

    # OAuth2 advertiser list (api domain也可用；保留原实现)
    @property
    def url_advertiser_get(self) -> str:
        return f"{self.cfg.base_url_api}/open_api/oauth2/advertiser/get/"

    # 店铺->千川广告账户列表
    @property
    def url_qc_shop_advertiser_list(self) -> str:
        return f"{self.cfg.base_url_api}/open_api/v1.0/qianchuan/shop/advertiser/list/"

    # 代理商->广告账户列表（如果后续补授权，可继续用）
    @property
    def url_agent_advertiser_select(self) -> str:
        return f"{self.cfg.base_url_ad}/open_api/2/agent/advertiser/select/"

    # 千川广告账户基础信息（GET + JSON body 兼容）
    @property
    def url_qc_advertiser_public_info(self) -> str:
        return f"{self.cfg.base_url_ad}/open_api/2/advertiser/public_info/"

    # 获取账户余额
    @property
    def url_qc_account_balance_get(self) -> str:
        return f"{self.cfg.base_url_api}/open_api/v1.0/qianchuan/account/balance/get/"

    # 获取财务流水信息
    @property
    def url_qc_finance_detail_get(self) -> str:
        return f"{self.cfg.base_url_ad}/open_api/v1.0/qianchuan/finance/detail/get/"

    # ---- cache helpers ----
    def load_cache(self) -> Dict[str, Any]:
        return read_json(self.cfg.token_file)

    def save_cache(self, cache: Dict[str, Any]) -> None:
        write_json(self.cfg.token_file, cache)

    def set_refresh_token(self, refresh_token: str) -> None:
        cache = self.load_cache()
        cache.update({
            "refresh_token": refresh_token.strip(),
            "updated_at": now_str(),
            "oauth_base_url": self.cfg.oauth_base_url,
            "base_url_api": self.cfg.base_url_api,
            "base_url_ad": self.cfg.base_url_ad,
            "app_id": self.cfg.app_id,
        })
        self.save_cache(cache)

    def _write_tokens_to_cache(self, data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        data 期望包含：access_token, expires_in, refresh_token, refresh_token_expires_in
        """
        cache = self.load_cache()
        now_epoch = int(time.time())

        access_token = (data.get("access_token") or "").strip()
        refresh_token = (data.get("refresh_token") or "").strip()
        expires_in = int(data.get("expires_in") or 0) or 0
        rt_expires_in = int(data.get("refresh_token_expires_in") or data.get("refresh_token_expires") or 0) or 0

        if access_token:
            cache["access_token"] = access_token
        if refresh_token:
            cache["refresh_token"] = refresh_token

        cache.update({
            "oauth_base_url": self.cfg.oauth_base_url,
            "base_url_api": self.cfg.base_url_api,
            "base_url_ad": self.cfg.base_url_ad,
            "app_id": self.cfg.app_id,
            "updated_at": now_str(),
            "token_source": source,
        })

        if expires_in:
            cache["expires_in"] = expires_in
            cache["expires_at_epoch"] = now_epoch + expires_in

        if rt_expires_in:
            cache["refresh_token_expires_in"] = rt_expires_in
            cache["refresh_expires_at_epoch"] = now_epoch + rt_expires_in

        self.save_cache(cache)
        return cache

    # ---- OAuth2: get token by auth_code (one-time) ----
    def exchange_auth_code(self, auth_code: str) -> Dict[str, Any]:
        payload = {
            "app_id": self.cfg.app_id,
            "secret": self.cfg.app_secret,
            "grant_type": "auth_code",
            "auth_code": auth_code.strip(),
        }
        resp = http_post_json(self.url_access_token, payload, timeout=self.cfg.timeout_sec)
        data = oe_check_ok(resp, "oauth2_access_token")
        return self._write_tokens_to_cache(data, source="auth_code")

    # ---- refresh token automatically ----
    def get_refresh_token(self, cli_refresh_token: Optional[str]) -> str:
        if cli_refresh_token and cli_refresh_token.strip():
            return cli_refresh_token.strip()
        env_rt = os.getenv("OE_REFRESH_TOKEN", "").strip()
        if env_rt:
            return env_rt
        cache = self.load_cache()
        rt = (cache.get("refresh_token") or "").strip()
        if rt:
            return rt
        return ""

    def refresh_access_token(self, refresh_token: str, force: bool = False) -> Dict[str, Any]:
        """
        刷新策略：
        - access_token 未过期（提前120s）则复用缓存；否则刷新
        - 刷新成功会回写新的 access_token + refresh_token（按文档会返回新 refresh_token）
        """
        cache = self.load_cache()
        now_epoch = int(time.time())
        access_token = (cache.get("access_token") or "").strip()
        expires_at = int(cache.get("expires_at_epoch") or 0)

        # 提前120秒刷新
        if (not force) and access_token and expires_at and now_epoch < (expires_at - 120):
            return cache

        payload = {
            "app_id": self.cfg.app_id,
            "secret": self.cfg.app_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token.strip(),
        }
        resp = http_post_json(self.url_refresh_token, payload, timeout=self.cfg.timeout_sec)
        data = oe_check_ok(resp, "oauth2_refresh_token")
        return self._write_tokens_to_cache(data, source="refresh_token")

    def ensure_access_token(self, cli_refresh_token: Optional[str], cli_auth_code: Optional[str]) -> Dict[str, Any]:
        """
        目标：返回包含可用 access_token 的 cache（必要时自动获取/刷新）。
        顺序：
        1) 有 refresh_token：尽力刷新/复用 -> 成功返回
        2) 没 refresh_token 但有 auth_code：exchange -> 返回
        3) 都没有：报错（需要一次性授权）
        """
        rt = self.get_refresh_token(cli_refresh_token)
        if rt:
            try:
                return self.refresh_access_token(rt)
            except Exception as e:
                logging.warning("refresh_token 刷新失败，将尝试使用 auth_code（如提供）。err=%s", e)

        auth_code = (cli_auth_code or os.getenv("OE_AUTH_CODE", "")).strip()
        if auth_code:
            cache = self.exchange_auth_code(auth_code)
            # auth_code 只可用一次，成功后建议清空环境变量，避免误用
            return cache

        raise RuntimeError("缺少 refresh_token 且未提供 auth_code：需要先完成一次浏览器授权拿到 auth_code")

    # ---- business APIs ----
    def advertiser_get_all(self, access_token: str, page_size: int = 100) -> List[Dict[str, Any]]:
        headers = {"Access-Token": access_token}
        out: List[Dict[str, Any]] = []
        page = 1
        while True:
            resp = self._http_get_retry(self.url_advertiser_get, params={"page": page, "page_size": page_size}, headers=headers, api_name="advertiser_get")
            data = oe_check_ok(resp, "oauth2_advertiser_get")
            d = _dig_data(data)
            lst: List[Dict[str, Any]] = []
            if isinstance(d, dict):
                for k in ("list", "items", "advertiser_list", "data_list"):
                    v = d.get(k)
                    if isinstance(v, list):
                        lst = [x for x in v if isinstance(x, dict)]
                        break
            if not lst:
                break
            out.extend(lst)
            if len(lst) < page_size:
                break
            page += 1
        return out

    def qc_shop_advertiser_list(self, access_token: str, shop_id: int, page_size: int = 100) -> Tuple[List[int], List[Dict[str, Any]]]:
        headers = {"Access-Token": access_token}
        adv_ids_all: List[int] = []
        adv_items_all: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = {"shop_id": shop_id, "page": page, "page_size": page_size}
            resp = self._http_get_retry(self.url_qc_shop_advertiser_list, params=params, headers=headers, api_name="qc_shop_advertiser_list")
            data = oe_check_ok(resp, "qianchuan_shop_advertiser_list")
            adv_ids, adv_items = parse_shop_adv_list(data)
            adv_ids_all.extend(adv_ids)
            adv_items_all.extend(adv_items)

            d = _dig_data(data)
            page_info = d.get("page_info") if isinstance(d, dict) else None
            total_page = int(page_info.get("total_page") or 0) if isinstance(page_info, dict) else 0

            if total_page and page >= total_page:
                break
            if (not adv_ids) and (not adv_items):
                break
            if (not total_page) and (len(adv_ids) + len(adv_items)) < page_size:
                break
            page += 1

        adv_ids_all = sorted({x for x in adv_ids_all if isinstance(x, int)})
        return adv_ids_all, adv_items_all

    def agent_advertiser_select(self, access_token: str, agent_advertiser_id: int, page_size: int = 100) -> List[int]:
        headers = {"Access-Token": access_token}
        out: List[int] = []
        page = 1
        while True:
            params = {"advertiser_id": agent_advertiser_id, "page": page, "page_size": page_size}
            resp = self._http_get_retry(self.url_agent_advertiser_select, params=params, headers=headers, api_name="agent_advertiser_select")
            data = oe_check_ok(resp, "agent_advertiser_select")
            ids = parse_number_list(data)
            if not ids:
                break
            out.extend(ids)

            d = _dig_data(data)
            page_info = d.get("page_info") if isinstance(d, dict) else None
            total_page = int(page_info.get("total_page") or 0) if isinstance(page_info, dict) else 0
            if total_page and page >= total_page:
                break
            if len(ids) < page_size and not total_page:
                break
            page += 1

        return sorted({x for x in out if isinstance(x, int)})

    def qc_advertiser_public_info(self, access_token: str, advertiser_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not advertiser_ids:
            return {}
        headers = {"Access-Token": access_token}
        out: Dict[int, Dict[str, Any]] = {}

        batch_size = 100
        for i in range(0, len(advertiser_ids), batch_size):
            batch = advertiser_ids[i:i + batch_size]
            payload = {"advertiser_ids": batch}

            resp = self._http_get_with_json_body_retry(self.url_qc_advertiser_public_info, json_payload=payload, headers=headers, api_name="qc_advertiser_public_info")
            try:
                data = oe_check_ok(resp, "qc_advertiser_public_info")
            except Exception:
                resp2 = self._http_get_retry(self.url_qc_advertiser_public_info, params={"advertiser_ids": batch}, headers=headers, api_name="qc_advertiser_public_info")
                data = oe_check_ok(resp2, "qc_advertiser_public_info")

            items = deep_get_list(data)
            for item in items:
                adv_id = _as_int(item.get("id") or item.get("advertiser_id") or item.get("account_id"))
                if adv_id is None:
                    continue
                out[adv_id] = item

        return out

    def qc_account_balance_get(self, access_token: str, advertiser_id: int) -> Dict[str, Any]:
        headers = {"Access-Token": access_token}
        params = {"advertiser_id": advertiser_id}
        resp = self._http_get_retry(self.url_qc_account_balance_get, params=params, headers=headers, api_name="qc_account_balance_get")
        data = oe_check_ok(resp, "qc_account_balance_get")
        d = _dig_data(data)
        return d if isinstance(d, dict) else {}

    def qc_finance_detail_get(self, access_token: str, advertiser_id: int, start_date: str, end_date: str, page: int, page_size: int) -> Dict[str, Any]:
        headers = {"Access-Token": access_token}
        params = {
            "advertiser_id": advertiser_id,
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "page_size": page_size,
        }
        resp = self._http_get_retry(self.url_qc_finance_detail_get, params=params, headers=headers, api_name="qc_finance_detail_get")
        data = oe_check_ok(resp, "qc_finance_detail_get")
        d = _dig_data(data)
        return d if isinstance(d, dict) else {}


# ---------------------------
# Inventory builder (same as v9)
# ---------------------------

def infer_shops(authorized: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    shops = []
    for a in authorized:
        role = (a.get("account_role") or a.get("account_type") or "").strip()
        if role == "PLATFORM_ROLE_SHOP_ACCOUNT":
            shop_id_int = _as_int(a.get("account_id") or a.get("advertiser_id") or a.get("id"))
            if shop_id_int is None:
                continue
            shops.append({
                "shop_id": shop_id_int,
                "shop_name": a.get("account_name") or a.get("advertiser_name") or "",
                "raw": a,
            })
    return shops


def infer_agents(authorized: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agents = []
    for a in authorized:
        role = (a.get("account_role") or a.get("account_type") or "").strip()
        if "AGENT" in role:
            agent_id_int = _as_int(a.get("account_id") or a.get("advertiser_id") or a.get("id"))
            if agent_id_int is None:
                continue
            agents.append({
                "agent_advertiser_id": agent_id_int,
                "agent_name": a.get("account_name") or a.get("advertiser_name") or "",
                "raw": a,
            })
    return agents


def _safe_num(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def compute_spend_from_detail(detail_list: List[Dict[str, Any]], query_dt: datetime, spend_days: int) -> Tuple[float, float]:
    y_dt = query_dt.date() - timedelta(days=1)
    start_dt = query_dt.date() - timedelta(days=spend_days)
    end_dt = y_dt

    y_str = y_dt.strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    cost_y = 0.0
    cost_n = 0.0
    for row in detail_list:
        d = (row.get("date") or "").strip()
        c = _safe_num(row.get("cost"))
        if d == y_str:
            cost_y += c
        if start_str <= d <= end_str:
            cost_n += c
    return cost_y, cost_n


def build_inventory(client: OEClient, access_token: str, spend_days: int, finance_page_size: int) -> Dict[str, Any]:
    query_dt = today_date()
    query_date_str = fmt_date(query_dt)
    y_date_str = fmt_date(query_dt - timedelta(days=1))
    start_date_str = fmt_date(query_dt - timedelta(days=spend_days))

    authorized = client.advertiser_get_all(access_token)
    shops = infer_shops(authorized)
    agents = infer_agents(authorized)
    logging.info("授权账户=%d；识别店铺=%d；识别代理商=%d", len(authorized), len(shops), len(agents))

    shop_map_rows: List[Dict[str, Any]] = []
    agent_map_rows: List[Dict[str, Any]] = []
    all_adv_ids: List[int] = []

    for s in shops:
        try:
            adv_ids, adv_items = client.qc_shop_advertiser_list(access_token, s["shop_id"])
        except Exception as e:
            logging.warning("店铺接口失败 shop_id=%s err=%s", s["shop_id"], e)
            continue

        item_by_id: Dict[int, Dict[str, Any]] = {}
        for it in adv_items:
            adv_id = _as_int(it.get("adv_id") or it.get("id") or it.get("advertiser_id"))
            if adv_id is not None:
                item_by_id[adv_id] = it

        for adv_id in adv_ids:
            all_adv_ids.append(adv_id)
            it = item_by_id.get(adv_id, {})
            extra_perm = it.get("extra_permission")
            if isinstance(extra_perm, list):
                extra_perm = ",".join([str(x) for x in extra_perm if x is not None])

            shop_map_rows.append({
                "parent_type": "shop",
                "parent_id": s["shop_id"],
                "parent_name": s["shop_name"],
                "advertiser_id": adv_id,
                "extra_permission": extra_perm or "",
                "account_source": "QC_QIANCHUAN",
            })

    for ag in agents:
        try:
            adv_ids = client.agent_advertiser_select(access_token, ag["agent_advertiser_id"])
        except Exception as e:
            logging.warning("代理商接口失败 agent_advertiser_id=%s err=%s", ag["agent_advertiser_id"], e)
            continue

        for adv_id in adv_ids:
            all_adv_ids.append(adv_id)
            agent_map_rows.append({
                "parent_type": "agent",
                "parent_id": ag["agent_advertiser_id"],
                "parent_name": ag["agent_name"],
                "advertiser_id": adv_id,
                "extra_permission": "",
                "account_source": "QC_QIANCHUAN",
            })

    uniq_adv_ids = sorted({x for x in all_adv_ids if isinstance(x, int)})
    logging.info("汇总千川广告账户 advertiser_id=%d", len(uniq_adv_ids))

    adv_info_map: Dict[int, Dict[str, Any]] = {}
    if uniq_adv_ids:
        try:
            adv_info_map = client.qc_advertiser_public_info(access_token, uniq_adv_ids)
        except Exception as e:
            logging.warning("获取千川广告账户基础信息失败 err=%s", e)

    balance_map: Dict[int, Dict[str, Any]] = {}
    finance_detail_map: Dict[int, List[Dict[str, Any]]] = {}
    cost_map: Dict[int, Dict[str, Any]] = {}

    for idx, adv_id in enumerate(uniq_adv_ids, start=1):
        try:
            bal = client.qc_account_balance_get(access_token, adv_id)
            balance_map[adv_id] = bal
        except Exception as e:
            logging.warning("余额接口失败 advertiser_id=%s err=%s", adv_id, e)
            balance_map[adv_id] = {}

        try:
            page = 1
            rows_all: List[Dict[str, Any]] = []
            while True:
                d = client.qc_finance_detail_get(access_token, adv_id, start_date_str, y_date_str, page, finance_page_size)
                rows = deep_get_list(d)
                if rows:
                    rows_all.extend(rows)

                page_info = d.get("page_info") if isinstance(d, dict) else None
                total_page = int(page_info.get("total_page") or 0) if isinstance(page_info, dict) else 0
                if total_page and page >= total_page:
                    break
                if (not total_page) and (not rows or len(rows) < finance_page_size):
                    break
                page += 1

            finance_detail_map[adv_id] = rows_all
            cost_y, cost_n = compute_spend_from_detail(rows_all, query_dt=query_dt, spend_days=spend_days)
            cost_map[adv_id] = {
                "cost_yesterday": cost_y,
                f"cost_last{spend_days}d_excl_today": cost_n,
                "spend_start_date": start_date_str,
                "spend_end_date": y_date_str,
                "spend_query_date": query_date_str,
            }
        except Exception as e:
            logging.warning("财务流水接口失败 advertiser_id=%s err=%s", adv_id, e)
            finance_detail_map[adv_id] = []
            cost_map[adv_id] = {
                "cost_yesterday": 0.0,
                f"cost_last{spend_days}d_excl_today": 0.0,
                "spend_start_date": start_date_str,
                "spend_end_date": y_date_str,
                "spend_query_date": query_date_str,
                "_error": str(e)[:500],
            }

        if idx % 20 == 0 or idx == len(uniq_adv_ids):
            logging.info("进度：余额/消耗 %d/%d", idx, len(uniq_adv_ids))

    def enrich(rows: List[Dict[str, Any]]) -> None:
        for r in rows:
            adv_id = r.get("advertiser_id")
            info = adv_info_map.get(int(adv_id), {}) if isinstance(adv_id, int) else {}
            bal = balance_map.get(int(adv_id), {}) if isinstance(adv_id, int) else {}
            cst = cost_map.get(int(adv_id), {}) if isinstance(adv_id, int) else {}

            r["advertiser_name"] = info.get("name") or info.get("advertiser_name") or ""
            r["company"] = info.get("company") or ""
            r["first_industry_name"] = info.get("first_industry_name") or ""
            r["second_industry_name"] = info.get("second_industry_name") or ""

            r["account_total"] = bal.get("account_total")
            r["account_valid"] = bal.get("account_valid")
            r["account_frozen"] = bal.get("account_frozen")

            r["account_general_total"] = bal.get("account_general_total")
            r["account_general_valid"] = bal.get("account_general_valid")
            r["account_general_frozen"] = bal.get("account_general_frozen")

            r["account_bidding_total"] = bal.get("account_bidding_total")
            r["account_bidding_valid"] = bal.get("account_bidding_valid")
            r["account_bidding_frozen"] = bal.get("account_bidding_frozen")

            r["cost_yesterday"] = cst.get("cost_yesterday")
            r[f"cost_last{spend_days}d_excl_today"] = cst.get(f"cost_last{spend_days}d_excl_today")

            r["spend_start_date"] = cst.get("spend_start_date")
            r["spend_end_date"] = cst.get("spend_end_date")
            r["spend_query_date"] = cst.get("spend_query_date")

    enrich(shop_map_rows)
    enrich(agent_map_rows)

    advertisers_rows: List[Dict[str, Any]] = []
    for adv_id in uniq_adv_ids:
        info = adv_info_map.get(adv_id, {})
        bal = balance_map.get(adv_id, {})
        cst = cost_map.get(adv_id, {})
        advertisers_rows.append({
            "advertiser_id": adv_id,
            "advertiser_name": info.get("name") or info.get("advertiser_name") or "",
            "company": info.get("company") or "",
            "first_industry_name": info.get("first_industry_name") or "",
            "second_industry_name": info.get("second_industry_name") or "",

            "account_total": bal.get("account_total"),
            "account_valid": bal.get("account_valid"),
            "account_frozen": bal.get("account_frozen"),

            "account_general_total": bal.get("account_general_total"),
            "account_general_valid": bal.get("account_general_valid"),
            "account_general_frozen": bal.get("account_general_frozen"),

            "account_bidding_total": bal.get("account_bidding_total"),
            "account_bidding_valid": bal.get("account_bidding_valid"),
            "account_bidding_frozen": bal.get("account_bidding_frozen"),

            "cost_yesterday": cst.get("cost_yesterday"),
            f"cost_last{spend_days}d_excl_today": cst.get(f"cost_last{spend_days}d_excl_today"),
            "spend_start_date": cst.get("spend_start_date"),
            "spend_end_date": cst.get("spend_end_date"),
            "spend_query_date": cst.get("spend_query_date"),
        })

    return {
        "generated_at": now_str(),
        "spend_days": spend_days,
        "spend_query_date": query_date_str,
        "spend_start_date": start_date_str,
        "spend_end_date": y_date_str,

        "shops": shops,
        "agents": agents,
        "shop_advertiser_map": shop_map_rows,
        "agent_advertiser_map": agent_map_rows,

        "advertiser_ids": uniq_adv_ids,
        "advertiser_info_map": adv_info_map,
        "balances_map": balance_map,
        "finance_detail_map": finance_detail_map,
        "cost_map": cost_map,
        "advertisers": advertisers_rows,
    }


# ---------------------------
# Output
# ---------------------------

def write_outputs(output_dir: Path, inv: Dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    archive_json = output_dir / f"qianchuan_advertisers_finance_{ts}.json"
    latest_json = output_dir / "latest.json"
    text = json.dumps(inv, ensure_ascii=False, indent=2)
    archive_json.write_text(text, encoding="utf-8")
    latest_json.write_text(text, encoding="utf-8")

    spend_days = int(inv.get("spend_days") or 7)
    map_rows = (inv.get("shop_advertiser_map") or []) + (inv.get("agent_advertiser_map") or [])
    fieldnames_map = [
        "parent_type", "parent_id", "parent_name",
        "advertiser_id", "advertiser_name",
        "company", "first_industry_name", "second_industry_name",
        "account_total", "account_valid", "account_frozen",
        "account_general_total", "account_general_valid", "account_general_frozen",
        "account_bidding_total", "account_bidding_valid", "account_bidding_frozen",
        "cost_yesterday", f"cost_last{spend_days}d_excl_today",
        "spend_start_date", "spend_end_date", "spend_query_date",
        "account_source", "extra_permission",
    ]

    archive_csv = output_dir / f"qianchuan_advertisers_finance_{ts}.csv"
    latest_csv = output_dir / "latest.csv"

    def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fieldnames})

    write_csv(archive_csv, map_rows, fieldnames_map)
    write_csv(latest_csv, map_rows, fieldnames_map)

    advertisers_rows = inv.get("advertisers") or []
    fieldnames_adv = [
        "advertiser_id", "advertiser_name", "company",
        "first_industry_name", "second_industry_name",
        "account_total", "account_valid", "account_frozen",
        "account_general_total", "account_general_valid", "account_general_frozen",
        "account_bidding_total", "account_bidding_valid", "account_bidding_frozen",
        "cost_yesterday", f"cost_last{spend_days}d_excl_today",
        "spend_start_date", "spend_end_date", "spend_query_date",
    ]
    archive_adv_csv = output_dir / f"qianchuan_advertisers_only_finance_{ts}.csv"
    latest_adv_csv = output_dir / "latest_advertisers.csv"
    write_csv(archive_adv_csv, advertisers_rows, fieldnames_adv)
    write_csv(latest_adv_csv, advertisers_rows, fieldnames_adv)

    logging.info("输出完成：%s  %s  %s", archive_json, latest_csv, latest_adv_csv)


# ---------------------------
# CLI
# ---------------------------

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="写入 refresh_token（可选）或用 auth_code 换取并写入token缓存")
    p_init.add_argument("--refresh-token", help="你的 refresh_token（可选）")
    p_init.add_argument("--auth-code", help="一次性授权拿到的 auth_code（可选，优先于 refresh-token）")

    p_run = sub.add_parser("run", help="自动获取/刷新token，并拉取千川广告账户清单（含余额与消耗）")
    p_run.add_argument("--once", action="store_true", help="只跑一次（推荐配合cron）")
    p_run.add_argument("--interval-sec", type=int, default=3600, help="循环间隔秒（默认3600）")
    p_run.add_argument("--refresh-token", help="可选：直接传 refresh_token（覆盖本地/环境变量）")
    p_run.add_argument("--auth-code", help="可选：当没有refresh_token或刷新失败时，用auth_code换取token")
    p_run.add_argument("--spend-days", type=int, default=7, help="近N天总消耗（不含查询当日），默认7")
    p_run.add_argument("--page-size", type=int, default=200, help="财务流水每页数量，默认200")

    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("OE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    oauth_base_url = os.getenv("OE_OAUTH_BASE_URL", os.getenv("OE_BASE_URL_OAUTH", "https://ad.oceanengine.com")).rstrip("/")
    base_url_api = os.getenv("OE_BASE_URL_API", os.getenv("OE_BASE_URL", "https://api.oceanengine.com")).rstrip("/")
    base_url_ad = os.getenv("OE_BASE_URL_AD", "https://ad.oceanengine.com").rstrip("/")

    cfg = Config(
        oauth_base_url=oauth_base_url,
        base_url_api=base_url_api,
        base_url_ad=base_url_ad,
        app_id=must_env("OE_APP_ID"),
        app_secret=must_env("OE_APP_SECRET"),
        token_file=Path(os.getenv("OE_TOKEN_FILE", "./oe_token_cache.json")),
        output_dir=Path(os.getenv("OE_OUTPUT_DIR", "./output")),
        timeout_sec=int(os.getenv("OE_TIMEOUT_SEC", "20")),
        request_spacing_ms=int(os.getenv("OE_REQUEST_SPACING_MS", "250")),
        retry_max_attempts=int(os.getenv("OE_RETRY_MAX_ATTEMPTS", "6")),
        retry_base_sleep_sec=float(os.getenv("OE_RETRY_BASE_SLEEP_SEC", "1.0")),
        retry_max_sleep_sec=float(os.getenv("OE_RETRY_MAX_SLEEP_SEC", "20.0")),
    )

    client = OEClient(cfg)

    if args.cmd == "init":
        # auth_code 优先
        ac = (args.auth_code or os.getenv("OE_AUTH_CODE", "")).strip()
        if ac:
            client.exchange_auth_code(ac)
            logging.info("已用 auth_code 换取并写入 token 缓存：%s", cfg.token_file)
            return

        rt = (args.refresh_token or os.getenv("OE_REFRESH_TOKEN", "")).strip()
        if rt:
            client.set_refresh_token(rt)
            logging.info("已写入 refresh_token 到 %s", cfg.token_file)
            return

        raise RuntimeError("init 需要 --auth-code 或 --refresh-token（至少一个）")

    if args.cmd == "run":
        interval = max(60, int(args.interval_sec))
        spend_days = max(1, int(args.spend_days))
        page_size = max(1, min(200, int(args.page_size)))

        while True:
            try:
                cache = client.ensure_access_token(args.refresh_token, args.auth_code)
                access_token = (cache.get("access_token") or "").strip()
                inv = build_inventory(client, access_token, spend_days=spend_days, finance_page_size=page_size)
                write_outputs(cfg.output_dir, inv)
            except Exception as e:
                logging.exception("本轮失败：%s", e)

            if args.once:
                break
            time.sleep(interval)


if __name__ == "__main__":
    main()
