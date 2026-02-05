#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Load OceanEngine/Qianchuan output/latest.json into PostgreSQL (UPSERT).
# - Upserts:
#   - oe.dim_advertiser
#   - oe.fact_balance_snapshot
#   - oe.fact_finance_daily

import argparse
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras


TZ_CN = timezone(timedelta(hours=8))


def parse_generated_at(s: str) -> datetime:
    # expected: "YYYY-MM-DD HH:MM:SS" (local time, CN)
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=TZ_CN)


def num(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def get_conn():
    dsn = os.getenv("PG_DSN", "").strip()
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST", "").strip()
    db = os.getenv("PGDATABASE", "").strip()
    user = os.getenv("PGUSER", "").strip()
    pwd = os.getenv("PGPASSWORD", "").strip()
    port = os.getenv("PGPORT", "5432").strip()
    sslmode = os.getenv("PGSSLMODE", "require").strip()  # cloud DB usually requires SSL

    missing = [k for k, v in [("PGHOST", host), ("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", pwd)] if not v]
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)} (or set PG_DSN)")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, sslmode=sslmode)


def upsert_dim_advertiser(cur, adv: Dict[str, Any], snapshot_ts: datetime):
    adv_id = int(adv["advertiser_id"])
    cur.execute(
        '''
        INSERT INTO oe.dim_advertiser (
          advertiser_id, advertiser_name, company, first_industry_name, second_industry_name, status,
          first_seen_at, last_seen_at
        )
        VALUES (%(advertiser_id)s, %(advertiser_name)s, %(company)s, %(first_industry_name)s, %(second_industry_name)s, %(status)s,
                %(ts)s, %(ts)s)
        ON CONFLICT (advertiser_id) DO UPDATE SET
          advertiser_name = EXCLUDED.advertiser_name,
          company = COALESCE(EXCLUDED.company, oe.dim_advertiser.company),
          first_industry_name = COALESCE(EXCLUDED.first_industry_name, oe.dim_advertiser.first_industry_name),
          second_industry_name = COALESCE(EXCLUDED.second_industry_name, oe.dim_advertiser.second_industry_name),
          status = COALESCE(EXCLUDED.status, oe.dim_advertiser.status),
          last_seen_at = EXCLUDED.last_seen_at
        ''',
        {
            "advertiser_id": adv_id,
            "advertiser_name": adv.get("advertiser_name"),
            "company": adv.get("company"),
            "first_industry_name": adv.get("first_industry_name"),
            "second_industry_name": adv.get("second_industry_name"),
            "status": str(adv.get("status") or ""),
            "ts": snapshot_ts,
        },
    )


def upsert_balance_snapshot(cur, adv: Dict[str, Any], snapshot_ts: datetime, raw: Dict[str, Any]):
    adv_id = int(adv["advertiser_id"])
    payload = {
        "advertiser_id": adv_id,
        "snapshot_ts": snapshot_ts,
        "account_total": num(adv.get("account_total")),
        "account_valid": num(adv.get("account_valid")),
        "account_frozen": num(adv.get("account_frozen")),
        "account_general_total": num(adv.get("account_general_total")),
        "account_general_valid": num(adv.get("account_general_valid")),
        "account_general_frozen": num(adv.get("account_general_frozen")),
        "account_bidding_total": num(adv.get("account_bidding_total")),
        "account_bidding_valid": num(adv.get("account_bidding_valid")),
        "account_bidding_frozen": num(adv.get("account_bidding_frozen")),
        "raw": psycopg2.extras.Json(raw),
    }

    cur.execute(
        '''
        INSERT INTO oe.fact_balance_snapshot (
          advertiser_id, snapshot_ts,
          account_total, account_valid, account_frozen,
          account_general_total, account_general_valid, account_general_frozen,
          account_bidding_total, account_bidding_valid, account_bidding_frozen,
          raw
        )
        VALUES (
          %(advertiser_id)s, %(snapshot_ts)s,
          %(account_total)s, %(account_valid)s, %(account_frozen)s,
          %(account_general_total)s, %(account_general_valid)s, %(account_general_frozen)s,
          %(account_bidding_total)s, %(account_bidding_valid)s, %(account_bidding_frozen)s,
          %(raw)s
        )
        ON CONFLICT (advertiser_id, snapshot_ts) DO UPDATE SET
          account_total = EXCLUDED.account_total,
          account_valid = EXCLUDED.account_valid,
          account_frozen = EXCLUDED.account_frozen,
          account_general_total = EXCLUDED.account_general_total,
          account_general_valid = EXCLUDED.account_general_valid,
          account_general_frozen = EXCLUDED.account_general_frozen,
          account_bidding_total = EXCLUDED.account_bidding_total,
          account_bidding_valid = EXCLUDED.account_bidding_valid,
          account_bidding_frozen = EXCLUDED.account_bidding_frozen,
          raw = EXCLUDED.raw
        ''',
        payload,
    )


def upsert_finance_daily(cur, advertiser_id: int, row: Dict[str, Any]):
    dt = row.get("date") or row.get("stat_date")
    if not dt:
        return
    payload = {
        "advertiser_id": advertiser_id,
        "date": dt,
        "deduction_cost": num(row.get("deduction_cost")),
        "cost": num(row.get("cost")),
        "cash_cost": num(row.get("cash_cost")),
        "grant_cost": num(row.get("grant_cost")),
        "income": num(row.get("income")),
        "transfer_in": num(row.get("transfer_in")),
        "transfer_out": num(row.get("transfer_out")),
        "cash_balance": num(row.get("cash_balance")),
        "grant_balance": num(row.get("grant_balance")),
        "total_balance": num(row.get("total_balance")),
        "share_cost": num(row.get("share_cost")),
        "qc_aweme_cost": num(row.get("qc_aweme_cost")),
        "qc_aweme_cash_cost": num(row.get("qc_aweme_cash_cost")),
        "qc_aweme_grant_cost": num(row.get("qc_aweme_grant_cost")),
        "share_wallet_cost": num(row.get("share_wallet_cost")),
        "coupon_cost": num(row.get("coupon_cost")),
        "view_delivery_type": row.get("view_delivery_type"),
        "raw": psycopg2.extras.Json(row),
    }

    cur.execute(
        '''
        INSERT INTO oe.fact_finance_daily (
          advertiser_id, date,
          deduction_cost, cost, cash_cost, grant_cost, income, transfer_in, transfer_out,
          cash_balance, grant_balance, total_balance,
          share_cost, qc_aweme_cost, qc_aweme_cash_cost, qc_aweme_grant_cost, share_wallet_cost,
          coupon_cost, view_delivery_type, raw
        )
        VALUES (
          %(advertiser_id)s, %(date)s,
          %(deduction_cost)s, %(cost)s, %(cash_cost)s, %(grant_cost)s, %(income)s, %(transfer_in)s, %(transfer_out)s,
          %(cash_balance)s, %(grant_balance)s, %(total_balance)s,
          %(share_cost)s, %(qc_aweme_cost)s, %(qc_aweme_cash_cost)s, %(qc_aweme_grant_cost)s, %(share_wallet_cost)s,
          %(coupon_cost)s, %(view_delivery_type)s, %(raw)s
        )
        ON CONFLICT (advertiser_id, date) DO UPDATE SET
          deduction_cost = EXCLUDED.deduction_cost,
          cost = EXCLUDED.cost,
          cash_cost = EXCLUDED.cash_cost,
          grant_cost = EXCLUDED.grant_cost,
          income = EXCLUDED.income,
          transfer_in = EXCLUDED.transfer_in,
          transfer_out = EXCLUDED.transfer_out,
          cash_balance = EXCLUDED.cash_balance,
          grant_balance = EXCLUDED.grant_balance,
          total_balance = EXCLUDED.total_balance,
          share_cost = EXCLUDED.share_cost,
          qc_aweme_cost = EXCLUDED.qc_aweme_cost,
          qc_aweme_cash_cost = EXCLUDED.qc_aweme_cash_cost,
          qc_aweme_grant_cost = EXCLUDED.qc_aweme_grant_cost,
          share_wallet_cost = EXCLUDED.share_wallet_cost,
          coupon_cost = EXCLUDED.coupon_cost,
          view_delivery_type = EXCLUDED.view_delivery_type,
          raw = EXCLUDED.raw
        ''',
        payload,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="Path to output/latest.json")
    args = ap.parse_args()

    inv = json.loads(Path(args.json).read_text(encoding="utf-8"))

    gen_at = inv.get("generated_at") or ""
    if not gen_at:
        raise SystemExit("latest.json missing generated_at")
    snapshot_ts = parse_generated_at(gen_at)

    advertisers = inv.get("advertisers") or []
    balances_map = inv.get("balances_map") or {}
    finance_map = inv.get("finance_detail_map") or {}

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for adv in advertisers:
                if not adv or not adv.get("advertiser_id"):
                    continue
                upsert_dim_advertiser(cur, adv, snapshot_ts)

                adv_id = int(adv["advertiser_id"])
                raw_balance = balances_map.get(str(adv_id)) or balances_map.get(adv_id) or {}
                upsert_balance_snapshot(cur, adv, snapshot_ts, raw_balance)

                rows = finance_map.get(str(adv_id)) or finance_map.get(adv_id) or []
                if isinstance(rows, list):
                    for r in rows:
                        if isinstance(r, dict):
                            upsert_finance_daily(cur, adv_id, r)

        conn.commit()
        print(f"OK: loaded advertisers={len(advertisers)} snapshot_ts={snapshot_ts.isoformat()}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
