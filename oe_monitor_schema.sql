-- OceanEngine/Qianchuan monitoring schema (PostgreSQL)
-- Recommended: set DB timezone to Asia/Shanghai at the instance level if possible.

CREATE SCHEMA IF NOT EXISTS oe;

-- 1) Advertiser dimension
CREATE TABLE IF NOT EXISTS oe.dim_advertiser (
  advertiser_id        BIGINT PRIMARY KEY,
  advertiser_name      TEXT,
  company              TEXT,
  first_industry_name  TEXT,
  second_industry_name TEXT,
  status               TEXT,
  first_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2) Balance snapshot (30min/hour/day snapshots)
CREATE TABLE IF NOT EXISTS oe.fact_balance_snapshot (
  advertiser_id BIGINT NOT NULL,
  snapshot_ts   TIMESTAMPTZ NOT NULL,

  -- account balances
  account_total   NUMERIC(20,6),
  account_valid   NUMERIC(20,6),
  account_frozen  NUMERIC(20,6),

  -- general balances
  account_general_total  NUMERIC(20,6),
  account_general_valid  NUMERIC(20,6),
  account_general_frozen NUMERIC(20,6),

  -- bidding balances
  account_bidding_total  NUMERIC(20,6),
  account_bidding_valid  NUMERIC(20,6),
  account_bidding_frozen NUMERIC(20,6),

  raw JSONB,

  PRIMARY KEY (advertiser_id, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS ix_balance_adv_ts ON oe.fact_balance_snapshot (advertiser_id, snapshot_ts DESC);

-- 3) Finance daily detail (from /qianchuan/finance/detail/get/, day granularity)
CREATE TABLE IF NOT EXISTS oe.fact_finance_daily (
  advertiser_id BIGINT NOT NULL,
  date          DATE NOT NULL,

  deduction_cost NUMERIC(20,6),
  cost           NUMERIC(20,6),
  cash_cost      NUMERIC(20,6),
  grant_cost     NUMERIC(20,6),
  income         NUMERIC(20,6),
  transfer_in    NUMERIC(20,6),
  transfer_out   NUMERIC(20,6),
  cash_balance   NUMERIC(20,6),
  grant_balance  NUMERIC(20,6),
  total_balance  NUMERIC(20,6),

  share_cost         NUMERIC(20,6),
  qc_aweme_cost      NUMERIC(20,6),
  qc_aweme_cash_cost NUMERIC(20,6),
  qc_aweme_grant_cost NUMERIC(20,6),
  share_wallet_cost  NUMERIC(20,6),
  coupon_cost        NUMERIC(20,6),
  view_delivery_type TEXT,

  raw JSONB,

  PRIMARY KEY (advertiser_id, date)
);

CREATE INDEX IF NOT EXISTS ix_finance_adv_date ON oe.fact_finance_daily (advertiser_id, date DESC);

-- Optional: hourly spend (only if you later integrate an hour-granularity reporting API)
CREATE TABLE IF NOT EXISTS oe.fact_spend_hourly (
  advertiser_id BIGINT NOT NULL,
  hour_ts       TIMESTAMPTZ NOT NULL, -- start of hour, e.g. 2025-12-19 13:00:00+08
  spend         NUMERIC(20,6) NOT NULL,
  raw JSONB,
  PRIMARY KEY (advertiser_id, hour_ts)
);

-- 4) Alert events (dedup + audit trail)
CREATE TABLE IF NOT EXISTS oe.fact_alert_event (
  id BIGSERIAL PRIMARY KEY,
  alert_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  advertiser_id BIGINT NOT NULL,

  rule_id      TEXT NOT NULL,        -- RULE_00 / RULE_30M / RULE_1H
  severity     TEXT NOT NULL,        -- info/warn/crit
  balance_valid NUMERIC(20,6) NOT NULL,
  baseline_spend NUMERIC(20,6) NOT NULL,
  threshold_multiplier NUMERIC(10,2) NOT NULL,
  ratio        NUMERIC(20,6) NOT NULL, -- balance_valid / baseline_spend (if baseline>0)

  snapshot_ts  TIMESTAMPTZ,
  baseline_ts  TIMESTAMPTZ,
  status       TEXT NOT NULL DEFAULT 'open', -- open/acked/closed
  dedup_key    TEXT NOT NULL,
  detail       JSONB,

  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_dedup ON oe.fact_alert_event (dedup_key);
CREATE INDEX IF NOT EXISTS ix_alert_adv_ts ON oe.fact_alert_event (advertiser_id, alert_ts DESC);
