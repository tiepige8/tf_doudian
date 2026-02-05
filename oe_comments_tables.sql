-- 评论入库 & 隐藏动作记录表（schema=oe）
-- 说明：
-- - oe.fact_comment：存评论明细（raw 里保留全部字段）
-- - oe.fact_comment_action：存动作明细（目前仅 hide），用于飞书汇总去重（notified_at）

CREATE TABLE IF NOT EXISTS oe.fact_comment (
  advertiser_id  BIGINT NOT NULL,
  comment_id     BIGINT NOT NULL,
  comment_time   TIMESTAMPTZ NULL,
  comment_text   TEXT NULL,

  emotion_type   TEXT NULL,      -- NEGATIVE / NEUTRAL / POSITIVE
  hide_status    TEXT NULL,      -- NOT_HIDE / HIDE
  level_type     TEXT NULL,      -- LEVEL_ALL / LEVEL_ONE / LEVEL_TWO

  is_replied     BOOLEAN NULL,
  reply_count    INT NULL,
  like_count     INT NULL,

  user_id        BIGINT NULL,
  user_name      TEXT NULL,

  aweme_id       BIGINT NULL,
  aweme_name     TEXT NULL,
  ad_id          BIGINT NULL,
  ad_name        TEXT NULL,
  creative_id    BIGINT NULL,
  item_id        BIGINT NULL,
  item_title     TEXT NULL,

  raw            JSONB NOT NULL,

  first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  hidden_at      TIMESTAMPTZ NULL,

  PRIMARY KEY (advertiser_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_comment_time
  ON oe.fact_comment (comment_time);

CREATE INDEX IF NOT EXISTS idx_fact_comment_emotion_hide
  ON oe.fact_comment (emotion_type, hide_status);


CREATE TABLE IF NOT EXISTS oe.fact_comment_action (
  advertiser_id  BIGINT NOT NULL,
  comment_id     BIGINT NOT NULL,
  action         TEXT NOT NULL,        -- hide
  action_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status         TEXT NOT NULL,        -- success / failed
  request_id     TEXT NULL,
  error_code     INT NULL,
  error_message  TEXT NULL,
  raw            JSONB NULL,

  notified_at    TIMESTAMPTZ NULL,

  PRIMARY KEY (advertiser_id, comment_id, action)
);

CREATE INDEX IF NOT EXISTS idx_fact_comment_action_notify
  ON oe.fact_comment_action (action, status, notified_at, action_ts);
