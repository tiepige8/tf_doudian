#!/usr/bin/env bash
set -euo pipefail

# oe_run_pipeline.sh
# 30分钟跑一次：
# 1) 拉取千川广告账户（含余额+昨日/近7天消耗）-> output/latest.json
# 2) 入库（dim_advertiser / fact_balance_snapshot / fact_finance_daily）
# 3) 执行报警规则 RULE_30M，并在有命中时通过飞书 Webhook 通知（需配置 FEISHU_WEBHOOK_URL）

export TZ="Asia/Shanghai"
umask 027

log() { echo "$(date '+%F %T') [PIPELINE] $*"; }
die() { log "ERROR: $*"; exit 1; }

require_env() {
  local k="$1"
  if [[ -z "${!k:-}" ]]; then
    die "missing env var: ${k} (check /root/oe_env.sh)"
  fi
}

mask() {
  local s="${1:-}"
  if [[ -z "$s" ]]; then echo ""; return; fi
  echo "***"
}

ROOT="/root"
ENV_FILE="${ROOT}/oe_env.sh"
VENV_PY="${ROOT}/venv_oe/bin/python"
ACCOUNTS_PY="${ROOT}/oe_qianchuan_accounts.py"
LOADER_PY="${ROOT}/oe_pg_loader.py"
RULES_PY="${ROOT}/oe_monitor_rules.py"
OUT_JSON="${ROOT}/output/latest.json"

[[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"

# 基础环境检查（避免 cron 环境缺变量）
require_env OE_APP_ID
require_env OE_APP_SECRET
require_env PGHOST
require_env PGPORT
require_env PGDATABASE
require_env PGUSER
require_env PGPASSWORD

cd "$ROOT"

# 增加随机抖动，降低系统级限流碰撞概率
sleep $((RANDOM % 90))

log "START"
log "ENV: PGHOST=${PGHOST} PGPORT=${PGPORT} PGDATABASE=${PGDATABASE} PGUSER=${PGUSER} PGPASSWORD=$(mask "${PGPASSWORD}") PGSSLMODE=${PGSSLMODE:-} FEISHU_WEBHOOK_URL=$(mask "${FEISHU_WEBHOOK_URL:-}")"
log "BIN: VENV_PY=${VENV_PY}"

t0="$(date +%s)"

log "STEP1/3: fetch snapshot -> ${OUT_JSON}"
"${VENV_PY}" "${ACCOUNTS_PY}" run --once

[[ -s "${OUT_JSON}" ]] || die "latest.json missing or empty: ${OUT_JSON}"
log "OK snapshot generated: size=$(stat -c%s "${OUT_JSON}") bytes"

log "STEP2/3: load snapshot into Postgres"
"${VENV_PY}" "${LOADER_PY}" --json "${OUT_JSON}"
log "OK loaded"

log "STEP3/3: evaluate alerts (RULE_30M) + notify"
# 若未设置 FEISHU_WEBHOOK_URL，oe_monitor_rules.py 会给 WARNING 并跳过发送，但仍会入库
"${VENV_PY}" "${RULES_PY}" --rule RULE_30M --notify
log "OK rule evaluated"

t1="$(date +%s)"
log "DONE elapsed=$((t1 - t0))s"
