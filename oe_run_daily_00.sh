#!/usr/bin/env bash
set -euo pipefail

# oe_run_daily_00.sh
# 每天 00:05 跑一次：
# - 执行日报警规则 RULE_00，并在有命中时通过飞书 Webhook 通知（需配置 FEISHU_WEBHOOK_URL）
#
# 说明：RULE_00 依赖昨日消耗（oe.fact_finance_daily），因此建议确保 pipeline 每天至少跑过一次并完成入库。

export TZ="Asia/Shanghai"
umask 027

log() { echo "$(date '+%F %T') [DAILY00] $*"; }
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
RULES_PY="${ROOT}/oe_monitor_rules.py"

[[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"

require_env PGHOST
require_env PGPORT
require_env PGDATABASE
require_env PGUSER
require_env PGPASSWORD

cd "$ROOT"

log "START"
log "ENV: PGHOST=${PGHOST} PGPORT=${PGPORT} PGDATABASE=${PGDATABASE} PGUSER=${PGUSER} PGPASSWORD=$(mask "${PGPASSWORD}") PGSSLMODE=${PGSSLMODE:-} FEISHU_WEBHOOK_URL=$(mask "${FEISHU_WEBHOOK_URL:-}")"
log "BIN: VENV_PY=${VENV_PY}"

t0="$(date +%s)"
log "STEP1/1: evaluate alerts (RULE_00) + notify"
"${VENV_PY}" "${RULES_PY}" --rule RULE_00 --notify --always-notify --report-max-items 80

t1="$(date +%s)"
log "DONE elapsed=$((t1 - t0))s"
