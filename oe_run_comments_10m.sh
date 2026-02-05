#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
APP_ROOT="${APP_ROOT:-${SCRIPT_DIR}}"
ENV_FILE="${OE_ENV_FILE:-/etc/tf_doudian/oe_env.sh}"
if [[ ! -f "${ENV_FILE}" && -f "${APP_ROOT}/oe_env.sh" ]]; then
  ENV_FILE="${APP_ROOT}/oe_env.sh"
fi

TS="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS [COMMENT_PIPE] START"

# 一定要 source 环境变量（PG / OE / FEISHU）
[[ -f "${ENV_FILE}" ]] || { echo "$TS [COMMENT_PIPE] ERROR env file not found: ${ENV_FILE}"; exit 1; }
# shellcheck disable=SC1090
source "${ENV_FILE}"

if [[ -z "${OE_TOKEN_CACHE:-}" && -n "${OE_TOKEN_FILE:-}" ]]; then
  export OE_TOKEN_CACHE="${OE_TOKEN_FILE}"
fi

# 简要打印关键环境（避免泄露密码）
echo "$TS [COMMENT_PIPE] ENV: PGHOST=${PGHOST:-} PGPORT=${PGPORT:-} PGDATABASE=${PGDATABASE:-} PGUSER=${PGUSER:-} PGSSLMODE=${PGSSLMODE:-} FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK_URL:+***}"

VENV_PY="${PYTHON_BIN:-${APP_ROOT}/venv_oe/bin/python}"
COMMENTS_PY="${APP_ROOT}/oe_qianchuan_comments.py"
[[ -x "${VENV_PY}" ]] || { echo "$TS [COMMENT_PIPE] ERROR python not found: ${VENV_PY}"; exit 1; }
[[ -f "${COMMENTS_PY}" ]] || { echo "$TS [COMMENT_PIPE] ERROR missing file: ${COMMENTS_PY}"; exit 1; }
echo "$TS [COMMENT_PIPE] BIN: VENV_PY=$VENV_PY"

# 增量拉取 + 隐藏负向评论（入库到 PG）
"$VENV_PY" "$COMMENTS_PY" run --once

TS2="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS2 [COMMENT_PIPE] DONE"
