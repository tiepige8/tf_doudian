#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
APP_ROOT="${APP_ROOT:-${SCRIPT_DIR}}"
ENV_FILE="${OE_ENV_FILE:-/etc/tf_doudian/oe_env.sh}"
if [[ ! -f "${ENV_FILE}" && -f "${APP_ROOT}/oe_env.sh" ]]; then
  ENV_FILE="${APP_ROOT}/oe_env.sh"
fi

TS="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS [COMMENT_NOTIFY] START"

[[ -f "${ENV_FILE}" ]] || { echo "$TS [COMMENT_NOTIFY] ERROR env file not found: ${ENV_FILE}"; exit 1; }
# shellcheck disable=SC1090
source "${ENV_FILE}"

if [[ -z "${OE_TOKEN_CACHE:-}" && -n "${OE_TOKEN_FILE:-}" ]]; then
  export OE_TOKEN_CACHE="${OE_TOKEN_FILE}"
fi

echo "$TS [COMMENT_NOTIFY] ENV: FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK_URL:+***}"

VENV_PY="${PYTHON_BIN:-${APP_ROOT}/venv_oe/bin/python}"
COMMENTS_PY="${APP_ROOT}/oe_qianchuan_comments.py"
[[ -x "${VENV_PY}" ]] || { echo "$TS [COMMENT_NOTIFY] ERROR python not found: ${VENV_PY}"; exit 1; }
[[ -f "${COMMENTS_PY}" ]] || { echo "$TS [COMMENT_NOTIFY] ERROR missing file: ${COMMENTS_PY}"; exit 1; }
"$VENV_PY" "$COMMENTS_PY" notify --window-hours 24

TS2="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS2 [COMMENT_NOTIFY] DONE"
