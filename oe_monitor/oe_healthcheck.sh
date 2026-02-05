#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
APP_ROOT="${APP_ROOT:-$(cd "${DIR}/.." && pwd -P)}"
ENV_FILE="${OE_ENV_FILE:-/etc/tf_doudian/oe_env.sh}"
if [[ ! -f "${ENV_FILE}" && -f "${APP_ROOT}/oe_env.sh" ]]; then
  ENV_FILE="${APP_ROOT}/oe_env.sh"
fi
[[ -f "${ENV_FILE}" ]] && source "${ENV_FILE}"
PY="${PYTHON_BIN:-${APP_ROOT}/venv_oe/bin/python}"
"$PY" "$DIR/oe_healthcheck.py"
