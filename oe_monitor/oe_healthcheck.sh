#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$DIR/oe_env.sh" ]] && source "$DIR/oe_env.sh"
PY="${PYTHON_BIN:-/root/venv_oe/bin/python}"
"$PY" "$DIR/oe_healthcheck.py"
