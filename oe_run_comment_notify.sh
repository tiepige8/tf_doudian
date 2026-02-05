#!/usr/bin/env bash
set -euo pipefail

TS="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS [COMMENT_NOTIFY] START"

source /root/oe_env.sh
echo "$TS [COMMENT_NOTIFY] ENV: FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK_URL:+***}"

VENV_PY="/root/venv_oe/bin/python"
$VENV_PY /root/oe_qianchuan_comments.py notify --window-hours 24

TS2="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS2 [COMMENT_NOTIFY] DONE"
