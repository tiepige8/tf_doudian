#!/usr/bin/env bash
set -euo pipefail

TS="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS [COMMENT_PIPE] START"

# 一定要 source 环境变量（PG / OE / FEISHU）
source /root/oe_env.sh

# 简要打印关键环境（避免泄露密码）
echo "$TS [COMMENT_PIPE] ENV: PGHOST=${PGHOST:-} PGPORT=${PGPORT:-} PGDATABASE=${PGDATABASE:-} PGUSER=${PGUSER:-} PGSSLMODE=${PGSSLMODE:-} FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK_URL:+***}"

VENV_PY="/root/venv_oe/bin/python"
echo "$TS [COMMENT_PIPE] BIN: VENV_PY=$VENV_PY"

# 增量拉取 + 隐藏负向评论（入库到 PG）
$VENV_PY /root/oe_qianchuan_comments.py run --once

TS2="$(date '+%Y-%m-%d %H:%M:%S')"
echo "$TS2 [COMMENT_PIPE] DONE"
