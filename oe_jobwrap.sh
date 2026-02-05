#!/usr/bin/env bash
set -euo pipefail

# Wrap a cron job with DB run logging.
# - Works whether you use PG_DSN or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
# - Avoids "unbound variable" by using safe parameter expansion
# - Optionally auto-sources oe_env.sh if present (recommended for cron)

JOB_NAME="${1:-}"
shift || true

if [[ -z "${JOB_NAME}" || $# -lt 1 ]]; then
  echo "Usage: bash oe_jobwrap.sh <job_name> <command...>" >&2
  exit 2
fi

# Auto-load env for cron/subshell execution (best-effort)
if [[ -f "./oe_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "./oe_env.sh"
elif [[ -f "/root/oe_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "/root/oe_env.sh"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"

psql_cmd() {
  if [[ -n "${PG_DSN:-}" ]]; then
    psql "${PG_DSN}" -v ON_ERROR_STOP=1 -qAt "$@"
  else
    # Require the classic env vars if PG_DSN is not provided
    : "${PGHOST:?missing PGHOST}"
    : "${PGDATABASE:?missing PGDATABASE}"
    : "${PGUSER:?missing PGUSER}"
    : "${PGPASSWORD:?missing PGPASSWORD}"
    psql "host=${PGHOST} port=${PGPORT:-5432} dbname=${PGDATABASE} user=${PGUSER} password=${PGPASSWORD} sslmode=${PGSSLMODE:-require}"       -v ON_ERROR_STOP=1 -qAt "$@"
  fi
}

# start
psql_cmd -c "INSERT INTO oe.ops_job_run(job_name, run_id, started_at, status)
             VALUES ('${JOB_NAME}', '${RUN_ID}', now(), 'running')
             ON CONFLICT (job_name, run_id) DO NOTHING;" >/dev/null || true

set +e
"$@"
EC=$?
set -e

STATUS="success"
MSG=""
if [[ "${EC}" -ne 0 ]]; then
  STATUS="fail"
  MSG="exit_code=${EC}"
fi

psql_cmd -c "UPDATE oe.ops_job_run
             SET finished_at=now(), status='${STATUS}', exit_code=${EC}, message='${MSG}'
             WHERE job_name='${JOB_NAME}' AND run_id='${RUN_ID}';" >/dev/null || true

exit "${EC}"
