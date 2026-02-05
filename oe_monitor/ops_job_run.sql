CREATE SCHEMA IF NOT EXISTS oe;
CREATE TABLE IF NOT EXISTS oe.ops_job_run (
 id bigserial PRIMARY KEY,
 job_name text NOT NULL,
 run_id text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz,
 status text NOT NULL DEFAULT 'running',
 exit_code int,
 message text
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ops_job_run ON oe.ops_job_run(job_name,run_id);
