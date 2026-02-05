#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OE/Qianchuan Healthcheck (read-only)"""
import os, sys
from datetime import timedelta
import psycopg2

def env_int(k, d):
    try: return int(os.getenv(k, d))
    except: return d

def fmt_td(td):
    if td is None: return "N/A"
    s = int(td.total_seconds())
    if s < 60: return f"{s}s"
    if s < 3600: return f"{s//60}m{s%60:02d}s"
    return f"{s//3600}h{(s%3600)//60:02d}m"

def level(age, warn, crit):
    if age >= crit: return "CRIT"
    if age >= warn: return "WARN"
    return "OK"

def conn():
    dsn = os.getenv("PG_DSN")
    if dsn: return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT","5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        sslmode=os.getenv("PGSSLMODE","require"),
    )

def one(cur, sql):
    cur.execute(sql)
    return cur.fetchone()

def table_exists(cur, schema, table):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",(schema,table))
    return cur.fetchone() is not None

def main():
    BAL_WARN=env_int("HC_BAL_WARN_SEC",2700)
    BAL_CRIT=env_int("HC_BAL_CRIT_SEC",7200)
    CMT_WARN=env_int("HC_CMT_WARN_SEC",1200)
    CMT_CRIT=env_int("HC_CMT_CRIT_SEC",3600)
    UNNOT_WARN=env_int("HC_UNNOT_WARN",1)
    UNNOT_CRIT=env_int("HC_UNNOT_CRIT",50)
    rc=0
    c=conn()
    try:
        with c.cursor() as cur:
            print("==== OE Healthcheck ====")
            now,max_ts,age=one(cur,"SELECT now(),max(snapshot_ts),now()-max(snapshot_ts) FROM oe.fact_balance_snapshot")
            sec=int(age.total_seconds()) if age else 10**9
            lv=level(sec,BAL_WARN,BAL_CRIT)
            print(f"[{lv}] balance_snapshot latest={max_ts} lag={fmt_td(age)}")
            rc=max(rc,2 if lv=="CRIT" else 1 if lv=="WARN" else 0)
            now,max_ts,age=one(cur,"SELECT now(),max(last_seen_at),now()-max(last_seen_at) FROM oe.fact_comment")
            sec=int(age.total_seconds()) if age else 10**9
            lv=level(sec,CMT_WARN,CMT_CRIT)
            print(f"[{lv}] fact_comment     latest={max_ts} lag={fmt_td(age)}")
            rc=max(rc,2 if lv=="CRIT" else 1 if lv=="WARN" else 0)
            (cnt,)=one(cur,"SELECT count(*) FROM oe.fact_comment_action WHERE action='hide' AND status='success' AND notified_at IS NULL")
            lv="CRIT" if cnt>=UNNOT_CRIT else "WARN" if cnt>=UNNOT_WARN else "OK"
            print(f"[{lv}] comment_notify  unnotified_hide_success={cnt}")
            rc=max(rc,2 if lv=="CRIT" else 1 if lv=="WARN" else 0)
            if table_exists(cur,"oe","ops_job_run"):
                print("---- runlog ----")
                cur.execute("SELECT job_name,count(*) FROM oe.ops_job_run WHERE started_at>=now()-interval '24 hours' GROUP BY job_name")
                for j,cnt in cur.fetchall():
                    print(f"[INFO] runlog:{j} runs_24h={cnt}")
            else:
                print("[WARN] runlog not enabled (cannot prove missed runs)")
    finally:
        c.close()
    sys.exit(rc)

if __name__=="__main__":
    main()
