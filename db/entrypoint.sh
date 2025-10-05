#!/usr/bin/env sh
set -eu

: "${POSTGRES_HOST:?need POSTGRES_HOST}"
: "${POSTGRES_DB:?need POSTGRES_DB}"
: "${POSTGRES_USER:?need POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?need POSTGRES_PASSWORD}"
: "${POSTGRES_PORT:=5432}"

echo "[step1] waiting for postgres at ${POSTGRES_HOST}:${POSTGRES_PORT}вЂ¦"
python - <<'PY'
import os, time, sys
import psycopg2
dsn = (
    f"host={os.environ['POSTGRES_HOST']} "
    f"port={os.environ.get('POSTGRES_PORT','5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)
for i in range(120):
    try:
        conn = psycopg2.connect(dsn)
        conn.close()
        print("[step1] postgres is up")
        break
    except Exception as e:
        print(f"[step1] waitingвЂ¦ ({i+1}) {e}")
        time.sleep(2)
else:
    print("[step1] postgres didn't become ready in time", file=sys.stderr)
    sys.exit(1)
PY

echo "[step1] seeding database $POSTGRES_DB"
python -u seed/seed.py
