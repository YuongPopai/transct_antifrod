import os, sys, json, time, random
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from dateutil import tz
import psycopg2
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tx.all")  # публикуем в общий поток
BATCH_FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "200"))
SLEEP_MIN = float(os.getenv("SLEEP_MIN", "0.35"))
SLEEP_MAX = float(os.getenv("SLEEP_MAX", "1.2"))
REFRESH_ACCOUNTS_SEC = int(os.getenv("REFRESH_ACCOUNTS_SEC", "300"))

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "antifraud")
PG_USER = os.getenv("POSTGRES_USER", "antifraud")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "antifraud")

LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "Europe/Moscow"))

def now_utc():
    return datetime.now(timezone.utc)

def local_hour_ok(dt: datetime) -> bool:
    loc = dt.astimezone(LOCAL_TZ)
    h, m = loc.hour, loc.minute
    return (h > 6) and (h < 22 or (h == 22 and m <= 30))

def random_amount():
    base = random.lognormvariate(4.0, 0.6)
    amt = min(max(base, 15.0), 800.0)     
    cents = random.choice([0.13,0.27,0.39,0.42,0.58,0.61,0.73,0.87,0.94])
    val = round(float(int(amt)) + cents, 2)
    return val

def random_channel():
    r = random.random()
    if r < 0.55:
        return "online"
    if r < 0.90:
        return "mobile"
    if r < 0.96:
        return "card"
    if r < 0.99:
        return "branch"
    return "atm"

def connect_pg():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    return psycopg2.connect(dsn)

def load_accounts(cur):
    cur.execute("""
        SELECT a.account_id
        FROM accounts a
        WHERE a.status = 'active'
        ORDER BY a.account_id
    """)
    rows = [r[0] for r in cur.fetchall()]
    return rows

def main():
    print(f"[producer-valid] starting; kafka={KAFKA_BROKERS} topic={TOPIC}", flush=True)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=BATCH_FLUSH_MS,
        acks="all",
        max_in_flight_requests_per_connection=1,
        retries=5,
    )

    last_sent_time = {}                   
    recent_dests = defaultdict(lambda: deque(maxlen=5))  

    conn = connect_pg(); conn.autocommit = True
    cur = conn.cursor()
    accounts = load_accounts(cur)
    if len(accounts) < 2:
        print("[producer-valid] need at least 2 active accounts in DB", file=sys.stderr)
        sys.exit(1)
    print(f"[producer-valid] {len(accounts)} active accounts loaded")

    next_refresh = now_utc() + timedelta(seconds=REFRESH_ACCOUNTS_SEC)
    rng = random.Random()

    while True:
        t0 = now_utc()
        if t0 >= next_refresh:
            try:
                accounts = load_accounts(cur)
                print(f"[producer-valid] refreshed accounts: {len(accounts)}")
            except Exception as e:
                print(f"[producer-valid] refresh failed: {e}", file=sys.stderr)
            next_refresh = now_utc() + timedelta(seconds=REFRESH_ACCOUNTS_SEC)

        for _ in range(50):
            src = rng.choice(accounts)
            last = last_sent_time.get(src)
            if (last is None) or ((t0 - last).total_seconds() >= 2.0):
                break
        else:
            time.sleep(0.2)
            continue

        for _ in range(50):
            dst = rng.choice(accounts)
            if dst != src and dst not in recent_dests[src]:
                break
        if dst == src:
            continue

        ts = now_utc()
        amt = min(random_amount(), 200.0) if not local_hour_ok(ts) else random_amount()

        evt = {
            "type": "transfer",
            "label": "legit",
            "src_account_id": src,
            "dst_account_id": dst,
            "amount": amt,
            "currency": rng.choice(os.getenv("SEED_CURRENCIES","RUB,USD,EUR").split(",")),
            "initiated_at": ts.isoformat(),
            "channel": random_channel(),
            "meta": {"generator":"producer-valid","v":1}
        }

        try:
            producer.send(TOPIC, evt)
            producer.flush()
            last_sent_time[src] = ts
            recent_dests[src].append(dst)
        except Exception as e:
            print(f"[producer-valid] send failed: {e}", file=sys.stderr)

        time.sleep(rng.uniform(SLEEP_MIN, SLEEP_MAX))

if __name__ == "__main__":
    main()