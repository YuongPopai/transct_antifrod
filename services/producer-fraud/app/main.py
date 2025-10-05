import os, sys, json, time, random
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from dateutil import tz
import psycopg2
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tx.all")
SLEEP_MIN = float(os.getenv("SLEEP_MIN", "0.8"))
SLEEP_MAX = float(os.getenv("SLEEP_MAX", "2.5"))
BATCH_FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "100"))
REFRESH_ACCOUNTS_SEC = int(os.getenv("REFRESH_ACCOUNTS_SEC", "300"))
LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "Europe/Amsterdam"))

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "antifraud")
PG_USER = os.getenv("POSTGRES_USER", "antifraud")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "antifraud")

NIGHT_CHANCE = float(os.getenv("NIGHT_CHANCE", "0.45"))
ROUND_DOLLAR_SHARE = float(os.getenv("ROUND_DOLLAR_SHARE", "0.35"))

ENABLE = {
    "structuring_burst": os.getenv("FRAUD_STRUCTURING", "true").lower() not in ("0","false","no"),
    "velocity_many_recipients": os.getenv("FRAUD_VELOCITY", "true").lower() not in ("0","false","no"),
    "ping_pong": os.getenv("FRAUD_PINGPONG", "true").lower() not in ("0","false","no"),
    "cashout_watchlist": os.getenv("FRAUD_CASHOUT_WATCH", "true").lower() not in ("0","false","no"),
    "cross_currency": os.getenv("FRAUD_XCUR", "true").lower() not in ("0","false","no"),
}

def now_utc():
    return datetime.now(timezone.utc)

def as_local(dt: datetime):
    return dt.astimezone(LOCAL_TZ)

def at_night(dt: datetime) -> bool:
    loc = as_local(dt)
    return loc.hour < 6 or loc.hour >= 23

def rnd_amount(mid=950.0, spread=300.0, round_bias=ROUND_DOLLAR_SHARE):
    val = random.gauss(mid, spread)
    val = max(50.0, min(5000.0, val))
    if random.random() < round_bias:
        return float(int(val // 10) * 10)
    cents = random.choice([0.00, 0.09, 0.19, 0.29, 0.49, 0.79, 0.99])
    return round(float(int(val)) + cents, 2)

def connect_pg():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    return psycopg2.connect(dsn)

def load_accounts(cur):
    cur.execute("""
        SELECT a.account_id, COALESCE(a.is_watchlisted, false) AS watch, a.currency
        FROM accounts a
        WHERE a.status='active'
        ORDER BY a.account_id
    """)
    rows = cur.fetchall()
    accounts = [r[0] for r in rows]
    by_id = {r[0]: {"watch": r[1], "currency": r[2]} for r in rows}
    watch_ids = [r[0] for r in rows if r[1]]
    return accounts, by_id, watch_ids

def choose_time_for_fraud():
    base = now_utc()
    if random.random() < NIGHT_CHANCE:
        loc = as_local(base).replace(hour=random.choice([0,1,2,3,4,5,23]), minute=random.randint(0,59), second=random.randint(0,59), microsecond=0)
        return loc.astimezone(timezone.utc)
    return base

def random_channel():
    r = random.random()
    if r < 0.40: return "card"
    if r < 0.70: return "online"
    if r < 0.90: return "mobile"
    if r < 0.97: return "atm"
    return "branch"

def emit(producer, evt):
    producer.send(TOPIC, evt); producer.flush()

def currencies_pool(cur):
    raw = os.getenv("SEED_CURRENCIES", "RUB,USD,EUR").split(",")
    cur.execute("SELECT DISTINCT currency FROM accounts WHERE status='active'")
    db_curs = [r[0] for r in cur.fetchall()]
    for c in db_curs:
        if c not in raw:
            raw.append(c)
    return raw

def sc_structuring_burst(rng, producer, accs, meta, cur_list):
    dst = rng.choice(accs)
    srcs = rng.sample(accs, k=min(5, max(3, len(accs)//200)))
    base_ts = choose_time_for_fraud()
    curcy = rng.choice(cur_list)
    for _ in range(rng.randint(8, 16)):
        src = rng.choice(srcs)
        if src == dst: continue
        amt = rnd_amount(mid=980.0, spread=120.0)
        ts = base_ts + timedelta(seconds=rng.randint(0, 300))
        evt = {"type":"transfer","label":"fraud","tags":["structuring","velocity"],
               "src_account_id":src,"dst_account_id":dst,"amount":amt,"currency":curcy,
               "initiated_at":ts.isoformat(),"channel":random_channel(),"meta":meta}
        emit(producer, evt)

def sc_velocity_many_recipients(rng, producer, accs, meta, cur_list):
    if len(accs) < 20: return
    src = rng.choice(accs)
    dests = rng.sample([a for a in accs if a != src], k=rng.randint(8, 14))
    base_ts = choose_time_for_fraud()
    curcy = rng.choice(cur_list)
    for i, dst in enumerate(dests):
        evt = {"type":"transfer","label":"fraud","tags":["velocity","new-recipients"],
               "src_account_id":src,"dst_account_id":dst,
               "amount":rnd_amount(mid=420.0, spread=180.0),"currency":curcy,
               "initiated_at":(base_ts + timedelta(seconds=i * rng.randint(5,25))).isoformat(),
               "channel":random_channel(),"meta":meta}
        emit(producer, evt)

def sc_ping_pong(rng, producer, accs, meta, cur_list):
    if len(accs) < 2: return
    a, b = rng.sample(accs, k=2)
    base_ts = choose_time_for_fraud()
    curcy = rng.choice(cur_list)
    amt = rnd_amount(mid=700.0, spread=80.0)
    steps = rng.randint(4, 8)
    ts = base_ts
    for i in range(steps):
        src, dst = (a, b) if i % 2 == 0 else (b, a)
        evt = {"type":"transfer","label":"fraud","tags":["ping-pong"],
               "src_account_id":src,"dst_account_id":dst,"amount":amt if i<steps-1 else rnd_amount(mid=amt, spread=30.0),
               "currency":curcy,"initiated_at":ts.isoformat(),"channel":random_channel(),"meta":meta}
        emit(producer, evt); ts += timedelta(seconds=rng.randint(20,90))

def sc_cashout_watchlist(rng, producer, accs, meta, cur_list, acc_meta):
    watch = [aid for aid in accs if acc_meta[aid]["watch"]]
    if not watch or len(accs) < 5: return
    dst = rng.choice(watch)
    srcs = rng.sample([a for a in accs if a != dst], k=rng.randint(3, 6))
    base_ts = choose_time_for_fraud()
    curcy = acc_meta[dst]["currency"]
    for i, src in enumerate(srcs):
        evt = {"type":"transfer","label":"fraud","tags":["cashout","watchlist"],
               "src_account_id":src,"dst_account_id":dst,
               "amount":rnd_amount(mid=1100.0, spread=250.0, round_bias=0.6),
               "currency":curcy,"initiated_at":(base_ts + timedelta(seconds=i * rng.randint(15,60))).isoformat(),
               "channel":"card","meta":meta}
        emit(producer, evt)

def sc_cross_currency(rng, producer, accs, meta, cur_list, acc_meta):
    if len(accs) < 2: return
    src, dst = rng.sample(accs, k=2)
    candidates = [c for c in cur_list if c not in (acc_meta[src]["currency"], acc_meta[dst]["currency"])]
    if not candidates: candidates = cur_list
    curcy = rng.choice(candidates)
    ts = choose_time_for_fraud()
    evt = {"type":"transfer","label":"fraud","tags":["currency-anomaly"],
           "src_account_id":src,"dst_account_id":dst,"amount":rnd_amount(mid=880.0, spread=220.0, round_bias=0.5),
           "currency":curcy,"initiated_at":ts.isoformat(),"channel":random_channel(),"meta":meta}
    emit(producer, evt)

def main():
    print(f"[producer-fraud] starting; kafka={KAFKA_BROKERS} topic={TOPIC}", flush=True)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=BATCH_FLUSH_MS, acks="all", max_in_flight_requests_per_connection=1, retries=5,
    )
    conn = connect_pg(); conn.autocommit = True
    cur = conn.cursor()
    accounts, acc_meta, watch_ids = load_accounts(cur)
    if len(accounts) < 2:
        print("[producer-fraud] need at least 2 active accounts in DB", file=sys.stderr); sys.exit(1)
    cur_list = currencies_pool(cur)
    print(f"[producer-fraud] accounts={len(accounts)}, watchlisted={len(watch_ids)}, currencies={cur_list}")
    rng = random.Random()
    next_refresh = now_utc() + timedelta(seconds=REFRESH_ACCOUNTS_SEC)
    scenarios = [
        ("structuring_burst", sc_structuring_burst),
        ("velocity_many_recipients", sc_velocity_many_recipients),
        ("ping_pong", sc_ping_pong),
        ("cashout_watchlist", sc_cashout_watchlist),
        ("cross_currency", sc_cross_currency),
    ]
    weights = {"structuring_burst":0.30,"velocity_many_recipients":0.25,"ping_pong":0.15,"cashout_watchlist":0.20,"cross_currency":0.10}
    sent = 0
    while True:
        if now_utc() >= next_refresh:
            try:
                accounts, acc_meta, watch_ids = load_accounts(cur)
                cur_list = currencies_pool(cur)
                print(f"[producer-fraud] refreshed: accounts={len(accounts)}, watchlisted={len(watch_ids)}")
            except Exception as e:
                print(f"[producer-fraud] refresh failed: {e}", file=sys.stderr)
            next_refresh = now_utc() + timedelta(seconds=REFRESH_ACCOUNTS_SEC)

        pool = [(n,f,weights[n]) for n,f in scenarios if ENABLE.get(n, True)]
        total_w = sum(w for _,_,w in pool) or 1.0
        x = rng.random() * total_w; upto = 0.0; chosen = pool[0]
        for name, fn, w in pool:
            if upto + w >= x: chosen = (name, fn, w); break
            upto += w

        meta = {"generator":"producer-fraud","scenario":chosen[0],"v":1}
        try:
            if chosen[0] == "structuring_burst":
                chosen[1](rng, producer, accounts, meta, cur_list)
            elif chosen[0] == "velocity_many_recipients":
                chosen[1](rng, producer, accounts, meta, cur_list)
            elif chosen[0] == "ping_pong":
                chosen[1](rng, producer, accounts, meta, cur_list)
            elif chosen[0] == "cashout_watchlist":
                chosen[1](rng, producer, accounts, meta, cur_list, acc_meta)
            elif chosen[0] == "cross_currency":
                chosen[1](rng, producer, accounts, meta, cur_list, acc_meta)
            sent += 1
            if sent % 3 == 0:
                print(f"[producer-fraud] batches_sent={sent}", flush=True)
        except Exception as e:
            print(f"[producer-fraud] scenario {chosen[0]} failed: {e}", file=sys.stderr)

        time.sleep(rng.uniform(SLEEP_MIN, SLEEP_MAX))

if __name__ == "__main__":
    main()
