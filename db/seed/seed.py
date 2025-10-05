import os
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta
from random import randint, choice, random
from dateutil.relativedelta import relativedelta


try:
    from config import (
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS,
        USERS_TARGET, MIN_AGR, MAX_AGR,
        TRANSACTIONS_PER_ACCOUNT_MIN as TXN_MIN_DEFAULT,
        TRANSACTIONS_PER_ACCOUNT_MAX as TXN_MAX_DEFAULT,
        BATCH_SIZE as BATCH_SIZE_DEFAULT, CURRENCIES as CURRENCIES_DEFAULT
    )
except Exception:
    PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
    PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    PG_DB   = os.getenv("POSTGRES_DB", "antifraud")
    PG_USER = os.getenv("POSTGRES_USER", "antifraud")
    PG_PASS = os.getenv("POSTGRES_PASSWORD", "antifraud")
    USERS_TARGET = int(os.getenv("SEED_USERS", "10000"))
    MIN_AGR = int(os.getenv("SEED_MIN_AGR", "1"))
    MAX_AGR = int(os.getenv("SEED_MAX_AGR", "3"))
    TXN_MIN_DEFAULT = int(os.getenv("SEED_TXN_PER_ACC_MIN", "5"))
    TXN_MAX_DEFAULT = int(os.getenv("SEED_TXN_PER_ACC_MAX", "15"))
    BATCH_SIZE_DEFAULT = int(os.getenv("SEED_BATCH", "5000"))
    CURRENCIES_DEFAULT = os.getenv("SEED_CURRENCIES", "RUB,USD,EUR").split(",")

ONLY_IF_EMPTY = os.getenv("SEED_IF_EMPTY_ONLY", "true").lower() not in ("0","false","no")
SEED_RESET = os.getenv("SEED_RESET", "false").lower() in ("1","true","yes")
SEED_FORCE = os.getenv("SEED_FORCE", "false").lower() in ("1","true","yes")
TXN_MIN = int(os.getenv("SEED_TXN_PER_ACC_MIN", str(TXN_MIN_DEFAULT)))
TXN_MAX = int(os.getenv("SEED_TXN_PER_ACC_MAX", str(TXN_MAX_DEFAULT)))
BATCH_SIZE = int(os.getenv("SEED_BATCH", str(BATCH_SIZE_DEFAULT)))
CURRENCIES = os.getenv("SEED_CURRENCIES", ",".join(CURRENCIES_DEFAULT)).split(",")

def db_has_data(cur) -> bool:
    cur.execute("SELECT EXISTS (SELECT 1 FROM customers LIMIT 1)")
    return bool(cur.fetchone()[0])

def reset_database(cur):
    cur.execute("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='transactions') THEN
            EXECUTE 'TRUNCATE TABLE transactions RESTART IDENTITY CASCADE';
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='account_balances') THEN
            EXECUTE 'TRUNCATE TABLE account_balances RESTART IDENTITY CASCADE';
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='accounts') THEN
            EXECUTE 'TRUNCATE TABLE accounts RESTART IDENTITY CASCADE';
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='agreements') THEN
            EXECUTE 'TRUNCATE TABLE agreements RESTART IDENTITY CASCADE';
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='customers') THEN
            EXECUTE 'TRUNCATE TABLE customers RESTART IDENTITY CASCADE';
          END IF;
        END$$;
    """)

def _clampf(x, lo, hi):
    try:
        v = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, v))

SUSPICIOUS_USER_SHARE = _clampf(os.getenv("SEED_SUSPICIOUS_USER_SHARE", "0.10"), 0.0, 1.0)
EMPTY_USER_SHARE      = _clampf(os.getenv("SEED_EMPTY_USER_SHARE", "0.15"), 0.0, 1.0)
FRAUD_TXN_SHARE       = _clampf(os.getenv("SEED_FRAUD_TXN_SHARE", "0.03"), 0.0, 1.0)

APPLY_BALANCES        = os.getenv("SEED_RECOMPUTE_BALANCES", "true").lower() not in ("0","false","no")

fake = Faker('ru_RU')
DSN = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

def connect():
    return psycopg2.connect(DSN)


def ensure_schema(cur):
    sql_files = ["sql/extensions.sql", "sql/schema.sql", "sql/indexes.sql"]
    optional = ["sql/fraud_basics.sql"]  
    for path in sql_files + optional:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                cur.execute(f.read())


def detect_columns(cur):
    """Detect whether optional columns exist (segment / is_watchlisted / label,tags)."""
    def _has_col(table, col):
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
            LIMIT 1
        """,
            (table, col),
        )
        return cur.fetchone() is not None

    has_customer_segment = _has_col("customers", "segment")
    has_account_watch = _has_col("accounts", "is_watchlisted")
    has_txn_label = _has_col("transactions", "label")
    has_txn_tags = _has_col("transactions", "tags")
    return {
        "customer_segment": has_customer_segment,
        "account_watch": has_account_watch,
        "txn_label": has_txn_label,
        "txn_tags": has_txn_tags,
    }


def random_amount():
    base = randint(1, 10_000)
    if random() < 0.02:
        base *= randint(5, 30)
    return round(base * 1.0, 2)


def random_dates(now, months_back=12):
    start = now - relativedelta(months=months_back)
    delta = now - start
    initiated = start + timedelta(seconds=randint(0, int(delta.total_seconds())))
    settled = initiated + timedelta(minutes=randint(1, 120))
    return initiated, settled


def rand_channel():
    return choice(["online", "mobile", "atm", "branch", "card"])


def seed_customers(cur, n_customers, has_segment: bool):
    rows = []
    for _ in range(n_customers):
        segment = ("suspicious" if random() < SUSPICIOUS_USER_SHARE else "normal") if has_segment else None
        rows.append(
            (
                fake.first_name(),
                fake.last_name(),
                fake.date_of_birth(minimum_age=18, maximum_age=85),
                fake.unique.email(),
                fake.phone_number(),
                fake.street_address(),
                None,
                fake.city(),
                "RU",
                round(random() * 5, 2),
                segment,
            )
        )

    if has_segment:
        ids = execute_values(
            cur,
            """
            INSERT INTO customers(
              first_name,last_name,date_of_birth,email,phone,
              address_line1,address_line2,city,country,risk_score,segment
            ) VALUES %s
            RETURNING customer_id
        """,
            rows,
            fetch=True,
        )
    else:
        ids = execute_values(
            cur,
            """
            INSERT INTO customers(
              first_name,last_name,date_of_birth,email,phone,
              address_line1,address_line2,city,country,risk_score
            ) VALUES %s
            RETURNING customer_id
        """,
            [r[:-1] for r in rows],
            fetch=True,
        )

    return [i[0] for i in ids]


def seed_agreements_and_accounts(cur, customer_ids, has_watchlist: bool, has_segment: bool):
    seg = {}
    if has_segment:
        cur.execute("SELECT customer_id, segment FROM customers WHERE customer_id = ANY(%s)", (customer_ids,))
        seg = {cid: s for cid, s in cur.fetchall()}

    agreements_rows = []
    for cid in customer_ids:
        n_agr = randint(MIN_AGR, MAX_AGR)
        for _ in range(n_agr):
            agr_type = choice(["current", "savings", "deposit"])
            agreements_rows.append((cid, agr_type))

    agr_ids = execute_values(
        cur,
        """
        INSERT INTO agreements(customer_id, type)
        VALUES %s
        RETURNING agreement_id
    """,
        agreements_rows,
        fetch=True,
    )
    agr_ids = [x[0] for x in agr_ids]

    cur.execute("SELECT agreement_id, customer_id FROM agreements WHERE agreement_id = ANY(%s)", (agr_ids,))
    agr_to_cust = dict(cur.fetchall())

    accounts_rows = []
    for agr_id in agr_ids:
        cid = agr_to_cust[agr_id]
        currency = choice(CURRENCIES)
        watch = False
        if has_watchlist and has_segment and seg.get(cid) == "suspicious" and random() < 0.3:
            watch = True
        accounts_rows.append((agr_id, None, currency, watch))

    if has_watchlist:
        acc_ids = execute_values(
            cur,
            """
            INSERT INTO accounts(agreement_id, iban, currency, is_watchlisted)
            VALUES %s
            RETURNING account_id
        """,
            accounts_rows,
            fetch=True,
        )
    else:
        acc_ids = execute_values(
            cur,
            """
            INSERT INTO accounts(agreement_id, iban, currency)
            VALUES %s
            RETURNING account_id
        """,
            [r[:-1] for r in accounts_rows],
            fetch=True,
        )

    return [x[0] for x in acc_ids]


def seed_transactions(cur, account_ids, caps, has_labels: bool, has_tags: bool, owners_info):
    """
    caps: (TXN_MIN, TXN_MAX)
    owners_info: dict customer_id -> {"segment": str, "accounts": [ids]}
    """
    TXN_MIN, TXN_MAX = caps
    if TXN_MAX <= 0:
        return  

    now = datetime.utcnow()
    tx_rows = []
    all_accs = account_ids

    owners = list(owners_info.keys())
    empty = {cid for cid in owners if random() < EMPTY_USER_SHARE}

    def emit(src, dst, amt, curcy, init, settl, label, tags_json):
        status = "settled"   
        desc = None         
        if has_labels and has_tags:
            tx_rows.append((src, dst, amt, curcy, init, settl, rand_channel(), status, desc, label, tags_json))
        elif has_labels and not has_tags:
            tx_rows.append((src, dst, amt, curcy, init, settl, rand_channel(), status, desc, label))
        else:
            tx_rows.append((src, dst, amt, curcy, init, settl, rand_channel(), status, desc))

    def flush():
        nonlocal tx_rows
        if not tx_rows:
            return
        if has_labels and has_tags:
            execute_values(
                cur,
                """
                INSERT INTO transactions(
                    src_account_id, dst_account_id, amount, currency,
                    initiated_at, settled_at, channel, status, description, label, tags
                ) VALUES %s
            """,
                tx_rows,
            )
        elif has_labels and not has_tags:
            execute_values(
                cur,
                """
                INSERT INTO transactions(
                    src_account_id, dst_account_id, amount, currency,
                    initiated_at, settled_at, channel, status, description, label
                ) VALUES %s
            """,
                tx_rows,
            )
        else:
            execute_values(
                cur,
                """
                INSERT INTO transactions(
                    src_account_id, dst_account_id, amount, currency,
                    initiated_at, settled_at, channel, status, description
                ) VALUES %s
            """,
                tx_rows,
            )
        tx_rows = []

    for cust, data in owners_info.items():
        accs = data["accounts"]
        if not accs:
            continue
        if cust in empty:
            continue

        n_legit = randint(TXN_MIN, TXN_MAX)
        for _ in range(n_legit):
            src = choice(accs)
            dst = choice(all_accs)
            if dst == src:
                continue
            amt = random_amount()
            init, settl = random_dates(now, months_back=12)
            emit(src, dst, amt, choice(CURRENCIES), init, settl, "legit", "[]")

            if len(tx_rows) >= BATCH_SIZE:
                flush()

        suspicious = data.get("segment") == "suspicious"
        if (suspicious or random() < FRAUD_TXN_SHARE) and has_labels:
            dst = choice(all_accs)
            base_time, _ = random_dates(now, months_back=3)
            for _ in range(randint(8, 15)):
                src = choice(accs)
                amt = float(randint(9000, 9900)) / 10.0  
                init = base_time
                settl = base_time
                tags = '["structuring","velocity"]' if has_tags else None
                emit(src, dst, amt, choice(CURRENCIES), init, settl, "fraud", tags)

            if len(tx_rows) >= BATCH_SIZE:
                flush()

    flush()


def recompute_balances(cur):
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_name='account_balances' LIMIT 1
    """
    )
    if cur.fetchone() is None:
        return
    cur.execute("DROP TABLE IF EXISTS _tmp_moves;")
    cur.execute(
        """
        CREATE TEMP TABLE _tmp_moves AS
        SELECT
          a.account_id,
          date(t.initiated_at) AS d,
          SUM(CASE WHEN t.dst_account_id = a.account_id THEN t.amount ELSE 0 END) AS incoming,
          SUM(CASE WHEN t.src_account_id = a.account_id THEN t.amount ELSE 0 END) AS outgoing
        FROM accounts a
        LEFT JOIN transactions t ON t.src_account_id = a.account_id OR t.dst_account_id = a.account_id
        GROUP BY 1,2;
    """
    )

    cur.execute("DELETE FROM account_balances;")
    cur.execute(
        """
        INSERT INTO account_balances(account_id, as_of_date, balance)
        SELECT account_id, d,
               COALESCE(SUM(incoming - outgoing) OVER (PARTITION BY account_id ORDER BY d
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS balance
        FROM _tmp_moves
        ORDER BY account_id, d;
    """
    )


def main():
    with connect() as conn:
        conn.autocommit = True
        cur = conn.cursor()

        print("[step1] applying schema…", flush=True)
        ensure_schema(cur)
        print("[step1] checking database state…", flush=True)
        if SEED_RESET:
            print("[step1] SEED_RESET=true → truncating tables…", flush=True)
            reset_database(cur)
        elif ONLY_IF_EMPTY and db_has_data(cur):
            print("[step1] database already has data → skipping seeding (SEED_IF_EMPTY_ONLY=true).", flush=True)
            if APPLY_BALANCES:
                print("[step1] computing balances…", flush=True)
                recompute_balances(cur)
            print("[step1] done.", flush=True)
            return
        elif (not ONLY_IF_EMPTY) and (not SEED_FORCE) and db_has_data(cur):
            print("[step1] WARNING: database not empty. Set SEED_FORCE=true to append or SEED_RESET=true to truncate.", flush=True)

        caps = (TXN_MIN, TXN_MAX)

        flags = detect_columns(cur)
        print(f"[step1] detected flags: {flags}", flush=True)

        total = int(USERS_TARGET)
        batch = 5000
        created_accounts = []
        done = 0

        while done < total:
            n = min(batch, total - done)
            print(f"[step1] seeding customers {done+1}..{done+n}", flush=True)
            customer_ids = seed_customers(cur, n, has_segment=flags["customer_segment"])

            acc_ids = seed_agreements_and_accounts(
                cur, customer_ids,
                has_watchlist=flags["account_watch"],
                has_segment=flags["customer_segment"],
            )
            created_accounts.extend(acc_ids)
            done += n

        cur.execute(
            """
          SELECT a.account_id, g.customer_id, c.segment
          FROM accounts a
          JOIN agreements g ON g.agreement_id = a.agreement_id
          JOIN customers c  ON c.customer_id = g.customer_id
        """
        )
        owners_info = {}
        for acc_id, cust_id, segment in cur.fetchall():
            owners_info.setdefault(cust_id, {"segment": segment, "accounts": []})["accounts"].append(acc_id)

        print("[step1] seeding transactions…", flush=True)
        seed_transactions(
            cur,
            created_accounts,
            caps,
            has_labels=flags["txn_label"],
            has_tags=flags["txn_tags"],
            owners_info=owners_info,
        )

        if APPLY_BALANCES:
            print("[step1] computing balances…", flush=True)
            recompute_balances(cur)

        print("[step1] done.", flush=True)


if __name__ == "__main__":
    main()
