BEGIN;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agreement_type') THEN
    CREATE TYPE agreement_type AS ENUM ('current', 'savings', 'deposit');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_status') THEN
    CREATE TYPE account_status  AS ENUM ('active', 'blocked', 'closed');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'txn_status') THEN
    CREATE TYPE txn_status      AS ENUM ('initiated', 'settled', 'reversed', 'failed');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'txn_channel') THEN
    CREATE TYPE txn_channel     AS ENUM ('online', 'mobile', 'atm', 'branch', 'card');
  END IF;
END $$;

CREATE SEQUENCE IF NOT EXISTS seq_agreement_no START 1;
CREATE SEQUENCE IF NOT EXISTS seq_account_no   START 1;

-- Клиенты
CREATE TABLE IF NOT EXISTS customers (
    customer_id     BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    date_of_birth   DATE NOT NULL,
    email           CITEXT UNIQUE,
    phone           TEXT,
    address_line1   TEXT,
    address_line2   TEXT,
    city            TEXT,
    country         TEXT DEFAULT 'RU',
    risk_score      NUMERIC(5,2),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);

-- Договоры
CREATE TABLE IF NOT EXISTS agreements (
    agreement_id    BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    agreement_no    TEXT NOT NULL UNIQUE DEFAULT ('AGR' || lpad(nextval('seq_agreement_no')::text, 9, '0')),
    type            agreement_type NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'active'
);

-- Счета
CREATE TABLE IF NOT EXISTS accounts (
    account_id      BIGSERIAL PRIMARY KEY,
    agreement_id    BIGINT NOT NULL REFERENCES agreements(agreement_id) ON DELETE CASCADE,
    account_no      TEXT NOT NULL UNIQUE DEFAULT ('40817' || lpad(nextval('seq_account_no')::text, 10, '0')),
    iban            TEXT,
    currency        CHAR(3) NOT NULL DEFAULT 'RUB',
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          account_status NOT NULL DEFAULT 'active'
);

-- Транзакции
CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL PRIMARY KEY,
    src_account_id  BIGINT REFERENCES accounts(account_id),
    dst_account_id  BIGINT REFERENCES accounts(account_id),
    amount          NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    currency        CHAR(3) NOT NULL,
    initiated_at    TIMESTAMPTZ NOT NULL,
    settled_at      TIMESTAMPTZ,
    channel         txn_channel NOT NULL,
    status          txn_status NOT NULL,
    description     TEXT
);

-- Снимки балансов
CREATE TABLE IF NOT EXISTS account_balances (
    account_id  BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    as_of_date  DATE NOT NULL,
    balance     NUMERIC(18,2) NOT NULL,
    PRIMARY KEY (account_id, as_of_date)
);

COMMIT;
