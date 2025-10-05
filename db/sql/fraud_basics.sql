BEGIN;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'customer_segment') THEN
    CREATE TYPE customer_segment AS ENUM ('normal', 'suspicious');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'txn_label') THEN
    CREATE TYPE txn_label AS ENUM ('legit', 'fraud');
  END IF;
END $$;

ALTER TABLE IF EXISTS customers
  ADD COLUMN IF NOT EXISTS segment customer_segment NOT NULL DEFAULT 'normal';

ALTER TABLE IF EXISTS accounts
  ADD COLUMN IF NOT EXISTS is_watchlisted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE IF EXISTS transactions
  ADD COLUMN IF NOT EXISTS label txn_label NOT NULL DEFAULT 'legit',
  ADD COLUMN IF NOT EXISTS tags  JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(segment);
CREATE INDEX IF NOT EXISTS idx_accounts_watchlist ON accounts(is_watchlisted);
CREATE INDEX IF NOT EXISTS idx_txn_label ON transactions(label);
CREATE INDEX IF NOT EXISTS idx_txn_tags_gin ON transactions USING GIN (tags);

COMMIT;