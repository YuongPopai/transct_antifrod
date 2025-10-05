CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at);

CREATE INDEX IF NOT EXISTS idx_agreements_customer ON agreements(customer_id);
CREATE INDEX IF NOT EXISTS idx_accounts_agreement   ON accounts(agreement_id);

CREATE INDEX IF NOT EXISTS idx_txn_src ON transactions(src_account_id);
CREATE INDEX IF NOT EXISTS idx_txn_dst ON transactions(dst_account_id);
CREATE INDEX IF NOT EXISTS idx_txn_initiated ON transactions(initiated_at);
CREATE INDEX IF NOT EXISTS idx_txn_status    ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_txn_currency  ON transactions(currency);
