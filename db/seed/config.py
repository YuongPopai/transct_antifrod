import os


PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "antifraud")
PG_USER = os.getenv("POSTGRES_USER", "antifraud")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "antifraud")


USERS_TARGET = int(os.getenv("SEED_USERS", "50000"))
MIN_AGR = int(os.getenv("SEED_MIN_AGR", "1"))
MAX_AGR = int(os.getenv("SEED_MAX_AGR", "3"))
TRANSACTIONS_PER_ACCOUNT_MIN = int(os.getenv("SEED_TXN_PER_ACC_MIN", "50"))
TRANSACTIONS_PER_ACCOUNT_MAX = int(os.getenv("SEED_TXN_PER_ACC_MAX", "120"))
BATCH_SIZE = int(os.getenv("SEED_BATCH", "5000"))
CURRENCIES = os.getenv("SEED_CURRENCIES", "RUB,USD,EUR").split(",")

SUSPICIOUS_USER_SHARE = float(os.getenv("SEED_SUSPICIOUS_USER_SHARE", "0.1"))  
EMPTY_USER_SHARE      = float(os.getenv("SEED_EMPTY_USER_SHARE", "0.15"))      
FRAUD_TXN_SHARE       = float(os.getenv("SEED_FRAUD_TXN_SHARE", "0.03"))       