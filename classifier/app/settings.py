import os


def getenv(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise RuntimeError(f"Missing env {name}")
        return default
    return v


DATABASE_URL = getenv("DATABASE_URL")
KAFKA_BOOTSTRAP = getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = getenv("TOPIC_IN", "events.in")
TOPIC_OUT = getenv("TOPIC_OUT", "events.classified")
RULES_REFRESH_SEC = int(getenv("RULES_REFRESH_SEC", "10"))
