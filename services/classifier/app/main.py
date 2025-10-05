import asyncio
import contextlib
import json
import logging
import os
import re
import signal
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import orjson
import pytz
import yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from dateutil import parser as dtparser


KEEP_FEATURE_SIGNALS = os.getenv("KEEP_FEATURE_SIGNALS", "true").lower() in ("1","true","yes")
BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOP_N_REASONS = int(os.getenv("TOP_N_REASONS", "0"))
TOPIC_IN = os.getenv("TOPIC_IN", "tx.all")
TOPIC_OUT_LEGIT = os.getenv("TOPIC_OUT_LEGIT", "tx.legit")
TOPIC_OUT_SUS = os.getenv("TOPIC_OUT_SUS", "tx.sus")
GROUP_ID = os.getenv("GROUP_ID", "classifier-v1")
RULES_PATH = os.getenv("RULES_PATH", "/app/app/rules_final.yaml")
REFRESH_RULES_SEC = float(os.getenv("REFRESH_RULES_SEC", "10"))
LOCAL_TZ = os.getenv("LOCAL_TZ", "UTC")
CREATE_TOPICS = os.getenv("CREATE_TOPICS", "true").lower() in ("1", "true", "yes")

WINDOW_MINUTES_IDF = 60
WINDOW_MINUTES_VELOCITY = 5
VELOCITY_EVENT_THRESHOLD = 4
PINGPONG_WINDOW_SEC = 15 * 60

DEFAULT_TAG_WEIGHTS: Dict[str, float] = {
    "structuring": 0.6,
    "velocity": 0.6,
    "new-recipients": 0.4,
    "ping-pong": 0.6,
    "cashout": 0.7,
    "watchlist": 0.7,
    "currency-anomaly": 0.5,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("classifier")

RULES: Dict[str, Any] = {}
RULES_MTIME: float = 0.0

DST_SEEN: Dict[int, Deque[datetime]] = defaultdict(lambda: deque())
SRC_ACTIVITY: Dict[int, Deque[datetime]] = defaultdict(lambda: deque())
PAIR_LAST: Dict[Tuple[int, int], Tuple[int, int, datetime]] = {}

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def to_dt(ts: Optional[str]) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    try:
        return dtparser.isoparse(ts).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def clamp01(x: Optional[float]) -> float:
    if x is None:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def is_round_amount(amount: Optional[float]) -> bool:
    try:
        if amount is None:
            return False
        major = int(float(amount))
        cents = int(round((float(amount) - major) * 100))
        return cents in (0, 99)
    except Exception:
        return False

def get_path(obj: Dict[str, Any], path: str):
    cur = obj
    for p in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur

def load_rules_once() -> None:
    global RULES, RULES_MTIME
    try:
        st = os.stat(RULES_PATH)
        mtime = st.st_mtime
    except FileNotFoundError:
        log.warning("rules file not found: %s", RULES_PATH)
        return

    if mtime <= RULES_MTIME:
        return

    try:
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError("rules root must be a mapping")
        RULES = data
        RULES_MTIME = mtime
        log.info("rules loaded from %s", RULES_PATH)
    except Exception as e:
        log.error("failed to parse rules at %s: %s", RULES_PATH, e)

async def rules_autoreload():
    while True:
        with contextlib.suppress(Exception):
            load_rules_once()
        await asyncio.sleep(REFRESH_RULES_SEC)

def update_idf(dst_id: Optional[int], t: datetime) -> float:
    if dst_id is None:
        return 0.0
    dq = DST_SEEN[dst_id]
    dq.append(t)
    cutoff = t - timedelta(minutes=WINDOW_MINUTES_IDF)
    while dq and dq[0] < cutoff:
        dq.popleft()
    import math
    idf = 1.0 / max(1.0, math.log1p(len(dq)))
    return clamp01(idf)

def update_velocity(src_id: Optional[int], t: datetime) -> float:
    if src_id is None:
        return 0.0
    dq = SRC_ACTIVITY[src_id]
    dq.append(t)
    cutoff = t - timedelta(minutes=WINDOW_MINUTES_VELOCITY)
    while dq and dq[0] < cutoff:
        dq.popleft()
    n = len(dq)
    if n <= VELOCITY_EVENT_THRESHOLD:
        return 0.0
    extra = n - VELOCITY_EVENT_THRESHOLD
    return clamp01(min(0.6, 0.1 * extra))

def detect_pingpong(src_id: Optional[int], dst_id: Optional[int], t: datetime) -> float:
    try:
        if src_id is None or dst_id is None:
            return 0.0
        a, b = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        key = (a, b)
        prev = PAIR_LAST.get(key)
        PAIR_LAST[key] = (src_id, dst_id, t)
        if not prev:
            return 0.0
        last_src, last_dst, last_time = prev
        if last_src == dst_id and last_dst == src_id:
            dt = (t - last_time).total_seconds()
            if 0 <= dt <= PINGPONG_WINDOW_SEC:
                return 0.6
        return 0.0
    except Exception:
        return 0.0

def score_by_tags(tags: Optional[List[str]]) -> float:
    if not tags:
        return 0.0
    s = 0.0
    for tag in tags:
        s += DEFAULT_TAG_WEIGHTS.get(tag, 0.0)
    return clamp01(1.0 - 1.0 / (1.0 + s))

def eval_predicate(pred: Dict[str, Any], event: Dict[str, Any], features: Dict[str, Any]) -> bool:
    if not isinstance(pred, dict):
        return False
    if "min_amount" in pred or "max_amount" in pred or "has_tag" in pred:
        if "min_amount" in pred:
            amt = safe_float(get_path(event, "amount"))
            if amt is None:
                return False
            if amt < float(pred["min_amount"]):
                return False
        if "max_amount" in pred:
            amt = safe_float(get_path(event, "amount"))
            if amt is None:
                return False
            if amt > float(pred["max_amount"]):
                return False
        if "has_tag" in pred:
            tags = get_path(event, "tags") or []
            if pred["has_tag"] not in tags:
                return False
        return True

    subject = pred.get("subject") or pred.get("field")
    if not subject:
        return False
    op = pred.get("op", "eq")
    value = pred.get("value")

    if subject.startswith("event."):
        subj_val = get_path(event, subject[len("event."):])
    elif subject.startswith("features.") or subject.startswith("feature."):
        subj_val = get_path(features, subject.split(".", 1)[1])
    else:
        subj_val = get_path(event, subject) if get_path(event, subject) is not None else get_path(features, subject)

    if op == "exists":
        return subj_val is not None

    if op == "contains":
        if subj_val is None:
            return False
        if isinstance(subj_val, (list, tuple, set)):
            return value in subj_val
        if isinstance(subj_val, str):
            return str(value) in subj_val
        return False

    if op == "in":
        return subj_val in (value or [])

    if op == "regex":
        try:
            return bool(re.search(value, str(subj_val or "")))
        except Exception:
            return False

    subj_num = safe_float(subj_val)
    val_num = safe_float(value)
    if op in ("gt", "gte", "lt", "lte"):
        if subj_num is None or val_num is None:
            return False
        if op == "gt":
            return subj_num > val_num
        if op == "gte":
            return subj_num >= val_num
        if op == "lt":
            return subj_num < val_num
        if op == "lte":
            return subj_num <= val_num

    if op == "eq":
        if isinstance(value, bool):
            return bool(subj_val) == value
        return str(subj_val) == str(value)
    if op == "neq":
        if isinstance(value, bool):
            return bool(subj_val) != value
        return str(subj_val) != str(value)

    return False

def apply_yaml_rules(event: Dict[str, Any], features: Dict[str, Any]) -> List[Dict[str, Any]]:
    reasons: List[Dict[str, Any]] = []
    rules = RULES.get("rules", []) if isinstance(RULES, dict) else []
    for r in rules:
        try:
            if not isinstance(r, dict):
                continue
            name = r.get("name", "rule")
            enabled = bool(r.get("enabled", True))
            if not enabled:
                continue
            add = float(r.get("add", r.get("weight", 0.0)))
            weight = clamp01(add)
            priority = r.get("priority")
            tags = r.get("tags", [])
            scope = r.get("scope")
            combine = r.get("combine", "all")
            negate = bool(r.get("negate", False))
            severity = r.get("severity")
            action = r.get("action")
            when = r.get("when", [])
            if isinstance(when, dict):
                preds = [when]
            elif isinstance(when, list):
                preds = when
            else:
                preds = []

            if not preds:
                matched = False
            else:
                results = [bool(eval_predicate(p, event, features)) for p in preds]
                if combine == "any":
                    matched = any(results)
                else:
                    matched = all(results)

            window_minutes = r.get("window_minutes")
            min_count = r.get("min_count")
            if window_minutes and min_count:
                try:
                    wm = int(window_minutes)
                    mc = int(min_count)
                    t = to_dt(event.get("initiated_at"))
                    if scope == "dst":
                        dst_id = event.get("dst_account_id")
                        dq = DST_SEEN.get(dst_id, deque())
                        cutoff = t - timedelta(minutes=wm)
                        count = sum(1 for ts in dq if ts >= cutoff)
                        if count < mc:
                            matched = False
                    elif scope == "src":
                        src_id = event.get("src_account_id")
                        dq = SRC_ACTIVITY.get(src_id, deque())
                        cutoff = t - timedelta(minutes=wm)
                        count = sum(1 for ts in dq if ts >= cutoff)
                        if count < mc:
                            matched = False
                    else:
                        src_id = event.get("src_account_id")
                        dst_id = event.get("dst_account_id")
                        dq_src = SRC_ACTIVITY.get(src_id, deque())
                        dq_dst = DST_SEEN.get(dst_id, deque())
                        cutoff = t - timedelta(minutes=wm)
                        count = sum(1 for ts in dq_src if ts >= cutoff) + sum(1 for ts in dq_dst if ts >= cutoff)
                        if count < mc:
                            matched = False
                except Exception:
                    pass

            if negate:
                matched = not matched

            if matched and weight > 0.0:
                evidence = {
                    "when": when,
                    "meta": {"priority": priority, "tags": tags, "scope": scope, "combine": combine, "negate": negate},
                }
                reason = {"rule": name, "score": weight, "evidence": evidence}
                if severity:
                    reason["severity"] = severity
                if action:
                    reason["action"] = action
                reasons.append(reason)
        except Exception as e:
            log.debug("yaml rule error: %s (%s)", r, e)
    return reasons

@dataclass
class Classification:
    decision: str    
    risk: float       
    severity: str   
    reasons: List[Dict[str, Any]]

def classify_event(event: Dict[str, Any]) -> Classification:
    reasons: List[Dict[str, Any]] = []

    tags = event.get("tags") or []
    amt = safe_float(event.get("amount"))
    t = to_dt(event.get("initiated_at"))
    src_id = event.get("src_account_id")
    dst_id = event.get("dst_account_id")

    features: Dict[str, Any] = {}
    features["tag_score"] = score_by_tags(tags)
    features["is_round"] = is_round_amount(amt)
    features["idf"] = update_idf(dst_id, t)
    features["velocity"] = update_velocity(src_id, t)
    features["pingpong"] = detect_pingpong(src_id, dst_id, t)
    features["amount"] = amt
    features["src_account_id"] = src_id
    features["dst_account_id"] = dst_id
    features["tags"] = tags
    features["initiated_at"] = t.isoformat()

    if features["tag_score"] > 0:
        reasons.append({"rule": "tags", "score": clamp01(features["tag_score"]), "evidence": {"tags": tags}})
    if features["is_round"]:
        reasons.append({"rule": "round_amount", "score": 0.08, "evidence": {"round": True}})
    if features["idf"] > 0.5:
        reasons.append({"rule": "dst_idf_rare", "score": 0.12, "evidence": {"idf": round(features["idf"], 3)}})
    if features["velocity"] > 0:
        reasons.append({"rule": "velocity_local", "score": clamp01(features["velocity"]), "evidence": {"events_in_window": len(SRC_ACTIVITY.get(src_id, []))}})
    if features["pingpong"] > 0:
        reasons.append({"rule": "ping_pong", "score": features["pingpong"], "evidence": {"window_sec": PINGPONG_WINDOW_SEC}})
        
    reasons.extend(apply_yaml_rules(event, features))

    total_full = clamp01(sum(float(r.get("score", 0.0)) for r in reasons))
    if total_full < 0.25:
        severity_full = "info"
    elif total_full < 0.6:
        severity_full = "warn"
    else:
        severity_full = "high"

    decision_full = "sus" if total_full >= 0.5 else "legit"

    if reasons:
        sorted_reasons = sorted(
            reasons,
            key=lambda r: (-float(r.get("score", 0.0)), str(r.get("rule", "")))
        )
        top_reason = sorted_reasons[0]
        out_reasons = [top_reason]
    else:
        out_reasons = []

    return Classification(
        decision=decision_full,
        risk=round(total_full, 3),
        severity=severity_full,
        reasons=out_reasons
    )

async def ensure_topics(brokers: str, topics: List[str]) -> None:
    if not CREATE_TOPICS:
        return
    try:
        admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
        await admin.start()
        try:
            existing = set((await admin.list_topics()) or [])
            to_create = [t for t in topics if t not in existing]
            if to_create:
                new = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in to_create]
                with contextlib.suppress(Exception):
                    await admin.create_topics(new_topics=new, validate_only=False)
                log.info("topics ensured: %s", ", ".join(topics))
        finally:
            await admin.close()
    except Exception as e:
        log.info("ensure_topics skipped (%s)", e)

async def handle_record(val_bytes: bytes, producer: AIOKafkaProducer):
    try:
        try:
            data = orjson.loads(val_bytes)
        except Exception:
            data = json.loads(val_bytes.decode("utf-8", errors="ignore"))
        if not isinstance(data, dict):
            return

        event = data.get("event", data)
        if not isinstance(event, dict):
            return

        cls = classify_event(event)
        out = {
            "event": event,
            "decision": cls.decision,
            "risk": cls.risk,
            "severity": cls.severity,
            "reasons": cls.reasons,
            "classified_at": now_utc_iso(),
        }

        topic = TOPIC_OUT_LEGIT if cls.decision == "legit" else TOPIC_OUT_SUS
        await producer.send_and_wait(topic, orjson.dumps(out))

    except Exception as e:
        try:
            preview = val_bytes[:300]
        except Exception:
            preview = b"<n/a>"
        log.error("[classifier] classify error: %s | raw=%r", e, preview)

async def main_async():
    tz = pytz.timezone(LOCAL_TZ) if LOCAL_TZ in pytz.all_timezones else pytz.UTC
    load_rules_once()

    await ensure_topics(BROKERS, [TOPIC_OUT_LEGIT, TOPIC_OUT_SUS])

    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BROKERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda v: v,
        max_poll_records=256,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BROKERS,
        linger_ms=25,
        acks="all",
        value_serializer=lambda v: v,
    )

    stop = asyncio.Event()

    def _graceful(*_):
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _graceful)

    reload_task = asyncio.create_task(rules_autoreload())

    await consumer.start()
    await producer.start()
    log.info(
        "[classifier] starting; kafka=%s in=%s out_legit=%s out_sus=%s",
        BROKERS, TOPIC_IN, TOPIC_OUT_LEGIT, TOPIC_OUT_SUS
    )

    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=512)
            if not batch:
                await asyncio.sleep(0.05)
                continue
            for _tp, records in batch.items():
                for r in records:
                    await handle_record(r.value, producer)
    finally:
        with contextlib.suppress(Exception):
            await consumer.stop()
        with contextlib.suppress(Exception):
            await producer.stop()
        reload_task.cancel()
        with contextlib.suppress(Exception):
            await reload_task

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
