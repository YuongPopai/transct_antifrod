from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import orjson
import logging

from app.cache import RulesCache
from app.db import connect
from app.rule_engine import eval_rule
from app.settings import KAFKA_BOOTSTRAP, TOPIC_IN, TOPIC_OUT, RULES_REFRESH_SEC
from app.validate import validate_payload

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(obj) -> bytes:
    return orjson.dumps(obj)


async def main() -> None:
    conn = await connect()
    cache = RulesCache()
    await cache.refresh(conn)

    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="classifier",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: _json_dumps(v),
    )

    await consumer.start()
    await producer.start()

    async def refresh_loop():
        while True:
            try:
                await cache.refresh(conn)
            except Exception:
                pass
            await asyncio.sleep(RULES_REFRESH_SEC)

    refresher = asyncio.create_task(refresh_loop())

    try:
        async for msg in consumer:
            event = msg.value
            await handle_event(event, conn, cache, producer)
    finally:
        refresher.cancel()
        await consumer.stop()
        await producer.stop()
        await conn.close()


async def handle_event(event: dict, conn, cache: RulesCache, producer: AIOKafkaProducer) -> None:
    event_id = event.get("event_id") or str(uuid.uuid4())
    entity_key = event.get("entity_key")
    occurred_at_s = event.get("occurred_at")
    payload = event.get("payload") or {}
    meta = event.get("meta") or {}

    reasons = []
    matched_rules = []
    decision = "legit"
    risk = 0.0

    if not isinstance(payload, dict):
        decision = "invalid"
        reasons.append({"type": "format", "message": "payload must be object"})
        payload = {}

    if not entity_key or entity_key not in cache.entities:
        decision = "invalid"
        reasons.append({"type": "missing_entity", "message": "unknown or missing entity_key"})
        entity = None
    else:
        entity = cache.entities[entity_key]

    if occurred_at_s:
        try:
            occurred_at = datetime.fromisoformat(str(occurred_at_s).replace("Z", "+00:00"))
        except Exception:
            occurred_at = _now()
    else:
        occurred_at = _now()

    if entity:
        attrs = cache.attributes.get(entity.entity_id, {})
        val_errors = validate_payload(payload, attrs)
        if val_errors:
            decision = "invalid"
            reasons.extend([{"type": "validation", **e} for e in val_errors])
        else:
            rules = cache.rules_by_entity.get(entity.entity_id, [])
            for r in rules:
                try:
                    if eval_rule(r, payload):
                        matched_rules.append({"rule_id": r.rule_id, "name": r.name, "severity": r.severity})
                        reasons.append({"type": "rule_match", "rule_id": r.rule_id, "name": r.name})
                except Exception as e:
                    logger.exception("Error evaluating rule %s", r.rule_id)
                    reasons.append({"type": "rule_error", "rule_id": r.rule_id, "error": str(e)})
            if matched_rules:
                decision = "alert"
                sev_max = {"info": 0.2, "warn": 0.6, "high": 0.9}
                risk = max(sev_max.get(m.get("severity"), 0.6) for m in matched_rules)
            else:
                decision = "legit"
                risk = 0.0

    classified_at = _now()

    entity_id = entity.entity_id if entity else None
    event_pk = None
    if entity_id is not None:
        event_pk = await conn.fetchval(
            """
            INSERT INTO app.events(event_id, entity_id, occurred_at, payload, meta)
            VALUES ($1::uuid, $2, $3, $4::jsonb, $5::jsonb)
            ON CONFLICT (event_id) DO UPDATE SET payload=EXCLUDED.payload
            RETURNING event_pk
            """,
            event_id,
            entity_id,
            occurred_at,
            json.dumps(payload),
            json.dumps(meta),
        )
        await conn.execute(
            """
            INSERT INTO app.classifications(event_pk, decision, risk, matched_rules, reasons, classified_at)
            VALUES ($1, $2::decision_type, $3, $4::jsonb, $5::jsonb, $6)
            """,
            event_pk,
            decision,
            float(risk),
            json.dumps(matched_rules),
            json.dumps(reasons),
            classified_at,
        )

    out = {
        "event_id": str(event_id),
        "entity_key": entity_key or "",
        "occurred_at": occurred_at.isoformat(),
        "payload": payload,
        "decision": decision,
        "risk": risk,
        "matched_rules": matched_rules,
        "reasons": reasons,
        "classified_at": classified_at.isoformat(),
    }

    await producer.send_and_wait(TOPIC_OUT, out)


if __name__ == "__main__":
    asyncio.run(main())
