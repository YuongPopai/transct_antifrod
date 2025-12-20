from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.core.settings import settings
from app.ws.manager import manager


async def run_kafka_to_ws(stop_event: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(
        settings.topic_classified,
        bootstrap_servers=settings.kafka_bootstrap,
        enable_auto_commit=True,
        group_id="backend-ws-bridge",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    try:
        while not stop_event.is_set():
            msg = await consumer.getone()
            await manager.broadcast(msg.value)
    except Exception:
        raise
    finally:
        await consumer.stop()
