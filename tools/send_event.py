import json
import os
import sys
from aiokafka import AIOKafkaProducer
import asyncio

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.getenv("TOPIC_IN", "events.in")


async def main():
    if len(sys.argv) < 2:
        print("Usage: python send_event.py '<json>'")
        print("Example: python send_event.py '{" + "\"entity_key\":\"transaction\",\"payload\":{\"sender\":\"A\",\"receiver\":\"B\",\"amount\":3}}'")
        sys.exit(1)
    evt = json.loads(sys.argv[1])
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()
    try:
        await producer.send_and_wait(TOPIC_IN, json.dumps(evt).encode("utf-8"))
        print("sent")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
