import json
from confluent_kafka import Producer, Consumer
from typing import Callable, Optional

def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,   # <--- CAMBIA en .env si es necesario
        "enable.idempotence": True,
        "linger.ms": 10
    })

def build_consumer(bootstrap_servers: str, group: str, topics: list[str]) -> Consumer:
    c = Consumer({
        "bootstrap.servers": bootstrap_servers,   # <--- CAMBIA en .env si es necesario
        "group.id": group,
        "auto.offset.reset": "earliest"
    })
    c.subscribe(topics)
    return c

def send(producer: Producer, topic: str, payload: dict):
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()

def poll_loop(consumer: Consumer, handler: Callable[[str, dict], None]):
    import json as _json
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka] Error: {msg.error()}")
                continue
            try:
                data = _json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[Kafka] JSON decode error: {e}")
                continue
            handler(msg.topic(), data)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()