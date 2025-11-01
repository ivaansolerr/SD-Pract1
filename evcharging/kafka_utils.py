import json
from confluent_kafka import Producer, Consumer
from typing import Callable

def build_producer(bootstrap_servers) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 0,              # Enviar inmediatamente
        "batch.num.messages": 1,     # No acumular en buffer
    })

def build_consumer(bootstrap_servers, group, topics):
    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 6000,       # 6 segundos para detección de fallos
        "max.poll.interval.ms": 300000,   # 5 minutos, mínimo requerido
    })
    c.subscribe(topics)
    return c



def send(producer, topic, payload):
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()  # envía inmediatamente


def poll_loop(consumer, handler: Callable[[str, dict], None]):
    try:
        while True:
            # Poll muy corto → latencia mínima
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka] Error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                handler(msg.topic(), data)
            except Exception as e:
                print(f"[Kafka] JSON decode error: {e}")
                continue
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
