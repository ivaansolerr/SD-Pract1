
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket

def _delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA] Delivery failed: {err}")

def make_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": socket.gethostname(),
        "linger.ms": 5
    }
    return Producer(conf)

def make_consumer(bootstrap_servers: str, group_id: str, topics: list):
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    return consumer

def kafka_poll_loop(consumer, handler):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            handler(msg.topic(), msg.value())
    finally:
        consumer.close()
