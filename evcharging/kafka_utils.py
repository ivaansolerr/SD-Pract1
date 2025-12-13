import json
from confluent_kafka import Producer, Consumer
from typing import Callable

def buildProducer(bootstrapServers):
    return Producer({
        "bootstrap.servers": bootstrapServers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 0,              
        "batch.num.messages": 1,     
    })

def buildConsumer(bootstrapServers, group, topics):
    c = Consumer({
        "bootstrap.servers": bootstrapServers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 6000,       
        "max.poll.interval.ms": 300000,   
    })
    c.subscribe(topics)
    return c

def send(producer, topic, payload):
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()  

def pollLoop(consumer, handler):
    try:
        while True:
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