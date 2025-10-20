
import json, threading, time, os
from dotenv import load_dotenv
from kafka_bus import make_producer, make_consumer, kafka_poll_loop, _delivery_report

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_DRIVER_REQUESTS   = os.getenv("TOPIC_DRIVER_REQUESTS", "driver-requests")
TOPIC_CENTRAL_EVENTS    = os.getenv("TOPIC_CENTRAL_EVENTS", "central-events")

class EVDriver:
    def __init__(self, driver_id: str):
        self.driver_id = driver_id
        self.producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = make_consumer(KAFKA_BOOTSTRAP_SERVERS, f"driver-{driver_id}", [TOPIC_CENTRAL_EVENTS])

    def request_supply(self, cp_id: str):
        payload = {"driverId": self.driver_id, "cpId": cp_id}
        self.producer.produce(TOPIC_DRIVER_REQUESTS, json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        self.producer.flush()

    def start_listening(self):
        def on_evt(topic, value):
            evt = json.loads(value.decode("utf-8"))
            if evt.get("driverId") == self.driver_id:
                print(f"[DRIVER {self.driver_id}] {evt['message']}")
        threading.Thread(target=lambda: kafka_poll_loop(self.consumer, on_evt), daemon=True).start()

def main():
    # ⚠️ Cambia el driver_id y el cp objetivo
    d = EVDriver(driver_id="D-001")
    d.start_listening()
    time.sleep(1)
    d.request_supply("CP-001")
    while True:
        time.sleep(2)

if __name__ == "__main__":
    main()
