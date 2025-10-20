
import threading, time, json, os
from dotenv import load_dotenv
from kafka_bus import make_consumer, make_producer, kafka_poll_loop, _delivery_report

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_CP_COMMANDS  = os.getenv("TOPIC_CP_COMMANDS", "cp-commands")
TOPIC_CP_TELEMETRY = os.getenv("TOPIC_CP_TELEMETRY", "cp-telemetry")

class EVCPEngine:
    def __init__(self, cp_id: str, price_per_kwh: float = 0.40):
        self.cp_id = cp_id
        self.price_per_kwh = price_per_kwh
        self.status = "AVAILABLE"
        self.current_driver = None
        self.producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = make_consumer(KAFKA_BOOTSTRAP_SERVERS, f"cp-engine-{cp_id}", [TOPIC_CP_COMMANDS])

    def _emit(self, payload: dict):
        self.producer.produce(TOPIC_CP_TELEMETRY, json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        self.producer.flush()

    def start_supply(self, driver_id: str):
        if self.status == "SUPPLYING":
            return
        self.status = "SUPPLYING"
        self.current_driver = driver_id
        self._emit({"cpId": self.cp_id, "driverId": driver_id, "event": "STARTED"})
        threading.Thread(target=self._tick_loop, daemon=True).start()

    def _tick_loop(self):
        kwh_per_sec = 0.2
        while self.status == "SUPPLYING":
            time.sleep(1)
            self._emit({"cpId": self.cp_id, "driverId": self.current_driver, "kwh": kwh_per_sec, "price": round(kwh_per_sec * self.price_per_kwh, 4), "event": "TICK"})

    def stop_supply(self):
        if self.status != "SUPPLYING":
            return
        self.status = "AVAILABLE"
        self._emit({"cpId": self.cp_id, "driverId": self.current_driver, "event": "ENDED"})
        self.current_driver = None

    def resume(self):
        if self.status != "SUPPLYING":
            self.status = "AVAILABLE"

    def start(self):
        def on_cmd(topic, value):
            msg = json.loads(value.decode("utf-8"))
            if msg.get("cpId") != self.cp_id:
                return
            cmd = msg.get("command")
            if cmd == "START_SUPPLY":
                self.start_supply(msg.get("driverId"))
            elif cmd == "STOP":
                self.stop_supply()
            elif cmd == "RESUME":
                self.resume()
        kafka_poll_loop(self.consumer, on_cmd)

def main():
    # ⚠️ Cambia el cp_id y price según tu despliegue
    engine = EVCPEngine(cp_id="CP-001", price_per_kwh=0.40)
    engine.start()

if __name__ == "__main__":
    main()
