import socket, threading, json, sys, os
from typing import Dict
from dotenv import load_dotenv
from kafka_bus import make_producer, make_consumer, kafka_poll_loop, _delivery_report
from database import Database
from protocol import pack_message, unpack_message

# Load .env
load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_DRIVER_REQUESTS = os.getenv("TOPIC_DRIVER_REQUESTS", "driver-requests")
TOPIC_CP_COMMANDS = os.getenv("TOPIC_CP_COMMANDS", "cp-commands")
TOPIC_CP_TELEMETRY = os.getenv("TOPIC_CP_TELEMETRY", "cp-telemetry")
TOPIC_BILLING = os.getenv("TOPIC_BILLING", "billing")
TOPIC_CENTRAL_EVENTS = os.getenv("TOPIC_CENTRAL_EVENTS", "central-events")

# Central socket
CENTRAL_LISTEN_HOST = os.getenv("CENTRAL_LISTEN_HOST", "0.0.0.0")
CENTRAL_LISTEN_PORT = int(os.getenv("CENTRAL_LISTEN_PORT", "5051"))

class EVCentral:
    def __init__(self):
        # Kafka
        self.producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)
        self.driver_consumer    = make_consumer(KAFKA_BOOTSTRAP_SERVERS, "central-driver-requests", [TOPIC_DRIVER_REQUESTS])
        self.telemetry_consumer = make_consumer(KAFKA_BOOTSTRAP_SERVERS, "central-telemetry",       [TOPIC_CP_TELEMETRY])
        self.billing_consumer   = make_consumer(KAFKA_BOOTSTRAP_SERVERS, "central-billing",         [TOPIC_BILLING])

        # DB
        self.db = Database()

        # Sockets
        self.running = False
        self.sock = None
        self.cp_connections: Dict[str, socket.socket] = {}

        # Sessions (driver:cp -> key)
        self.session_keys: Dict[str, str] = {}

    # ====== SOCKETS: aceptar monitores (EV_CP_M) ======
    def _handle_monitor_client(self, conn: socket.socket, addr):
        buffer = b''
        cp_id = None
        try:
            while self.running:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data
                while True:
                    payload, used = unpack_message(buffer)
                    if not payload:
                        break
                    buffer = buffer[used:]
                    msg = json.loads(payload)
                    op = msg.get("op")
                    if op == "REGISTER":
                        cp_id = msg["cpId"]
                        location = msg.get("location","unknown")
                        price = float(msg.get("price", 0.35))
                        self.db.upsert_cp(cp_id, location, price, status="AVAILABLE")
                        self.cp_connections[cp_id] = conn
                        print(f"[CENTRAL] CP registrado: {cp_id} @ {location}")
                    elif op == "HEALTH":
                        cp_id = msg["cpId"]
                        status = msg.get("status","AVAILABLE")
                        self.db.set_cp_status(cp_id, status)
                    elif op == "KO":
                        cp_id = msg["cpId"]
                        self.db.set_cp_status(cp_id, "BROKEN")
                        print(f"[CENTRAL] Avería reportada en {cp_id}")
                    else:
                        print(f"[CENTRAL] Mensaje monitor desconocido: {msg}")
        finally:
            if cp_id and self.cp_connections.get(cp_id) is conn:
                del self.cp_connections[cp_id]
            conn.close()

    def start_socket_server(self):
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((CENTRAL_LISTEN_HOST, CENTRAL_LISTEN_PORT))
        self.sock.listen()
        print(f"[CENTRAL] Escuchando monitores en {CENTRAL_LISTEN_HOST}:{CENTRAL_LISTEN_PORT}")
        def accept_loop():
            while self.running:
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_monitor_client, args=(conn, addr), daemon=True).start()
        threading.Thread(target=accept_loop, daemon=True).start()

    # ====== KAFKA: Consumers ======
    def start_kafka_consumers(self):
        # DRIVER REQUESTS
        def on_driver_request(topic, value):
            msg = json.loads(value.decode("utf-8"))
            driver_id = msg["driverId"]
            cp_id = msg["cpId"]
            cp = self.db.get_cp(cp_id)
            if not cp or cp.get("status") != "AVAILABLE":
                self._emit_event(driver_id, f"CP {cp_id} no disponible", level="WARN")
                return
            # autorizar
            self.db.set_cp_status(cp_id, "SUPPLYING", current_driver=driver_id)
            self._emit_event(driver_id, f"Autorizado en {cp_id}")
            # crear sesión
            key = self.db.start_session(driver_id, cp_id)
            self.session_keys[f"{driver_id}:{cp_id}"] = key
            # enviar comando a CP
            self._send_command(cp_id, "START_SUPPLY", driver_id=driver_id)

        def run_driver_requests():
            kafka_poll_loop(self.driver_consumer, on_driver_request)

        # TELEMETRY
        def on_telemetry(topic, value):
            msg = json.loads(value.decode("utf-8"))
            cp_id = msg["cpId"]
            event = msg.get("event","TICK")
            driver_id = msg.get("driverId")
            key = self.session_keys.get(f"{driver_id}:{cp_id}")
            if event == "TICK":
                kwh = float(msg.get("kwh", 0))
                price = float(msg.get("price", 0))
                if key: self.db.accumulate_session(key, kwh, price)
            elif event == "ENDED":
                if key:
                    self.db.end_session(key)
                    s = self.db.get_session(key) or {}
                    # emitir ticket
                    self._emit_billing(driver_id, cp_id, float(s.get("kwh",0.0)), float(s.get("price",0.0)))
                    # reset CP
                    self.db.set_cp_status(cp_id, "AVAILABLE", current_driver=None)
                    self.session_keys.pop(f"{driver_id}:{cp_id}", None)

        def run_telemetry():
            kafka_poll_loop(self.telemetry_consumer, on_telemetry)

        # BILLING -> notificar al driver
        def on_billing(topic, value):
            bill = json.loads(value.decode("utf-8"))
            self._emit_event(
                bill["driverId"],
                f"Ticket {bill['cpId']}: {round(bill['totalKwh'],3)} kWh, {round(bill['totalPrice'],2)} €",
                level="INFO"
            )

        def run_billing():
            kafka_poll_loop(self.billing_consumer, on_billing)

        threading.Thread(target=run_driver_requests, daemon=True).start()
        threading.Thread(target=run_telemetry, daemon=True).start()
        threading.Thread(target=run_billing, daemon=True).start()

    # ====== Admin commands ======
    def _send_command(self, cp_id: str, command: str, driver_id: str = None):
        payload = {"cpId": cp_id, "command": command}
        if driver_id: payload["driverId"] = driver_id
        self.producer.produce(TOPIC_CP_COMMANDS, json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        self.producer.flush()

    def _emit_event(self, driver_id: str, message: str, level: str = "INFO"):
        payload = {"driverId": driver_id, "message": message, "level": level}
        self.producer.produce(TOPIC_CENTRAL_EVENTS, json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        self.producer.flush()

    def _emit_billing(self, driver_id: str, cp_id: str, total_kwh: float, total_price: float):
        payload = {"driverId": driver_id, "cpId": cp_id, "totalKwh": total_kwh, "totalPrice": total_price}
        self.producer.produce(TOPIC_BILLING, json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        self.producer.flush()

    def stop_cp(self, cp_id: str):
        self._send_command(cp_id, "STOP")

    def resume_cp(self, cp_id: str):
        self._send_command(cp_id, "RESUME")

    def shutdown_system(self):
        for cp in self.db.cps.find({}, {"id": 1}):
            self._send_command(cp["id"], "STOP")

    # ====== CLI admin ======
    def start_admin_cli(self):
        print("""
[ADMIN] Comandos:
  STOP_CP <CP_ID>
  RESUME_CP <CP_ID>
  SHUTDOWN_SYSTEM
  EXIT
        """.strip())
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()
            if cmd == "STOP_CP" and len(parts) == 2:
                self.stop_cp(parts[1]); print(f"[ADMIN] STOP enviado a {parts[1]}")
            elif cmd == "RESUME_CP" and len(parts) == 2:
                self.resume_cp(parts[1]); print(f"[ADMIN] RESUME enviado a {parts[1]}")
            elif cmd == "SHUTDOWN_SYSTEM":
                self.shutdown_system(); print("[ADMIN] STOP enviado a todos los CPs")
            elif cmd == "EXIT":
                break
            else:
                print("[ADMIN] Comando no reconocido")

def main():
    central = EVCentral()
    central.start_socket_server()
    central.start_kafka_consumers()
    central.start_admin_cli()

if __name__ == "__main__":
    main()
