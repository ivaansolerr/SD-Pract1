
import socket, time, json, threading, os
from dotenv import load_dotenv
from protocol import pack_message

load_dotenv()

CENTRAL_PUBLIC_HOST = os.getenv("CENTRAL_PUBLIC_HOST", "127.0.0.1")
CENTRAL_PUBLIC_PORT = int(os.getenv("CENTRAL_PUBLIC_PORT", "5051"))

class EVCpMonitor:
    def __init__(self, cp_id: str, location: str, price: float):
        self.cp_id = cp_id
        self.location = location
        self.price = price
        self.sock = None

    def _connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((CENTRAL_PUBLIC_HOST, CENTRAL_PUBLIC_PORT))
        payload = json.dumps({"op":"REGISTER","cpId": self.cp_id, "location": self.location, "price": self.price})
        self.sock.sendall(pack_message(payload))

    def start(self):
        self._connect()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _send(self, msg: dict):
        try:
            self.sock.sendall(pack_message(json.dumps(msg)))
        except Exception:
            try:
                time.sleep(1)
                self._connect()
            except Exception:
                pass

    def _heartbeat_loop(self):
        while True:
            time.sleep(1)
            self._send({"op":"HEALTH","cpId": self.cp_id, "status":"AVAILABLE"})

    def report_ko(self):
        self._send({"op":"KO","cpId": self.cp_id})

def main():
    # ⚠️ Cambia estos datos en cada CP
    monitor = EVCpMonitor(cp_id="CP-001", location="Alicante", price=0.40)
    monitor.start()
    while True:
        time.sleep(5)

if __name__ == "__main__":
    main()
