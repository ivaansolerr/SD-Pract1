import socket, sys, threading, time, queue
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk

from . import db
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

# --- Constantes de protocolo ---
STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

# --- Cola para comunicación GUI ↔ servidor ---
gui_queue = queue.Queue()

# === FUNCIONES DE PROTOCOLO ===
def xor_checksum(data: bytes) -> bytes:
    lrc = 0
    for b in data:
        lrc ^= b
    return bytes([lrc])

def encodeMess(msg) -> bytes:
    data = msg.encode()
    return STX + data + ETX + xor_checksum(data)

def parse_frame(frame):
    if len(frame) < 3 or frame[0] != 2 or frame[-2] != 3:
        return None
    data = frame[1:-2]
    return data.decode() if xor_checksum(data) == frame[-1:] else None

def cp_exists(cp_id):
    cp = db.get_cp(cp_id)
    return cp is not None

# === LÓGICA DEL SERVIDOR CENTRAL ===
def handle_client(conn, addr):
    print(f"[CENTRAL] Nueva conexión desde {addr}")
    cp_id = None
    try:
        if conn.recv(1024) != b"<ENC>":
            conn.close()
            return
        conn.send(ACK)

        cp_id = parse_frame(conn.recv(1024))
        if cp_id is None:
            conn.send(NACK)
            conn.close()
            return

        conn.send(ACK)

        if cp_exists(cp_id):
            print(f"[CENTRAL] CP :) {cp_id} autenticado. Estado → AVAILABLE")
            db.upsert_cp({"id": cp_id, "state": "AVAILABLE"})
            gui_queue.put(("update", cp_id, "AVAILABLE"))
            result = "OK"
        else:
            print(f"[CENTRAL] CP {cp_id} NO existe en la base de datos")
            result = "NO"

        conn.send(encodeMess(result))

        if conn.recv(1024) != ACK:
            conn.close()
            return

        conn.send(b"<EOT>")
        print(f"[CENTRAL] Handshake completado con CP {cp_id}, esperando mensajes...")

        conn.settimeout(10)
        while True:
            try:
                msg = conn.recv(1024)
                if not msg:
                    break

                if msg == b"KO":
                    print(f"[CENTRAL] :( CP {cp_id} averiado → UNAVAILABLE")
                    db.upsert_cp({
                        "id": cp_id,
                        "state": "UNAVAILABLE",
                        "last_seen": datetime.now(timezone.utc)
                    })
                    gui_queue.put(("update", cp_id, "UNAVAILABLE"))
                    conn.send(ACK)

                elif msg == b"OK":
                    print(f"[CENTRAL] :) CP {cp_id} recuperado → AVAILABLE")
                    db.upsert_cp({
                        "id": cp_id,
                        "state": "AVAILABLE",
                        "last_seen": datetime.now(timezone.utc)
                    })
                    gui_queue.put(("update", cp_id, "AVAILABLE"))
                    conn.send(ACK)

                else:
                    print(f"[CENTRAL] Mensaje desconocido de {cp_id}: {msg}")

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[CENTRAL] Error recibiendo de {cp_id}: {e}")
                break

    except Exception as e:
        print(f"[CENTRAL] Error con {addr}: {e}")

    finally:
        conn.close()
        if cp_id:
            print(f"[CENTRAL] Conexión cerrada con {addr}")
            gui_queue.put(("update", cp_id, "DISCONNECTED"))

def server_thread(port):
    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(5)
    print(f"[CENTRAL] En escucha permanente en {port}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

# === INTERFAZ GRÁFICA ===
class CentralGUI:
    COLORS = {
        "AVAILABLE": "#4CAF50",   # Verde
        "UNAVAILABLE": "#F44336", # Rojo
        "OUT_OF_ORDER": "#FFA500",# Naranja
        "SUPPLYING": "#00C853",   # Verde fuerte (cargando)
        "DISCONNECTED": "#9E9E9E",# Gris
        "UNKNOWN": "#BDBDBD"
    }

    def __init__(self, root):
        self.root = root
        self.root.title("EV Charging Central Monitor")
        self.root.geometry("900x600")

        self.frame = ttk.Frame(root, padding=10)
        self.frame.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(self.frame)
        self.scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.cp_widgets = {}

        # Mostrar todos los CPs existentes en gris al inicio
        self.init_from_db()

        # Inicia el loop que escucha actualizaciones
        self.update_gui_loop()

    def init_from_db(self):
        cps = db.list_charging_points()
        if not cps:
            print("[GUI] No hay puntos de carga registrados.")
        for cp in cps:
            cp["state"] = cp.get("state", "DISCONNECTED") or "DISCONNECTED"
            self.update_cp_display(cp)

    def update_cp_display(self, cp):
        """Crea o actualiza la tarjeta visual de un CP"""
        cp_id = cp.get("id")
        state = cp.get("state", "UNKNOWN")
        location = cp.get("location", "N/A")
        price = cp.get("price_eur_kwh", "N/A")

        color = self.COLORS.get(state, "#9E9E9E")
        text = (
            f"ID: {cp_id}\n"
            f"Ubicación: {location}\n"
            f"Precio: {price} €/kWh\n"
            f"Estado: {state}"
        )

        if cp_id not in self.cp_widgets:
            frame = tk.Frame(self.scrollable_frame, bg=color, bd=2, relief="groove", padx=10, pady=10)
            label = tk.Label(frame, text=text, bg=color, fg="white", font=("Arial", 12), justify="left")
            label.pack(fill="both", expand=True)
            frame.pack(fill="x", pady=5)
            self.cp_widgets[cp_id] = (frame, label)
        else:
            frame, label = self.cp_widgets[cp_id]
            frame.configure(bg=color)
            label.configure(text=text, bg=color)

    def update_gui_loop(self):
        """Procesa eventos de la cola y actualiza la interfaz"""
        try:
            while not gui_queue.empty():
                action, cp_id, state = gui_queue.get_nowait()
                cp = db.get_cp(cp_id)
                if not cp:
                    cp = {"id": cp_id, "state": state}
                else:
                    cp["state"] = state
                self.update_cp_display(cp)
        except Exception as e:
            print(f"[GUI] Error actualizando: {e}")
        finally:
            self.root.after(500, self.update_gui_loop)

# === MAIN ===
def main():
    if len(sys.argv) != 3:
        print("Uso: central.py <puerto> <kafka_ip:port>")
        return

    port = int(sys.argv[1])
    kafka_info = sys.argv[2]

    root = tk.Tk()
    gui = CentralGUI(root)

    threading.Thread(target=server_thread, args=(port,), daemon=True).start()
    root.mainloop()

if __name__ == "__main__":
    main()