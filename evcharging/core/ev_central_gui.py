# central_gui.py
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import socket
import json
import time
from datetime import datetime, timezone

# IMPORTS compatibles con tu estructura de paquete (igual que central.py)
from . import db
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

# ------------------------------
# Config / estado global
# ------------------------------
STATE_COLORS = {
    "AVAILABLE": "#4CAF50",
    "SUPPLYING": "#2E7D32",
    "UNAVAILABLE": "#FF5252",
    "OUT_OF_SERVICE": "#FF8C00",
    "OFFLINE": "#BDBDBD",
    None: "#BDBDBD"
}

# Estructuras locales para la GUI (copia/reflect del estado real)
cp_cache = {}            # cp_id -> cp dict (mirror de db)
driver_requests = []     # lista de tuples (date, time, user_id, cp_id)
system_messages = []     # líneas de mensajes
active_sessions = {}     # session_id -> {driver_id, cp_id, start_time}

# GUI root
root = tk.Tk()
root.title("SD EV CHARGING — CENTRAL MONITOR PANEL")
root.geometry("1200x840")
root.config(bg="#1f3b57")

# ---------- Helpers de UI ----------

def add_system_message(msg: str):
    """Añade un mensaje nuevo y actualiza caja de mensajes"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} - {msg}"
    system_messages.append(line)
    # mantén solo 200 mensajes
    if len(system_messages) > 200:
        system_messages.pop(0)
    # UI update via after
    root.after(0, lambda: _ui_append_message(line))

def _ui_append_message(line: str):
    msg_box.configure(state="normal")
    msg_box.insert(tk.END, line + "\n")
    msg_box.see(tk.END)
    msg_box.configure(state="disabled")

def add_driver_request(date_s, time_s, user_id, cp_id):
    driver_requests.append((date_s, time_s, user_id, cp_id))
    root.after(0, lambda: req_table.insert("", tk.END, values=(date_s, time_s, user_id, cp_id)))

def refresh_cp_cache_from_db():
    """Lee la DB y actualiza cp_cache"""
    try:
        cps = db.listChargingPoints() or []
    except Exception as e:
        add_system_message(f"ERROR leyendo DB: {e}")
        cps = []

    new_cache = {}
    for cp in cps:
        cid = cp.get("id")
        new_cache[cid] = cp.copy()
    # replace cache atomically
    cp_cache.clear()
    cp_cache.update(new_cache)
    # schedule UI refresh
    root.after(0, draw_cp_grid)

def set_cp_state(cp_id, state):
    """Actualiza DB y cache y UI"""
    try:
        db.setCpState(cp_id, state)
    except Exception:
        # fallback to upsert
        try:
            db.upsertCp({"id": cp_id, "state": state})
        except Exception as e:
            add_system_message(f"ERROR actualizando estado DB para {cp_id}: {e}")

    # update local cache if present
    if cp_id in cp_cache:
        cp_cache[cp_id]["state"] = state
    else:
        # add lightweight entry
        cp_cache[cp_id] = {"id": cp_id, "location": "N/A", "price_eur_kwh": "N/A", "state": state}

    root.after(0, draw_cp_grid)

# ---------- GUI Layout ----------

title = tk.Label(root, text="*** SD EV CHARGING SOLUTION. MONITOR PANEL ***",
                 bg="#1f3b57", fg="white", font=("Arial", 18, "bold"))
title.pack(pady=8)

# Frame superior: grid de CPs
grid_frame = tk.Frame(root, bg="#1f3b57")
grid_frame.pack(pady=6, fill="x")

# Canvas inside scrollable frame to hold CP cards
canvas = tk.Canvas(grid_frame, height=300, bg="#1f3b57", highlightthickness=0)
hbar = tk.Scrollbar(grid_frame, orient=tk.HORIZONTAL, command=canvas.xview)
canvas.configure(xscrollcommand=hbar.set)
hbar.pack(side=tk.BOTTOM, fill=tk.X)
canvas.pack(side=tk.TOP, fill="both", expand=True)

cards_frame = tk.Frame(canvas, bg="#1f3b57")
canvas.create_window((0,0), window=cards_frame, anchor="nw")

def on_cards_config(event):
    canvas.configure(scrollregion=canvas.bbox("all"))

cards_frame.bind("<Configure>", on_cards_config)

# Middle: driver requests (table)
req_label = tk.Label(root, text="*** ON GOING DRIVERS REQUESTS ***", bg="#1f3b57", fg="white",
                     font=("Arial", 14, "bold"))
req_label.pack(pady=(8,0))

req_frame = tk.Frame(root)
req_frame.pack(pady=4, fill="x")

req_table = ttk.Treeview(req_frame, columns=("Date", "Time", "User", "CP"), show="headings", height=6)
for h, w in (("Date", 110), ("Time", 80), ("User", 80), ("CP", 100)):
    req_table.heading(h, text=h)
    req_table.column(h, width=w)
req_table.pack(side=tk.LEFT, fill="x", expand=True)

req_scroll = tk.Scrollbar(req_frame, orient=tk.VERTICAL, command=req_table.yview)
req_table.configure(yscrollcommand=req_scroll.set)
req_scroll.pack(side=tk.RIGHT, fill=tk.Y)

# Bottom: messages and controls
bottom_frame = tk.Frame(root, bg="#1f3b57")
bottom_frame.pack(pady=8, fill="both", expand=True)

msg_label = tk.Label(bottom_frame, text="*** APPLICATION MESSAGES ***", bg="#1f3b57", fg="white",
                     font=("Arial", 14, "bold"))
msg_label.pack()

msg_box = scrolledtext.ScrolledText(bottom_frame, height=8, state="disabled", font=("Arial", 11))
msg_box.pack(fill="both", padx=6, pady=6, expand=True)

# ---------- UI: drawing CP cards ----------

card_widgets = {}  # cp_id -> frame

def draw_cp_grid():
    # destroy widgets not in cache
    for w in cards_frame.winfo_children():
        w.destroy()

    # Layout: fixed number of columns (e.g., 5)
    cols = 5
    idx = 0
    # Ensure deterministic order (by id)
    for cid in sorted(cp_cache.keys()):
        cp = cp_cache[cid]
        state = cp.get("state", None)
        color = STATE_COLORS.get(state, STATE_COLORS[None])
        # Create card frame
        card = tk.Frame(cards_frame, bg=color, bd=1, relief="raised", padx=8, pady=6, width=200, height=120)
        card.grid_propagate(False)
        r = idx // cols
        c = idx % cols
        card.grid(row=r, column=c, padx=6, pady=6)
        # ID
        tk.Label(card, text=cp.get("id", cid), bg=color, fg="white", font=("Arial", 12, "bold")).pack(anchor="w")
        # Location (may be long)
        tk.Label(card, text=cp.get("location", "N/A"), bg=color, fg="white", font=("Arial", 10)).pack(anchor="w")
        # Price
        price = cp.get("price_eur_kwh")
        price_text = f"{price} €/kWh" if price is not None else "N/A"
        tk.Label(card, text=price_text, bg=color, fg="white", font=("Arial", 10)).pack(anchor="w")
        # Extra: if supplying show driver id
        if state == "SUPPLYING":
            # try to find driver id in active_sessions
            driver_id = None
            for sid, s in active_sessions.items():
                if s.get("cp_id") == cid:
                    driver_id = s.get("driver_id")
                    break
            txt = f"Driver: {driver_id}" if driver_id else "Driver: -"
            tk.Label(card, text=txt, bg=color, fg="white", font=("Arial", 10, "italic")).pack(anchor="w", pady=(6,0))
        idx += 1

# ---------- Networking: TCP server to accept monitors ----------

def handle_monitor_connection(conn, addr):
    """Implement handshake similarly a central.handleClient but simplified for monitor messages"""
    try:
        add_system_message(f"Conexión entrante desde monitor {addr}")
        # Expect <ENC> handshake
        start = conn.recv(1024)
        if start != b"<ENC>":
            # possibility: monitor sending a quick CENTRAL_DOWN before handshake
            # Accept also immediate status messages (we will log and close)
            if start in (b"CENTRAL_DOWN", b"CENTRAL_UP_AGAIN"):
                add_system_message(f"Mensaje prematuro desde {addr}: {start.decode(errors='ignore')}")
                conn.close()
                return
            else:
                add_system_message(f"Handshake esperado <ENC>, recibido: {start!r}")
                conn.close()
                return
        conn.send(socketCommunication.ACK)
        # Expect CP_ID framed
        raw = conn.recv(1024)
        cp_id = socketCommunication.parseFrame(raw)
        if cp_id is None:
            conn.send(socketCommunication.NACK)
            conn.close()
            return
        # Mark cp available
        add_system_message(f"Monitor registrado para CP {cp_id}")
        db.upsertCp({"id": cp_id, "state": "AVAILABLE"})
        # Continue handshake
        conn.send(socketCommunication.ACK)
        conn.send(socketCommunication.encodeMess("OK"))
        # wait final ack
        if conn.recv(1024) != socketCommunication.ACK:
            conn.close()
            return
        conn.send(b"<EOT>")
        add_system_message(f"Handshake completado con CP {cp_id}")

        # Heartbeat loop
        conn.settimeout(10)
        while True:
            try:
                msg = conn.recv(1024)
                if not msg:
                    break
                # Handle standard messages
                if msg == b"PING":
                    conn.send(b"PONG")
                elif msg == b"KO":
                    add_system_message(f"CP {cp_id} reports KO (avería).")
                    set_cp_state(cp_id, "UNAVAILABLE")
                    # ack
                    try:
                        conn.send(socketCommunication.ACK)
                    except Exception:
                        pass
                elif msg == b"OK":
                    add_system_message(f"CP {cp_id} reports OK (recuperado).")
                    set_cp_state(cp_id, "AVAILABLE")
                    try:
                        conn.send(socketCommunication.ACK)
                    except Exception:
                        pass
                elif msg == b"CENTRAL_DOWN":
                    add_system_message(f"Monitor indica CENTRAL_DOWN for CP {cp_id}")
                    # In your central the meaning is: central is down (for that monitor), we mark CP OUT_OF_SERVICE
                    set_cp_state(cp_id, "OUT_OF_SERVICE")
                    try:
                        conn.send(socketCommunication.ACK)
                    except Exception:
                        pass
                elif msg == b"CENTRAL_UP_AGAIN":
                    add_system_message(f"Monitor indicates CENTRAL_UP_AGAIN for CP {cp_id}")
                    set_cp_state(cp_id, "AVAILABLE")
                    try:
                        conn.send(socketCommunication.ACK)
                    except Exception:
                        pass
                else:
                    add_system_message(f"Mensaje desconocido de {cp_id}: {msg!r}")
            except socket.timeout:
                continue
            except Exception as e:
                add_system_message(f"Error en conexión monitor {addr}: {e}")
                break
    finally:
        try:
            conn.close()
        except Exception:
            pass
        add_system_message(f"Monitor {addr} desconectado")

def tcp_server_thread(listen_port):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", int(listen_port)))
    s.listen(5)
    add_system_message(f"TCP server listening on port {listen_port}")
    while True:
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_monitor_connection, args=(conn, addr), daemon=True).start()
        except Exception as e:
            add_system_message(f"Error accept socket: {e}")
            time.sleep(1)

# ---------- Kafka listener thread ----------

def kafka_listener_thread(kafka_info):
    """
    Subscribes to driver topics and updates GUI state:
      - EV_SUPPLY_REQUEST -> add to driver_requests and log
      - EV_SUPPLY_CONNECTED -> if status CONNECTED set CP to SUPPLYING, save active session, else set rejected
      - EV_SUPPLY_END -> set CP AVAILABLE and produce ticket (we only log)
    """
    producer = kafka_utils.buildProducer(kafka_info)
    consumer = kafka_utils.buildConsumer(kafka_info, "central-gui", [
        topics.EV_SUPPLY_REQUEST,
        topics.EV_SUPPLY_CONNECTED,
        topics.EV_SUPPLY_END
    ])
    add_system_message("Kafka listener started")
    while True:
        try:
            msg = consumer.poll(0.5)
            if not msg or msg.error():
                continue
            topic = msg.topic()
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                add_system_message(f"Malformed kafka message on {topic}")
                continue

            if topic == topics.EV_SUPPLY_REQUEST:
                driver_id = payload.get("driver_id")
                cp_id = payload.get("cp_id")
                ts = datetime.now().strftime("%d/%m/%y")
                tm = datetime.now().strftime("%H:%M")
                add_driver_request(ts, tm, driver_id, cp_id)
                add_system_message(f"EV_SUPPLY_REQUEST from driver {driver_id} for {cp_id}")

            elif topic == topics.EV_SUPPLY_CONNECTED:
                driver_id = payload.get("driver_id")
                cp_id = payload.get("cp_id")
                status = payload.get("status")
                if status == "CONNECTED":
                    set_cp_state(cp_id, "SUPPLYING")
                    session_id = f"{driver_id}_{cp_id}"
                    active_sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id, "start_time": datetime.now(timezone.utc)}
                    add_system_message(f"ENGINE reported CONNECTED -> {cp_id} supplying driver {driver_id}")
                else:
                    add_system_message(f"ENGINE reported REJECTED for {driver_id}@{cp_id}")

            elif topic == topics.EV_SUPPLY_END:
                driver_id = payload.get("driver_id")
                cp_id = payload.get("cp_id")
                set_cp_state(cp_id, "AVAILABLE")
                # close session if exists
                sid = f"{driver_id}_{cp_id}"
                active_sessions.pop(sid, None)
                add_system_message(f"EV_SUPPLY_END for {driver_id} @ {cp_id}")

        except Exception as e:
            add_system_message(f"Kafka listener error: {e}")
            time.sleep(1)

# ---------- Background refresher ----------

def periodic_refresh():
    # Pull fresh CP list from DB and redraw
    try:
        refresh_cp_cache_from_db()
    except Exception as e:
        add_system_message(f"Refresh error: {e}")
    # schedule next refresh in 5s
    root.after(5000, periodic_refresh)

# ---------- Init UI and start threads ----------

def start_background_services(tcp_port, kafka_info):
    # initial load
    refresh_cp_cache_from_db()

    # start tcp server to receive monitor connections
    t_tcp = threading.Thread(target=tcp_server_thread, args=(tcp_port,), daemon=True)
    t_tcp.start()

    # start kafka listener
    t_kafka = threading.Thread(target=kafka_listener_thread, args=(kafka_info,), daemon=True)
    t_kafka.start()

# ---------- Entrypoint UI bindings ----------

# Placeholders filled by code above
draw_cp_grid()
# Pack requests initial (empty)
# req_table already exists; populate if any
for r in driver_requests:
    req_table.insert("", tk.END, values=r)

# Start background services (use same args as central.py)
# Example usage: central_gui.main( <tcp_port>, "<kafka_ip>:<port>" )
def main(tcp_port=9000, kafka_info="127.0.0.1:9092"):
    # start bg tasks
    start_background_services(tcp_port, kafka_info)
    # schedule periodic refresh
    root.after(2000, periodic_refresh)
    root.mainloop()

# Allow running as script (adjust import path accordingly if needed)
if __name__ == "__main__":
    # default values; override via argv if called directly
    import sys
    port = 9000
    kafka = "127.0.0.1:9092"
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])
    if len(sys.argv) >= 3:
        kafka = sys.argv[2]
    main(port, kafka)