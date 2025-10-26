import tkinter as tk
from tkinter import ttk, messagebox
import threading
import queue
import time
from datetime import datetime
from typing import Dict, Any

# IMPORTANT: adjust these imports to match how your project is structured.
# If the GUI lives inside the same package as the central module, use relative imports.
# For simplicity this file assumes top-level imports:
try:
    import config
    import topics
    import kafka_utils
    import db
    import utils
except Exception:
    # fallback: try relative import (when placed inside package)
    from . import config, topics, kafka_utils, db, utils  # type: ignore

from confluent_kafka import Consumer

# GUI constants for state colors
STATE_BG = {
    "ACTIVATED": "#2ecc71",      # green
    "SUPPLYING": "#27ae60",      # darker green
    "OUT_OF_SERVICE": "#e67e22", # orange
    "BROKEN": "#e74c3c",         # red
    "DISCONNECTED": "#95a5a6"    # grey
}

POLL_INTERVAL_MS = 200  # UI loop polling interval


class CPCard(ttk.Frame):
    """Visual card representing a charging point"""
    def __init__(self, parent, cp_id: str, **kwargs):
        super().__init__(parent, relief="raised", borderwidth=2, padding=(6,6))
        self.cp_id = cp_id
        self.configure(style="Card.TFrame")

        self.var_id = tk.StringVar(value=cp_id)
        self.var_loc = tk.StringVar(value="-")
        self.var_price = tk.StringVar(value="0.00€/kWh")
        self.var_state = tk.StringVar(value="DISCONNECTED")

        # dynamic supplying fields
        self.var_kw = tk.StringVar(value="0.00 kW")
        self.var_eur = tk.StringVar(value="0.00 €")
        self.var_driver = tk.StringVar(value="-")

        # Layout
        self.lbl_id = ttk.Label(self, textvariable=self.var_id, font=(None, 12, "bold"))
        self.lbl_id.grid(row=0, column=0, sticky="w")

        self.lbl_loc = ttk.Label(self, textvariable=self.var_loc)
        self.lbl_loc.grid(row=1, column=0, sticky="w")

        self.lbl_price = ttk.Label(self, textvariable=self.var_price)
        self.lbl_price.grid(row=2, column=0, sticky="w")

        self.lbl_state = ttk.Label(self, textvariable=self.var_state, font=(None, 10, "bold"))
        self.lbl_state.grid(row=3, column=0, sticky="w", pady=(4,0))

        # Supplying area (hidden unless state==SUPPLYING)
        self.supplying_frame = ttk.Frame(self)
        self.supplying_frame.grid(row=4, column=0, sticky="we", pady=(6,0))
        ttk.Label(self.supplying_frame, text="kW:").grid(row=0, column=0, sticky="w")
        ttk.Label(self.supplying_frame, textvariable=self.var_kw).grid(row=0, column=1, sticky="w")
        ttk.Label(self.supplying_frame, text="€:").grid(row=1, column=0, sticky="w")
        ttk.Label(self.supplying_frame, textvariable=self.var_eur).grid(row=1, column=1, sticky="w")
        ttk.Label(self.supplying_frame, text="Driver:").grid(row=2, column=0, sticky="w")
        ttk.Label(self.supplying_frame, textvariable=self.var_driver).grid(row=2, column=1, sticky="w")

        # Buttons for manual actions
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=5, column=0, sticky="we", pady=(6,0))
        self.btn_out = ttk.Button(btn_frame, text="Out", width=8, command=self.mark_out)
        self.btn_out.grid(row=0, column=0, padx=(0,6))
        self.btn_recover = ttk.Button(btn_frame, text="Recover", width=8, command=self.recover)
        self.btn_recover.grid(row=0, column=1)

        self.update_visual()

    def update_from_doc(self, doc: Dict[str, Any]):
        self.var_id.set(doc.get("id", self.cp_id))
        self.var_loc.set(doc.get("location", "-"))
        price = doc.get("price_eur_kwh", 0.0)
        self.var_price.set(f"{price:.2f} €/kWh")
        state = doc.get("state", "DISCONNECTED")
        self.var_state.set(state)
        self.update_visual()

    def update_supply(self, kw: float, euros: float, driver_id: str):
        self.var_kw.set(f"{kw:.2f} kW")
        self.var_eur.set(f"{euros:.2f} €")
        self.var_driver.set(driver_id)
        self.var_state.set("SUPPLYING")
        self.update_visual()

    def clear_supply(self):
        self.var_kw.set("0.00 kW")
        self.var_eur.set("0.00 €")
        self.var_driver.set("-")
        if self.var_state.get() == "SUPPLYING":
            self.var_state.set("ACTIVATED")
        self.update_visual()

    def update_visual(self):
        state = self.var_state.get()
        bg = STATE_BG.get(state, STATE_BG["DISCONNECTED"]) if state else STATE_BG["DISCONNECTED"]
        # set background for the frame and children by configuring style dynamically
        style = ttk.Style()
        style_name = f"Card.{self.cp_id}.TFrame"
        style.configure(style_name, background=bg)
        self.configure(style=style_name)
        # Apply background to labels by changing their background via direct config (ttk doesn't support bg well)
        for lbl in (self.lbl_id, self.lbl_loc, self.lbl_price, self.lbl_state):
            try:
                lbl.configure(background=bg)
            except Exception:
                pass

        # Show or hide supplying_frame
        if state == "SUPPLYING":
            self.supplying_frame.grid()
        else:
            self.supplying_frame.grid_remove()

    def mark_out(self):
        # call DB and optionally send notification via kafka
        try:
            db.set_cp_state(self.cp_id, "OUT_OF_SERVICE")
            utils.warn(f"[GUI] CP {self.cp_id} marked OUT_OF_SERVICE from GUI")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to mark CP out: {e}")

    def recover(self):
        try:
            db.set_cp_state(self.cp_id, "ACTIVATED")
            utils.ok(f"[GUI] CP {self.cp_id} recovered from GUI")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to recover CP: {e}")


class EVCentralGUI(tk.Tk):
    def __init__(self, kafka_bootstrap: str = None):
        super().__init__()
        self.title("SD EV Charging Solution - Monitor")
        self.geometry("1100x700")

        # Shared structures
        self.cp_cards: Dict[str, CPCard] = {}
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.msg_queue = queue.Queue()  # queue for messages from background threads

        # Build UI
        self._build_header()
        self._build_cp_grid()
        self._build_right_panel()

        # Start background consumer thread
        self.consumer = kafka_utils.build_consumer(kafka_bootstrap or config.KAFKA_BOOTSTRAP_SERVERS, "gui-monitor", [
            topics.EV_REGISTER,
            topics.EV_HEALTH,
            topics.EV_SUPPLY_REQUEST,
            topics.EV_SUPPLY_TELEMETRY,
            topics.EV_SUPPLY_DONE
        ])
        self._start_kafka_thread()

        # Populate initial CPs from DB
        self._load_initial_cps()

        # Start UI loop polling the message queue
        self.after(POLL_INTERVAL_MS, self._process_queue)

    def _build_header(self):
        header = ttk.Frame(self, padding=(10,10))
        header.pack(side="top", fill="x")
        ttk.Label(header, text="*** SD EV CHARGING SOLUTION. MONITORIZATION PANEL ***", font=(None, 14, "bold")).pack()

    def _build_cp_grid(self):
        frame = ttk.Frame(self, padding=(8,8))
        frame.pack(side="left", fill="both", expand=True)
        self.cp_grid_frame = ttk.Frame(frame)
        self.cp_grid_frame.pack(fill="both", expand=True)

        # Use a canvas+scrollbar for many CPs
        self.canvas = tk.Canvas(self.cp_grid_frame)
        self.scroll = ttk.Scrollbar(self.cp_grid_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scroll.set)
        self.scroll.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.inner_frame = ttk.Frame(self.canvas)
        self.canvas.create_window((0,0), window=self.inner_frame, anchor="nw")
        self.inner_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))

    def _build_right_panel(self):
        right = ttk.Frame(self, width=320, padding=(8,8))
        right.pack(side="right", fill="y")

        # Ongoing drivers (Treeview)
        ttk.Label(right, text="*** ON_GOING DRIVERS REQUESTS ***", font=(None,10,'bold')).pack()
        cols = ("date","start_time","driver_id","cp_id","session")
        self.tree = ttk.Treeview(right, columns=cols, show='headings', height=10)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=60, anchor='center')
        self.tree.pack(fill='x')

        # Application messages
        ttk.Label(right, text="*** APPLICATION MESSAGES ***", font=(None,10,'bold')).pack(pady=(10,0))
        self.msg_list = tk.Listbox(right, height=8)
        self.msg_list.pack(fill='both', expand=True)

        # Central status
        self.central_status = tk.StringVar(value="CENTRAL system status OK")
        ttk.Label(right, textvariable=self.central_status, font=(None,10,'bold')).pack(pady=(6,0))

    def _load_initial_cps(self):
        try:
            for idx, cp in enumerate(db.charging_points.find({})):
                cp_id = cp['id']
                card = CPCard(self.inner_frame, cp_id)
                card.grid(row=idx//2, column=idx%2, padx=6, pady=6, sticky='nsew')
                card.update_from_doc(cp)
                self.cp_cards[cp_id] = card
        except Exception as e:
            utils.err(f"[GUI] Failed to load CPs from DB: {e}")

    def _start_kafka_thread(self):
        t = threading.Thread(target=self._kafka_poll_loop, daemon=True)
        t.start()

    def _kafka_poll_loop(self):
        """Poll Kafka consumer and push events into the UI queue."""
        cons: Consumer = self.consumer
        cons.subscribe([topics.EV_REGISTER, topics.EV_HEALTH, topics.EV_SUPPLY_REQUEST, topics.EV_SUPPLY_TELEMETRY, topics.EV_SUPPLY_DONE])
        while True:
            try:
                msg = cons.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    utils.err(f"[GUI] Kafka error: {msg.error()}")
                    continue
                topic = msg.topic()
                try:
                    data = msg.value()
                    if isinstance(data, (bytes, bytearray)):
                        import json
                        data = json.loads(data.decode('utf-8'))
                except Exception:
                    utils.err("[GUI] Failed to decode Kafka message payload")
                    continue
                # push to queue
                self.msg_queue.put((topic, data))
            except Exception as e:
                utils.err(f"[GUI] Exception in kafka poll loop: {e}")
                time.sleep(1)

    def _process_queue(self):
        processed = 0
        while not self.msg_queue.empty() and processed < 50:
            topic, data = self.msg_queue.get()
            self._handle_message(topic, data)
            processed += 1
        self.after(POLL_INTERVAL_MS, self._process_queue)

    def _handle_message(self, topic: str, data: Dict[str, Any]):
        # mirror logic from your central 'handler' but for UI updates
        if topic == topics.EV_REGISTER:
            cp_id = data["id"]
            doc = {
                "id": cp_id,
                "location": data.get("location","N/A"),
                "price_eur_kwh": data.get("price_eur_kwh", config.DEFAULT_PRICE_EUR_KWH),
                "state": "ACTIVATED",
                "updated_at": datetime.utcnow()
            }
            db.upsert_cp(doc)
            utils.ok(f"[GUI] CP activado: {cp_id}")
            # create card if missing
            if cp_id not in self.cp_cards:
                idx = len(self.cp_cards)
                card = CPCard(self.inner_frame, cp_id)
                card.grid(row=idx//2, column=idx%2, padx=6, pady=6, sticky='nsew')
                card.update_from_doc(doc)
                self.cp_cards[cp_id] = card
            else:
                self.cp_cards[cp_id].update_from_doc(doc)

        elif topic == topics.EV_HEALTH:
            cp_id = data["id"]
            status = data.get("status")
            if cp_id in self.cp_cards:
                card = self.cp_cards[cp_id]
                if status == "OK":
                    db.set_cp_state(cp_id, "ACTIVATED")
                    card.update_from_doc({"id":cp_id, "state":"ACTIVATED"})
                elif status == "KO":
                    db.set_cp_state(cp_id, "BROKEN")
                    card.update_from_doc({"id":cp_id, "state":"BROKEN"})
                elif status == "RECOVERED":
                    db.set_cp_state(cp_id, "ACTIVATED")
                    card.update_from_doc({"id":cp_id, "state":"ACTIVATED"})
                elif status == "SUPPLYING":
                    db.set_cp_state(cp_id, "SUPPLYING")
                    card.update_from_doc({"id":cp_id, "state":"SUPPLYING"})
            self._push_msg(f"Health {cp_id}: {status}")

        elif topic == topics.EV_SUPPLY_REQUEST:
            driver_id = data.get("driver_id")
            cp_id = data.get("cp_id")
            # insert driver and show on ongoing list (start time now)
            db.upsert_driver({"id": driver_id, "name": driver_id})
            if cp_id not in self.cp_cards:
                # create placeholder
                card = CPCard(self.inner_frame, cp_id)
                card.grid(row=len(self.cp_cards)//2, column=len(self.cp_cards)%2, padx=6, pady=6, sticky='nsew')
                self.cp_cards[cp_id] = card
            # authorize in UI: we'll create a session
            session_id = f"{driver_id}-{cp_id}-{int(time.time())}"
            self.active_sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id, "start": time.time(), "kwh":0.0, "eur":0.0}
            self._add_session_row(session_id, driver_id, cp_id)
            # set card to supplying immediately (price fetched from db)
            cp_doc = db.get_cp(cp_id) or {"id":cp_id, "price_eur_kwh": config.DEFAULT_PRICE_EUR_KWH, "location":"N/A", "state":"SUPPLYING"}
            card = self.cp_cards[cp_id]
            card.update_from_doc(cp_doc)
            card.update_supply(0.0, 0.0, driver_id)
            db.set_cp_state(cp_id, "SUPPLYING")
            self._push_msg(f"AUTH OK -> START to CP {cp_id} for driver {driver_id} (session={session_id})")

        elif topic == topics.EV_SUPPLY_TELEMETRY:
            sid = data.get("session_id")
            cp_id = data.get("cp_id")
            kw = float(data.get("kw", 0.0))
            euros = float(data.get("euros", 0.0))
            driver = data.get("driver_id", "-")
            # Update internal session
            if sid in self.active_sessions:
                self.active_sessions[sid]["kwh"] = kw
                self.active_sessions[sid]["eur"] = euros
            # Update CP card
            if cp_id in self.cp_cards:
                self.cp_cards[cp_id].update_supply(kw, euros, driver)
            # Update ongoing drivers table row if present
            self._update_session_row(sid, kw, euros)

        elif topic == topics.EV_SUPPLY_DONE:
            sid = data.get("session_id")
            cp_id = data.get("cp_id")
            total_kwh = float(data.get("total_kwh", 0.0))
            total_eur = float(data.get("total_eur", 0.0))
            self._push_msg(f"FIN suministro: session={sid} kWh={total_kwh:.3f} €={total_eur:.3f}")
            db.set_cp_state(cp_id, "ACTIVATED")
            if cp_id in self.cp_cards:
                self.cp_cards[cp_id].clear_supply()
            # remove session from active table
            self._remove_session_row(sid)
            if sid in self.active_sessions:
                del self.active_sessions[sid]

        else:
            self._push_msg(f"Mensaje no manejado en topic {topic}: {data}")

    # --- UI helpers for sessions and messages
    def _add_session_row(self, session_id, driver_id, cp_id):
        now = datetime.utcnow()
        date = now.strftime('%Y-%m-%d')
        start_time = now.strftime('%H:%M:%S')
        self.tree.insert('', 'end', iid=session_id, values=(date, start_time, driver_id, cp_id, session_id))

    def _update_session_row(self, session_id, kw, euros):
        # update the 'start_time' column to show current consumption (we'll keep columns compact)
        if session_id in self.tree.get_children(''):
            vals = list(self.tree.item(session_id, 'values'))
            # we put kw into start_time and euros into date for quick glance (or extend tree columns in real app)
            vals[1] = f"{kw:.2f}kW"
            vals[0] = f"{euros:.2f}€"
            self.tree.item(session_id, values=vals)

    def _remove_session_row(self, session_id):
        try:
            self.tree.delete(session_id)
        except Exception:
            pass

    def _push_msg(self, text: str):
        ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        self.msg_list.insert(0, f"{ts} - {text}")
        # keep list bounded
        if self.msg_list.size() > 200:
            self.msg_list.delete(200, 'end')


def run_gui():
    app = EVCentralGUI()
    app.mainloop()


if __name__ == '__main__':
    run_gui()

