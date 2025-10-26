import threading, socket, time, sys, json, random
from typing import Tuple
from . import config, topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

KO_FLAG = False      # 'k' -> KO, 'r' -> RECOVER
AUTH_OK = False      # True cuando CENTRAL aprueba la autenticación

def heartbeat_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((config.CP_ENGINE_HOST, config.CP_ENGINE_PORT))
    srv.listen(5)
    utils.info(f"[ENGINE] Heartbeat TCP on {config.CP_ENGINE_HOST}:{config.CP_ENGINE_PORT}")

    def _loop():
        global KO_FLAG, AUTH_OK
        while True:
            conn, _ = srv.accept()
            with conn:
                _ = conn.recv(32)  # “PING”
                if AUTH_OK and not KO_FLAG:
                    conn.sendall(b"OK")
                else:
                    conn.sendall(b"KO")
    threading.Thread(target=_loop, daemon=True).start()

def input_loop():
    global KO_FLAG
    utils.warn("[ENGINE] Pulsa 'k'+Enter para KO, 'r'+Enter para RECOVER")
    for line in sys.stdin:
        line = line.strip().lower()
        if line == "k":
            KO_FLAG = True
            utils.err("[ENGINE] Estado -> KO (simulado)")
        elif line == "r":
            KO_FLAG = False
            utils.ok("[ENGINE] Estado -> RECOVERED (simulado)")

def main():
    heartbeat_server()
    threading.Thread(target=input_loop, daemon=True).start()

    prod = kafka_utils.build_producer(config.KAFKA_BOOTSTRAP_SERVERS)
    cons = kafka_utils.build_consumer(
        config.KAFKA_BOOTSTRAP_SERVERS,
        "engine-group",
        [topics.EV_AUTH_RESULT, topics.EV_SUPPLY_START, topics.EV_COMMANDS]
    )

    utils.ok(f"[ENGINE] Iniciado en {config.CP_ENGINE_HOST}:{config.CP_ENGINE_PORT}, esperando autenticación...")

    current_session = None
    cp_id = None

    def handler(topic, data):
        nonlocal current_session, cp_id
        global AUTH_OK, KO_FLAG

        # --- Autenticación ---
        if topic == topics.EV_AUTH_RESULT:
            if data.get("status") == "APPROVED":
                AUTH_OK = True
                cp_id = data.get("cp_id")
                utils.ok(f"[ENGINE] Autenticado por CENTRAL (CP={cp_id})")
            else:
                AUTH_OK = False
                utils.err("[ENGINE] Autenticación DENEGADA")

        # --- Inicio de suministro ---
        elif topic == topics.EV_SUPPLY_START and AUTH_OK:
            if KO_FLAG:
                utils.err("[ENGINE] No puedo empezar (KO)")
                return
            if cp_id != data.get("cp_id"):
                return  # ignorar sesiones de otro CP

            current_session = {
                "session_id": data["session_id"],
                "driver_id": data["driver_id"],
                "price": float(data.get("price", config.DEFAULT_PRICE_EUR_KWH)),
                "kwh": 0.0,
                "eur": 0.0,
            }
            utils.ok(f"[ENGINE] START session={current_session['session_id']} price={current_session['price']}")

            def supply_loop():
                while current_session and not KO_FLAG:
                    kw = random.uniform(5.0, 30.0)
                    current_session["kwh"] += kw / 3600.0
                    current_session["eur"] = current_session["kwh"] * current_session["price"]

                    kafka_utils.send(prod, topics.EV_SUPPLY_TELEMETRY, {
                        "session_id": current_session["session_id"],
                        "cp_id": cp_id,
                        "driver_id": current_session["driver_id"],
                        "kw": kw,
                        "euros": current_session["eur"],
                    })
                    time.sleep(1.0)

                # Fin del suministro
                if current_session:
                    kafka_utils.send(prod, topics.EV_SUPPLY_DONE, {
                        "session_id": current_session["session_id"],
                        "cp_id": cp_id,
                        "driver_id": current_session["driver_id"],
                        "total_kwh": current_session["kwh"],
                        "total_eur": current_session["eur"],
                        "reason": "KO" if KO_FLAG else "FINISHED"
                    })

            threading.Thread(target=supply_loop, daemon=True).start()

        # --- Comandos de CENTRAL ---
        elif topic == topics.EV_COMMANDS and AUTH_OK:
            if data.get("cp_id") != cp_id:
                return
            cmd = data.get("cmd")
            if cmd == "STOP_SUPPLY" and current_session:
                utils.warn("[ENGINE] STOP by CENTRAL")
                tmp = current_session
                current_session = None
                kafka_utils.send(prod, topics.EV_SUPPLY_DONE, {
                    "session_id": tmp["session_id"],
                    "cp_id": cp_id,
                    "driver_id": tmp["driver_id"],
                    "total_kwh": tmp["kwh"],
                    "total_eur": tmp["eur"],
                    "reason": "STOP_BY_CENTRAL",
                })
            elif cmd == "OUT_OF_ORDER":
                KO_FLAG = True
                utils.err("[ENGINE] Marcado como OUT_OF_ORDER (KO)")
            elif cmd == "RESUME":
                KO_FLAG = False
                utils.ok("[ENGINE] RESUME (RECOVERED)")

    kafka_utils.poll_loop(cons, handler)

if __name__ == "__main__":
    main()
