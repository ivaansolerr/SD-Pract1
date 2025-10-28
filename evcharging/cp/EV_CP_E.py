import threading, socket, time, sys, json, random
from typing import Tuple
from evcharging import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

KO_FLAG = False      # 'k' -> KO, 'r' -> RECOVER
AUTH_OK = False      # True cuando CENTRAL aprueba la autenticación

ENGINE_LISTEN_IP = "0.0.0.0"
ENGINE_PORT = 7100

def heartbeat_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((ENGINE_LISTEN_IP, ENGINE_PORT))
    srv.listen(5)
    utils.info(f"[ENGINE] Heartbeat TCP on {ENGINE_LISTEN_IP}:{ENGINE_PORT}")

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

def main():
    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <broker_ip> <broker_port>")
        sys.exit(1)

    broker_ip = sys.argv[1]
    broker_port = sys.argv[2]
    bootstrap_servers = f"{broker_ip}:{broker_port}"

    utils.ok(f"[ENGINE] Broker configurado: {bootstrap_servers}")

    heartbeat_server()
    
    # --- 🔸 Crear productor y consumidor Kafka usando el broker proporcionado ---
    prod = kafka_utils.build_producer(bootstrap_servers)
    cons = kafka_utils.build_consumer(
    bootstrap_servers,
    "engine-group",
    [
        topics.EV_SUPPLY_START,    # inicio de suministro
        topics.EV_COMMANDS,        # comandos de Central
        topics.EV_SUPPLY_AUTH,     # autorización de suministro (si aplica)
        topics.EV_AUTH_REQUEST,    # solicitud de autenticación
        topics.EV_AUTH_RESULT,     # resultado de autenticación
        topics.EV_AUTH_RESULT_ENG  # resultado de autenticación del engine
    ]
)
    utils.ok(f"[ENGINE] Iniciado en {ENGINE_LISTEN_IP}:{ENGINE_PORT}, esperando autenticación...")

    current_session = None
    cp_id = None

    def handler(topic, data):
        nonlocal current_session, cp_id
        global AUTH_OK, KO_FLAG

        # Auth con monitor
        if topic == topics.EV_AUTH_RESULT_ENG:
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
                "price": float(data.get("price", 0.30)), #cambiar esto
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
            elif cmd == "KO":
                KO_FLAG = True
                utils.err("[ENGINE] Marcado como KO")
            elif cmd == "RESUME":
                KO_FLAG = False
                utils.ok("[ENGINE] RESUME (RECOVERED)")

    kafka_utils.poll_loop(cons, handler)

if __name__ == "__main__":
    main()