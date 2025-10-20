import argparse, threading, socket, time, sys, json, random
from typing import Tuple
from . import config, topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

KO_FLAG = False  # pulsar 'k' para KO, 'r' para recover

def heartbeat_server(host: str, port: int) -> None:
    # Servidor TCP: responde "OK" o "KO" al monitor
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(5)
    utils.info(f"[ENGINE] Heartbeat TCP on {host}:{port}")
    def _loop():
        global KO_FLAG
        while True:
            conn, addr = srv.accept()
            with conn:
                data = conn.recv(32)
                if KO_FLAG:
                    conn.sendall(b"KO")
                else:
                    conn.sendall(b"OK")
    threading.Thread(target=_loop, daemon=True).start()

def input_loop():
    global KO_FLAG
    utils.warn("[ENGINE] Pulsa 'k'+Enter para KO, 'r'+Enter para recuperar")
    for line in sys.stdin:
        line=line.strip().lower()
        if line == "k":
            KO_FLAG = True
            utils.err("[ENGINE] Estado -> KO (simulado)")
        elif line == "r":
            KO_FLAG = False
            utils.ok("[ENGINE] Estado -> RECOVERED (simulado)")

def main():
    parser = argparse.ArgumentParser(prog="EV_CP_E")
    parser.add_argument("--id", required=True, help="ID del CP (debe coincidir con el de su Monitor)")
    parser.add_argument("--kafka", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--host", type=str, default=config.CP_ENGINE_HOST)
    parser.add_argument("--port", type=int, default=config.CP_ENGINE_PORT)
    args = parser.parse_args()

    heartbeat_server(args.host, args.port)
    threading.Thread(target=input_loop, daemon=True).start()

    prod: Producer = kafka_utils.build_producer(args.kafka)
    cons: Consumer = kafka_utils.build_consumer(args.kafka, f"engine-{args.id}", [topics.EV_SUPPLY_START, topics.EV_COMMANDS])

    utils.ok(f"[ENGINE {args.id}] Iniciado y a la espera de órdenes")

    current_session = None

    def handler(topic: str, data: dict):
        nonlocal current_session
        if topic == topics.EV_SUPPLY_START and data["cp_id"] == args.id:
            if KO_FLAG:
                utils.err(f"[ENGINE {args.id}] No puedo empezar (KO)")
                return
            current_session = {
                "session_id": data["session_id"],
                "driver_id": data["driver_id"],
                "price": float(data.get("price", 0.3)),
                "kwh": 0.0, "eur": 0.0
            }
            utils.ok(f"[ENGINE {args.id}] START session={current_session['session_id']} price={current_session['price']}")
            # bucle de suministro en hilo
            def supply_loop():
                while current_session and not KO_FLAG:
                    # Simulación de consumo (kW instantáneo) ~ 5-30kW, acumula kWh cada segundo/3600
                    kw = random.uniform(5.0, 30.0)
                    current_session["kwh"] += kw/3600.0
                    current_session["eur"] = current_session["kwh"] * current_session["price"]
                    kafka_utils.send(prod, topics.EV_SUPPLY_TELEMETRY, {
                        "session_id": current_session["session_id"],
                        "cp_id": args.id,
                        "driver_id": current_session["driver_id"],
                        "kw": kw,
                        "euros": current_session["eur"]
                    })
                    time.sleep(1.0)
                if current_session:
                    # Fin normal (simulamos 20s) o KO rompe el loop
                    kafka_utils.send(prod, topics.EV_SUPPLY_DONE, {
                        "session_id": current_session["session_id"],
                        "cp_id": args.id,
                        "driver_id": current_session["driver_id"],
                        "total_kwh": current_session["kwh"],
                        "total_eur": current_session["eur"],
                        "reason": "KO" if KO_FLAG else "FINISHED"
                    })
            threading.Thread(target=supply_loop, daemon=True).start()
        elif topic == topics.EV_COMMANDS and data.get("cp_id") == args.id:
            cmd = data.get("cmd")
            if cmd == "STOP_SUPPLY" and current_session:
                utils.warn(f"[ENGINE {args.id}] STOP by CENTRAL")
                # terminar el loop
                tmp = current_session
                current_session = None
                kafka_utils.send(prod, topics.EV_SUPPLY_DONE, {
                    "session_id": tmp["session_id"],
                    "cp_id": args.id,
                    "driver_id": tmp["driver_id"],
                    "total_kwh": tmp["kwh"],
                    "total_eur": tmp["eur"],
                    "reason": "STOP_BY_CENTRAL"
                })
            elif cmd == "OUT_OF_ORDER":
                utils.warn(f"[ENGINE {args.id}] Marked OUT_OF_ORDER")
            elif cmd == "RESUME":
                utils.ok(f"[ENGINE {args.id}] RESUME")
        else:
            pass

    kafka_utils.poll_loop(cons, handler)

if __name__ == "__main__":
    main()