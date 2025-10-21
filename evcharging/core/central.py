import argparse, threading, socket, time, json
from datetime import datetime
from typing import Dict, Any
from . import config, topics, kafka_utils, db, utils
from confluent_kafka import Producer, Consumer

# Tabla en memoria de sesiones activas: key = (driver_id, cp_id)
active_sessions: Dict[str, Dict[str, Any]] = {} # esto hace falta?

def server_loop(soc):
    while True:
        conn, addr = soc.accept()
        data = conn.recv(1024)
        conn.sendall(b"ACK")

def start_socket_server(host, port):
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # creo que no hace falta, yo lo voy a quitar de momento
    soc.bind((host, port))
    soc.listen()
    utils.info(f"[CENTRAL] TCP listening on {host}:{port}")
    t = threading.Thread(target=server_loop, daemon=True) 
    t.start() 

def main():
    # esto de pillar args yo lo haría de otra manera, full chatGPT
    parser = argparse.ArgumentParser(prog="EV_Central")
    parser.add_argument("--port", type=int, default=config.CENTRAL_PORT)
    parser.add_argument("--kafka", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--mongo", type=str, default=config.MONGO_URI)
    args = parser.parse_args()

    # Socket de escucha (no usado extensivamente en esta release)
    start_socket_server(config.CENTRAL_HOST, args.port)

    # Kafka
    prod = kafka_utils.build_producer(args.kafka)
    cons = kafka_utils.build_consumer(args.kafka, "central-group", [
        topics.EV_REGISTER,
        topics.EV_HEALTH,
        topics.EV_SUPPLY_REQUEST,
        topics.EV_SUPPLY_TELEMETRY,
        topics.EV_SUPPLY_DONE
    ])

    utils.ok("[CENTRAL] Iniciado")
    utils.info("[CENTRAL] Mostrando CPs conocidos (estado inicial = DISCONNECTED hasta que conecten)")
    for cp in db.charging_points.find({}):
        print(f" - CP {cp['id']} @ {cp.get('location','N/A')} price={cp.get('price_eur_kwh',0.3)} state={cp.get('state','DISCONNECTED')}")

    def handler(topic, data):
        if topic == topics.EV_REGISTER: # si se registra un nuevo charging point se muestra y se actualiza
            cp_id = data["id"]
            doc = {
                "id": cp_id,
                "location": data.get("location","N/A"),
                "price_eur_kwh": data.get("price_eur_kwh", config.DEFAULT_PRICE_EUR_KWH),
                "state": "ACTIVATED",
                "updated_at": datetime.utcnow()
            }
            db.upsert_cp(doc)
            utils.ok(f"[CENTRAL] CP registrado/activado: {cp_id}")
        elif topic == topics.EV_HEALTH: # esto es para mandar el estado al monitor
            cp_id = data["id"]
            status = data["status"]  # "OK" | "KO" | "RECOVERED"
            if status == "OK":
                db.set_cp_state(cp_id, "ACTIVATED")
            elif status == "KO":
                db.set_cp_state(cp_id, "BROKEN")
                # Si estaba suministrando, abortar
            elif status == "RECOVERED":
                db.set_cp_state(cp_id, "ACTIVATED")
            utils.warn(f"[CENTRAL] Health {cp_id}: {status}")
        elif topic == topics.EV_SUPPLY_REQUEST:
            driver_id = data["driver_id"]; cp_id = data["cp_id"]
            db.upsert_driver({"id": driver_id, "name": driver_id})
            cp = db.get_cp(cp_id)
            if not cp:
                utils.err(f"[CENTRAL] Supply request a CP inexistente: {cp_id}")
                kafka_utils.send(prod, topics.EV_SUPPLY_AUTH, {"driver_id": driver_id, "cp_id": cp_id, "authorized": False, "reason": "CP not found"})
                return
            if cp.get("state") != "ACTIVATED":
                kafka_utils.send(prod, topics.EV_SUPPLY_AUTH, {"driver_id": driver_id, "cp_id": cp_id, "authorized": False, "reason": f"CP state={cp.get('state')}"})
                return
            # Autorizar
            kafka_utils.send(prod, topics.EV_SUPPLY_AUTH, {"driver_id": driver_id, "cp_id": cp_id, "authorized": True})
            # Orden a Engine para empezar
            session_id = f"{driver_id}-{cp_id}-{int(time.time())}"
            active_sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id, "start": time.time(), "kwh": 0.0, "eur": 0.0}
            kafka_utils.send(prod, topics.EV_SUPPLY_START, {"session_id": session_id, "cp_id": cp_id, "driver_id": driver_id, "price": cp.get("price_eur_kwh", 0.3)})
            utils.ok(f"[CENTRAL] AUTH OK -> START to CP {cp_id} for driver {driver_id} (session={session_id})")
        elif topic == topics.EV_SUPPLY_TELEMETRY:
            sid = data["session_id"]
            # Mostrar en "panel"
            print(f"[CENTRAL] CP {data['cp_id']} SUPPLYING kw={data['kw']:.2f} €={data['euros']:.2f} driver={data['driver_id']} session={sid}")
        elif topic == topics.EV_SUPPLY_DONE:
            sid = data["session_id"]
            utils.ok(f"[CENTRAL] FIN suministro: session={sid} kWh={data['total_kwh']:.3f} €={data['total_eur']:.3f}")
        else:
            print(f"[CENTRAL] Mensaje no manejado en topic {topic}: {data}")

    kafka_utils.poll_loop(cons, handler)

if __name__ == "__main__":
    main()