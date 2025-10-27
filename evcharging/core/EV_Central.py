import argparse, threading, socket, time, json, sys
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils
from . import db
from confluent_kafka import Producer, Consumer

# Tabla en memoria de sesiones activas: key = (driver_id, cp_id)
active_sessions: Dict[str, Dict[str, Any]] = {} # esto hace falta?

def server_loop(soc):
    while True:
        conn, addr = soc.accept()
        data = conn.recv(1024)
        conn.sendall(b"ACK")

def monitor_sessions(prod):
    """Hilo que vigila las sesiones activas y las cierra si superan 15s."""
    while True:
        now = time.time()
        expired = []

        for sid, session in list(active_sessions.items()):
            duration = now - session["start"]
            if duration > 15:  # 15 segundos de suministro
                cp_id = session["cp_id"]
                driver_id = session["driver_id"]
                utils.warn(f"[CENTRAL] Tiempo máximo superado en {sid} ({duration:.1f}s). Terminando sesión.")

                # Enviar fin de suministro
                kafka_utils.send(prod, topics.EV_SUPPLY_DONE, {
                    "session_id": sid,
                    "cp_id": cp_id,
                    "driver_id": driver_id,
                    "total_kwh": session["kwh"],
                    "total_eur": session["eur"]
                })

                # Cambiar estado en BD
                db.set_cp_state(cp_id, "ACTIVATED")

                # Marcar para eliminar
                expired.append(sid)

        # Eliminar sesiones terminadas
        for sid in expired:
            del active_sessions[sid]

        time.sleep(1)  # comprobar cada segundo

def start_socket_server(host, port):
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # creo que no hace falta, yo lo voy a quitar de momento
    soc.bind((host, port))
    soc.listen()
    utils.info(f"[CENTRAL] TCP listening on {host}:{port}")
    t = threading.Thread(target=server_loop, args=(soc,), daemon=True) 
    t.start() 

def handler(topic, data):
        if topic == topics.EV_REGISTER: # si se registra un nuevo charging point se muestra y se actualiza
            cp_id = data["id"]
            doc = {
                "id": cp_id,
                "location": data.get("location","N/A"),
                "price_eur_kwh": data.get("price_eur_kwh", "0.30"),
                "state": "ACTIVATED",
                "updated_at": datetime.utcnow()
            }
            db.upsert_cp(doc)
            utils.ok(f"[CENTRAL] CP activado: {cp_id}")
        elif topic == topics.EV_HEALTH: # esto es para mandar el estado al monitor
            cp_id = data["id"]
            status = data["status"]  # "OK" | "KO" | "RECOVERED" | "SUPPLYING"
            if status == "OK":
                db.set_cp_state(cp_id, "ACTIVATED")
            elif status == "KO":
                db.set_cp_state(cp_id, "BROKEN")
                # Si estaba suministrando, abortar
            elif status == "RECOVERED":
                db.set_cp_state(cp_id, "ACTIVATED")
            elif status == "SUPPLYING":
                db.set_cp_state(cp_id, "SUPPLYING")
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
            db.set_cp_state(cp_id, "SUPPLYING")
        elif topic == topics.EV_SUPPLY_TELEMETRY:
            sid = data["session_id"]
            # Mostrar en "panel"
            print(f"[CENTRAL] CP {data['cp_id']} SUPPLYING kw={data['kw']:.2f} €={data['euros']:.2f} driver={data['driver_id']} session={sid}")
        elif topic == topics.EV_SUPPLY_DONE:
            sid = data["session_id"]
            utils.ok(f"[CENTRAL] FIN suministro: session={sid} kWh={data['total_kwh']:.3f} €={data['total_eur']:.3f}")
            db.set_cp_state(data["cp_id"], "ACTIVATED")
        else:
            print(f"[CENTRAL] Mensaje no manejado en topic {topic}: {data}")

def main():
    
    if len(sys.argv) != 3:
        print("Uso: python EV_Central.py <port> <kafka_bootstrap_servers>")
        sys.exit(1)

    central_port = int(sys.argv[1])
    kafka = sys.argv[2]

    # Socket de escucha para la auth con el cp
    start_socket_server("0.0.0.0", central_port)

    # Kafka
    prod = kafka_utils.build_producer(kafka)
    threading.Thread(target=monitor_sessions, args=(prod,), daemon=True).start() # hilo para monitorizar sesiones
    cons = kafka_utils.build_consumer(kafka, "central-group", [
        topics.EV_REGISTER,
        topics.EV_HEALTH,
        topics.EV_SUPPLY_REQUEST,
        topics.EV_SUPPLY_TELEMETRY,
        topics.EV_SUPPLY_DONE
    ])

    utils.ok("[CENTRAL] Iniciado")
    utils.info("[CENTRAL] Mostrando CPs conocidos (estado inicial = DISCONNECTED hasta que conecten)")
    for cp in db.charging_points.find({}):
        print(f" - CP {cp['id']} @ {cp.get('location','N/A')} price={cp.get('price_eur_kwh',0.3)}€ state={cp.get('state','DISCONNECTED')}")

    kafka_utils.poll_loop(cons, handler)

if __name__ == "__main__":
    main()