import socket, sys
import threading, time
import threading
from typing import Dict, Any
from datetime import datetime, timezone
from . import db
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"
active_sessions: Dict[str, Dict[str, Any]] = {}

def xor_checksum(data: bytes) -> bytes:
    lrc = 0
    for b in data: lrc ^= b
    return bytes([lrc])

def encodeMess(msg) -> bytes:
    data = msg.encode()
    return STX + data + ETX + xor_checksum(data)

def print_cp_panel():
    cps = db.list_charging_points()
    print("\n=== Charging Points en Sistema ===")
    if not cps:
        print("No hay puntos de carga registrados.")
        return

    for cp in cps:
        print(
            f"ID: {cp.get('id')} | "
            f"State: {cp.get('state', 'UNKNOWN')} | "
            f"Price: {cp.get('price_eur_kwh', 'N/A')} eur | "
            f"Location: {cp.get('location', 'N/A')}"
        )

def parse_frame(frame):
    if len(frame) < 3 or frame[0] != 2 or frame[-2] != 3:
        return None
    data = frame[1:-2]
    return data.decode() if xor_checksum(data) == frame[-1:] else None

def cp_exists(cp_id):
    cp = db.get_cp(cp_id)  # consulta real a Mongo
    return cp is not None

def handle_client(conn, addr):
    print(f"[CENTRAL] Nueva conexión desde {addr}")

    try:
        if conn.recv(1024) != b"<ENC>":
            conn.close()
            return
        conn.send(ACK)

        cp = parse_frame(conn.recv(1024))
        if cp is None:
            conn.send(NACK)
            conn.close()
            return

        print(f"[CENTRAL] Solicitud por CP: {cp}")
        conn.send(ACK)

        if cp_exists(cp):
            print(f"[CENTRAL] CP :) {cp} autenticado. Estado → AVAILABLE")
            db.upsert_cp({
                "id": cp,
                "state": "AVAILABLE",
                #"last_seen": datetime.now(timezone.utc)
            })
            result = "OK"
        else:
            print(f"[CENTRAL] CP {cp} NO existe en la base de datos")
            result = "NO"

        conn.send(encodeMess(result))

        if conn.recv(1024) != ACK:
            conn.close()
            return

        conn.send(b"<EOT>")
        print(f"[CENTRAL] Handshake completado con CP {cp}, esperando mensajes...")

        conn.settimeout(10)
        while True:
            try:
                msg = conn.recv(1024)
                if not msg:
                    break

                if msg == b"KO":
                    print(f"[CENTRAL] :( CP {cp} averiado → UNAVAILABLE")
                    db.upsert_cp({
                        "id": cp,
                        "state": "UNAVAILABLE",
                        "last_seen": datetime.now(timezone.utc)
                    })
                    conn.send(ACK)  
                elif msg == b"OK":
                    print(f"[CENTRAL] :) CP {cp} recuperado → AVAILABLE")
                    db.upsert_cp({
                        "id": cp,
                        "state": "AVAILABLE",
                        "last_seen": datetime.now(timezone.utc)
                    })
                    conn.send(ACK)
                else:
                    print(f"[CENTRAL] Mensaje desconocido de {cp}: {msg}")

            except socket.timeout:
                continue  # seguir escuchando
            except Exception as e:
                print(f"[CENTRAL] Error recibiendo de {cp}: {e}")
                break

    except Exception as e:
        print(f"[CENTRAL] Error con {addr}: {e}")

    finally:
        conn.close()
        print(f"[CENTRAL] Conexión cerrada con {addr}")

def handle_driver(topic, data, prod):

    if topic == topics.EV_SUPPLY_REQUEST:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")

        print(f"[CENTRAL] Driver {driver_id} solicita recarga en CP {cp_id}")

        # Lógica de autorización simple (si el CP existe y está AVAILABLE)
        cp = db.get_cp(cp_id)
        if not cp:
            authorized = False
            reason = "CP no existe"
            print(f"[CENTRAL] Recarga denegada para Driver {driver_id} en CP {cp_id}: {reason}")
        elif not driver_id: # Si no existe el driver lo añadimos a la base de datos
            db.upsert_driver({
                "id": driver_id,
                "name": f"{driver_id} name"})

        if cp and cp.get("state") == "AVAILABLE":
            authorized = True
            reason = "CP disponible"
            print(f"[CENTRAL] Recarga autorizada para Driver {driver_id} en CP {cp_id}")
        else:
            authorized = False
            reason = "CP no disponible"
            print(f"[CENTRAL] Recarga denegada para Driver {driver_id} en CP {cp_id}: {reason}")

        kafka_utils.send(prod, topics.EV_SUPPLY_AUTH_DRI, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "authorized": authorized,
            "reason": reason
        })
        kafka_utils.send(prod, topics.EV_SUPPLY_AUTH, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "authorized": authorized,
            "reason": reason
        })
    elif topic == topics.EV_SUPPLY_CONNECTED:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")
        print(f"[CENTRAL] Notificación enviada a Driver {driver_id} de confirmación")
        if data.get("status") != "CONNECTED":
            print(f"[CENTRAL] Recarga rechazada por ENGINE para Driver {driver_id} en CP {cp_id}")
            kafka_utils.send(prod, topics.EV_SUPPLY_STARTED, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "status": "REJECTED"
            })
            return
        kafka_utils.send(prod, topics.EV_SUPPLY_STARTED, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": "APPROVED"
        })
        db.set_cp_state(cp_id, "SUPPLYING")
        session_id = f"{driver_id}_{cp_id}"
        active_sessions[session_id] = {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "start_time": datetime.now(timezone.utc)
            }
    elif topic == topics.EV_SUPPLY_END:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")
        print(f"[CENTRAL] Driver {driver_id} ha terminado la recarga en CP {cp_id}")
        db.set_cp_state(cp_id, "AVAILABLE")
        start_time = active_sessions.get(f"{driver_id}_{cp_id}", {}).get("start_time")

        if start_time:
            duration = datetime.now(timezone.utc) - start_time
            hours = duration.total_seconds() / 3600
        else:
            hours = 0

        cp_price = db.get_cp(cp_id).get("price_eur_kwh", 0.3)
        total_price = hours * cp_price
        kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "price": total_price
        })
        active_sessions.pop(f"{driver_id}_{cp_id}", None)

def tcp_server(s):
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

def kafka_listener(kafka_info):
    kafka_producer = kafka_utils.build_producer(kafka_info)
    kafka_consumer = kafka_utils.build_consumer(
        kafka_info, 
        "central-driver",
        [
        topics.EV_SUPPLY_CONNECTED,
        topics.EV_SUPPLY_END,
        topics.EV_SUPPLY_REQUEST
    ]
    )
    print("[CENTRAL] Esperando mensajes Kafka...")

    kafka_utils.poll_loop(
        kafka_consumer,
        lambda topic, data: handle_driver(topic, data, kafka_producer)
    )

def main():
    if len(sys.argv) != 3:
        print("Uso: central.py <puerto> <kafka_ip:port>")
        return

    port = int(sys.argv[1])
    kafka_info = sys.argv[2]

    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(5)
    print(f"[CENTRAL] En escucha permanente en {port}")
    print_cp_panel()

    threading.Thread(target=tcp_server, args=(s,), daemon=True).start()
    threading.Thread(target=kafka_listener, args=(kafka_info,), daemon=True).start()

    while True:
        time.sleep(1)

main()