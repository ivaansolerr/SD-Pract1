import socket, sys, threading, time
from typing import Dict, Any
from datetime import datetime, timezone
from . import db
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

active_sessions: Dict[str, Dict[str, Any]] = {}

def printCpPanel():
    cps = db.listChargingPoints()
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

def cpExists(cp_id):
    cp = db.getCp(cp_id) 
    return cp is not None

def handleClient(conn, addr):
    print(f"[CENTRAL] Nueva conexión desde {addr}")

    try:
        if conn.recv(1024) != b"<ENC>": # primer mensaje según el protocolo
            conn.close()
            return
        conn.send(socketCommunication.ACK) # mandamos el ACK

        cp = socketCommunication.parseFrame(conn.recv(1024)) # sacamos el id del CP
        if cp is None:
            conn.send(socketCommunication.NACK)
            conn.close()
            return

        print(f"[CENTRAL] Solicitud por CP: {cp}")
        conn.send(socketCommunication.ACK)

        if cpExists(cp): # comprobamos que exista en la base de datos y autenticamos
            print(f"[CENTRAL] CP :) {cp} autenticado. Estado → AVAILABLE")
            db.upsertCp({
                "id": cp,
                "state": "AVAILABLE",
                #"last_seen": datetime.now(timezone.utc)
            })
            result = "OK"
        else:
            print(f"[CENTRAL] CP {cp} NO existe en la base de datos")
            result = "NO"

        conn.send(socketCommunication.encodeMess(result)) # mandamos la respuesta

        if conn.recv(1024) != socketCommunication.ACK:
            conn.close()
            return

        conn.send(b"<EOT>") # fin de comunicación
        print(f"[CENTRAL] Handshake completado con CP {cp}, esperando mensajes...")

        conn.settimeout(10)
        while True:
            try:
                msg = conn.recv(1024)
                if not msg:
                    break
                
                if msg == b"PING":
                    # Nuevo: respuesta al heartbeat del monitor
                    conn.send(b"PONG")

                elif msg == b"KO":
                    print(f"[CENTRAL] :( CP {cp} averiado → UNAVAILABLE")
                    db.upsertCp({
                        "id": cp,
                        "state": "UNAVAILABLE",
                    })
                    conn.send(socketCommunication.ACK)

                elif msg == b"OK":
                    print(f"[CENTRAL] :) CP {cp} recuperado → AVAILABLE")
                    db.upsertCp({
                        "id": cp,
                        "state": "AVAILABLE",
                    })
                    conn.send(socketCommunication.ACK)

                else:
                    print(f"[CENTRAL] Mensaje desconocido de {cp}: {msg}")
    
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[CENTRAL] Error recibiendo de {cp}: {e}")
                break

    except Exception as e:
        print(f"[CENTRAL] Error con {addr}: {e}")

    finally:
        conn.close()
        print(f"[CENTRAL] Conexión cerrada con {addr}")

def handleDriver(topic, data, prod):

    if topic == topics.EV_SUPPLY_REQUEST:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")

        print(f"[CENTRAL] Driver {driver_id} solicita recarga en CP {cp_id}")

        cp = db.getCp(cp_id)
        if not cp:
            authorized = False
            reason = "CP no existe"
            print(f"[CENTRAL] Recarga denegada para Driver {driver_id} en CP {cp_id}: {reason}")
        elif not driver_id: # Si no existe el driver lo añadimos a la base de datos
            db.upsertDriver({
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
        # autorizamos driver y envíamos al driver
        kafka_utils.send(prod, topics.EV_SUPPLY_AUTH_DRI, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "authorized": authorized,
            "reason": reason
        })
        # autorizamos driver y envíamos al engine
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
        if data.get("status") != "CONNECTED": # si el cp ya está en uso
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
        db.setCpState(cp_id, "SUPPLYING") # aquí almjr habría que crear un nuevo topic para mandar los datos en directo
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
        db.setCpState(cp_id, "AVAILABLE")
        start_time = active_sessions.get(f"{driver_id}_{cp_id}", {}).get("start_time")

        if start_time:
            duration = datetime.now(timezone.utc) - start_time
            hours = duration.total_seconds() / 3600
        else:
            hours = 0

        cp_price = db.getCp(cp_id).get("price_eur_kwh", 0.3)
        total_price = hours * cp_price
        kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "price": total_price
        })
        active_sessions.pop(f"{driver_id}_{cp_id}", None)

def tcpServer(s):
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handleClient, args=(conn, addr), daemon=True)
        t.start()

def kafkaListener(kafkaInfo):
    kafkaProducer = kafka_utils.buildProducer(kafkaInfo)
    kafkaConsumer = kafka_utils.buildConsumer(
        kafkaInfo, 
        "central-driver",
        [
        topics.EV_SUPPLY_CONNECTED,
        topics.EV_SUPPLY_END,
        topics.EV_SUPPLY_REQUEST
    ]
    )
    print("[CENTRAL] Esperando mensajes Kafka...")

    kafka_utils.pollLoop(
        kafkaConsumer,
        lambda topic, data: handleDriver(topic, data, kafkaProducer)
    )

def main():
    if len(sys.argv) != 3:
        print("Uso: central.py <puerto> <kafka_ip:port>")
        return

    port = int(sys.argv[1])
    kafkaInfo = sys.argv[2]

    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(5)
    print(f"[CENTRAL] En escucha permanente en {port}")
    printCpPanel()

    # un thread para kafka y otro para el socket para que ambos puedan tener los bucles escuchando
    threading.Thread(target=tcpServer, args=(s,), daemon=True).start()
    threading.Thread(target=kafkaListener, args=(kafkaInfo,), daemon=True).start()

    while True:
        time.sleep(1)

main()