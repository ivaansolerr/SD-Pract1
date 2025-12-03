import threading
import socket
import time
import json
import sys
import random
from datetime import datetime
from typing import Dict, Any, Optional

from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

# ==========================
# ESTADO GLOBAL
# ==========================
registered_cp: Optional[str] = None
registered_cp_event = threading.Event()
prod: Optional[Producer] = None

stop_event = threading.Event()      # Señala fin de suministro (local o remoto)
forced_end = threading.Event()      # Señala fin forzado por CENTRAL
cp_price = 0.0

current_driver_id: Optional[str] = None
session_lock = threading.Lock()
session_thread: Optional[threading.Thread] = None


# ======================================================
# SOCKET HANDLER (REGISTRO + HEARTBEAT DEL MONITOR)
# ======================================================
def handle(conn):
    global registered_cp

    msgEnc = conn.recv(1024)

    # Mensajes fuera de handshake
    if msgEnc in [b"CENTRAL_DOWN", b"CENTRAL_UP_AGAIN"]:
        print(f"[ENGINE] {msgEnc}")
        conn.close()
        return
    else:
        if msgEnc != b"<ENC>":
            print(f"[ENGINE] Error: Esperaba <ENC>, recibí {msgEnc}")
            conn.close()
            return

    conn.send(socketCommunication.ACK)

    cp = socketCommunication.parseFrame(conn.recv(1024))
    if cp is None:
        print("[ENGINE] Error: No obtuvo el CP_ID")
        conn.send(socketCommunication.NACK)
        conn.close()
        return

    registered_cp = cp
    registered_cp_event.set()
    print(f"[ENGINE] CP registrado: {registered_cp}")

    conn.send(socketCommunication.ACK)  # siguiendo el protocolo
    conn.send(socketCommunication.encodeMess("OK"))  # todo está OK

    if conn.recv(1024) != socketCommunication.ACK:
        print("[ENGINE] Error: No recibió ACK final")
        conn.close()
        return

    conn.send(b"<EOT>")
    print(f"[ENGINE {registered_cp}] Registrado, esperando mensajes del monitor...")

    while True:
        try:
            beat = conn.recv(1024)
            if not beat:
                break

            if beat == b"PING":
                conn.send(b"PONG")

            elif beat == b"CENTRAL_DOWN":
                print("[ENGINE] ⚠️ CENTRAL caída detectada")
                print("[ENGINE] 🚨 Modo aislamiento activado...")

            elif beat == b"CENTRAL_UP_AGAIN":
                print("[ENGINE] ✅ CENTRAL ha vuelto")
                print("[ENGINE] 🔄 Reanudando coordinación con CENTRAL")

        except Exception:
            break

    print("[ENGINE] Monitor desconectado")
    conn.close()


# ======================================================
# HILO DE HEARTBEATS DE SUMINISTRO
# ======================================================
def send_supply_heartbeat(driver_id: str):
    global cp_price

    energy = 0.0
    while not stop_event.is_set():
        consumption = round(random.uniform(0.5, 2.5), 2)  # kW instantáneo simulado
        energy += consumption * (3 / 3600)  # kWh cada 3 segundos
        precio = round(energy * cp_price, 6)

        print(f"[ENGINE] consumiendo {consumption} kW, energía total: {round(energy, 3)} kWh, precio: {precio} eur")

        kafka_utils.send(prod, topics.EV_SUPPLY_HEARTBEAT, {
            "timestamp": int(time.time() * 1000),
            "cp_id": registered_cp,
            "driver_id": driver_id,
            "power_kw": consumption,
            "energy_kwh": round(energy, 3)
        })

        time.sleep(3)

    print("[ENGINE] 💀 Heartbeat detenido.")


# ======================================================
# HILO DE SESIÓN DE SUMINISTRO
# (SE LANZA EN UN HILO SEPARADO DEL LISTENER KAFKA)
# ======================================================
def run_supply_session(driver_id: str):
    global current_driver_id

    with session_lock:
        current_driver_id = driver_id

    utils.info(f"[ENGINE] Iniciando suministro para driver {driver_id}")
    stop_event.clear()
    forced_end.clear()

    heartbeat_thread = threading.Thread(
        target=send_supply_heartbeat,
        args=(driver_id,),
        daemon=True
    )
    heartbeat_thread.start()

    print("Escriba 'FIN' para finalizar el suministro manualmente, o espere orden remota de CENTRAL.")

    # IMPORTANTE:
    # Esta parte puede quedarse bloqueada en input(),
    # pero ya NO bloquea el hilo que escucha Kafka.
    try:
        while not stop_event.is_set():
            cmd = input().strip()
            if cmd.upper() == "FIN":
                utils.info(f"[ENGINE] FIN manual recibido para driver {driver_id}")
                stop_event.set()
                break
    except EOFError:
        # Por si el stdin no está disponible o se cierra
        pass

    # Esperamos a que el hilo de heartbeat termine
    heartbeat_thread.join(timeout=2)

    # Si no es un fin forzado por CENTRAL, enviamos nosotros el EV_SUPPLY_END
    if not forced_end.is_set():
        kafka_utils.send(prod, topics.EV_SUPPLY_END, {
            "driver_id": driver_id,
            "cp_id": registered_cp
        })
        utils.info(f"[ENGINE] Suministro finalizado localmente para driver {driver_id}")
    else:
        utils.info(f"[ENGINE] Suministro ya fue finalizado por CENTRAL para driver {driver_id}")

    with session_lock:
        current_driver_id = None


# ======================================================
# MANEJO DE MENSAJES KAFKA DESDE CENTRAL
# (RÁPIDO, SIN BLOQUEAR)
# ======================================================
def handleRequest(topic, data):
    global cp_price, session_thread

    # ---------- AUTORIZACIÓN DE SUMINISTRO ----------
    if topic == topics.EV_SUPPLY_AUTH:
        if (data.get("authorized") is True) and (data.get("cp_id") == registered_cp):
            driver_id = data.get("driver_id")
            utils.ok(f"[ENGINE] Autorización aprobada para driver {driver_id}")

            cp_price = data.get("price_eur_kwh", 0.3)

            kafka_utils.send(prod, topics.EV_SUPPLY_CONNECTED, {
                "driver_id": driver_id,
                "cp_id": registered_cp,
                "status": "CONNECTED"
            })

            # Lanzamos la sesión en OTRO HILO
            session_thread = threading.Thread(
                target=run_supply_session,
                args=(driver_id,),
                daemon=True
            )
            session_thread.start()

    # ---------- FINALIZACIÓN REMOTA DESDE CENTRAL ----------
    elif topic == topics.EV_SUPPLY_END_ENGINE:
        if data.get("cp_id") == registered_cp:
            driver_id = data.get("driver_id")
            utils.info(f"[ENGINE] Suministro finalizado por CENTRAL para driver {driver_id}")
            forced_end.set()
            stop_event.set()

            # CENTRAL nos dice que acabemos: replicamos con EV_SUPPLY_END si queremos
            kafka_utils.send(prod, topics.EV_SUPPLY_END, {
                "driver_id": driver_id,
                "cp_id": registered_cp
            })


# ======================================================
# SERVIDOR SOCKET (HILO)
# ======================================================
def socketServer(socketPort):
    s = socket.socket()
    s.bind(("0.0.0.0", int(socketPort)))
    s.listen(1)
    print(f"[ENGINE] Esperando monitor en puerto {socketPort}...")

    while True:
        conn, _ = s.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


# ======================================================
# LISTENER KAFKA (HILO)
# ======================================================
def kafkaListener(kafkaIp, kafkaPort):
    kafka_info = f"{kafkaIp}:{kafkaPort}"
    global prod

    # Esperamos hasta tener CP registrado
    registered_cp_event.wait()

    prod = kafka_utils.buildProducer(kafka_info)
    consumer = kafka_utils.buildConsumer(
        kafka_info,
        f"engine-{registered_cp}",
        [
            topics.EV_SUPPLY_AUTH,
            topics.EV_SUPPLY_CONNECTED,
            topics.EV_SUPPLY_END,
            topics.EV_SUPPLY_REQUEST,
            topics.EV_SUPPLY_END_ENGINE
        ]
    )

    print(f"[ENGINE {registered_cp}] Escuchando mensajes de CENTRAL...")

    while True:
        msg = consumer.poll(0.1)
        if not msg or msg.error():
            continue

        data = json.loads(msg.value().decode("utf-8"))
        handleRequest(msg.topic(), data)


# ======================================================
# MAIN
# ======================================================
def main():
    if len(sys.argv) != 4:
        print("Uso: engine.py <kafka_ip> <kafka_port> <socket_port>")
        return

    kafka_ip = sys.argv[1]
    kafka_port = sys.argv[2]
    socket_port = sys.argv[3]

    t1 = threading.Thread(target=socketServer, args=(socket_port,), daemon=True)
    t2 = threading.Thread(target=kafkaListener, args=(kafka_ip, kafka_port), daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()


main()
