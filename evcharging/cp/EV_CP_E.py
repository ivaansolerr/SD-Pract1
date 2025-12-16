import threading
import socket
import time
import json
import sys
import random
import ssl
from datetime import datetime
from typing import Dict, Any, Optional

from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken
import os

registered_cp: Optional[str] = None
registered_cp_event = threading.Event()
prod: Optional[Producer] = None

stop_event = threading.Event()      # Señala fin de suministro (local o remoto)
forced_end = threading.Event()      # Señala fin forzado por CENTRAL
cp_price = 0.0

current_driver_id: Optional[str] = None
session_lock = threading.Lock()
session_thread: Optional[threading.Thread] = None
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../evcharging/cp
TLS_CERT = os.path.join(BASE_DIR, "certServ.pem")      # .../evcharging/cp/certServ.pem
KEYS_DIR = os.path.join(BASE_DIR, "keys")        # .../evcharging/cp/keys

def _derive_fernet_key(key_str: str) -> bytes:
    digest = hashlib.sha256(key_str.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)

def _encrypt_kafka(payload: Dict[str, Any], cp_id: str) -> Dict[str, Any]:
    key_path = os.path.join(KEYS_DIR, f"{cp_id}_key.txt")
    kafka_key = None
    try:
        with open(key_path, "r") as key_file:
            kafka_key = key_file.read().strip()
    except FileNotFoundError:
        print(f"cp_id={cp_id} key_file_not_found")
    if not kafka_key:
        return payload

    f = Fernet(_derive_fernet_key(kafka_key))
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    token = f.encrypt(raw).decode("utf-8")

    return {
        "encrypted": True,
        "cp_id": registered_cp,
        "payload": token
    }

def _decrypt_kafka(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict) or not data.get("encrypted"):
        return data

    # Validaciones mínimas del sobre
    if "payload" not in data or "cp_id" not in data:
        utils.info("[ENGINE] ⚠️ Sobre cifrado inválido (faltan campos).")
        return {}
    
    key_path = os.path.join(KEYS_DIR, f"{registered_cp}_key.txt")
    kafka_key = None
    try:
        with open(key_path, "r") as key_file:
            kafka_key = key_file.read().strip()
    except FileNotFoundError:
        audit_log("LOCAL", "ENCRYPTION_KEY_MISSING", f"cp_id={registered_cp} key_file_not_found")
    if not kafka_key:
        utils.info("[ENGINE] ⚠️ Mensaje cifrado recibido sin kafka_key disponible")
        return {}

    # Aseguramos que el mensaje es para este CP
    if data.get("cp_id") != registered_cp:
        return {}

    f = Fernet(_derive_fernet_key(kafka_key))
    try:
        raw = f.decrypt(str(data["payload"]).encode("utf-8"))  # MOD
        decoded = raw.decode("utf-8")  # MOD
        return json.loads(decoded)  # MOD
    except (InvalidToken, ValueError, TypeError):  # MOD
        utils.info("[ENGINE] ❌ Error descifrando mensaje Kafka")
        return {}

def kafka_send(prod: Producer, topic: str, payload: Dict[str, Any]):
    kafka_utils.send(prod, topic, _encrypt_kafka(payload, registered_cp))

def handle(conn):
    global registered_cp

    try:
        msgEnc = conn.recv(1024)
        if msgEnc != b"<ENC>":
            print(f"[ENGINE] Error Handshake: Esperaba <ENC>, recibí {msgEnc}")
            conn.close()
            return
        
        conn.send(socketCommunication.ACK)

        cp = socketCommunication.parseFrame(conn.recv(1024))
        key = socketCommunication.parseFrame(conn.recv(1024))
        if cp is None:
            conn.send(socketCommunication.NACK)
            conn.close()
            return

        registered_cp = cp
        registered_cp_event.set()
        utils.info(f"[ENGINE] Clave recibida de MONITOR para CP {registered_cp}")
        print(f"[ENGINE] CP registrado: {registered_cp}")

        conn.send(socketCommunication.ACK)
        conn.send(socketCommunication.encodeMess("OK")) 

        if conn.recv(1024) != socketCommunication.ACK:
            conn.close()
            return

        conn.send(b"<EOT>")
        
        os.makedirs(KEYS_DIR, exist_ok=True)
        with open(os.path.join(KEYS_DIR, f"{registered_cp}_key.txt"), "w") as kf:
            kf.write(key)

    except Exception as e:
        print(f"[ENGINE] Error durante handshake: {e}")
        conn.close()
        return

    print(f"[ENGINE {registered_cp}] Registrado, esperando heartbeats y estado...")

    while True:
        try:
            beat = conn.recv(1024)
            if not beat:
                break # Socket cerrado por el cliente

            if beat == b"PING":
                conn.send(b"PONG")

            elif beat == b"CENTRAL_DOWN":
                print("[ENGINE] ⚠️ ALERTA: CENTRAL CAÍDA DETECTADA")

            elif beat == b"CENTRAL_UP_AGAIN":
                print("[ENGINE] ✅ INFO: CENTRAL RECUPERADA")

        except Exception as e:
            print(f"[ENGINE] Error en conexión monitor: {e}")
            break

    print("[ENGINE] Monitor desconectado")
    conn.close()

def send_supply_heartbeat(driver_id: str):
    global cp_price

    energy = 0.0
    while not stop_event.is_set():
        consumption = round(random.uniform(0.5, 2.5), 2)  # kW instantáneo simulado
        energy += consumption * (3 / 3600)  # kWh cada 3 segundos
        precio = round(energy * cp_price, 6)

        print(f"[ENGINE] consumiendo {consumption} kW, energía total: {round(energy, 3)} kWh, precio: {precio} eur")

        kafka_send(prod, topics.EV_SUPPLY_HEARTBEAT, {
            "timestamp": int(time.time() * 1000),
            "cp_id": registered_cp,
            "driver_id": driver_id,
            "power_kw": consumption,
            "energy_kwh": round(energy, 3)
        })

        time.sleep(3)

    print("[ENGINE] 💀 Heartbeat detenido.")

def run_supply_session(driver_id: str):
    global current_driver_id

    # 1. Bloqueamos el CP
    with session_lock:
        current_driver_id = driver_id

    utils.info(f"[ENGINE] Iniciando suministro para driver {driver_id}")
    stop_event.clear()
    forced_end.clear()

    # Lanzamos el heartbeat
    heartbeat_thread = threading.Thread(
        target=send_supply_heartbeat,
        args=(driver_id,),
        daemon=True
    )
    heartbeat_thread.start()

    print("Escriba 'FIN' para finalizar el suministro manualmente, o espere orden remota de CENTRAL.")

    try:
        # Bucle de espera manual
        while not stop_event.is_set():
            cmd = input().strip()
            if cmd.upper() == "FIN":
                utils.info(f"[ENGINE] FIN manual recibido para driver {driver_id}")
                stop_event.set()
                break
                
    except EOFError:
        pass
    except Exception as e:
        print(f"[ENGINE] Error en sesión: {e}")

    finally:
        heartbeat_thread.join(timeout=2)

        if not forced_end.is_set():
            # Si fue manual, avisamos a Central
            kafka_send(prod, topics.EV_SUPPLY_END, {
                "driver_id": driver_id,
                "cp_id": registered_cp
            })
            utils.info(f"[ENGINE] Suministro finalizado localmente para driver {driver_id}")
        else:
            utils.info(f"[ENGINE] Suministro ya fue finalizado por CENTRAL para driver {driver_id}")

        # Liberamos el cerrojo (si no lo liberó ya handleRequest)
        with session_lock:
            if current_driver_id == driver_id:
                current_driver_id = None
                print("[ENGINE] 🔓 CP liberado (Fin de hilo de sesión).")

def handleRequest(topic, data):
    global cp_price, session_thread, current_driver_id

    if topic == topics.EV_SUPPLY_AUTH:
        data = _decrypt_kafka(data)
        if not data:
            return
  
        with session_lock:
            if current_driver_id is not None:
                print(f"[ENGINE] ⚠️ IGNORADO: Autorización para {data.get('driver_id')} recibida, pero el CP ya está ocupado por {current_driver_id}.")
                return

        if (data.get("authorized") is True) and (data.get("cp_id") == registered_cp):
            driver_id = data.get("driver_id")
            utils.ok(f"[ENGINE] Autorización aprobada para driver {driver_id}")

            cp_price = data.get("price_eur_kwh", 0.3)

            kafka_send(prod, topics.EV_SUPPLY_CONNECTED, {
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
    
    elif topic == topics.EV_REVOKE_KEY:
        # (mismo código que tenías)
        data = _decrypt_kafka(data)
        if not data: return
        if data.get("cp_id") == registered_cp:
            key_path = os.path.join(KEYS_DIR, f"{registered_cp}_key.txt")
            try:
                os.remove(key_path)
                print(f"[CENTRAL] 🔑 Clave de cifrado revocada.")
            except: pass

    elif topic == topics.EV_SUPPLY_END_ENGINE:
        data = _decrypt_kafka(data)
        if not data:
            return
        if data.get("cp_id") == registered_cp:
            driver_id = data.get("driver_id")
            utils.info(f"[ENGINE] Suministro finalizado por CENTRAL para driver {driver_id}")
            
            forced_end.set()
            stop_event.set()

            with session_lock:
                if current_driver_id == driver_id:
                    current_driver_id = None
                    print("[ENGINE] 🔓 CP liberado forzosamente tras orden de CENTRAL.")

            kafka_send(prod, topics.EV_SUPPLY_END, {
                "driver_id": driver_id,
                "cp_id": registered_cp
            })

def socketServer(socketPort):
    # Contexto TLS servidor
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=TLS_CERT, keyfile=TLS_CERT)

    s = socket.socket()
    s.bind(("0.0.0.0", int(socketPort)))
    s.listen(5)
    print(f"[ENGINE] 🔒 Esperando MONITOR TLS en puerto {socketPort}...")

    while True:
        raw_conn, addr = s.accept()
        try:
            conn = ctx.wrap_socket(raw_conn, server_side=True)
            threading.Thread(target=handle, args=(conn,), daemon=True).start()
        except Exception as e:
            print(f"[ENGINE] ❌ Error TLS handshake desde {addr}: {e}")
            try:
                raw_conn.close()
            except:
                pass

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
            topics.EV_SUPPLY_END_ENGINE,
            topics.EV_REVOKE_KEY
        ]
    )

    print(f"[ENGINE {registered_cp}] Escuchando mensajes de CENTRAL...")

    while True:
        msg = consumer.poll(0.1)
        if not msg or msg.error():
            continue

        data = json.loads(msg.value().decode("utf-8"))
        handleRequest(msg.topic(), data)

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