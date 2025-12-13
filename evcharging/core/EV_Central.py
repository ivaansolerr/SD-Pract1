import socket, sys, threading, time
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from . import db
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer
from hashlib import md5
import secrets
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken
import logging
from logging.handlers import RotatingFileHandler
import json
from datetime import datetime, timezone


active_sessions: Dict[str, Dict[str, Any]] = {}

# ======================================================
# LOGS (AUDIT) A ARCHIVO
# ======================================================

AUDIT_LOG_PATH = "./central_audit.log"

_audit_logger = logging.getLogger("central_audit")
_audit_logger.setLevel(logging.INFO)
_audit_logger.propagate = False

if not _audit_logger.handlers:
    handler = RotatingFileHandler(
        AUDIT_LOG_PATH,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8"
    )
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    _audit_logger.addHandler(handler)


def audit_log(actor_ip: str, action: str, details: str = "", when: Optional[datetime] = None):
    ts = (when or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    actor_ip = actor_ip or "UNKNOWN"
    action = action or "UNKNOWN_ACTION"
    details = details or ""
    _audit_logger.info(f"{ts} | {actor_ip} | {action} | {details}")


# ======================================================
# CIFRADO / DESCIFRADO KAFKA (CENTRAL)
# ======================================================
def _derive_fernet_key(key_str: str) -> bytes:
    digest = hashlib.sha256(key_str.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _encrypt_for_cp(cp_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    cp = db.getCp(cp_id)
    key = (cp or {}).get("engine_key")
    if not key:
        return payload

    f = Fernet(_derive_fernet_key(key))
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    token = f.encrypt(raw).decode("utf-8")

    return {
        "encrypted": True,
        "cp_id": cp_id,
        "payload": token
    }


def _decrypt_from_cp(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict) or not data.get("encrypted"):
        return data

    cp_id = data.get("cp_id")
    if not cp_id:
        audit_log("KAFKA", "DECRYPT_FAIL", "mensaje sin cp_id")
        return {}

    cp = db.getCp(cp_id)
    key = (cp or {}).get("engine_key")
    if not key:
        audit_log("KAFKA", "DECRYPT_FAIL", f"cp_id={cp_id} sin engine_key en BD")
        return {}

    f = Fernet(_derive_fernet_key(key))
    try:
        raw = f.decrypt(data["payload"].encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except InvalidToken:
        utils.info(f"[CENTRAL] ❌ Error descifrando mensaje de CP {cp_id}")
        audit_log("KAFKA", "DECRYPT_FAIL", f"cp_id={cp_id} InvalidToken")
        return {}
    except Exception as e:
        audit_log("KAFKA", "DECRYPT_ERROR", f"cp_id={cp_id} err={repr(e)}")
        return {}


def kafka_send_cp(prod: Producer, topic: str, cp_id: str, payload: Dict[str, Any]):
    kafka_utils.send(prod, topic, _encrypt_for_cp(cp_id, payload))

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
            f"Price: {cp.get('price', 'N/A')} eur | "
            f"Location: {cp.get('location', 'N/A')}"
        )

def cpExists(cp_id):
    cp = db.getCp(cp_id) 
    return cp is not None

def heartbeat_loop(kafkaInfo):
    prod = kafka_utils.buildProducer(kafkaInfo)
    while True:
        try:
            kafka_send_cp(prod, topics.EV_CENTRAL_HEARTBEAT, {
                "timestamp": int(time.time() * 1000)
            })
            audit_log("LOCAL", "CENTRAL_HEARTBEAT_SENT", "topic=EV_CENTRAL_HEARTBEAT")
        except Exception as e:
            audit_log("LOCAL", "CENTRAL_HEARTBEAT_ERROR", f"err={repr(e)}")
        time.sleep(5)

def stopCP(cp_id, kafkaInfo):
    cp = db.getCp(cp_id)
    if not cp:
        print(f"[CENTRAL] ❌ No existe CP {cp_id}")
        audit_log("LOCAL", "ADMIN_STOP_CP_FAIL", f"cp_id={cp_id} no_existe")
        return

    state = cp.get("state", "").upper()
    audit_log("LOCAL", "ADMIN_STOP_CP", f"cp_id={cp_id} state={state}")

    if state == "SUPPLYING":
        print(f"[CENTRAL] 🛑 Parando CP {cp_id} (SUPPLYING) → Finalizando sesión")

        session = db.sessions.find_one({"cp_id": cp_id})
        if session:
            driver_id = session["driver_id"]
            energy_total = session.get("energy_kwh", 0.0)
            price = cp.get("price", 0.3)
            total_price = round(energy_total * price, 2)

            prod = kafka_utils.buildProducer(kafkaInfo)
            kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "price": total_price,
                "energy_kwh": round(energy_total, 3)
            })
            audit_log("LOCAL", "TICKET_ISSUED_BY_STOP",
                      f"driver_id={driver_id} cp_id={cp_id} energy_kwh={round(energy_total,3)} price={total_price}")

            kafka_utils.send(prod, topics.EV_DRIVER_SUPPLY_ERROR, {
                "driver_id": driver_id,
                "cp_id": cp_id,
            })
            audit_log("LOCAL", "DRIVER_SUPPLY_ERROR_SENT", f"driver_id={driver_id} cp_id={cp_id}")

            db.deleteSession(driver_id, cp_id)
            audit_log("LOCAL", "SESSION_DELETED_DB", f"driver_id={driver_id} cp_id={cp_id}")

            session_key = f"{driver_id}_{cp_id}"
            active_sessions.pop(session_key, None)
            audit_log("LOCAL", "SESSION_REMOVED_MEMORY", f"session_id={session_key}")

        db.upsertCp({
            "id": cp_id,
            "state": "FUERA DE SERVICIO",
        })
        audit_log("LOCAL", "CP_STATE_CHANGE", f"cp_id={cp_id} SUPPLYING->FUERA_DE_SERVICIO")
        print(f"[CENTRAL] 🔴 CP {cp_id} → FUERA DE SERVICIO")

    else:
        print(f"[CENTRAL] 🛑 Parando CP {cp_id} (NO supplying) → FUERA DE SERVICIO")
        db.upsertCp({
            "id": cp_id,
            "state": "FUERA DE SERVICIO",
        })
        audit_log("LOCAL", "CP_STATE_CHANGE", f"cp_id={cp_id} {state}->FUERA_DE_SERVICIO")

def enableCP(cp_id):
    cp = db.getCp(cp_id)
    if not cp:
        print(f"[CENTRAL] ❌ No existe CP {cp_id}")
        audit_log("LOCAL", "ADMIN_ENABLE_CP_FAIL", f"cp_id={cp_id} no_existe")
        return

    if cp.get("state") != "FUERA DE SERVICIO":
        print(f"[CENTRAL] ⚠️ CP {cp_id} no está fuera de servicio.")
        audit_log("LOCAL", "ADMIN_ENABLE_CP_FAIL",
                  f"cp_id={cp_id} state={cp.get('state')} no_es_fuera_de_servicio")
        return

    db.setCpState(cp_id, "AVAILABLE")
    audit_log("LOCAL", "CP_STATE_CHANGE", f"cp_id={cp_id} FUERA_DE_SERVICIO->AVAILABLE")
    print(f"[CENTRAL] 🟢 CP {cp_id} habilitado → AVAILABLE")


def handleClient(conn, addr, kafkaInfo):
    print(f"[CENTRAL] Nueva conexión desde {addr}")
    cp_ip = addr[0] if addr else "UNKNOWN"
    audit_log(cp_ip, "TCP_CONNECT", f"addr={addr}")

    try:
        first = conn.recv(1024)
        if first != b"<ENC>":
            audit_log(cp_ip, "AUTH_PROTOCOL_FAIL", f"primer_msg={first!r}")
            conn.close()
            return
        conn.send(socketCommunication.ACK)

        cp = socketCommunication.parseFrame(conn.recv(1024))
        key = socketCommunication.parseFrame(conn.recv(1024))
        if cp is None:
            audit_log(cp_ip, "AUTH_FAIL", "cp_id=None (frame inválido)")
            conn.send(socketCommunication.NACK)
            conn.close()
            return

        audit_log(cp_ip, "AUTH_ATTEMPT", f"cp_id={cp}")
        print(f"[CENTRAL] Solicitud por CP: {cp}")
        conn.send(socketCommunication.ACK)

        if cpExists(cp):
            stored_hash = db.getCp(cp).get("key", "")
            new_hash = md5(key.encode()).hexdigest()
            if stored_hash != new_hash:
                print(f"[CENTRAL] CP {cp} falló autenticación (key incorrecta)")
                audit_log(cp_ip, "AUTH_FAIL", f"cp_id={cp} key_incorrecta")
                conn.send(socketCommunication.NACK)
                conn.close()
                return
            else:
                print(f"[CENTRAL] CP :) {cp} autenticado. Estado → AVAILABLE")
                sym_key = secrets.token_hex(32)

                db.upsertCp({
                    "id": cp,
                    "state": "AVAILABLE",
                    "engine_key": sym_key,
                })
                audit_log(cp_ip, "AUTH_OK", f"cp_id={cp} state->AVAILABLE engine_key_generada=True")
                audit_log(cp_ip, "CP_STATE_CHANGE", f"cp_id={cp} -> AVAILABLE")

                prod = kafka_utils.buildProducer(kafkaInfo)
                kafka_utils.send(prod, "EV_ENGINE_KEY_EXCHANGE", {
                    "cp_id": cp,
                    "key": sym_key
                })
                audit_log(cp_ip, "KEY_EXCHANGE_SENT", f"cp_id={cp} topic=EV_ENGINE_KEY_EXCHANGE")

                result = "OK"
        else:
            print(f"[CENTRAL] CP {cp} NO existe en la base de datos")
            audit_log(cp_ip, "AUTH_FAIL", f"cp_id={cp} no_existe_en_bd")
            result = "NO"

        conn.send(socketCommunication.encodeMess(result))

        if conn.recv(1024) != socketCommunication.ACK:
            audit_log(cp_ip, "AUTH_PROTOCOL_FAIL", f"cp_id={cp} no_recibe_ACK_final")
            conn.close()
            return

        conn.send(b"<EOT>")
        audit_log(cp_ip, "HANDSHAKE_OK", f"cp_id={cp}")
        print(f"[CENTRAL] Handshake completado con CP {cp}, esperando mensajes...")

        conn.settimeout(10)
        while True:
            try:
                msg = conn.recv(1024)
                if not msg:
                    audit_log(cp_ip, "TCP_DISCONNECT", f"cp_id={cp} socket_cerrado_remoto")
                    break

                if msg == b"PING":
                    conn.send(b"PONG")
                    audit_log(cp_ip, "MONITOR_PING", f"cp_id={cp} PING->PONG")

                elif msg == b"KO":
                    print(f"[CENTRAL] :( CP {cp} averiado → UNAVAILABLE")
                    db.upsertCp({
                        "id": cp,
                        "state": "UNAVAILABLE",
                    })
                    audit_log(cp_ip, "INCIDENT_CP_KO", f"cp_id={cp} state->UNAVAILABLE")
                    audit_log(cp_ip, "CP_STATE_CHANGE", f"cp_id={cp} -> UNAVAILABLE")
                    conn.send(socketCommunication.ACK)

                    session_found = None
                    session_key = None
                    for sid, session_data in active_sessions.items():
                        if session_data.get("cp_id") == cp:
                            session_found = session_data
                            session_key = sid
                            break

                    if session_found:
                        driver_id = session_found.get("driver_id")
                        energy_total = session_found.get("energy_kwh", 0.0)
                        price = db.getCp(cp).get("price", 0.3)
                        total_price = energy_total * price
                        print(f"[CENTRAL] 🚨 CP {cp} estaba en sesión activa con Driver {driver_id}. Notificando error...")

                        audit_log(cp_ip, "INCIDENT_ACTIVE_SESSION_INTERRUPTED",
                                  f"cp_id={cp} driver_id={driver_id} energy_kwh={round(energy_total,3)}")

                        prod = kafka_utils.buildProducer(kafkaInfo)
                        kafka_utils.send(prod, topics.EV_DRIVER_SUPPLY_ERROR, {
                            "driver_id": driver_id,
                            "cp_id": cp,
                        })
                        audit_log(cp_ip, "DRIVER_SUPPLY_ERROR_SENT", f"driver_id={driver_id} cp_id={cp}")

                        kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
                            "driver_id": driver_id,
                            "cp_id": cp,
                            "price": round(total_price, 2),
                            "energy_kwh": round(energy_total, 3)
                        })
                        audit_log(cp_ip, "TICKET_ISSUED",
                                  f"driver_id={driver_id} cp_id={cp} price={round(total_price,2)} energy_kwh={round(energy_total,3)}")

                        active_sessions.pop(session_key, None)
                        audit_log(cp_ip, "SESSION_REMOVED_MEMORY", f"session_id={session_key}")
                        db.deleteSession(driver_id, cp)
                        audit_log(cp_ip, "SESSION_DELETED_DB", f"driver_id={driver_id} cp_id={cp}")

                elif msg == b"OK":
                    print(f"[CENTRAL] :) CP {cp} recuperado → AVAILABLE")
                    db.upsertCp({
                        "id": cp,
                        "state": "AVAILABLE",
                    })
                    audit_log(cp_ip, "CP_RECOVERED", f"cp_id={cp} state->AVAILABLE")
                    audit_log(cp_ip, "CP_STATE_CHANGE", f"cp_id={cp} -> AVAILABLE")
                    conn.send(socketCommunication.ACK)

                else:
                    print(f"[CENTRAL] Mensaje desconocido de {cp}: {msg}")
                    audit_log(cp_ip, "UNKNOWN_MESSAGE", f"cp_id={cp} msg={msg!r}")

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[CENTRAL] Error recibiendo de {cp}: {e}")
                audit_log(cp_ip, "TCP_ERROR", f"cp_id={cp} err={repr(e)}")
                break

    except Exception as e:
        print(f"[CENTRAL] Error con {addr}: {e}")
        audit_log(cp_ip, "HANDLECLIENT_ERROR", f"addr={addr} err={repr(e)}")

    finally:
        conn.close()
        print(f"[CENTRAL] Conexión cerrada con {addr}")
        audit_log(cp_ip, "TCP_CLOSE", f"addr={addr}")

        try:
            if 'cp' in locals() and cp is not None:
                print(f"[CENTRAL] ⚠️ CP {cp} desconectado → DISCONNECTED")
                db.upsertCp({
                    "id": cp,
                    "state": "DISCONNECTED"
                })
                audit_log(cp_ip, "CP_STATE_CHANGE", f"cp_id={cp} -> DISCONNECTED")

                session_found = None
                session_key = None
                for sid, session_data in active_sessions.items():
                    if session_data.get("cp_id") == cp:
                        session_found = session_data
                        session_key = sid
                        break

                if session_found:
                    driver_id = session_found.get("driver_id")
                    energy_total = session_found.get("energy_kwh", 0.0)
                    price = db.getCp(cp).get("price", 0.3)
                    total_price = energy_total * price
                    print(f"[CENTRAL] 🚨 CP {cp} estaba en sesión activa con Driver {driver_id}. Notificando error...")

                    audit_log(cp_ip, "INCIDENT_ACTIVE_SESSION_INTERRUPTED",
                              f"cp_id={cp} driver_id={driver_id} energy_kwh={round(energy_total,3)} reason=DISCONNECT")

                    prod = kafka_utils.buildProducer(kafkaInfo)
                    kafka_utils.send(prod, topics.EV_DRIVER_SUPPLY_ERROR, {
                        "driver_id": driver_id,
                        "cp_id": cp,
                    })
                    audit_log(cp_ip, "DRIVER_SUPPLY_ERROR_SENT", f"driver_id={driver_id} cp_id={cp}")

                    kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
                        "driver_id": driver_id,
                        "cp_id": cp,
                        "price": round(total_price, 2),
                        "energy_kwh": round(energy_total, 3)
                    })
                    audit_log(cp_ip, "TICKET_ISSUED",
                              f"driver_id={driver_id} cp_id={cp} price={round(total_price,2)} energy_kwh={round(energy_total,3)}")

                    active_sessions.pop(session_key, None)
                    audit_log(cp_ip, "SESSION_REMOVED_MEMORY", f"session_id={session_key}")
                    db.deleteSession(driver_id, cp)
                    audit_log(cp_ip, "SESSION_DELETED_DB", f"driver_id={driver_id} cp_id={cp}")

        except Exception as e:
            print(f"[CENTRAL] ❌ Error marcando CP desconectado: {e}")
            audit_log(cp_ip, "DISCONNECT_HANDLER_ERROR", f"cp_id={cp if 'cp' in locals() else None} err={repr(e)}")


def handleDriver(topic, data, prod):
    data = _decrypt_from_cp(data)
    if not data:
        audit_log("KAFKA", "DROP_MESSAGE", f"topic={topic} motivo=data_vacia_o_no_descifrada")
        return

    # [AUDIT] Log de recepción de topic (sin volcar payload completo por seguridad)
    audit_log("KAFKA", "KAFKA_RX", f"topic={topic}")

    if topic == topics.EV_SUPPLY_REQUEST:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")

        audit_log("KAFKA", "SUPPLY_REQUEST", f"driver_id={driver_id} cp_id={cp_id}")
        print(f"[CENTRAL] Driver {driver_id} solicita recarga en CP {cp_id}")

        cp = db.getCp(cp_id)
        if not cp:
            authorized = False
            reason = "CP no existe"
            audit_log("KAFKA", "SUPPLY_DENIED", f"driver_id={driver_id} cp_id={cp_id} reason={reason}")
            print(f"[CENTRAL] Recarga denegada para Driver {driver_id} en CP {cp_id}: {reason}")
        elif not driver_id:
            db.upsertDriver({
                "id": driver_id,
                "name": f"{driver_id} name"})
            audit_log("KAFKA", "DRIVER_UPSERT", f"driver_id={driver_id}")

        if cp and cp.get("state") == "AVAILABLE":
            authorized = True
            reason = "CP disponible"
            audit_log("KAFKA", "SUPPLY_AUTHORIZED", f"driver_id={driver_id} cp_id={cp_id}")
            print(f"[CENTRAL] Recarga autorizada para Driver {driver_id} en CP {cp_id}")
        else:
            authorized = False
            reason = "CP no disponible"
            audit_log("KAFKA", "SUPPLY_DENIED", f"driver_id={driver_id} cp_id={cp_id} reason={reason}")
            print(f"[CENTRAL] Recarga denegada para Driver {driver_id} en CP {cp_id}: {reason}")

        kafka_utils.send(prod, topics.EV_SUPPLY_AUTH_DRI, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "authorized": authorized,
            "reason": reason
        })
        audit_log("KAFKA", "SUPPLY_AUTH_SENT_TO_DRIVER",
                  f"driver_id={driver_id} cp_id={cp_id} authorized={authorized} reason={reason}")

        kafka_send_cp(prod, topics.EV_SUPPLY_AUTH, cp_id, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "authorized": authorized,
            "reason": reason,
            "price": cp.get("price", 0.3) if cp else None
        })
        audit_log("KAFKA", "SUPPLY_AUTH_SENT_TO_ENGINE",
                  f"driver_id={driver_id} cp_id={cp_id} authorized={authorized} price={cp.get('price',0.3) if cp else None}")

    elif topic == topics.EV_SUPPLY_CONNECTED:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")
        status = data.get("status")

        audit_log("KAFKA", "SUPPLY_CONNECTED", f"driver_id={driver_id} cp_id={cp_id} status={status}")
        print(f"[CENTRAL] Notificación enviada a Driver {driver_id} de confirmación")

        if status != "CONNECTED":
            audit_log("KAFKA", "SUPPLY_REJECTED_BY_ENGINE", f"driver_id={driver_id} cp_id={cp_id} status={status}")
            print(f"[CENTRAL] Recarga rechazada por ENGINE para Driver {driver_id} en CP {cp_id}")
            kafka_send_cp(prod, topics.EV_SUPPLY_STARTED, cp_id, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "status": "REJECTED"
            })
            audit_log("KAFKA", "SUPPLY_STARTED_SENT", f"driver_id={driver_id} cp_id={cp_id} status=REJECTED")
            return

        kafka_send_cp(prod, topics.EV_SUPPLY_STARTED, cp_id, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": "APPROVED"
        })
        audit_log("KAFKA", "SUPPLY_STARTED_SENT", f"driver_id={driver_id} cp_id={cp_id} status=APPROVED")

        db.setCpState(cp_id, "SUPPLYING")
        audit_log("KAFKA", "CP_STATE_CHANGE", f"cp_id={cp_id} AVAILABLE->SUPPLYING")

        session_id = f"{driver_id}_{cp_id}"
        active_sessions[session_id] = {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "start_time": datetime.now(timezone.utc),
            "energy_kwh": 0.0
        }
        audit_log("KAFKA", "SESSION_START", f"session_id={session_id}")

        db.upsertSession({
            "driver_id": driver_id,
            "cp_id": cp_id,
            "start_time": datetime.now(timezone.utc),
            "energy_kwh": 0.0
        })
        audit_log("KAFKA", "SESSION_UPSERT_DB", f"session_id={session_id}")

    elif topic == topics.EV_SUPPLY_HEARTBEAT:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")
        power_kw = data.get("power_kw", 0.0)
        energy_kwh = data.get("energy_kwh", 0.0)
        timestamp = data.get("timestamp", 0)

        session_id = f"{driver_id}_{cp_id}"
        if session_id in active_sessions:
            last_energy = active_sessions[session_id].get("energy_kwh", 0.0)
            delta = max(0, energy_kwh - last_energy)
            active_sessions[session_id]["energy_kwh"] = last_energy + delta
            db.updateSessionEnergy(driver_id, cp_id, active_sessions[session_id]["energy_kwh"])

            audit_log("KAFKA", "SUPPLY_HEARTBEAT",
                      f"driver_id={driver_id} cp_id={cp_id} power_kw={power_kw} energy_kwh={energy_kwh} delta={round(delta,3)} ts={timestamp}")

            kafka_utils.send(prod, topics.EV_DRIVER_SUPPLY_HEARTBEAT, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "power_kw": power_kw,
                "energy_kwh": energy_kwh,
                "timestamp": timestamp
            })
            audit_log("KAFKA", "DRIVER_HEARTBEAT_FORWARD", f"driver_id={driver_id} cp_id={cp_id}")
        else:
            print(f"[CENTRAL] Heartbeat recibido de Driver {driver_id} en CP {cp_id} sin sesión activa.")
            audit_log("KAFKA", "INCIDENT_NO_ACTIVE_SESSION", f"driver_id={driver_id} cp_id={cp_id}")

            kafka_utils.send(prod, topics.EV_DRIVER_SUPPLY_ERROR, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "reason": "NO_ACTIVE_SESSION"
            })
            audit_log("KAFKA", "DRIVER_SUPPLY_ERROR_SENT",
                      f"driver_id={driver_id} cp_id={cp_id} reason=NO_ACTIVE_SESSION")

            kafka_send_cp(prod, topics.EV_SUPPLY_END_ENGINE, cp_id, {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "status": "FORCED_END"
            })
            audit_log("KAFKA", "FORCED_END_SENT_TO_ENGINE", f"driver_id={driver_id} cp_id={cp_id}")

            db.setCpState(cp_id, "FUERA DE SERVICIO")
            audit_log("KAFKA", "CP_STATE_CHANGE", f"cp_id={cp_id} -> FUERA_DE_SERVICIO (FORCED_END)")

    elif topic == topics.EV_SUPPLY_END:
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")

        audit_log("KAFKA", "SUPPLY_END", f"driver_id={driver_id} cp_id={cp_id}")
        print(f"[CENTRAL] Driver {driver_id} ha terminado la recarga en CP {cp_id}")

        cp_state = db.getCp(cp_id).get("state", "UNKNOWN").upper()
        if cp_state != "FUERA DE SERVICIO":
            db.setCpState(cp_id, "AVAILABLE")
            audit_log("KAFKA", "CP_STATE_CHANGE", f"cp_id={cp_id} SUPPLYING->AVAILABLE")
        else:
            audit_log("KAFKA", "CP_STATE_KEEP", f"cp_id={cp_id} state=FUERA_DE_SERVICIO")

        session_id = f"{driver_id}_{cp_id}"
        session = active_sessions.get(session_id)

        if session:
            start_time = session.get("start_time")
            energy_total = session.get("energy_kwh", 0.0)
            duration = datetime.now(timezone.utc) - start_time
            hours = duration.total_seconds() / 3600
            audit_log("KAFKA", "SESSION_END",
                      f"session_id={session_id} energy_kwh={round(energy_total,3)} hours={round(hours,4)}")
        else:
            energy_total = 0.0
            hours = 0.0
            audit_log("KAFKA", "SESSION_END", f"session_id={session_id} session_no_en_memoria")

        cp_price = db.getCp(cp_id).get("price", 0.3)
        total_price = energy_total * cp_price

        kafka_utils.send(prod, topics.EV_SUPPLY_TICKET, {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "price": round(total_price, 2),
            "energy_kwh": round(energy_total, 3)
        })
        audit_log("KAFKA", "TICKET_ISSUED",
                  f"driver_id={driver_id} cp_id={cp_id} price={round(total_price,2)} energy_kwh={round(energy_total,3)}")

        active_sessions.pop(session_id, None)
        audit_log("KAFKA", "SESSION_REMOVED_MEMORY", f"session_id={session_id}")
        db.deleteSession(driver_id, cp_id)
        audit_log("KAFKA", "SESSION_DELETED_DB", f"driver_id={driver_id} cp_id={cp_id}")


def tcpServer(s, kafkaInfo):
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handleClient, args=(conn, addr, kafkaInfo), daemon=True)
        t.start()

def kafkaListener(kafkaInfo):
    kafkaProducer = kafka_utils.buildProducer(kafkaInfo)
    kafkaConsumer = kafka_utils.buildConsumer(
        kafkaInfo,
        "central-driver",
        [
            topics.EV_SUPPLY_CONNECTED,
            topics.EV_SUPPLY_END,
            topics.EV_SUPPLY_REQUEST,
            topics.EV_SUPPLY_HEARTBEAT
        ]
    )
    print("[CENTRAL] Esperando mensajes...")
    audit_log("LOCAL", "KAFKA_LISTENER_START", "group=central-driver")

    kafka_utils.pollLoop(
        kafkaConsumer,
        lambda topic, data: handleDriver(topic, data, kafkaProducer)
    )

def load_active_sessions():
    sessions_in_db = db.sessions.find()
    for session in sessions_in_db:
        session_id = f"{session['driver_id']}_{session['cp_id']}"
        if isinstance(session["start_time"], datetime) and session["start_time"].tzinfo is None:
            session["start_time"] = session["start_time"].replace(tzinfo=timezone.utc)
        active_sessions[session_id] = {
            "driver_id": session["driver_id"],
            "cp_id": session["cp_id"],
            "start_time": session["start_time"],
            "energy_kwh": session["energy_kwh"]
        }
        print(f"[CENTRAL] Cargada sesión desde base de datos: {session_id}")
        audit_log("LOCAL", "SESSION_LOADED_DB",
                  f"session_id={session_id} energy_kwh={session.get('energy_kwh')} start_time={session.get('start_time')}")

def main():
    if len(sys.argv) != 3:
        print("Uso: central.py <puerto> <kafka_ip:port>")
        audit_log("LOCAL", "STARTUP_FAIL", "args_invalidos")
        return

    port = int(sys.argv[1])
    kafkaInfo = sys.argv[2]

    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(5)
    print(f"[CENTRAL] En escucha permanente en {port}")
    audit_log("LOCAL", "CENTRAL_START", f"port={port} kafka={kafkaInfo}")

    load_active_sessions()
    printCpPanel()

    threading.Thread(target=tcpServer, args=(s, kafkaInfo), daemon=True).start()
    audit_log("LOCAL", "THREAD_START", "tcpServer")
    threading.Thread(target=kafkaListener, args=(kafkaInfo,), daemon=True).start()
    audit_log("LOCAL", "THREAD_START", "kafkaListener")
    threading.Thread(target=heartbeat_loop, args=(kafkaInfo,), daemon=True).start()
    audit_log("LOCAL", "THREAD_START", "heartbeat_loop")

    print("\n[COMANDOS CENTRAL ACTIVADOS]")
    print("  stop <cp_id>   → Detiene carga, genera ticket y pasa a FUERA DE SERVICIO")
    print("  enable <cp_id> → Vuelve a poner el CP en AVAILABLE\n")

    while True:
        try:
            raw = input().strip()
            if not raw:
                continue
            cmd = raw.split()
            audit_log("LOCAL", "ADMIN_CMD", f"cmd={raw}")

            if len(cmd) == 2:
                if cmd[0].lower() == "stop":
                    stopCP(cmd[1], kafkaInfo)
                elif cmd[0].lower() == "enable":
                    enableCP(cmd[1])
                else:
                    print("[CENTRAL] Comando no reconocido.")
                    audit_log("LOCAL", "ADMIN_CMD_UNKNOWN", f"cmd={raw}")
            else:
                print("[CENTRAL] Formato inválido. Usa: stop <id> o enable <id>")
                audit_log("LOCAL", "ADMIN_CMD_INVALID_FORMAT", f"cmd={raw}")
        except KeyboardInterrupt:
            print("\n[CENTRAL] Saliendo...")
            audit_log("LOCAL", "CENTRAL_STOP", "KeyboardInterrupt")
            break
        except Exception as e:
            print(f"[CENTRAL] Error procesando comando: {e}")
            audit_log("LOCAL", "ADMIN_CMD_ERROR", f"err={repr(e)}")


main()
