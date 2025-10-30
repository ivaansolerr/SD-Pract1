import socket
import sys
import time
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils  # si no usás estos, podés quitarlos
from confluent_kafka import Producer, Consumer

STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

def xor_checksum(data: bytes) -> bytes:
    """Calcula el checksum XOR simple (LRC)"""
    lrc = 0
    for b in data:
        lrc ^= b
    return bytes([lrc])

def build_frame(msg: str) -> bytes:
    """Crea un frame con STX, ETX y checksum"""
    data = msg.encode()
    return STX + data + ETX + xor_checksum(data)

def parse_frame(frame: bytes):
    """Valida y extrae datos de un frame"""
    if len(frame) < 3 or frame[0] != 2 or frame[-2] != 3:
        return None
    data = frame[1:-2]
    return data.decode() if xor_checksum(data) == frame[-1:] else None

def handshake(sock, cp, name=""):
    """Ejecuta el protocolo de autenticación con CENTRAL o ENGINE"""
    try:
        sock.settimeout(5)
        sock.send(b"<ENC>")
        if sock.recv(1024) != ACK:
            print(f"[MONITOR] {name} no envió ACK tras <ENC>")
            return False

        sock.send(build_frame(cp))
        if sock.recv(1024) != ACK:
            print(f"[MONITOR] {name} no envió ACK tras CP_ID")
            return False

        ans = parse_frame(sock.recv(1024))
        if ans != "OK":
            print(f"[MONITOR] {name} rechazó autenticación ({ans})")
            return False

        sock.send(ACK)
        if sock.recv(1024) != b"<EOT>":
            print(f"[MONITOR] {name} no envió <EOT>")
            return False

        return True
    except socket.timeout:
        print(f"[MONITOR] Timeout durante handshake con {name}")
        return False
    except Exception as e:
        print(f"[MONITOR] Error en handshake con {name}: {e}")
        return False

def connect_with_retry(ip, port, name, retries=5, wait=3):
    """Intenta conectar con reintentos y pausas"""
    for attempt in range(1, retries + 1):
        try:
            sock = socket.socket()
            sock.settimeout(5)
            sock.connect((ip, port))
            print(f"[MONITOR] Conectado a {name} ({ip}:{port})")
            return sock
        except Exception as e:
            print(f"[MONITOR] Intento {attempt}/{retries} falló con {name}: {e}")
            time.sleep(wait)
    return None

def main():
    if len(sys.argv) != 6:
        print("Uso: monitor.py <ipCentral> <portCentral> <ipEngine> <portEngine> <CP_ID>")
        return

    ipC, pC = sys.argv[1], int(sys.argv[2])
    ipE, pE = sys.argv[3], int(sys.argv[4])
    cp = sys.argv[5]

    # === Conexión indefinida con CENTRAL ===
    ko_sent = False  # opcional: si queremos marcar que ya enviamos KO por fallo de handshake
    failed_attempts = 0

    while True:
        try:
            sc = socket.socket()
            sc.settimeout(5)
            sc.connect((ipC, pC))
            print(f"[MONITOR] Conectado a CENTRAL ({ipC}:{pC})")

            if handshake(sc, cp, "CENTRAL"):
                print("[MONITOR] ✅ CP validado por CENTRAL")
                break  # handshake exitoso, seguimos al Engine
            else:
                failed_attempts += 1
                print(f"[MONITOR] Fallo de handshake con CENTRAL ({failed_attempts})")
        except Exception as e:
            failed_attempts += 1
            print(f"[MONITOR] Error conectando con CENTRAL: {e}")

        # Retardo entre reintentos
        time.sleep(5)

    # === Conexión y reintentos con ENGINE indefinidamente ===
    failed_attempts = 0
    ko_sent = False

    while True:
        se = connect_with_retry(ipE, pE, "ENGINE", retries=1, wait=2)
        if se and handshake(se, cp, "ENGINE"):
            print("[MONITOR] ✅ CP registrado en ENGINE")

            # Si antes habíamos enviado KO, ahora notificamos recuperación
            if ko_sent:
                try:
                    sc.send(b"OK")  # mensaje de recuperación a Central
                    print("[MONITOR] ✅ OK enviado a CENTRAL: CP recuperado")
                    ko_sent = False
                except Exception as e:
                    print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")

            failed_attempts = 0
            break  # listo para iniciar heartbeat
        else:
            failed_attempts += 1
            print(f"[MONITOR] Fallo handshake/conexión con ENGINE ({failed_attempts}/5)")
            if failed_attempts >= 5 and not ko_sent:
                try:
                    sc.send(b"KO")
                    print("[MONITOR] ⚠️ KO enviado a CENTRAL tras 5 fallos consecutivos con ENGINE")
                    ko_sent = True
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
            time.sleep(2)

    # === 3️⃣ Heartbeat loop ===
    se.settimeout(3)
    heartbeat_interval = 2
    engine_alive = True  # Estado inicial: ENGINE está respondiendo

    while True:
        try:
            se.send(b"PING")
            resp = se.recv(1024)
            if resp != b"PONG":
                raise socket.timeout

            # Si antes estaba caído y ahora responde, avisamos a CENTRAL
            if not engine_alive:
                try:
                    sc.send(b"OK")
                    print("[MONITOR] OK enviado a CENTRAL (ENGINE volvió a responder)")
                except Exception as e:
                    print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")
                engine_alive = True  # Marcamos que volvió

            print("[MONITOR] Heartbeat OK")

        except (socket.timeout, ConnectionError):
            # Si antes estaba bien y ahora falla, mandamos KO
            if engine_alive:
                print("[MONITOR] :( Timeout esperando PONG de ENGINE → avisar CENTRAL")
                try:
                    sc.send(b"KO")
                    print("[MONITOR] KO enviado a CENTRAL por caída del ENGINE")
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
                engine_alive = False  # Marcamos que está caído

            # Si ya estaba caído, no repetimos el KO
            else:
                # === Conexión y reintentos con ENGINE indefinidamente ===
                failed_attempts = 0
                ko_sent = False
            
                while True:
                    se = connect_with_retry(ipE, pE, "ENGINE", retries=1, wait=2)
                    if se and handshake(se, cp, "ENGINE"):
                        print("[MONITOR] ✅ CP registrado en ENGINE")
            
                        # Si antes habíamos enviado KO, ahora notificamos recuperación
                        if ko_sent:
                            try:
                                sc.send(b"OK")  # mensaje de recuperación a Central
                                print("[MONITOR] ✅ OK enviado a CENTRAL: CP recuperado")
                                ko_sent = False
                            except Exception as e:
                                print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")
            
                        failed_attempts = 0
                        break  # listo para iniciar heartbeat
                    else:
                        failed_attempts += 1
                        print(f"[MONITOR] Fallo handshake/conexión con ENGINE ({failed_attempts}/5)")
                        if failed_attempts >= 5 and not ko_sent:
                            try:
                                sc.send(b"KO")
                                print("[MONITOR] ⚠️ KO enviado a CENTRAL tras 5 fallos consecutivos con ENGINE")
                                ko_sent = True
                            except Exception as e:
                                print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
                        time.sleep(2)

        time.sleep(heartbeat_interval)

    # === 4️⃣ Cierre ordenado ===
    sc.close()
    se.close()
    print("[MONITOR] Finalizado.")

if __name__ == "__main__":
    main()