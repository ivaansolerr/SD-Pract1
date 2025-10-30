import socket, sys
import threading, time
from datetime import datetime, timezone
from . import db
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

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

    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

main()