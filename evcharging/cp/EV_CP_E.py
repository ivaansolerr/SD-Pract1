import socket, sys
import argparse, threading, socket, time, json, sys
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

registered_cp = None

def xor_checksum(data: bytes) -> bytes:
    lrc = 0
    for b in data: lrc ^= b
    return bytes([lrc])

def build_frame(msg: str) -> bytes:
    data = msg.encode()
    return STX + data + ETX + xor_checksum(data)

def parse_frame(frame: bytes):
    if len(frame) < 3 or frame[0] != 2 or frame[-2] != 3:
        return None
    data = frame[1:-2]
    return data.decode() if xor_checksum(data) == frame[-1:] else None

def handle(conn):
    global registered_cp
    print("[ENGINE] Conectado monitor")

    if conn.recv(1024) != b"<ENC>": conn.close(); return
    conn.send(ACK)

    cp = parse_frame(conn.recv(1024))
    if cp is None: conn.send(NACK); conn.close(); return

    registered_cp = cp
    print(f"[ENGINE] CP registrado: {registered_cp}")
    conn.send(ACK)
    conn.send(build_frame("OK"))

    if conn.recv(1024) != ACK: conn.close(); return
    conn.send(b"<EOT>")
    print("[ENGINE] Registrado, esperando heartbeats...")

    while True:
        beat = conn.recv(1024)
        if not beat: break
        if beat == b"PING":
            conn.send(b"PONG")

    print("[ENGINE] Monitor desconectado")
    conn.close()

def main():
    if len(sys.argv) != 3:
        print("Uso: engine.py <kafka_ip> <kafka_port>")
        return

    s = socket.socket()
    s.bind(("0.0.0.0", 7100))
    s.listen(1)
    print("[ENGINE] Esperando monitor en puerto 7100...")

    while True:
        conn, _ = s.accept()
        threading.Thread(target=handle, args=(conn,)).start()

main()