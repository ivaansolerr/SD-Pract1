import threading, socket, time, json, sys
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

registered_cp: str | None = None
registered_cp_event = threading.Event()
prod = None

def handle(conn):
    global registered_cp
    print("[ENGINE] Conectado monitor")

    if conn.recv(1024) != b"<ENC>": conn.close(); return
    conn.send(ACK)

    cp = socketCommunication.parseFrame(conn.recv(1024))
    if cp is None: conn.send(NACK); conn.close(); return

    registered_cp = cp
    registered_cp_event.set() 
    print(f"[ENGINE] CP registrado: {registered_cp}")
    conn.send(ACK)
    conn.send(socketCommunication.encodeMess("OK"))

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

def handle_request(topic, data):
    if topic == topics.EV_SUPPLY_AUTH:
        if(data.get("authorized") == True and data.get("cp_id") == registered_cp):
            utils.ok(f"[ENGINE] Autorización aprobada para driver {data.get('driver_id')}")
            print("[OPCIONES] \n"
                  "1. Iniciar suministro\n2. Rechazar")
            choice = input("Seleccione una opción: ")
            if choice == "1":
                utils.info(f"[ENGINE] Iniciando suministro para driver {data.get('driver_id')}")
                kafka_utils.send(prod, topics.EV_SUPPLY_CONNECTED, {
                    "driver_id": data.get("driver_id"),
                    "cp_id": registered_cp,
                    "status": "CONNECTED"
                })
                print("Ingrese 'FIN' para finalizar el suministro cuando desee.")
                input_cmd = input().strip()
                if input_cmd.upper() == "FIN":
                    kafka_utils.send(prod, topics.EV_SUPPLY_END, {
                        "driver_id": data.get("driver_id"),
                        "cp_id": registered_cp
                    })
                    utils.info(f"[ENGINE] Suministro finalizado para driver {data.get('driver_id')}")
            else:
                utils.err(f"[ENGINE] Rechazando suministro para driver {data.get('driver_id')}")
                kafka_utils.send(prod, topics.EV_SUPPLY_CONNECTED, {
                    "driver_id": data.get("driver_id"),
                    "cp_id": registered_cp,
                    "status": "REJECTED"
                })

def socket_server(socket_port):
    s = socket.socket()
    s.bind(("0.0.0.0", int(socket_port)))
    s.listen(1)
    print(f"[ENGINE] Esperando monitor en puerto {socket_port}...")

    while True:
        conn, _ = s.accept()
        threading.Thread(target=handle, args=(conn,)).start()

def kafka_listener(kafka_ip, kafka_port):
    kafka_info = f"{kafka_ip}:{kafka_port}"
    global prod
    registered_cp_event.wait()
    prod = kafka_utils.build_producer(kafka_info)
    kafka_consumer = kafka_utils.build_consumer(kafka_info, f"engine-{registered_cp}", [
        topics.EV_SUPPLY_AUTH,
        topics.EV_SUPPLY_CONNECTED,
        topics.EV_SUPPLY_END,
        topics.EV_SUPPLY_REQUEST
    ])
    print(f"[ENGINE {registered_cp}] Escuchando mensajes de CENTRAL...")
    while True:
        msg = kafka_consumer.poll(0.1)
        if not msg or msg.error():
            continue
        data = json.loads(msg.value().decode("utf-8"))
        handle_request(msg.topic(), data)

def main():
    if len(sys.argv) != 4:
        print("Uso: engine.py <kafka_ip> <kafka_port> <socket_port>")
        return

    kafka_ip = sys.argv[1]
    kafka_port = sys.argv[2]
    socket_port = sys.argv[3]

    t1 = threading.Thread(target=socket_server, args=(socket_port,), daemon=True)
    t2 = threading.Thread(target=kafka_listener, args=(kafka_ip, kafka_port), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

main()