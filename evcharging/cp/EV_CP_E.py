import threading, socket, time, json, sys, random
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

registered_cp: str | None = None
registered_cp_event = threading.Event()
prod = None
stop_event = threading.Event()

def handle(conn):
    global registered_cp
    #print("[ENGINE] Conectando con monitor...")
    
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
    
    conn.send(socketCommunication.ACK) # siguiendo el protocolo
    conn.send(socketCommunication.encodeMess("OK")) # todo está OK

    if conn.recv(1024) != socketCommunication.ACK: 
        print("[ENGINE] Error: No recibió ACK final")
        conn.close() 
        return
    
    conn.send(b"<EOT>")
    print(f"[ENGINE {registered_cp}] Registrado, esperando suministro/heartbeat")

    while True:
        try:
            beat = conn.recv(1024)
            if not beat:
                break

            if beat == b"PING":
                conn.send(b"PONG")

            elif beat == b"CENTRAL_DOWN":
                print("[ENGINE] ⚠️ CENTRAL caída detectada")
                print("[ENGINE] 🚨 Actuando en modo aislamiento...")

            elif beat == b"CENTRAL_UP_AGAIN":
                print("[ENGINE] ✅ CENTRAL ha vuelto")
                print("[ENGINE] 🔄 Reanudando coordinación con CENTRAL")

        except Exception:
            break


    print("[ENGINE] Monitor desconectado")
    conn.close()

def send_supply_heartbeat(prod, driver_id: str):
    energy = 0.0
    while not stop_event.is_set():
        consumption = round(random.uniform(0.5, 2.5), 2)  # kW instantáneo simulado
        energy += consumption * (3 / 3600)  # kWh cada 3 segundos
        kafka_utils.send(prod, topics.EV_SUPPLY_HEARTBEAT, {
            "timestamp": int(time.time() * 1000),
            "cp_id": registered_cp,
            "driver_id": driver_id,
            "power_kw": consumption,
            "energy_kwh": round(energy, 3)
        })
        time.sleep(3)

def handleRequest(topic, data):
    if topic == topics.EV_SUPPLY_AUTH:
        if (data.get("authorized") == True) and (data.get("cp_id") == registered_cp):
            utils.ok(f"[ENGINE] Autorización aprobada para driver {data.get('driver_id')}")
            # print("[OPCIONES] \n"
            #       "1. Iniciar suministro\n2. Rechazar")
            # choice = input("Seleccione una opción: ")
            # if choice == "1":
            utils.info(f"[ENGINE] Iniciando suministro para driver {data.get('driver_id')}")
            kafka_utils.send(prod, topics.EV_SUPPLY_CONNECTED, {
                "driver_id": data.get("driver_id"),
                "cp_id": registered_cp,
                "status": "CONNECTED"
            })
            stop_event.clear()
            heartbeat_thread = threading.Thread(target=send_supply_heartbeat, args=(prod, data.get("driver_id")), daemon=True)
            heartbeat_thread.start()
            print("Ingrese 'FIN' para finalizar el suministro cuando desee.")
            input_cmd = input().strip()
            if input_cmd.upper() == "FIN":
                stop_event.set()
                heartbeat_thread.join(1)
                kafka_utils.send(prod, topics.EV_SUPPLY_END, {
                    "driver_id": data.get("driver_id"),
                    "cp_id": registered_cp
                })
                utils.info(f"[ENGINE] Suministro finalizado para driver {data.get('driver_id')}")
            # else:
            #     utils.err(f"[ENGINE] Rechazando suministro para driver {data.get('driver_id')}")
            #     kafka_utils.send(prod, topics.EV_SUPPLY_CONNECTED, {
            #         "driver_id": data.get("driver_id"),
            #         "cp_id": registered_cp,
            #         "status": "REJECTED"
            #     })

def socketServer(socketPort):
    s = socket.socket()
    s.bind(("0.0.0.0", int(socketPort)))
    s.listen(1)
    print(f"[ENGINE] Esperando monitor en puerto {socketPort}...")

    while True:
        conn, _ = s.accept()
        threading.Thread(target=handle, args=(conn,)).start()

def kafkaListener(kafkaIp, kafkaPort):
    kafka_info = f"{kafkaIp}:{kafkaPort}"
    global prod
    registered_cp_event.wait()
    prod = kafka_utils.buildProducer(kafka_info)
    kafka_consumer = kafka_utils.buildConsumer(kafka_info, f"engine-{registered_cp}", [
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