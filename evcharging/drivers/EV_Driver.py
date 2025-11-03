import sys, time, threading, json, datetime
from datetime import datetime, timezone
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

ticket_event = threading.Event()
central_alive = True
last_heartbeat = 0

def handle_kafka_message(topic, data, prod, clientId):
    if topic == topics.EV_SUPPLY_AUTH_DRI:
        cp_id = data.get("cp_id")
        authorized = data.get("authorized")
        reason = data.get("reason", "")

        if authorized:
            utils.ok(f"[DRIVER {clientId}] Autorizado para recargar en {cp_id} ({reason})")
        else:
            utils.err(f"[DRIVER {clientId}] Recarga DENEGADA en {cp_id}: {reason}")

    elif topic == topics.EV_SUPPLY_STARTED:
        status = data.get("status")
        cp_id = data.get("cp_id")

        if status == "APPROVED":
            utils.ok(f"[DRIVER {clientId}] Carga iniciada en {cp_id}")
        else:
            utils.err(f"[DRIVER {clientId}] Carga RECHAZADA por ENGINE")

    elif topic == topics.EV_SUPPLY_TICKET:
        cp_id = data.get("cp_id")
        price = data.get("price", 0)
        utils.info(f"[DRIVER {clientId}] Ticket recibido de {cp_id}: {price:.6f} EUR")
        ticket_event.set()
        return
    
    elif topic == topics.EV_CENTRAL_HEARTBEAT:
        global central_alive, last_heartbeat
        last_heartbeat = time.time()
        if not central_alive:
            utils.ok(f"[DRIVER {clientId}] CENTRAL ha vuelto a estar activa")
        central_alive = True

    elif topic == topics.EV_DRIVER_SUPPLY_HEARTBEAT:
        cp_id = data.get("cp_id")
        power_kw = data.get("power_kw", 0.0)
        energy_kwh = data.get("energy_kwh", 0.0)
        timestamp = data.get("timestamp", 0)
        hora = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S")
        utils.info(f"[DRIVER {clientId} Consumiendo en {cp_id}: "
                   f"Power: {power_kw} kW, Energy: {energy_kwh} kWh at {hora}")

    else:
        utils.info(f"[DRIVER {clientId}] Mensaje Kafka desconocido en {topic}: {data}")

def heartbeat_monitor():
    global central_alive, last_heartbeat
    while True:
        now = time.time()
        if last_heartbeat != 0 and (now - last_heartbeat) > 5:
            if central_alive:
                utils.err("[DRIVER] CENTRAL no responde")
            central_alive = False
        time.sleep(1)

def kafka_listener(kafka, clientId):
    try:
        prod = kafka_utils.buildProducer(kafka)
        cons = kafka_utils.buildConsumer(kafka, f"driver-{clientId}", [
            topics.EV_SUPPLY_TICKET,
            topics.EV_SUPPLY_STARTED,
            topics.EV_SUPPLY_AUTH_DRI,
            topics.EV_CENTRAL_HEARTBEAT,
            topics.EV_DRIVER_SUPPLY_HEARTBEAT
        ])
    except Exception as e:
        utils.err(f"[DRIVER {clientId}] Error al conectar con Kafka: {e}")
        return

    print(f"[DRIVER {clientId}] Esperando mensajes Kafka...")

    kafka_utils.pollLoop(
        cons,
        lambda topic, data: handle_kafka_message(topic, data, prod, clientId)
    )

def main():
    global central_alive, last_heartbeat
    if len(sys.argv) != 4:
        print("Uso: python EV_Driver.py <kafkaIp:KafkaPort> <clientId> <fileName>")
        sys.exit(1)

    kafka = sys.argv[1]
    clientId = sys.argv[2]
    fileName = sys.argv[3]

    prod = kafka_utils.buildProducer(kafka)

    # Lanzamos el listener de Kafka en paralelo
    threading.Thread(target=kafka_listener, args=(kafka, clientId), daemon=True).start()
    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    timeout = 8
    waited = 0
    while waited < timeout:
        if last_heartbeat != 0:
            utils.ok(f"[DRIVER {clientId}] CENTRAL está activa")
            central_alive = True
            break
        time.sleep(1)
        waited += 1
    
    if not central_alive:
        utils.err(f"[DRIVER {clientId}] CENTRAL no está activa tras {timeout}s, saliendo")
        return

    # Peticiones de recarga leídas del archivo
    with open(fileName, "r") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    for cp_id in lines:
        utils.info(f"[DRIVER {clientId}] Solicito recarga en {cp_id}")
        ticket_event.clear()
        kafka_utils.send(prod, topics.EV_SUPPLY_REQUEST, {
            "driver_id": clientId,
            "cp_id": cp_id
        })
        if ticket_event.wait(timeout=60):
            utils.ok(f"[DRIVER {clientId}] Ticket recibido para {cp_id}")
        else:
            utils.err(f"[DRIVER {clientId}] No se recibió ticket en 60s para {cp_id}")

    time.sleep(5)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[DRIVER] Finalizado por usuario")
