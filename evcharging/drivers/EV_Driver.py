import sys, time, threading, json, datetime
from datetime import datetime, timezone
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

ticket_event = threading.Event()
central_alive = True
last_heartbeat = 0
supply_active = False

def handle_kafka_message(topic, data, prod, clientId):
    global supply_active
    if topic == topics.EV_SUPPLY_AUTH_DRI:
        if data.get("driver_id") != clientId:
            return
        cp_id = data.get("cp_id")
        authorized = data.get("authorized")
        reason = data.get("reason", "")

        if authorized:
            utils.ok(f"[DRIVER {clientId}] Autorizado para recargar en {cp_id} ({reason})")
        else:
            utils.err(f"[DRIVER {clientId}] Recarga DENEGADA en {cp_id}: {reason}")

    elif topic == topics.EV_SUPPLY_STARTED:
        if data.get("driver_id") != clientId:
            return
        status = data.get("status")
        cp_id = data.get("cp_id")

        if status == "APPROVED":
            utils.ok(f"[DRIVER {clientId}] Carga iniciada en {cp_id}")
            supply_active = True
        else:
            utils.err(f"[DRIVER {clientId}] Carga RECHAZADA por ENGINE")
            supply_active = False

    elif topic == topics.EV_SUPPLY_TICKET:
        if data.get("driver_id") != clientId:
            return
        cp_id = data.get("cp_id")
        price = data.get("price", 0)
        utils.info(f"[DRIVER {clientId}] Ticket recibido de {cp_id}: {price:.6f} EUR")
        ticket_event.set()
        supply_active = False
        return
    
    elif topic == topics.EV_CENTRAL_HEARTBEAT:
        global central_alive, last_heartbeat
        last_heartbeat = time.time()
        if not central_alive:
            utils.ok(f"[DRIVER {clientId}] CENTRAL ha vuelto a estar activa")
        central_alive = True

    elif topic == topics.EV_DRIVER_SUPPLY_HEARTBEAT:
        if data.get("driver_id") != clientId:
            return
        cp_id = data.get("cp_id")
        power_kw = data.get("power_kw", 0.0)
        energy_kwh = data.get("energy_kwh", 0.0)
        timestamp = data.get("timestamp", 0)
        hora = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S")
        utils.info(f"[DRIVER {clientId} Consumiendo en {cp_id}: "
                   f"Power: {power_kw} kW, Energy: {energy_kwh} kWh at {hora}")

    elif topic == topics.EV_DRIVER_SUPPLY_ERROR:
        if data.get("driver_id") != clientId:
            return
        cp_id = data.get("cp_id")
        utils.err(f"[DRIVER {clientId}] se ha caído el charging point: {cp_id}")

    elif topic == topics.EV_DRIVER_SUPPLY_ERROR:
        if data.get("driver_id") != clientId:
            return
        cp_id = data.get("cp_id")
        utils.err(f"[DRIVER {clientId}] se ha producido un error durante el suministro en {cp_id}")

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
            topics.EV_DRIVER_SUPPLY_HEARTBEAT,
            topics.EV_DRIVER_SUPPLY_ERROR
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
    global central_alive, last_heartbeat, supply_active
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

    with open(fileName, "r") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    for cp_id in lines:
        utils.info(f"[DRIVER {clientId}] Solicito recarga en {cp_id}")

        ticket_received = False
        for attempt in range(2):
            ticket_event.clear()
            kafka_utils.send(prod, topics.EV_SUPPLY_REQUEST, {
                "driver_id": clientId,
                "cp_id": cp_id
            })
            time.sleep(5)
            timeout = 10 if not supply_active else None
            if ticket_event.wait(timeout=timeout):  # Esperar el tiempo definido
                utils.ok(f"[DRIVER {clientId}] Ticket recibido para {cp_id}")
                ticket_received = True
                break
            else:
                utils.err(f"[DRIVER {clientId}] No se recibió ticket para {cp_id}, intento {attempt + 1}")

        if not ticket_received:
            utils.err(f"[DRIVER {clientId}] No se pudo recibir ticket para {cp_id} tras 2 intentos, pasando al siguiente CP.")
            continue

        time.sleep(5)  # Espera antes de proceder al siguiente paso

    sys.exit(0)  # Termina cuando no haya más CPs



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[DRIVER] Finalizado por usuario")