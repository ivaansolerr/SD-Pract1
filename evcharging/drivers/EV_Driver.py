import sys, time, threading, json
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

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
        return

    else:
        utils.info(f"[DRIVER {clientId}] Mensaje Kafka desconocido en {topic}: {data}")


def kafka_listener(kafka, clientId):
    prod = kafka_utils.buildProducer(kafka)
    cons = kafka_utils.buildConsumer(kafka, f"driver-{clientId}", [
        topics.EV_SUPPLY_TICKET,
        topics.EV_SUPPLY_STARTED,
        topics.EV_SUPPLY_AUTH_DRI
    ])

    print(f"[DRIVER {clientId}] Esperando mensajes Kafka...")

    kafka_utils.pollLoop(
        cons,
        lambda topic, data: handle_kafka_message(topic, data, prod, clientId)
    )

def main():
    if len(sys.argv) != 4:
        print("Uso: python EV_Driver.py <kafkaIp:KafkaPort> <clientId> <fileName>")
        sys.exit(1)

    kafka = sys.argv[1]
    clientId = sys.argv[2]
    fileName = sys.argv[3]

    prod = kafka_utils.buildProducer(kafka)

    # Lanzamos el listener de Kafka en paralelo
    threading.Thread(target=kafka_listener, args=(kafka, clientId), daemon=True).start()

    # Peticiones de recarga leídas del archivo
    with open(fileName, "r") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    for cp_id in lines:
        utils.info(f"[DRIVER {clientId}] Solicito recarga en {cp_id}")
        kafka_utils.send(prod, topics.EV_SUPPLY_REQUEST, {
            "driver_id": clientId,
            "cp_id": cp_id
        })
        time.sleep(10)
    
    #time.sleep(5)  # Esperamos a recibir los tickets antes de salir
    while True:
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[DRIVER] Finalizado por usuario")
