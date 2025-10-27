import argparse, time, sys
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

def main():
    if len(sys.argv) != 4:
        print("Uso: python EV_Driver.py <kafkaIp:KafkaPort> <clientId> <fileName>")
        sys.exit(1)

    kafka = sys.argv[1]
    clientId = sys.argv[2]
    fileName = sys.argv[3]

    prod = kafka_utils.build_producer(kafka)
    cons = kafka_utils.build_consumer(kafka, f"driver-{clientId}", [topics.EV_SUPPLY_AUTH])

    def request(cp_id: str):
        utils.info(f"[DRIVER {clientId}] Solicito recarga en {cp_id}")
        kafka_utils.send(prod, topics.EV_SUPPLY_REQUEST, {"driver_id": clientId, "cp_id": cp_id})
        # Esperar respuesta de auth (simple)
        start = time.time()
        authorized = None; reason = ""
        while time.time() - start < 10:
            msg = cons.poll(1.0)
            if not msg: continue
            if msg.error(): continue
            import json
            data = json.loads(msg.value().decode("utf-8"))
            if data.get("driver_id") == clientId and data.get("cp_id") == cp_id:
                authorized = data.get("authorized"); reason = data.get("reason", "")
                break
        if authorized is None:
            utils.err("[DRIVER] No response from CENTRAL")
        elif authorized:
            utils.ok("[DRIVER] Autorizado. Esperando a que el CP empiece el suministro...")
        else:
            utils.err(f"[DRIVER] Denegado: {reason}")

    # estoy hay que cambiarlo para que lo lea siempre y vigilar extensión
    if fileName:
        with open(fileName, "r") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        for cp_id in lines:
            request(cp_id)
            time.sleep("4") # no sé si es string o int
    else:
        print("Debes indicar el nombre de un archivo")

if __name__ == "__main__":
    main()