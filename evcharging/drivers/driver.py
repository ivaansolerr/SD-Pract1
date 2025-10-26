import argparse, time, sys
from .. import config, topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

def main():
    parser = argparse.ArgumentParser(prog="EV_Driver")
    parser.add_argument("--driver-id", required=True)
    parser.add_argument("--cp", help="CP objetivo")
    parser.add_argument("--file", help="Ruta a fichero con IDs de CP (uno por línea)")
    parser.add_argument("--kafka", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    prod: Producer = kafka_utils.build_producer(args.kafka)
    cons: Consumer = kafka_utils.build_consumer(args.kafka, f"driver-{args.driver_id}", [topics.EV_SUPPLY_AUTH])

    def request(cp_id: str):
        utils.info(f"[DRIVER {args.driver_id}] Solicito recarga en {cp_id}")
        kafka_utils.send(prod, topics.EV_SUPPLY_REQUEST, {"driver_id": args.driver_id, "cp_id": cp_id})
        # Esperar respuesta de auth (simple)
        start = time.time()
        authorized = None; reason = ""
        while time.time() - start < 10:
            msg = cons.poll(1.0)
            if not msg: continue
            if msg.error(): continue
            import json
            data = json.loads(msg.value().decode("utf-8"))
            if data.get("driver_id") == args.driver_id and data.get("cp_id") == cp_id:
                authorized = data.get("authorized"); reason = data.get("reason", "")
                break
        if authorized is None:
            utils.err("[DRIVER] No response from CENTRAL")
        elif authorized:
            utils.ok("[DRIVER] Autorizado. Esperando a que el CP empiece el suministro...")
        else:
            utils.err(f"[DRIVER] Denegado: {reason}")

    if args.file:
        with open(args.file, "r") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        for cp_id in lines:
            request(cp_id)
            time.sleep(config.DRIVER_NEXT_DELAY_SEC)
    else:
        if not args.cp:
            print("Debes indicar --cp o --file")
            sys.exit(1)
        request(args.cp)

if __name__ == "__main__":
    main()