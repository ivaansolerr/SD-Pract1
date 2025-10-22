import argparse, socket, time, threading
from . import config, topics, kafka_utils, utils
from confluent_kafka import Producer

def tcp_healthcheck(engine_host: str, engine_port: int, timeout=1.0) -> bool:
    try:
        with socket.create_connection((engine_host, engine_port), timeout=timeout) as s:
            s.sendall(b"PING")
            data = s.recv(8)
            return data == b"OK"
    except Exception:
        return False

def main():
    parser = argparse.ArgumentParser(prog="EV_CP_M")
    parser.add_argument("--id", required=True, help="ID del CP (único en la red)")
    parser.add_argument("--engine-host", required=True)
    parser.add_argument("--engine-port", type=int, required=True)
    parser.add_argument("--central-host", required=True)
    parser.add_argument("--central-port", type=int, required=True)
    parser.add_argument("--location", type=str, default="Unknown")
    parser.add_argument("--price", type=float, default=config.DEFAULT_PRICE_EUR_KWH)
    args = parser.parse_args()

    prod: Producer = kafka_utils.build_producer(config.KAFKA_BOOTSTRAP_SERVERS)

    # Registro + autenticación
    kafka_utils.send(prod, topics.EV_REGISTER, {
        "id": args.id,
        "location": args.location,
        "price_eur_kwh": args.price,
    })
    kafka_utils.send(prod, topics.EV_AUTH_REQUEST, {
        "cp_id": args.id,
        "engine_host": args.engine_host,
        "engine_port": args.engine_port,
    })
    utils.ok(f"[MONITOR {args.id}] Registrado y autenticación solicitada a CENTRAL")

    last_state_ok = None

    def loop():
        nonlocal last_state_ok
        while True:
            ok = tcp_healthcheck(args.engine_host, args.engine_port, timeout=1.0)
            if ok != last_state_ok:
                status = "OK" if ok else "KO"
                kafka_utils.send(prod, topics.EV_HEALTH, {"id": args.id, "status": status})
                if ok:
                    utils.ok(f"[MONITOR {args.id}] Health RECOVERED")
                    kafka_utils.send(prod, topics.EV_HEALTH, {"id": args.id, "status": "RECOVERED"})
                else:
                    utils.err(f"[MONITOR {args.id}] Health KO")
                    kafka_utils.send(prod, topics.EV_AVERIA, {
                        "id": args.id,
                        "reason": "ENGINE_UNRESPONSIVE_OR_KO"
                    })
                last_state_ok = ok
            time.sleep(1.0)

    threading.Thread(target=loop, daemon=True).start()

    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
