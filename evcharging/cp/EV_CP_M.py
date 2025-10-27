import sys, socket, time, threading
from .. import topics, kafka_utils, utils
from confluent_kafka import Producer

def tcp_healthcheck(engine_host, engine_port, timeout = 1.0):
    """Envía un PING al Engine y espera respuesta OK o KO."""
    try:
        with socket.create_connection((engine_host, engine_port), timeout=timeout) as s:
            s.sendall(b"PING")
            data = s.recv(8)
            return data == b"OK"
    except Exception:
        return False

def main():
    # --- 🔸 Verificar argumentos obligatorios ---
    if len(sys.argv) != 6:
        print("Uso: python EV_CP_M.py <engine_ip> <engine_port> <central_ip> <central_port> <cp_id>")
        sys.exit(1)

    engine_host = sys.argv[1]
    engine_port = int(sys.argv[2])
    central_host = sys.argv[3]
    central_port = int(sys.argv[4])
    cp_id = sys.argv[5]

    utils.ok(f"[MONITOR {cp_id}] Iniciado")
    utils.info(f"[MONITOR {cp_id}] Engine: {engine_host}:{engine_port}")
    utils.info(f"[MONITOR {cp_id}] Central: {central_host}:{central_port}")

    # --- 🔸 Crear productor Kafka ---
    prod = kafka_utils.build_producer(central_host+":"+str(central_port))

    # --- 🔸 Registro + autenticación con CENTRAL ---
    kafka_utils.send(prod, topics.EV_REGISTER, {"id": cp_id,})

    # estoy es para auth con central pero este topic no existe
    # kafka_utils.send(prod, topics.EV_AUTH_REQUEST, {
    #     "cp_id": cp_id,
    #     "engine_host": engine_host,
    #     "engine_port": engine_port,
    #     "central_host": central_host,
    #     "central_port": central_port
    # })

    utils.ok(f"[MONITOR {cp_id}] Registrado y autenticación solicitada a CENTRAL")

    last_state_ok = None

    def loop():
        """Bucle principal de monitorización (envío de PINGs al Engine)."""
        nonlocal last_state_ok
        while True:
            ok = tcp_healthcheck(engine_host, engine_port, timeout=1.0)
            if ok != last_state_ok:
                status = "OK" if ok else "KO"
                kafka_utils.send(prod, topics.EV_HEALTH, {"id": cp_id, "status": status})
                if ok:
                    utils.ok(f"[MONITOR {cp_id}] Health RECOVERED")
                    kafka_utils.send(prod, topics.EV_HEALTH, {"id": cp_id, "status": "RECOVERED"})
                else:
                    utils.err(f"[MONITOR {cp_id}] Health KO")
                    kafka_utils.send(prod, topics.EV_HEALTH, {
                        "id": cp_id,
                        "status": "KO",
                        "reason": "ENGINE_UNRESPONSIVE_OR_KO"
                    })

                last_state_ok = ok
            time.sleep(1.0)

    threading.Thread(target=loop, daemon=True).start()

    # Mantener el proceso vivo
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()