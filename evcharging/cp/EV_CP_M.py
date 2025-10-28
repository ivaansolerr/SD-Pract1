import sys, socket, time, threading
from evcharging import topics, kafka_utils, utils
from confluent_kafka import Producer, Consumer

def tcp_healthcheck(engine_host, engine_port, timeout=1.0):
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

    bootstrap_servers = f"{central_host}:{central_port}"

    # --- 🔸 Crear productor y consumidor Kafka ---
    prod = kafka_utils.build_producer(bootstrap_servers)
    cons = kafka_utils.build_consumer(bootstrap_servers, f"monitor-{cp_id}", [
        topics.EV_AUTH_RESULT
    ])

    # --- 🔸 Registro + solicitud de autenticación con CENTRAL ---
    kafka_utils.send(prod, topics.EV_REGISTER, {"id": cp_id})
    kafka_utils.send(prod, topics.EV_AUTH_REQUEST, {
        "cp_id": cp_id,
        "engine_host": engine_host,
        "engine_port": engine_port,
        "central_host": central_host,
        "central_port": central_port
    })

    # Estado de conexión del Engine
    last_state_ok = None

    def loop_healthcheck():
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

    # --- 🔸 Hilo de healthcheck
    threading.Thread(target=loop_healthcheck, daemon=True).start()

    # --- 🔸 Handler de mensajes Kafka
    def handler(topic, data):
        if topic == topics.EV_AUTH_RESULT and data.get("cp_id") == cp_id:
            status = data.get("status", "DENIED")
            if status == "APPROVED":
                utils.ok(f"[MONITOR {cp_id}] Autenticación con CENTRAL APROBADA ✅")
            else:
                utils.err(f"[MONITOR {cp_id}] Autenticación con CENTRAL DENEGADA ❌")

            # Notificar al Engine el resultado de la autenticación
            kafka_utils.send(prod, topics.EV_AUTH_RESULT_ENG, {
                "cp_id": cp_id,
                "status": status
            })

    # --- 🔸 Bucle principal Kafka (consumer)
    kafka_utils.poll_loop(cons, handler)


if __name__ == "__main__":
    main()
