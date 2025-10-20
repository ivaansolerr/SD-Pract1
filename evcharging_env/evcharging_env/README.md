
# EVCharging (ENV-based) — Kafka + Mongo + Sockets

## 1) Pre-reqs
```bash
pip install confluent-kafka pymongo python-dotenv
```
Asegúrate de que tu `docker-compose` expone Kafka y Mongo en la red (no solo localhost).

## 2) Configura `.env`
Edita el archivo `.env` (incluido como ejemplo) en **cada máquina** con las IPs reales:
- `KAFKA_BOOTSTRAP_SERVERS=IP_KAFKA:9092`
- `MONGO_URI=mongodb://IP_MONGO:27017`
- En la **central**: `CENTRAL_LISTEN_HOST=0.0.0.0` y `CENTRAL_LISTEN_PORT=5051`
- En los **CPs**: `CENTRAL_PUBLIC_HOST=IP_CENTRAL` y `CENTRAL_PUBLIC_PORT=5051`

## 3) Arranque por máquinas

### Máquina CENTRAL
```bash
python central.py
```
**CLI admin** (en la consola de Central):
```
STOP_CP CP-001
RESUME_CP CP-001
SHUTDOWN_SYSTEM
EXIT
```

### Máquina CP (Engine + Monitor)
Terminal 1 (Engine):
```bash
python cp_engine.py      # edita cp_id/price en main()
```
Terminal 2 (Monitor):
```bash
python cp_monitor.py     # edita cp_id/location/price en main()
```

### Máquina DRIVER
```bash
python driver.py         # edita driver_id/cp_id en main()
```

## 4) Flujo
- Monitor se registra por **socket** en Central.
- Driver pide carga → Central autoriza → envía **START_SUPPLY** por Kafka a Engine.
- Engine emite **telemetría** → Central acumula en Mongo → al finalizar, emite **billing**.
- Central publica a `central-events` el **ticket** para el Driver.

## 5) Consejos Kafka
- Usa `KAFKA_ADVERTISED_LISTENERS` con la IP del host donde corre Kafka para clientes externos.
