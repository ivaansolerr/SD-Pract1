# EVCharging (Release 1) — Python reference implementation

Componentes implementados (según enunciado):
- **EV_Central** (`central.py`): gobierna la red, autoriza suministros, monitoriza CPs, controla estados. Guarda en MongoDB.
- **EV_CP_M (Monitor)** (`cp_monitor.py`): autentica el CP en la central, envía heartbeats al Engine por **sockets TCP** y estados a la central por **Kafka**.
- **EV_CP_E (Engine)** (`cp_engine.py`): recibe órdenes de la central por **Kafka**, simula el suministro y emite telemetrías cada segundo; atiende al Monitor por **sockets TCP**.
- **EV_Driver** (`driver.py`): solicita recargas (uno a uno o desde fichero) y muestra el flujo autorizado por la central. Comunicación con la central por **Kafka**.

## Requisitos
- Python 3.10+
- Kafka en marcha (puedes usar el `compose.yml` suministrado).
- MongoDB en marcha (puedes usar el `compose.yml`).
- Instalar dependencias: `pip install -r requirements.txt`

## Configuración
Copia `.env` y **CAMBIA**:
- `KAFKA_BOOTSTRAP_SERVERS` → IP:puerto de tu **broker** (no `localhost` si es remoto).
- `MONGO_URI` → IP/host de Mongo (si no es local).
- Puertos/hosts de CENTRAL y CP Engine si es necesario.

## Tópicos Kafka (por defecto)
- `ev.register` (CP_M → Central): altas/estado CP.
- `ev.health` (CP_M → Central): health/avería/recovery CP.
- `ev.commands` (Central → CP_E): órdenes (START_SUPPLY, STOP_SUPPLY, OUT_OF_ORDER, RESUME).
- `ev.supply.request` (Driver → Central): solicitud de recarga.
- `ev.supply.auth` (Central → Driver): autorización/denegación.
- `ev.supply.start` (Central → CP_E): inicio de suministro.
- `ev.supply.telemetry` (CP_E → Central): kW, € en tiempo real.
- `ev.supply.done` (CP_E → Central): fin de suministro (ticket). Central lo reenvía al Driver.

> Puedes cambiar los nombres en `evcharging/topics.py` si hace falta.

## Ejecución típica en **3 máquinas** (mínimo)
1. **Broker + Mongo** (PC-A): levantar con Docker Compose.
Central: python -m evcharging.cp.EV_CP_E {ip_kafka} 9092
Engine: python -m evcharging.cp.EV_CP_E {ip_kafka} 9092
Monitor: python -m evcharging.cp.EV_CP_M {ip_central} {puerto_central} {ip_engine} {puerto_engine} {id_cp}

##Readme Custom
Docker compose yp
Crear venv -> instalar requirements
Crear topics cambiando IPs
Inicializar base de datos

##Inicializar cada cosa
Ir al root donde está el .env
Exportar: 
```bash
export $(grep -v '^#' .env | xargs)
```

Ir a al src y ejecutar 
```bash
python3 -m core.central
python3 -m charging_point.cp_engine --id CP-001 --kafka $KAFKA_BOOTSTRAP_SERVERS
python3 -m charging_point.cp_monitor \
  --id CP-001 \
  --engine-host 127.0.0.1 \
  --engine-port 7100 \
  --central-host 192.168.1.39 \
  --central-port 7000 \
  --kafka 192.168.1.39:9092
python3 -m drivers.driver --driver-id D-001 --cp CP-001 --kafka $KAFKA_BOOTSTRAP_SERVERS
```

#Listar BBDD
```bash
docker exec -it mongo mongosh
```
```bash
show dbs
use evcharging_db
show collections
db.charging_points.find().pretty()
```

