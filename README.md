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
2. **Central** (PC-B):  
   ```bash
   export $(grep -v '^#' .env | xargs)
   python -m evcharging.central --port 7000 --kafka ${KAFKA_BOOTSTRAP_SERVERS} --mongo ${MONGO_URI}
   ```
3. **CP #1 (Monitor + Engine)** (PC-C): en 2 terminales diferentes:  
   Engine:  
   ```bash
   export $(grep -v '^#' .env | xargs)
   python -m evcharging.cp_engine --id CP-001 --kafka ${KAFKA_BOOTSTRAP_SERVERS} --host 0.0.0.0 --port 7100
   ```
   Monitor:  
   ```bash
   export $(grep -v '^#' .env | xargs)
   python -m evcharging.cp_monitor --id CP-001 --engine-host <IP_ENGINE> --engine-port 7100 --central-host <IP_CENTRAL> --central-port 7000 --kafka ${KAFKA_BOOTSTRAP_SERVERS}
   ```

4. **Driver** (PC-D o PC-B):  
   - Solicitud única:  
     ```bash
     export $(grep -v '^#' .env | xargs)
     python -m evcharging.driver --driver-id D-001 --cp CP-001 --kafka ${KAFKA_BOOTSTRAP_SERVERS}
     ```
   - Desde fichero (IDs de CP uno por línea):  
     ```bash
     python -m evcharging.driver --driver-id D-001 --file ./requests.txt --kafka ${KAFKA_BOOTSTRAP_SERVERS}
     ```

## Simulación
- En el **Engine**, pulsa **`k`** + Enter para simular KO (avería). Pulsa **`r`** + Enter para recuperar.
- En el **Monitor**, se envía heartbeat TCP cada 1 s al Engine. Si no responde o responde KO, se notifica a la central (estado ROJO). Al recuperar, la central cambia a VERDE.
- La **Central** almacena CPs, Drivers y Sesiones en Mongo:
  - `charging_points`: `{_id, id, location, price, state, updated_at}`
  - `drivers`: `{_id, id, name}`
  - `sessions`: con telemetrías acumuladas y ticket final.

## Dónde cambiar IPs/puertos **(marcado en comentarios)**:
- `.env` (Kafka, Mongo, CENTRAL, CP Engine)
- Flags `--kafka`, `--mongo`, `--host/--port` al ejecutar
- Comentarios `# <--- CAMBIA` dentro del código

## Notas
- Esta es una **base funcional**: puedes extender GUI, más controles, y métricas avanzadas.
- Para la memoria, captura las salidas de consola de cada actor mostrando el flujo completo.





#Readme Custom
Docker compose yp
Crear venv -> instalar requirements
Crear topics
Inicializar base de datos

#Inicializar cada cosa
Ir al root donde está el .env
Exportar: export $(grep -v '^#' .env | xargs)
Ir a al src y ejecutar 
python3 -m core.central
python3 -m charging_point.cp_engine --id CP-001 --kafka $KAFKA_BOOTSTRAP_SERVERS
python3 -m charging_point.cp_monitor --id CP-001 --engine-host <IP_ENGINE> --engine-port 7100 \
  --central-host <IP_CENTRAL> --central-port 7000 --kafka $KAFKA_BOOTSTRAP_SERVERS
python3 -m drivers.driver --driver-id D-001 --cp CP-001 --kafka $KAFKA_BOOTSTRAP_SERVERS

#Listar BBDD
docker exec -it mongo mongosh

show dbs
use evcharging_db
show collections
db.charging_points.find().pretty()

