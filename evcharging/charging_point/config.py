import os
from dotenv import load_dotenv

load_dotenv(override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")  # <--- CAMBIA
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/evcharging_db")     # <--- CAMBIA

CENTRAL_HOST = os.getenv("CENTRAL_HOST", "0.0.0.0")
CENTRAL_PORT = int(os.getenv("CENTRAL_PORT", "7000"))

CP_ENGINE_HOST = os.getenv("CP_ENGINE_HOST", "0.0.0.0")
CP_ENGINE_PORT = int(os.getenv("CP_ENGINE_PORT", "7100"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_PRICE_EUR_KWH = float(os.getenv("DEFAULT_PRICE_EUR_KWH", "0.30"))
DRIVER_NEXT_DELAY_SEC = int(os.getenv("DRIVER_NEXT_DELAY_SEC", "4"))