from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv

load_dotenv()

# 🔧 Usa el MONGO_URI de tu .env
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/evcharging_db")

client = MongoClient(MONGO_URI)
db = client.get_default_database()

# 1️⃣ Crear colecciones (Mongo las crea automáticamente al insertar, pero forzamos aquí)
db.create_collection("charging_points")
db.create_collection("drivers")
db.create_collection("sessions")

# 2️⃣ Crear índices
db.charging_points.create_index([("id", ASCENDING)], unique=True)
db.drivers.create_index([("id", ASCENDING)], unique=True)
db.sessions.create_index([("driver_id", ASCENDING), ("cp_id", ASCENDING), ("start_time", ASCENDING)])

print("✅ Base de datos 'evcharging_db' configurada correctamente.")

# 3️⃣ (Opcional) insertar CP inicial
db.charging_points.insert_one({
    "id": "CP-001",
    "location": "UA-Lab",
    "price_eur_kwh": 0.30,
    "state": "DISCONNECTED",
    "updated_at": None
})
print("🚀 CP de ejemplo insertado.")
