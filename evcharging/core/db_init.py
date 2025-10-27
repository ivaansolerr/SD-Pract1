from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/evcharging_db")

client = MongoClient(MONGO_URI)

client.drop_database("evcharging_db")
print("Borramos la base de datos para evitar conflictos...")

db = client.get_database("evcharging_db")

db.create_collection("charging_points")
db.create_collection("drivers")
db.create_collection("sessions")

db.charging_points.create_index([("id")], unique=True)
db.drivers.create_index([("id")], unique=True)
db.sessions.create_index(
    [("driver_id"), ("cp_id"), ("start_time")]
)

db.charging_points.insert_one({
    "id": "cp-001",
    "location": "UA-Lab",
    "price_eur_kwh": 0.30,
    "state": "DISCONNECTED",
    "updated_at": None
})

print("Creamos de nuevo e insertamos los CPs")