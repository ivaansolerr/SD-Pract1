from pymongo import MongoClient, ASCENDING
import os

client = MongoClient("mongodb://127.0.0.1:27017/evcharging_db")
db = client.get_database("evcharging_db")

client.drop_database("evcharging_db")
print("Borramos la base de datos para evitar conflictos...")

db.create_collection("charging_points")
db.create_collection("drivers")
db.create_collection("sessions")

db.charging_points.create_index([("id")], unique=True)
db.drivers.create_index([("id")], unique=True)
db.sessions.create_index(
    [("driver_id"), ("cp_id"), ("start_time")]
)

db.charging_points.insert_one(
    {
    "id": "cp-001",
    "location": "UA-Lab",
    "price_eur_kwh": 1.50,
    "state": "DISCONNECTED",
    "updated_at": None 
    }
)

db.charging_points.insert_one(
    {
    "id": "cp-002",
    "location": "Luceros",
    "price_eur_kwh": 0.72,
    "state": "DISCONNECTED",
    "updated_at": None
    }
)

print("Creamos de nuevo e insertamos los CPs")