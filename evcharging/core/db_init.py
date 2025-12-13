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
    [("driver_id"), ("cp_id")]
)

# db.charging_points.insert_one(
#     {
#     "id": "cp-001",
#     "location": "UA-Lab",
#     "price_eur_kwh": 1.53,
#     "state": "DISCONNECTED",
#     "updated_at": None 
#     }
# )

# db.charging_points.insert_one(
#     {
#     "id": "cp-002",
#     "location": "Luceros",
#     "price_eur_kwh": 2.00,
#     "state": "DISCONNECTED",
#     "updated_at": None
#     }
# )

# db.charging_points.insert_one(
#     {
#     "id": "cp-003",
#     "location": "SanJuan",
#     "price_eur_kwh": 0.28,
#     "state": "DISCONNECTED",
#     "updated_at": None
#     }
# )

# db.charging_points.insert_one(
#     {
#     "id": "cp-004",
#     "location": "TheOne",
#     "price_eur_kwh": 2.11,
#     "state": "DISCONNECTED",
#     "updated_at": None
#     }
# )

# db.charging_points.insert_one(
#     {
#     "id": "cp-005",
#     "location": "SantaPola",
#     "price_eur_kwh": 1.13,
#     "state": "DISCONNECTED",
#     "updated_at": None
#     }
# )

# db.charging_points.insert_one(
#     {
#     "id": "cp-006",
#     "location": "Elche",
#     "price_eur_kwh": 1.98,
#     "state": "DISCONNECTED",
#     "updated_at": None
#     }
# )

print("Creamos de nuevo e insertamos los CPs")