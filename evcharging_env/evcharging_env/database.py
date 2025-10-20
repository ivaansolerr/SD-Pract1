
from pymongo import MongoClient, ASCENDING
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import os

# Load .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "practica1_db")
COLL_CHARGING_POINTS = os.getenv("COLL_CHARGING_POINTS", "charging_points")
COLL_DRIVERS = os.getenv("COLL_DRIVERS", "drivers")
COLL_SESSIONS = os.getenv("COLL_SESSIONS", "sessions")

class Database:
    def __init__(self):
        # No auth (as requested). If later you enable auth, update MONGO_URI to include credentials.
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB_NAME]
        self.cps = self.db[COLL_CHARGING_POINTS]
        self.drivers = self.db[COLL_DRIVERS]
        self.sessions = self.db[COLL_SESSIONS]

        # Minimal indexes
        self.cps.create_index([("id", ASCENDING)], unique=True)
        self.drivers.create_index([("id", ASCENDING)], unique=True)
        self.sessions.create_index([("key", ASCENDING)], unique=True)

    # Charging Points
    def upsert_cp(self, cp_id: str, location: str, price: float, status: str = "DISCONNECTED"):
        self.cps.update_one(
            {"id": cp_id},
            {"$set": {"id": cp_id, "location": location, "price_per_kwh": price, "status": status, "current_driver": None}},
            upsert=True
        )

    def set_cp_status(self, cp_id: str, status: str, current_driver: Optional[str] = None):
        self.cps.update_one({"id": cp_id}, {"$set": {"status": status, "current_driver": current_driver}})

    def get_cp(self, cp_id: str) -> Optional[Dict[str, Any]]:
        return self.cps.find_one({"id": cp_id})

    # Drivers
    def upsert_driver(self, driver_id: str, balance: float = 100.0):
        self.drivers.update_one(
            {"id": driver_id},
            {"$setOnInsert": {"id": driver_id, "balance": balance}},
            upsert=True
        )

    def get_driver(self, driver_id: str) -> Optional[Dict[str, Any]]:
        return self.drivers.find_one({"id": driver_id})

    # Sessions
    def start_session(self, driver_id: str, cp_id: str):
        key = f"{driver_id}:{cp_id}:{int(datetime.now().timestamp())}"
        self.sessions.insert_one({
            "key": key, "driver_id": driver_id, "cp_id": cp_id,
            "start_time": datetime.utcnow(), "end_time": None,
            "kwh": 0.0, "price": 0.0
        })
        return key

    def accumulate_session(self, key: str, kwh_delta: float, price_delta: float):
        self.sessions.update_one({"key": key}, {"$inc": {"kwh": kwh_delta, "price": price_delta}})

    def end_session(self, key: str):
        self.sessions.update_one({"key": key}, {"$set": {"end_time": datetime.utcnow()}})

    def get_session(self, key: str) -> Optional[Dict[str, Any]]:
        return self.sessions.find_one({"key": key})
