from pymongo import MongoClient, ASCENDING
from typing import Optional, Dict, Any
from . import config

_client = MongoClient(config.MONGO_URI)  # <--- CAMBIA si tu Mongo requiere auth/SSL
_db = _client.get_default_database()

charging_points = _db.get_collection("charging_points")
drivers = _db.get_collection("drivers")
sessions = _db.get_collection("sessions")

# Índices básicos
charging_points.create_index([("id", ASCENDING)], unique=True)
drivers.create_index([("id", ASCENDING)], unique=True)
sessions.create_index([("driver_id", ASCENDING), ("cp_id", ASCENDING), ("start_time", ASCENDING)])

def upsert_cp(cp: Dict[str, Any]):
    charging_points.update_one({"id": cp["id"]}, {"$set": cp}, upsert=True)

def get_cp(cp_id: str) -> Optional[Dict[str, Any]]:
    return charging_points.find_one({"id": cp_id})

def set_cp_state(cp_id: str, state: str):
    charging_points.update_one({"id": cp_id}, {"$set": {"state": state}})

def upsert_driver(driver: Dict[str, Any]):
    drivers.update_one({"id": driver["id"]}, {"$set": driver}, upsert=True)

def create_session(doc: Dict[str, Any]) -> str:
    res = sessions.insert_one(doc)
    return str(res.inserted_id)

def append_telemetry(session_id: str, telemetry: Dict[str, Any]):
    sessions.update_one({"_id": sessions.codec_options.document_class()._factory(session_id)}, {"$push": {"telemetry": telemetry}})

def update_session(session_id: str, updates: Dict[str, Any]):
    from bson import ObjectId
    sessions.update_one({"_id": ObjectId(session_id)}, {"$set": updates})