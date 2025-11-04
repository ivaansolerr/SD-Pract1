from pymongo import MongoClient, ASCENDING
from typing import Optional, Dict, Any

client = MongoClient("mongodb://127.0.0.1:27017/evcharging_db")
db = client.get_database()

charging_points = db.get_collection("charging_points")
drivers = db.get_collection("drivers")
sessions = db.get_collection("sessions")

def upsertCp(cp):
    charging_points.update_one({"id": cp["id"]}, {"$set": cp}, upsert=True)

def listChargingPoints():
    return list(charging_points.find({}, {"_id": 0}))

def getCp(cpId):
    return charging_points.find_one({"id": cpId})

def setCpState(cpId, state):
    charging_points.update_one({"id": cpId}, {"$set": {"state": state}})

def upsertDriver(driver):
    drivers.update_one({"id": driver["id"]}, {"$set": driver}, upsert=True)

def createSession(doc):
    res = sessions.insert_one(doc)
    return str(res.inserted_id)

def appendTelemetry(sessionId, telemetry):
    sessions.update_one({"_id": sessions.codec_options.document_class()._factory(sessionId)}, {"$push": {"telemetry": telemetry}})

def updateSession(sessionId, updates):
    from bson import ObjectId
    sessions.update_one({"_id": ObjectId(sessionId)}, {"$set": updates})

def upsertSession(session):
    sessions.update_one(
        {
            "driver_id": session["driver_id"],
            "cp_id": session["cp_id"]
        },
        {
            "$set": session
        },
        upsert=True
    )

def updateSessionEnergy(driver_id: str, cp_id: str, energy_kwh: float):
    sessions.update_one(
        {
            "driver_id": driver_id,
            "cp_id": cp_id
        },
        {
            "$set": {
                "energy_kwh": energy_kwh
            }
        }
    )

def deleteSession(driver_id: str, cp_id: str):
    sessions.delete_one(
        {
            "driver_id": driver_id,
            "cp_id": cp_id
        }
    )