from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any, List
from datetime import datetime

CPState = Literal["ACTIVATED", "OUT_OF_ORDER", "SUPPLYING", "BROKEN", "DISCONNECTED", "STOPPED"]

class ChargingPoint(BaseModel):
    id: str
    location: str = "N/A"
    price_eur_kwh: float = 0.30
    state: CPState = "DISCONNECTED"
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class Driver(BaseModel):
    id: str
    name: str = "Anonymous"

class SupplyRequest(BaseModel):
    driver_id: str
    cp_id: str

class SupplyAuth(BaseModel):
    driver_id: str
    cp_id: str
    authorized: bool
    reason: str = ""

class Telemetry(BaseModel):
    cp_id: str
    kw: float
    euros: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class Session(BaseModel):
    driver_id: str
    cp_id: str
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    total_kwh: float = 0.0
    total_eur: float = 0.0
    telemetry: List[Telemetry] = Field(default_factory=list)
    status: Literal["RUNNING","FINISHED","ABORTED"] = "RUNNING"