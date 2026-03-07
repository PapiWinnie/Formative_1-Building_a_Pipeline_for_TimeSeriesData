"""
Metro Interstate Traffic Volume – REST API
Task 3: CRUD + Time-Series Endpoints for MySQL (SQLAlchemy) and MongoDB (Motor)

Run:
    python -m uvicorn app:app --reload --port 8000

Docs: http://localhost:8000/docs
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, Depends
from pydantic import BaseModel, Field
from sqlalchemy import (
    create_engine,
    Column, Integer, BigInteger, Float, String, DateTime, ForeignKey, SmallInteger
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# Configuration
MYSQL_URL  = os.getenv("MYSQL_URL",  "mysql+pymysql://root:@localhost:3306/traffic_db")
MONGO_URL  = os.getenv("MONGO_URL",  "mongodb://localhost:27017")
MONGO_DB   = os.getenv("MONGO_DB",   "traffic_db")
MONGO_COLL = os.getenv("MONGO_COLL", "traffic_records")

# SQLAlchemy setup
engine = create_engine(MYSQL_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class HolidayORM(Base):
    __tablename__ = "holidays"
    holiday_id   = Column(Integer, primary_key=True, index=True)
    holiday_name = Column(String(100), nullable=False, unique=True)


class WeatherORM(Base):
    __tablename__ = "weather_conditions"
    weather_id          = Column(Integer, primary_key=True, index=True)
    weather_main        = Column(String(50), nullable=False)
    weather_description = Column(String(100), nullable=False)


class TrafficORM(Base):
    __tablename__ = "traffic_records"
    record_id      = Column(BigInteger, primary_key=True, index=True)
    date_time      = Column(DateTime, nullable=False, index=True)
    holiday_id     = Column(Integer, ForeignKey("holidays.holiday_id"), nullable=False)
    weather_id     = Column(Integer, ForeignKey("weather_conditions.weather_id"), nullable=False)
    temp           = Column(Float, nullable=False)
    rain_1h        = Column(Float, nullable=False, default=0)
    snow_1h        = Column(Float, nullable=False, default=0)
    clouds_all     = Column(SmallInteger, nullable=False)
    traffic_volume = Column(Integer, nullable=False)


# MongoDB setup
mongo_client: AsyncIOMotorClient = None

def get_mongo_collection():
    return mongo_client[MONGO_DB][MONGO_COLL]


# Pydantic schemas
class WeatherIn(BaseModel):
    main:        str = Field(..., example="Clouds")
    description: str = Field(..., example="scattered clouds")

class TimeFeatures(BaseModel):
    year:        int
    month:       int
    day:         int
    hour:        int
    day_of_week: int
    is_weekend:  bool

class SQLTrafficCreate(BaseModel):
    date_time:      datetime
    holiday_id:     int
    weather_id:     int
    temp:           float
    rain_1h:        float = 0.0
    snow_1h:        float = 0.0
    clouds_all:     int
    traffic_volume: int

class SQLTrafficUpdate(BaseModel):
    temp:           Optional[float] = None
    rain_1h:        Optional[float] = None
    snow_1h:        Optional[float] = None
    clouds_all:     Optional[int]   = None
    traffic_volume: Optional[int]   = None
    holiday_id:     Optional[int]   = None
    weather_id:     Optional[int]   = None

class SQLTrafficOut(SQLTrafficCreate):
    record_id: int
    class Config:
        from_attributes = True

class MongoTrafficCreate(BaseModel):
    date_time:      datetime
    holiday:        str  = "None"
    is_holiday:     bool = False
    temp_kelvin:    float
    temp_celsius:   float
    rain_1h_mm:     float = 0.0
    snow_1h_mm:     float = 0.0
    clouds_pct:     int
    traffic_volume: int
    weather:        WeatherIn
    time_features:  TimeFeatures

class MongoTrafficUpdate(BaseModel):
    temp_kelvin:    Optional[float]     = None
    temp_celsius:   Optional[float]     = None
    rain_1h_mm:     Optional[float]     = None
    snow_1h_mm:     Optional[float]     = None
    clouds_pct:     Optional[int]       = None
    traffic_volume: Optional[int]       = None
    weather:        Optional[WeatherIn] = None
    holiday:        Optional[str]       = None
    is_holiday:     Optional[bool]      = None


# App
app = FastAPI(
    title="Metro Traffic Volume API",
    description="Task 3 – CRUD & Time-Series Endpoints (MySQL + MongoDB)",
    version="1.0.0"
)


@app.on_event("startup")
async def startup():
    global mongo_client
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Could not create SQL tables: {e}")


@app.on_event("shutdown")
async def shutdown():
    if mongo_client:
        mongo_client.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _serial_doc(doc: dict) -> dict:
    doc["_id"] = str(doc["_id"])
    if isinstance(doc.get("date_time"), datetime):
        doc["date_time"] = doc["date_time"].isoformat()
    return doc


# SQL endpoints

@app.post("/sql/traffic", response_model=SQLTrafficOut, status_code=201, tags=["SQL CRUD"])
def sql_create(payload: SQLTrafficCreate, db: Session = Depends(get_db)):
    """Insert a new traffic record into MySQL."""
    row = TrafficORM(**payload.dict())
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@app.get("/sql/traffic", response_model=List[SQLTrafficOut], tags=["SQL CRUD"])
def sql_list(limit: int = Query(20, le=200), offset: int = 0, db: Session = Depends(get_db)):
    """List traffic records (paginated)."""
    return db.query(TrafficORM).order_by(TrafficORM.date_time.desc()).offset(offset).limit(limit).all()


@app.get("/sql/traffic/latest", response_model=SQLTrafficOut, tags=["SQL Time-Series"])
def sql_latest(db: Session = Depends(get_db)):
    """Retrieve the most recent traffic record."""
    row = db.query(TrafficORM).order_by(TrafficORM.date_time.desc()).first()
    if not row:
        raise HTTPException(404, "No records found")
    return row


@app.get("/sql/traffic/range", response_model=List[SQLTrafficOut], tags=["SQL Time-Series"])
def sql_date_range(
    start: datetime = Query(..., example="2012-10-02T00:00:00"),
    end:   datetime = Query(..., example="2012-10-02T23:59:59"),
    db:    Session  = Depends(get_db)
):
    """Return records within a date/time range."""
    return (
        db.query(TrafficORM)
        .filter(TrafficORM.date_time >= start, TrafficORM.date_time <= end)
        .order_by(TrafficORM.date_time)
        .all()
    )


@app.get("/sql/traffic/{record_id}", response_model=SQLTrafficOut, tags=["SQL CRUD"])
def sql_read(record_id: int = Path(...), db: Session = Depends(get_db)):
    """Retrieve a single record by ID."""
    row = db.query(TrafficORM).filter(TrafficORM.record_id == record_id).first()
    if not row:
        raise HTTPException(404, f"Record {record_id} not found")
    return row


@app.put("/sql/traffic/{record_id}", response_model=SQLTrafficOut, tags=["SQL CRUD"])
def sql_update(record_id: int, payload: SQLTrafficUpdate, db: Session = Depends(get_db)):
    """Update fields of an existing record."""
    row = db.query(TrafficORM).filter(TrafficORM.record_id == record_id).first()
    if not row:
        raise HTTPException(404, f"Record {record_id} not found")
    for field, val in payload.dict(exclude_none=True).items():
        setattr(row, field, val)
    db.commit()
    db.refresh(row)
    return row


@app.delete("/sql/traffic/{record_id}", status_code=204, tags=["SQL CRUD"])
def sql_delete(record_id: int, db: Session = Depends(get_db)):
    """Remove a record by ID."""
    row = db.query(TrafficORM).filter(TrafficORM.record_id == record_id).first()
    if not row:
        raise HTTPException(404, f"Record {record_id} not found")
    db.delete(row)
    db.commit()


# MongoDB endpoints

@app.post("/mongo/traffic", status_code=201, tags=["MongoDB CRUD"])
async def mongo_create(payload: MongoTrafficCreate):
    """Insert a new document into MongoDB."""
    doc = payload.dict()
    result = await get_mongo_collection().insert_one(doc)
    return {"inserted_id": str(result.inserted_id)}


@app.get("/mongo/traffic", tags=["MongoDB CRUD"])
async def mongo_list(limit: int = Query(20, le=200), skip: int = 0):
    """List documents (paginated)."""
    cursor = get_mongo_collection().find({}).sort("date_time", -1).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_serial_doc(d) for d in docs]


@app.get("/mongo/traffic/latest", tags=["MongoDB Time-Series"])
async def mongo_latest():
    """Retrieve the most recent document."""
    doc = await get_mongo_collection().find_one({}, sort=[("date_time", -1)])
    if not doc:
        raise HTTPException(404, "No documents found")
    return _serial_doc(doc)


@app.get("/mongo/traffic/range", tags=["MongoDB Time-Series"])
async def mongo_date_range(
    start: datetime = Query(..., example="2012-10-02T00:00:00"),
    end:   datetime = Query(..., example="2012-10-02T23:59:59"),
):
    """Return documents within a date/time range."""
    cursor = get_mongo_collection().find(
        {"date_time": {"$gte": start, "$lte": end}}
    ).sort("date_time", 1)
    docs = await cursor.to_list(length=500)
    return [_serial_doc(d) for d in docs]


@app.get("/mongo/traffic/{doc_id}", tags=["MongoDB CRUD"])
async def mongo_read(doc_id: str = Path(...)):
    """Retrieve a single document by ObjectId string."""
    from bson import ObjectId
    try:
        oid = ObjectId(doc_id)
    except Exception:
        raise HTTPException(400, "Invalid ObjectId")
    doc = await get_mongo_collection().find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, f"Document {doc_id} not found")
    return _serial_doc(doc)


@app.put("/mongo/traffic/{doc_id}", tags=["MongoDB CRUD"])
async def mongo_update(doc_id: str, payload: MongoTrafficUpdate):
    """Update fields of an existing document."""
    from bson import ObjectId
    try:
        oid = ObjectId(doc_id)
    except Exception:
        raise HTTPException(400, "Invalid ObjectId")
    update_data = {k: v for k, v in payload.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(400, "No fields to update")
    result = await get_mongo_collection().update_one({"_id": oid}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(404, f"Document {doc_id} not found")
    return {"modified_count": result.modified_count}


@app.delete("/mongo/traffic/{doc_id}", status_code=204, tags=["MongoDB CRUD"])
async def mongo_delete(doc_id: str):
    """Remove a document by ObjectId string."""
    from bson import ObjectId
    try:
        oid = ObjectId(doc_id)
    except Exception:
        raise HTTPException(400, "Invalid ObjectId")
    result = await get_mongo_collection().delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(404, f"Document {doc_id} not found")