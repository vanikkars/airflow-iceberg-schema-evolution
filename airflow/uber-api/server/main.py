import logging

from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import date
import os
import databases
import sqlalchemy

logger = logging.getLogger(__name__)

# Postgres URL
DATABASE_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
DATA_TIME_SHIFT = 1 # years between current date and the source uber data
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

# Table definition
ride_bookings = sqlalchemy.Table(
    "ride_bookings",
    metadata,
    sqlalchemy.Column("booking_date", sqlalchemy.Date),
    sqlalchemy.Column("booking_time", sqlalchemy.Time),
    sqlalchemy.Column("booking_id", sqlalchemy.String),
    sqlalchemy.Column("booking_status", sqlalchemy.String),
    sqlalchemy.Column("customer_id", sqlalchemy.String),
    sqlalchemy.Column("vehicle_type", sqlalchemy.String),
    sqlalchemy.Column("pickup_location", sqlalchemy.String),
    sqlalchemy.Column("drop_location", sqlalchemy.String),
    sqlalchemy.Column("avg_vtat", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("avg_ctat", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("cancelled_rides_by_customer", sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column("reason_for_cancelling_by_customer", sqlalchemy.String, nullable=True),
    sqlalchemy.Column("cancelled_rides_by_driver", sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column("driver_cancellation_reason", sqlalchemy.String, nullable=True),
    sqlalchemy.Column("incomplete_rides", sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column("incomplete_rides_reason", sqlalchemy.String, nullable=True),
    sqlalchemy.Column("booking_value", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("ride_distance", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("driver_ratings", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("customer_rating", sqlalchemy.Numeric, nullable=True),
    sqlalchemy.Column("payment_method", sqlalchemy.String, nullable=True),
)
import datetime as dt

# Pydantic model
class RideBooking(BaseModel):
    booking_date: date
    booking_time: Optional[dt.time]
    booking_id: str
    booking_status: str
    customer_id: str
    vehicle_type: str
    pickup_location: str
    drop_location: str
    avg_vtat: Optional[float] = None
    avg_ctat: Optional[float] = None
    cancelled_rides_by_customer: Optional[int] = None
    reason_for_cancelling_by_customer: Optional[str] = None
    cancelled_rides_by_driver: Optional[int] = None
    driver_cancellation_reason: Optional[str] = None
    incomplete_rides: Optional[int] = None
    incomplete_rides_reason: Optional[str] = None
    booking_value: Optional[float] = None
    ride_distance: Optional[float] = None
    driver_ratings: Optional[float] = None
    customer_rating: Optional[float] = None
    payment_method: Optional[str] = None

app = FastAPI(title="Ride Bookings API")


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/rides", response_model=List[RideBooking])
async def get_rides(
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date")
):
    start_date = start_date.replace(year=start_date.year - DATA_TIME_SHIFT)
    end_date = end_date.replace(year=end_date.year - DATA_TIME_SHIFT)
    query = ride_bookings.select().where(
        ride_bookings.c.booking_date.between(start_date, end_date)
    )
    results = await database.fetch_all(query)
    output = []
    logger.info(f"Fetched {len(results)} records from {start_date} to {end_date}")
    for r in results:
        rb = RideBooking(**dict(r))
        rb.booking_date = rb.booking_date.replace(year=rb.booking_date.year + DATA_TIME_SHIFT)
        output.append(rb)
    logger.info(f"Transformed {len(output)} records to current date range")
    return output
