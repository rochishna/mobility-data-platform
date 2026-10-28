"""Builders for synthetic TLC rows, shared by tests."""
from datetime import datetime, timedelta

CONTRACT = "configs/sources/tlc_yellow.yaml"


def trip(**overrides):
    """One valid raw TLC row (source column names). Override only what a test breaks."""
    row = dict(
        VendorID=1,
        tpep_pickup_datetime=datetime(2025, 1, 15, 10, 0),
        tpep_dropoff_datetime=datetime(2025, 1, 15, 10, 20),
        passenger_count=1.0,
        trip_distance=3.2,
        RatecodeID=1.0,
        PULocationID=100,
        DOLocationID=200,
        payment_type=1,
        fare_amount=18.5,
        total_amount=24.0,
    )
    row.update(overrides)
    return row


def clean_trips(n, start=datetime(2025, 1, 10, 8, 0)):
    """n valid trips with distinct keys."""
    return [
        trip(tpep_pickup_datetime=start + timedelta(minutes=i),
             tpep_dropoff_datetime=start + timedelta(minutes=i + 15))
        for i in range(n)
    ]
