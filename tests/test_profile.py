"""Tests for the profiler.

We build a tiny fake month (January 2025) where we know EXACTLY which problems exist,
then check the profiler counts each one correctly. This is the habit that matters:
never trust a data-quality check you haven't seen catch a planted bug.
"""
from datetime import datetime, timedelta

import polars as pl
import pytest

from mdp.profile import Profiler


def trip(**overrides):
    """One clean, valid trip. Each test row changes only the fields it needs to break."""
    base = dict(
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
    base.update(overrides)
    return base


def clean_trip(i):
    """Clean trips that differ from each other, so they don't count as duplicates."""
    return trip(tpep_pickup_datetime=datetime(2025, 1, 15, 10, 0) + timedelta(minutes=i),
                tpep_dropoff_datetime=datetime(2025, 1, 15, 10, 20) + timedelta(minutes=i))


@pytest.fixture
def profiler(tmp_path):
    rows = [clean_trip(i) for i in range(20)]
    rows += [
        trip(tpep_pickup_datetime=datetime(2025, 1, 3, 9, 0),              # dropoff_before_pickup
             tpep_dropoff_datetime=datetime(2025, 1, 3, 8, 50)),
        trip(tpep_pickup_datetime=datetime(2025, 1, 4, 9, 0),              # zero_duration
             tpep_dropoff_datetime=datetime(2025, 1, 4, 9, 0)),
        trip(tpep_pickup_datetime=datetime(2025, 1, 5, 9, 0),              # over_24h
             tpep_dropoff_datetime=datetime(2025, 1, 7, 9, 0)),
        trip(fare_amount=-5.0, total_amount=-3.0, PULocationID=1),         # negative_fare + negative_total
        trip(fare_amount=4000.0, PULocationID=2),                          # huge_fare
        trip(trip_distance=0.0, PULocationID=3),                           # zero_distance
        trip(trip_distance=350.0, PULocationID=4),                         # huge_distance
        trip(tpep_pickup_datetime=datetime(2024, 12, 31, 23, 50),          # pickup_outside_month
             tpep_dropoff_datetime=datetime(2025, 1, 1, 0, 10)),
        trip(passenger_count=0.0, PULocationID=5),                         # zero_passengers
        trip(PULocationID=264),                                            # unknown_zone
        trip(RatecodeID=99.0, PULocationID=6),                             # ratecode_99
        trip(passenger_count=None, RatecodeID=None, PULocationID=7),       # nulls
    ]
    rows += [clean_trip(0)]                                                # exact duplicate of clean_trip(0)
    rows += [{**clean_trip(1), "total_amount": 99.0}]                      # same key as clean_trip(1), different total

    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return Profiler(path, "2025-01")


def by_check(profiler):
    return {f.check: f.rows for f in profiler.rule_findings()}


def test_row_count(profiler):
    assert profiler.total == 34


@pytest.mark.parametrize("check,expected", [
    ("dropoff_before_pickup", 1),
    ("zero_duration", 1),
    ("over_24h", 1),
    ("negative_fare", 1),
    ("negative_total", 1),
    ("huge_fare", 1),
    ("negative_distance", 0),
    ("zero_distance", 1),
    ("huge_distance", 1),
    ("pickup_outside_month", 1),
    ("zero_passengers", 1),
    ("unknown_zone", 1),
    ("ratecode_99", 1),
])
def test_rule_checks_find_planted_problems(profiler, check, expected):
    assert by_check(profiler)[check] == expected


def test_null_counts(profiler):
    nulls = profiler.null_counts()
    assert nulls["passenger_count"] == 1
    assert nulls["RatecodeID"] == 1
    assert nulls["fare_amount"] == 0


def test_duplicates(profiler):
    # Two extra rows share a candidate key with another row; only one is a byte-for-byte copy.
    assert profiler.key_duplicates().rows == 2
    assert profiler.exact_duplicates().rows == 1


def test_missing_column_skips_check(tmp_path):
    """Older TLC files can lack columns; the profiler should skip, not crash."""
    df = pl.DataFrame([trip()]).drop("RatecodeID")
    path = tmp_path / "old.parquet"
    df.write_parquet(path)
    checks = by_check(Profiler(path, "2025-01"))
    assert "ratecode_99" not in checks
    assert "negative_fare" in checks


def test_report_renders(profiler):
    text = profiler.report()
    assert "Rows:** 34" in text
    assert "`pickup_outside_month`" in text


def test_rejects_bad_month(tmp_path):
    path = tmp_path / "x.parquet"
    pl.DataFrame([trip()]).write_parquet(path)
    with pytest.raises(ValueError):
        Profiler(path, "Jan 2025")
