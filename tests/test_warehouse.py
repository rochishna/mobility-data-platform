"""Warehouse tests: star schema loads, idempotency, and late-arrival handling."""
from dataclasses import replace
from datetime import datetime, timedelta

import duckdb
import polars as pl
import pytest

from factories import CONTRACT, clean_trips, small_batch_contract, trip
from mdp import warehouse
from mdp.pipeline import InjectedFault, Lake, run


@pytest.fixture
def setup(tmp_path, monkeypatch):
    """A lake + warehouse using the small-batch contract (production gates need 1,000+ rows)."""
    cfg = small_batch_contract()
    monkeypatch.setattr(warehouse, "load_source_config", lambda _: cfg)
    lake = Lake(tmp_path / "lake")
    db = tmp_path / "wh.duckdb"

    def ingest(name, rows, month, **kw):
        path = tmp_path / name
        pl.DataFrame(rows).write_parquet(path)
        return run(lake, cfg, path, month, **kw)

    def build():
        return warehouse.build(lake.root, db, CONTRACT)

    def q(sql):
        with duckdb.connect(str(db), read_only=True) as con:
            return con.execute(sql).fetchall()

    return lake, cfg, ingest, build, q


def dec_trip(i):
    t = datetime(2024, 12, 31, 23, 0) + timedelta(minutes=i)
    return trip(tpep_pickup_datetime=t, tpep_dropoff_datetime=t + timedelta(minutes=10))


def test_loads_star_schema(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(20) + [trip(fare_amount=-5.0, total_amount=-8.0)], "2025-01")
    assert build() == {"2025-01": 21}
    assert q("SELECT count(*) FROM fact_trip") == [(21,)]
    assert q("SELECT sum(trips) FROM agg_daily_zone_hour") == [(20,)]          # reversal not counted as a trip
    assert q("SELECT round(sum(total_amount), 2) FROM fact_trip") == q("SELECT round(sum(revenue), 2) FROM agg_daily_zone_hour")


def test_every_fact_row_joins_to_every_dimension(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(5) + [trip(VendorID=42, PULocationID=999)], "2025-01")
    build()
    orphans = q("""SELECT count(*) FROM fact_trip f
        LEFT JOIN dim_date d ON d.date_key = f.pickup_date_key
        LEFT JOIN dim_location pu ON pu.location_id = f.pu_location_id
        LEFT JOIN dim_location dl ON dl.location_id = f.do_location_id
        LEFT JOIN dim_vendor v ON v.vendor_id = f.vendor_id
        WHERE d.date_key IS NULL OR pu.location_id IS NULL OR dl.location_id IS NULL OR v.vendor_id IS NULL""")
    assert orphans == [(0,)]
    assert q("SELECT vendor_name FROM dim_vendor WHERE vendor_id = 42") == [("Undocumented code 42",)]


def test_rebuild_is_idempotent(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10), "2025-01")
    build()
    assert build() == {}                                                       # nothing changed, nothing reloaded
    assert q("SELECT count(*) FROM fact_trip") == [(10,)]


def test_corrected_month_replaces_not_appends(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10), "2025-01")
    build()
    ingest("jan_v2.parquet", clean_trips(7), "2025-01")
    assert build() == {"2025-01": 7}
    assert q("SELECT count(*) FROM fact_trip") == [(7,)]


def test_uncommitted_month_is_never_loaded(setup):
    lake, cfg, ingest, build, q = setup
    with pytest.raises(InjectedFault):
        ingest("jan.parquet", clean_trips(10), "2025-01", fault_point="after_publish_before_commit")
    assert build() == {}
    assert q("SELECT count(*) FROM fact_trip") == [(0,)]


# ---------- late arrivals ----------

def test_late_rows_join_their_real_month(setup):
    lake, cfg, ingest, build, q = setup
    ingest("dec.parquet", [dec_trip(i) for i in range(5)], "2024-12")
    ingest("jan.parquet", clean_trips(10) + [dec_trip(30), dec_trip(31)], "2025-01")   # 2 Dec trips arrive late
    build()
    assert q("SELECT pickup_month, count(*) FROM fact_trip GROUP BY 1 ORDER BY 1") == [("2024-12", 7), ("2025-01", 10)]
    assert q("SELECT late_rows FROM load_log WHERE pickup_month = '2024-12'") == [(2,)]


def test_month_reloads_when_late_rows_arrive_after_it_was_loaded(setup):
    lake, cfg, ingest, build, q = setup
    ingest("dec.parquet", [dec_trip(i) for i in range(5)], "2024-12")
    build()
    ingest("jan.parquet", clean_trips(10) + [dec_trip(30)], "2025-01")
    loaded = build()
    assert loaded == {"2024-12": 6, "2025-01": 10}                             # December picked up its late trip
    assert build() == {}


def test_late_row_already_in_its_month_is_not_double_counted(setup):
    lake, cfg, ingest, build, q = setup
    ingest("dec.parquet", [dec_trip(i) for i in range(5)], "2024-12")
    ingest("jan.parquet", clean_trips(10) + [dec_trip(2)], "2025-01")          # re-sent copy of a Dec trip
    build()
    assert q("SELECT count(*) FROM fact_trip WHERE pickup_month = '2024-12'") == [(5,)]


def test_late_rows_wait_until_their_month_exists(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10) + [dec_trip(30)], "2025-01")
    build()
    assert q("SELECT pickup_month, count(*) FROM fact_trip GROUP BY 1") == [("2025-01", 10)]
    ingest("dec.parquet", [dec_trip(i) for i in range(5)], "2024-12")
    assert build() == {"2024-12": 6}


def test_late_rows_from_crashed_run_are_ignored(setup):
    lake, cfg, ingest, build, q = setup
    ingest("dec.parquet", [dec_trip(i) for i in range(5)], "2024-12")
    with pytest.raises(InjectedFault):     # late file is written, but the January run never commits
        ingest("jan.parquet", clean_trips(10) + [dec_trip(30)], "2025-01", fault_point="after_publish_before_commit")
    build()
    assert q("SELECT count(*) FROM fact_trip WHERE pickup_month = '2024-12'") == [(5,)]
