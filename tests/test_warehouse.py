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

    reviews = tmp_path / "reviews.csv"

    def build():
        return warehouse.build(lake.root, db, CONTRACT, reviews_csv=reviews)

    def q(sql):
        with duckdb.connect(str(db), read_only=True) as con:
            return con.execute(sql).fetchall()

    build.reviews = reviews
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
    assert q("SELECT round(sum(total_amount), 2) FROM fact_trip") == q("SELECT round(sum(net_revenue), 2) FROM agg_daily_zone_hour")


def test_gross_and_net_revenue(setup):
    """Gross = charges only. Net = after reversals. Decision by Rochishna: show both."""
    lake, cfg, ingest, build, q = setup
    rows = clean_trips(3)                                               # 3 x $24.00
    rows.append({**rows[0], "fare_amount": -18.5, "total_amount": -24.0})   # a reversal of trip 0
    ingest("jan.parquet", rows, "2025-01")
    build()
    assert q("SELECT sum(gross_revenue), sum(net_revenue) FROM agg_daily_zone_hour") == [(72.0, 48.0)]


def test_zone_names_come_from_lookup(setup, tmp_path, monkeypatch):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(3) + [trip(PULocationID=132)], "2025-01")
    warehouse.build(lake.root, tmp_path / "named.duckdb", CONTRACT, "configs/reference/taxi_zone_lookup.csv")
    with duckdb.connect(str(tmp_path / "named.duckdb"), read_only=True) as con:
        assert con.execute("SELECT borough, zone FROM dim_location WHERE location_id = 132").fetchall() == [("Queens", "JFK Airport")]
        assert con.execute("SELECT count(*) FROM dim_location").fetchall() == [(265,)]


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


# ---------- review queue (decision: Rochishna, held until reviewed) ----------

def bad_fare():
    return trip(fare_amount=863372.12, total_amount=863380.37, PULocationID=138,
                tpep_dropoff_datetime=datetime(2025, 1, 15, 10, 5, 24))


def write_review(path, decision):
    path.write_text(
        "vendor_id,pickup_datetime,dropoff_datetime,pu_location_id,do_location_id,fare_amount,total_amount,"
        "decision,reviewer,reviewed_on,note\n"
        f"1,2025-01-15 10:00:00,2025-01-15 10:05:24,138,200,863372.12,863380.37,{decision},Rochishna,2026-11-01,test\n")


def test_suspect_trip_is_held_out_of_rollup_but_kept_in_fact(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10) + [bad_fare()], "2025-01")
    build()
    assert q("SELECT count(*) FROM fact_trip") == [(11,)]                       # kept
    assert q("SELECT sum(trips), sum(net_revenue) FROM agg_daily_zone_hour") == [(10, 240.0)]   # not counted
    assert q("SELECT count(*) FROM review_queue") == [(1,)]


def test_approved_trip_counts_after_rebuild(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10) + [bad_fare()], "2025-01")
    build()
    write_review(build.reviews, "approved")
    assert build() == {"2025-01": 11}                                            # review change -> rollup rebuilt
    assert q("SELECT sum(trips) FROM agg_daily_zone_hour") == [(11,)]
    assert q("SELECT count(*) FROM review_queue") == [(0,)]


def test_rejected_trip_stays_out_and_leaves_queue(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10) + [bad_fare()], "2025-01")
    write_review(build.reviews, "rejected")
    build()
    assert q("SELECT sum(trips) FROM agg_daily_zone_hour") == [(10,)]
    assert q("SELECT count(*) FROM review_queue") == [(0,)]
    assert q("SELECT count(*) FROM fact_trip WHERE suspect_fare") == [(1,)]     # still kept for audit


def test_invalid_review_decision_is_refused(setup):
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10), "2025-01")
    write_review(build.reviews, "maybe")
    with pytest.raises(ValueError, match="approved"):
        build()


def test_review_cli_round_trip(setup, tmp_path):
    from mdp import review
    lake, cfg, ingest, build, q = setup
    ingest("jan.parquet", clean_trips(10) + [bad_fare()], "2025-01")
    build()
    db = str(lake.root.parent / "wh.duckdb")
    todo = tmp_path / "todo.csv"
    assert review.export(db, todo) == 1
    text = todo.read_text().replace(",,,,", ",rejected,Rochishna,,", 1)       # fill decision + reviewer
    todo.write_text(text)
    assert review.apply(todo, build.reviews) == (1, 0)
    build()
    assert q("SELECT count(*) FROM review_queue") == [(0,)]
