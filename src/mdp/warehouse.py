"""Star schema built from committed lake partitions.

    dim_date ─┐
    dim_location (pickup / dropoff) ─┤
    dim_vendor ─┼── fact_trip ──> agg_daily_zone_hour (pre-aggregated rollup)
    dim_payment_type ─┤
    dim_rate_code ─┘

Rules:
  * Only partitions with a 'committed' manifest record are loaded. A half-finished run is invisible here.
  * Loading a month is delete-then-insert inside one transaction, so reloading is idempotent and a
    failure mid-load leaves the previous version of that month in place.
  * Late arrivals (valid trips that landed in another month's file) are added to their real month,
    unless that month's own file already contains the same trip key.
  * Code meanings for vendor / payment / rate code come from the TLC yellow taxi data dictionary
    (March 18, 2025).

Usage:
    python -m mdp.warehouse --lake lake/ --db warehouse.duckdb [--zones configs/reference/taxi_zone_lookup.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .config import SourceConfig, load_source_config
from .pipeline import Lake

VENDORS = {1: "Creative Mobile Technologies", 2: "Curb Mobility", 6: "Myle Technologies", 7: "Helix"}
PAYMENT_TYPES = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute",
                 5: "Unknown", 6: "Voided trip"}
RATE_CODES = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
              5: "Negotiated fare", 6: "Group ride", 99: "Null/unknown"}

DDL = """
CREATE TABLE IF NOT EXISTS dim_vendor       (vendor_id INTEGER PRIMARY KEY, vendor_name VARCHAR);
CREATE TABLE IF NOT EXISTS dim_payment_type (payment_type INTEGER PRIMARY KEY, payment_name VARCHAR);
CREATE TABLE IF NOT EXISTS dim_rate_code    (ratecode_id INTEGER PRIMARY KEY, rate_name VARCHAR);
CREATE TABLE IF NOT EXISTS dim_location     (location_id INTEGER PRIMARY KEY, borough VARCHAR, zone VARCHAR, service_zone VARCHAR);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, date DATE, year INTEGER, month INTEGER, day INTEGER,
    day_of_week INTEGER, day_name VARCHAR, is_weekend BOOLEAN
);
CREATE TABLE IF NOT EXISTS fact_trip (
    pickup_month      VARCHAR,
    pickup_date_key   INTEGER,
    pickup_hour       INTEGER,
    pickup_datetime   TIMESTAMP,
    dropoff_datetime  TIMESTAMP,
    pu_location_id    INTEGER,
    do_location_id    INTEGER,
    vendor_id         INTEGER,
    payment_type      INTEGER,
    ratecode_id       INTEGER,
    passenger_count   DOUBLE,
    trip_distance     DOUBLE,
    duration_min      DOUBLE,
    fare_amount       DOUBLE,
    total_amount      DOUBLE,
    is_reversal       BOOLEAN,
    is_flex_fare      BOOLEAN,
    suspect_distance  BOOLEAN,
    zero_distance     BOOLEAN,
    unknown_zone      BOOLEAN
);
CREATE TABLE IF NOT EXISTS agg_daily_zone_hour (
    pickup_month    VARCHAR,
    pickup_date_key INTEGER,
    pickup_hour     INTEGER,
    pu_location_id  INTEGER,
    trips           BIGINT,     -- trip records excluding reversal rows
    revenue         DOUBLE,     -- net: charges plus reversals
    total_distance  DOUBLE,     -- excludes suspect distances
    total_minutes   DOUBLE
);
CREATE TABLE IF NOT EXISTS load_log (
    pickup_month VARCHAR PRIMARY KEY, batch_id VARCHAR, late_batches VARCHAR, fact_rows BIGINT, late_rows BIGINT, loaded_at TIMESTAMP
);
"""


def _seed_static_dims(con: duckdb.DuckDBPyConnection, zones_csv: str | Path | None) -> None:
    for table, key, label, mapping in [
        ("dim_vendor", "vendor_id", "vendor_name", VENDORS),
        ("dim_payment_type", "payment_type", "payment_name", PAYMENT_TYPES),
        ("dim_rate_code", "ratecode_id", "rate_name", RATE_CODES),
    ]:
        con.execute(f"DELETE FROM {table}")
        con.executemany(f"INSERT INTO {table} ({key}, {label}) VALUES (?, ?)", list(mapping.items()))

    con.execute("DELETE FROM dim_location")
    if zones_csv and Path(zones_csv).exists():
        con.execute("""
            INSERT INTO dim_location
            SELECT "LocationID", "Borough", "Zone", service_zone FROM read_csv_auto(?, header=true)
        """, [str(zones_csv)])
    # Any location id seen in facts but missing from the lookup still gets a row, so joins never drop trips.


def _fill_missing_dims(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        INSERT INTO dim_location
        SELECT id, 'Unknown', 'Zone ' || id, NULL FROM (
            SELECT pu_location_id AS id FROM fact_trip UNION SELECT do_location_id FROM fact_trip)
        WHERE id NOT IN (SELECT location_id FROM dim_location)
    """)
    con.execute("""
        INSERT INTO dim_date
        SELECT k, d, year(d), month(d), day(d), isodow(d), dayname(d), isodow(d) >= 6 FROM (
            SELECT DISTINCT pickup_date_key AS k, strptime(pickup_date_key::VARCHAR, '%Y%m%d')::DATE AS d FROM fact_trip)
        WHERE k NOT IN (SELECT date_key FROM dim_date)
    """)
    for table, key, label in [("dim_vendor", "vendor_id", "vendor_name"),
                              ("dim_payment_type", "payment_type", "payment_name"),
                              ("dim_rate_code", "ratecode_id", "rate_name")]:
        con.execute(f"""
            INSERT INTO {table} SELECT DISTINCT {key}, 'Undocumented code ' || {key} FROM fact_trip
            WHERE {key} IS NOT NULL AND {key} NOT IN (SELECT {key} FROM {table})
        """)


def committed_months(lake: Lake, cfg: SourceConfig) -> list[str]:
    return sorted({r["logical_partition"] for r in lake.manifest.history(cfg.source) if r["status"] == "committed"})


def late_files_for(lake: Lake, cfg: SourceConfig, month: str) -> list[tuple[str, str]]:
    """(batch_id, path) of committed late-arrival files from OTHER months holding rows for `month`.
    Only each month's latest committed batch counts, so crashed or superseded runs are ignored."""
    out = []
    for other in committed_months(lake, cfg):
        if other == month:
            continue
        bid = lake.manifest.latest_committed(cfg.source, other)["batch_id"]
        path = lake.late_path(cfg, other, bid)
        if path.exists():
            n = duckdb.sql(f"SELECT count(*) FROM read_parquet('{path}') WHERE {cfg.partition_column} = ?",
                           params=[month]).fetchall()[0][0]
            if n:
                out.append((bid, str(path)))
    return out


def load_month(con: duckdb.DuckDBPyConnection, lake: Lake, cfg: SourceConfig, month: str) -> int:
    """Replace one month in fact_trip and the rollup from its committed lake partition plus late arrivals."""
    committed = lake.manifest.latest_committed(cfg.source, month)
    if committed is None:
        raise RuntimeError(f"{cfg.source} {month} has no committed batch; refusing to load")
    path = str(lake.curated_dir(cfg, month) / "data.parquet")
    late = late_files_for(lake, cfg, month)
    key = ", ".join(cfg.primary_key)
    # Late rows for this month, minus any trip the month's own file already has (no double counting).
    late_sql = " UNION ALL ".join(
        f"SELECT * FROM read_parquet('{p}') WHERE {cfg.partition_column} = '{month}'" for _, p in late
    )
    source_sql = f"SELECT * FROM read_parquet('{path}')"
    if late:
        source_sql += f"""
            UNION ALL BY NAME
            SELECT * FROM ({late_sql}) l
            WHERE ({key}) NOT IN (SELECT ({key}) FROM read_parquet('{path}'))"""

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM fact_trip WHERE pickup_month = ?", [month])
        con.execute("DELETE FROM agg_daily_zone_hour WHERE pickup_month = ?", [month])
        # Rows are inserted sorted by date then zone. DuckDB keeps min/max per row group (zone maps),
        # so date- and zone-filtered queries skip most of the table: the local analogue of a sort key.
        con.execute(f"""
            INSERT INTO fact_trip
            SELECT pickup_month,
                   CAST(strftime(pickup_datetime, '%Y%m%d') AS INTEGER),
                   hour(pickup_datetime),
                   pickup_datetime, dropoff_datetime,
                   pu_location_id, do_location_id, vendor_id, payment_type,
                   CAST(ratecode_id AS INTEGER), passenger_count, trip_distance,
                   date_diff('second', pickup_datetime, dropoff_datetime) / 60.0,
                   fare_amount, total_amount,
                   is_reversal, is_flex_fare, suspect_distance, zero_distance, unknown_zone
            FROM ({source_sql})
            ORDER BY pickup_datetime, pu_location_id
        """)
        con.execute("""
            INSERT INTO agg_daily_zone_hour
            SELECT pickup_month, pickup_date_key, pickup_hour, pu_location_id,
                   count(*) FILTER (WHERE NOT is_reversal),
                   sum(total_amount),
                   sum(trip_distance) FILTER (WHERE NOT suspect_distance),
                   sum(duration_min)
            FROM fact_trip WHERE pickup_month = ?
            GROUP BY ALL
            ORDER BY pickup_date_key, pu_location_id, pickup_hour
        """, [month])
        n = con.execute("SELECT count(*) FROM fact_trip WHERE pickup_month = ?", [month]).fetchone()[0]
        own = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
        con.execute("INSERT OR REPLACE INTO load_log VALUES (?, ?, ?, ?, ?, now())",
                    [month, committed["batch_id"], late_signature(late), n, n - own])
        _fill_missing_dims(con)
        con.execute("COMMIT")
        return n
    except Exception:
        con.execute("ROLLBACK")
        raise


def late_signature(late: list[tuple[str, str]]) -> str:
    return ",".join(sorted(bid for bid, _ in late))


def build(lake_root: str | Path, db_path: str | Path, contract: str | Path,
          zones_csv: str | Path | None = None, months: list[str] | None = None) -> dict[str, int]:
    """Load every committed month (or the given ones). Months already loaded from the same batch are skipped."""
    lake = Lake(lake_root)
    cfg = load_source_config(contract)
    with duckdb.connect(str(db_path)) as con:
        con.execute(DDL)
        _seed_static_dims(con, zones_csv)
        if months is None:
            months = committed_months(lake, cfg)
        loaded = {}
        for month in months:
            # Reload a month if its own batch changed OR a new late-arrival file now targets it.
            current = lake.manifest.latest_committed(cfg.source, month)
            sig = late_signature(late_files_for(lake, cfg, month))
            prev = con.execute("SELECT batch_id, late_batches FROM load_log WHERE pickup_month = ?", [month]).fetchone()
            if prev and current and prev == (current["batch_id"], sig):
                continue
            loaded[month] = load_month(con, lake, cfg, month)
        _fill_missing_dims(con)
        return loaded


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build the star schema from committed lake partitions.")
    p.add_argument("--lake", default="lake")
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--contract", default="configs/sources/tlc_yellow.yaml")
    p.add_argument("--zones", default="configs/reference/taxi_zone_lookup.csv")
    args = p.parse_args(argv)
    loaded = build(args.lake, args.db, args.contract, args.zones)
    if not loaded:
        print("warehouse up to date")
    for month, n in loaded.items():
        print(f"loaded {month}: {n:,} fact rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
