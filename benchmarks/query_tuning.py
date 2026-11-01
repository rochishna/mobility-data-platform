"""Query-tuning case study: the same dashboard questions, answered three ways.

  A. raw     scan the curated Parquet directly, computing everything per query
  B. star    fact_trip sorted by date and zone (zone maps let filters skip row groups)
  C. rollup  agg_daily_zone_hour, pre-aggregated at the grain the dashboard needs

Plus a sort-order experiment: the selective drill-down query against fact_trip as loaded
(sorted) versus a shuffled copy, to isolate what physical order alone buys.

Every variant must return the same answer as the raw version, or the script fails.
Timings: median of N runs after one warm-up, same process, same machine.

Usage:
    python benchmarks/query_tuning.py --db warehouse.duckdb --lake lake --out docs/QUERY_TUNING.md
"""
from __future__ import annotations

import argparse
import os
import platform
import statistics
import time
from datetime import date
from pathlib import Path

import duckdb

# Trips held for review are excluded from the rollup, so raw and star must exclude them too, or the
# answers would (correctly) fail the equality check below. Approved trips count everywhere.
_KEY = "vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount, total_amount"
COUNTED = (f"(NOT coalesce(suspect_fare, FALSE) OR ({_KEY}) IN "
           f"(SELECT ({_KEY}) FROM review_decision WHERE decision = 'approved'))")

QUERIES = {
    "Q1 daily trend": {
        "question": "Net revenue and trip count per day for the month",
        "raw": """
            SELECT CAST(pickup_datetime AS DATE) AS day,
                   count(*) FILTER (WHERE NOT is_reversal) AS trips, round(sum(total_amount), 2) AS revenue
            FROM read_parquet('{curated}') WHERE {counted} GROUP BY 1 ORDER BY 1""",
        "star": """
            SELECT d.date AS day, count(*) FILTER (WHERE NOT f.is_reversal) AS trips, round(sum(f.total_amount), 2) AS revenue
            FROM fact_trip f JOIN dim_date d ON d.date_key = f.pickup_date_key WHERE {counted} GROUP BY 1 ORDER BY 1""",
        "rollup": """
            SELECT d.date AS day, sum(a.trips) AS trips, round(sum(a.net_revenue), 2) AS revenue
            FROM agg_daily_zone_hour a JOIN dim_date d ON d.date_key = a.pickup_date_key GROUP BY 1 ORDER BY 1""",
    },
    "Q2 demand heatmap": {
        "question": "Trips by weekday and hour, per borough",
        "raw": """
            SELECT coalesce(l.borough, 'Unknown') AS borough, isodow(t.pickup_datetime) AS dow, hour(t.pickup_datetime) AS hr,
                   count(*) FILTER (WHERE NOT t.is_reversal) AS trips
            FROM read_parquet('{curated}') t LEFT JOIN dim_location l ON l.location_id = t.pu_location_id
            WHERE {counted} GROUP BY ALL ORDER BY ALL""",
        "star": """
            SELECT l.borough, d.day_of_week AS dow, f.pickup_hour AS hr, count(*) FILTER (WHERE NOT f.is_reversal) AS trips
            FROM fact_trip f JOIN dim_date d ON d.date_key = f.pickup_date_key
                             JOIN dim_location l ON l.location_id = f.pu_location_id
            WHERE {counted} GROUP BY ALL ORDER BY ALL""",
        "rollup": """
            SELECT l.borough, d.day_of_week AS dow, a.pickup_hour AS hr, sum(a.trips) AS trips
            FROM agg_daily_zone_hour a JOIN dim_date d ON d.date_key = a.pickup_date_key
                                       JOIN dim_location l ON l.location_id = a.pu_location_id
            GROUP BY ALL ORDER BY ALL""",
    },
    "Q3 zone drill-down": {
        "question": "One zone, one day: trips and revenue by hour",
        "raw": """
            SELECT hour(pickup_datetime) AS hr, count(*) FILTER (WHERE NOT is_reversal) AS trips, round(sum(total_amount), 2) AS revenue
            FROM read_parquet('{curated}')
            WHERE pickup_datetime >= TIMESTAMP '{day}' AND pickup_datetime < TIMESTAMP '{day}' + INTERVAL 1 DAY
              AND pu_location_id = {zone} AND {counted}
            GROUP BY 1 ORDER BY 1""",
        "star": """
            SELECT pickup_hour AS hr, count(*) FILTER (WHERE NOT is_reversal) AS trips, round(sum(total_amount), 2) AS revenue
            FROM fact_trip WHERE pickup_date_key = {day_key} AND pu_location_id = {zone} AND {counted}
            GROUP BY 1 ORDER BY 1""",
        "rollup": """
            SELECT pickup_hour AS hr, sum(trips) AS trips, round(sum(net_revenue), 2) AS revenue
            FROM agg_daily_zone_hour WHERE pickup_date_key = {day_key} AND pu_location_id = {zone}
            GROUP BY 1 ORDER BY 1""",
    },
}


def timed(con, sql: str, runs: int) -> tuple[float, list]:
    result = con.execute(sql).fetchall()   # warm-up, and the answer we verify
    samples = []
    for _ in range(runs):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        samples.append((time.perf_counter() - t) * 1000)
    return statistics.median(samples), result


def busiest_zone_and_day(con) -> tuple[int, str]:
    zone, day = con.execute("""
        SELECT pu_location_id, strptime(pickup_date_key::VARCHAR, '%Y%m%d')::DATE
        FROM agg_daily_zone_hour GROUP BY 1, 2 ORDER BY sum(trips) DESC LIMIT 1""").fetchone()
    return zone, str(day)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--lake", default="lake")
    p.add_argument("--month", default="2025-01")
    p.add_argument("--runs", type=int, default=15)
    p.add_argument("--out", default="docs/QUERY_TUNING.md")
    args = p.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    curated = str(Path(args.lake) / "curated/tlc_yellow" / f"pickup_month={args.month}" / "data.parquet")
    zone, day = busiest_zone_and_day(con)
    params = dict(curated=curated, zone=zone, day=day, day_key=day.replace("-", ""), counted=COUNTED)
    sizes = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("fact_trip", "agg_daily_zone_hour")}

    rows = []
    for name, q in QUERIES.items():
        times, answers = {}, {}
        for variant in ("raw", "star", "rollup"):
            times[variant], answers[variant] = timed(con, q[variant].format(**params), args.runs)
        for variant in ("star", "rollup"):
            if answers[variant] != answers["raw"]:
                raise SystemExit(f"{name}: {variant} answer differs from raw. Benchmark invalid.")
        rows.append((name, q["question"], times))
        print(f"{name:20s} raw {times['raw']:7.1f} ms  star {times['star']:7.1f} ms  rollup {times['rollup']:6.1f} ms  (answers match)")

    # Sort-order experiment: same query, same data, only physical order differs.
    mem = duckdb.connect()
    mem.execute(f"ATTACH '{args.db}' AS wh (READ_ONLY)")
    mem.execute("CREATE TABLE review_decision AS SELECT * FROM wh.review_decision")
    mem.execute("CREATE TABLE sorted   AS SELECT * FROM wh.fact_trip ORDER BY pickup_date_key, pu_location_id")
    mem.execute("CREATE TABLE shuffled AS SELECT * FROM wh.fact_trip ORDER BY hash(pickup_datetime, pu_location_id, total_amount)")
    drill = QUERIES["Q3 zone drill-down"]["star"].format(**params)
    t_sorted, a1 = timed(mem, drill.replace("FROM fact_trip", "FROM sorted"), args.runs)
    t_shuffled, a2 = timed(mem, drill.replace("FROM fact_trip", "FROM shuffled"), args.runs)
    assert a1 == a2, "sort experiment answers differ"
    print(f"sort order (Q3):     shuffled {t_shuffled:.1f} ms  sorted {t_sorted:.1f} ms")

    def x(a, b):
        return f"{a / b:,.0f}×" if b > 0 else "–"

    lines = [
        "# Query-Tuning Case Study",
        "",
        f"Measured {date.today()} on {os.cpu_count()} vCPU, {platform.system()}, DuckDB {duckdb.__version__}. "
        f"Data: {args.month}, {sizes['fact_trip']:,} trips. Median of {args.runs} runs after a warm-up. "
        "Each tuned variant is checked to return exactly the raw answer.",
        "",
        "Reproduce: `python benchmarks/query_tuning.py`",
        "",
        "## Three ways to answer the same question",
        "",
        "- **Raw:** scan curated Parquet and compute everything per query.",
        "- **Star:** `fact_trip` joined to dimensions, stored sorted by date and pickup zone.",
        f"- **Rollup:** `agg_daily_zone_hour`, pre-aggregated to day × hour × zone: "
        f"{sizes['agg_daily_zone_hour']:,} rows instead of {sizes['fact_trip']:,} "
        f"({sizes['fact_trip'] / sizes['agg_daily_zone_hour']:.0f}× fewer).",
        "",
        "| Query | Question | Raw (ms) | Star (ms) | Rollup (ms) | Raw → rollup |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for name, question, t in rows:
        lines.append(f"| {name} | {question} | {t['raw']:.1f} | {t['star']:.1f} | {t['rollup']:.1f} | {x(t['raw'], t['rollup'])} faster |")
    lines += [
        "",
        f"Q3 uses the busiest zone-day in the data (zone {zone}, {day}).",
        "",
        "## What physical sort order alone buys",
        "",
        "Same table, same query (Q3, star version), same rows; only the on-disk order differs. "
        "Sorted by date then zone, DuckDB's per-row-group min/max stats let the filter skip most of the table. "
        "This is the local equivalent of a Redshift sort key.",
        "",
        "| Storage order | Q3 (ms) |",
        "|---|---:|",
        f"| Shuffled | {t_shuffled:.1f} |",
        f"| Sorted by date, zone | {t_sorted:.1f} |",
        "",
        f"Sorting alone: {x(t_shuffled, t_sorted)} faster on the selective query.",
        "",
        "## Takeaways",
        "",
        "- Dashboard tiles read the rollup; ad-hoc questions read `fact_trip`.",
        "- The rollup is rebuilt in the same transaction as the fact month, so the two can never disagree; "
        "the revenue totals match to the cent.",
        "- Trips exclude reversal rows; revenue is net (charges plus reversals), consistent with "
        "[DATA_QUIRKS.md](DATA_QUIRKS.md).",
    ]
    Path(args.out).write_text("\n".join(lines) + "\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
