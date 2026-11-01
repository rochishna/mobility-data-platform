# Query-Tuning Case Study

Measured 2026-10-31 on 2 vCPU, Linux, DuckDB 2.0.0. Data: 2025-01, 3,475,059 trips. Median of 15 runs after a warm-up. Each tuned variant is checked to return exactly the raw answer.

Reproduce: `python benchmarks/query_tuning.py`

## Three ways to answer the same question

- **Raw:** scan curated Parquet and compute everything per query.
- **Star:** `fact_trip` joined to dimensions, stored sorted by date and pickup zone.
- **Rollup:** `agg_daily_zone_hour`, pre-aggregated to day × hour × zone: 96,986 rows instead of 3,475,059 (36× fewer).

| Query | Question | Raw (ms) | Star (ms) | Rollup (ms) | Raw → rollup |
|---|---|---:|---:|---:|---:|
| Q1 daily trend | Net revenue and trip count per day for the month | 83.5 | 27.1 | 2.6 | 33× faster |
| Q2 demand heatmap | Trips by weekday and hour, per borough | 104.4 | 44.8 | 4.6 | 23× faster |
| Q3 zone drill-down | One zone, one day: trips and revenue by hour | 11.0 | 1.9 | 1.5 | 7× faster |

Q3 uses the busiest zone-day in the data (zone 161, 2025-01-23).

## What physical sort order alone buys

Same table, same query (Q3, star version), same rows; only the on-disk order differs. Sorted by date then zone, DuckDB's per-row-group min/max stats let the filter skip most of the table. This is the local equivalent of a Redshift sort key.

| Storage order | Q3 (ms) |
|---|---:|
| Shuffled | 7.7 |
| Sorted by date, zone | 1.6 |

Sorting alone: 5× faster on the selective query.

## Takeaways

- Dashboard tiles read the rollup; ad-hoc questions read `fact_trip`.
- The rollup is rebuilt in the same transaction as the fact month, so the two can never disagree; the revenue totals match to the cent.
- Trips exclude reversal rows; revenue is net (charges plus reversals), consistent with [DATA_QUIRKS.md](DATA_QUIRKS.md).
