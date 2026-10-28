# Benchmarks

Measured on 2026-10-28 · 2 vCPU, 7 GB RAM Linux container · Python 3.13, Polars 2.0, DuckDB 2.0
Input: `yellow_tripdata_2025-01.parquet` (58.8 MB, 3,475,226 rows)

## Ingestion

| Run | Result | Wall time | Peak memory |
|---|---|---:|---:|
| First run | committed: 3,475,059 published, 167 quarantined (1 duplicate) | 5.5 s | 2.7 GB |
| Same file again | skipped (already committed) | 0.4 s | 97 MB |

## Crash recovery on real data

A run was crashed at each fault point, then rerun. The rerun's published data was compared to the
clean run by SHA-256 of its Arrow IPC bytes.

| Crash point | Rerun | Rows | Output identical to clean run |
|---|---|---:|---|
| after_raw | committed, 4.7 s | 3,475,059 | yes |
| after_quarantine | committed, 4.6 s | 3,475,059 | yes |
| after_publish_before_commit | committed, 4.6 s | 3,475,059 | yes |

## Flags in published data (Jan 2025)

| Flag | Rows |
|---|---:|
| is_flex_fare | 540,030 |
| is_reversal | 144,437 |
| zero_distance | 90,888 |
| unknown_zone | 28,354 |
| suspect_distance | 121 |

Reproduce: `python -m mdp.ingest --contract configs/sources/tlc_yellow.yaml --file <file> --month 2025-01 --lake lake/`
