# Mobility Data Platform

A fault-tolerant, metadata-driven ingestion platform for NYC mobility data (TLC trips, NOAA weather).

> **Status: Stage 1 — data profiling.** The pipeline, contracts, and AWS deployment come next.

## Why

Public trip data is messy: trips that end before they start, negative fares, rows from the wrong month,
and no trip ID at all. Before building a pipeline, this project measures exactly what's wrong and records
a decision for each problem in [`docs/DATA_QUIRKS.md`](docs/DATA_QUIRKS.md).

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

pytest                                             # 19 tests, no data download needed
python scripts/download.py 2025-01                 # ~60 MB into data/raw/
python -m mdp.profile data/raw/yellow_tripdata_2025-01.parquet 2025-01 --out docs/profile_2025-01.md
```

## Layout

```
scripts/download.py     fetch one month of TLC data
src/mdp/profile.py      data-quality profiler (SQL checks via DuckDB)
tests/                  tests on a synthetic month with planted problems
docs/DATA_QUIRKS.md     findings + decisions (human-written)
```

## Roadmap

1. ✅ Profile the raw data
2. ⬜ Data contract + quality rules (from DATA_QUIRKS decisions)
3. ⬜ Idempotent ingestion with quarantine and run manifest
4. ⬜ Star schema in a warehouse + query-tuning case study
5. ⬜ AWS deployment (S3, Glue, Redshift) with Terraform
6. ⬜ Streaming path (Kinesis/Firehose)
7. ⬜ Shadow migration verifier
