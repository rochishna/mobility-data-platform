# Mobility Data Platform

A fault-tolerant, metadata-driven ingestion platform for NYC mobility data (TLC trips, NOAA weather).

> **Status:** profiling, data contracts, and idempotent ingestion are done, tested, and run on real data:
> 3.47M January 2025 trips in 5.5 s, with crash-and-rerun output byte-identical to a clean run.
> AWS deployment, warehouse modeling, and the dashboard come next.

## What it guarantees

Each guarantee has a test that fails if it breaks. `tests/test_pipeline.py` crashes runs on purpose
at every step and checks that the rerun matches a clean run exactly.

| Guarantee | How | Proof |
|---|---|---|
| Rerunning the same file is a no-op | batch id = hash(source, month, file checksum); committed batches are skipped | `test_rerun_same_file_is_noop` |
| A crash at any step, then a rerun, yields exactly the clean-run output | publish to staging → swap → commit record written **last** | `test_crash_then_rerun_matches_clean_run` (3 fault points) |
| Raw input is immutable | stored once per checksum, never overwritten | `test_raw_is_immutable_and_kept_per_version` |
| A corrected file replaces its month instead of appending | partition-scoped overwrite | `test_corrected_file_replaces_partition_not_appends` |
| Bad data never publishes, and the last good month survives | schema contract + batch gates before publish | `test_gate_breach_publishes_nothing_and_keeps_prior` |
| No row disappears silently | rejected rows go to quarantine with every rule they broke | `test_clean_run_commits` |
| Reversals are never deduplicated away | key includes `total_amount`, so +X/−X pairs stay distinct | `test_charge_and_reversal_are_not_duplicates` |
| Same input, same output, regardless of row order | deterministic dedupe and sorted publication | `test_dedupe_is_order_independent` |

## What the real data taught it

Profiling January 2025 overturned the obvious rules. 4% of fares are negative, but they're reversals and
Flex Fare adjustments, not errors. The "duplicate" trips were charge/reversal pairs that net to $0, so
deduplicating them would have corrupted revenue. The contract now **rejects** only rows whose core facts are
unusable (167 of 3.47M) and **flags** real-but-unusual records instead. Evidence for every decision:
[`docs/DATA_QUIRKS.md`](docs/DATA_QUIRKS.md).

## How a source is defined

Sources are onboarded through a YAML data contract instead of new code. See
[`configs/sources/tlc_yellow.yaml`](configs/sources/tlc_yellow.yaml): canonical names and types, required
columns, candidate key, monthly partition, SQL quality rules, and batch gates.

Each source defines **reject rules** (row quarantined), **flags** (row kept, boolean column added), and **batch
gates** (whole run fails, nothing published).

Schema drift policy: a missing required column or an incompatible type rejects the batch. A missing optional
column is filled with nulls. Unknown new columns are allowed, recorded in the run manifest, and not loaded.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest                                                  # 68 tests, no download needed

python scripts/download.py 2025-01
python -m mdp.profile data/raw/yellow_tripdata_2025-01.parquet 2025-01 --out docs/profile_2025-01.md
python -m mdp.ingest --contract configs/sources/tlc_yellow.yaml \
    --file data/raw/yellow_tripdata_2025-01.parquet --month 2025-01 --lake lake/
```

## Layout

```
configs/sources/        data contracts, one per source
src/mdp/profile.py      data-quality profiler (SQL checks via DuckDB)
src/mdp/config.py       contract loading and validation
src/mdp/contracts.py    schema checks and conformance
src/mdp/quality.py      row rules, quarantine, deterministic dedupe
src/mdp/manifest.py     run history and commit records
src/mdp/pipeline.py     the ingestion run: land → check → quarantine → gate → publish → commit
src/mdp/ingest.py       command-line entry point
docs/DATA_QUIRKS.md     findings from real data and the decision for each
```

Lake layout: `raw/` (immutable inputs) · `curated/<source>/pickup_month=YYYY-MM/` · `quarantine/` ·
`_meta/manifest.duckdb`.

## Roadmap

1. ✅ Profile the raw data
2. ✅ Data contract and quality rules
3. ✅ Idempotent ingestion with quarantine, run manifest, and failure-injection tests
4. ✅ Real-data decisions ([DATA_QUIRKS.md](docs/DATA_QUIRKS.md)) and measured benchmarks ([BENCHMARKS.md](docs/BENCHMARKS.md))
5. ⬜ NOAA weather as a second source (config only)
6. ⬜ Star schema in Redshift + query-tuning case study
7. ⬜ AWS deployment (S3, Glue, Redshift) with Terraform
8. ⬜ Plotly Dash analytics app
