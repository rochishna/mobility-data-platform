"""Command-line entry point for one ingestion run.

Usage:
    python -m mdp.ingest --contract configs/sources/tlc_yellow.yaml \\
        --file data/raw/yellow_tripdata_2025-01.parquet --month 2025-01 --lake lake/
"""
from __future__ import annotations

import argparse
import sys

from .config import load_source_config
from .pipeline import Lake, run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest one month of one source into the lake.")
    p.add_argument("--contract", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--lake", default="lake")
    args = p.parse_args(argv)

    cfg = load_source_config(args.contract)
    try:
        result = run(Lake(args.lake), cfg, args.file, args.month)
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    s = result.stats
    print(f"{result.status}: batch {result.batch_id}")
    if result.status == "committed":
        print(f"  read {s.rows_read:,} · published {s.rows_published:,} · "
              f"quarantined {s.rows_quarantined:,} (duplicates {s.duplicate_rows:,})")
        if s.additive_columns:
            print(f"  additive columns not loaded: {', '.join(s.additive_columns)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
