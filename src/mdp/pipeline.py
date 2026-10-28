"""One generic ingestion pipeline, driven entirely by a source contract.

Guarantees (each has a test in tests/test_pipeline.py):
  1. Idempotent: rerunning the same file is a no-op, and rerunning after a crash at any
     step produces exactly the output of a clean run, never duplicates.
  2. Raw is immutable: each distinct input file is stored once, named by its checksum,
     and never modified or deleted.
  3. Partition-scoped: a run replaces exactly one month; other months are untouched.
  4. Nothing is published unless the contract and batch gates pass; a failed run leaves
     the previously published partition intact.

Lake layout:
  raw/<source>/<month>/<sha256>.parquet           immutable input copies
  curated/<source>/<partition_col>=<month>/       published, validated rows
  quarantine/<source>/<month>/<batch_id>.parquet  rejected rows + reasons
  _meta/manifest.duckdb                           run history + commit records
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from . import contracts, quality
from .config import SourceConfig
from .manifest import Manifest, RunStats, batch_id, file_checksum, utcnow

# Steps where tests can inject a crash, in the order the pipeline reaches them.
FAULT_POINTS = ("after_raw", "after_quarantine", "after_publish_before_commit")


class GateError(RuntimeError):
    """A batch-level threshold was breached; nothing was published."""


class InjectedFault(RuntimeError):
    """Raised on purpose by failure-injection tests."""


@dataclass(frozen=True)
class RunResult:
    batch_id: str
    status: str          # committed | skipped
    stats: RunStats


class Lake:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        (self.root / "_meta").mkdir(parents=True, exist_ok=True)
        self.manifest = Manifest(self.root / "_meta" / "manifest.duckdb")

    def raw_path(self, cfg: SourceConfig, month: str, checksum: str) -> Path:
        return self.root / "raw" / cfg.source / month / f"{checksum}.{cfg.format}"

    def curated_dir(self, cfg: SourceConfig, month: str) -> Path:
        return self.root / "curated" / cfg.source / f"{cfg.partition_column}={month}"

    def quarantine_path(self, cfg: SourceConfig, month: str, bid: str) -> Path:
        return self.root / "quarantine" / cfg.source / month / f"{bid}.parquet"

    def read_curated(self, cfg: SourceConfig, month: str) -> pl.DataFrame:
        return pl.read_parquet(self.curated_dir(cfg, month) / "data.parquet")


def _publish(df: pl.DataFrame, target: Path, bid: str) -> None:
    """Swap a fully written partition into place.

    Data is written to a staging directory first, so a crash mid-write never leaves a
    half-written partition at the target path. The swap itself is two directory renames;
    if a crash lands between them, the partition is briefly missing, but it is not
    committed, so the next run republishes it.
    """
    staging = target.parent / f"_staging_{target.name}_{bid}"
    retired = target.parent / f"_retired_{target.name}_{bid}"
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(retired, ignore_errors=True)
    staging.mkdir(parents=True)
    df.write_parquet(staging / "data.parquet")
    if target.exists():
        target.rename(retired)
    staging.rename(target)
    shutil.rmtree(retired, ignore_errors=True)


def run(lake: Lake, cfg: SourceConfig, input_path: str | Path, month: str,
        *, fault_point: str | None = None) -> RunResult:
    if fault_point is not None and fault_point not in FAULT_POINTS:
        raise ValueError(f"unknown fault point {fault_point!r}")

    started = utcnow()
    checksum = file_checksum(input_path)
    bid = batch_id(cfg.source, month, checksum)
    meta = dict(batch=bid, source=cfg.source, logical_partition=month,
                checksum=checksum, schema_version=cfg.schema_version, started_at=started)

    # Same bytes already committed for this month: nothing to do.
    prior = lake.manifest.latest_committed(cfg.source, month)
    if prior and prior["source_checksum"] == checksum:
        lake.manifest.record(**meta, status="skipped")
        return RunResult(bid, "skipped", RunStats())

    lake.manifest.record(**meta, status="started")
    stats = RunStats()
    try:
        # 1. Land the raw file once, keyed by its checksum. Never overwrite.
        raw = lake.raw_path(cfg, month, checksum)
        if not raw.exists():
            raw.parent.mkdir(parents=True, exist_ok=True)
            tmp = raw.with_suffix(".tmp")
            shutil.copyfile(input_path, tmp)
            tmp.rename(raw)
        if fault_point == "after_raw":
            raise InjectedFault(fault_point)

        # 2. Enforce the schema contract.
        df = pl.read_parquet(raw)
        stats.rows_read = df.height
        stats.additive_columns = contracts.check(df.schema, cfg).additive_columns
        df = contracts.conform(df, cfg).with_columns(
            pl.col(cfg.partition_derived_from).dt.strftime("%Y-%m").alias(cfg.partition_column)
        )

        # 3. Reject rules, then dedupe the survivors.
        valid, failed = quality.split_valid(df, cfg, month)
        valid, dups = quality.dedupe(valid, cfg.primary_key)
        quarantined = pl.concat([failed, dups], how="vertical_relaxed")
        stats.duplicate_rows = dups.height
        stats.rows_quarantined = quarantined.height

        if quarantined.height:
            q = lake.quarantine_path(cfg, month, bid)
            q.parent.mkdir(parents=True, exist_ok=True)
            quarantined.write_parquet(q)
        if fault_point == "after_quarantine":
            raise InjectedFault(fault_point)

        # 4. Batch gates: fail the whole run rather than publish a bad month.
        if stats.rows_read < cfg.min_rows:
            raise GateError(f"{stats.rows_read} rows read, minimum is {cfg.min_rows}")
        ratio = stats.rows_quarantined / max(stats.rows_read, 1)
        if ratio > cfg.max_quarantine_ratio:
            raise GateError(f"quarantine ratio {ratio:.2%} exceeds {cfg.max_quarantine_ratio:.2%}")

        # 5. Flag kept-but-notable rows, then publish, sorted so identical input
        #    always yields identical files.
        valid = quality.add_flags(valid, cfg)
        valid = valid.sort(valid.columns, nulls_last=True)
        _publish(valid, lake.curated_dir(cfg, month), bid)
        stats.rows_published = valid.height
        if fault_point == "after_publish_before_commit":
            raise InjectedFault(fault_point)

        # 6. Commit. Only now is the partition officially published.
        lake.manifest.record(**meta, status="committed", stats=stats)
        return RunResult(bid, "committed", stats)

    except Exception as e:
        lake.manifest.record(**meta, status="failed", stats=stats, error=f"{type(e).__name__}: {e}")
        raise
