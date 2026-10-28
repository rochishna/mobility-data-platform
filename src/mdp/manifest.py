"""Run manifest: the record of every run, and the commit point for publication.

A partition counts as published only when a 'committed' row exists for it. That row is
written LAST, after the curated data is in place. So a crash at any earlier point leaves
nothing committed, and the next run simply redoes the work.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    batch_id          VARCHAR,
    source            VARCHAR,
    logical_partition VARCHAR,
    source_checksum   VARCHAR,
    schema_version    INTEGER,
    status            VARCHAR,   -- started | committed | failed | skipped
    rows_read         BIGINT,
    rows_published    BIGINT,
    rows_quarantined  BIGINT,
    duplicate_rows    BIGINT,
    additive_columns  VARCHAR,   -- JSON list
    error             VARCHAR,
    started_at        TIMESTAMP,
    finished_at       TIMESTAMP
)
"""


def batch_id(source: str, logical_partition: str, checksum: str) -> str:
    """Deterministic: the same source, partition and bytes always give the same id."""
    return hashlib.sha256(f"{source}|{logical_partition}|{checksum}".encode()).hexdigest()[:16]


def file_checksum(path: str | Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class RunStats:
    rows_read: int = 0
    rows_published: int = 0
    rows_quarantined: int = 0
    duplicate_rows: int = 0
    additive_columns: tuple[str, ...] = field(default_factory=tuple)


class Manifest:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with duckdb.connect(self.path) as con:
            con.execute(_SCHEMA)

    def latest_committed(self, source: str, logical_partition: str) -> dict | None:
        with duckdb.connect(self.path) as con:
            row = con.execute(
                """SELECT batch_id, source_checksum FROM runs
                   WHERE source = ? AND logical_partition = ? AND status = 'committed'
                   ORDER BY finished_at DESC LIMIT 1""",
                [source, logical_partition],
            ).fetchone()
        return {"batch_id": row[0], "source_checksum": row[1]} if row else None

    def record(self, *, batch: str, source: str, logical_partition: str, checksum: str,
               schema_version: int, status: str, started_at: datetime,
               stats: RunStats | None = None, error: str | None = None) -> None:
        s = stats or RunStats()
        with duckdb.connect(self.path) as con:
            con.execute(
                "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [batch, source, logical_partition, checksum, schema_version, status,
                 s.rows_read, s.rows_published, s.rows_quarantined, s.duplicate_rows,
                 json.dumps(list(s.additive_columns)), error, started_at, utcnow()],
            )

    def history(self, source: str | None = None) -> list[dict]:
        with duckdb.connect(self.path) as con:
            sql = "SELECT * FROM runs" + (" WHERE source = ?" if source else "") + " ORDER BY finished_at"
            cur = con.execute(sql, [source] if source else [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
