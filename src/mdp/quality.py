"""Row-level quality rules and key deduplication.

Rules are SQL boolean expressions from the contract, evaluated by DuckDB.
A rule that is FALSE or NULL fails the row. Every required column also gets an
implicit not-null rule. Failed rows keep all their data plus a `quarantine_reason`
listing every rule they broke, so nothing is silently dropped.
"""
from __future__ import annotations

import re

import duckdb
import polars as pl

from .config import SourceConfig

_MONTH = re.compile(r"^\d{4}-\d{2}$")


def rules_for(cfg: SourceConfig, logical_partition: str) -> list[tuple[str, str]]:
    if not _MONTH.match(logical_partition):
        raise ValueError(f"logical partition must be YYYY-MM, got {logical_partition!r}")
    rules = [(f"not_null_{c.name}", f"{c.name} IS NOT NULL") for c in cfg.columns if c.required]
    for r in cfg.quality_rules:
        rules.append((r.name, r.expr.replace(":logical_partition", f"'{logical_partition}'")))
    return rules


def split_valid(df: pl.DataFrame, cfg: SourceConfig, logical_partition: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (valid_rows, quarantined_rows)."""
    checks = ", ".join(
        f"CASE WHEN coalesce(({expr}), FALSE) THEN NULL ELSE '{name}' END"
        for name, expr in rules_for(cfg, logical_partition)
    )
    with duckdb.connect() as con:
        con.register("batch", df.to_arrow())
        scored = con.sql(f"SELECT *, concat_ws(',', {checks}) AS quarantine_reason FROM batch").pl()
    failed = pl.col("quarantine_reason") != ""
    return scored.filter(~failed).drop("quarantine_reason"), scored.filter(failed)


def dedupe(df: pl.DataFrame, primary_key: tuple[str, ...]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Keep one row per key; return (unique, duplicates).

    Which copy survives must not depend on file order, or two runs over the same data
    could publish different rows. So we sort by every column first and keep the first.
    """
    ordered = df.sort(df.columns, nulls_last=True).with_row_index("_rn")
    is_first = ordered.select(pl.col("_rn") == pl.col("_rn").min().over(list(primary_key))).to_series()
    unique = ordered.filter(is_first).drop("_rn")
    dups = ordered.filter(~is_first).drop("_rn").with_columns(
        pl.lit("duplicate_key").alias("quarantine_reason")
    )
    return unique, dups
