"""Schema contract enforcement: does this file match what the contract promises?

Policy (each case has a test):
  missing required column        -> breaking: whole batch rejected
  missing optional column        -> filled with nulls, recorded
  extra (unknown) column         -> additive: allowed, recorded, not loaded
  type not safely castable       -> breaking: whole batch rejected
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .config import SourceConfig

POLARS_TYPES = {
    "int64": pl.Int64,
    "float64": pl.Float64,
    "string": pl.Utf8,
    "datetime": pl.Datetime("us"),
}

_INTS = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32}
# Observed dtypes that can be cast into each contract type without losing meaning.
SAFE_CASTS = {
    "int64": _INTS,
    "float64": _INTS | {pl.Float32, pl.Float64},
    "string": {pl.Utf8, pl.Categorical},
    "datetime": {pl.Datetime},
}


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class ContractResult:
    additive_columns: tuple[str, ...]
    missing_optional: tuple[str, ...]


def _safe(observed: pl.DataType, target: str) -> bool:
    return observed.base_type() in SAFE_CASTS[target]


def check(schema: pl.Schema, cfg: SourceConfig) -> ContractResult:
    present = set(schema.names())
    if missing := sorted(cfg.required_raw - present):
        raise ContractError(f"{cfg.source}: missing required columns {missing}")

    bad_types = [
        f"{c.raw_name}: {schema[c.raw_name]} cannot become {c.type}"
        for c in cfg.columns
        if c.raw_name in present and not _safe(schema[c.raw_name], c.type)
    ]
    if bad_types:
        raise ContractError(f"{cfg.source}: incompatible types {bad_types}")

    return ContractResult(
        additive_columns=tuple(sorted(present - cfg.known_raw)),
        missing_optional=tuple(sorted(
            c.raw_name for c in cfg.columns if not c.required and c.raw_name not in present
        )),
    )


def conform(df: pl.DataFrame, cfg: SourceConfig) -> pl.DataFrame:
    """Keep contract columns only, rename to canonical names, cast to contract types.
    Call only after check() has passed."""
    exprs = []
    for c in cfg.columns:
        dtype = POLARS_TYPES[c.type]
        if c.raw_name in df.columns:
            exprs.append(pl.col(c.raw_name).cast(dtype).alias(c.name))
        else:
            exprs.append(pl.lit(None, dtype=dtype).alias(c.name))
    return df.select(exprs)
