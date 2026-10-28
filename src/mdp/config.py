"""Load and validate a source contract (configs/sources/*.yaml).

The contract is the metadata that drives every pipeline run. Validation happens here,
at load time, so a broken config fails before any data is touched.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

SUPPORTED_TYPES = {"int64", "float64", "string", "datetime"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnSpec:
    raw_name: str   # name in the source file
    name: str       # canonical name in our tables
    type: str
    required: bool


@dataclass(frozen=True)
class QualityRule:
    name: str
    expr: str       # SQL boolean expression over canonical column names


@dataclass(frozen=True)
class SourceConfig:
    source: str
    format: str
    schema_version: int
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    partition_column: str
    partition_derived_from: str
    quality_rules: tuple[QualityRule, ...]
    flags: tuple[QualityRule, ...]   # kept rows, marked with a boolean column
    max_quarantine_ratio: float
    min_rows: int

    @property
    def required_raw(self) -> frozenset[str]:
        return frozenset(c.raw_name for c in self.columns if c.required)

    @property
    def known_raw(self) -> frozenset[str]:
        return frozenset(c.raw_name for c in self.columns)


def load_source_config(path: str | Path) -> SourceConfig:
    raw = yaml.safe_load(Path(path).read_text())
    try:
        columns = tuple(
            ColumnSpec(raw_name=k, name=v["name"], type=v["type"], required=bool(v.get("required", False)))
            for k, v in raw["columns"].items()
        )
        if bad := [c.raw_name for c in columns if c.type not in SUPPORTED_TYPES]:
            raise ConfigError(f"unsupported column types: {bad}")

        canonical = [c.name for c in columns]
        if len(canonical) != len(set(canonical)):
            raise ConfigError("two raw columns map to the same canonical name")

        pk = tuple(raw["primary_key"])
        if missing := set(pk) - set(canonical):
            raise ConfigError(f"primary key uses unknown columns: {sorted(missing)}")

        part = raw["partition"]
        if part["grain"] != "month":
            raise ConfigError("only monthly partitions are supported")
        if part["derived_from"] not in canonical:
            raise ConfigError(f"partition derived from unknown column {part['derived_from']!r}")

        gates = raw.get("batch_gates", {})
        ratio = float(gates.get("max_quarantine_ratio", 1.0))
        if not 0.0 <= ratio <= 1.0:
            raise ConfigError("max_quarantine_ratio must be between 0 and 1")

        rules = tuple(QualityRule(r["name"], r["expr"]) for r in raw.get("quality_rules", []))
        if len({r.name for r in rules}) != len(rules):
            raise ConfigError("quality rule names must be unique")

        flags = tuple(QualityRule(f["name"], f["expr"]) for f in raw.get("flags", []))
        flag_names = [f.name for f in flags]
        if len(set(flag_names)) != len(flag_names):
            raise ConfigError("flag names must be unique")
        if clash := set(flag_names) & (set(canonical) | {part["column"]}):
            raise ConfigError(f"flag names clash with columns: {sorted(clash)}")

        return SourceConfig(
            source=raw["source"],
            format=raw["format"],
            schema_version=int(raw["schema_version"]),
            columns=columns,
            primary_key=pk,
            partition_column=part["column"],
            partition_derived_from=part["derived_from"],
            quality_rules=rules,
            flags=flags,
            max_quarantine_ratio=ratio,
            min_rows=int(gates.get("min_rows", 0)),
        )
    except KeyError as e:
        raise ConfigError(f"missing config key: {e}") from e
