"""Contract and quality-rule tests: every policy line gets a test that would fail if it broke."""
from datetime import datetime

import polars as pl
import pytest

from factories import CONTRACT, clean_trips, small_batch_contract, trip
from mdp import contracts, quality
from mdp.config import ConfigError, load_source_config


@pytest.fixture(scope="module")
def cfg():
    return small_batch_contract()


# ---------- config ----------

def test_contract_loads(cfg):
    assert cfg.source == "tlc_yellow"
    assert "vendor_id" in cfg.primary_key


def test_config_rejects_unknown_key_column(tmp_path):
    text = open(CONTRACT).read().replace("primary_key: [vendor_id", "primary_key: [no_such_col")
    p = tmp_path / "bad.yaml"
    p.write_text(text)
    with pytest.raises(ConfigError, match="primary key"):
        load_source_config(p)


# ---------- schema contract ----------

def test_missing_required_column_rejects_batch(cfg):
    df = pl.DataFrame(clean_trips(3)).drop("fare_amount")
    with pytest.raises(contracts.ContractError, match="fare_amount"):
        contracts.check(df.schema, cfg)


def test_missing_optional_column_is_filled_with_nulls(cfg):
    df = pl.DataFrame(clean_trips(3)).drop("passenger_count")
    result = contracts.check(df.schema, cfg)
    assert result.missing_optional == ("passenger_count",)
    out = contracts.conform(df, cfg)
    assert out["passenger_count"].null_count() == 3


def test_extra_column_is_additive(cfg):
    df = pl.DataFrame(clean_trips(3)).with_columns(pl.lit(0.75).alias("cbd_congestion_fee"))
    result = contracts.check(df.schema, cfg)
    assert result.additive_columns == ("cbd_congestion_fee",)
    assert "cbd_congestion_fee" not in contracts.conform(df, cfg).columns


def test_incompatible_type_rejects_batch(cfg):
    df = pl.DataFrame(clean_trips(3)).with_columns(pl.lit("eighteen").alias("fare_amount"))
    with pytest.raises(contracts.ContractError, match="fare_amount"):
        contracts.check(df.schema, cfg)


def test_safe_widening_cast_is_allowed(cfg):
    df = pl.DataFrame(clean_trips(3)).with_columns(pl.col("PULocationID").cast(pl.Int32))
    contracts.check(df.schema, cfg)
    assert contracts.conform(df, cfg)["pu_location_id"].dtype == pl.Int64


def test_conform_renames_to_canonical(cfg):
    out = contracts.conform(pl.DataFrame(clean_trips(1)), cfg)
    assert out.columns == [c.name for c in cfg.columns]


# ---------- row rules ----------

def conformed(cfg, rows):
    df = contracts.conform(pl.DataFrame(rows), cfg)
    return df.with_columns(pl.col("pickup_datetime").dt.strftime("%Y-%m").alias("pickup_month"))


@pytest.mark.parametrize("bad_row,rule", [
    (trip(tpep_dropoff_datetime=datetime(2025, 1, 15, 9, 0)), "dropoff_not_before_pickup"),
    (trip(tpep_dropoff_datetime=datetime(2025, 1, 17, 10, 0)), "duration_under_24h"),
    (trip(tpep_pickup_datetime=datetime(2024, 12, 31, 23, 0),
          tpep_dropoff_datetime=datetime(2024, 12, 31, 23, 30)), "pickup_in_logical_month"),
    (trip(fare_amount=None), "not_null_fare_amount"),
])
def test_each_rule_quarantines_its_row(cfg, bad_row, rule):
    valid, bad = quality.split_valid(conformed(cfg, clean_trips(5) + [bad_row]), cfg, "2025-01")
    assert valid.height == 5
    assert bad.height == 1
    assert rule in bad["quarantine_reason"][0].split(",")


def test_row_breaking_two_rules_lists_both(cfg):
    row = trip(tpep_pickup_datetime=datetime(2024, 12, 31, 23, 0),
               tpep_dropoff_datetime=datetime(2024, 12, 31, 22, 0))
    _, bad = quality.split_valid(conformed(cfg, [row]), cfg, "2025-01")
    assert set(bad["quarantine_reason"][0].split(",")) == {"pickup_in_logical_month", "dropoff_not_before_pickup"}


@pytest.mark.parametrize("row", [
    trip(fare_amount=-5.0, total_amount=-8.0),     # reversal / refund
    trip(trip_distance=276423.57),                 # broken meter distance, real fare
    trip(trip_distance=-1.0),
    trip(payment_type=0, passenger_count=None, RatecodeID=None),   # Flex Fare
])
def test_real_records_are_kept_not_rejected(cfg, row):
    """Jan 2025 evidence: these are genuine money records. Rejecting them would lose revenue."""
    valid, bad = quality.split_valid(conformed(cfg, [row]), cfg, "2025-01")
    assert (valid.height, bad.height) == (1, 0)


def test_quarantine_keeps_all_original_data(cfg):
    row = trip(tpep_dropoff_datetime=datetime(2025, 1, 15, 9, 0), total_amount=77.7)
    _, bad = quality.split_valid(conformed(cfg, [row]), cfg, "2025-01")
    assert bad["total_amount"][0] == 77.7


def test_bad_partition_string_rejected(cfg):
    with pytest.raises(ValueError):
        quality.rules_for(cfg, "2025-1")


# ---------- flags ----------

def flagged(cfg, rows):
    return quality.add_flags(conformed(cfg, rows), cfg).row(0, named=True)


@pytest.mark.parametrize("row,flag", [
    (trip(fare_amount=-5.0, total_amount=-8.0), "is_reversal"),
    (trip(fare_amount=0.0, total_amount=-2.5), "is_reversal"),
    (trip(payment_type=0), "is_flex_fare"),
    (trip(trip_distance=276423.57), "suspect_distance"),
    (trip(trip_distance=0.0), "zero_distance"),
    (trip(PULocationID=264), "unknown_zone"),
])
def test_each_flag_marks_its_row(cfg, row, flag):
    flags = {f.name: v for f in cfg.flags for k, v in flagged(cfg, [row]).items() if k == f.name}
    assert flags[flag] is True
    assert sum(flags.values()) == 1          # and nothing else


def test_clean_row_has_no_flags(cfg):
    row = flagged(cfg, [trip()])
    assert not any(row[f.name] for f in cfg.flags)


def test_null_in_flag_input_means_not_flagged(cfg):
    row = flagged(cfg, [trip(payment_type=None)])
    assert row["is_flex_fare"] is False


def test_flag_name_clash_rejected(tmp_path):
    text = open(CONTRACT).read().replace("- name: zero_distance", "- name: trip_distance")
    p = tmp_path / "bad.yaml"
    p.write_text(text)
    with pytest.raises(ConfigError, match="clash"):
        load_source_config(p)


# ---------- dedupe ----------

def test_charge_and_reversal_are_not_duplicates(cfg):
    """Jan 2025: 294 of 299 old-key collisions were +X/-X pairs. Both halves must survive."""
    charge = trip(fare_amount=0.0, total_amount=2.5)
    reversal = trip(fare_amount=0.0, total_amount=-2.5)
    unique, dups = quality.dedupe(conformed(cfg, [charge, reversal]), cfg.primary_key)
    assert (unique.height, dups.height) == (2, 0)
    assert unique["total_amount"].sum() == 0.0


def test_dedupe_keeps_one_per_key(cfg):
    rows = clean_trips(4)
    rows.append({**rows[0], "passenger_count": 3.0})   # same key, conflicting re-send (seen once in Jan 2025)
    rows.append(dict(rows[1]))                         # exact copy
    unique, dups = quality.dedupe(conformed(cfg, rows), cfg.primary_key)
    assert unique.height == 4
    assert dups.height == 2
    assert set(dups["quarantine_reason"]) == {"duplicate_key"}


def test_dedupe_is_order_independent(cfg):
    """Shuffled input must publish the exact same rows, or reruns would not be reproducible."""
    rows = clean_trips(4)
    rows.append({**rows[0], "passenger_count": 3.0})
    df = conformed(cfg, rows)
    a, _ = quality.dedupe(df, cfg.primary_key)
    b, _ = quality.dedupe(df.sample(fraction=1.0, shuffle=True, seed=7), cfg.primary_key)
    assert a.sort(a.columns).equals(b.sort(b.columns))
