"""Pipeline guarantees, proven by failure injection.

The pattern: get the output of a clean run, then crash a run at each step, rerun it,
and assert the result is byte-for-byte the same data as the clean run.
"""
from datetime import datetime

import polars as pl
import pytest

from factories import CONTRACT, clean_trips, trip
from mdp import pipeline
from mdp.config import load_source_config
from mdp.contracts import ContractError
from mdp.pipeline import FAULT_POINTS, GateError, InjectedFault, Lake, run


@pytest.fixture(scope="module")
def cfg():
    return load_source_config(CONTRACT)


def january_rows():
    """60 clean trips + one each of: bad duration, duplicate key, wrong month."""
    rows = clean_trips(60)
    rows.append(trip(tpep_dropoff_datetime=datetime(2025, 1, 15, 9, 0)))       # rule failure
    rows.append(dict(rows[3]))                                                 # duplicate
    rows.append(trip(tpep_pickup_datetime=datetime(2024, 12, 31, 23, 0),
                     tpep_dropoff_datetime=datetime(2024, 12, 31, 23, 20)))    # out of month
    return rows


def write(tmp_path, name, rows):
    path = tmp_path / name
    pl.DataFrame(rows).write_parquet(path)
    return path


@pytest.fixture
def jan_file(tmp_path):
    return write(tmp_path, "jan.parquet", january_rows())


def statuses(lake, cfg):
    return [r["status"] for r in lake.manifest.history(cfg.source)]


# ---------- happy path ----------

def test_clean_run_commits(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    result = run(lake, cfg, jan_file, "2025-01")
    assert result.status == "committed"
    assert (result.stats.rows_read, result.stats.rows_published, result.stats.rows_quarantined) == (63, 60, 3)
    assert result.stats.duplicate_rows == 1
    assert lake.read_curated(cfg, "2025-01").height == 60
    q = pl.read_parquet(lake.quarantine_path(cfg, "2025-01", result.batch_id))
    assert set(q["quarantine_reason"]) == {"dropoff_not_before_pickup", "duplicate_key", "pickup_in_logical_month"}


def test_batch_id_is_deterministic(tmp_path, cfg, jan_file):
    a = run(Lake(tmp_path / "a"), cfg, jan_file, "2025-01")
    b = run(Lake(tmp_path / "b"), cfg, jan_file, "2025-01")
    assert a.batch_id == b.batch_id


# ---------- idempotency ----------

def test_rerun_same_file_is_noop(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    run(lake, cfg, jan_file, "2025-01")
    before = lake.read_curated(cfg, "2025-01")
    assert run(lake, cfg, jan_file, "2025-01").status == "skipped"
    assert lake.read_curated(cfg, "2025-01").equals(before)
    assert statuses(lake, cfg) == ["started", "committed", "skipped"]


@pytest.mark.parametrize("fault", FAULT_POINTS)
def test_crash_then_rerun_matches_clean_run(tmp_path, cfg, jan_file, fault):
    clean = Lake(tmp_path / "clean")
    run(clean, cfg, jan_file, "2025-01")
    expected = clean.read_curated(cfg, "2025-01")

    lake = Lake(tmp_path / "crashy")
    with pytest.raises(InjectedFault):
        run(lake, cfg, jan_file, "2025-01", fault_point=fault)
    assert lake.manifest.latest_committed(cfg.source, "2025-01") is None   # crash never commits

    result = run(lake, cfg, jan_file, "2025-01")                             # not skipped: nothing committed
    assert result.status == "committed"
    assert lake.read_curated(cfg, "2025-01").equals(expected)                # identical, no duplicates
    assert statuses(lake, cfg) == ["started", "failed", "started", "committed"]


def test_no_staging_debris_after_crash_and_rerun(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    with pytest.raises(InjectedFault):
        run(lake, cfg, jan_file, "2025-01", fault_point="after_publish_before_commit")
    run(lake, cfg, jan_file, "2025-01")
    leftovers = [p.name for p in (lake.root / "curated" / cfg.source).iterdir() if p.name.startswith("_")]
    assert leftovers == []


# ---------- corrections and partition scope ----------

def test_corrected_file_replaces_partition_not_appends(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    run(lake, cfg, jan_file, "2025-01")
    fixed = write(tmp_path, "jan_v2.parquet", clean_trips(25))
    assert run(lake, cfg, fixed, "2025-01").status == "committed"
    assert lake.read_curated(cfg, "2025-01").height == 25


def test_raw_is_immutable_and_kept_per_version(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    run(lake, cfg, jan_file, "2025-01")
    raw_dir = lake.root / "raw" / cfg.source / "2025-01"
    first = {p.name: p.read_bytes() for p in raw_dir.iterdir()}
    run(lake, cfg, write(tmp_path, "jan_v2.parquet", clean_trips(25)), "2025-01")
    after = {p.name: p.read_bytes() for p in raw_dir.iterdir()}
    assert len(after) == 2
    assert all(after[name] == data for name, data in first.items())          # old version untouched


def test_months_are_independent(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    run(lake, cfg, jan_file, "2025-01")
    jan = lake.read_curated(cfg, "2025-01")
    feb_rows = clean_trips(10, start=datetime(2025, 2, 3, 8, 0))
    run(lake, cfg, write(tmp_path, "feb.parquet", feb_rows), "2025-02")
    assert lake.read_curated(cfg, "2025-01").equals(jan)
    assert lake.read_curated(cfg, "2025-02").height == 10


def test_same_file_for_different_month_is_not_skipped(tmp_path, cfg):
    """Skip logic is per (source, month), so it can't wrongly skip another month."""
    lake = Lake(tmp_path / "lake")
    rows = clean_trips(5)
    path = write(tmp_path, "f.parquet", rows)
    run(lake, cfg, path, "2025-01")
    with pytest.raises(GateError):          # all 5 rows are January -> 100% quarantined for Feb
        run(lake, cfg, path, "2025-02")


# ---------- failures publish nothing ----------

def test_gate_breach_publishes_nothing_and_keeps_prior(tmp_path, cfg, jan_file):
    lake = Lake(tmp_path / "lake")
    run(lake, cfg, jan_file, "2025-01")
    good = lake.read_curated(cfg, "2025-01")
    mostly_bad = clean_trips(5) + [trip(trip_distance=-1.0, PULocationID=i) for i in range(5)]
    with pytest.raises(GateError, match="quarantine ratio"):
        run(lake, cfg, write(tmp_path, "bad.parquet", mostly_bad), "2025-01")
    assert lake.read_curated(cfg, "2025-01").equals(good)
    assert statuses(lake, cfg)[-1] == "failed"


def test_contract_break_publishes_nothing(tmp_path, cfg):
    lake = Lake(tmp_path / "lake")
    broken = pl.DataFrame(clean_trips(5)).drop("fare_amount")
    path = tmp_path / "broken.parquet"
    broken.write_parquet(path)
    with pytest.raises(ContractError):
        run(lake, cfg, path, "2025-01")
    assert not lake.curated_dir(cfg, "2025-01").exists()
    assert "ContractError" in lake.manifest.history(cfg.source)[-1]["error"]


def test_additive_column_recorded(tmp_path, cfg):
    lake = Lake(tmp_path / "lake")
    rows = [{**r, "cbd_congestion_fee": 0.75} for r in clean_trips(5)]
    result = run(lake, cfg, write(tmp_path, "new.parquet", rows), "2025-01")
    assert result.stats.additive_columns == ("cbd_congestion_fee",)
    assert "cbd_congestion_fee" not in lake.read_curated(cfg, "2025-01").columns


def test_unknown_fault_point_rejected(tmp_path, cfg, jan_file):
    with pytest.raises(ValueError):
        run(Lake(tmp_path / "lake"), cfg, jan_file, "2025-01", fault_point="nowhere")


# ---------- CLI ----------

def test_cli_end_to_end(tmp_path, cfg, jan_file, capsys):
    from mdp.ingest import main
    args = ["--contract", CONTRACT, "--file", str(jan_file), "--month", "2025-01", "--lake", str(tmp_path / "lake")]
    assert main(args) == 0
    assert "committed" in capsys.readouterr().out
    assert main(args) == 0
    assert "skipped" in capsys.readouterr().out
