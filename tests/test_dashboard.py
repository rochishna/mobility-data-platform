"""Dashboard tests: the query layer returns correct numbers, and the app builds and renders."""
import polars as pl
import pytest

from factories import CONTRACT, clean_trips, small_batch_contract, trip
from mdp import dashboard, warehouse
from mdp.pipeline import Lake, run


@pytest.fixture
def wh(tmp_path, monkeypatch):
    cfg = small_batch_contract()
    monkeypatch.setattr(warehouse, "load_source_config", lambda _: cfg)
    lake = Lake(tmp_path / "lake")
    rows = clean_trips(10)                                           # 10 trips at $24.00, zone 100, Jan 10
    rows.append(trip(PULocationID=132, total_amount=80.0))           # one JFK trip, Jan 15
    rows.append({**rows[0], "fare_amount": -18.5, "total_amount": -24.0})   # reversal of trip 0
    path = tmp_path / "jan.parquet"
    pl.DataFrame(rows).write_parquet(path)
    run(lake, cfg, path, "2025-01")
    db = tmp_path / "wh.duckdb"
    warehouse.build(lake.root, db, CONTRACT, "configs/reference/taxi_zone_lookup.csv")
    return dashboard.Warehouse(str(db))


def test_kpis_gross_net_and_reversal_share(wh):
    k = wh.kpis()
    assert k["trips"] == 11
    assert k["gross"] == pytest.approx(10 * 24.0 + 80.0)
    assert k["net"] == pytest.approx(10 * 24.0 + 80.0 - 24.0)
    assert k["reversal_share"] == pytest.approx(24.0 / 320.0)


def test_borough_filter(wh):
    assert wh.kpis("Queens")["trips"] == 1                           # JFK is in Queens
    assert wh.kpis("Manhattan")["trips"] == 10                       # zone 100 = Garment District
    assert wh.kpis(dashboard.ALL)["trips"] == 11


def test_heatmap_is_7x24_and_sums_per_day(wh):
    grid = wh.heatmap()
    assert len(grid) == 7 and all(len(row) == 24 for row in grid)
    assert sum(map(sum, grid)) == pytest.approx(11)                  # each of the 2 days appears once


def test_zone_hourly_has_every_hour(wh):
    rows = wh.zone_hourly(132, "2025-01-15")
    assert [r["hr"] for r in rows] == list(range(24))
    assert sum(r["trips"] for r in rows) == 1
    assert sum(r["trips"] for r in wh.zone_hourly(132, "2025-01-20")) == 0


def test_top_zones_ranked_by_net(wh):
    top = wh.top_zones()
    assert [z["zone"] for z in top] == ["Garment District", "JFK Airport"]


def test_figures_build(wh):
    assert len(dashboard.daily_figure(wh.daily()).data) == 2
    assert dashboard.heatmap_figure(wh.heatmap()).data[0].type == "heatmap"
    assert len(dashboard.zone_figure(wh.zone_hourly(132, "2025-01-15")).data) == 2
    assert dashboard.top_zones_figure(wh.top_zones()).data[0].orientation == "h"


def test_app_layout_and_callbacks(wh):
    app = dashboard.create_app(wh.path)
    assert app.layout is not None
    assert len(app.callback_map) == 2          # overview (borough filter) + zone drill-down
