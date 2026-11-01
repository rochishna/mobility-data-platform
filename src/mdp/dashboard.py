"""NYC taxi analytics dashboard (Plotly Dash) on top of the warehouse.

Every tile reads the pre-aggregated rollup (agg_daily_zone_hour), so each interaction is a
few-millisecond query; see docs/QUERY_TUNING.md.

Revenue is shown two ways (decision: Rochishna):
  * gross revenue: all charges (sum of positive totals)
  * net revenue: what was actually collected after reversals and refunds
Neither is profit: the data has no costs.

Usage:
    python -m mdp.dashboard --db warehouse.duckdb      # then open http://127.0.0.1:8050
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date

import duckdb
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
ALL = "All boroughs"

# Colors: one hue per meaning, used consistently across tiles.
GROSS, NET, TRIPS, INK, MUTED, GRID = "#9db4d6", "#1f4e8c", "#d98c2b", "#1d2433", "#6b7280", "#e5e7eb"


# ---------- data access (pure functions: easy to test, no Dash involved) ----------

class Warehouse:
    def __init__(self, path: str):
        self.path = path

    @contextmanager
    def _con(self):
        # A fresh read-only connection per query: Dash serves callbacks from several threads.
        con = duckdb.connect(self.path, read_only=True)
        try:
            yield con
        finally:
            con.close()

    def _rows(self, sql: str, params: list | None = None) -> list[dict]:
        with self._con() as con:
            cur = con.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def _borough_filter(borough: str | None) -> tuple[str, list]:
        if not borough or borough == ALL:
            return "", []
        return "AND l.borough = ?", [borough]

    def boroughs(self) -> list[str]:
        rows = self._rows("""SELECT DISTINCT l.borough FROM agg_daily_zone_hour a
                             JOIN dim_location l ON l.location_id = a.pu_location_id
                             WHERE l.borough NOT IN ('Unknown', 'N/A') ORDER BY 1""")
        return [r["borough"] for r in rows]

    def zones(self) -> list[dict]:
        """Zones with trips, busiest first, labelled 'Zone (Borough)'."""
        return self._rows("""
            SELECT l.location_id AS value, l.zone || ' (' || l.borough || ')' AS label
            FROM agg_daily_zone_hour a JOIN dim_location l ON l.location_id = a.pu_location_id
            GROUP BY ALL ORDER BY sum(a.trips) DESC""")

    def date_range(self) -> tuple[date, date]:
        r = self._rows("""SELECT min(d.date) AS lo, max(d.date) AS hi FROM agg_daily_zone_hour a
                          JOIN dim_date d ON d.date_key = a.pickup_date_key""")[0]
        return r["lo"], r["hi"]

    def kpis(self, borough: str | None = None) -> dict:
        where, params = self._borough_filter(borough)
        r = self._rows(f"""
            SELECT sum(a.trips) AS trips, sum(a.gross_revenue) AS gross, sum(a.net_revenue) AS net,
                   sum(a.total_distance) / nullif(sum(a.trips), 0) AS avg_miles
            FROM agg_daily_zone_hour a JOIN dim_location l ON l.location_id = a.pu_location_id
            WHERE TRUE {where}""", params)[0]
        r["reversal_share"] = (r["gross"] - r["net"]) / r["gross"] if r["gross"] else 0.0
        return r

    def daily(self, borough: str | None = None) -> list[dict]:
        where, params = self._borough_filter(borough)
        return self._rows(f"""
            SELECT d.date AS day, sum(a.trips) AS trips, sum(a.gross_revenue) AS gross, sum(a.net_revenue) AS net
            FROM agg_daily_zone_hour a
            JOIN dim_date d ON d.date_key = a.pickup_date_key
            JOIN dim_location l ON l.location_id = a.pu_location_id
            WHERE TRUE {where} GROUP BY 1 ORDER BY 1""", params)

    def heatmap(self, borough: str | None = None) -> list[list[float]]:
        """7 x 24 grid: average trips per day for each weekday (rows) and hour (columns)."""
        where, params = self._borough_filter(borough)
        rows = self._rows(f"""
            WITH per_day AS (
                SELECT d.day_of_week AS dow, a.pickup_date_key, a.pickup_hour AS hr, sum(a.trips) AS trips
                FROM agg_daily_zone_hour a
                JOIN dim_date d ON d.date_key = a.pickup_date_key
                JOIN dim_location l ON l.location_id = a.pu_location_id
                WHERE TRUE {where} GROUP BY ALL)
            SELECT dow, hr, avg(trips) AS avg_trips FROM per_day GROUP BY ALL""", params)
        grid = [[0.0] * 24 for _ in range(7)]
        for r in rows:
            grid[r["dow"] - 1][r["hr"]] = float(r["avg_trips"])
        return grid

    def zone_hourly(self, zone_id: int, day: str) -> list[dict]:
        """24 rows (one per hour) for one pickup zone on one day; empty hours are zero."""
        return self._rows("""
            SELECT h.hr, coalesce(sum(a.trips), 0) AS trips, coalesce(sum(a.net_revenue), 0) AS net
            FROM range(24) h(hr)
            LEFT JOIN agg_daily_zone_hour a
              ON a.pickup_hour = h.hr AND a.pu_location_id = ?
             AND a.pickup_date_key = CAST(strftime(CAST(? AS DATE), '%Y%m%d') AS INTEGER)
            GROUP BY 1 ORDER BY 1""", [zone_id, day])

    def top_zones(self, borough: str | None = None, n: int = 10) -> list[dict]:
        where, params = self._borough_filter(borough)
        return self._rows(f"""
            SELECT l.zone, sum(a.trips) AS trips, sum(a.net_revenue) AS net,
                   sum(a.net_revenue) / nullif(sum(a.trips), 0) AS net_per_trip
            FROM agg_daily_zone_hour a JOIN dim_location l ON l.location_id = a.pu_location_id
            WHERE l.borough NOT IN ('Unknown', 'N/A') {where}
            GROUP BY 1 ORDER BY net DESC LIMIT {int(n)}""", params)


# ---------- figures ----------

def _style(fig: go.Figure, height: int = 320) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=48, r=16, t=8, b=36), plot_bgcolor="white", paper_bgcolor="white",
        font=dict(family="Inter, system-ui, sans-serif", size=12, color=INK),
        legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"), hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


def daily_figure(rows: list[dict]) -> go.Figure:
    days = [r["day"] for r in rows]
    fig = go.Figure()
    # Net first, then gross filled down to it: the shaded band IS the money reversed or refunded.
    fig.add_trace(go.Scatter(x=days, y=[r["net"] for r in rows], name="Net revenue",
                             line=dict(color=NET, width=2.5), hovertemplate="$%{y:,.0f}"))
    fig.add_trace(go.Scatter(x=days, y=[r["gross"] for r in rows], name="Gross revenue (band = reversed/refunded)",
                             line=dict(color=GROSS, width=1.5), fill="tonexty", fillcolor="rgba(217,140,43,0.35)",
                             hovertemplate="$%{y:,.0f}"))
    fig.update_yaxes(tickprefix="$", tickformat=".2s")
    return _style(fig)


def heatmap_figure(grid: list[list[float]]) -> go.Figure:
    fig = go.Figure(go.Heatmap(
        z=grid, x=[f"{h:02d}" for h in range(24)], y=DAYS, colorscale="Blues",
        hovertemplate="%{y} %{x}:00<br>%{z:,.0f} trips/day<extra></extra>",
        colorbar=dict(title=dict(text="trips/day", side="right"), thickness=10),
    ))
    fig.update_yaxes(autorange="reversed", showgrid=False)
    fig.update_xaxes(tickangle=0, dtick=3)
    fig = _style(fig, 300)
    fig.update_layout(hovermode="closest")
    return fig


def zone_figure(rows: list[dict]) -> go.Figure:
    hrs = [f"{r['hr']:02d}" for r in rows]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=hrs, y=[r["trips"] for r in rows], name="Trips", marker_color=TRIPS,
                         hovertemplate="%{y:,} trips"))
    fig.add_trace(go.Scatter(x=hrs, y=[r["net"] for r in rows], name="Net revenue", yaxis="y2",
                             line=dict(color=NET, width=2.5), hovertemplate="$%{y:,.0f}"))
    fig.update_layout(yaxis2=dict(overlaying="y", side="right", tickprefix="$", tickformat=".2s", showgrid=False,
                                  rangemode="tozero"))
    fig.update_yaxes(rangemode="tozero")
    return _style(fig, 300)


def top_zones_figure(rows: list[dict]) -> go.Figure:
    rows = list(reversed(rows))   # largest at top
    fig = go.Figure(go.Bar(
        y=[r["zone"] for r in rows], x=[r["net"] for r in rows], orientation="h", marker_color=NET,
        customdata=[[r["trips"], r["net_per_trip"]] for r in rows],
        hovertemplate="%{y}<br>$%{x:,.0f} net · %{customdata[0]:,} trips · $%{customdata[1]:.2f}/trip<extra></extra>",
        text=[f"${r['net_per_trip']:.0f}/trip" for r in rows], textposition="auto", insidetextanchor="end",
    ))
    fig.update_xaxes(tickprefix="$", tickformat="~s")
    fig = _style(fig, 340)
    fig.update_layout(hovermode="closest", margin=dict(l=170, r=16, t=8, b=36))
    return fig


# ---------- layout ----------

def _card(title: str, value_id: str, note_id: str | None = None) -> html.Div:
    children = [html.Div(title, className="kpi-title"), html.Div(id=value_id, className="kpi-value")]
    if note_id:
        children.append(html.Div(id=note_id, className="kpi-note"))
    return html.Div(children, className="kpi")


def _panel(title: str, subtitle: str, *body) -> html.Div:
    return html.Div([html.H3(title), html.P(subtitle, className="sub"), *body], className="panel")


CSS = """
body { margin: 0; background: #f6f7f9; font-family: Inter, system-ui, sans-serif; color: #1d2433; }
.wrap { max-width: 1180px; margin: 0 auto; padding: 24px 16px 48px; }
header { display: flex; justify-content: space-between; align-items: flex-end; gap: 16px; flex-wrap: wrap; margin-bottom: 16px; }
h1 { font-size: 22px; margin: 0; } .lede { color: #6b7280; margin: 4px 0 0; font-size: 13px; }
.filter { min-width: 220px; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-bottom: 12px; }
.kpi, .panel { background: white; border: 1px solid #e5e7eb; border-radius: 10px; }
.kpi { padding: 14px 16px; } .kpi-title { font-size: 12px; color: #6b7280; }
.kpi-value { font-size: 24px; font-weight: 600; margin-top: 4px; font-variant-numeric: tabular-nums; }
.kpi-note { font-size: 12px; color: #6b7280; margin-top: 2px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 860px) { .grid { grid-template-columns: 1fr; } }
.panel { padding: 14px 16px 6px; } .panel h3 { margin: 0; font-size: 15px; }
.sub { margin: 2px 0 8px; color: #6b7280; font-size: 12px; }
.controls { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 4px; } .controls > * { flex: 1 1 200px; }
footer { color: #6b7280; font-size: 12px; margin-top: 16px; }
"""


def create_app(db_path: str) -> Dash:
    wh = Warehouse(db_path)
    lo, hi = wh.date_range()
    zones = wh.zones()
    jfk = next((z["value"] for z in zones if z["value"] == 132), zones[0]["value"])

    app = Dash(__name__, title="NYC Taxi Analytics")
    app.index_string = app.index_string.replace("{%css%}", "{%css%}<style>" + CSS + "</style>")
    app.layout = html.Div(className="wrap", children=[
        html.Header([
            html.Div([html.H1("NYC Yellow Taxi Analytics"),
                      html.P(f"Trips picked up {lo:%b %d} – {hi:%b %d, %Y}. Source: NYC TLC trip records.",
                             className="lede")]),
            dcc.Dropdown(id="borough", options=[ALL] + wh.boroughs(), value=ALL, clearable=False, className="filter"),
        ]),
        html.Div(className="kpis", children=[
            _card("Trips", "kpi-trips"),
            _card("Gross revenue", "kpi-gross", "kpi-gross-note"),
            _card("Net revenue", "kpi-net", "kpi-net-note"),
            _card("Avg trip distance", "kpi-miles"),
        ]),
        html.Div(className="grid", children=[
            _panel("Daily revenue", "Gross = all charges. Net = after reversals and refunds. Neither is profit.",
                   dcc.Graph(id="daily", config={"displayModeBar": False})),
            _panel("When demand peaks", "Average trips per day, by weekday and pickup hour",
                   dcc.Graph(id="heatmap", config={"displayModeBar": False})),
            _panel("Top zones by net revenue", "Bar label = net revenue per trip",
                   dcc.Graph(id="topzones", config={"displayModeBar": False})),
            _panel("Zone drill-down", "Trips and net revenue by hour for one pickup zone on one day",
                   html.Div(className="controls", children=[
                       dcc.Dropdown(id="zone", options=zones, value=jfk, clearable=False),
                       dcc.DatePickerSingle(id="day", min_date_allowed=lo, max_date_allowed=hi, date=lo,
                                            display_format="MMM D, YYYY"),
                   ]),
                   dcc.Graph(id="zonehourly", config={"displayModeBar": False})),
        ]),
        html.Footer("Reversals: negative charges that cancel or refund an earlier charge (see docs/DATA_QUIRKS.md). "
                    "Distance excludes meter readings flagged as implausible."),
    ])

    @app.callback(
        Output("kpi-trips", "children"), Output("kpi-gross", "children"), Output("kpi-gross-note", "children"),
        Output("kpi-net", "children"), Output("kpi-net-note", "children"), Output("kpi-miles", "children"),
        Output("daily", "figure"), Output("heatmap", "figure"), Output("topzones", "figure"),
        Input("borough", "value"),
    )
    def update_overview(borough):
        k = wh.kpis(borough)
        return (
            f"{k['trips']:,}", f"${k['gross'] / 1e6:,.2f}M", "all charges",
            f"${k['net'] / 1e6:,.2f}M", f"{k['reversal_share']:.1%} reversed or refunded",
            f"{k['avg_miles']:.2f} mi",
            daily_figure(wh.daily(borough)), heatmap_figure(wh.heatmap(borough)),
            top_zones_figure(wh.top_zones(borough)),
        )

    @app.callback(Output("zonehourly", "figure"), Input("zone", "value"), Input("day", "date"))
    def update_zone(zone_id, day):
        return zone_figure(wh.zone_hourly(int(zone_id), str(day)[:10]))

    return app


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Run the NYC taxi analytics dashboard.")
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8050)
    p.add_argument("--debug", action="store_true")
    args = p.parse_args(argv)
    create_app(args.db).run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
