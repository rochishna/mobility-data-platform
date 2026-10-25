"""Profile one month of NYC TLC Yellow Taxi data and report data-quality findings.

Usage:
    python -m mdp.profile data/raw/yellow_tripdata_2025-01.parquet 2025-01 [--out docs/profile_2025-01.md]

Every check is a plain SQL query run by DuckDB directly against the Parquet file
(no loading into memory first). Each check returns a Finding: what we looked for,
how many rows hit it, and what share of the file that is.

The report is EVIDENCE. Decisions about what the pipeline should do with each
problem go in docs/DATA_QUIRKS.md, written by a human after reading this.
"""
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

# Candidate natural key. TLC publishes no trip ID, so we test whether these columns
# together identify a trip. If they produce duplicates, that is itself a finding.
CANDIDATE_KEY = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
]


# Which columns each rule check reads. Used to skip checks on files that lack a column.
_PU, _DO = "tpep_pickup_datetime", "tpep_dropoff_datetime"
COLUMNS_USED = {
    "dropoff_before_pickup": {_PU, _DO},
    "zero_duration": {_PU, _DO},
    "over_24h": {_PU, _DO},
    "negative_fare": {"fare_amount"},
    "negative_total": {"total_amount"},
    "huge_fare": {"fare_amount"},
    "negative_distance": {"trip_distance"},
    "zero_distance": {"trip_distance"},
    "huge_distance": {"trip_distance"},
    "pickup_outside_month": {_PU},
    "zero_passengers": {"passenger_count"},
    "unknown_zone": {"PULocationID", "DOLocationID"},
    "ratecode_99": {"RatecodeID"},
}


@dataclass
class Finding:
    check: str        # short id, e.g. "dropoff_before_pickup"
    question: str     # the human question this answers
    rows: int         # rows affected
    share: float      # rows / total rows
    sql: str          # the exact query, so anyone can reproduce the number


class Profiler:
    def __init__(self, parquet_path: str | Path, month: str):
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            raise ValueError(f"month must look like YYYY-MM, got {month!r}")
        self.path = str(parquet_path)
        self.month = month
        self.con = duckdb.connect()
        # A view lets every query say "FROM trips" instead of repeating the file path.
        self.con.execute(f"CREATE VIEW trips AS SELECT * FROM read_parquet('{self.path}')")
        self.total = self.con.execute("SELECT count(*) FROM trips").fetchone()[0]

    # ---------- Q1: shape and types ----------
    def schema(self) -> list[tuple[str, str]]:
        """(column_name, column_type) for every column."""
        return [(r[0], r[1]) for r in self.con.execute("DESCRIBE trips").fetchall()]

    # ---------- Q2: nulls per column ----------
    def null_counts(self) -> dict[str, int]:
        cols = [name for name, _ in self.schema()]
        # count(col) skips NULLs, so count(*) - count(col) = number of NULLs in col.
        select = ", ".join(f'count(*) - count("{c}") AS "{c}"' for c in cols)
        row = self.con.execute(f"SELECT {select} FROM trips").fetchone()
        return dict(zip(cols, row))

    # ---------- Q3–Q7: rule checks ----------
    def _count(self, check: str, question: str, where: str) -> Finding:
        sql = f"SELECT count(*) FROM trips WHERE {where}"
        n = self.con.execute(sql).fetchone()[0]
        return Finding(check, question, n, n / self.total if self.total else 0.0, sql)

    def rule_findings(self) -> list[Finding]:
        month_start = f"DATE '{self.month}-01'"
        month_end = f"({month_start} + INTERVAL 1 MONTH)"
        checks = [
            # Q3: time travel
            ("dropoff_before_pickup", "Dropoff earlier than pickup?",
             "tpep_dropoff_datetime < tpep_pickup_datetime"),
            ("zero_duration", "Pickup and dropoff at the exact same time?",
             "tpep_dropoff_datetime = tpep_pickup_datetime"),
            ("over_24h", "Trips longer than 24 hours?",
             "tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL 24 HOUR"),
            # Q4: impossible or suspicious money and distance
            ("negative_fare", "Negative fare_amount?", "fare_amount < 0"),
            ("negative_total", "Negative total_amount?", "total_amount < 0"),
            ("huge_fare", "fare_amount above $1,000?", "fare_amount > 1000"),
            ("negative_distance", "Negative trip_distance?", "trip_distance < 0"),
            ("zero_distance", "trip_distance exactly 0?", "trip_distance = 0"),
            ("huge_distance", "trip_distance above 100 miles?", "trip_distance > 100"),
            # Q5: rows that don't belong to the file's month
            ("pickup_outside_month", f"Pickup outside {self.month}?",
             f"tpep_pickup_datetime < {month_start} OR tpep_pickup_datetime >= {month_end}"),
            # Q7: other things worth knowing
            ("zero_passengers", "passenger_count = 0?", "passenger_count = 0"),
            ("unknown_zone", "Pickup or dropoff zone 264/265 (TLC's 'Unknown' / 'Outside NYC')?",
             "PULocationID IN (264, 265) OR DOLocationID IN (264, 265)"),
            ("ratecode_99", "RatecodeID = 99 (not a documented rate code)?", "RatecodeID = 99"),
        ]
        available = {name for name, _ in self.schema()}
        findings = []
        for check, question, where in checks:
            # Skip checks whose columns don't exist in this month's file (TLC schemas drift over time).
            if not COLUMNS_USED[check] <= available:
                continue
            findings.append(self._count(check, question, where))
        return findings

    # ---------- Q6: is there a usable key? ----------
    def key_duplicates(self) -> Finding:
        cols = ", ".join(CANDIDATE_KEY)
        # Rows that share a key with at least one other row, excluding the first copy.
        sql = f"""
            SELECT coalesce(sum(n - 1), 0) FROM (
                SELECT count(*) AS n FROM trips GROUP BY {cols} HAVING count(*) > 1
            )
        """
        n = int(self.con.execute(sql).fetchone()[0])
        return Finding("duplicate_candidate_key", f"Extra rows sharing the key ({cols})?",
                       n, n / self.total if self.total else 0.0, " ".join(sql.split()))

    def exact_duplicates(self) -> Finding:
        sql = "SELECT (SELECT count(*) FROM trips) - (SELECT count(*) FROM (SELECT DISTINCT * FROM trips))"
        n = self.con.execute(sql).fetchone()[0]
        return Finding("exact_duplicate_rows", "Rows that are 100% identical copies of another row?",
                       n, n / self.total if self.total else 0.0, sql)

    # ---------- distributions for context ----------
    def value_counts(self, column: str, limit: int = 10) -> list[tuple]:
        return self.con.execute(
            f'SELECT "{column}", count(*) AS n FROM trips GROUP BY 1 ORDER BY n DESC LIMIT {limit}'
        ).fetchall()

    def pickup_range(self) -> tuple:
        return self.con.execute(
            "SELECT min(tpep_pickup_datetime), max(tpep_pickup_datetime) FROM trips"
        ).fetchone()

    # ---------- report ----------
    def report(self) -> str:
        schema = self.schema()
        nulls = self.null_counts()
        findings = self.rule_findings() + [self.key_duplicates(), self.exact_duplicates()]
        lo, hi = self.pickup_range()

        out = [f"# Profile: `{Path(self.path).name}` (expected month {self.month})", ""]
        out += [f"- **Rows:** {self.total:,}", f"- **Columns:** {len(schema)}",
                f"- **Pickup range:** {lo} → {hi}", ""]

        out += ["## Schema and nulls", "", "| Column | Type | Nulls | Null % |", "|---|---|---:|---:|"]
        for name, typ in schema:
            n = nulls[name]
            out.append(f"| {name} | {typ} | {n:,} | {n / self.total:.2%} |" if self.total else f"| {name} | {typ} | 0 | - |")

        out += ["", "## Rule checks", "", "| Check | Question | Rows | Share |", "|---|---|---:|---:|"]
        for f in findings:
            out.append(f"| `{f.check}` | {f.question} | {f.rows:,} | {f.share:.3%} |")

        for col in ("payment_type", "RatecodeID", "passenger_count", "VendorID"):
            if col in dict(schema):
                out += ["", f"## Top values: `{col}`", "", "| Value | Rows |", "|---|---:|"]
                out += [f"| {v} | {n:,} |" for v, n in self.value_counts(col)]

        out += ["", "## Queries used", ""]
        out += [f"- `{f.check}`: `{f.sql}`" for f in findings]
        return "\n".join(out) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("parquet")
    p.add_argument("month", help="Month the file claims to cover, YYYY-MM")
    p.add_argument("--out", help="Write the markdown report here instead of printing it")
    args = p.parse_args()

    text = Profiler(args.parquet, args.month).report()
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
        print(f"Wrote {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
