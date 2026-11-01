"""Human review of trips held by the suspect_fare flag.

Workflow:
  1. python -m mdp.review --export review_todo.csv
       writes every pending trip, with empty decision / reviewer / note columns
  2. open review_todo.csv, set decision to 'approved' or 'rejected' (leave blank to keep it pending),
     fill in reviewer and a short note
  3. python -m mdp.review --apply review_todo.csv
       appends the decided rows to configs/reviews/tlc_yellow.csv (the reviewed record, kept in git)
  4. python -m mdp.warehouse
       rebuilds the rollup: approved trips now count, rejected ones stay excluded

Run with no flags to just list what is waiting.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path

import duckdb

from .warehouse import REVIEW_KEY

FIELDS = REVIEW_KEY + ["decision", "reviewer", "reviewed_on", "note"]
CONTEXT = ["trip_distance", "duration_min", "ratecode_id", "payment_type", "fare_per_minute"]


def pending(db: str) -> list[dict]:
    with duckdb.connect(db, read_only=True) as con:
        cur = con.execute("SELECT * FROM review_queue")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def export(db: str, out: str | Path) -> int:
    rows = pending(db)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS + CONTEXT)
        w.writeheader()
        for r in rows:
            w.writerow({**{k: r[k] for k in REVIEW_KEY + CONTEXT},
                        "decision": "", "reviewer": "", "reviewed_on": "", "note": ""})
    return len(rows)


def apply(todo: str | Path, reviews: str | Path) -> tuple[int, int]:
    """Append decided rows to the review record. Returns (applied, still_pending)."""
    applied, still_pending, out = 0, 0, []
    with open(todo, newline="") as f:
        for r in csv.DictReader(f):
            decision = (r.get("decision") or "").strip().lower()
            if not decision:
                still_pending += 1
                continue
            if decision not in ("approved", "rejected"):
                raise ValueError(f"decision must be 'approved' or 'rejected', got {r['decision']!r}")
            if not (r.get("reviewer") or "").strip():
                raise ValueError("every decision needs a reviewer name")
            out.append({**{k: r[k] for k in REVIEW_KEY}, "decision": decision, "reviewer": r["reviewer"].strip(),
                        "reviewed_on": r.get("reviewed_on") or date.today().isoformat(), "note": r.get("note", "")})
            applied += 1
    new_file = not Path(reviews).exists()
    with open(reviews, "a", newline="") as f:
        # "\n", not csv's default "\r\n": appending CRLF rows to an LF file produced mixed line
        # endings the warehouse's CSV reader could not parse (found applying the first real review).
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        if new_file:
            w.writeheader()
        w.writerows(out)
    return applied, still_pending


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Review trips held by the suspect_fare flag.")
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--reviews", default="configs/reviews/tlc_yellow.csv")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--export", metavar="TODO_CSV")
    g.add_argument("--apply", metavar="TODO_CSV")
    args = p.parse_args(argv)

    if args.export:
        print(f"wrote {export(args.db, args.export)} pending trips to {args.export}")
    elif args.apply:
        done, left = apply(args.apply, args.reviews)
        print(f"recorded {done} decisions in {args.reviews}; {left} left pending. Now run: python -m mdp.warehouse")
    else:
        rows = pending(args.db)
        print(f"{len(rows)} trips waiting for review")
        for r in rows:
            print(f"  {r['pickup_datetime']}  zone {r['pu_location_id']:>3}  ${r['total_amount']:>12,.2f}  "
                  f"{r['trip_distance']:>6.2f} mi  {r['duration_min']:>6.2f} min  ${r['fare_per_minute']:>10,.2f}/min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
