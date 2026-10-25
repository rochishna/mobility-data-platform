# Data Quirks: NYC TLC Yellow Taxi

What's wrong with the raw data, the evidence for it, and what the pipeline does about it.
Numbers come from `python -m mdp.profile` and are reproducible from the queries listed in each profile report.

**Month profiled:** _YYYY-MM_ · **Rows:** _N_ · **Report:** `docs/profile_YYYY-MM.md`

Decision options:
- **Reject** → row goes to quarantine with a reason, never reaches curated tables
- **Fix** → corrected in a documented, deterministic way (and flagged)
- **Keep + flag** → plausible but unusual; kept, with a boolean flag column for analysts
- **Keep** → normal, no action

| # | Quirk | Rows | Share | Why it happens (best guess) | Decision |
|---|---|---:|---:|---|---|
| 1 | Dropoff before pickup | | | | |
| 2 | Zero-duration trips | | | | |
| 3 | Trips over 24 hours | | | | |
| 4 | Negative fare / total | | | | |
| 5 | Fare above $1,000 | | | | |
| 6 | Zero distance | | | | |
| 7 | Distance above 100 mi | | | | |
| 8 | Pickup outside the file's month | | | | |
| 9 | passenger_count = 0 | | | | |
| 10 | Nulls in passenger_count / RatecodeID / ... | | | | |
| 11 | Unknown zones (264/265) | | | | |
| 12 | RatecodeID = 99 | | | | |
| 13 | No trip ID; candidate-key duplicates | | | | |
| 14 | Exact duplicate rows | | | | |
| 15 | _Anything else you noticed_ | | | | |

## Notes

<!-- For anything that needs more than a table cell: e.g. "negative fares look like
refunds: they come paired with a positive fare for the same trip — checked N cases". -->
