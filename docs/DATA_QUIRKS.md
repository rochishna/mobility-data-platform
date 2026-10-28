# Data Quirks: NYC TLC Yellow Taxi

What's wrong with the raw data, the evidence, and what the pipeline does about it.

**Month profiled:** 2025-01 · **Rows:** 3,475,226 · **Columns:** 19 · **Full report:** [`profile_2025-01.md`](profile_2025-01.md)
Code meanings come from the [TLC yellow taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) (March 18, 2025).

Decisions:
- **Reject**: quarantined with a reason; never reaches curated data
- **Flag**: a real record, kept, with a boolean column so analysts can filter
- **Keep**: normal, documented, no action

## The headline finding

The obvious move, rejecting negative fares and deduplicating on a trip key, would have
**silently corrupted revenue totals**. The negative rows are mostly reversals and adjustments, and the
"duplicates" were charge/reversal pairs. Reject rules ended up touching only **167 rows (0.005%)**.

## Findings and decisions

| # | Quirk | Rows | Share | Evidence / likely cause | Decision |
|---|---|---:|---:|---|---|
| 1 | Negative fare | 144,118 | 4.15% | ~52K have a positive twin (same vendor, times, zones, opposite fare): reversals. 84,822 are Flex Fare rows (avg −$4.57); the rest are disputes, cash, no-charge | **Flag** `is_reversal` |
| 2 | Negative total | 63,037 | 1.81% | Same population as #1 | **Flag** `is_reversal` |
| 3 | Key "duplicates" (vendor, times, zones, fare) | 301 | 0.009% | 294 of 299 groups net to $0: +$2.50 / −$2.50 pairs. Zero exact duplicate rows | **Key fixed**: added `total_amount`. 1 true conflict remains (same trip re-sent with different passenger count and distance) → **Reject** as `duplicate_key` |
| 4 | Five columns null together (passenger_count, RatecodeID, store_and_fwd_flag, congestion_surcharge, Airport_fee) | 540,149 | 15.54% | Exactly the rows with `payment_type = 0`, which the dictionary defines as **Flex Fare** (upfront-priced) trips | **Flag** `is_flex_fare`; nulls allowed |
| 5 | Distance > 100 mi | 162 | 0.005% | Up to 276,423 mi, mostly Flex Fare rows with normal fares (avg $20.89): meter errors, not long trips | **Flag** `suspect_distance` (> 200 mi or < 0); fare kept |
| 6 | Zero distance | 90,893 | 2.62% | Cancellations, no-move trips, meter resets | **Flag** `zero_distance` |
| 7 | Dropoff before pickup | 124 | 0.004% | Clock or device errors; duration and partition can't be trusted | **Reject** |
| 8 | Trip over 24 hours | 20 | 0.001% | Stuck meters | **Reject** |
| 9 | Pickup outside January | 22 | 0.001% | 21 from Dec 31, 20:47–23:56, and one at Feb 1 00:00:44: late-arriving records from adjacent months | **Reject** from this month (`pickup_in_logical_month`). They belong to another partition, not to the trash |
| 10 | Zero-duration trips | 1,927 | 0.06% | Same population as zero distance | **Keep** |
| 11 | passenger_count = 0 | 24,656 | 0.71% | Driver didn't enter a count | **Keep** |
| 12 | Zones 264/265 | 28,357 | 0.82% | Dictionary: "Unknown" and "Outside of NYC" | **Flag** `unknown_zone` |
| 13 | RatecodeID = 99 | 41,963 | 1.21% | Dictionary: "Null/unknown" | **Keep** |
| 14 | Vendors 6 and 7 | 1,695 | 0.05% | New providers (Myle, Helix), present in the 2025 dictionary | **Keep** |
| 15 | Fare > $1,000 | 3 | 0.0001% | Too few to rule on | **Keep** |
| 16 | `cbd_congestion_fee` in dictionary, absent from this file | — | — | Congestion pricing began Jan 5, 2025. Schema will drift when it appears | Contract treats it as **additive**: accepted and recorded, not loaded |

## Gates

Reject rules quarantined 167 rows in January (0.005%). The batch gate is set at **0.1%**, 20× that,
so a normal month passes, while a source change that breaks a real share of rows fails loudly and
publishes nothing. `min_rows: 1000` stops a truncated download from replacing a month.

## Open questions

- Are unpaired negative Flex Fare rows adjustments to trips in another month? Needs February data.
- Should late-arriving rows (#9) be routed to their correct month instead of quarantined?
