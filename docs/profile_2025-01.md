# Profile: `yellow_tripdata_2025-01.parquet` (expected month 2025-01)

- **Rows:** 3,475,226
- **Columns:** 19
- **Pickup range:** 2024-12-31 20:47:55 → 2025-02-01 00:00:44

## Schema and nulls

| Column | Type | Nulls | Null % |
|---|---|---:|---:|
| VendorID | INTEGER | 0 | 0.00% |
| tpep_pickup_datetime | TIMESTAMP | 0 | 0.00% |
| tpep_dropoff_datetime | TIMESTAMP | 0 | 0.00% |
| passenger_count | BIGINT | 540,149 | 15.54% |
| trip_distance | DOUBLE | 0 | 0.00% |
| RatecodeID | BIGINT | 540,149 | 15.54% |
| store_and_fwd_flag | VARCHAR | 540,149 | 15.54% |
| PULocationID | INTEGER | 0 | 0.00% |
| DOLocationID | INTEGER | 0 | 0.00% |
| payment_type | BIGINT | 0 | 0.00% |
| fare_amount | DOUBLE | 0 | 0.00% |
| extra | DOUBLE | 0 | 0.00% |
| mta_tax | DOUBLE | 0 | 0.00% |
| tip_amount | DOUBLE | 0 | 0.00% |
| tolls_amount | DOUBLE | 0 | 0.00% |
| improvement_surcharge | DOUBLE | 0 | 0.00% |
| total_amount | DOUBLE | 0 | 0.00% |
| congestion_surcharge | DOUBLE | 540,149 | 15.54% |
| Airport_fee | DOUBLE | 540,149 | 15.54% |

## Rule checks

| Check | Question | Rows | Share |
|---|---|---:|---:|
| `dropoff_before_pickup` | Dropoff earlier than pickup? | 124 | 0.004% |
| `zero_duration` | Pickup and dropoff at the exact same time? | 1,927 | 0.055% |
| `over_24h` | Trips longer than 24 hours? | 20 | 0.001% |
| `negative_fare` | Negative fare_amount? | 144,118 | 4.147% |
| `negative_total` | Negative total_amount? | 63,037 | 1.814% |
| `huge_fare` | fare_amount above $1,000? | 3 | 0.000% |
| `negative_distance` | Negative trip_distance? | 0 | 0.000% |
| `zero_distance` | trip_distance exactly 0? | 90,893 | 2.615% |
| `huge_distance` | trip_distance above 100 miles? | 162 | 0.005% |
| `pickup_outside_month` | Pickup outside 2025-01? | 22 | 0.001% |
| `zero_passengers` | passenger_count = 0? | 24,656 | 0.709% |
| `unknown_zone` | Pickup or dropoff zone 264/265 (TLC's 'Unknown' / 'Outside NYC')? | 28,357 | 0.816% |
| `ratecode_99` | RatecodeID = 99 (not a documented rate code)? | 41,963 | 1.207% |
| `duplicate_candidate_key` | Extra rows sharing the key (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount)? | 301 | 0.009% |
| `exact_duplicate_rows` | Rows that are 100% identical copies of another row? | 0 | 0.000% |

## Top values: `payment_type`

| Value | Rows |
|---|---:|
| 1 | 2,444,393 |
| 0 | 540,149 |
| 2 | 390,429 |
| 4 | 76,481 |
| 3 | 23,773 |
| 5 | 1 |

## Top values: `RatecodeID`

| Value | Rows |
|---|---:|
| 1 | 2,756,472 |
| None | 540,149 |
| 2 | 94,420 |
| 99 | 41,963 |
| 5 | 26,501 |
| 3 | 8,622 |
| 4 | 7,092 |
| 6 | 7 |

## Top values: `passenger_count`

| Value | Rows |
|---|---:|
| 1 | 2,322,434 |
| None | 540,149 |
| 2 | 407,761 |
| 3 | 91,409 |
| 4 | 59,009 |
| 0 | 24,656 |
| 5 | 17,786 |
| 6 | 12,004 |
| 8 | 11 |
| 7 | 4 |

## Top values: `VendorID`

| Value | Rows |
|---|---:|
| 2 | 2,719,860 |
| 1 | 753,671 |
| 7 | 1,206 |
| 6 | 489 |

## Queries used

- `dropoff_before_pickup`: `SELECT count(*) FROM trips WHERE tpep_dropoff_datetime < tpep_pickup_datetime`
- `zero_duration`: `SELECT count(*) FROM trips WHERE tpep_dropoff_datetime = tpep_pickup_datetime`
- `over_24h`: `SELECT count(*) FROM trips WHERE tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL 24 HOUR`
- `negative_fare`: `SELECT count(*) FROM trips WHERE fare_amount < 0`
- `negative_total`: `SELECT count(*) FROM trips WHERE total_amount < 0`
- `huge_fare`: `SELECT count(*) FROM trips WHERE fare_amount > 1000`
- `negative_distance`: `SELECT count(*) FROM trips WHERE trip_distance < 0`
- `zero_distance`: `SELECT count(*) FROM trips WHERE trip_distance = 0`
- `huge_distance`: `SELECT count(*) FROM trips WHERE trip_distance > 100`
- `pickup_outside_month`: `SELECT count(*) FROM trips WHERE tpep_pickup_datetime < DATE '2025-01-01' OR tpep_pickup_datetime >= (DATE '2025-01-01' + INTERVAL 1 MONTH)`
- `zero_passengers`: `SELECT count(*) FROM trips WHERE passenger_count = 0`
- `unknown_zone`: `SELECT count(*) FROM trips WHERE PULocationID IN (264, 265) OR DOLocationID IN (264, 265)`
- `ratecode_99`: `SELECT count(*) FROM trips WHERE RatecodeID = 99`
- `duplicate_candidate_key`: `SELECT coalesce(sum(n - 1), 0) FROM ( SELECT count(*) AS n FROM trips GROUP BY VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount HAVING count(*) > 1 )`
- `exact_duplicate_rows`: `SELECT (SELECT count(*) FROM trips) - (SELECT count(*) FROM (SELECT DISTINCT * FROM trips))`
