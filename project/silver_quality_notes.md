# Silver — Quality Decisions and Verified Results

## Run evidence

Run: `20261110T080247399254Z` (10 November 2026), Spark 3.5.0. Observed execution time: 58.93 seconds. Full report: `data/silver/runs/20261110T080247399254Z/quality_report.json`.

Generated outputs remain local and are excluded from Git. This note preserves a summary of the measurements and their interpretation. It describes this specific run and is not regenerated automatically; every Silver run creates its own JSON report.

## Missing-age handling

The local official guide's `2024_code_list` defines `-1` for `age_of_driver` and `age_of_casualty` as "Data missing or out of range". It is not a numerical age. Including this code in an average would distort the result. Rejecting the entire vehicle or casualty would discard other potentially useful information.

The source columns are therefore retained alongside `age_of_driver_clean` and `age_of_casualty_clean`. Negative values become `null` in the prepared columns. Age aggregates must use these columns and disclose their non-missing denominator: an average of recorded ages is not necessarily representative of all records.

For example, source values `-1, 20, 40` become `null, 20, 40`. The recorded-age average is 30 years, based on two of the three records. All three records remain.

| Measure | Vehicles | Casualties |
|---|---:|---:|
| Input rows | 937,265 | 652,821 |
| Source age code `-1` | 142,240 | 14,106 |
| Source age nulls | 0 | 0 |
| Prepared age nulls | 142,240 | 14,106 |
| Other negative ages | 0 | 0 |
| Recorded age minimum–maximum | 1–105 | 0–105 |
| Output rows | 937,265 | 652,821 |

Zero remains a valid casualty age. No arbitrary upper limit was applied. The observed extremes alone do not establish that every recorded age is correct. A negative value other than `-1` would require review and prevent publication.

## Other checks confirmed for this run

- 513,801 collision input and output rows; no rows deleted from any source table.
- No missing required keys or duplicate keys at the defined grains.
- No failed date/time conversions or discrepancies with source year or weekday.
- 53 missing longitudes and 53 missing latitudes. No recorded coordinates outside global ranges; precise geographic accuracy has not been established.
- No vehicle or casualty without a matching collision; no casualty without a matching vehicle.
- No discrepancies between observed vehicle/casualty counts and collision counters.
- No unrecognised codes in the seven categorical fields being decoded.
- Parquet row counts match after reloading the outputs.

`passed` means the implemented checks succeeded. It does not establish that every possible issue has been investigated or that the source is exhaustive. Missing coordinates are retained for analyses that do not depend on location. Seven small Spark tests passed, including age handling and missing-value checks. The observed duration is not evidence of a performance improvement.

## Further validation

Review speed-limit codes, complete input provenance, strengthen tests for aggregation and publication behaviour, and inspect execution plans before building Gold. The CI workflow is configured to run the small tests once its files are pushed to GitHub; it does not process the large CSVs on each change.
