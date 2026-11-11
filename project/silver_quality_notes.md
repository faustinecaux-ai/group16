# Silver — Quality Decisions and Verified Results

## Run evidence

Run: `20261110T150552746288Z` (10 November 2026), Spark 3.5.0. Observed execution time: 59.49 seconds. Full report: [versioned Silver JSON report](reports/silver_quality.json).

Generated datasets remain local and are excluded from Git. The small JSON quality report is versioned as assessment evidence; this note explains its measurements. It describes this specific run and is not regenerated automatically; every Silver run creates its own JSON report.

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

`passed` means the implemented checks succeeded. It does not establish that every possible issue has been investigated or that the source is exhaustive. Missing coordinates are retained for analyses that do not depend on location. The saved suite records 17 successful tests, including age handling, relationship-safe enrichment, annual denominators and failed-update publication. The observed duration is not evidence of a performance improvement.

## Scope of validation

Three speed limits are coded missing; no unsupported speed values were found.
Pooled and annual Gold summaries reconcile to Silver. The end-to-end fixture test
also verifies that an inconsistent update blocks Gold and preserves previous
validated Silver and Gold pointers. The pipeline manifest is the reference for
consistent end-to-end consumption because pointers are published per layer.

Source retrieval dates remain unknown. Numeric schemas are inferred, the guide
reader depends on its selected workbook layout, and global coordinate ranges do
not prove geographic accuracy. These are documented limits of the current
snapshot, rather than additional claims of validation.
