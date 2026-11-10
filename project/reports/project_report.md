# DAT535 — Great Britain Road Collision Pipeline

## Engineering report

**Evidence run:** `20261110T150552746288Z`. **Scope:** local Bronze–Silver–Gold implementation, tests and analysis. GitHub deployment and schedule configuration are prepared; a remote execution and repository protection settings have not been verified. This report is a technical draft for review, not a claim that those external steps are complete.

## 1. Problem, consumer and scope

The project addresses this question: **Which lighting, road-type and speed-limit profiles account for the largest numbers of fatal or serious reported collisions in Great Britain, 2021–2025?** The intended consumer is a road safety analyst identifying groups for closer local investigation. Counts identify recorded caseload, while percentages put each group in context; neither metric establishes exposure-adjusted risk or selects an intervention. Silver preserves reusable detail; Gold serves specific analytical questions through CSV/Parquet tables and an executed notebook.

The snapshot contains 513,801 collisions, 937,265 vehicle records and 652,821 casualty records across 2021–2025. It describes police-reported personal injury collisions, not all road accidents. The analysis is descriptive. It does not establish causality or estimate collision risk per distance travelled, because traffic exposure is absent.

The [DfT source page](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data) describes publication frequency, revisions and reference files. Input byte identities are recorded below. Original retrieval dates were not recorded and cannot be reconstructed from validation timestamps. Mutable last-five-years download links require an externally retained snapshot for exact reproduction. Attribution and source links are documented in `data/bronze/stats19/metadata/source_catalog.json`.

## 2. Bronze: preservation and traceability

Bronze retains the three CSVs unchanged and separates them from the official reference guide. The validation script checks file presence, non-zero size, essential headers, duplicate header names and SHA-256 hashes. It records source headers and file change status in a run directory. Hashes detect file changes; they do not deduplicate records. Initial versioned metadata is retained rather than overwritten on each run.

| Dataset | Bytes | SHA-256 |
|---|---:|---|
| collisions | 97,669,586 | `06a03c0e5504175e23ed98e3e0b4767ac61d396c073405a1bddab9339c18f948` |
| vehicles | 103,741,631 | `05453dca2651ad8a27b4ac0e6a0450a852c506e4d495111ffca398f3d311e26a` |
| casualties | 53,685,509 | `870be5a934ae10baf5c52163d59eccba0ab2f40327ffa6b2bd354d2e7ee6b108` |

All three files were unchanged against the previous validated snapshot in this run. The guide hash is `51b08606ef3dad1ae5d4c8726241b022fc8e4bbdc910ef0e5cc2fb92b054fc12`. The source-code reader checks expected guide rules instead of inventing category mappings. Acquisition is deliberately manual: the engineering focus is reliable processing, not a fragile download scraper. An updated source snapshot must be reviewed for changed periods, columns and codes.

## 3. Silver: model and engineering decisions

### Preserve the source grains

| Table | One record represents | Key |
|---|---|---|
| Collisions | One reported collision | `collision_index` |
| Vehicles | One vehicle within a collision | `collision_index`, `vehicle_reference` |
| Casualties | One casualty within a collision | `collision_index`, `casualty_reference` |

Directly joining both child tables by collision would create a vehicle-by-casualty cross-product. For example, two vehicles and three casualties would produce six rows for one collision. The pipeline first aggregates each child table independently, then joins the summaries to collisions. A synthetic test verifies one output collision with counts two and three. Vehicle-type Gold membership is separately deduplicated by collision/type; overlapping types remain explicit.

### Minimal, justified transformations

CSV numeric types are inferred, while collision identifiers, reference strings and time text are explicitly preserved. This avoids losing leading zeroes or mistaking `HH:mm` for a date-bearing timestamp. Dates become Spark dates; month, hour and weekend indicators follow. Parse failures and inconsistencies with source year/weekday are measured. Schema inference is convenient for this snapshot but requires review when the source changes.

Official labels are added for collision severity, weather, light, road surface, road type, vehicle type and casualty severity. Source codes remain available for traceability. The guide separates main Fatal/Serious/Slight codes from enhanced seriousness categories. No remapping of the already stable main severity codes is needed. No severity adjustment is applied.

Age code `-1` means missing or out of range. Separate clean columns exclude negative values from numerical calculations while preserving the original codes and every record. Zero remains valid for casualties. No arbitrary upper age cutoff is used. The speed-limit clean column accepts the six public-highway limits listed in the guide, in mph, while retaining source missing/unknown codes separately.

### Quality evidence and publication rules

| Table | Input rows | Output rows | Missing keys | Duplicate-key excess |
|---|---:|---:|---:|---:|
| collisions | 513,801 | 513,801 | 0 | 0 |
| vehicles | 937,265 | 937,265 | 0 | 0 |
| casualties | 652,821 | 652,821 | 0 | 0 |

Observed quality findings:

- No failed date/time conversions or calendar inconsistencies.
- No unmatched collision or vehicle relationships and no parent/child year mismatches.
- No difference between recounted vehicle/casualty totals and source collision counters.
- No unrecognised codes in the seven decoded categorical fields.
- 142,240 driver age codes and 14,106 casualty age codes are `-1`; these become null only in clean columns.
- 53 longitude values and 53 latitude values are missing; recorded values pass global range checks.
- Three speed limits are coded `-1`; no unsupported speed values were found.
- All Parquet outputs retain expected row counts and data types after reloading.

The rise in clean-age null counts is intentional and not a loss of records. Averages must disclose their known-age denominator. Missing coordinates do not prevent temporal analysis, so those records are retained. Global coordinate ranges do not prove location accuracy. No rows were rejected in this run.

Missing/duplicate keys and empty tables cause failure. Structural discrepancies, unsupported values, conversion failures and required-field failures prevent Silver publication. Reports retain anomaly evidence for review. Gold runs only when the current Silver run passes; an old successful Silver pointer cannot substitute for a failed current stage.

### What the Silver checks establish

Silver is the main engineering contribution. It exposes reusable collision, vehicle and casualty tables rather than embedding a particular Gold ranking into the cleaned model. The analytical question determines which fields need interpretation; it does not justify manufacturing transformations for fields that are already correctly typed or standardised.

| Field or rule | Silver decision | Evidence and limit |
|---|---|---|
| Collision identifiers and references | Preserve text, including leading zeroes | CSV preservation test; uniqueness at the correct grain |
| Date and time | Parse dates, retain time text and derive calendar fields | Failed conversions and calendar mismatches are counted |
| Official category codes | Retain codes and add guide-derived labels | Mapping coverage distinguishes recognised unknowns from unrecognised codes |
| Missing-age code | Preserve source `-1`; expose a separate nullable age | Row counts unchanged; averages use known-age denominators |
| Coordinates | Count missing values separately from range failures | A range check cannot establish geographic accuracy |
| Child records | Validate foreign keys, parent years and declared counts | Separate checks detect orphans and count discrepancies |
| Parquet outputs | Reload and compare counts and data types | Publication depends on successful validation |

A null-only audit would miss all 142,240 coded missing driver ages, because `-1` is a populated numeric value. Interpreting the guide therefore matters more than applying generic null replacement. Similarly, a recognised weather category labelled unknown remains traceable, whereas a code absent from the selected guide requires review. The pipeline retains useful rows and reports information loss at the attribute level instead of silently deleting them.

The relationship checks provide complementary evidence. Matching foreign keys alone would not prove that all expected child records are present. Comparing observed vehicle and casualty counts with collision counters checks completeness relative to those counters. Aggregating children separately and verifying the enriched collision count guards against join multiplication. These tests establish structural consistency of this snapshot, not independent confirmation of every police record.

All three source tables and the enriched table remain available, allowing downstream questions to change without rebuilding a flattened vehicle-by-casualty dataset. Before/after metrics and saved schemas make transformations reviewable. Source hashes identify the input bytes used; the guide hash identifies the vocabulary. Together they support reruns and help distinguish code changes from source revisions.

## 4. Gold: a focused analytical use of Silver

Gold remains a small descriptive consumer of the validated Silver tables. Its primary table contains 180 lighting × road-type × speed-limit profiles; an annual version contains 803 observed year/profile combinations. Counts reconcile to Silver both globally and within each year. Supplementary tables provide context without changing the Silver model.

### Volume and reference population

The pooled top five profiles contain 78,008 severe collisions (62.725% of all severe cases). A profile can have a large caseload because it is common among recorded collisions. To interpret that concentration, compare its share of all collisions with its share of severe cases:

| Profile | Share of all collisions (%) | Share of severe collisions (%) | Severe-minus-all gap (percentage points) |
|---|---:|---:|---:|
| Daylight / Single carriageway / 30 mph | 30.371 | 28.247 | -2.124 |
| Darkness - no lighting / Single carriageway / 60 mph | 2.624 | 4.025 | 1.401 |

The leading profile contains 35,130 severe cases, but its share of severe cases is below its share of all recorded collisions. Its absolute importance should therefore be distinguished from over-representation. The contrasting profile contains 5,006 severe cases and has a positive gap: it contributes more to severe cases than its overall recorded frequency would suggest. Neither comparison measures traffic exposure, causal effects or statistical significance.

### Annual sensitivity check and useful conclusion

The pooled leading profile ranks 1–1 by severe-case count across the five years and has a negative gap in 5 years. The contrasting profile has a positive gap in 5 years. Each year uses its own collision and severe-case totals. Profiles are selected from the pooled data and followed across the same period; this is descriptive sensitivity analysis, not out-of-sample validation. Changes in recording and case composition remain relevant.

The practical conclusion is to inspect two aspects together: large recorded caseloads and relative over-representation among severe cases. An analyst can use these groups to organise a local investigation, then obtain location, exposure and infrastructure information. The output cannot select an intervention or estimate its effectiveness. The [results and figures](results.md) and [executed notebook](../notebooks/analysis.ipynb) provide detail; the main project contribution remains Silver's validated, reusable data.

## 5. Spark implementation and performance evidence

Transformations use built-in DataFrame functions. Monthly Gold aggregation uses Spark SQL over a validated temporary view, demonstrating both APIs. Python row UDFs are unnecessary. Source frames are cached because audits, transformations, relationship checks and writes reuse them, then unpersisted. Eight shuffle partitions are a small-VM starting point, not a claimed optimum. The saved enrichment plan exposes child aggregations and joins.

Silver outputs, including the extra enriched table, total 107,975,683 Parquet bytes. This is an output-size observation, not a like-for-like compression ratio against the three raw inputs. Silver is not blindly partitioned by year/month; the current snapshot is small, and additional directory partitions could increase small-file overhead. Small Gold outputs use one Parquet file per table.

A separate benchmark runs the same three-column yearly aggregation against the source CSV and Silver Parquet, using an explicit CSV schema and no Spark cache. All query results match. Three trials alternate format order:

| Format | Trial seconds | Median seconds |
|---|---|---:|
| CSV | 6.0336, 1.038, 0.743 | 1.038 |
| Parquet | 1.4044, 0.3758, 0.2923 | 0.3758 |

The Parquet plan shows a batched, three-column scan; the CSV plan also prunes its projected schema but scans textual input. Both use partial aggregation and a shuffle before final aggregation. This experiment supports Parquet for these queries. JVM warm-up, OS page cache and VM conditions are uncontrolled; three trials do not establish general performance or multi-node scalability. Benchmark evidence is tied to run `20261110T084021906581Z` on the same input snapshot.

The full evidence run took **81.37 seconds**: Bronze 0.95 s, Silver 59.49 s and Gold 19.48 s, plus session/orchestration overhead. These are observations, not a service-level guarantee.

## 6. Testing, delivery and operation

The test suite uses synthetic records, the small versioned guide and temporary directories. It covers keys, string preservation, dates, missing codes, age averages, collision-grain enrichment, overlapping vehicle groups, source change detection, rejected publication, profile reference populations and end-to-end totals. Annual-profile tests ensure that each year uses its own denominator rather than the pooled period. A deliberate bad update removes a vehicle: Silver requires review, Gold does not run, and previous Silver/Gold pointers remain unchanged. The small-data suite requires no raw CSVs; the versioned guide is its only source-data dependency. The latest suite completed 17 tests successfully.

The reproducible command is `bash scripts/run_project.sh --env dev`. Development and production configurations share the fixed input snapshot but use separate outputs. A lock prevents concurrent full runs against the same operational directory. Reports are persisted before atomic publication pointers. Logs and non-zero exits expose failures; failed-run directories remain inspectable. Retention is manual and must be defined for long-running operation.

GitHub Actions is configured to run the small tests on changes to `dev` and Pull Requests. The main workflow tests, executes Bronze–Silver–Gold against the VM snapshot and uploads evidence. It supports manual dispatch and a monthly schedule; legacy labs are manual jobs. The schedule reprocesses available data and does not acquire new source releases. A GitHub run, branch protection and environment approvals remain to be demonstrated and verified. YAML configuration alone is not deployment evidence.

## 7. Reflection, limitations and next improvements

The strongest design decision is preserving grains and making publication depend on quality, rather than applying generic cleaning operations. Decoding improves usability without discarding the source vocabulary. Missing-code handling avoids biased numeric calculations while retaining records for other analyses. The trade-off is that downstream users must still choose valid denominators and interpret unknown groups.

A concurrent test/full-run attempt ended with a killed process (exit 137), consistent with memory pressure on the 8 GB VM. Sequential tests with a 1 GB driver succeeded. Resource settings and scheduling therefore matter even at this modest volume; increasing partitions or running jobs in parallel is not automatically an improvement.

The design is intentionally a full batch recalculation, not an incremental platform. With larger volumes or frequent revisions, priorities would include explicit schema contracts, archived source versions, period-aware ingestion, measured partitioning and resource sizing. A changed guide may need reader changes. Original download dates remain unknown. Age extremes are observed, not independently validated. Reporting bias, severity-method changes and missing exposure limit analytical interpretation.

## 8. Conclusion

The project's main contribution is a reproducible Silver layer that preserves the three source grains, interprets official codes, documents missing information and validates relationships before publication. The full local pipeline and its 17 tests passed, providing reusable tables and traceable quality evidence.

Gold demonstrates their analytical use: the profiles with the largest severe-collision counts can differ from those over-represented among recorded severe cases. Comparing counts, reference shares and annual results makes that distinction explicit. These findings support further investigation while remaining limited by reporting practices and the absence of traffic-exposure data. Remote CI and deployment verification remain outstanding, as documented in the delivery section.
