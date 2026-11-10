# DAT535 — Project Definition

## Great Britain Road Collision Data Pipeline

The project follows the Bronze → Silver → Gold Medallion Architecture. Its main contribution is a trusted Silver layer, supported by a reproducible batch pipeline, quality evidence and a focused analytical output. This document defines the target design; the README records implementation status.

### 1. Project statement and scope

Build a PySpark pipeline that converts STATS19 records into reusable tables and indicators addressing this question: **Which lighting, road-type and speed-limit profiles account for the largest numbers of fatal or serious reported collisions in Great Britain, 2021–2025?**

The intended consumer is a road safety analyst identifying profile groups for closer local investigation. The outputs show absolute numbers alongside within-profile severity and each profile's share of all severe cases; they do not prescribe an intervention. Deliverables are three Silver tables, purpose-specific Gold aggregates, and charts and tables in an analytical notebook. A web dashboard is outside the initial scope.

The analysis describes associations, not causal effects. Without exposure data, such as distance travelled, it cannot estimate collision risk per kilometre. Prediction, real-time processing and coverage of every road accident are excluded.

### 2. Dataset source and suitability

The source is UK Department for Transport STATS19 CSV data: collisions, vehicles and casualties. The local snapshot covers 2021–2025 and contains 513,801 collisions, 937,265 vehicle records and 652,821 casualty records, totalling approximately 255 MB. The official Excel guide provides code definitions and specification conversion information.

The related tables support distributed scans, joins and aggregations. This volume is manageable on one VM; Spark provides practical experience with distributed processing and a path to processing additional years, rather than evidence that a cluster is necessary for this snapshot.

STATS19 describes police-reported personal injury collisions in Great Britain. Limitations include under-reporting, missing information and changes in recording or categories. Publication status, licensing and attribution requirements must be verified and documented. Source release frequency will guide updates; the initial pipeline processes a fixed, traceable snapshot.

### 3. Bronze — ingest and preserve

Preserve the three CSVs unchanged in separate dataset folders alongside the reference guide. Validate file presence, non-zero size and SHA-256 checksums; record file metadata and source column names. Manual acquisition is acceptable with documented source links, retrieval dates and instructions.

Traceability should include a run identifier and guide version. An unchanged hash identifies the same file; a changed hash prompts a schema and period review. Retain metadata for input versions to identify late revisions. Initially, rerun the complete snapshot instead of introducing complex incremental processing. Exclude raw and generated data from Git. Small fixtures should test missing or empty input files and change detection.

### 4. Silver — primary focus

**Data model.** Keep one row per collision (`collision_index`), vehicle (`collision_index`, `vehicle_reference`) and casualty (`collision_index`, `casualty_reference`). Validate keys and collision relationships, including the casualty-to-vehicle relationship. Joining vehicles directly to casualties by collision would multiply rows and distort counts.

**Understanding and cleaning.** Audit schemas, years, duplicates, missing values and distributions. Convert types only when necessary, particularly dates, and measure conversion failures. Interpret special values using the official guide. Retain records with unknown attributes when other information remains useful. For example, preserve source age codes and provide a separate numeric age column that excludes missing codes from calculations. Flag anomalies; justify and count any rejected records and retain their rejection reasons. Do not arbitrarily resolve duplicate keys with conflicting values.

**Harmonisation and enrichment.** Retain source codes and add official labels for severity, weather, lighting, road surface, road type and vehicle type. Review specification differences, particularly severity, and distinguish recognised unknown categories from unrecognised codes. Derive month, hour and weekend indicators. For collision-level enrichment, aggregate vehicles and casualties separately before joining their counts and relevant characteristics to collisions. Compare observed counts with source collision counters.

**Quality evidence.** Generate a report for each run containing input/output counts, missing and duplicate keys, missing important fields, failed conversions, suspicious values, unmatched references, mapping coverage and count discrepancies. Missing or duplicate output keys prevent validation. Other anomalies require measurement and review, with documented tolerances. Retaining useful records with missing coordinates or ages is preferable to treating deletion as improvement.

**Implementation, performance and scalability.** Use readable PySpark DataFrame and Spark SQL operations, built-in functions and small testable functions. Write Parquet outputs and validate their reloaded counts and schemas. Measure execution times, inspect plans and identify significant scans, joins and shuffles. Justify caching, broadcast joins and partitioning with evidence; avoid excessive small files. Explain the implications of more years, revised inputs, skew and schema changes for the initial full-snapshot approach.

### 5. Gold — serve the analytical outcome

The main Gold table groups collisions by lighting, road type and speed limit. It reports collision counts and within-profile severity, comparing each profile's share of all collisions with its share of severe cases. An annual table checks these patterns using each year's own reference totals. Rank profiles by absolute severe-collision counts, keeping unknown categories. This focused three-dimension cross-tab remains small enough to inspect and reconciles to Silver totals. Supplementary monthly, hourly, environmental and vehicle summaries provide context. Vehicle comparisons may group collisions involving at least one vehicle of a selected type; overlapping groups must not be added as if mutually exclusive.

Define each table's grain, denominator and treatment of unknown values. Reconcile aggregates with Silver totals. Publish outputs only after required quality checks pass, recording the source run identifier and completion status. A failed run must not replace the latest validated results.

### 6. End-to-end delivery and lifecycle

Provide a documented Bronze → Silver → quality checks → Gold command, compatible dependencies and separate development/production output settings. Use notebooks for exploration and presentation, and Python modules or scripts for reusable logic. Another engineer should be able to configure and run the pipeline.

Develop on `dev` and use GitHub Actions on the VM runner to test representative small fixtures and reference data. Check changes and Pull Requests to `main`; review and merge validated code before controlled execution on the VM. Demonstrate a scheduled end-to-end run on the local snapshot without assuming that each run requires new data or a download.

Keep logs, run status, rejected-record information and quality reports visible. The project group owns failure review and update validation. The report will explain engineering decisions, measured results, limitations and reflection, supported by tests, quality metrics, execution evidence and performance analysis.
