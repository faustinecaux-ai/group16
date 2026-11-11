# DAT535 — STATS19 Road Collision Pipeline

**Faustine Caux and Jules Gombert · Group 16 · University of Stavanger**

[Project report (PDF)](reports/pdf/report.pdf) · [LaTeX source](reports/pdf/report.tex) · [Executed notebook](notebooks/analysis.ipynb) · [Analytical results](reports/results.md)

This project implements a reproducible PySpark batch pipeline for police-reported personal injury collisions in Great Britain, 2021–2025. The principal engineering contribution is **Silver**: three reusable tables at their original grains, official-code interpretation, collision-level enrichment and quality gates. Gold provides a focused descriptive application of those validated tables.

## Scope and assessment evidence

The analytical question is: **Which lighting, road-type and speed-limit profiles account for the largest numbers of fatal or serious reported collisions in Great Britain, 2021–2025?** The intended consumer is a road safety analyst selecting groups for local investigation. Outputs distinguish absolute caseload, within-profile severity and shares of all severe cases. They do not estimate causal effects, risk per kilometre or severity-adjusted official trends.

| Evidence | Location |
|---|---|
| Engineering report | [Six-page PDF](reports/pdf/report.pdf), with [LaTeX source and build instructions](reports/pdf/README.md) |
| Silver validation | [Versioned JSON report](reports/silver_quality.json) and [quality notes](silver_quality_notes.md) |
| Analysis | [Executed notebook](notebooks/analysis.ipynb), [results and figures](reports/results.md), [machine-readable summary](reports/results.json) |
| Performance | [Measured trials](reports/performance.json), [CSV plan](reports/csv_plan.txt), [Parquet plan](reports/parquet_plan.txt) |
| Design | [Project definition](project_definition.md) |
| Reproduction | [Execution guide](GETTING_STARTED.md), scripts and environment configuration |

The reference development run is `20261110T150552746288Z`: Bronze, Silver and Gold passed in 81.37 seconds. The saved suite records 17 successful tests. A retained production-directory manifest identifies passed run `20261110T180208676518Z` (74.00 seconds). These are recorded execution measurements, rather than performance guarantees. The production manifest alone does not identify its triggering GitHub event or establish environment approvals.

## Layer design

**Bronze** preserves the three raw CSVs and official guide. Validation records file sizes, essential headers, duplicate-header checks, SHA-256 hashes and change status in a separate run directory. Hashes detect file changes; they do not deduplicate records or replace review of changed inputs.

**Silver** preserves these grains:

| Table | Key | Reference rows |
|---|---|---:|
| Collisions | `collision_index` | 513,801 |
| Vehicles | `collision_index`, `vehicle_reference` | 937,265 |
| Casualties | `collision_index`, `casualty_reference` | 652,821 |

Source identifiers remain textual, dates are parsed and calendar fields derived. Original category codes remain alongside official labels for severity, weather, lighting, road surface, road type and vehicle type. Source ages are retained with `age_of_driver_clean` / `age_of_casualty_clean`; negative codes become null only in prepared columns. Zero is valid for casualties. Recognised speeds populate `speed_limit_mph_clean`, while unknown values retain their source codes.

Vehicles and casualties are aggregated independently before joining to collisions. This prevents a vehicle-by-casualty cross-product and preserves one row per enriched collision. Checks cover missing/duplicate keys, important fields, date/time parsing, calendar consistency, coordinate ranges, mapping coverage, foreign keys, parent/child years and declared versus observed child counts. Parquet outputs are reloaded to compare counts and data types.

The reference run retains every source row. It records 142,240 missing driver-age codes, 14,106 missing casualty-age codes, 53 missing longitudes and 53 missing latitudes, and three missing speed limits. Missing attributes are measured without deleting otherwise useful records. The [Silver notes](silver_quality_notes.md) explain their interpretation.

**Gold** publishes ten summaries as small Parquet and CSV tables: pooled collision profiles, annual profiles, monthly, hourly, weather, lighting, road type, speed limit, weekend and vehicle type. Exclusive summaries reconcile to Silver; annual profiles also reconcile by year. Vehicle memberships count a collision once per type but overlap across types. Profiles compare severe-case counts, within-profile severity and shares of recorded populations. Unknown groups remain represented.

## Source data and environment

The documented VM uses Python 3.11, Spark 3.5.0 at `/opt/spark`, Java 8 at `/usr/lib/jvm/java-8-openjdk-amd64`, and the `$HOME/spark-env` Python environment. Spark supplies its Python libraries through `spark-submit`. Presentation dependencies are pinned in [requirements.txt](requirements.txt). The transformation pipeline and fixture tests use Spark and Python's standard library without pytest or an Excel engine.

The source paths are:

```text
data/bronze/stats19/
├── raw/collisions/collisions_last5years.csv
├── raw/vehicles/vehicles_last5years.csv
├── raw/casualties/casualties_last5years.csv
└── reference/stats19_data_guide.xlsx
```

Source links, licence and provenance limitations are recorded in [source_catalog.json](data/bronze/stats19/metadata/source_catalog.json). Raw files remain outside Git. Exact reproduction requires the retained snapshot matching the versioned baseline hashes; current downloads from mutable last-five-years URLs may differ. Validation timestamps do not establish acquisition dates. A changed source schema or guide requires review.

## Reproduction commands

From the `project` directory, on the documented environment:

```bash
bash scripts/run_tests.sh
bash scripts/run_project.sh --env dev
source "$HOME/spark-env/bin/activate"
python scripts/render_results.py --env dev
```

The suite uses tiny fixtures and temporary outputs. A deliberately invalid update is expected to print `needs_review` or `failed`; the overall `unittest` result is `OK` when detection behaves correctly. Spark processes run sequentially on the 8 GB VM; the full launcher defaults to a 2 GB driver and the test launcher to 1 GB.

Development and production configurations share inputs and separate outputs. External paths are supported:

```bash
bash scripts/run_project.sh --env dev \
  --bronze /path/to/stats19 --output-root /path/to/output
python scripts/render_results.py --gold /path/to/output/gold --output /path/to/presentation
```

Launchers accept `SPARK_HOME`, `JAVA_HOME`, `SPARK_VENV`, `SPARK_MASTER` and `SPARK_DRIVER_MEMORY` overrides. On another installation, compatible Spark, Java and Python are required; `python -m pip install -r requirements.txt` supplies presentation dependencies. Bash `set -o pipefail` preserves a failing pipeline exit code when output is piped through `tee`.

The report's additional figures are reproducible from versioned evidence without Spark or raw CSVs:

```bash
python scripts/render_report_figures.py
```

The executed notebook includes saved outputs. Re-execution reads the small validated Gold CSVs and associated manifests at their recorded paths; these runtime datasets are not included in Git. `STATS19_GOLD_DIR=/path/to/gold` and `DAT535_ENV=prod` select alternative locations.

## Quality gates and publication

```text
data/pipeline/dev/runs/<run_id>/quality_report.json
data/silver/dev/runs/<run_id>/{collisions,vehicles,casualties,collisions_enriched}/
data/gold/dev/runs/<run_id>/{monthly,hourly,weather,...}/
```

Each layer has a `latest_successful.json` pointer. Reports are written before atomic pointer replacement. Publication is atomic per pointer, rather than transactional across layers; the **pipeline manifest** identifies a consistent passed end-to-end run. A file lock prevents concurrent full runs against the same operational directory. Standalone stages require separate coordination.

Missing/duplicate keys and empty tables cause failure. Implemented relationship, parsing, category and required-field violations block Silver publication. Standalone Silver exits with code 2 for `needs_review`; the full pipeline stops before Gold. Failed attempts preserve their own reports and the previous successful pointers. A Gold failure can follow a successful Silver publication, so the latest attempt and pipeline manifest remain relevant. `passed` denotes the implemented checks, rather than exhaustive correctness. Runtime output retention is currently manual.

## Performance and CI/CD

The [benchmark](reports/performance.json) compares the same three-column yearly aggregation on CSV and Silver Parquet, using an explicit CSV schema, alternating execution order and no Spark cache. All results match. Median observed times were 1.038 s for CSV and 0.3758 s for Parquet. JVM warm-up, OS page cache and VM conditions were uncontrolled; three trials do not establish general scalability gains.

[dat535-dev.yml](../.github/workflows/dat535-dev.yml) runs the small tests on `dev` pushes, same-repository pull requests targeting `dev`/`main`, and manual dispatch. Fork pull requests are excluded from the self-hosted test job. Logs are retained for seven days. Legacy labs are optional manual jobs.

[dat535-prod.yml](../.github/workflows/dat535-prod.yml) tests and runs Bronze–Silver–Gold on `main` pushes or manual dispatch. Inputs use `STATS19_BRONZE_DIR` (repository variable; default `/home/ubuntu/group16/project/data/bronze/stats19`), with persistent outputs under `$HOME/stats19-pipeline/prod`. The workflow uploads run evidence and presentation artifacts. **No periodic schedule is currently enabled.** The separate legacy workflow is manual only. Branch checks and environment approvals depend on GitHub settings, which are not established by these files.

## Limitations and attribution

The pipeline recalculates a fixed snapshot on one VM. Numeric schemas are inferred, and the guide reader depends on the selected workbook layout. Original retrieval dates and upstream revision history are incomplete. Reporting bias, missing traffic exposure and unadjusted severity limit analytical interpretation. Global coordinate ranges do not establish geographic accuracy, and recorded age extremes have not been independently validated. The report discusses these limits and possible extensions.

Source: UK Department for Transport, [STATS19 road safety open data](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data). Contains public sector information licensed under the Open Government Licence v3.0.
