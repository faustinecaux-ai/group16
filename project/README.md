# DAT535 — STATS19 Road Collision Pipeline

[Read the five-page project report (PDF)](reports/pdf/report.pdf) · [LaTeX source](reports/pdf/report.tex)

A reproducible PySpark batch pipeline for police-reported personal injury collisions in Great Britain, following **Bronze → Silver → Gold**. The local snapshot covers 2021–2025. The pipeline, small-data tests, analytical notebook and local execution evidence are available. GitHub workflows are configured but their first remote execution remains to be demonstrated after pushing and reviewing the code.

Start with [Getting Started](GETTING_STARTED.md). See the [project definition](project_definition.md), [engineering report](reports/project_report.md), [analytical results](reports/results.md), [Silver quality notes](silver_quality_notes.md) and [notebook](notebooks/analysis.ipynb).

## Scope and analytical question

Which lighting, road-type and speed-limit profiles account for the largest numbers of fatal or serious reported collisions in Great Britain, 2021–2025? The intended use is to identify profile groups for further investigation, using both severe-case counts and percentages. Outputs describe associations within recorded collisions. They do not estimate causal effects, risk per kilometre travelled or adjusted official severity trends.

## Setup and source data

The course VM uses Python 3.11, Spark 3.5.0 at `/opt/spark`, Java 8 at `/usr/lib/jvm/java-8-openjdk-amd64`, and `$HOME/spark-env`. Spark supplies its Python libraries through `spark-submit`; a separately installed, mismatched PySpark package must not replace them. Presentation dependencies and their tested versions are listed in `requirements.txt`.

Another installation needs compatible Java/Spark/Python, plus `python -m pip install -r requirements.txt` for plots and notebooks. The transformation pipeline and tests use Python's standard library and Spark, without pytest or an Excel engine. Launchers accept `SPARK_HOME`, `JAVA_HOME`, `SPARK_VENV`, `SPARK_MASTER` and `SPARK_DRIVER_MEMORY` overrides.

Acquire the three CSVs and guide manually from the URLs in [source_catalog.json](data/bronze/stats19/metadata/source_catalog.json), and place them at:

```text
data/bronze/stats19/
├── raw/collisions/collisions_last5years.csv
├── raw/vehicles/vehicles_last5years.csv
├── raw/casualties/casualties_last5years.csv
└── reference/stats19_data_guide.xlsx
```

The last-five-years URLs can change. To reproduce the recorded results, retain the original raw snapshot outside Git and compare its hashes against the versioned ingestion metadata. A validation timestamp is not a retrieval timestamp; the original retrieval date was not recorded. Review a changed guide and source schema rather than assuming identical results from newer downloads.

## Run the project

From the `project` directory:

```bash
bash scripts/run_tests.sh
bash scripts/run_project.sh --env dev
source "$HOME/spark-env/bin/activate"
python scripts/render_results.py --env dev
```

The test suite uses tiny fixtures, including an end-to-end run and a deliberately failed update. Run Spark processes sequentially on this 8 GB VM to avoid memory contention. The pipeline wrapper uses 2 GB driver memory; the test wrapper uses 1 GB.

`config/dev.json` and `config/prod.json` share the input snapshot and separate outputs. To override paths:

```bash
bash scripts/run_project.sh --env dev \
  --bronze /path/to/stats19 --output-root /path/to/output
python scripts/render_results.py --gold /path/to/output/gold --output /path/to/presentation
```

For a full log, use Bash with `pipefail` so a failing process cannot be hidden by `tee`:

```bash
set -o pipefail
bash scripts/run_project.sh --env dev 2>&1 | tee /tmp/stats19-pipeline.log
```

## Layer design

**Bronze:** validate source files, non-zero size, essential headers, checksums and the guide. Preserve raw files and initial versioned metadata. Each run records file hashes, column names and change status in a separate output directory. File hashes detect changes; they do not deduplicate records.

**Silver:** retain the three grains: one row per collision, vehicle and casualty. Check missing/duplicate keys, critical fields, date/time parsing, calendar consistency, coordinate ranges, category mapping coverage, foreign keys, parent/child years and declared versus observed counts. Keep codes and official labels. Aggregate vehicles and casualties separately before enriching collisions. Write Parquet and validate reloaded types and counts.

The age columns retain source values and add `age_of_driver_clean` / `age_of_casualty_clean`. Negative values become null in the clean columns; the official `-1` means missing or out of range. Zero is valid for casualties. Unexpected negative ages require review. Valid speed limits use `speed_limit_mph_clean`; source missing codes remain in the original column and form a missing Gold group. Missing coordinates or officially coded missing ages do not justify deleting an otherwise useful record.

**Gold:** publish a lighting × road type × speed limit profile table, plus monthly, hourly, weather, lighting, road type, speed limit, weekend and vehicle-type summaries as Parquet and small CSVs. Measures include collision count, fatal-or-serious collision count/percentage, casualty counts and mean vehicles involved. The profile table compares shares of all collisions and all severe cases, alongside within-profile severity; profiles are ranked by severe-case count. Their difference is in percentage points and describes recorded-case representation, not traffic risk. `collision_profiles_by_year` repeats the comparison with separate reference totals for each year and reconciles annually to Silver. Non-overlapping summaries reconcile to Silver totals. Vehicle-type groups count a collision once per type and may overlap, so they must not be summed into the overall total.

## Quality gates, outputs and monitoring

```text
data/pipeline/dev/runs/<run_id>/quality_report.json
data/silver/dev/runs/<run_id>/{collisions,vehicles,casualties,collisions_enriched}/
data/gold/dev/runs/<run_id>/{monthly,hourly,weather,...}/
```

Production uses corresponding `prod` directories. Each layer has `latest_successful.json`; the pipeline pointer identifies a completely passed Bronze–Silver–Gold run. Reports are written before publishing pointers, and pointer replacement is atomic. A file lock prevents concurrent full runs against the same operational directory. Separate standalone stage commands should not be run concurrently against the same outputs.

`passed` means the implemented rules passed, not that data is perfect. Missing/duplicate keys or empty tables cause failure. Relationship mismatches, unrecognised codes, parsing errors, inconsistent years/days, unexpected negative ages, unsupported speeds, invalid coordinates or missing required collision attributes prevent Silver publication. `needs_review` gives exit code 2 for standalone Silver; the full pipeline stops with a non-zero status before Gold. Execution failures produce `failed` reports. Existing validated outputs remain available. Inspect the latest attempt as well as the successful pointer; old outputs do not prove a new run succeeded.

Every run produces its own machine-readable quality metrics. The Markdown notes are a dated explanation, not an automatically refreshed report. Saved plans and Parquet byte counts support performance analysis. Old output directories are retained; monitor space and define retention before operating long-term.

## Analysis and performance evidence

Open `notebooks/analysis.ipynb` and use **Restart Kernel and Run All**. It reads small validated Gold tables, associated quality reports and saved plans. Set `DAT535_ENV=prod` or `STATS19_GOLD_DIR=/path/to/gold` before execution when using a different output location. The checked-in notebook includes executed outputs.

`reports/results.md` and `reports/figures/` are regenerated by `render_results.py`. A separate measured CSV/Parquet comparison is available in `reports/performance.json` with its execution plans. To rerun it:

```bash
PYSPARK_DRIVER_PYTHON="$HOME/spark-env/bin/python" \
PYSPARK_PYTHON="$HOME/spark-env/bin/python" /opt/spark/bin/spark-submit \
  --master 'local[2]' --driver-memory 1g scripts/benchmark_io.py
```

The benchmark uses the same three-column aggregation, an explicit CSV schema, alternating execution order and no Spark cache. It checks identical results. VM conditions, JVM warm-up and OS page cache are uncontrolled, so measured differences do not establish general scalability gains.

## CI and production execution

[dat535-dev.yml](../.github/workflows/dat535-dev.yml) runs the small project tests after pushes to `dev`, Pull Requests targeting `dev`/`main` and manual dispatch. Raw CSVs are not required. Test logs are uploaded for seven days. Legacy lab jobs are optional manual runs, controlled by `run_labs`.

[dat535-prod.yml](../.github/workflows/dat535-prod.yml) tests and runs the full project on `main`, manually or monthly at 03:00 UTC on the first day. It uses the external snapshot at `STATS19_BRONZE_DIR` (repository variable; default `/home/ubuntu/group16/project/data/bronze/stats19`) and persistent outputs under `$HOME/stats19-pipeline/prod`. It publishes run evidence and presentation artifacts, not large raw datasets. A schedule reprocesses a fixed snapshot; it does not download new releases. The separate legacy workflow is manual only.

Configure the VM runner, the `main` GitHub Environment and its required reviewers, and make `STATS19 pipeline tests` a required PR check as appropriate. Those settings cannot be established by YAML alone. Scheduled GitHub workflows operate from the default branch, so the new schedule requires reviewed code in `main`. A first successful GitHub run and approval evidence remain separate from the local checks.

## Known limits

The pipeline recalculates the full snapshot and uses inferred numeric schemas with protected textual identifiers. It is not an incremental or multi-node production platform. A schema change requires review. Results use reported severity; no adjustment for reporting-system changes is applied. Original retrieval dates and exact upstream version history are incomplete. A live GitHub deployment/schedule demonstration is pending. These limits are recorded in the engineering report rather than hidden as completed work.

Contains public sector information licensed under the Open Government Licence v3.0. Source: [Department for Transport, STATS19](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data).
