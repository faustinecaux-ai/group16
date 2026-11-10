# Getting Started

## 1. Understand the files

- `data/bronze/stats19/raw/`: original CSV files; never edited by the pipeline.
- `scripts/ingest_bronze.py`: file validation, hashes and source headers.
- `scripts/build_silver.py`: transformations, relationships and quality checks.
- `scripts/build_gold.py`: analytical summaries and reconciliation.
- `scripts/run_project.py`: ordered execution and failure handling.
- `scripts/test_silver.py`: small synthetic and end-to-end tests.
- `config/dev.json`, `config/prod.json`: shared inputs, separate outputs.
- `notebooks/analysis.ipynb`: presentation using validated Gold tables.
- `reports/`: exported charts, results and performance evidence.

## 2. Run the small tests

From the `project` directory:

```bash
bash scripts/run_tests.sh
```

Tests use synthetic data in temporary directories and the small versioned guide.
They do not read the large STATS19 CSVs. A test that deliberately creates bad
data can print `needs_review` or `failed`; the overall `unittest` result must
still say `OK`, because detecting that failure is the expected behaviour.

## 3. Run the complete project

```bash
bash scripts/run_project.sh --env dev
```

The launcher selects the VM's Python, Java and Spark installations. It runs
Bronze validation, then Silver, then Gold. A blocking Silver quality issue stops
the process before Gold. Successful results are recorded under:

```text
data/pipeline/dev/runs/<run_id>/quality_report.json
data/silver/dev/runs/<run_id>/
data/gold/dev/runs/<run_id>/
```

Each layer has a `latest_successful.json` pointer. The pipeline pointer identifies
an entire successful run. A failed update leaves the previous successful pointers
available. Always inspect run status: the presence of old outputs does not mean
the latest attempt succeeded.

To use another source location or output root:

```bash
bash scripts/run_project.sh --env dev \
  --bronze /path/to/stats19 --output-root /path/to/output
```

## 4. Read the results

Activate the course environment for presentation tools:

```bash
source "$HOME/spark-env/bin/activate"
python scripts/render_results.py --env dev
```

Open `reports/results.md` for tables and figures. Open `notebooks/analysis.ipynb`
in Jupyter or VS Code and select **Restart Kernel and Run All**. It reads the
latest validated Gold run and does not need Spark or the raw CSVs for plotting.
It does need the small Gold CSVs and associated reports at their recorded paths.

For a different output root, pass `--gold /path/to/output/gold` to the rendering
script, or set `STATS19_GOLD_DIR` before running the notebook.

## 5. Understand CI and scheduled execution

Git records code versions; GitHub hosts them. After the necessary files are pushed,
GitHub Actions asks the VM runner to execute the small tests for a change to `dev`
or a Pull Request targeting `dev`/`main`. Results appear in the Actions tab.

After the validated code reaches `main`, the production workflow runs the complete
project using the VM's external input snapshot. It also defines a monthly schedule
and supports a manual launch. Environment approvals and mandatory checks must be
configured separately in GitHub; YAML files do not establish those protections.

Local validation of workflow commands is not evidence of a GitHub-hosted run.
No new source data is automatically downloaded by the schedule.

## 6. Interpret the profile results

The main question is: which lighting, road-type and speed-limit profiles account for the largest numbers of fatal or serious reported collisions in Great Britain, 2021–2025? Use the ranked profile table in `reports/results.md` to identify groups for further investigation. It does not select interventions.

Each profile has two distinct percentages:

- Within-profile severity: severe collisions divided by all recorded collisions in that profile.
- Overall concentration: severe collisions in that profile divided by all severe collisions in the snapshot.

Compare the overall concentration with the profile's share of all recorded collisions. The gap is in percentage points; it is not an exposure-adjusted risk estimate. Annual profiles repeat this comparison with each year's own totals. Counts distinguish a large recorded caseload from a small group with a high percentage.

### Within-profile percentage

The severity percentage is:

```text
100 × fatal or serious reported collisions / all reported collisions in the group
```

It is not a rate per kilometre travelled, a causal estimate or an adjusted official
trend. Compare counts as well as percentages, retain unknown groups and consult
the source severity guidance before interpreting changes between years.
