# DAT535 — STATS19 Road Collision Pipeline

**Faustine Caux and Jules Gombert · Group 16 · University of Stavanger**

This repository presents a PySpark Bronze → Silver → Gold pipeline for police-reported personal injury collisions in Great Britain, 2021–2025. Its main contribution is a validated, reusable **Silver layer** that preserves collision, vehicle and casualty grains, interprets official codes, and checks relationships before publication. A focused Gold analysis demonstrates its use.

## Submission materials

| Material | Contents |
|---|---|
| [Project report (PDF)](project/reports/pdf/report.pdf) | Six-page engineering report: data model, Silver preparation and validation, tests, analytical application, performance and limitations |
| [LaTeX source](project/reports/pdf/report.tex) | Editable report with [compilation instructions](project/reports/pdf/README.md) |
| [Project documentation](project/README.md) | Implementation, evidence and reproduction requirements |
| [Executed notebook](project/notebooks/analysis.ipynb) | Analytical tables, figures and interpretation |
| [Silver quality evidence](project/reports/silver_quality.json) | Recorded counts, schemas, missing values, relationship checks and Parquet validation |
| [Analytical results](project/reports/results.md) | Ranked profiles, denominator definitions and annual checks |
| [Performance evidence](project/reports/performance.json) | Measured CSV/Parquet comparison and [saved plans](project/reports/parquet_plan.txt) |
| [Project definition](project/project_definition.md) | Scope, analytical question and design objectives |

## Main results

The reference Silver run retains all **513,801 collisions, 937,265 vehicles and 652,821 casualties**. No missing or duplicate keys, unmatched relationships or discrepancies with declared child counts were found. Source codes remain available alongside guide-derived labels and prepared columns; missing-age codes become null only in the clean age columns. The saved test suite records **17 successful tests**, including a failed-update integration test.

Gold answers which lighting, road-type and speed-limit profiles account for the most fatal or serious reported collisions. Daylight / single carriageway / 30 mph contains **35,130 severe collisions (28.247% of all severe cases)**. The five leading profiles account for **62.725%** and retain the same ranks in each observed year. Counts and within-profile severity use different denominators. These findings describe recorded cases; they do not estimate exposure-adjusted risk or causal effects.

## Repository organisation

```text
project/
├── scripts/          # Bronze, Silver, Gold, orchestration, tests and rendering
├── config/           # Development and production paths
├── data/bronze/      # Versioned metadata and reference guide; raw CSVs ignored
├── notebooks/        # Executed analytical notebook
├── reports/          # PDF, figures, quality/results evidence and performance plans
└── README.md         # Technical documentation and reproduction
.github/workflows/    # STATS19 checks and production execution; manual legacy labs
```

The root-level lab notebooks and `lab*_pipeline.py` / `run_pipeline.py` scripts are retained course exercises. The assessed STATS19 implementation is under `project/`.

## Reproduction and execution

The documented VM environment uses Python 3.11, Spark 3.5.0 and Java 8. The fixture suite requires Spark and the versioned guide, but no large raw CSVs:

```bash
cd project
bash scripts/run_tests.sh
```

Full processing additionally requires the three externally retained CSVs at the paths documented in the [source setup](project/README.md#source-data-and-environment). Raw and generated layer datasets are excluded from Git. The PDF, executed notebook and exported evidence can be inspected without processing the raw snapshot. [Reproduction instructions](project/README.md#reproduction-commands) describe the complete commands and path overrides.

The development workflow runs small tests on `dev` pushes, same-repository pull requests targeting `dev`/`main`, and manual dispatch. Production runs tests and the full pipeline on `main` pushes or manual dispatch using the VM snapshot. No periodic schedule is currently enabled. A retained production manifest records a successful run; GitHub protection and approval settings are separate from the versioned workflow definitions.

## Data source and attribution

Source: UK Department for Transport, [STATS19 road safety open data](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data). File hashes and source links are recorded in the [Bronze metadata](project/data/bronze/stats19/metadata/source_catalog.json). Original retrieval dates are unknown; reported severity is unadjusted and traffic exposure is unavailable.

Contains public sector information licensed under the Open Government Licence v3.0.
