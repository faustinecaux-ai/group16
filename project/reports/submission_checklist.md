# DAT535 — Delivery Evidence Checklist

This checklist distinguishes local evidence from configuration awaiting a remote
demonstration. It is not a claim that repository hosting settings are configured.

| Requirement | Evidence | Status |
|---|---|---|
| Concise six-section definition | `../project_definition.md` | Written |
| Source, scope and limitations | Definition, report, source catalog | Documented |
| Unchanged Bronze snapshot and hashes | Bronze run manifests and versioned baseline | Verified locally |
| Separate collision, vehicle and casualty grains | Silver schemas and aggregation tests | Verified locally |
| Cleaning, decoding and traceability | Source codes retained; age/speed clean columns; guide hash | Implemented |
| Before/after quality metrics | Per-run JSON reports; dated quality notes | Generated |
| DataFrame and Spark SQL APIs | Silver DataFrames; monthly Gold SQL | Implemented |
| Performance evidence | `performance.json`, CSV/Parquet plans | Measured on one VM |
| Gold tied to an analytical consumer | Ten tables including pooled and annual profiles, CSVs, notebook and charts | Implemented |
| Quality-gated publication | Failed-update integration test and atomic pointers | Tested locally |
| Reproducible full run | `bash scripts/run_project.sh --env dev` | Demonstrated locally |
| Development/production separation | `../config/dev.json`, `../config/prod.json` | Configured |
| Automated checks on GitHub | Dev workflow and locally tested command | Awaiting commit/push and remote run |
| Production and scheduled execution | Main workflow, monthly cron | Configured; remote execution pending |
| Required checks and environment approvals | GitHub repository settings | Not verified |
| Report evidence and reflection | `project_report.md`, results, notebook, tests | Draft ready for student review |

Before submission, read and explain the implementation, verify the first GitHub
run, configure repository protection where available, and record any deployment
approval or scheduled-run evidence required by the instructor. Keep the limits
and incomplete provenance visible rather than presenting them as resolved.
