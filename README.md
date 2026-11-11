# FDA Adverse Event Analytics Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

A batch data pipeline that ingests a capped sample of drug adverse-event reports from the **openFDA API**, transforms them through **Bronze → Silver → Gold** Delta tables on Databricks, and prepares analytics for an AI/BI dashboard.

**[Live dashboard (requires Databricks access)](https://dbc-a4bd6c77-a376.cloud.databricks.com/dashboardsv3/01f1c1f6d2611bda9a1ade2a711c3ed2/published?o=7474648475439044)**

> Dashboard screenshot: not published yet. The live link requires an authenticated Databricks workspace.

## Overview

| Metric | Value |
|---|---|
| Reports analyzed | 5,000 |
| Unique drugs | 3,293 |
| Countries | 65 |
| Unique reactions | 2,123 |
| Period covered | 2008 – 2015 |

> **Disclaimer:** FAERS data is based on voluntary reporting. Report counts do not establish causation or relative drug safety.

## Architecture

```mermaid
flowchart LR
    A[openFDA API] -->|pagination + retries| B[(Bronze<br/>raw JSON)]
    B -->|parse, explode, quality gates| C[(Silver<br/>normalized Delta tables)]
    C -->|aggregate| D[(Gold<br/>analytics tables)]
    D --> E[Lakeview Dashboard]
```

| Layer | Notebook | Responsibility |
|---|---|---|
| **Bronze** | `01_bronze_ingest` | Fetches raw JSON from the openFDA API with pagination (HTTP 404 = clean end of results), transient-only retries (429/5xx) with exponential backoff, request timeouts, streaming writes, and API-key redaction in every logged or raised message. Each run is tagged with a unique `run_id` timestamp, so re-runs append rather than overwrite. |
| **Silver** | `02_silver_transform` | Parses JSON into three normalized Delta tables with explicit grains, normalizes patient age from multiple NCIT unit codes, keeps only the latest `safetyreportversion` of each report, synchronizes drug/reaction child rows via `whenNotMatchedBySourceDelete`, and enforces hard quality gates. |
| **Gold** | `03_gold_aggregate` | Pre-aggregates by country, drug, and reaction over time using `countDistinct(report_id)` to avoid inflated counts from join fan-out; reports with unparseable dates are excluded from the monthly trend and logged. Tables include Unity Catalog comments and FAERS disclaimers. |

## Data Model

**Silver** (normalized)

| Table | Grain |
|---|---|
| `fda_reports` | One row per report (`report_id`) |
| `fda_report_drugs` | One row per normalized drug per report (`report_id`, `drug_name`); role conflicts resolve to Suspect (1), then Concomitant (2), then Interacting (3) |
| `fda_report_reactions` | One row per reaction per report (`report_id`, `adverse_reaction`) |

**Gold** (pre-aggregated)

| Table | Description |
|---|---|
| `fda_reports_by_country` | Unique reports by country |
| `fda_top_drugs` | Suspect-drug rankings (role 1) with average patient age |
| `fda_reactions_over_time` | Monthly reaction trends |

## Engineering Highlights

- **Idempotent by design.** Each Delta table MERGE is atomic on its own; the three Silver MERGEs are not one cross-table transaction. The Silver task is retried and can be safely replayed from the full Bronze history to converge after a partial failure. Bronze preserves every ingestion run.
- **Version-aware upserts.** openFDA reports carry `safetyreportversion`; silver keeps only the highest version of each report, and only a strictly newer version can update an existing report; equal versions are immutable, and conflicting same-version payloads fail before any write.
- **Child-table sync.** When an updated report version drops a drug or reaction, the child MERGE (`whenNotMatchedBySourceDelete`) removes the stale target row. This operation relies on Silver reading the complete append-only Bronze history on every run.
- **Data quality checks.** Before any Silver write, the pipeline validates raw report IDs and rejects conflicting payloads that share the same report ID/version. Blank drug/reaction terms are counted and warned about before extraction filters them. Table-level grain and referential-integrity gates currently run after the three Silver MERGEs; a failure blocks Gold but does not roll back already-written Silver tables. Moving these gates before the MERGEs is planned work.
- **Soft warnings.** Null dates, invalid sex/age values, and blank names are logged as warnings without blocking execution.
- **Secret hygiene.** openFDA only accepts the API key as a URL query parameter, so `requests` exception messages embed it. Bronze redacts every logged or raised message and never re-raises the original exception (`raise ... from None`).
- **Resilient ingestion.** HTTP 404 past the first page is treated as a clean end-of-pagination; only transient errors (429, 5xx, network) are retried; permanent 4xx fails fast. Without an API key the pipeline falls back to unauthenticated requests (500 records per page instead of 1,000).
- **Tested.** Pytest covers bronze API error paths and key redaction, silver normalization/version selection/raw quality gates/Delta MERGE semantics, and gold aggregation fan-out safety, including suspect-only drug rankings. CI runs lint and the local Spark/Delta integration suite.
- **Governed tables.** Unity Catalog provides governance and lineage, with documented Gold tables.

## Design decisions

- **Separate entity grains.** Reports, drugs, and reactions are stored separately to avoid a drug × reaction Cartesian fan-out.
- **Version-aware updates.** The highest report version wins; only a strictly newer version updates an existing report.
- **Replayable Silver.** Silver currently reads the complete append-only Bronze history on every run. Per-table Delta MERGEs are idempotent, but the three-table update is not one transaction.
- **Recovery after partial writes.** The Silver task retries and replays full Bronze history so child-table synchronization can converge after a partial failure.
- **Suspect-only drug ranking.** Gold ranks only role 1 (Suspect) drugs; concomitant and interacting drugs are excluded.

## Limitations

- This is a batch portfolio project, not a production pharmacovigilance system.
- Bronze currently ingests a capped 5,000-report sample per run. The scheduled job therefore does not provide incremental historical coverage yet.
- Silver reprocesses the full Bronze history on every run; incremental ingestion requires a compatible child-table synchronization strategy.
- Table-level duplicate and orphan gates currently run after the Silver MERGEs. A failure blocks Gold but does not roll back Silver writes.
- The local Spark/Delta suite validates transformation and MERGE behavior, but it does not prove that the complete pipeline has run successfully in every Databricks workspace configuration.
- Databricks bundle validation requires workspace credentials; without them the CI job reports a skip.

## Dashboard

A single-page overview of the adverse event data:

- **KPI counters:** total reports, unique drugs, countries, and reactions
- **Top 10 suspect drugs by report volume** (LETAIRIS leads with 1,278 reports)
- **Top 10 countries by reports** (US leads with 3,820)
- **Top 15 adverse reactions** (most common: Dyspnoea, Type 2 Diabetes Mellitus, Drug Ineffective)
- **Patient sex distribution** (64% female, 29% male, 7% unknown)
- **Monthly report volume**, 2008 – 2015
- **Age group distribution** (60–74 is the largest group, with 1,134 reports)
- **Top 10 drugs with average patient age**

## Getting Started

**Requirements**

- Databricks workspace with Unity Catalog
- Unity Catalog Volume: `workspace.default.fda_data`
- *(Optional)* openFDA API key set as the `FDA_API_KEY` environment variable

**Option 1: Run manually.** Clone the repo into a Databricks Git folder and run the notebooks in order: Bronze → Silver → Gold.

**Option 2: Deploy as a job.** The bundle defines a daily schedule (currently paused by default):

```bash
databricks bundle deploy
```

**Local development & tests.** The test suite runs Spark locally — no Databricks workspace needed:

```bash
pip install -r requirements-dev.txt   # pytest, flake8, pyspark, delta-spark
pytest -v                             # JDK 17 required for the Spark tests
flake8 notebooks/ tests/               # max-line-length 120; ignores E203, W503 and W292
```

Tests extract the transformation functions from the notebook sources via AST (so no Databricks session is needed) and run them against a local Spark + Delta session. Coverage includes raw-input rejection before filters, conflicting report-version detection, deterministic drug-role resolution, version-aware MERGE, child-table synchronization, and gold aggregation fan-out safety.

**CI (GitHub Actions)** runs three jobs on every push and pull request: flake8 lint over notebooks and tests, the pytest suite on Ubuntu with Temurin JDK 17, and `databricks bundle validate` (skipped with a notice when workspace credentials are not configured).

## Tech Stack

| Component | Technology |
|---|---|
| Compute & storage | Databricks (serverless), Delta Lake |
| Processing | PySpark |
| Governance | Unity Catalog |
| Orchestration | Declarative Automation Bundles |
| Source | openFDA API |
| Visualization | Databricks AI/BI (Lakeview) |

## Roadmap

- [x] Automated tests for the transform logic (pytest, local Spark + Delta)
- [x] CI with GitHub Actions (lint, tests, bundle validation)
- [ ] Orchestrate with Apache Airflow as an alternative to bundles
- [ ] Add incremental ingestion with a persisted receivedate watermark
- [ ] Move table-level duplicate/orphan gates before the Silver MERGEs
- [ ] Expand ingestion beyond the current 5,000-report sample
- [x] Rank only suspect drugs (role 1) in Gold

## Author

**Nicolás Romero**, Junior Data Engineer
[LinkedIn](https://www.linkedin.com/in/nicolasromero-data) · [GitHub](https://github.com/nicodatas)
