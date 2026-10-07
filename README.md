# FDA Adverse Event Analytics Pipeline

A batch ETL pipeline that ingests FDA adverse event reports via the openFDA API, processes them through a medallion architecture, and publishes analytics through a Lakeview dashboard. Built on Databricks with Delta Lake, Unity Catalog, and orchestrated with a Declarative Automation Bundle.

*(Dashboard screenshot not included in repo)*

**[Live Dashboard](https://dbc-a4bd6c77-a376.cloud.databricks.com/dashboardsv3/01f1c1f6d2611bda9a1ade2a711c3ed2/published?o=7474648475439044)**

The dashboard visualizes 5,000 FDA adverse event reports across 3,293 drugs, 65 countries, and 2,123 unique reactions. It features KPI counters, bar charts for top drugs/reactions/countries, a temporal trend line, patient demographic breakdowns by sex and age group, and a ranked drug table with average patient age.

> **Note**: FAERS data is based on voluntary reporting. Report counts don't establish causation or relative drug safety.

## Architecture

The pipeline follows a standard medallion pattern:

**Bronze** (`01_bronze_ingest`): Fetches raw JSON from the openFDA API with pagination, retry logic, and streaming writes. Each run gets a unique `run_id` timestamp so re-runs append rather than overwrite.

**Silver** (`02_silver_transform`): Parses JSON into three normalized Delta tables:
- `fda_reports` (one row per report)
- `fda_report_drugs` (one row per drug per report)
- `fda_report_reactions` (one row per reaction per report)

The transform handles array explosions, normalizes patient age from multiple NCIT unit codes, and applies hard quality gates (null keys, duplicates, orphan records). Failed quality checks raise an exception and skip downstream tasks.

**Gold** (`03_gold_aggregate`): Pre-aggregates reports by country, drug, and reaction over time using `countDistinct(report_id)` to avoid inflating counts. Tables include Unity Catalog comments and FAERS reporting disclaimers.

**Idempotency**: Silver uses Delta MERGE with composite keys so re-running the pipeline on the same data is safe. Bronze preserves all ingestion runs; silver deduplicates on grain.

## Data Model

**Silver**: Three normalized tables
- `fda_reports`: One row per report (`report_id`)
- `fda_report_drugs`: One row per drug per report (`report_id`, `drug_name`)
- `fda_report_reactions`: One row per reaction per report (`report_id`, `adverse_reaction`)

**Gold**: Pre-aggregated analytics tables
- `fda_reports_by_country`: Unique reports by country
- `fda_top_drugs`: Drug rankings with average patient age
- `fda_reactions_over_time`: Monthly reaction trends

## Running the Pipeline

Clone the repo into a Databricks Git folder and run notebooks in sequence (Bronze → Silver → Gold), or deploy the job via the Declarative Automation Bundle:

```bash
databricks bundle deploy
```

Requires:
- Databricks workspace with Unity Catalog
- UC Volume: `workspace.default.fda_data`
- Optional: openFDA API key as `FDA_API_KEY` environment variable

The bundle configures daily execution. Without an API key, the pipeline falls back to unauthenticated requests (500 records/page instead of 1000).

## Data Quality

The silver transform enforces hard quality gates:
- Null or duplicate `report_id` (violates grain)
- Orphan drug/reaction records (referential integrity)

Failed gates raise `RuntimeError` and skip downstream tasks. Warnings (null dates, invalid age) are logged but don't block execution.

## Dashboard

The AI/BI Lakeview dashboard provides a single-page overview of adverse event data:

- **KPI Counters**: Total Reports (5,000), Unique Drugs (3,293), Unique Countries (65), Unique Reactions (2,123)
- **Top 10 Drugs by Report Volume**: Horizontal bar chart ranking the most frequently reported drugs (LETAIRIS leads with 1,278 reports)
- **Top 10 Countries by Reports**: Bar chart showing geographic distribution (US dominates with 3,820 reports)
- **Top 15 Adverse Reactions**: Horizontal bar chart of most common reactions (Dyspnoea, Type 2 Diabetes Mellitus, Drug Ineffective)
- **Patient Sex Distribution**: Donut chart showing female vs. male vs. unknown breakdown (64% female, 29% male, 7% unknown)
- **Reports Over Time**: Line chart tracking monthly report volume from 2008 to 2015
- **Age Group Distribution**: Bar chart showing patient age brackets (60-74 is the largest group at 1,134 reports)
- **Top 10 Drugs with Avg Patient Age**: Data table combining drug rankings with average patient age

> **Note**: FAERS data is voluntary — report frequency does not imply causality or relative drug safety.

## Tech Stack

- **Databricks** (serverless compute, Delta Lake)
- **PySpark** (distributed transforms)
- **Unity Catalog** (governance, lineage)
- **openFDA API** (data source)
- **Declarative Automation Bundles** (orchestration)