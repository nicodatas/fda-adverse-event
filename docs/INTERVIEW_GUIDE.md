# Interview Guide — FDA Adverse Event Analytics Pipeline

This guide prepares you to defend every engineering decision in this repo.
Each section is a question an interviewer is likely to ask, followed by the
answer you should be able to give from memory. Anything in **bold** is the
one sentence to remember.

---

## 1. The 60-second pitch

> "I built a batch ETL pipeline that ingests FDA drug adverse event reports
> from the openFDA API into Databricks using a medallion architecture:
> bronze lands raw JSON, silver parses it into three normalized Delta tables
> with explicit grains and hard data-quality gates, and gold pre-aggregates
> for dashboards. Everything is idempotent — silver uses version-aware Delta
> MERGE with child-table sync — and the whole transformation logic is covered
> by a 50-test pytest suite that runs a local Spark + Delta session in CI,
> without any Databricks workspace."

---

## 2. Architecture

**Why a medallion architecture?**

Bronze keeps the immutable raw truth (you can always replay), silver is the
conformed, validated business grain, gold is pre-aggregated for analytics.
Each layer has one job, so failures are isolated and reprocessing is cheap.

**Why three separate silver tables instead of one wide table?**

Reports, drugs, and reactions have different grains. A report has many drugs
AND many reactions — one wide table would create a drug×reaction Cartesian
product. Splitting them by grain (with `report_id` as the join key) keeps
counts honest and lets each table enforce its own quality gates.

**Why not partition the silver tables?**

The dataset is small (~55K rows). Partitioning by `report_date` would create
many tiny files with no pruning benefit at this scale. If volume grew, liquid
clustering on `report_date` would be the next step, not static partitions.

**Why Delta MERGE instead of INSERT OVERWRITE?**

Bronze accumulates every ingestion run, so silver re-reads overlapping data
across runs. MERGE on the table's natural key upserts new rows and updates
existing ones without duplicating. INSERT OVERWRITE would also work (silver
is recomputed from full bronze history), but MERGE keeps the target table
continuously consistent and keeps write volume proportional to the change.

---

## 3. Idempotency & MERGE

**What happens if you run silver twice on the same bronze data?**

Nothing changes. The source is deduplicated on the merge key
(`dropDuplicates`), the MERGE matches rows by the table's grain
(`report_id`, or `report_id` + `drug_name`/`adverse_reaction`), and
`whenMatchedUpdateAll` rewrites identical values. This is proven by
`test_reports_merge_is_idempotent` and `test_end_to_end_version_flow` (which
runs the whole merge sequence twice).

**Why dedupe the source inside merge_to_delta?**

If two source rows match the same target row, Delta throws
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. The same drug can
legitimately appear twice in one report (Suspect and Concomitant roles), so
the source is collapsed to the table grain first.

**Why is the gold layer written with overwrite, not MERGE?**

Gold tables are pure functions of silver — recomputed fully each run. A full
overwrite is simpler, provably correct (no drift), and idempotent by
construction. MERGE would only pay off if gold were incremental.

---

## 4. Report versioning & child-table sync

**What is safetyreportversion and why does it matter?**

openFDA can carry multiple versions of the same report — a follow-up version
may add or remove drugs/reactions. If silver kept every version, the same
report would produce duplicate report rows and, worse, stale child rows
(drugs removed in v2 would still be there from v1).

**How do you handle it?**

1. `extract_reports` keeps `report_version` (default 0 for legacy records).
2. `keep_latest_version` collapses to one row per `report_id` — highest
   version wins, ties broken by latest `report_date` (window + `row_number`).
3. `keep_children_of_latest_reports` keeps only child rows tagged with the
   latest version of their report.
4. The reports MERGE has a guarded update:
   `source.report_version >= coalesce(target.report_version, 0)` — a
   late-arriving OLDER version can never overwrite a newer target row.
5. Child MERGEs run with `whenNotMatchedBySourceDelete` — target child rows
   absent from the source are deleted, so a drug removed in v2 is purged.

**Why is whenNotMatchedBySourceDelete safe here but dangerous in general?**

Because silver re-reads the COMPLETE bronze history every run, the source
always represents the full desired state of the child table. If silver were
incremental (processing only new files), delete-by-source would wipe rows from
runs it did not see — that assumption is documented in the silver notebook
header and would be the first thing to change when moving to incremental
processing.

---

## 5. Data quality

**What are your hard gates and why those?**

- null `report_id` — the report cannot be joined to anything; grain violation.
- duplicate `report_id` in `fda_reports` — violates one-row-per-report grain.
- orphan drugs/reactions — child rows with no parent report; referential
  integrity.

They raise `RuntimeError`, the job's `ALL_SUCCESS` dependency skips gold, and
nothing wrong ships downstream. **Fail fast, poison late.**

**What stays a soft warning?**

Null report dates, unknown sex codes, ages outside 0–120, blank names. These
are real-world data realities in FAERS; blocking on them would make the
pipeline useless. They are logged per metric so an analyst can trace them.

**Why check quality on the tables, not the batch?**

The gates validate the persisted silver tables (post-merge state), not just
the current batch — a merge could introduce duplicates via a bad source even
if the batch itself was clean. The checks also compute in ONE aggregate action
per table instead of one Spark job per metric.

---

## 6. Security

**How does the API key leak, and how did you fix it?**

openFDA only accepts the API key as a URL query parameter. `requests` embeds
the full URL in exception messages, so ANY unhandled `requests` exception
(ConnectionError, HTTPError, Timeout) that gets logged or re-raised leaks the
key into logs and tracebacks.

Fixes: every message is passed through `redact_secrets()` (which replaces the
key with `***REDACTED***`) before logging or raising, and every raise uses
`from None` so the original exception (with the key in its message) is never
chained into the displayed traceback. Proven by
`test_api_key_never_leaks_in_logs_or_exception` and
`test_raised_error_hides_original_exception`.

**What did the old code do wrong?**

It used `except Exception` and re-raised the raw exception — retrying even on
permanent 4xx errors and leaking the key on every failure path.

---

## 7. Testing strategy

**How can you test Databricks notebooks? They can't be imported.**

The notebooks are plain `.py` files, but importing them would execute
Databricks-specific side effects (Spark session from the platform, volume
reads, CREATE TABLE DDL). Instead, `tests/conftest.py` parses the notebook
source with `ast`, extracts ONLY the function definitions (plus selected
constants like `RETRY_ATTEMPTS`), and exec's them into a controlled namespace
seeded with the imports they need (`pyspark.sql.functions`, `requests`, ...).
Tests can then call e.g. `extract_reports()` directly.

**What runs where?**

- Bronze tests: pure Python — `requests.get` is faked via monkeypatch. No
  Spark, no network.
- Silver/gold tests: a local `local[2]` Spark session with Delta Lake
  (`configure_spark_with_delta_pip`), real Delta MERGEs against throwaway
  databases in a temp warehouse.

**What is covered?**

- Bronze (14): every pagination end condition (short page, 404, empty page),
  404-on-first-page as config error, retry-on-5xx, backoff sleeps, 401
  fail-fast, malformed JSON, bad response structure, key redaction, exception
  chaining.
- Silver transforms (18): country/sex normalization, null-id drops, version
  defaults, all NCIT age units, blank drug/reaction filtering, latest-version
  collapse, tie-breaking, child filtering.
- Silver MERGE (7): idempotency, source dedup, old-version guard, same-version
  update, child delete sync, partial-source safety, full end-to-end flow run
  twice.
- DQ gates (6): each hard gate raises, clean pass, soft warnings log-but-don't
  block, metric computation on clean/dirty tables.
- Gold (5): unique-report counts by country/drug/month, fan-out safety, avg
  age null handling, null-date exclusion + warning log, ordering.

---

## 8. CI/CD

**What does CI do?**

Three jobs on every push/PR:

1. **lint** — flake8 over `notebooks/` and `tests/`. The config ignores only
   E203/W503 (formatter conflicts) and W292 (notebook export newline);
   previously it also ignored F401, F821, F811 — real error codes that hid
   broken code.
2. **test** — pytest on Ubuntu + Temurin JDK 17 (Spark needs a JVM).
3. **validate-bundle** — `databricks bundle validate`, degrading gracefully to
   a notice when workspace secrets are absent.

**Why fix notebook_path in databricks.yml?**

The paths had a leading slash (`/notebooks/...`), which the CLI resolves
against the workspace root instead of the bundle root — deployment would fail
to find the notebooks. Bundle-relative paths must not start with `/`.

---

## 9. Known limitations & trade-offs

Be honest about these — interviewers respect it:

- **Full-history re-read.** Silver re-processes all of bronze every run. Fine
  at ~55K rows; at millions you'd add incremental processing (Auto Loader /
  streaming tables, or process only new `run_id`s) — and then
  `whenNotMatchedBySourceDelete` would need to become explicit sync logic.
- **5,000-report sample per ingestion run.** Deterministic slice, chosen for
  demo cost predictability; scaling is a config change (MAX_PAGES/API key).
- **No schema registry / contract tests on bronze JSON.** Spark infers the
  schema; malformed records become nulls. A production system would pin the
  openFDA schema.
- **Gold is unpartitioned.** Same small-data reasoning as silver.
- **API key via query param is openFDA's only option** — redaction mitigates
  exposure in logs but the key still transits URLs. A proxy or workspace
  secret-backed header injection would be needed elsewhere.

---

## 10. Quick-fire Q&A

- **Grain of fda_report_drugs?** One row per (report_id, drug_name).
- **Why countDistinct and not count?** Defense-in-depth: the join is on a
  unique key so count(*) would be correct today, but countDistinct(report_id)
  keeps gold correct even if the child grain ever changes.
- **What breaks if bronze deletes old runs?** Nothing in silver's correctness
  for reports already merged (MERGE never deletes parent rows), but
  child delete-by-source would start purging children of reports no longer in
  bronze — the full-history assumption again.
- **Why is report_version 0 for missing values?** Legacy records predate the
  field; 0 makes them comparable and guarantees any real version (1+) wins.
- **Hour age unit (805) not converted?** It applies to neonates; converting to
  years produces near-zero values that would distort averages.
- **What would you do first at 100× scale?** Incremental silver (process only
  new bronze runs), liquid clustering on report_date, and partition the
  bronze volume reads by ingestion_date.
