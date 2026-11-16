# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Silver Layer Title
# MAGIC %md
# MAGIC # Silver Layer — Cleansed & Conformed FDA Events
# MAGIC
# MAGIC Reads raw JSON from the bronze volume and builds three separate Delta tables with explicit grains:
# MAGIC
# MAGIC - `workspace.silver.fda_reports` — one row per FDA safety report (report-level attributes)
# MAGIC - `workspace.silver.fda_report_drugs` — one row per drug within a report
# MAGIC - `workspace.silver.fda_report_reactions` — one row per adverse reaction within a report
# MAGIC
# MAGIC Drugs and reactions are extracted independently to avoid a drug×reaction Cartesian product.
# MAGIC The `report_id` column links the three entities. No drug-to-reaction causal relationship is implied.
# MAGIC Drug-role conflicts for the same normalized drug resolve deterministically: Suspect (1),
# MAGIC then Concomitant (2), then Interacting (3).
# MAGIC
# MAGIC **Report versioning:** openFDA reports carry `safetyreportversion`. Identical copies are expected in
# MAGIC append-only Bronze, but conflicting payloads with the same report ID/version fail before writes.
# MAGIC Silver keeps the highest version; only a strictly newer version may update an existing report.
# MAGIC Child rows (drugs/reactions) are kept only from the latest version of each report, and the child
# MAGIC MERGE deletes target rows that are no longer present in the source (child-table sync).
# MAGIC
# MAGIC **Assumption and recovery:** silver re-reads the COMPLETE bronze history on every run (bronze files
# MAGIC are append-only). Each Delta MERGE is atomic individually, not across all three tables. A task retry
# MAGIC replays the full history to converge after a partial multi-table write.

# COMMAND ----------

# DBTITLE 1,Silver — Transform & MERGE

from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================
# SILVER - CELL 1
# Read Bronze and extract FDA reports
# ============================================

BRONZE_PATH = (
    "/Volumes/workspace/default/fda_data/bronze/"
    "ingestion_date=*/run_id=*/page_*.json"
)

print("Reading Bronze JSON files...")

# Read original Bronze data
raw_envelopes = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

# Preserve source lineage
raw_envelopes = (
    raw_envelopes
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file")
    )
    .withColumn(
        "_run_id",
        F.regexp_extract(
            F.col("_source_file"),
            r"run_id=([^/]+)",
            1
        )
    )
    .withColumn(
        "_page",
        F.regexp_extract(
            F.col("_source_file"),
            r"(page_[^/]+\.json)",
            1
        )
    )
    .withColumn(
        "_api_last_updated",
        F.col("meta.last_updated")
    )
)

# Extract individual reports from each API response
df_reports = (
    raw_envelopes
    .select(
        "_source_file",
        "_run_id",
        "_page",
        "_api_last_updated",
        F.explode_outer("results").alias("report")
    )
    .select(
        "_source_file",
        "_run_id",
        "_page",
        "_api_last_updated",
        "report.*"
    )
)

# Basic ingestion checks (no deduplication)
print("Bronze envelopes:", raw_envelopes.count())
print("Extracted reports:", df_reports.count())
print("Distinct report IDs:", df_reports.select("safetyreportid").distinct().count())

print("Extracted schema:")
df_reports.printSchema()

display(df_reports.limit(10))


# COMMAND ----------

# DBTITLE 1,Silver — Data Quality Validation

from pyspark.sql import functions as F

# ============================================================
# SILVER - CELL 2
# Data Quality Validation
# ============================================================

REPORTS_TABLE = "workspace.silver.fda_reports"
DRUGS_TABLE = "workspace.silver.fda_report_drugs"
REACTIONS_TABLE = "workspace.silver.fda_report_reactions"

# Read existing Silver tables
df_reports_silver = spark.table(REPORTS_TABLE)
df_drugs = spark.table(DRUGS_TABLE)
df_reactions = spark.table(REACTIONS_TABLE)

# ------------------------------------------------------------
# 1. Validate required columns
# ------------------------------------------------------------

required_columns = {
    "reports": {
        "report_id",
        "report_date",
        "patient_sex",
        "patient_age_years"
    },
    "drugs": {
        "report_id",
        "drug_name"
    },
    "reactions": {
        "report_id",
        "adverse_reaction"
    }
}

dataframes = {
    "reports": df_reports_silver,
    "drugs": df_drugs,
    "reactions": df_reactions
}

for name, required in required_columns.items():
    missing = required - set(dataframes[name].columns)

    if missing:
        raise ValueError(
            f"{name}: missing required columns: {sorted(missing)}"
        )

# ------------------------------------------------------------
# 2. Reports quality metrics
# ------------------------------------------------------------

reports_metrics = (
    df_reports_silver
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(F.col("report_id").isNull(), 1).otherwise(0)
        ).alias("null_report_ids"),
        F.sum(
            F.when(F.col("report_date").isNull(), 1).otherwise(0)
        ).alias("null_dates"),
        F.sum(
            F.when(
                F.col("patient_sex").isNotNull()
                & ~F.col("patient_sex").isin(
                    "MALE", "FEMALE", "UNKNOWN"
                ),
                1
            ).otherwise(0)
        ).alias("invalid_sex"),
        F.sum(
            F.when(
                F.col("patient_age_years").isNotNull()
                & (
                    (F.col("patient_age_years") < 0)
                    | (F.col("patient_age_years") > 120)
                ),
                1
            ).otherwise(0)
        ).alias("invalid_age")
    )
    .first()
    .asDict()
)

duplicate_report_ids = (
    df_reports_silver
    .groupBy("report_id")
    .count()
    .filter(
        (F.col("report_id").isNotNull())
        & (F.col("count") > 1)
    )
    .count()
)

# ------------------------------------------------------------
# 3. Drug quality metrics
# ------------------------------------------------------------

drugs_metrics = (
    df_drugs
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("drug_name").isNull()
                | (F.trim(F.col("drug_name")) == ""),
                1
            ).otherwise(0)
        ).alias("empty_drug_names")
    )
    .first()
    .asDict()
)

# Exact duplicate rows, not repeated drug names.
# A report can legitimately contain the same drug more than once.
duplicate_drug_rows = (
    df_drugs
    .groupBy(*df_drugs.columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# ------------------------------------------------------------
# 4. Reaction quality metrics
# ------------------------------------------------------------

reactions_metrics = (
    df_reactions
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("adverse_reaction").isNull()
                | (F.trim(F.col("adverse_reaction")) == ""),
                1
            ).otherwise(0)
        ).alias("empty_reaction_names")
    )
    .first()
    .asDict()
)

# Exact duplicate rows, not repeated reaction names.
duplicate_reaction_rows = (
    df_reactions
    .groupBy(*df_reactions.columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# ------------------------------------------------------------
# 5. Referential integrity
# ------------------------------------------------------------

orphan_drugs = (
    df_drugs
    .filter(F.col("report_id").isNotNull())
    .join(
        df_reports_silver.select("report_id").distinct(),
        on="report_id",
        how="left_anti"
    )
    .count()
)

orphan_reactions = (
    df_reactions
    .filter(F.col("report_id").isNotNull())
    .join(
        df_reports_silver.select("report_id").distinct(),
        on="report_id",
        how="left_anti"
    )
    .count()
)

# ------------------------------------------------------------
# 6. Consolidate metrics
# ------------------------------------------------------------

dq_metrics = {
    "total_reports": reports_metrics["total_rows"],
    "null_report_ids": reports_metrics["null_report_ids"] or 0,
    "duplicate_report_ids": duplicate_report_ids,
    "null_dates": reports_metrics["null_dates"] or 0,
    "invalid_sex": reports_metrics["invalid_sex"] or 0,
    "invalid_age": reports_metrics["invalid_age"] or 0,

    "total_drugs": drugs_metrics["total_rows"],
    "empty_drug_names": drugs_metrics["empty_drug_names"] or 0,
    "exact_duplicate_drug_rows": duplicate_drug_rows,

    "total_reactions": reactions_metrics["total_rows"],
    "empty_reaction_names": reactions_metrics["empty_reaction_names"] or 0,
    "exact_duplicate_reaction_rows": duplicate_reaction_rows,

    "orphan_drugs": orphan_drugs,
    "orphan_reactions": orphan_reactions
}

# ------------------------------------------------------------
# 7. Print validation report
# ------------------------------------------------------------

print("=" * 70)
print("DATA QUALITY VALIDATION")
print("=" * 70)

print("\n-- fda_reports --")
print(f"Total rows:                 {dq_metrics['total_reports']:,}")
print(f"Null report IDs:            {dq_metrics['null_report_ids']}")
print(f"Duplicate report IDs:       {dq_metrics['duplicate_report_ids']}")
print(f"Null report dates:          {dq_metrics['null_dates']}")
print(f"Invalid patient sex:        {dq_metrics['invalid_sex']}")
print(f"Age out of range:           {dq_metrics['invalid_age']}")

print("\n-- fda_report_drugs --")
print(f"Total rows:                 {dq_metrics['total_drugs']:,}")
print(f"Empty drug names:           {dq_metrics['empty_drug_names']}")
print(f"Exact duplicate rows:       {dq_metrics['exact_duplicate_drug_rows']}")

print("\n-- fda_report_reactions --")
print(f"Total rows:                 {dq_metrics['total_reactions']:,}")
print(f"Empty reaction names:       {dq_metrics['empty_reaction_names']}")
print(f"Exact duplicate rows:       {dq_metrics['exact_duplicate_reaction_rows']}")

print("\n-- Referential integrity --")
print(f"Orphan drug records:        {dq_metrics['orphan_drugs']}")
print(f"Orphan reaction records:    {dq_metrics['orphan_reactions']}")

# ------------------------------------------------------------
# 8. Hard quality gates
# ------------------------------------------------------------

HARD_GATES = [
    "null_report_ids",
    "duplicate_report_ids",
    "orphan_drugs",
    "orphan_reactions"
]

failed_gates = {
    name: dq_metrics[name]
    for name in HARD_GATES
    if dq_metrics[name] > 0
}

if failed_gates:
    raise RuntimeError(
        f"DATA QUALITY GATE FAILED: {failed_gates}"
    )

print("\nAll hard quality gates passed.")

# ------------------------------------------------------------
# 9. Soft warnings
# ------------------------------------------------------------

SOFT_METRICS = [
    "null_dates",
    "invalid_sex",
    "invalid_age",
    "empty_drug_names",
    "empty_reaction_names",
    "exact_duplicate_drug_rows",
    "exact_duplicate_reaction_rows"
]

warnings = {
    name: dq_metrics[name]
    for name in SOFT_METRICS
    if dq_metrics[name] > 0
}

if warnings:
    print("\nDATA QUALITY WARNINGS:")
    for name, value in warnings.items():
        print(f"WARNING: {name} = {value}")
else:
    print("No soft quality warnings.")


# COMMAND ----------

# DBTITLE 1,Silver — Exploratory Statistics
# ─────────────────────────────────────────────────────────────────────────────
# Exploratory Statistics
# Analytical summaries for understanding data distributions.
# These are NOT pass/fail validations — they inform dashboard design.
#
# NOTE: FAERS reports are spontaneous. Report frequency does not imply
# causality or relative drug safety. These statistics describe reporting
# volume, not risk.
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 70)
print("EXPLORATORY STATISTICS")
print("=" * 70)

# Entity counts
unique_reports = df_reports.select("report_id").distinct().count()
unique_drugs = df_drugs.select("drug_name").distinct().count()
unique_reactions = df_reactions.select("adverse_reaction").distinct().count()

print(f"\nUnique safety reports:   {unique_reports:,}")
print(f"Unique drug names:        {unique_drugs:,}")
print(f"Unique adverse reactions: {unique_reactions:,}")

# Top countries — from fda_reports (one row per report, so count is correct)
print("\n-- Top 10 countries by report count --")
df_reports.groupBy("country") \
    .agg(count("*").alias("report_count")) \
    .orderBy(col("report_count").desc()) \
    .limit(10) \
    .show()

# Gender distribution — from fda_reports
print("\n-- Patient gender distribution --")
df_reports.groupBy("patient_sex") \
    .agg(count("*").alias("report_count")) \
    .orderBy(col("report_count").desc()) \
    .show()

# Top reactions — count unique reports per reaction
print("\n-- Top 15 adverse reactions (by unique report count) --")
df_reactions.groupBy("adverse_reaction") \
    .agg(countDistinct("report_id").alias("report_count")) \
    .orderBy(col("report_count").desc()) \
    .limit(15) \
    .show()

# Top drugs — count unique reports per drug, join reports for avg age
print("\n-- Top 10 suspect drugs by report count (with avg patient age) --")
df_drugs.filter(col("drug_characterization") == 1).join(df_reports, "report_id", "inner") \
    .groupBy("drug_name") \
    .agg(
        countDistinct("report_id").alias("report_count"),
        round(avg("patient_age_years"), 2).alias("avg_patient_age_years"),
    ) \
    .orderBy(col("report_count").desc()) \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_PATH = (
    "/Volumes/workspace/default/fda_data/bronze/"
    "ingestion_date=*/run_id=*/page_*.json"
)

print("=" * 65)
print("BRONZE vs SILVER — REPORT VERSION AUDIT")
print("=" * 65)

# 1. Read Bronze and extract only report identifiers and versions
bronze_raw = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

bronze_keys = (
    bronze_raw
    .select(
        F.explode_outer("results").alias("report"),
        F.col("_metadata.file_path").alias("source_file")
    )
    .select(
        F.col("report.safetyreportid").cast("string").alias("report_id"),
        F.col("report.safetyreportversion").cast("int").alias("report_version"),
        F.regexp_extract("source_file", r"run_id=([^/]+)", 1).alias("run_id")
    )
)

# 2. Read existing Silver reports
silver_reports = (
    spark.table("workspace.silver.fda_reports")
    .select(
        F.col("report_id").cast("string"),
        F.col("report_version").cast("int")
    )
)

# 3. Basic Bronze metrics
bronze_valid = bronze_keys.filter(
    F.col("report_id").isNotNull()
    & F.col("report_version").isNotNull()
)

bronze_unique_keys = bronze_valid.select(
    "report_id", "report_version"
).distinct()

bronze_multi_version = (
    bronze_unique_keys
    .groupBy("report_id")
    .agg(F.countDistinct("report_version").alias("versions"))
    .filter(F.col("versions") > 1)
)

duplicate_bronze_keys = (
    bronze_valid
    .groupBy("report_id", "report_version")
    .count()
    .filter(F.col("count") > 1)
)

# 4. Basic Silver metrics
silver_valid = silver_reports.filter(
    F.col("report_id").isNotNull()
    & F.col("report_version").isNotNull()
)

silver_unique_keys = silver_valid.select(
    "report_id", "report_version"
).distinct()

# 5. Compare distinct report/version keys
silver_keys_missing_in_bronze = (
    silver_unique_keys.join(
        bronze_unique_keys,
        ["report_id", "report_version"],
        "left_anti"
    )
)

bronze_keys_missing_in_silver = (
    bronze_unique_keys.join(
        silver_unique_keys,
        ["report_id", "report_version"],
        "left_anti"
    )
)

# 6. Print results
print("\n--- BRONZE ---")
print("Total extracted records:", bronze_keys.count())
print("Distinct report IDs:", bronze_valid.select("report_id").distinct().count())
print("Distinct report/version keys:", bronze_unique_keys.count())
print("Null ID or version records:", bronze_keys.filter(
    F.col("report_id").isNull() | F.col("report_version").isNull()
).count())
print("Keys repeated across Bronze records:", duplicate_bronze_keys.count())
print("Report IDs with multiple versions:", bronze_multi_version.count())
print("Distinct ingestion runs:", bronze_keys.select("run_id").distinct().count())

print("\n--- SILVER ---")
print("Total report rows:", silver_reports.count())
print("Distinct report IDs:", silver_valid.select("report_id").distinct().count())
print("Distinct report/version keys:", silver_unique_keys.count())

print("\n--- COMPARISON ---")
print("Silver keys NOT found in Bronze:", silver_keys_missing_in_bronze.count())
print("Bronze keys NOT represented in Silver:", bronze_keys_missing_in_silver.count())

print("\n--- SAMPLE: REPORTS WITH MULTIPLE VERSIONS ---")
bronze_multi_version.orderBy(
    F.col("versions").desc()
).show(10, truncate=False)

print("\nAUDIT COMPLETED — NO TABLES WERE MODIFIED")


# COMMAND ----------


from pyspark.sql import functions as F

print("=" * 65)
print("BRONZE vs SILVER — DISCREPANCY INVESTIGATION")
print("=" * 65)

# Rebuild Bronze keys with source information
bronze_investigation = (
    bronze_raw
    .select(
        F.explode_outer("results").alias("report"),
        F.col("_metadata.file_path").alias("source_file")
    )
    .select(
        F.col("report.safetyreportid").cast("string").alias("report_id"),
        F.col("report.safetyreportversion").cast("int").alias("report_version"),
        F.col("source_file"),
        F.regexp_extract(
            "source_file", r"run_id=([^/]+)", 1
        ).alias("run_id")
    )
)

# A. Find the Silver key absent from Bronze
missing_silver = (
    silver_unique_keys.join(
        bronze_unique_keys,
        ["report_id", "report_version"],
        "left_anti"
    )
)

print("\nA. SILVER KEYS NOT FOUND IN BRONZE")
missing_silver.show(20, truncate=False)

# B. Inspect Bronze records with missing identifiers or versions
print("\nB. BRONZE RECORDS WITH NULL ID OR VERSION")

null_records = bronze_investigation.filter(
    F.col("report_id").isNull()
    | F.col("report_version").isNull()
)

print("Total incomplete records:", null_records.count())

null_records.groupBy(
    "run_id"
).count().orderBy("run_id").show(50, truncate=False)

print("\nSample incomplete records:")
null_records.select(
    "report_id",
    "report_version",
    "run_id",
    "source_file"
).show(20, truncate=False)

# C. Check how frequently each valid key appears
print("\nC. BRONZE KEY OCCURRENCE DISTRIBUTION")

key_frequency = (
    bronze_valid
    .groupBy("report_id", "report_version")
    .agg(
        F.count("*").alias("occurrences"),
        F.countDistinct("run_id").alias("distinct_runs")
    )
)

key_frequency.select(
    F.min("occurrences").alias("min_occurrences"),
    F.max("occurrences").alias("max_occurrences"),
    F.min("distinct_runs").alias("min_runs"),
    F.max("distinct_runs").alias("max_runs")
).show()

print("\nD. SAMPLE KEYS WITH THE FEWEST OCCURRENCES")
key_frequency.orderBy("occurrences").show(10, truncate=False)

print("\nINVESTIGATION COMPLETED — NO TABLES MODIFIED")


# COMMAND ----------

from pyspark.sql import functions as F

TARGET_ID = "5801206-7"

BRONZE_PATH = (
    "/Volumes/workspace/default/fda_data/bronze/"
    "ingestion_date=*/run_id=*/page_*.json"
)

# Read original Bronze JSON files
raw_envelopes = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

# Extract reports
df_reports = (
    raw_envelopes
    .select(
        F.explode_outer("results").alias("report"),
        F.col("_metadata.file_path").alias("_source_file")
    )
    .withColumn(
        "_run_id",
        F.regexp_extract("_source_file", r"run_id=([^/]+)", 1)
    )
    .select(
        "_run_id",
        F.col("report.safetyreportid").cast("string").alias("safetyreportid"),
        F.col("report.safetyreportversion").cast("string").alias("safetyreportversion")
    )
)

print("--- BRONZE ---")
df_reports.filter(
    F.col("safetyreportid") == TARGET_ID
).show(50, truncate=False)

print("--- SILVER ---")
spark.table("workspace.silver.fda_reports") \
    .filter(F.col("report_id") == TARGET_ID) \
    .show(truncate=False)

print("INSPECTION COMPLETED — NO TABLES MODIFIED")

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    "workspace.silver.fda_reports",
    "workspace.silver.fda_report_drugs",
    "workspace.silver.fda_report_reactions"
]

for table_name in tables:
    print("\n" + "=" * 70)
    print(f"DELTA HISTORY: {table_name}")
    print("=" * 70)

    try:
        history = spark.sql(
            f"DESCRIBE HISTORY {table_name}"
        )

        history.select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "userName",
            "notebook"
        ).show(10, truncate=False)

    except Exception as e:
        print(f"Could not retrieve history: {e}")

print("\nHISTORY INSPECTION COMPLETED — NO TABLES MODIFIED")

# COMMAND ----------

history = spark.sql(
    "DESCRIBE HISTORY workspace.silver.fda_reports"
)

history.filter(
    F.upper(F.col("operation")).isin(
        "MERGE",
        "WRITE",
        "CREATE TABLE",
        "CREATE OR REPLACE TABLE",
        "UPDATE",
        "DELETE"
    )
).select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "operationMetrics",
    "userName",
    "notebook"
).show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

TARGET_ID = "5801206-7"

print("=== CURRENT SILVER RECORD ===")

spark.sql("""
    SELECT report_id, report_version
    FROM workspace.silver.fda_reports
    WHERE report_id = '5801206-7'
""").show(truncate=False)

print("=== REPORT VERSION DISTRIBUTION ===")

spark.table("workspace.silver.fda_reports") \
    .groupBy("report_version") \
    .count() \
    .orderBy("report_version") \
    .show(truncate=False)

print("=== NULL VERSION CHECK ===")

spark.table("workspace.silver.fda_reports") \
    .filter(F.col("report_version").isNull()) \
    .count()

# COMMAND ----------

from pyspark.sql import functions as F

TARGET_ID = "5801206-7"

print("=== CURRENT SILVER RECORD ===")

spark.table("workspace.silver.fda_reports") \
    .filter(F.col("report_id") == TARGET_ID) \
    .select("report_id", "report_version") \
    .show(truncate=False)

print("=== VERSION DISTRIBUTION ===")

spark.table("workspace.silver.fda_reports") \
    .groupBy("report_version") \
    .count() \
    .orderBy("report_version") \
    .show(truncate=False)

# COMMAND ----------


from pyspark.sql import functions as F

# ============================================================
# BRONZE vs SILVER — VERSION AUDIT
# Read-only. Rebuilds all DataFrames from scratch.
# ============================================================

BRONZE_PATH = (
    "/Volumes/workspace/default/fda_data/bronze/"
    "ingestion_date=*/run_id=*/page_*.json"
)

SILVER_TABLE = "workspace.silver.fda_reports"
TARGET_ID = "10004006"

# 1. Read Bronze from disk
print("1. READING BRONZE...")

raw_envelopes = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

# 2. Extract report ID and version, preserving source file
df_reports = (
    raw_envelopes
    .select(
        F.explode_outer("results").alias("report"),
        F.col("_metadata.file_path").alias("_source_file")
    )
    .withColumn(
        "_run_id",
        F.regexp_extract("_source_file", r"run_id=([^/]+)", 1)
    )
    .select(
        "_run_id",
        F.col("report.safetyreportid").cast("string").alias("report_id"),
        F.col("report.safetyreportversion").cast("int").alias("bronze_version")
    )
)

print("Bronze records:", df_reports.count())

# 3. Inspect the selected report
print("\n2. TARGET REPORT — BRONZE VERSIONS")

df_reports.filter(
    F.col("report_id") == TARGET_ID
).select(
    "report_id",
    "bronze_version",
    "_run_id"
).distinct().orderBy(
    "bronze_version"
).show(100, truncate=False)

# 4. Calculate highest available Bronze version per report
# NULL is treated as 0, matching the documented legacy convention.
bronze_latest = (
    df_reports
    .groupBy("report_id")
    .agg(
        F.coalesce(
            F.max("bronze_version"),
            F.lit(0)
        ).alias("bronze_latest_version")
    )
)

# 5. Read Silver directly
silver = (
    spark.table(SILVER_TABLE)
    .select(
        F.col("report_id"),
        F.col("report_version").cast("int").alias("silver_version")
    )
)

# 6. Compare Bronze latest version against Silver
comparison = (
    bronze_latest
    .join(silver, on="report_id", how="full_outer")
    .withColumn(
        "status",
        F.when(F.col("report_id").isNull(), "MISSING_REPORT_ID")
         .when(F.col("bronze_latest_version").isNull(), "NOT_IN_BRONZE")
         .when(F.col("silver_version").isNull(), "NOT_IN_SILVER")
         .when(
             F.col("bronze_latest_version") == F.col("silver_version"),
             "OK"
         )
         .otherwise("VERSION_MISMATCH")
    )
)

print("\n3. AUDIT SUMMARY")

comparison.groupBy("status").count().show(truncate=False)

print("\n4. VERSION MISMATCHES")

comparison.filter(
    F.col("status") != "OK"
).orderBy("report_id").show(100, truncate=False)

print("\n5. TARGET REPORT — BRONZE vs SILVER")

comparison.filter(
    F.col("report_id") == TARGET_ID
).show(truncate=False)

print("\nAUDIT COMPLETED — NO TABLES MODIFIED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_PATH = (
    "/Volumes/workspace/default/fda_data/bronze/"
    "ingestion_date=*/run_id=*/page_*.json"
)

# Rebuild Bronze independently
raw = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

bronze = (
    raw.select(F.explode_outer("results").alias("r"))
    .select(
        F.col("r.safetyreportid").cast("string").alias("report_id"),
        F.col("r.safetyreportversion").cast("int").alias("version")
    )
)

# Latest available version per report
bronze_latest = (
    bronze.groupBy("report_id")
    .agg(
        F.coalesce(F.max("version"), F.lit(0))
        .alias("bronze_version")
    )
)

# Current Silver
silver = (
    spark.table("workspace.silver.fda_reports")
    .select(
        "report_id",
        F.col("report_version").cast("int").alias("silver_version")
    )
)

# Compare
audit = bronze_latest.join(
    silver, "report_id", "full_outer"
)

audit = audit.withColumn(
    "status",
    F.when(F.col("bronze_version").isNull(), "ONLY_IN_SILVER")
     .when(F.col("silver_version").isNull(), "ONLY_IN_BRONZE")
     .when(
         F.col("bronze_version") != F.col("silver_version"),
         "VERSION_MISMATCH"
     )
     .otherwise("OK")
)

print("=== BRONZE ===")
print("Total records:", bronze.count())
print("Distinct report IDs:", bronze.select("report_id").distinct().count())

print("\n=== SILVER ===")
print("Total rows:", silver.count())
print("Distinct report IDs:", silver.select("report_id").distinct().count())

print("\n=== AUDIT RESULTS ===")
audit.groupBy("status").count().show()

print("\n=== PROBLEM EXAMPLES ===")
audit.filter(F.col("status") != "OK").show(20, truncate=False)

print("\nREAD-ONLY AUDIT COMPLETED")
