# Databricks notebook source
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
# MAGIC Drugs and reactions are extracted independently to avoid a drug\u00d7reaction Cartesian product. The `report_id` column links the three entities. No drug-to-reaction causal relationship is implied.

# COMMAND ----------

# DBTITLE 1,Silver — Transform & MERGE
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    explode,
    lit,
    to_date,
    trim,
    upper,
    when,
)

from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# glob pattern picks up all bronze files across every run_id
BRONZE_JSON_PATH = "/Volumes/workspace/default/fda_data/bronze/ingestion_date=*/run_id=*/page_*.json"

REPORTS_TABLE = "workspace.silver.fda_reports"
DRUGS_TABLE = "workspace.silver.fda_report_drugs"
REACTIONS_TABLE = "workspace.silver.fda_report_reactions"


def read_bronze_data(spark_session: SparkSession, path: str) -> DataFrame:
    logger.info(f"Reading raw JSON data from path: {path}")
    # multiline=true because openFDA returns pretty-printed JSON, not JSONL
    return spark_session.read.option("multiline", "true").json(path)


def extract_reports(df: DataFrame) -> DataFrame:
    """Extract one row per FDA report with report-level attributes.

    Grain: one row per safety report (safetyreportid).
    No array explosion — drugs and reactions are handled in separate tables.
    """
    logger.info("Extracting report-level records (one row per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    return df_exploded.select(
        col("result.safetyreportid").alias("report_id"),
        # openFDA dates come as yyyyMMdd strings, convert to proper DateType
        to_date(col("result.receiptdate"), "yyyyMMdd").alias("report_date"),
        # reportercountry is where the report originated — more reliable than occurcountry
        coalesce(upper(trim(col("result.primarysource.reportercountry"))), lit("UNKNOWN")).alias("country"),
        # sex is coded as 1/2 in the raw data, map to readable values
        when(col("result.patient.patientsex") == "1", "MALE")
        .when(col("result.patient.patientsex") == "2", "FEMALE")
        .otherwise("UNKNOWN").alias("patient_sex"),
        # preserve raw age and unit — do NOT assume years
        col("result.patient.patientonsetage").alias("patient_age_raw"),
        col("result.patient.patientonsetageunit").alias("patient_age_unit"),
    )


def normalize_age(df: DataFrame) -> DataFrame:
    """Convert patient_age_raw + patient_age_unit into patient_age_years.

    FAERS patientonsetageunit uses NCIT (NCI Thesaurus) codes:
      800 = Decade, 801 = Year, 802 = Month, 803 = Week, 804 = Day, 805 = Hour

    Only convert when the unit is one we can safely convert from.
    Hour (805) is excluded — it only applies to neonates and produces
    negligible year values not useful for aggregate analytics.
    """
    logger.info("Normalizing patient age to years...")
    age_raw_dbl = col("patient_age_raw").cast("double")
    return df.withColumn(
        "patient_age_years",
        when(col("patient_age_unit") == "800", age_raw_dbl * 10.0)
        .when(col("patient_age_unit") == "801", age_raw_dbl)
        .when(col("patient_age_unit") == "802", age_raw_dbl / 12.0)
        .when(col("patient_age_unit") == "803", age_raw_dbl / 52.0)
        .when(col("patient_age_unit") == "804", age_raw_dbl / 365.25)
        .otherwise(None),
    )


def extract_drugs(df: DataFrame) -> DataFrame:
    """Extract one row per drug within each FDA report.

    Grain: one row per (report_id, drug_name).
    Only the drug array is exploded — reactions are NOT included here.
    This avoids creating a drug x reaction Cartesian product.
    """
    logger.info("Extracting drug records (one row per drug per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    df_drugs = df_exploded.select(
        col("result.safetyreportid").alias("report_id"),
        explode(col("result.patient.drug")).alias("drug_info"),
    )
    return df_drugs.select(
        col("report_id"),
        upper(trim(col("drug_info.medicinalproduct"))).alias("drug_name"),
        # 1=Suspect, 2=Concomitant, 3=Interacting per openFDA docs
        col("drug_info.drugcharacterization").cast("int").alias("drug_characterization"),
    )


def extract_reactions(df: DataFrame) -> DataFrame:
    """Extract one row per adverse reaction within each FDA report.

    Grain: one row per (report_id, adverse_reaction).
    Only the reaction array is exploded — drugs are NOT included here.
    This avoids creating a drug x reaction Cartesian product.
    """
    logger.info("Extracting reaction records (one row per reaction per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    df_reactions = df_exploded.select(
        col("result.safetyreportid").alias("report_id"),
        explode(col("result.patient.reaction")).alias("reaction_info"),
    )
    return df_reactions.select(
        col("report_id"),
        upper(trim(col("reaction_info.reactionmeddrapt"))).alias("adverse_reaction"),
    )


def merge_to_delta(df: DataFrame, table_name: str, merge_condition: str, dedup_cols: list) -> None:
    """Idempotent MERGE — deduplicate source on the merge key, then upsert.

    Deduplication prevents DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    when the same bronze data is processed more than once.
    """
    logger.info(f"MERGE into {table_name} (dedup on {dedup_cols})")
    # If the same drug appears twice in one report (e.g., as both Suspect
    # and Concomitant), we keep only the first occurrence. This is acceptable
    # because our gold-layer analytics count unique reports per drug, not
    # drug characterization roles.
    df = df.dropDuplicates(dedup_cols)
    delta_table = DeltaTable.forName(spark, table_name)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"MERGE complete: {table_name}")


# -- Table DDL ----------------------------------------------------------------
# No partitioning: the dataset is small (~55K rows). Partitioning by report_date
# would create many tiny partitions with no meaningful performance benefit.
# A single unpartitioned Delta table is simpler and performs well at this scale.
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REPORTS_TABLE} (
        report_id STRING,
        report_date DATE,
        country STRING,
        patient_sex STRING,
        patient_age_raw STRING,
        patient_age_unit STRING,
        patient_age_years DOUBLE
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DRUGS_TABLE} (
        report_id STRING,
        drug_name STRING,
        drug_characterization INT
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REACTIONS_TABLE} (
        report_id STRING,
        adverse_reaction STRING
    ) USING DELTA
""")


if __name__ == "__main__":
    logger.info("Starting Silver Layer Pipeline Execution...")

    raw_df = read_bronze_data(spark, BRONZE_JSON_PATH)

    # Extract each entity independently — NO drug x reaction Cartesian product
    df_reports = raw_df.transform(extract_reports).transform(normalize_age)
    df_drugs = raw_df.transform(extract_drugs)
    df_reactions = raw_df.transform(extract_reactions)

    # Quality gates at the correct grain for each entity
    df_reports = df_reports.filter(col("report_id").isNotNull())
    df_drugs = df_drugs.filter(
        col("report_id").isNotNull() & col("drug_name").isNotNull() & (col("drug_name") != "")
    )
    df_reactions = df_reactions.filter(
        col("report_id").isNotNull() & col("adverse_reaction").isNotNull() & (col("adverse_reaction") != "")
    )

    # Log row counts for pipeline observability
    n_reports = df_reports.count()
    n_drugs = df_drugs.count()
    n_reactions = df_reactions.count()
    logger.info(f"Reports:   {n_reports:,}")
    logger.info(f"Drugs:     {n_drugs:,}")
    logger.info(f"Reactions: {n_reactions:,}")

    # MERGE with keys matching each table's grain — idempotent on re-runs
    # fda_reports:          key = report_id (natural PK, one row per report)
    # fda_report_drugs:     key = (report_id, drug_name) — one row per drug per report
    # fda_report_reactions: key = (report_id, adverse_reaction) — one row per reaction per report
    merge_to_delta(df_reports, REPORTS_TABLE,
        "target.report_id = source.report_id", ["report_id"])
    merge_to_delta(df_drugs, DRUGS_TABLE,
        "target.report_id = source.report_id AND target.drug_name = source.drug_name",
        ["report_id", "drug_name"])
    merge_to_delta(df_reactions, REACTIONS_TABLE,
        "target.report_id = source.report_id AND target.adverse_reaction = source.adverse_reaction",
        ["report_id", "adverse_reaction"])

    logger.info("Silver Pipeline Lifecycle Complete")

# COMMAND ----------

# DBTITLE 1,Silver — Data Quality Validation
# ─────────────────────────────────────────────────────────────────────────────
# Data Quality Validation
# Pass/fail checks at the correct grain for each silver table.
# These are NOT exploratory statistics — they validate correctness.
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import col, count, countDistinct, when, sum as _sum

df_reports = spark.table("workspace.silver.fda_reports")
df_drugs = spark.table("workspace.silver.fda_report_drugs")
df_reactions = spark.table("workspace.silver.fda_report_reactions")

print("=" * 70)
print("DATA QUALITY VALIDATION")
print("=" * 70)

# --- workspace.silver.fda_reports (grain: one row per report) ---
total_reports = df_reports.count()
null_report_ids = df_reports.filter(col("report_id").isNull()).count()
duplicate_report_ids = total_reports - df_reports.select("report_id").distinct().count()
null_dates = df_reports.filter(col("report_date").isNull()).count()
invalid_sex = df_reports.filter(~col("patient_sex").isin("MALE", "FEMALE", "UNKNOWN")).count()
invalid_age = df_reports.filter(
    col("patient_age_years").isNotNull() &
    ((col("patient_age_years") < 0) | (col("patient_age_years") > 120))
).count()

print(f"\n-- workspace.silver.fda_reports --")
print(f"  Total rows:                      {total_reports:,}")
print(f"  Null report_id:                   {null_report_ids}")
print(f"  Duplicate report_id:              {duplicate_report_ids}")
print(f"  Null report_date:                 {null_dates:,}")
print(f"  Invalid patient_sex:              {invalid_sex}")
print(f"  Age out of range (0-120):         {invalid_age}")

# --- workspace.silver.fda_report_drugs (grain: one row per drug per report) ---
total_drugs = df_drugs.count()
null_drug_names = df_drugs.filter(col("drug_name").isNull() | (col("drug_name") == "")).count()
duplicate_drugs = df_drugs.groupBy("report_id", "drug_name").count().filter(col("count") > 1).count()

print(f"\n-- workspace.silver.fda_report_drugs --")
print(f"  Total rows:                       {total_drugs:,}")
print(f"  Null/empty drug_name:             {null_drug_names}")
print(f"  Duplicate (report_id, drug_name): {duplicate_drugs}")

# --- workspace.silver.fda_report_reactions (grain: one row per reaction per report) ---
total_reactions = df_reactions.count()
null_reactions = df_reactions.filter(col("adverse_reaction").isNull() | (col("adverse_reaction") == "")).count()
duplicate_reactions = df_reactions.groupBy("report_id", "adverse_reaction").count().filter(col("count") > 1).count()

print(f"\n-- workspace.silver.fda_report_reactions --")
print(f"  Total rows:                          {total_reactions:,}")
print(f"  Null/empty adverse_reaction:         {null_reactions}")
print(f"  Duplicate (report_id, adverse_reaction): {duplicate_reactions}")

# --- Referential integrity ---
# Every report_id in drugs/reactions should exist in fda_reports
orphan_drugs = df_drugs.join(df_reports, "report_id", "left_anti").count()
orphan_reactions = df_reactions.join(df_reports, "report_id", "left_anti").count()

print(f"\n-- Referential integrity --")
print(f"  Drug records with no matching report:      {orphan_drugs}")
print(f"  Reaction records with no matching report:  {orphan_reactions}")

# ─────────────────────────────────────────────────────────────────────────────
# Hard Quality Gates
# These checks FAIL the pipeline if violated. They are not optional.
# If a gate fails, the silver notebook raises RuntimeError and the downstream
# gold task is skipped (due to ALL_SUCCESS dependency in the job).
# ─────────────────────────────────────────────────────────────────────────────

HARD_GATE_CHECKS = {
    "null_report_ids": null_report_ids,
    "duplicate_report_ids": duplicate_report_ids,
    "orphan_drugs": orphan_drugs,
    "orphan_reactions": orphan_reactions,
}

failed_checks = {name: value for name, value in HARD_GATE_CHECKS.items() if value > 0}

if failed_checks:
    msg = "DATA QUALITY GATE FAILED:\n"
    for name, value in failed_checks.items():
        msg += f"  {name}: {value}\n"
    msg += "Pipeline aborted. Investigate bronze data quality before re-running."
    raise RuntimeError(msg)

print("\nAll hard quality gates passed.")

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

from pyspark.sql.functions import col, count, countDistinct, avg, round

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
print("\n-- Top 10 drugs by report count (with avg patient age) --")
df_drugs.join(df_reports, "report_id", "inner") \
    .groupBy("drug_name") \
    .agg(
        countDistinct("report_id").alias("report_count"),
        round(avg("patient_age_years"), 2).alias("avg_patient_age_years"),
    ) \
    .orderBy(col("report_count").desc()) \
    .limit(10) \
    .show(truncate=False)