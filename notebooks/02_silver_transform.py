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
# MAGIC Drugs and reactions are extracted independently to avoid a drug×reaction Cartesian product.
# MAGIC The `report_id` column links the three entities. No drug-to-reaction causal relationship is implied.
# MAGIC
# MAGIC **Report versioning:** openFDA reports carry `safetyreportversion`. When the same report appears
# MAGIC multiple times in bronze (repeated ingestion or follow-up versions), silver keeps only the highest
# MAGIC version — and the MERGE update is guarded so an older version can never overwrite a newer one.
# MAGIC Child rows (drugs/reactions) are kept only from the latest version of each report, and the child
# MAGIC MERGE deletes target rows that are no longer present in the source (child-table sync).
# MAGIC
# MAGIC **Assumption:** silver re-reads the COMPLETE bronze history on every run (bronze files are
# MAGIC append-only). All version-collision and child-sync logic depends on that assumption.

# COMMAND ----------

# DBTITLE 1,Silver — Transform & MERGE
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    explode,
    lit,
    round,
    row_number,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.functions import max as _max
from pyspark.sql.functions import sum as _sum
from pyspark.sql.window import Window

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

    Grain: one row per safety report VERSION (safetyreportid + safetyreportversion).
    report_version defaults to 0 for legacy records that lack the field.
    Null, empty and whitespace-only reporter countries are mapped to UNKNOWN.
    Reports without a safetyreportid are dropped here — a report we cannot
    identify cannot be joined to its drugs and reactions.
    """
    logger.info("Extracting report-level records (one row per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    country = upper(trim(col("result.primarysource.reportercountry")))
    return (
        df_exploded.select(
            col("result.safetyreportid").alias("report_id"),
            # openFDA dates come as yyyyMMdd strings, convert to proper DateType
            to_date(col("result.receiptdate"), "yyyyMMdd").alias("report_date"),
            # reportercountry is where the report originated — more reliable than occurcountry
            # null, empty and whitespace-only values are all mapped to UNKNOWN
            when(country.isNull() | (country == ""), lit("UNKNOWN"))
            .otherwise(country)
            .alias("country"),
            # sex is coded as 1/2 in the raw data, map to readable values
            when(col("result.patient.patientsex") == "1", "MALE")
            .when(col("result.patient.patientsex") == "2", "FEMALE")
            .otherwise("UNKNOWN")
            .alias("patient_sex"),
            # preserve raw age and unit — do NOT assume years
            col("result.patient.patientonsetage").alias("patient_age_raw"),
            col("result.patient.patientonsetageunit").alias("patient_age_unit"),
            # version-aware MERGE needs the source version; 0 = legacy record without one
            coalesce(col("result.safetyreportversion").cast("int"), lit(0)).alias("report_version"),
        )
        .filter(col("report_id").isNotNull())
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

    Grain: one row per (report_id, report_version, drug_name). The version tag
    is used to keep only child rows from the LATEST version of each report
    (see keep_children_of_latest_reports) and is dropped before the MERGE.
    Rows with a null/blank drug name are dropped — an unnamed drug would
    violate the persisted table grain (report_id, drug_name).
    """
    logger.info("Extracting drug records (one row per drug per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    df_drugs = df_exploded.select(
        col("result.safetyreportid").alias("report_id"),
        coalesce(col("result.safetyreportversion").cast("int"), lit(0)).alias("report_version"),
        explode(col("result.patient.drug")).alias("drug_info"),
    )
    return (
        df_drugs.select(
            col("report_id"),
            col("report_version"),
            upper(trim(col("drug_info.medicinalproduct"))).alias("drug_name"),
            # 1=Suspect, 2=Concomitant, 3=Interacting per openFDA docs
            col("drug_info.drugcharacterization").cast("int").alias("drug_characterization"),
        )
        .filter(
            col("report_id").isNotNull()
            & col("drug_name").isNotNull()
            & (col("drug_name") != "")
        )
    )


def extract_reactions(df: DataFrame) -> DataFrame:
    """Extract one row per adverse reaction within each FDA report.

    Grain: one row per (report_id, report_version, adverse_reaction); the
    version tag is dropped after filtering to the latest report version.
    Rows with a null/blank reaction term are dropped.
    """
    logger.info("Extracting reaction records (one row per reaction per report)...")
    df_exploded = df.select(explode(col("results")).alias("result"))
    df_reactions = df_exploded.select(
        col("result.safetyreportid").alias("report_id"),
        coalesce(col("result.safetyreportversion").cast("int"), lit(0)).alias("report_version"),
        explode(col("result.patient.reaction")).alias("reaction_info"),
    )
    return (
        df_reactions.select(
            col("report_id"),
            col("report_version"),
            upper(trim(col("reaction_info.reactionmedrapt"))).alias("adverse_reaction"),
        )
        .filter(
            col("report_id").isNotNull()
            & col("adverse_reaction").isNotNull()
            & (col("adverse_reaction") != "")
        )
    )


def keep_latest_version(df: DataFrame, key_cols: list,
                        version_col: str = "report_version") -> DataFrame:
    """Keep one row per key: the highest report version (ties: latest report_date)."""
    order_cols = [col(version_col).desc()]
    if "report_date" in df.columns:
        order_cols.append(col("report_date").desc())
    window = Window.partitionBy(*key_cols).orderBy(*order_cols)
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def keep_children_of_latest_reports(df_children: DataFrame, df_reports: DataFrame) -> DataFrame:
    """Keep child rows only from the latest version of each report.

    Without this, drugs/reactions of an older report version would linger
    after the report was updated. The version tag is dropped here — the
    persisted child grain is (report_id, drug_name) / (report_id,
    adverse_reaction).
    """
    latest_keys = df_reports.groupBy("report_id").agg(
        _max("report_version").alias("report_version")
    )
    return (
        df_children.join(latest_keys, ["report_id", "report_version"], "inner")
        .drop("report_version")
    )


def merge_to_delta(df: DataFrame, table_name: str, merge_condition: str, dedup_cols: list,
                   version_col: str = None, delete_not_matched_by_source: bool = False) -> None:
    """Idempotent MERGE — deduplicate source on the merge key, then upsert.

    Source dedup prevents DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    when the same bronze data is processed more than once. If the same drug
    appears twice in one report (e.g., as both Suspect and Concomitant), only
    the first occurrence is kept — acceptable because gold counts unique
    reports per drug, not characterization roles.

    version_col: when set (reports table), the update is guarded so an older
    source version can never overwrite a newer target row.

    delete_not_matched_by_source: when True (child tables), target rows absent
    from the source are deleted — synchronizes child tables when an updated
    report removes a drug or reaction. Safe ONLY because silver re-reads the
    complete bronze history every run.
    """
    logger.info(f"MERGE into {table_name} (dedup on {dedup_cols})")
    df = df.dropDuplicates(dedup_cols)
    delta_table = DeltaTable.forName(spark, table_name)
    merge_builder = delta_table.alias("target").merge(df.alias("source"), merge_condition)
    if version_col:
        merge_builder = merge_builder.whenMatchedUpdateAll(
            condition=f"source.{version_col} >= coalesce(target.{version_col}, 0)"
        )
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()
    merge_builder = merge_builder.whenNotMatchedInsertAll()
    if delete_not_matched_by_source:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()
    merge_builder.execute()
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
        patient_age_years DOUBLE,
        report_version INT
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

# One-time migration for tables created before report versioning was added:
# backfill existing rows to version 0 so the guarded MERGE treats them as the
# oldest possible version.
existing_cols = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {REPORTS_TABLE}").collect()}
if "report_version" not in existing_cols:
    spark.sql(f"ALTER TABLE {REPORTS_TABLE} ADD COLUMN report_version INT")
    spark.sql(f"UPDATE {REPORTS_TABLE} SET report_version = 0")


if __name__ == "__main__":
    logger.info("Starting Silver Layer Pipeline Execution...")

    raw_df = read_bronze_data(spark, BRONZE_JSON_PATH)

    # Extract each entity independently — NO drug x reaction Cartesian product
    df_reports_all = raw_df.transform(extract_reports).transform(normalize_age)
    df_drugs_all = raw_df.transform(extract_drugs)
    df_reactions_all = raw_df.transform(extract_reactions)

    # Collapse to the latest version of each report; keep children only from
    # that version. Children that exist only in an older version (removed by a
    # follow-up report) are excluded from the source, and the child MERGE
    # deletes the stale target rows (delete_not_matched_by_source).
    df_reports = keep_latest_version(df_reports_all, ["report_id"])
    df_drugs = keep_children_of_latest_reports(df_drugs_all, df_reports_all)
    df_reactions = keep_children_of_latest_reports(df_reactions_all, df_reports_all)

    # Log row counts for pipeline observability
    n_reports = df_reports.count()
    n_drugs = df_drugs.count()
    n_reactions = df_reactions.count()
    logger.info(f"Reports:   {n_reports:,}")
    logger.info(f"Drugs:     {n_drugs:,}")
    logger.info(f"Reactions: {n_reactions:,}")

    # MERGE with keys matching each table's grain — idempotent on re-runs
    # fda_reports:          key = report_id; version guard: old never overwrites new
    # fda_report_drugs:     key = (report_id, drug_name); deletes stale child rows
    # fda_report_reactions: key = (report_id, adverse_reaction); deletes stale child rows
    merge_to_delta(df_reports, REPORTS_TABLE,
                   "target.report_id = source.report_id", ["report_id"],
                   version_col="report_version")
    merge_to_delta(df_drugs, DRUGS_TABLE,
                   "target.report_id = source.report_id AND target.drug_name = source.drug_name",
                   ["report_id", "drug_name"], delete_not_matched_by_source=True)
    merge_to_delta(df_reactions, REACTIONS_TABLE,
                   "target.report_id = source.report_id AND "
                   "target.adverse_reaction = source.adverse_reaction",
                   ["report_id", "adverse_reaction"], delete_not_matched_by_source=True)

    logger.info("Silver Pipeline Lifecycle Complete")

# COMMAND ----------

# DBTITLE 1,Silver — Data Quality Validation
# ─────────────────────────────────────────────────────────────────────────────
# Data Quality Validation
# Table-level pass/fail checks at the correct grain for each silver table.
# These are NOT exploratory statistics — they validate correctness.
#
# Hard gates (fail the pipeline): null/duplicate report_id, orphan children.
# Soft warnings (log only): null dates, invalid sex/age, blank names.
#
# The orphan checks compare children against the full reports TABLE (not just
# the current batch), so they stay correct even if the pipeline later becomes
# incremental — a child may legitimately reference a report ingested earlier.
# ─────────────────────────────────────────────────────────────────────────────

df_reports = spark.table("workspace.silver.fda_reports")
df_drugs = spark.table("workspace.silver.fda_report_drugs")
df_reactions = spark.table("workspace.silver.fda_report_reactions")


def compute_dq_metrics(df_reports: DataFrame, df_drugs: DataFrame,
                       df_reactions: DataFrame) -> dict:
    """Compute data-quality metrics for the three silver tables.

    Counts are consolidated into one aggregate per table to avoid a separate
    Spark action per metric.
    """
    reports_row = df_reports.agg(
        count(lit(1)).alias("rows"),
        _sum(when(col("report_id").isNull(), 1).otherwise(0)).alias("null_report_ids"),
        _sum(when(col("report_date").isNull(), 1).otherwise(0)).alias("null_dates"),
        _sum(
            when(~col("patient_sex").isin("MALE", "FEMALE", "UNKNOWN"), 1).otherwise(0)
        ).alias("invalid_sex"),
        _sum(
            when(
                col("patient_age_years").isNotNull()
                & ((col("patient_age_years") < 0) | (col("patient_age_years") > 120)),
                1,
            ).otherwise(0)
        ).alias("invalid_age"),
    ).collect()[0]

    duplicate_report_ids = (
        reports_row["rows"] - df_reports.select("report_id").distinct().count()
    )

    drugs_row = df_drugs.agg(
        count(lit(1)).alias("rows"),
        _sum(
            when(col("drug_name").isNull() | (col("drug_name") == ""), 1).otherwise(0)
        ).alias("null_drug_names"),
    ).collect()[0]

    duplicate_drugs = (
        df_drugs.groupBy("report_id", "drug_name").count().filter(col("count") > 1).count()
    )

    reactions_row = df_reactions.agg(
        count(lit(1)).alias("rows"),
        _sum(
            when(col("adverse_reaction").isNull() | (col("adverse_reaction") == ""),
                 1).otherwise(0)
        ).alias("null_reaction_names"),
    ).collect()[0]

    duplicate_reactions = (
        df_reactions.groupBy("report_id", "adverse_reaction")
        .count().filter(col("count") > 1).count()
    )

    orphan_drugs = df_drugs.join(df_reports, "report_id", "left_anti").count()
    orphan_reactions = df_reactions.join(df_reports, "report_id", "left_anti").count()

    return {
        "total_reports": reports_row["rows"],
        "null_report_ids": reports_row["null_report_ids"],
        "duplicate_report_ids": duplicate_report_ids,
        "null_dates": reports_row["null_dates"],
        "invalid_sex": reports_row["invalid_sex"],
        "invalid_age": reports_row["invalid_age"],
        "total_drugs": drugs_row["rows"],
        "null_drug_names": drugs_row["null_drug_names"],
        "duplicate_drugs": duplicate_drugs,
        "total_reactions": reactions_row["rows"],
        "null_reaction_names": reactions_row["null_reaction_names"],
        "duplicate_reactions": duplicate_reactions,
        "orphan_drugs": orphan_drugs,
        "orphan_reactions": orphan_reactions,
    }


# Gates that fail the pipeline. Everything else is informational or a soft warning.
HARD_GATE_CHECKS = ("null_report_ids", "duplicate_report_ids", "orphan_drugs", "orphan_reactions")


def enforce_hard_gates(metrics: dict, hard_gates: tuple = HARD_GATE_CHECKS) -> None:
    """Raise RuntimeError when any hard gate is violated.

    If a gate fails, the silver notebook raises and the downstream gold task
    is skipped (ALL_SUCCESS dependency in the job).
    """
    failed = {name: metrics.get(name, 0) for name in hard_gates if metrics.get(name, 0) > 0}
    if failed:
        details = "\n".join(f"  {name}: {value}" for name, value in failed.items())
        raise RuntimeError(
            "DATA QUALITY GATE FAILED:\n"
            f"{details}\n"
            "Pipeline aborted. Investigate silver table state before re-running."
        )


def warn_soft_metrics(metrics: dict) -> None:
    """Log soft data-quality warnings — suspicious but not pipeline-blocking."""
    soft_metrics = ("null_dates", "invalid_sex", "invalid_age", "null_drug_names",
                    "null_reaction_names")
    for name in soft_metrics:
        if metrics.get(name, 0) > 0:
            logger.warning(
                f"Soft DQ warning — {name}: {metrics[name]} (logged; pipeline continues)"
            )


dq_metrics = compute_dq_metrics(df_reports, df_drugs, df_reactions)

print("=" * 70)
print("DATA QUALITY VALIDATION")
print("=" * 70)
print("\n-- workspace.silver.fda_reports --")
print(f"  Total rows:                      {dq_metrics['total_reports']:,}")
print(f"  Null report_id:                   {dq_metrics['null_report_ids']}")
print(f"  Duplicate report_id:              {dq_metrics['duplicate_report_ids']}")
print(f"  Null report_date:                 {dq_metrics['null_dates']:,}")
print(f"  Invalid patient_sex:              {dq_metrics['invalid_sex']}")
print(f"  Age out of range (0-120):         {dq_metrics['invalid_age']}")
print("\n-- workspace.silver.fda_report_drugs --")
print(f"  Total rows:                       {dq_metrics['total_drugs']:,}")
print(f"  Null/empty drug_name:             {dq_metrics['null_drug_names']}")
print(f"  Duplicate (report_id, drug_name): {dq_metrics['duplicate_drugs']}")
print("\n-- workspace.silver.fda_report_reactions --")
print(f"  Total rows:                          {dq_metrics['total_reactions']:,}")
print(f"  Null/empty adverse_reaction:         {dq_metrics['null_reaction_names']}")
print(f"  Duplicate (report_id, adverse_reaction): {dq_metrics['duplicate_reactions']}")
print("\n-- Referential integrity --")
print(f"  Drug records with no matching report:      {dq_metrics['orphan_drugs']}")
print(f"  Reaction records with no matching report:  {dq_metrics['orphan_reactions']}")

warn_soft_metrics(dq_metrics)
enforce_hard_gates(dq_metrics)
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