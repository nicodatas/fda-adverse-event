# Databricks notebook source
# DBTITLE 1,Gold Layer Title
# MAGIC %md
# MAGIC # Gold Layer — Aggregated Tables & Governance
# MAGIC
# MAGIC Builds business-ready aggregate tables from the silver layer using `countDistinct(report_id)` for
# MAGIC report-count metrics. Reads from the three silver tables (`fda_reports`, `fda_report_drugs`,
# MAGIC `fda_report_reactions`) and joins where needed.
# MAGIC
# MAGIC **Disclaimer:** FAERS reports are spontaneous. Report frequency does not imply causality or relative
# MAGIC drug safety. These tables describe reporting volume, not risk.
# MAGIC
# MAGIC **Design:** each aggregate is built by a pure function that takes silver DataFrames and returns the
# MAGIC aggregate DataFrame — testable outside Databricks (see `tests/test_gold_aggregate.py`). Reports whose
# MAGIC `report_date` could not be parsed are excluded from the monthly trend table and logged; they remain
# MAGIC in silver.

# COMMAND ----------

# DBTITLE 1,Gold — Aggregated Tables
# Gold layer: pre-aggregated tables ready for dashboards and reporting.
# These are simple overwrites — gold tables are fully recomputed from silver
# each run, which makes them idempotent by construction.
#
# IMPORTANT: FAERS reports are spontaneous reports submitted to the FDA.
# Report frequency does NOT imply causality, drug safety, or relative risk.
# These tables describe reporting volume, not causal relationships.
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, countDistinct, date_format, round

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()


def build_reports_by_country(df_reports: DataFrame) -> DataFrame:
    """One row per country: unique report count.

    Source grain is one row per report, so count(*) would already be correct —
    countDistinct(report_id) is defense-in-depth against upstream grain changes.
    UNKNOWN countries are kept (they are real reports without a country).
    """
    return (
        df_reports.groupBy("country")
        .agg(countDistinct("report_id").alias("report_count"))
        .orderBy(col("report_count").desc())
    )


def build_top_drugs(df_drugs: DataFrame, df_reports: DataFrame) -> DataFrame:
    """One row per suspect drug: unique report count and average patient age.

    The join to fda_reports is on report_id, which is unique in that table, so
    the join does not fan out rows. countDistinct(report_id) guards against
    double counting if the child grain ever changes. Only drug_characterization=1 (Suspect)
    is included; concomitant/interacting drugs are excluded.
    """
    # Only role 1 (Suspect) is included in the ranking.
    suspect_drugs = df_drugs.filter(col("drug_characterization") == 1)
    return (
        suspect_drugs.join(df_reports, "report_id", "inner")
        .groupBy("drug_name")
        .agg(
            countDistinct("report_id").alias("report_count"),
            round(avg("patient_age_years"), 2).alias("avg_patient_age_years"),
        )
        .orderBy(col("report_count").desc())
    )


def build_reactions_over_time(df_reactions: DataFrame, df_reports: DataFrame) -> DataFrame:
    """One row per (month, adverse_reaction): unique report count.

    Reports whose date could not be parsed have a null report_month. They are
    EXCLUDED from the monthly trend (a null month row would be meaningless) and
    the exclusion is logged — the rows remain in silver, nothing is dropped
    upstream.
    """
    joined = (
        df_reactions.join(df_reports, "report_id", "inner")
        .withColumn("report_month", date_format(col("report_date"), "yyyy-MM"))
    )
    excluded = joined.filter(col("report_month").isNull()).count()
    if excluded:
        logger.warning(
            f"Excluded {excluded} reaction rows with null report_month from "
            "fda_reactions_over_time (report_date could not be parsed)"
        )
    return (
        joined.filter(col("report_month").isNotNull())
        .groupBy("report_month", "adverse_reaction")
        .agg(countDistinct("report_id").alias("report_count"))
        .orderBy(col("report_month").desc(), col("report_count").desc())
    )


if __name__ == "__main__":
    # create the gold schema if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

    # Read from the silver tables (grain-aware)
    df_reports = spark.table("workspace.silver.fda_reports")
    df_drugs = spark.table("workspace.silver.fda_report_drugs")
    df_reactions = spark.table("workspace.silver.fda_report_reactions")

    build_reports_by_country(df_reports).write.mode("overwrite").format("delta").saveAsTable(
        "workspace.gold.fda_reports_by_country"
    )
    build_top_drugs(df_drugs, df_reports).write.mode("overwrite").format("delta").saveAsTable(
        "workspace.gold.fda_top_drugs"
    )
    build_reactions_over_time(df_reactions, df_reports).write.mode("overwrite").format(
        "delta"
    ).saveAsTable("workspace.gold.fda_reactions_over_time")

    print("Gold tables created:")
    print("  - workspace.gold.fda_reports_by_country")
    print("  - workspace.gold.fda_top_drugs")
    print("  - workspace.gold.fda_reactions_over_time")

# COMMAND ----------

# DBTITLE 1,Gold — UC Comments
# Unity Catalog comments for governance and discoverability.
# These show up in Catalog Explorer and help other analysts understand the data model.


def comment_on(target: str, text: str) -> None:
    """Set a Unity Catalog comment on a table or column."""
    spark.sql(f"COMMENT ON {target} IS '{text}'")


# --- Silver: fda_reports (grain: one row per report) ---
comment_on("TABLE workspace.silver.fda_reports",
           "Silver layer: one row per FDA safety report. Contains report-level "
           "attributes only. No drug or reaction arrays.")
comment_on("COLUMN workspace.silver.fda_reports.report_id",
           "Unique safety report ID from openFDA (primary key)")
comment_on("COLUMN workspace.silver.fda_reports.report_date",
           "Date the report was received by FDA (parsed from yyyyMMdd)")
comment_on("COLUMN workspace.silver.fda_reports.country",
           "Reporter country from primarysource.reportercountry. UNKNOWN if not specified.")
comment_on("COLUMN workspace.silver.fda_reports.patient_sex",
           "MALE, FEMALE, or UNKNOWN (mapped from openFDA codes 1/2)")
comment_on("COLUMN workspace.silver.fda_reports.patient_age_raw",
           "Raw patient age value as reported (unit may vary — see patient_age_unit)")
comment_on("COLUMN workspace.silver.fda_reports.patient_age_unit",
           "FAERS age unit code (NCIT): 800=Decade, 801=Year, 802=Month, 803=Week, "
           "804=Day, 805=Hour, null=Unknown")
comment_on("COLUMN workspace.silver.fda_reports.patient_age_years",
           "Age normalized to years. Null when unit is unknown or not convertible.")
comment_on("COLUMN workspace.silver.fda_reports.report_version",
           "Report version from openFDA safetyreportversion (0 = legacy record "
           "without a version). The highest version wins during MERGE.")

# --- Silver: fda_report_drugs (grain: one row per drug per report) ---
comment_on("TABLE workspace.silver.fda_report_drugs",
           "Silver layer: one row per drug within a report. FK report_id links to "
           "fda_reports. No drug-to-reaction relationship is implied.")
comment_on("COLUMN workspace.silver.fda_report_drugs.report_id",
           "Foreign key to workspace.silver.fda_reports.report_id")
comment_on("COLUMN workspace.silver.fda_report_drugs.drug_name",
           "Brand or generic drug name (medicinalproduct field, uppercased and trimmed)")
comment_on("COLUMN workspace.silver.fda_report_drugs.drug_characterization",
           "Drug role: 1=Suspect, 2=Concomitant, 3=Interacting")

# --- Silver: fda_report_reactions (grain: one row per reaction per report) ---
comment_on("TABLE workspace.silver.fda_report_reactions",
           "Silver layer: one row per adverse reaction within a report. FK report_id "
           "links to fda_reports. No drug-to-reaction relationship is implied.")
comment_on("COLUMN workspace.silver.fda_report_reactions.report_id",
           "Foreign key to workspace.silver.fda_reports.report_id")
comment_on("COLUMN workspace.silver.fda_report_reactions.adverse_reaction",
           "Adverse reaction MedDRA preferred term (uppercased and trimmed)")

# --- Gold tables ---
comment_on("TABLE workspace.gold.fda_reports_by_country",
           "Gold layer: number of unique FDA reports grouped by reporter country. "
           "Report frequency does not imply causality or relative drug safety.")
comment_on("TABLE workspace.gold.fda_top_drugs",
           "Gold layer: role 1 (Suspect) drugs ranked by unique report count with average "
           "patient age. Concomitant/interacting drugs are excluded. Report frequency "
           "does not imply causality or relative drug safety.")
comment_on("TABLE workspace.gold.fda_reactions_over_time",
           "Gold layer: number of unique FDA reports per reaction per month. "
           "Excludes reports with an unparseable report_date. Report frequency "
           "does not imply causality or relative drug safety.")

print("Table and column comments applied.")