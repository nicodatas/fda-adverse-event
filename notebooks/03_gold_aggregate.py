# Databricks notebook source
# DBTITLE 1,Gold Layer Title
# MAGIC %md
# MAGIC # Gold Layer — Aggregated Tables & Governance
# MAGIC
# MAGIC Builds business-ready aggregate tables from the silver layer using `countDistinct(report_id)` for report-count metrics. Reads from the three silver tables (`fda_reports`, `fda_report_drugs`, `fda_report_reactions`) and joins where needed.
# MAGIC
# MAGIC **Disclaimer:** FAERS reports are spontaneous. Report frequency does not imply causality or relative drug safety. These tables describe reporting volume, not risk.

# COMMAND ----------

# DBTITLE 1,Gold — Aggregated Tables
# Gold layer: pre-aggregated tables ready for dashboards and reporting
# these are simple overwrites — gold tables are recomputed from silver each run
#
# IMPORTANT: FAERS reports are spontaneous reports submitted to the FDA.
# Report frequency does NOT imply causality, drug safety, or relative risk.
# These tables describe reporting volume, not causal relationships.
from pyspark.sql.functions import col, countDistinct, round, avg, date_format

# create the gold schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# Read from the new silver tables (grain-aware)
df_reports = spark.table("workspace.silver.fda_reports")
df_drugs = spark.table("workspace.silver.fda_report_drugs")
df_reactions = spark.table("workspace.silver.fda_report_reactions")

# 1. Reports by country — one row per country
# Source: fda_reports (grain: one row per report)
# countDistinct ensures we count unique reports even if grain changes upstream
(df_reports.groupBy("country")
    .agg(countDistinct("report_id").alias("report_count"))
    .orderBy(col("report_count").desc())
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.gold.fda_reports_by_country"))

# 2. Top drugs by report volume — one row per drug
# Source: fda_report_drugs joined with fda_reports for patient age
# countDistinct(report_id) counts unique reports per drug, not drug-row occurrences
(df_drugs.join(df_reports, "report_id", "inner")
    .groupBy("drug_name")
    .agg(
        countDistinct("report_id").alias("report_count"),
        round(avg("patient_age_years"), 2).alias("avg_patient_age_years"),
    )
    .orderBy(col("report_count").desc())
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.gold.fda_top_drugs"))

# 3. Reactions over time — one row per (month, reaction)
# Source: fda_report_reactions joined with fda_reports for report_date
# countDistinct(report_id) counts unique reports per reaction per month
(df_reactions.join(df_reports, "report_id", "inner")
    .withColumn("report_month", date_format(col("report_date"), "yyyy-MM"))
    .groupBy("report_month", "adverse_reaction")
    .agg(countDistinct("report_id").alias("report_count"))
    .orderBy(col("report_month").desc(), col("report_count").desc())
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.gold.fda_reactions_over_time"))

print("Gold tables created:")
print("  - workspace.gold.fda_reports_by_country")
print("  - workspace.gold.fda_top_drugs")
print("  - workspace.gold.fda_reactions_over_time")

# COMMAND ----------

# DBTITLE 1,Gold — UC Comments
# Unity Catalog comments for governance and discoverability
# These show up in Catalog Explorer and help other analysts understand the data model.

# --- Silver: fda_reports (grain: one row per report) ---
spark.sql("COMMENT ON TABLE workspace.silver.fda_reports IS 'Silver layer: one row per FDA safety report. Contains report-level attributes only. No drug or reaction arrays.'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.report_id IS 'Unique safety report ID from openFDA (primary key)'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.report_date IS 'Date the report was received by FDA (parsed from yyyyMMdd)'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.country IS 'Reporter country from primarysource.reportercountry. UNKNOWN if not specified.'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.patient_sex IS 'MALE, FEMALE, or UNKNOWN (mapped from openFDA codes 1/2)'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.patient_age_raw IS 'Raw patient age value as reported (unit may vary — see patient_age_unit)'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.patient_age_unit IS 'FAERS age unit code (NCIT): 800=Decade, 801=Year, 802=Month, 803=Week, 804=Day, 805=Hour, null=Unknown'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_reports.patient_age_years IS 'Age normalized to years. Null when unit is unknown or not convertible.'")

# --- Silver: fda_report_drugs (grain: one row per drug per report) ---
spark.sql("COMMENT ON TABLE workspace.silver.fda_report_drugs IS 'Silver layer: one row per drug within a report. FK report_id links to fda_reports. No drug-to-reaction relationship is implied.'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_report_drugs.report_id IS 'Foreign key to workspace.silver.fda_reports.report_id'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_report_drugs.drug_name IS 'Brand or generic drug name (medicinalproduct field, uppercased and trimmed)'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_report_drugs.drug_characterization IS 'Drug role: 1=Suspect, 2=Concomitant, 3=Interacting'")

# --- Silver: fda_report_reactions (grain: one row per reaction per report) ---
spark.sql("COMMENT ON TABLE workspace.silver.fda_report_reactions IS 'Silver layer: one row per adverse reaction within a report. FK report_id links to fda_reports. No drug-to-reaction relationship is implied.'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_report_reactions.report_id IS 'Foreign key to workspace.silver.fda_reports.report_id'")
spark.sql("COMMENT ON COLUMN workspace.silver.fda_report_reactions.adverse_reaction IS 'Adverse reaction MedDRA preferred term (uppercased and trimmed)'")

# --- Gold tables ---
spark.sql("COMMENT ON TABLE workspace.gold.fda_reports_by_country IS 'Gold layer: number of unique FDA reports grouped by reporter country. Report frequency does not imply causality or relative drug safety.'")
spark.sql("COMMENT ON TABLE workspace.gold.fda_top_drugs IS 'Gold layer: drugs ranked by unique report count with average patient age. Report frequency does not imply causality or relative drug safety.'")
spark.sql("COMMENT ON TABLE workspace.gold.fda_reactions_over_time IS 'Gold layer: number of unique FDA reports per reaction per month. Report frequency does not imply causality or relative drug safety.'")

print("Table and column comments applied.")