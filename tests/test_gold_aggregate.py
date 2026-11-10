"""Gold aggregate tests: distinct-count semantics, join fan-out safety,
and null-date exclusion from the monthly trend.
"""
import datetime

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

REPORTS_SCHEMA = StructType([
    StructField("report_id", StringType()),
    StructField("report_date", DateType()),
    StructField("country", StringType()),
    StructField("patient_sex", StringType()),
    StructField("patient_age_raw", StringType()),
    StructField("patient_age_unit", StringType()),
    StructField("patient_age_years", DoubleType()),
    StructField("report_version", IntegerType()),
])

DRUGS_SCHEMA = StructType([
    StructField("report_id", StringType()),
    StructField("drug_name", StringType()),
    StructField("drug_characterization", IntegerType()),
])

REACTIONS_SCHEMA = StructType([
    StructField("report_id", StringType()),
    StructField("adverse_reaction", StringType()),
])

D_JAN = datetime.date(2024, 1, 15)
D_JAN2 = datetime.date(2024, 1, 20)
D_FEB = datetime.date(2024, 2, 1)


def _tables(spark):
    reports = spark.createDataFrame(
        [("R1", D_JAN, "US", "MALE", "30", "801", 30.0, 1),
         ("R2", D_FEB, "UNKNOWN", "FEMALE", None, None, None, 0),
         ("R3", None, "CA", "MALE", "50", "801", 50.0, 0),  # unparseable date
         ("R4", D_JAN2, "US", "FEMALE", "60", "801", 60.0, 0)],
        REPORTS_SCHEMA)
    drugs = spark.createDataFrame(
        [("R1", "DRUG A", 1), ("R1", "DRUG B", 2),
         ("R2", "DRUG A", 1),
         ("R3", "DRUG C", 1),
         ("R4", "DRUG A", 1)],
        DRUGS_SCHEMA)
    reactions = spark.createDataFrame(
        [("R1", "HEADACHE"), ("R1", "NAUSEA"), ("R1", "RASH"),
         ("R2", "HEADACHE"),
         ("R3", "RASH")],
        REACTIONS_SCHEMA)
    return reports, drugs, reactions


def test_reports_by_country_counts_unique_reports(gold, spark_session):
    reports, _, _ = _tables(spark_session)
    rows = gold["build_reports_by_country"](reports).collect()
    by_country = {r["country"]: r["report_count"] for r in rows}
    assert by_country == {"US": 2, "UNKNOWN": 1, "CA": 1}
    # ordered by report_count descending
    counts = [r["report_count"] for r in rows]
    assert counts == sorted(counts, reverse=True)


def test_top_drugs_no_fanout_double_count(gold, spark_session):
    reports, drugs, _ = _tables(spark_session)
    rows = gold["build_top_drugs"](drugs, reports).collect()
    by_drug = {r["drug_name"]: r for r in rows}
    # R1 has 2 drugs and 3 reactions, but each drug is counted once per report.
    # DRUG A appears in R1, R2, R4 -> 3 unique reports, not 3 x reactions.
    assert by_drug["DRUG A"]["report_count"] == 3
    # avg age ignores nulls: (30 + 60) / 2
    assert by_drug["DRUG A"]["avg_patient_age_years"] == 45.0
    assert by_drug["DRUG B"]["report_count"] == 1
    assert by_drug["DRUG C"]["report_count"] == 1


def test_reactions_over_time_counts_unique_reports_per_month(gold, spark_session):
    reports, _, reactions = _tables(spark_session)
    rows = gold["build_reactions_over_time"](reactions, reports).collect()
    by_month = {}
    for r in rows:
        by_month.setdefault(r["report_month"], {})[r["adverse_reaction"]] = r["report_count"]
    # R1 (January) has 3 reactions: each counted once for that month
    assert by_month["2024-01"] == {"HEADACHE": 1, "NAUSEA": 1, "RASH": 1}
    assert by_month["2024-02"] == {"HEADACHE": 1}
    # R3's date could not be parsed: no null-month row is produced
    assert set(by_month) == {"2024-01", "2024-02"}


def test_null_month_rows_excluded_and_logged(gold, spark_session, caplog):
    reports, _, reactions = _tables(spark_session)
    with caplog.at_level("WARNING"):
        gold["build_reactions_over_time"](reactions, reports).collect()
    assert "null report_month" in caplog.text
    assert "Excluded 1" in caplog.text  # R3's single reaction row


def test_reactions_over_time_orders_month_desc(gold, spark_session):
    reports, _, reactions = _tables(spark_session)
    rows = gold["build_reactions_over_time"](reactions, reports).collect()
    first = rows[0]
    assert (first["report_month"], first["adverse_reaction"]) == ("2024-02", "HEADACHE")
