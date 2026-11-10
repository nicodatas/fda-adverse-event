"""Silver data-quality gate tests.

Hard gates (null/duplicate report_id, orphan children) fail the pipeline;
soft anomalies (null dates, invalid sex/age, blank names) only log warnings.
compute_dq_metrics is exercised against real DataFrames.
"""
import datetime

import pytest
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

GATES = ("null_report_ids", "duplicate_report_ids", "orphan_drugs", "orphan_reactions")


def _clean_tables(spark):
    reports = spark.createDataFrame(
        [("R1", datetime.date(2024, 1, 1), "US", "MALE", "40", "801", 40.0, 1),
         ("R2", datetime.date(2024, 2, 1), "CA", "FEMALE", "30", "801", 30.0, 0)],
        REPORTS_SCHEMA)
    drugs = spark.createDataFrame(
        [("R1", "DRUG A", 1), ("R2", "DRUG B", 2)], DRUGS_SCHEMA)
    reactions = spark.createDataFrame(
        [("R1", "HEADACHE"), ("R2", "NAUSEA")], REACTIONS_SCHEMA)
    return reports, drugs, reactions


# -- hard gates ---------------------------------------------------------------
def test_each_hard_gate_raises_when_violated(silver_dq):
    for gate in GATES:
        metrics = dict.fromkeys(GATES, 0)
        metrics[gate] = 1
        with pytest.raises(RuntimeError, match=gate):
            silver_dq["enforce_hard_gates"](metrics)


def test_clean_metrics_pass_all_hard_gates(silver_dq):
    silver_dq["enforce_hard_gates"](dict.fromkeys(GATES, 0))  # must not raise


def test_soft_metrics_log_but_do_not_block(silver_dq, caplog):
    metrics = {"null_dates": 5, "invalid_sex": 1, "invalid_age": 2,
               "null_drug_names": 0, "null_reaction_names": 0}
    with caplog.at_level("WARNING"):
        silver_dq["warn_soft_metrics"](metrics)
    assert "null_dates" in caplog.text
    assert "invalid_age" in caplog.text
    assert "null_drug_names" not in caplog.text  # zero -> not logged
    # soft failures never raise
    silver_dq["enforce_hard_gates"]({**metrics, **dict.fromkeys(GATES, 0)})


# -- compute_dq_metrics against DataFrames ------------------------------------
def test_compute_dq_metrics_clean_tables(silver_dq, spark_session):
    reports, drugs, reactions = _clean_tables(spark_session)
    m = silver_dq["compute_dq_metrics"](reports, drugs, reactions)
    assert m["total_reports"] == 2
    assert m["total_drugs"] == 2
    assert m["total_reactions"] == 2
    for gate in GATES:
        assert m[gate] == 0
    assert m["null_dates"] == 0
    assert m["invalid_sex"] == 0
    assert m["invalid_age"] == 0
    assert m["null_drug_names"] == 0
    assert m["null_reaction_names"] == 0
    assert m["duplicate_drugs"] == 0
    assert m["duplicate_reactions"] == 0


def test_compute_dq_metrics_detects_soft_anomalies(silver_dq, spark_session):
    reports = spark_session.createDataFrame(
        [("R1", None, "US", "ALIEN", "40", "801", 999.0, 1),
         ("R2", datetime.date(2024, 2, 1), "CA", "FEMALE", None, None, None, 0)],
        REPORTS_SCHEMA)
    drugs = spark_session.createDataFrame(
        [("R1", "DRUG A", 1), ("R1", "", 2), ("R2", None, 1)], DRUGS_SCHEMA)
    reactions = spark_session.createDataFrame(
        [("R1", "HEADACHE"), ("R1", ""), ("R2", None)], REACTIONS_SCHEMA)
    m = silver_dq["compute_dq_metrics"](reports, drugs, reactions)
    assert m["null_dates"] == 1
    assert m["invalid_sex"] == 1
    assert m["invalid_age"] == 1
    assert m["null_drug_names"] == 2
    assert m["null_reaction_names"] == 2


def test_compute_dq_metrics_detects_duplicates_and_orphans(silver_dq, spark_session):
    reports = spark_session.createDataFrame(
        [("R1", datetime.date(2024, 1, 1), "US", "MALE", "40", "801", 40.0, 1),
         ("R1", datetime.date(2024, 1, 2), "US", "MALE", "40", "801", 40.0, 1)],
        REPORTS_SCHEMA)
    drugs = spark_session.createDataFrame(
        [("R1", "DRUG A", 1), ("R1", "DRUG A", 2), ("R9", "ORPHAN DRUG", 1)],
        DRUGS_SCHEMA)
    reactions = spark_session.createDataFrame(
        [("R9", "ORPHAN REACTION")], REACTIONS_SCHEMA)
    m = silver_dq["compute_dq_metrics"](reports, drugs, reactions)
    assert m["duplicate_report_ids"] == 1  # 2 rows - 1 distinct id
    assert m["duplicate_drugs"] == 1
    assert m["orphan_drugs"] == 1
    assert m["orphan_reactions"] == 1
