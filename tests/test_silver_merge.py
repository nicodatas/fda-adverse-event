"""Silver Delta MERGE tests: idempotency, version guard, child-table sync.

Each test runs against fresh silver-shaped Delta tables (conftest.silver_tables)
so MERGE semantics are exercised against a real Delta transaction log.
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

D1 = datetime.date(2024, 1, 15)


def _reports_df(spark, rows):
    return spark.createDataFrame(rows, REPORTS_SCHEMA)


def _drugs_df(spark, rows):
    return spark.createDataFrame(rows, DRUGS_SCHEMA)


def _merge_reports(silver, df, table):
    silver["merge_to_delta"](
        df, table, "target.report_id = source.report_id", ["report_id"],
        version_col="report_version",
    )


def _merge_drugs(silver, df, table, delete=True):
    silver["merge_to_delta"](
        df, table,
        "target.report_id = source.report_id AND target.drug_name = source.drug_name",
        ["report_id", "drug_name"],
        delete_not_matched_by_source=delete,
    )


def test_reports_merge_is_idempotent(silver, silver_tables):
    spark = silver["spark"]
    reports = silver_tables["reports"]
    df = _reports_df(spark, [("R1", D1, "US", "MALE", "40", "801", 40.0, 1)])
    _merge_reports(silver, df, reports)
    _merge_reports(silver, df, reports)  # same source twice
    assert spark.table(reports).count() == 1


def test_source_dedup_is_stable_for_identical_child_rows(silver, silver_tables):
    spark = silver["spark"]
    drugs = silver_tables["drugs"]
    # Repeated ingestion of the same resolved child row must be idempotent.
    df = _drugs_df(spark, [("R1", "DRUG A", 1), ("R1", "DRUG A", 1)])
    _merge_drugs(silver, df, drugs)
    rows = spark.table(drugs).collect()
    assert len(rows) == 1
    assert rows[0]["drug_characterization"] == 1


def test_older_version_never_overwrites_newer(silver, silver_tables):
    spark = silver["spark"]
    reports = silver_tables["reports"]
    v2 = _reports_df(spark, [("R1", D1, "US", "MALE", "40", "801", 40.0, 2)])
    v1 = _reports_df(spark, [("R1", D1, "CANADA", "FEMALE", "41", "801", 41.0, 1)])
    _merge_reports(silver, v2, reports)
    _merge_reports(silver, v1, reports)  # late-arriving older version
    rows = spark.table(reports).collect()
    assert len(rows) == 1
    assert rows[0]["country"] == "US"
    assert rows[0]["report_version"] == 2


def test_same_version_update_is_allowed(silver, silver_tables):
    spark = silver["spark"]
    reports = silver_tables["reports"]
    v1a = _reports_df(spark, [("R1", D1, "US", "MALE", "40", "801", 40.0, 1)])
    v1b = _reports_df(spark, [("R1", D1, "USA", "MALE", "40", "801", 40.0, 1)])
    _merge_reports(silver, v1a, reports)
    _merge_reports(silver, v1b, reports)  # equal version: update permitted
    rows = spark.table(reports).collect()
    assert len(rows) == 1
    assert rows[0]["country"] == "USA"


def test_child_sync_removes_obsolete_drugs(silver, silver_tables):
    spark = silver["spark"]
    drugs = silver_tables["drugs"]
    seed = _drugs_df(
        spark, [("R1", "DRUG A", 1), ("R1", "DRUG B", 1), ("R2", "DRUG X", 1)]
    )
    _merge_drugs(silver, seed, drugs)
    # a newer version of R1 dropped DRUG A; R2 is untouched
    update = _drugs_df(spark, [("R1", "DRUG B", 1), ("R2", "DRUG X", 1)])
    _merge_drugs(silver, update, drugs)
    rows = {(r["report_id"], r["drug_name"]) for r in spark.table(drugs).collect()}
    assert rows == {("R1", "DRUG B"), ("R2", "DRUG X")}


def test_reports_merge_keeps_rows_missing_from_source(silver, silver_tables):
    spark = silver["spark"]
    reports = silver_tables["reports"]
    seed = _reports_df(
        spark,
        [("R1", D1, "US", "MALE", "40", "801", 40.0, 1),
         ("R2", D1, "CA", "FEMALE", "41", "801", 41.0, 1)],
    )
    _merge_reports(silver, seed, reports)
    # a partial source (only R1) must NOT delete R2: the reports MERGE is
    # upsert-only; delete-not-matched-by-source is reserved for child tables
    partial = _reports_df(spark, [("R1", D1, "US", "MALE", "40", "801", 40.0, 2)])
    _merge_reports(silver, partial, reports)
    assert spark.table(reports).count() == 2


VERSIONED_RAW = [
    {
        "safetyreportid": "100",
        "receiptdate": "20240115",
        "safetyreportversion": "1",
        "primarysource": {"reportercountry": "US"},
        "patient": {
            "patientsex": "1",
            "drug": [{"medicinalproduct": "DRUG A"}, {"medicinalproduct": "DRUG B"}],
            "reaction": [{"reactionmeddrapt": "HEADACHE"}],
        },
    },
    {
        "safetyreportid": "100",
        "receiptdate": "20240120",
        "safetyreportversion": "2",
        "primarysource": {"reportercountry": "US"},
        "patient": {
            "patientsex": "1",
            "drug": [{"medicinalproduct": "DRUG B"}],
            "reaction": [{"reactionmeddrapt": "HEADACHE"}],
        },
    },
    {
        "safetyreportid": "200",
        "receiptdate": "20240201",
        "primarysource": {"reportercountry": "CA"},
        "patient": {
            "patientsex": "2",
            "drug": [{"medicinalproduct": "DRUG B"}],
            "reaction": [{"reactionmeddrapt": "NAUSEA"}],
        },
    },
]


def test_end_to_end_version_flow(silver, silver_tables, make_bronze_df, tmp_path):
    """Full silver flow: extract -> latest version -> MERGE (twice for
    idempotency) -> child tables hold ONLY the latest version's rows."""
    spark = silver["spark"]
    raw = make_bronze_df(VERSIONED_RAW, tmp_path)

    reports_all = silver["extract_reports"](raw).transform(silver["normalize_age"])
    drugs_all = silver["extract_drugs"](raw)
    reactions_all = silver["extract_reactions"](raw)

    reports = silver["keep_latest_version"](reports_all, ["report_id"])
    drugs = silver["keep_children_of_latest_reports"](drugs_all, reports_all)
    reactions = silver["keep_children_of_latest_reports"](reactions_all, reports_all)

    t = silver_tables
    for _ in range(2):  # run the whole merge sequence twice
        _merge_reports(silver, reports, t["reports"])
        _merge_drugs(silver, drugs, t["drugs"])
        silver["merge_to_delta"](
            reactions, t["reactions"],
            "target.report_id = source.report_id AND "
            "target.adverse_reaction = source.adverse_reaction",
            ["report_id", "adverse_reaction"],
            delete_not_matched_by_source=True,
        )

    rep = {(r["report_id"], r["report_version"], r["country"])
           for r in spark.table(t["reports"]).collect()}
    assert rep == {("100", 2, "US"), ("200", 0, "CA")}
    dru = {(r["report_id"], r["drug_name"]) for r in spark.table(t["drugs"]).collect()}
    assert dru == {("100", "DRUG B"), ("200", "DRUG B")}  # v1's DRUG A purged
    rea = {(r["report_id"], r["adverse_reaction"])
           for r in spark.table(t["reactions"]).collect()}
    assert rea == {("100", "HEADACHE"), ("200", "NAUSEA")}
