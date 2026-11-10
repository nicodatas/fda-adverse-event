"""Silver transformation tests: normalization, versioning, child-row filtering.

Runs against a local Spark session (conftest.spark_session); raw input is
built from openFDA-shaped dicts via the make_bronze_df factory.
"""
import datetime

import pytest
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

RAW_REPORTS = [
    {
        "safetyreportid": "100",
        "receiptdate": "20240115",
        "safetyreportversion": "1",
        "primarysource": {"reportercountry": "canada"},
        "patient": {
            "patientsex": "1",
            "patientonsetage": "3",
            "patientonsetageunit": "800",
            "drug": [
                {"medicinalproduct": " drug a ", "drugcharacterization": "1"},
                {"medicinalproduct": "", "drugcharacterization": "2"},
                {"medicinalproduct": "   ", "drugcharacterization": "3"},
            ],
            "reaction": [
                {"reactionmeddrapt": " Headache "},
                {"reactionmeddrapt": ""},
                {"reactionmeddrapt": "   "},
            ],
        },
    },
    {
        "safetyreportid": "100",
        "receiptdate": "20240115",
        "safetyreportversion": "2",
        "primarysource": {"reportercountry": "CANADA"},
        "patient": {
            "patientsex": "1",
            "drug": [
                {"medicinalproduct": "DRUG A"},
                {"medicinalproduct": "DRUG C"},
            ],
            "reaction": [{"reactionmeddrapt": "Headache"}],
        },
    },
    {
        "safetyreportid": "200",
        "receiptdate": "20240201",
        "primarysource": {"reportercountry": ""},
        "patient": {
            "patientsex": "2",
            "patientonsetage": "6",
            "patientonsetageunit": "802",
            "drug": [{"medicinalproduct": "DRUG A", "drugcharacterization": "1"}],
            "reaction": [{"reactionmeddrapt": "Headache"}],
        },
    },
    {
        "safetyreportid": "300",
        "primarysource": {"reportercountry": "   "},
        "patient": {
            "patientsex": "0",
            "drug": [{"medicinalproduct": "Drug B"}],
            "reaction": [{"reactionmeddrapt": "Rash"}],
        },
    },
    {
        "safetyreportid": None,
        "primarysource": {"reportercountry": "US"},
        "patient": {
            "drug": [{"medicinalproduct": "X"}],
            "reaction": [{"reactionmeddrapt": "Y"}],
        },
    },
]


def _extract_reports(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS, tmp_path)
    return silver["extract_reports"](raw)


# -- extract_reports ---------------------------------------------------------
def test_extract_reports_drops_null_report_ids(silver, make_bronze_df, tmp_path):
    reports = _extract_reports(silver, make_bronze_df, tmp_path).collect()
    ids = sorted({r["report_id"] for r in reports})
    assert ids == ["100", "200", "300"]


def test_raw_validation_fails_before_null_report_id_is_filtered(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS, tmp_path)
    with pytest.raises(RuntimeError, match="null safetyreportid"):
        silver["validate_raw_input"](raw)


def test_raw_validation_counts_blank_children_before_extraction(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS[:-1], tmp_path)
    metrics = silver["validate_raw_input"](raw)
    assert metrics["null_report_ids"] == 0
    assert metrics["blank_drug_names"] == 2
    assert metrics["blank_reaction_names"] == 2


def test_drug_role_conflict_uses_documented_priority(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df([{
        "safetyreportid": "R1",
        "patient": {"drug": [
            {"medicinalproduct": "Drug A", "drugcharacterization": "2"},
            {"medicinalproduct": "DRUG A", "drugcharacterization": "1"},
            {"medicinalproduct": "Drug A", "drugcharacterization": "3"},
        ]},
    }], tmp_path)
    rows = silver["extract_drugs"](raw).collect()
    assert len(rows) == 1
    assert rows[0]["drug_name"] == "DRUG A"
    assert rows[0]["drug_characterization"] == 1


def test_extract_reports_normalizes_country(silver, make_bronze_df, tmp_path):
    rows = _extract_reports(silver, make_bronze_df, tmp_path).collect()
    countries = {r["report_id"]: r["country"] for r in rows}
    assert countries["100"] == "CANADA"  # trimmed + uppercased
    assert countries["200"] == "UNKNOWN"  # empty string
    assert countries["300"] == "UNKNOWN"  # whitespace-only


def test_extract_reports_maps_sex(silver, make_bronze_df, tmp_path):
    rows = _extract_reports(silver, make_bronze_df, tmp_path).collect()
    sexes = {r["report_id"]: r["patient_sex"] for r in rows}
    assert sexes["100"] == "MALE"
    assert sexes["200"] == "FEMALE"
    assert sexes["300"] == "UNKNOWN"  # code 0 is neither 1 nor 2


def test_extract_reports_dates_and_version_defaults(silver, make_bronze_df, tmp_path):
    rows = _extract_reports(silver, make_bronze_df, tmp_path).collect()
    by_id = {}
    for r in rows:
        by_id.setdefault(r["report_id"], []).append(r)
    assert by_id["200"][0]["report_date"] == datetime.date(2024, 2, 1)
    assert by_id["300"][0]["report_date"] is None  # receiptdate missing -> null
    versions_100 = sorted(r["report_version"] for r in by_id["100"])
    assert versions_100 == [1, 2]
    assert by_id["300"][0]["report_version"] == 0  # missing version defaults to 0


# -- normalize_age -----------------------------------------------------------
AGE_CASES = [
    ("800", "3", 30.0),  # decade
    ("801", "42", 42.0),  # year
    ("802", "6", 0.5),  # month
    ("803", "52", 1.0),  # week
    ("804", "730.5", 2.0),  # day
    ("805", "5", None),  # hour: excluded by design (neonates only)
    ("999", "40", None),  # unknown unit code
    (None, "40", None),  # missing unit
    ("801", "abc", None),  # non-numeric age
]


@pytest.mark.parametrize("unit,raw,expected", AGE_CASES)
def test_normalize_age_conversions(silver, spark_session, unit, raw, expected):
    df = spark_session.createDataFrame(
        [("R1", raw, unit)],
        StructType([
            StructField("report_id", StringType()),
            StructField("patient_age_raw", StringType()),
            StructField("patient_age_unit", StringType()),
        ]),
    )
    out = silver["normalize_age"](df).collect()
    if expected is None:
        assert out[0]["patient_age_years"] is None
    else:
        assert out[0]["patient_age_years"] == pytest.approx(expected)


# -- extract_drugs / extract_reactions ---------------------------------------
def test_extract_drugs_filters_blanks_and_tags_version(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS, tmp_path)
    rows = silver["extract_drugs"](raw).collect()
    triples = {(r["report_id"], r["report_version"], r["drug_name"]) for r in rows}
    # blank/whitespace-only names are dropped; names trimmed + uppercased
    assert ("100", 1, "DRUG A") in triples
    assert ("100", 2, "DRUG A") in triples
    assert ("100", 2, "DRUG C") in triples
    assert ("200", 0, "DRUG A") in triples
    assert ("300", 0, "DRUG B") in triples
    # no blank/null drug name and no row without a report_id survives
    assert all(r["drug_name"] for r in rows)
    assert all(r["report_id"] for r in rows)
    # characterization is cast to int
    row_200 = next(r for r in rows if r["report_id"] == "200")
    assert row_200["drug_characterization"] == 1


def test_extract_reactions_filters_blanks_and_tags_version(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS, tmp_path)
    rows = silver["extract_reactions"](raw).collect()
    triples = {(r["report_id"], r["report_version"], r["adverse_reaction"]) for r in rows}
    assert ("100", 1, "HEADACHE") in triples
    assert ("100", 2, "HEADACHE") in triples
    assert ("200", 0, "HEADACHE") in triples
    assert ("300", 0, "RASH") in triples
    assert all(r["adverse_reaction"] for r in rows)


# -- version collapse --------------------------------------------------------
def test_keep_latest_version_picks_highest_version(silver, make_bronze_df, tmp_path):
    reports = _extract_reports(silver, make_bronze_df, tmp_path)
    latest = silver["keep_latest_version"](reports, ["report_id"]).collect()
    by_id = {r["report_id"]: r for r in latest}
    assert set(by_id) == {"100", "200", "300"}
    assert by_id["100"]["report_version"] == 2
    assert by_id["300"]["report_version"] == 0


def test_keep_latest_version_breaks_ties_on_report_date(silver, spark_session):
    df = spark_session.createDataFrame(
        [
            ("R", 1, datetime.date(2024, 1, 1), "OLD"),
            ("R", 1, datetime.date(2024, 6, 1), "NEW"),
        ],
        ["report_id", "report_version", "report_date", "country"],
    )
    out = silver["keep_latest_version"](df, ["report_id"]).collect()
    assert len(out) == 1
    assert out[0]["country"] == "NEW"


def test_keep_children_of_latest_reports(silver, make_bronze_df, tmp_path):
    raw = make_bronze_df(RAW_REPORTS, tmp_path)
    reports_all = silver["extract_reports"](raw)
    drugs_all = silver["extract_drugs"](raw)
    drugs = silver["keep_children_of_latest_reports"](drugs_all, reports_all)
    pairs = {(r["report_id"], r["drug_name"]) for r in drugs.collect()}
    # report 100's latest version (2) contains DRUG A and DRUG C
    assert ("100", "DRUG A") in pairs
    assert ("100", "DRUG C") in pairs
    assert ("200", "DRUG A") in pairs
    assert ("300", "DRUG B") in pairs
    # the version tag is dropped: persisted grain is (report_id, drug_name)
    assert "report_version" not in drugs.columns
