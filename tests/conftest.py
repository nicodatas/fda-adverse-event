"""Shared fixtures for the fda-adverse-event test suite.

Databricks notebooks are plain Python source files, but they cannot be imported
directly: module-level code would execute Databricks-specific side effects
(Spark sessions, volume reads, table DDL). Instead we parse the notebook with
`ast` and exec only the function definitions (and selected constants) that
the tests need into a controlled namespace.
"""
import ast
import json as jsonlib
import logging
import os
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"


def load_from_notebook(notebook_stem, functions=(), constants=(), overrides=None):
    """AST-extract `functions` and `constants` from a notebook source file.

    The extracted segments are compiled and exec'd into a namespace seeded with
    the modules the notebook functions depend on. `overrides` replaces
    module-level values (e.g. API_KEY) for the test.
    """
    source = (NOTEBOOKS_DIR / f"{notebook_stem}.py").read_text()
    tree = ast.parse(source)
    wanted_functions = set(functions)
    wanted_constants = set(constants)
    segments = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            segments.append(ast.get_source_segment(source, node))
            wanted_functions.discard(node.name)
        elif (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in wanted_constants
        ):
            segments.append(ast.get_source_segment(source, node))
            wanted_constants.discard(node.targets[0].id)
    missing = wanted_functions | wanted_constants
    assert not missing, f"Not found in {notebook_stem}.py: {sorted(missing)}"

    # Overrides must be seeded BEFORE exec: some module-level constants in
    # the notebooks (e.g. PAGE_SIZE, MAX_PAGES) are computed from values like
    # API_KEY, so the override has to be in the namespace while those
    # statements run.
    namespace = _base_namespace()
    if overrides:
        namespace.update(overrides)
    exec(compile(ast.parse("\n\n".join(segments)), f"<{notebook_stem}>", "exec"), namespace)
    return namespace


def _base_namespace():
    """Modules and objects the notebook functions depend on."""
    import requests

    namespace = {
        "json": jsonlib,
        "logging": logging,
        "os": os,
        "time": time,
        "Path": Path,
        "requests": requests,
        "logger": logging.getLogger("tests"),
    }
    try:
        from pyspark.sql import DataFrame, SparkSession
        from pyspark.sql.functions import (
            avg,
            coalesce,
            col,
            count,
            countDistinct,
            date_format,
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

        namespace.update({
            "DataFrame": DataFrame,
            "SparkSession": SparkSession,
            "avg": avg,
            "coalesce": coalesce,
            "col": col,
            "count": count,
            "countDistinct": countDistinct,
            "date_format": date_format,
            "explode": explode,
            "lit": lit,
            "round": round,
            "row_number": row_number,
            "to_date": to_date,
            "trim": trim,
            "upper": upper,
            "when": when,
            "_max": _max,
            "_sum": _sum,
            "Window": Window,
        })
    except ImportError:
        pass  # bronze-only tests run without pyspark installed
    try:
        from delta.tables import DeltaTable

        namespace["DeltaTable"] = DeltaTable
    except ImportError:
        pass
    return namespace


# ---------------------------------------------------------------------------
# Local Spark session with Delta Lake support (for silver/gold tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark_session():
    try:
        import pyspark  # noqa: F401
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as e:
        pytest.skip(f"pyspark/delta not installed: {e}")

    import tempfile

    warehouse = tempfile.mkdtemp(prefix="fda_tests_warehouse_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("fda-adverse-event-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse)
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def make_bronze_df(spark_session):
    """Factory building a bronze-like raw DataFrame from openFDA report dicts."""

    def _make(reports, tmp_path):
        from pyspark.sql.types import (
            ArrayType,
            StringType,
            StructField,
            StructType,
        )

        # An explicit, all-nullable schema guarantees that fields absent from
        # every test record (e.g. patientonsetage) still exist as nulls,
        # instead of disappearing from the inferred schema and breaking
        # downstream selects.
        schema = StructType([
            StructField("meta", StructType([])),
            StructField("results", ArrayType(StructType([
                StructField("safetyreportid", StringType()),
                StructField("receiptdate", StringType()),
                StructField("safetyreportversion", StringType()),
                StructField("primarysource", StructType([
                    StructField("reportercountry", StringType()),
                ])),
                StructField("patient", StructType([
                    StructField("patientsex", StringType()),
                    StructField("patientonsetage", StringType()),
                    StructField("patientonsetageunit", StringType()),
                    StructField("drug", ArrayType(StructType([
                        StructField("medicinalproduct", StringType()),
                        StructField("drugcharacterization", StringType()),
                    ]))),
                    StructField("reaction", ArrayType(StructType([
                        StructField("reactionmeddrapt", StringType()),
                    ]))),
                ])),
            ]))),
        ])
        page = tmp_path / "page_000.json"
        page.write_text(jsonlib.dumps({"meta": {}, "results": reports}))
        return (
            spark_session.read.option("multiline", "true")
            .schema(schema)
            .json(str(page))
        )

    return _make


# ---------------------------------------------------------------------------
# Notebook namespaces under test
# ---------------------------------------------------------------------------
BRONZE_FUNCTIONS = (
    "redact_secrets",
    "active_secrets",
    "_is_retryable",
    "fetch_page",
    "fetch_and_save_raw_data",
)
BRONZE_CONSTANTS = (
    "PAGE_SIZE",
    "MAX_PAGES",
    "RETRY_ATTEMPTS",
    "RETRY_BACKOFF",
    "REQUEST_TIMEOUT",
    "REDACTED",
)


@pytest.fixture
def bronze():
    """Bronze functions with no API key configured (unauthenticated profile)."""
    return load_from_notebook(
        "01_bronze_ingest",
        BRONZE_FUNCTIONS,
        BRONZE_CONSTANTS,
        overrides={"API_KEY": ""},
    )


@pytest.fixture
def bronze_with_key():
    """Bronze functions with an API key configured (authenticated profile)."""
    return load_from_notebook(
        "01_bronze_ingest",
        BRONZE_FUNCTIONS,
        BRONZE_CONSTANTS,
        overrides={"API_KEY": "SECRET123"},
    )


SILVER_FUNCTIONS = (
    "read_bronze_data",
    "extract_reports",
    "normalize_age",
    "extract_drugs",
    "extract_reactions",
    "keep_latest_version",
    "keep_children_of_latest_reports",
    "merge_to_delta",
)


@pytest.fixture
def silver(spark_session):
    namespace = load_from_notebook("02_silver_transform", SILVER_FUNCTIONS)
    namespace["spark"] = spark_session
    return namespace


SILVER_DQ_FUNCTIONS = (
    "compute_dq_metrics",
    "enforce_hard_gates",
    "warn_soft_metrics",
)


@pytest.fixture
def silver_dq():
    return load_from_notebook(
        "02_silver_transform",
        SILVER_DQ_FUNCTIONS,
        constants=("HARD_GATE_CHECKS",),
    )


GOLD_FUNCTIONS = (
    "build_reports_by_country",
    "build_top_drugs",
    "build_reactions_over_time",
)


@pytest.fixture
def gold():
    return load_from_notebook("03_gold_aggregate", GOLD_FUNCTIONS)


# ---------------------------------------------------------------------------
# Fresh silver-shaped Delta tables (for MERGE tests)
# ---------------------------------------------------------------------------
@pytest.fixture
def silver_tables(spark_session, silver):
    """Empty Delta tables with the silver schemas in a throwaway database."""
    import uuid

    db = f"test_silver_{uuid.uuid4().hex[:8]}"
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    names = {
        "reports": f"{db}.reports",
        "drugs": f"{db}.drugs",
        "reactions": f"{db}.reactions",
    }
    spark_session.sql(f"""
        CREATE TABLE {names['reports']} (
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
    spark_session.sql(f"""
        CREATE TABLE {names['drugs']} (
            report_id STRING,
            drug_name STRING,
            drug_characterization INT
        ) USING DELTA
    """)
    spark_session.sql(f"""
        CREATE TABLE {names['reactions']} (
            report_id STRING,
            adverse_reaction STRING
        ) USING DELTA
    """)
    yield names
    spark_session.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
