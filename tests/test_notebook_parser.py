"""Tests for NotebookIOParser — I/O discovery from Databricks notebook source code."""

import pytest
from pathlib import Path

from ade_app.platforms.databricks.notebook_parser import NotebookIOParser, NotebookParseResult

DEMO_DIR = Path(__file__).resolve().parent.parent / "ade_data/demo/inputs/databricks"


@pytest.fixture
def parser():
    return NotebookIOParser()


@pytest.fixture
def ingest_result(parser):
    return parser.parse_file(DEMO_DIR / "acme_corp/bronze/01_ingest_raw_sales.py")


@pytest.fixture
def clean_result(parser):
    return parser.parse_file(DEMO_DIR / "acme_corp/silver/02_clean_sales.py")


@pytest.fixture
def aggregate_result(parser):
    return parser.parse_file(DEMO_DIR / "acme_corp/gold/03_aggregate_sales.py")


@pytest.fixture
def enrich_result(parser):
    return parser.parse_file(DEMO_DIR / "acme_corp/silver/04_enrich_customers.py")


@pytest.fixture
def utilities_result(parser):
    return parser.parse_file(DEMO_DIR / "acme_corp/shared/utilities.py")


class TestParseFile:
    def test_returns_parse_result(self, ingest_result):
        assert isinstance(ingest_result, NotebookParseResult)

    def test_notebook_name(self, ingest_result):
        assert ingest_result.notebook_name == "01_ingest_raw_sales"

    def test_has_source_code(self, ingest_result):
        assert len(ingest_result.source_code) > 0

    def test_file_not_found(self, parser):
        with pytest.raises(FileNotFoundError):
            parser.parse_file("/nonexistent/path.py")


class TestIngestNotebook:
    """01_ingest_raw_sales: reads /mnt/raw/sales/, writes bronze.raw_sales"""

    def test_input_file(self, ingest_result):
        input_names = [i.name for i in ingest_result.inputs]
        assert "/mnt/raw/sales/" in input_names

    def test_output_table(self, ingest_result):
        output_names = [o.name for o in ingest_result.outputs]
        assert "bronze.raw_sales" in output_names

    def test_output_variable_resolved(self, ingest_result):
        # TARGET_TABLE = "bronze.raw_sales" -> .saveAsTable(TARGET_TABLE)
        output = [o for o in ingest_result.outputs if o.name == "bronze.raw_sales"][0]
        assert output.confidence == "high"

    def test_no_python_imports_in_inputs(self, ingest_result):
        input_names = [i.name for i in ingest_result.inputs]
        for name in input_names:
            assert not name.startswith("pyspark.")
            assert not name.startswith("pandas.")


class TestCleanNotebook:
    """02_clean_sales: reads bronze.raw_sales, writes silver.clean_sales"""

    def test_input_table(self, clean_result):
        input_names = [i.name for i in clean_result.inputs]
        assert "bronze.raw_sales" in input_names

    def test_output_table(self, clean_result):
        output_names = [o.name for o in clean_result.outputs]
        assert "silver.clean_sales" in output_names


class TestAggregateNotebook:
    """03_aggregate_sales: reads silver.clean_sales, writes gold.daily_sales + gold.monthly_sales"""

    def test_input_table(self, aggregate_result):
        input_names = [i.name for i in aggregate_result.inputs]
        assert "silver.clean_sales" in input_names

    def test_output_tables(self, aggregate_result):
        output_names = [o.name for o in aggregate_result.outputs]
        assert "gold.daily_sales" in output_names
        assert "gold.monthly_sales" in output_names

    def test_two_outputs(self, aggregate_result):
        assert len(aggregate_result.outputs) == 2


class TestEnrichNotebook:
    """04_enrich_customers: reads bronze.raw_customers + silver.clean_sales, writes silver.enriched_customers"""

    def test_two_inputs(self, enrich_result):
        input_names = [i.name for i in enrich_result.inputs]
        assert "bronze.raw_customers" in input_names
        assert "silver.clean_sales" in input_names

    def test_output_table(self, enrich_result):
        output_names = [o.name for o in enrich_result.outputs]
        assert "silver.enriched_customers" in output_names


class TestUtilitiesNotebook:
    """utilities: no table reads/writes, only helper functions"""

    def test_no_outputs(self, utilities_result):
        # utilities only defines functions, no table writes
        table_outputs = [o for o in utilities_result.outputs if o.object_type == "table"]
        assert len(table_outputs) == 0


class TestVariableResolution:
    def test_resolves_simple_variable(self, parser):
        code = '''
TARGET = "gold.summary"
df.write.format("delta").saveAsTable(TARGET)
'''
        result = parser.parse_source(code)
        output_names = [o.name for o in result.outputs]
        assert "gold.summary" in output_names

    def test_unresolved_variable_low_confidence(self, parser):
        code = '''
df.write.format("delta").saveAsTable(UNKNOWN_VAR)
'''
        result = parser.parse_source(code)
        if result.outputs:
            assert result.outputs[0].confidence == "low"


class TestDeduplication:
    def test_deduplicates_same_table(self, parser):
        code = '''
df1 = spark.table("bronze.events")
df2 = spark.table("bronze.events")
'''
        result = parser.parse_source(code)
        names = [i.name for i in result.inputs]
        assert names.count("bronze.events") == 1

    def test_keeps_highest_confidence(self, parser):
        code = '''
SOURCE = "silver.data"
df = spark.table(SOURCE)
df2 = spark.table("silver.data")
'''
        result = parser.parse_source(code)
        match = [i for i in result.inputs if i.name == "silver.data"]
        assert len(match) == 1
        assert match[0].confidence == "high"


class TestExcludeImports:
    def test_excludes_pyspark_imports(self, parser):
        code = '''
from pyspark.sql import functions as F
from pyspark.sql.types import *
df = spark.table("bronze.sales")
'''
        result = parser.parse_source(code)
        input_names = [i.name for i in result.inputs]
        assert "pyspark.sql" not in input_names
        assert "pyspark.sql.types" not in input_names
        assert "bronze.sales" in input_names
