"""Tests for TMDL parser — uses the actual demo TMDL files."""

import pytest
from pathlib import Path

from ade_app.platforms.powerbi.tmdl_parser import (
    parse_table_file, parse_relationships_file, parse_model_file,
)

DEMO_DIR = Path(__file__).resolve().parent.parent / (
    "ade_data/demo/inputs/powerbi/AcmeSales.SemanticModel/definition"
)


@pytest.fixture
def sales_table():
    return parse_table_file(DEMO_DIR / "tables" / "Sales.tmdl")


@pytest.fixture
def customer_table():
    return parse_table_file(DEMO_DIR / "tables" / "Customer.tmdl")


@pytest.fixture
def calendar_table():
    return parse_table_file(DEMO_DIR / "tables" / "Calendar.tmdl")


@pytest.fixture
def product_table():
    return parse_table_file(DEMO_DIR / "tables" / "Product.tmdl")


@pytest.fixture
def relationships():
    return parse_relationships_file(DEMO_DIR / "relationships.tmdl")


@pytest.fixture
def model():
    return parse_model_file(DEMO_DIR / "model.tmdl")


class TestParseTableSales:
    def test_table_name(self, sales_table):
        assert sales_table.name == "Sales"

    def test_column_count(self, sales_table):
        assert len(sales_table.columns) == 6

    def test_column_names(self, sales_table):
        names = [c.name for c in sales_table.columns]
        assert "Transaction ID" in names
        assert "Product Key" in names
        assert "Amount" in names
        assert "Order Date" in names

    def test_column_data_types(self, sales_table):
        by_name = {c.name: c for c in sales_table.columns}
        assert by_name["Transaction ID"].data_type == "int64"
        assert by_name["Order Date"].data_type == "dateTime"
        assert by_name["Amount"].data_type == "decimal"

    def test_column_source_column(self, sales_table):
        by_name = {c.name: c for c in sales_table.columns}
        assert by_name["Transaction ID"].source_column == "Transaction ID"

    def test_measure_count(self, sales_table):
        assert len(sales_table.measures) == 3

    def test_measure_names(self, sales_table):
        names = [m.name for m in sales_table.measures]
        assert "Total Sales" in names
        assert "YTD Sales" in names
        assert "Avg Order Value" in names

    def test_multiline_dax(self, sales_table):
        by_name = {m.name: m for m in sales_table.measures}
        total = by_name["Total Sales"]
        assert "SUMX" in total.expression
        assert "Sales[Quantity]" in total.expression
        assert "Sales[Amount]" in total.expression

    def test_single_line_dax(self, sales_table):
        by_name = {m.name: m for m in sales_table.measures}
        avg = by_name["Avg Order Value"]
        assert "DIVIDE" in avg.expression

    def test_measure_display_folder(self, sales_table):
        for m in sales_table.measures:
            assert m.display_folder == "Revenue"

    def test_partition(self, sales_table):
        assert len(sales_table.partitions) >= 1
        p = sales_table.partitions[0]
        assert "Sales" in p.name
        assert p.mode == "import"
        assert "gold" in p.expression


class TestParseTableCustomer:
    def test_table_name(self, customer_table):
        assert customer_table.name == "Customer"

    def test_columns(self, customer_table):
        names = [c.name for c in customer_table.columns]
        assert "Customer Key" in names
        assert "Customer Name" in names
        assert "City" in names
        assert "Segment" in names

    def test_measure(self, customer_table):
        assert len(customer_table.measures) >= 1
        names = [m.name for m in customer_table.measures]
        assert "Customer Count" in names

    def test_customer_count_dax(self, customer_table):
        by_name = {m.name: m for m in customer_table.measures}
        assert "DISTINCTCOUNT" in by_name["Customer Count"].expression


class TestParseTableCalendar:
    def test_columns(self, calendar_table):
        names = [c.name for c in calendar_table.columns]
        assert "Date" in names
        assert "Year" in names
        assert "Month Name" in names

    def test_partition_expression(self, calendar_table):
        assert len(calendar_table.partitions) >= 1
        assert "List.Dates" in calendar_table.partitions[0].expression


class TestParseRelationships:
    def test_relationship_count(self, relationships):
        assert len(relationships) == 3

    def test_relationship_names(self, relationships):
        names = [r.name for r in relationships]
        assert "Sales_Product" in names
        assert "Sales_Calendar" in names
        assert "Sales_Customer" in names

    def test_from_to_columns(self, relationships):
        by_name = {r.name: r for r in relationships}
        sp = by_name["Sales_Product"]
        assert sp.from_table == "Sales"
        assert sp.from_column == "Product Key"
        assert sp.to_table == "Product"
        assert sp.to_column == "Product Key"

    def test_cross_filtering(self, relationships):
        by_name = {r.name: r for r in relationships}
        sc = by_name["Sales_Customer"]
        assert sc.cross_filtering == "bothDirections"

    def test_default_no_cross_filtering(self, relationships):
        by_name = {r.name: r for r in relationships}
        assert by_name["Sales_Product"].cross_filtering == ""


class TestParseModel:
    def test_model_name(self, model):
        assert model.name == "Model"

    def test_culture(self, model):
        assert model.culture == "en-US"

    def test_data_source_version(self, model):
        assert "February 2025" in model.default_power_bi_data_source_version


class TestParseEdgeCases:
    def test_nonexistent_relationships_file(self, tmp_path):
        result = parse_relationships_file(tmp_path / "missing.tmdl")
        assert result == []

    def test_nonexistent_model_file(self, tmp_path):
        result = parse_model_file(tmp_path / "missing.tmdl")
        assert result.name == ""
        assert result.culture == ""
