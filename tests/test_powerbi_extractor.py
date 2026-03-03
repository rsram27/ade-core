"""Tests for PowerBIExtractor — extraction and save_to_catalog."""

import pytest
from pathlib import Path

from ade_app.platforms.powerbi.extractor import PowerBIExtractor
from ade_app.core.catalog import CatalogDB

DEMO_DIR = Path(__file__).resolve().parent.parent / (
    "ade_data/demo/inputs/powerbi/AcmeSales.SemanticModel/definition"
)


@pytest.fixture
def extractor():
    return PowerBIExtractor(DEMO_DIR)


@pytest.fixture
def extracted_data(extractor):
    return extractor.extract_all()


@pytest.fixture
def catalog_db(tmp_path, extractor, extracted_data):
    db_path = tmp_path / "test.db"
    extractor.save_to_catalog(extracted_data, db_path)
    catalog = CatalogDB(db_path)
    yield catalog
    catalog.close()


class TestExtractor:
    def test_init_valid_path(self, extractor):
        assert extractor.definition_path.exists()

    def test_init_invalid_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            PowerBIExtractor(tmp_path / "nonexistent")

    def test_extract_tables(self, extractor):
        tables = extractor.extract_tables()
        assert len(tables) == 4
        names = [t.name for t in tables]
        assert "Sales" in names
        assert "Customer" in names
        assert "Product" in names
        assert "Calendar" in names

    def test_extract_relationships(self, extractor):
        rels = extractor.extract_relationships()
        assert len(rels) == 3

    def test_extract_model(self, extractor):
        model = extractor.extract_model()
        assert model.name == "Model"
        assert model.culture == "en-US"

    def test_extract_all_structure(self, extracted_data):
        assert "model" in extracted_data
        assert "tables" in extracted_data
        assert "relationships" in extracted_data


class TestSaveToCatalog:
    def test_total_objects(self, catalog_db):
        stats = catalog_db.get_stats(platform="powerbi")
        total = sum(stats.values())
        assert total == 31  # 4 tables + 19 columns + 5 measures + 3 relationships

    def test_tables_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="powerbi")
        assert stats["table"] == 4

    def test_columns_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="powerbi")
        assert stats["column"] == 19

    def test_measures_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="powerbi")
        assert stats["measure"] == 5

    def test_relationships_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="powerbi")
        assert stats["relationship"] == 3

    def test_table_has_children(self, catalog_db):
        sales = catalog_db.get_object("Sales", platform="powerbi", object_type="table")
        assert sales is not None
        children = catalog_db.get_children(sales["id"])
        assert len(children) > 0
        types = {c["object_type"] for c in children}
        assert "column" in types
        assert "measure" in types

    def test_measure_has_dax(self, catalog_db):
        measure = catalog_db.get_object("Total Sales")
        assert measure is not None
        assert "SUMX" in measure["source_code"]

    def test_column_metadata(self, catalog_db):
        results = catalog_db.search("Amount", object_type="column")
        amount = [r for r in results if r["name"] == "Amount"]
        assert len(amount) == 1
        assert amount[0]["data_type"] == "decimal"
        assert amount[0]["table"] == "Sales"

    def test_relationship_metadata(self, catalog_db):
        rel = catalog_db.get_object("Sales_Product")
        assert rel is not None
        assert rel["from_table"] == "Sales"
        assert rel["to_table"] == "Product"

    def test_extraction_recorded(self, catalog_db):
        row = catalog_db.conn.execute(
            "SELECT * FROM extraction_meta WHERE platform = 'powerbi'"
        ).fetchone()
        assert row is not None
        assert row["object_count"] == 31

    def test_search_dax_code(self, catalog_db):
        results = catalog_db.search("TOTALYTD")
        assert len(results) >= 1
        assert results[0]["name"] == "YTD Sales"

    def test_clear_and_re_extract(self, tmp_path):
        db_path = tmp_path / "reextract.db"
        extractor = PowerBIExtractor(DEMO_DIR)
        data = extractor.extract_all()
        extractor.save_to_catalog(data, db_path)
        extractor.save_to_catalog(data, db_path)  # second run should clear first
        catalog = CatalogDB(db_path)
        stats = catalog.get_stats(platform="powerbi")
        assert sum(stats.values()) == 31  # not doubled
        catalog.close()
