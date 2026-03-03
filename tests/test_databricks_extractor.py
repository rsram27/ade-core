"""Tests for Databricks extractors — both JSON-based and file-based."""

import json
import pytest
from pathlib import Path

from ade_app.platforms.databricks.extractor import save_to_catalog, DatabricksLocalExtractor
from ade_app.core.catalog import CatalogDB

JSON_DEMO_DIR = Path(__file__).resolve().parent.parent / "ade_data/demo/extractions/databricks"
FILE_DEMO_DIR = Path(__file__).resolve().parent.parent / "ade_data/demo/inputs/databricks"


# ==============================================================
# JSON-based save_to_catalog (legacy / API flow)
# ==============================================================

@pytest.fixture
def demo_data():
    """Load the real demo JSON files as extraction data."""
    notebooks = json.loads((JSON_DEMO_DIR / "notebooks.json").read_text(encoding="utf-8"))
    jobs = json.loads((JSON_DEMO_DIR / "jobs.json").read_text(encoding="utf-8"))
    return {
        "notebooks": notebooks,
        "jobs": jobs,
        "workspace": "https://demo.azuredatabricks.net",
        "extracted_at": "2025-01-01T00:00:00",
    }


@pytest.fixture
def json_catalog(tmp_path, demo_data):
    db_path = tmp_path / "test.db"
    save_to_catalog(demo_data, db_path)
    catalog = CatalogDB(db_path)
    yield catalog
    catalog.close()


class TestJsonSaveToCatalog:
    def test_total_objects(self, json_catalog):
        stats = json_catalog.get_stats(platform="databricks")
        total = sum(stats.values())
        assert total == 7  # 5 notebooks + 2 jobs

    def test_notebooks_stored(self, json_catalog):
        stats = json_catalog.get_stats(platform="databricks")
        assert stats["notebook"] == 5

    def test_jobs_stored(self, json_catalog):
        stats = json_catalog.get_stats(platform="databricks")
        assert stats["job"] == 2

    def test_notebook_has_source_code(self, json_catalog):
        nb = json_catalog.get_object("01_ingest_raw_sales")
        assert nb is not None
        assert nb["source_code"] is not None
        assert len(nb["source_code"]) > 0

    def test_notebook_language_metadata(self, json_catalog):
        nb = json_catalog.get_object("01_ingest_raw_sales")
        assert nb["language"].lower() == "python"

    def test_job_has_metadata(self, json_catalog):
        job = json_catalog.get_object("daily_sales_pipeline")
        assert job is not None
        assert job["job_id"] is not None

    def test_extraction_recorded(self, json_catalog):
        row = json_catalog.conn.execute(
            "SELECT * FROM extraction_meta WHERE platform = 'databricks'"
        ).fetchone()
        assert row is not None
        assert row["object_count"] == 7

    def test_empty_data(self, tmp_path):
        count = save_to_catalog({"notebooks": [], "jobs": []}, tmp_path / "empty.db")
        assert count == 0


# ==============================================================
# File-based DatabricksLocalExtractor
# ==============================================================

@pytest.fixture
def local_extractor():
    return DatabricksLocalExtractor(FILE_DEMO_DIR)


@pytest.fixture
def local_data(local_extractor):
    return local_extractor.extract_all()


@pytest.fixture
def local_catalog(tmp_path, local_extractor, local_data):
    db_path = tmp_path / "local.db"
    local_extractor.save_to_catalog(local_data, db_path)
    catalog = CatalogDB(db_path)
    yield catalog
    catalog.close()


class TestLocalExtractor:
    def test_init_valid_path(self, local_extractor):
        assert local_extractor.notebooks_path.exists()

    def test_init_invalid_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            DatabricksLocalExtractor(tmp_path / "nonexistent")

    def test_extract_notebooks_count(self, local_data):
        assert len(local_data["notebooks"]) == 5

    def test_notebooks_have_source_code(self, local_data):
        for nb in local_data["notebooks"]:
            assert nb["source_code"] is not None
            assert len(nb["source_code"]) > 0

    def test_notebooks_have_io(self, local_data):
        ingest = [n for n in local_data["notebooks"] if n["name"] == "01_ingest_raw_sales"][0]
        assert len(ingest["inputs"]) > 0
        assert len(ingest["outputs"]) > 0

    def test_no_jobs_from_local(self, local_data):
        assert local_data["jobs"] == []


class TestLocalSaveToCatalog:
    def test_notebooks_stored(self, local_catalog):
        stats = local_catalog.get_stats(platform="databricks")
        assert stats["notebook"] == 5

    def test_io_objects_stored(self, local_catalog):
        stats = local_catalog.get_stats(platform="databricks")
        # Should have input_table, output_table, and possibly input_file
        assert "output_table" in stats
        assert stats["output_table"] >= 5  # at least 5 output tables

    def test_notebook_has_children(self, local_catalog):
        nb = local_catalog.get_object("03_aggregate_sales", object_type="notebook")
        assert nb is not None
        children = local_catalog.get_children(nb["id"])
        assert len(children) > 0
        types = {c["object_type"] for c in children}
        assert "input_table" in types
        assert "output_table" in types

    def test_lineage_ingest(self, local_catalog):
        nb = local_catalog.get_object("01_ingest_raw_sales", object_type="notebook")
        children = local_catalog.get_children(nb["id"])
        output_names = [c["name"] for c in children if c["object_type"].startswith("output")]
        assert "bronze.raw_sales" in output_names

    def test_lineage_aggregate(self, local_catalog):
        nb = local_catalog.get_object("03_aggregate_sales", object_type="notebook")
        children = local_catalog.get_children(nb["id"])
        input_names = [c["name"] for c in children if c["object_type"].startswith("input")]
        output_names = [c["name"] for c in children if c["object_type"].startswith("output")]
        assert "silver.clean_sales" in input_names
        assert "gold.daily_sales" in output_names
        assert "gold.monthly_sales" in output_names

    def test_lineage_enrich(self, local_catalog):
        nb = local_catalog.get_object("04_enrich_customers", object_type="notebook")
        children = local_catalog.get_children(nb["id"])
        input_names = [c["name"] for c in children if c["object_type"].startswith("input")]
        assert "bronze.raw_customers" in input_names
        assert "silver.clean_sales" in input_names

    def test_no_python_imports(self, local_catalog):
        """Ensure pyspark.sql etc are not stored as input tables."""
        results = local_catalog.search("pyspark")
        table_results = [r for r in results if r["object_type"].startswith("input")]
        assert len(table_results) == 0

    def test_extraction_recorded(self, local_catalog):
        row = local_catalog.conn.execute(
            "SELECT * FROM extraction_meta WHERE platform = 'databricks'"
        ).fetchone()
        assert row is not None
        assert row["object_count"] > 5  # notebooks + I/O objects

    def test_idempotent_re_extract(self, tmp_path):
        db_path = tmp_path / "reextract.db"
        extractor = DatabricksLocalExtractor(FILE_DEMO_DIR)
        data = extractor.extract_all()
        extractor.save_to_catalog(data, db_path)
        count1 = sum(CatalogDB(db_path).get_stats(platform="databricks").values())
        extractor.save_to_catalog(data, db_path)
        catalog = CatalogDB(db_path)
        count2 = sum(catalog.get_stats(platform="databricks").values())
        assert count1 == count2  # not doubled
        catalog.close()
