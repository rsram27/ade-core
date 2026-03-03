"""Tests for CatalogDB — SQLite-backed metadata catalog."""

import json
import pytest
from pathlib import Path

from ade_app.core.catalog import CatalogDB


@pytest.fixture
def db(tmp_path):
    """Create a fresh CatalogDB in a temp directory."""
    catalog = CatalogDB(tmp_path / "test_catalog.db")
    yield catalog
    catalog.close()


class TestSchema:
    def test_creates_db_file(self, tmp_path):
        db_path = tmp_path / "new.db"
        assert not db_path.exists()
        catalog = CatalogDB(db_path)
        assert db_path.exists()
        catalog.close()

    def test_creates_parent_dirs(self, tmp_path):
        db_path = tmp_path / "nested" / "dir" / "catalog.db"
        catalog = CatalogDB(db_path)
        assert db_path.exists()
        catalog.close()

    def test_wal_mode(self, db):
        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_foreign_keys_on(self, db):
        fk = db.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1


class TestInsert:
    def test_insert_object_returns_id(self, db):
        row_id = db.insert_object(
            platform="test", object_type="table", name="my_table"
        )
        assert row_id is not None
        assert row_id > 0

    def test_insert_object_with_metadata(self, db):
        row_id = db.insert_object(
            platform="databricks", object_type="notebook", name="etl_01",
            path="/workspace/etl_01", description="ETL notebook",
            metadata={"language": "python"}, source_code="print('hello')"
        )
        row = db.conn.execute(
            "SELECT * FROM catalog_objects WHERE id = ?", (row_id,)
        ).fetchone()
        assert row["name"] == "etl_01"
        assert row["platform"] == "databricks"
        assert json.loads(row["metadata"])["language"] == "python"
        assert row["source_code"] == "print('hello')"

    def test_insert_object_with_parent(self, db):
        parent_id = db.insert_object(
            platform="powerbi", object_type="table", name="Sales"
        )
        child_id = db.insert_object(
            platform="powerbi", object_type="column", name="Amount",
            parent_id=parent_id
        )
        row = db.conn.execute(
            "SELECT parent_id FROM catalog_objects WHERE id = ?", (child_id,)
        ).fetchone()
        assert row["parent_id"] == parent_id

    def test_insert_objects_batch(self, db):
        objects = [
            {"platform": "test", "object_type": "table", "name": f"t{i}"}
            for i in range(5)
        ]
        count = db.insert_objects_batch(objects)
        assert count == 5
        total = db.conn.execute(
            "SELECT COUNT(*) FROM catalog_objects"
        ).fetchone()[0]
        assert total == 5

    def test_insert_batch_empty(self, db):
        count = db.insert_objects_batch([])
        assert count == 0


class TestClearPlatform:
    def test_clear_removes_only_target_platform(self, db):
        db.insert_object(platform="a", object_type="t", name="x")
        db.insert_object(platform="b", object_type="t", name="y")
        db.clear_platform("a")
        remaining = db.conn.execute(
            "SELECT COUNT(*) FROM catalog_objects"
        ).fetchone()[0]
        assert remaining == 1
        row = db.conn.execute(
            "SELECT platform FROM catalog_objects"
        ).fetchone()
        assert row["platform"] == "b"


class TestRecordExtraction:
    def test_record_extraction(self, db):
        db.record_extraction("databricks", 7, {"source": "api"})
        row = db.conn.execute(
            "SELECT * FROM extraction_meta WHERE platform = 'databricks'"
        ).fetchone()
        assert row["object_count"] == 7
        assert json.loads(row["source_info"])["source"] == "api"


class TestSearch:
    @pytest.fixture(autouse=True)
    def seed_data(self, db):
        db.insert_object(
            platform="databricks", object_type="notebook",
            name="ingest_raw_sales", path="/workspace/ingest"
        )
        db.insert_object(
            platform="databricks", object_type="notebook",
            name="clean_sales", path="/workspace/clean"
        )
        db.insert_object(
            platform="powerbi", object_type="measure",
            name="Total Sales", source_code="SUMX(Sales, Sales[Amount])"
        )
        db.insert_object(
            platform="powerbi", object_type="table",
            name="Customer", description="Customer dimension"
        )

    def test_search_by_name(self, db):
        results = db.search("sales")
        names = [r["name"] for r in results]
        assert "ingest_raw_sales" in names
        assert "clean_sales" in names
        assert "Total Sales" in names

    def test_search_wildcard(self, db):
        results = db.search("*")
        assert len(results) == 4

    def test_search_empty_string(self, db):
        results = db.search("")
        assert len(results) == 4

    def test_search_filter_platform(self, db):
        results = db.search("sales", platform="databricks")
        for r in results:
            assert r["platform"] == "databricks"

    def test_search_filter_object_type(self, db):
        results = db.search("", object_type="measure")
        assert len(results) == 1
        assert results[0]["name"] == "Total Sales"

    def test_search_limit(self, db):
        results = db.search("", limit=2)
        assert len(results) == 2

    def test_search_no_results(self, db):
        results = db.search("nonexistent_xyz")
        assert results == []


class TestGetObject:
    @pytest.fixture(autouse=True)
    def seed_data(self, db):
        db.insert_object(
            platform="powerbi", object_type="table", name="Sales"
        )
        db.insert_object(
            platform="databricks", object_type="notebook",
            name="ingest_raw_sales", path="/workspace/ingest_raw_sales"
        )

    def test_exact_match(self, db):
        obj = db.get_object("Sales")
        assert obj is not None
        assert obj["name"] == "Sales"

    def test_case_insensitive(self, db):
        obj = db.get_object("sales")
        assert obj is not None
        assert obj["name"] == "Sales"

    def test_partial_match_fallback(self, db):
        obj = db.get_object("ingest_raw")
        assert obj is not None
        assert obj["name"] == "ingest_raw_sales"

    def test_not_found(self, db):
        obj = db.get_object("nonexistent_xyz_123")
        assert obj is None

    def test_filter_by_platform(self, db):
        obj = db.get_object("Sales", platform="powerbi")
        assert obj is not None
        assert obj["platform"] == "powerbi"
        obj = db.get_object("Sales", platform="databricks")
        # "Sales" partial-matches "ingest_raw_sales" in databricks
        # so we verify the platform filter works correctly
        assert obj is None or obj["platform"] == "databricks"


class TestGetChildren:
    def test_returns_children(self, db):
        parent_id = db.insert_object(
            platform="powerbi", object_type="table", name="Sales"
        )
        db.insert_object(
            platform="powerbi", object_type="column",
            name="Amount", parent_id=parent_id
        )
        db.insert_object(
            platform="powerbi", object_type="column",
            name="Quantity", parent_id=parent_id
        )
        children = db.get_children(parent_id)
        assert len(children) == 2
        names = [c["name"] for c in children]
        assert "Amount" in names
        assert "Quantity" in names

    def test_no_children(self, db):
        parent_id = db.insert_object(
            platform="powerbi", object_type="table", name="Empty"
        )
        children = db.get_children(parent_id)
        assert children == []


class TestStats:
    @pytest.fixture(autouse=True)
    def seed_data(self, db):
        db.insert_object(platform="databricks", object_type="notebook", name="n1")
        db.insert_object(platform="databricks", object_type="notebook", name="n2")
        db.insert_object(platform="databricks", object_type="job", name="j1")
        db.insert_object(platform="powerbi", object_type="table", name="t1")

    def test_stats_all(self, db):
        stats = db.get_stats()
        assert stats["databricks"]["notebook"] == 2
        assert stats["databricks"]["job"] == 1
        assert stats["powerbi"]["table"] == 1

    def test_stats_single_platform(self, db):
        stats = db.get_stats(platform="databricks")
        assert stats["notebook"] == 2
        assert stats["job"] == 1
        assert "table" not in stats

    def test_get_platforms(self, db):
        platforms = db.get_platforms()
        assert "databricks" in platforms
        assert "powerbi" in platforms
