"""
CatalogDB — SQLite-backed metadata catalog for ADE.

Single portable .db file, zero infrastructure.
Uses only stdlib (sqlite3, json).
"""

import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS catalog_objects (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    platform     TEXT NOT NULL,
    object_type  TEXT NOT NULL,
    name         TEXT NOT NULL,
    path         TEXT DEFAULT '',
    parent_id    INTEGER,
    description  TEXT DEFAULT '',
    metadata     TEXT DEFAULT '{}',
    source_code  TEXT,
    created_at   TEXT,
    updated_at   TEXT,
    extracted_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (parent_id) REFERENCES catalog_objects(id)
);

CREATE INDEX IF NOT EXISTS idx_platform_type ON catalog_objects(platform, object_type);
CREATE INDEX IF NOT EXISTS idx_name ON catalog_objects(name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_parent_id ON catalog_objects(parent_id);

CREATE TABLE IF NOT EXISTS extraction_meta (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    platform     TEXT NOT NULL,
    extracted_at TEXT DEFAULT (datetime('now')),
    source_info  TEXT DEFAULT '{}',
    object_count INTEGER DEFAULT 0
);
"""

_FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS catalog_fts USING fts5(
    name, path, description, source_code,
    content='catalog_objects', content_rowid='id'
);
"""

_FTS_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS catalog_ai AFTER INSERT ON catalog_objects BEGIN
    INSERT INTO catalog_fts(rowid, name, path, description, source_code)
    VALUES (new.id, new.name, new.path, new.description, new.source_code);
END;

CREATE TRIGGER IF NOT EXISTS catalog_ad AFTER DELETE ON catalog_objects BEGIN
    INSERT INTO catalog_fts(catalog_fts, rowid, name, path, description, source_code)
    VALUES ('delete', old.id, old.name, old.path, old.description, old.source_code);
END;

CREATE TRIGGER IF NOT EXISTS catalog_au AFTER UPDATE ON catalog_objects BEGIN
    INSERT INTO catalog_fts(catalog_fts, rowid, name, path, description, source_code)
    VALUES ('delete', old.id, old.name, old.path, old.description, old.source_code);
    INSERT INTO catalog_fts(rowid, name, path, description, source_code)
    VALUES (new.id, new.name, new.path, new.description, new.source_code);
END;
"""


class CatalogDB:
    """SQLite-backed metadata catalog."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = None
        self._has_fts = False
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _ensure_schema(self):
        self.conn.executescript(_SCHEMA_SQL)
        try:
            self.conn.executescript(_FTS_SQL)
            self.conn.executescript(_FTS_TRIGGERS)
            self._has_fts = True
        except sqlite3.OperationalError:
            logger.info("FTS5 not available — falling back to LIKE search")
            self._has_fts = False

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert_object(self, *, platform: str, object_type: str, name: str,
                      path: str = "", parent_id: int | None = None,
                      description: str = "", metadata: dict | None = None,
                      source_code: str | None = None,
                      created_at: str | None = None,
                      updated_at: str | None = None) -> int:
        """Insert a single catalog object. Returns the new row id."""
        cur = self.conn.execute(
            """INSERT INTO catalog_objects
               (platform, object_type, name, path, parent_id,
                description, metadata, source_code, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (platform, object_type, name, path, parent_id,
             description, json.dumps(metadata or {}), source_code,
             created_at, updated_at)
        )
        self.conn.commit()
        return cur.lastrowid

    def insert_objects_batch(self, objects: list[dict]) -> int:
        """Insert multiple objects in a single transaction.

        Each dict must have keys: platform, object_type, name.
        Optional: path, parent_id, description, metadata, source_code,
                  created_at, updated_at.
        Returns number of rows inserted.
        """
        rows = []
        for obj in objects:
            rows.append((
                obj["platform"], obj["object_type"], obj["name"],
                obj.get("path", ""), obj.get("parent_id"),
                obj.get("description", ""),
                json.dumps(obj.get("metadata") or {}),
                obj.get("source_code"),
                obj.get("created_at"), obj.get("updated_at"),
            ))
        self.conn.executemany(
            """INSERT INTO catalog_objects
               (platform, object_type, name, path, parent_id,
                description, metadata, source_code, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
        self.conn.commit()
        return len(rows)

    def clear_platform(self, platform: str):
        """Delete all objects for a platform (used before re-extraction)."""
        self.conn.execute(
            "DELETE FROM catalog_objects WHERE platform = ?", (platform,))
        self.conn.commit()

    def record_extraction(self, platform: str, object_count: int,
                          source_info: dict | None = None):
        """Record an extraction run in extraction_meta."""
        self.conn.execute(
            """INSERT INTO extraction_meta (platform, object_count, source_info)
               VALUES (?, ?, ?)""",
            (platform, object_count, json.dumps(source_info or {}))
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Convert a Row to a dict, flattening the metadata JSON blob."""
        d = dict(row)
        meta = json.loads(d.pop("metadata", "{}"))
        d.update(meta)
        return d

    def search(self, query: str = "", platform: str | None = None,
               object_type: str | None = None, limit: int = 20) -> list[dict]:
        """Search catalog objects by name/path/description/source_code."""
        wildcard = not query or query in ("*", "%", "")

        if wildcard:
            return self._search_filter(platform, object_type, limit)

        # Try FTS first
        if self._has_fts:
            return self._search_fts(query, platform, object_type, limit)

        return self._search_like(query, platform, object_type, limit)

    def _search_filter(self, platform, object_type, limit):
        clauses, params = self._where_clauses(platform, object_type)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM catalog_objects {where} ORDER BY id LIMIT ?",
            params + [limit]
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def _search_fts(self, query, platform, object_type, limit):
        # FTS5 match — escape double quotes in the query
        fts_query = query.replace('"', '""')
        sql = """
            SELECT co.* FROM catalog_objects co
            JOIN catalog_fts fts ON co.id = fts.rowid
            WHERE catalog_fts MATCH ?
        """
        params: list = [f'"{fts_query}"']
        if platform:
            sql += " AND co.platform = ?"
            params.append(platform)
        if object_type:
            sql += " AND co.object_type = ?"
            params.append(object_type)
        sql += " ORDER BY rank LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def _search_like(self, query, platform, object_type, limit):
        clauses, params = self._where_clauses(platform, object_type)
        like = f"%{query}%"
        clauses.append(
            "(name LIKE ? COLLATE NOCASE OR path LIKE ? COLLATE NOCASE "
            "OR description LIKE ? COLLATE NOCASE)")
        params.extend([like, like, like])
        where = f"WHERE {' AND '.join(clauses)}"
        rows = self.conn.execute(
            f"SELECT * FROM catalog_objects {where} ORDER BY id LIMIT ?",
            params + [limit]
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_object(self, name: str, platform: str | None = None,
                   object_type: str | None = None) -> dict | None:
        """Get a single object by exact name match (case-insensitive).

        Falls back to partial match if exact not found.
        """
        # Exact match
        clauses, params = self._where_clauses(platform, object_type)
        clauses.append("name = ? COLLATE NOCASE")
        params.append(name)
        where = f"WHERE {' AND '.join(clauses)}"
        row = self.conn.execute(
            f"SELECT * FROM catalog_objects {where} LIMIT 1", params
        ).fetchone()
        if row:
            return self._row_to_dict(row)

        # Partial match
        clauses2, params2 = self._where_clauses(platform, object_type)
        like = f"%{name}%"
        clauses2.append(
            "(name LIKE ? COLLATE NOCASE OR path LIKE ? COLLATE NOCASE)")
        params2.extend([like, like])
        where2 = f"WHERE {' AND '.join(clauses2)}"
        row = self.conn.execute(
            f"SELECT * FROM catalog_objects {where2} LIMIT 1", params2
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_children(self, parent_id: int) -> list[dict]:
        """Get child objects (e.g., columns/measures for a table)."""
        rows = self.conn.execute(
            "SELECT * FROM catalog_objects WHERE parent_id = ? ORDER BY id",
            (parent_id,)
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_stats(self, platform: str | None = None) -> dict:
        """Get object counts grouped by platform and object_type."""
        if platform:
            rows = self.conn.execute(
                """SELECT object_type, COUNT(*) as cnt
                   FROM catalog_objects WHERE platform = ?
                   GROUP BY object_type ORDER BY object_type""",
                (platform,)
            ).fetchall()
            return {row["object_type"]: row["cnt"] for row in rows}
        else:
            rows = self.conn.execute(
                """SELECT platform, object_type, COUNT(*) as cnt
                   FROM catalog_objects
                   GROUP BY platform, object_type
                   ORDER BY platform, object_type"""
            ).fetchall()
            stats: dict[str, dict] = {}
            for row in rows:
                stats.setdefault(row["platform"], {})[row["object_type"]] = row["cnt"]
            return stats

    def get_platforms(self) -> list[str]:
        """Return list of distinct platforms in the catalog."""
        rows = self.conn.execute(
            "SELECT DISTINCT platform FROM catalog_objects ORDER BY platform"
        ).fetchall()
        return [row["platform"] for row in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _where_clauses(platform, object_type):
        clauses = []
        params = []
        if platform:
            clauses.append("platform = ?")
            params.append(platform)
        if object_type:
            clauses.append("object_type = ?")
            params.append(object_type)
        return clauses, params
