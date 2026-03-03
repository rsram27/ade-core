"""
Power BI PBIP Extractor

Extracts metadata from Power BI PBIP projects on disk (TMDL format).
Outputs to SQLite via CatalogDB.

Usage:
    python -m ade_app.platforms.powerbi.extractor --path <definition_dir> --db <catalog.db>
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from .tmdl_parser import (
    parse_table_file, parse_relationships_file, parse_model_file,
    TmdlTable, TmdlRelationship, TmdlModel,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PLATFORM = "powerbi"


class PowerBIExtractor:
    """Extract metadata from a Power BI PBIP / TMDL definition directory."""

    def __init__(self, definition_path: str | Path):
        """Initialize extractor.

        Args:
            definition_path: Path to the SemanticModel/definition/ directory.
        """
        self.definition_path = Path(definition_path)
        if not self.definition_path.exists():
            raise FileNotFoundError(f"Definition path not found: {self.definition_path}")

    def extract_tables(self) -> list[TmdlTable]:
        tables_dir = self.definition_path / "tables"
        if not tables_dir.exists():
            logger.warning(f"No tables directory found at {tables_dir}")
            return []
        tables = []
        for tmdl_file in sorted(tables_dir.glob("*.tmdl")):
            logger.info(f"Parsing table: {tmdl_file.name}")
            tables.append(parse_table_file(tmdl_file))
        logger.info(f"Extracted {len(tables)} tables")
        return tables

    def extract_relationships(self) -> list[TmdlRelationship]:
        rel_file = self.definition_path / "relationships.tmdl"
        if not rel_file.exists():
            return []
        rels = parse_relationships_file(rel_file)
        logger.info(f"Extracted {len(rels)} relationships")
        return rels

    def extract_model(self) -> TmdlModel:
        model_file = self.definition_path / "model.tmdl"
        return parse_model_file(model_file)

    def extract_all(self) -> dict:
        """Extract everything and return a structured dict."""
        model = self.extract_model()
        tables = self.extract_tables()
        relationships = self.extract_relationships()
        return {
            "model": model,
            "tables": tables,
            "relationships": relationships,
        }

    def save_to_catalog(self, data: dict, db_path: str | Path):
        """Write extracted Power BI metadata into a CatalogDB."""
        from ade_app.core.catalog import CatalogDB

        catalog = CatalogDB(db_path)
        catalog.clear_platform(PLATFORM)

        count = 0
        tables: list[TmdlTable] = data["tables"]
        relationships: list[TmdlRelationship] = data["relationships"]

        for table in tables:
            # Insert the table object
            table_id = catalog.insert_object(
                platform=PLATFORM,
                object_type="table",
                name=table.name,
                description=table.description,
                metadata={
                    "column_count": len(table.columns),
                    "measure_count": len(table.measures),
                },
            )
            count += 1

            # Insert columns as children
            for col in table.columns:
                catalog.insert_object(
                    platform=PLATFORM,
                    object_type="column",
                    name=col.name,
                    parent_id=table_id,
                    description=col.description,
                    metadata={
                        "data_type": col.data_type,
                        "source_column": col.source_column,
                        "format_string": col.format_string,
                        "table": table.name,
                    },
                )
                count += 1

            # Insert measures as children
            for m in table.measures:
                catalog.insert_object(
                    platform=PLATFORM,
                    object_type="measure",
                    name=m.name,
                    parent_id=table_id,
                    description=m.description,
                    source_code=m.expression,
                    metadata={
                        "format_string": m.format_string,
                        "display_folder": m.display_folder,
                        "table": table.name,
                    },
                )
                count += 1

        # Insert relationships
        for rel in relationships:
            catalog.insert_object(
                platform=PLATFORM,
                object_type="relationship",
                name=rel.name,
                metadata={
                    "from_table": rel.from_table,
                    "from_column": rel.from_column,
                    "to_table": rel.to_table,
                    "to_column": rel.to_column,
                    "cross_filtering": rel.cross_filtering,
                    "is_active": rel.is_active,
                },
            )
            count += 1

        catalog.record_extraction(PLATFORM, count, {
            "definition_path": str(self.definition_path),
        })
        catalog.close()
        logger.info(f"Saved {count} Power BI objects to {db_path}")
        return count


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract metadata from Power BI PBIP (TMDL) files"
    )
    parser.add_argument(
        "--path", required=True,
        help="Path to SemanticModel/definition/ directory"
    )
    parser.add_argument(
        "--db", required=True,
        help="Path to output catalog.db"
    )

    args = parser.parse_args()

    extractor = PowerBIExtractor(args.path)
    data = extractor.extract_all()
    count = extractor.save_to_catalog(data, args.db)

    print(f"\nExtraction complete!")
    print(f"  Tables: {len(data['tables'])}")
    print(f"  Relationships: {len(data['relationships'])}")
    print(f"  Total objects: {count}")
    print(f"  Output: {args.db}")


if __name__ == "__main__":
    main()
