"""
Build Demo Catalog

Reads Databricks notebook .py files and Power BI TMDL files from disk,
then writes everything into a single catalog.db for the demo environment.

Usage:
    python -m ade_app.scripts.build_demo_catalog
"""

import json
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent.parent.parent
DEMO_DIR = REPO_ROOT / "ade_data" / "demo"
DB_PATH = DEMO_DIR / "catalog.db"


def main():
    # Remove existing catalog.db so we start fresh
    for suffix in ("", "-shm", "-wal"):
        p = Path(str(DB_PATH) + suffix)
        if p.exists():
            try:
                p.unlink()
            except PermissionError:
                logger.warning(f"Could not delete {p} (in use) — will overwrite tables")
    if not DB_PATH.exists():
        logger.info("Removed existing catalog.db")

    # --- Databricks (file-based) ---
    from ade_app.platforms.databricks.extractor import DatabricksLocalExtractor
    db_path = DEMO_DIR / "inputs" / "databricks"
    if db_path.exists():
        db_extractor = DatabricksLocalExtractor(db_path)
        db_data = db_extractor.extract_all()
        db_count = db_extractor.save_to_catalog(db_data, DB_PATH)
        logger.info(f"Databricks: {db_count} objects written")
    else:
        logger.warning(f"Databricks notebooks not found at {db_path}")

    # --- Power BI ---
    from ade_app.platforms.powerbi.extractor import PowerBIExtractor
    pbip_path = DEMO_DIR / "inputs" / "powerbi" / "AcmeSales.SemanticModel" / "definition"
    if pbip_path.exists():
        pbi_extractor = PowerBIExtractor(pbip_path)
        pbi_data = pbi_extractor.extract_all()
        pbi_count = pbi_extractor.save_to_catalog(pbi_data, DB_PATH)
        logger.info(f"Power BI: {pbi_count} objects written")
    else:
        logger.warning(f"Power BI TMDL files not found at {pbip_path}")

    # --- Summary ---
    from ade_app.core.catalog import CatalogDB
    catalog = CatalogDB(DB_PATH)
    stats = catalog.get_stats()
    catalog.close()

    print(f"\nDemo catalog built: {DB_PATH}")
    print(f"Stats: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
