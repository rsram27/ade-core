"""
Build Demo Catalog

Reads existing Databricks JSON extractions and Power BI TMDL files,
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


def load_databricks_json() -> dict:
    """Load Databricks extractions from JSON files."""
    db_dir = DEMO_DIR / "extractions" / "databricks"
    data = {"notebooks": [], "jobs": []}

    nb_file = db_dir / "notebooks.json"
    if nb_file.exists():
        with open(nb_file, 'r', encoding='utf-8') as f:
            data["notebooks"] = json.load(f)
        logger.info(f"Loaded {len(data['notebooks'])} notebooks from JSON")

    jobs_file = db_dir / "jobs.json"
    if jobs_file.exists():
        with open(jobs_file, 'r', encoding='utf-8') as f:
            data["jobs"] = json.load(f)
        logger.info(f"Loaded {len(data['jobs'])} jobs from JSON")

    return data


def main():
    # Remove existing catalog.db so we start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()
        logger.info("Removed existing catalog.db")

    # --- Databricks ---
    from ade_app.platforms.databricks.extractor import save_to_catalog as save_databricks
    db_data = load_databricks_json()
    db_count = save_databricks(db_data, DB_PATH)
    logger.info(f"Databricks: {db_count} objects written")

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
