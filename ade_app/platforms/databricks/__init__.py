"""
ADE Databricks Platform

Metadata extraction from Databricks workspaces.
Supports notebooks (local files and API), jobs, and clusters.
"""

from .extractor import DatabricksExtractor, DatabricksLocalExtractor

__all__ = ["DatabricksExtractor", "DatabricksLocalExtractor"]
