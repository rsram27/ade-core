"""
ADE Power BI Platform

Metadata extraction from Power BI PBIP / TMDL files.
Supports tables, columns, measures (DAX), and relationships.
"""

from .extractor import PowerBIExtractor

__all__ = ["PowerBIExtractor"]
