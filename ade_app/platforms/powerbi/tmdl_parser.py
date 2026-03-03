"""
TMDL Parser — parse Power BI Tabular Model Definition Language files.

TMDL is a tab-indented, line-oriented format used in Power BI PBIP projects
under SemanticModel/definition/.  This parser handles:
  - table files  (tables/<Name>.tmdl)   → columns, measures, partitions
  - relationships.tmdl                  → relationship definitions
  - model.tmdl                          → model-level metadata

All parsing uses stdlib only (re, dataclasses).
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ======================================================================
# Dataclasses
# ======================================================================

@dataclass
class TmdlColumn:
    name: str
    data_type: str = ""
    description: str = ""
    source_column: str = ""
    format_string: str = ""
    is_hidden: bool = False
    properties: dict = field(default_factory=dict)


@dataclass
class TmdlMeasure:
    name: str
    expression: str = ""
    description: str = ""
    format_string: str = ""
    display_folder: str = ""
    is_hidden: bool = False
    properties: dict = field(default_factory=dict)


@dataclass
class TmdlPartition:
    name: str
    mode: str = ""
    source_type: str = ""
    expression: str = ""


@dataclass
class TmdlTable:
    name: str
    description: str = ""
    columns: list[TmdlColumn] = field(default_factory=list)
    measures: list[TmdlMeasure] = field(default_factory=list)
    partitions: list[TmdlPartition] = field(default_factory=list)
    properties: dict = field(default_factory=dict)


@dataclass
class TmdlRelationship:
    name: str = ""
    from_table: str = ""
    from_column: str = ""
    to_table: str = ""
    to_column: str = ""
    cross_filtering: str = ""
    is_active: bool = True
    properties: dict = field(default_factory=dict)


@dataclass
class TmdlModel:
    name: str = ""
    culture: str = ""
    description: str = ""
    default_power_bi_data_source_version: str = ""
    properties: dict = field(default_factory=dict)


# ======================================================================
# Helpers
# ======================================================================

def _unquote(name: str) -> str:
    """Remove surrounding single quotes from TMDL names: 'Product Key' → Product Key."""
    name = name.strip()
    if name.startswith("'") and name.endswith("'"):
        return name[1:-1]
    return name


def _indent_level(line: str) -> int:
    """Count leading tabs."""
    return len(line) - len(line.lstrip('\t'))


def _parse_description_comments(lines: list[str], start: int) -> tuple[str, int]:
    """Collect consecutive /// comments above a block. Returns (description, new_index)."""
    parts = []
    i = start
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith("///"):
            parts.append(stripped[3:].strip())
            i += 1
        else:
            break
    return "\n".join(parts), i


# ======================================================================
# Table file parser
# ======================================================================

def parse_table_file(path: str | Path) -> TmdlTable:
    """Parse a TMDL table file into a TmdlTable with columns, measures, partitions."""
    path = Path(path)
    lines = path.read_text(encoding="utf-8").splitlines()

    table = TmdlTable(name=path.stem)
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Table header: table <Name> or table '<Quoted Name>'
        m = re.match(r'^table\s+(.+)', stripped)
        if m:
            table.name = _unquote(m.group(1))
            i += 1
            continue

        # Description comments (///)
        if stripped.startswith("///"):
            desc, i = _parse_description_comments(lines, i)
            # Attach to the table if we haven't entered a sub-block yet
            if not table.columns and not table.measures:
                table.description = desc
            continue

        # Column block
        m = re.match(r'^\tcolumn\s+(.+)', line)
        if m:
            col, i = _parse_column(lines, i, m.group(1))
            table.columns.append(col)
            continue

        # Measure block
        m = re.match(r'^\tmeasure\s+(.+)', line)
        if m:
            measure, i = _parse_measure(lines, i, m.group(1))
            table.measures.append(measure)
            continue

        # Partition block
        m = re.match(r'^\tpartition\s+(.+)', line)
        if m:
            partition, i = _parse_partition(lines, i, m.group(1))
            table.partitions.append(partition)
            continue

        # Table-level property
        m = re.match(r'^\t(\w[\w\s]*\w|\w+)\s*[:=]\s*(.+)', line)
        if m:
            table.properties[m.group(1).strip()] = m.group(2).strip()

        i += 1

    return table


def _parse_column(lines: list[str], start: int, header: str) -> tuple[TmdlColumn, int]:
    """Parse a column block starting at 'start'. Returns (TmdlColumn, next_index)."""
    # Check for preceding description comments
    desc = ""
    if start > 0:
        j = start - 1
        desc_parts = []
        while j >= 0 and lines[j].strip().startswith("///"):
            desc_parts.insert(0, lines[j].strip()[3:].strip())
            j -= 1
        desc = "\n".join(desc_parts)

    col = TmdlColumn(name=_unquote(header.strip()))
    col.description = desc
    i = start + 1

    while i < len(lines):
        line = lines[i]
        if _indent_level(line) < 2 and line.strip():
            break
        stripped = line.strip()
        if not stripped:
            i += 1
            continue

        if stripped.startswith("///"):
            # description for column itself if we haven't set one
            if not col.description:
                d, i = _parse_description_comments(lines, i)
                col.description = d
                continue

        m = re.match(r'dataType:\s*(.+)', stripped)
        if m:
            col.data_type = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'sourceColumn:\s*(.+)', stripped)
        if m:
            col.source_column = _unquote(m.group(1))
            i += 1
            continue
        m = re.match(r'formatString:\s*(.+)', stripped)
        if m:
            col.format_string = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'isHidden', stripped)
        if m:
            col.is_hidden = True
            i += 1
            continue

        # Generic property
        m = re.match(r'(\w[\w\s]*\w|\w+)\s*[:=]\s*(.+)', stripped)
        if m:
            col.properties[m.group(1).strip()] = m.group(2).strip()

        i += 1

    return col, i


def _parse_measure(lines: list[str], start: int, header: str) -> tuple[TmdlMeasure, int]:
    """Parse a measure block. Handles multi-line DAX between ```. """
    # Check for preceding description comments
    desc = ""
    if start > 0:
        j = start - 1
        desc_parts = []
        while j >= 0 and lines[j].strip().startswith("///"):
            desc_parts.insert(0, lines[j].strip()[3:].strip())
            j -= 1
        desc = "\n".join(desc_parts)

    # Header may have inline '= <expr>' for single-line measures
    name_part = header.strip()
    inline_expr = ""
    eq_match = re.match(r"(.+?)\s*=\s*$", name_part)
    if eq_match:
        name_part = eq_match.group(1)
    elif "=" in name_part:
        parts = name_part.split("=", 1)
        name_part = parts[0].strip()
        inline_expr = parts[1].strip()

    measure = TmdlMeasure(name=_unquote(name_part), description=desc)
    i = start + 1

    # Collect expression (may be multi-line with ``` blocks or indented)
    expr_lines: list[str] = []
    in_code_block = False

    if inline_expr:
        expr_lines.append(inline_expr)

    while i < len(lines):
        line = lines[i]
        if _indent_level(line) < 2 and line.strip() and not in_code_block:
            break
        stripped = line.strip()
        if not stripped and not in_code_block:
            i += 1
            continue

        if stripped == "```":
            in_code_block = not in_code_block
            i += 1
            continue

        if in_code_block:
            expr_lines.append(line.lstrip('\t'))
            i += 1
            continue

        if stripped.startswith("///"):
            if not measure.description:
                d, i = _parse_description_comments(lines, i)
                measure.description = d
                continue

        m = re.match(r'formatString:\s*(.+)', stripped)
        if m:
            measure.format_string = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'displayFolder:\s*(.+)', stripped)
        if m:
            measure.display_folder = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'isHidden', stripped)
        if m:
            measure.is_hidden = True
            i += 1
            continue

        # Lines that look like DAX continuation (indented deeper)
        if _indent_level(line) >= 2 and not re.match(r'\w+\s*:', stripped):
            expr_lines.append(stripped)
            i += 1
            continue

        # Generic property
        m_prop = re.match(r'(\w[\w\s]*\w|\w+)\s*[:=]\s*(.+)', stripped)
        if m_prop:
            measure.properties[m_prop.group(1).strip()] = m_prop.group(2).strip()

        i += 1

    measure.expression = "\n".join(expr_lines).strip()
    return measure, i


def _parse_partition(lines: list[str], start: int, header: str) -> tuple[TmdlPartition, int]:
    """Parse a partition block."""
    partition = TmdlPartition(name=_unquote(header.strip()))
    i = start + 1
    expr_lines: list[str] = []
    in_code_block = False

    while i < len(lines):
        line = lines[i]
        if _indent_level(line) < 2 and line.strip() and not in_code_block:
            break
        stripped = line.strip()
        if not stripped and not in_code_block:
            i += 1
            continue

        if stripped == "```":
            in_code_block = not in_code_block
            i += 1
            continue

        if in_code_block:
            expr_lines.append(line.lstrip('\t'))
            i += 1
            continue

        m = re.match(r'mode:\s*(.+)', stripped)
        if m:
            partition.mode = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'source\s*=?\s*(.+)?', stripped)
        if m and m.group(1):
            partition.source_type = m.group(1).strip()
            i += 1
            continue

        i += 1

    partition.expression = "\n".join(expr_lines).strip()
    return partition, i


# ======================================================================
# Relationships file parser
# ======================================================================

def parse_relationships_file(path: str | Path) -> list[TmdlRelationship]:
    """Parse a relationships.tmdl file."""
    path = Path(path)
    if not path.exists():
        return []

    lines = path.read_text(encoding="utf-8").splitlines()
    relationships: list[TmdlRelationship] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        m = re.match(r'^relationship\s*(.*)', stripped)
        if m:
            rel, i = _parse_single_relationship(lines, i, m.group(1).strip())
            relationships.append(rel)
            continue

        i += 1

    return relationships


def _parse_single_relationship(lines, start, header):
    rel = TmdlRelationship(name=_unquote(header) if header else "")
    i = start + 1

    while i < len(lines):
        line = lines[i]
        if _indent_level(line) < 1 and line.strip():
            break
        stripped = line.strip()
        if not stripped:
            i += 1
            continue

        m = re.match(r'fromColumn:\s*(.+)\.(.+)', stripped)
        if m:
            rel.from_table = _unquote(m.group(1))
            rel.from_column = _unquote(m.group(2))
            i += 1
            continue
        m = re.match(r'toColumn:\s*(.+)\.(.+)', stripped)
        if m:
            rel.to_table = _unquote(m.group(1))
            rel.to_column = _unquote(m.group(2))
            i += 1
            continue
        m = re.match(r'crossFilteringBehavior:\s*(.+)', stripped)
        if m:
            rel.cross_filtering = m.group(1).strip()
            i += 1
            continue
        m = re.match(r'isActive:\s*(.+)', stripped)
        if m:
            rel.is_active = m.group(1).strip().lower() == "true"
            i += 1
            continue

        # Generic property
        m_prop = re.match(r'(\w[\w\s]*\w|\w+)\s*[:=]\s*(.+)', stripped)
        if m_prop:
            rel.properties[m_prop.group(1).strip()] = m_prop.group(2).strip()

        i += 1

    # Auto-generate name if empty
    if not rel.name and rel.from_table and rel.to_table:
        rel.name = f"{rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"

    return rel, i


# ======================================================================
# Model file parser
# ======================================================================

def parse_model_file(path: str | Path) -> TmdlModel:
    """Parse a model.tmdl file for model-level metadata."""
    path = Path(path)
    if not path.exists():
        return TmdlModel()

    lines = path.read_text(encoding="utf-8").splitlines()
    model = TmdlModel()

    for line in lines:
        stripped = line.strip()

        m = re.match(r'^model\s+(.*)', stripped)
        if m:
            model.name = _unquote(m.group(1)) if m.group(1).strip() else ""
            continue

        m = re.match(r'culture:\s*(.+)', stripped)
        if m:
            model.culture = m.group(1).strip()
            continue
        m = re.match(r'defaultPowerBIDataSourceVersion:\s*(.+)', stripped)
        if m:
            model.default_power_bi_data_source_version = m.group(1).strip()
            continue

        # Description from ///
        if stripped.startswith("///"):
            model.description = stripped[3:].strip()
            continue

        m = re.match(r'(\w[\w\s]*\w|\w+)\s*[:=]\s*(.+)', stripped)
        if m:
            model.properties[m.group(1).strip()] = m.group(2).strip()

    return model
