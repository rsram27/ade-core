# ADE Core - Claude Code Instructions

## Project Overview

ADE Core is the open-source version of the Analytics Data Environment framework.
This is a personal project by Roberto Butinar, designed to enable agentic data engineering workflows.

## Repository Structure

```
ade-core/
├── ade_app/                      # Framework code
│   ├── core/                     # Core services
│   ├── platforms/                # Platform parsers
│   │   ├── databricks/           # Databricks notebooks, jobs
│   │   ├── powerbi/              # Power BI datasets, measures
│   │   └── postgresql/           # PostgreSQL tables, views
│   ├── mcp_server/               # MCP Server for AI agents
│   ├── scripts/                  # CLI utilities
│   └── docs/                     # Documentation
│
├── ade_data/
│   └── demo/                     # Demo environment (synthetic data)
│
├── docs/
│   ├── getting-started/          # Setup guides
│   └── podcast/                  # "The Autonomous Data Engineer" series
│       ├── episodes/             # Episode scripts
│       └── production/           # Recording guides
│
└── examples/                     # Usage examples
```

## Key Principles

1. **This is a personal project** - All commits use `roberto.butinar@gmail.com`
2. **No client data** - Only synthetic demo data
3. **Open source** - Apache 2.0 license
4. **Educational** - Powers the YouTube video series

## Development Guidelines

- Keep code clean and well-documented
- All documentation in English
- Demo data must be realistic but entirely synthetic
- Never reference real client names or architectures

## Quick Commands

```bash
# Run MCP server
python -m ade_app.mcp_server.server

# Run tests
pytest

# Build docs
mkdocs serve
```

## Related

- YouTube: The Autonomous Data Engineer
- Full version: Private (enterprise features)
