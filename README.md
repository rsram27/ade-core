# ADE Core - Agentic Data Engineering Framework

**ADE** (Analytics Data Environment) is a lightweight framework that enables **autonomous data engineering** workflows using AI agents like Claude Code.

## What is Agentic Data Engineering?

It's not about AI writing code for you. It's about giving AI agents the **context** they need to reason about your entire data architecture — then letting them work with high autonomy while you supervise.

ADE provides three layers of context:
1. **Knowledge Graph** — Every table, pipeline, notebook, report, and their relationships
2. **MCP Server** — AI agents query the graph directly via Model Context Protocol
3. **Operations Config** — Strategic context about what matters in your environment

## Supported Platforms

| Platform | Parser | Lineage |
|----------|--------|---------|
| Databricks | ✅ | ✅ |
| Power BI | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ |

*More platforms available in the enterprise version.*

## Quick Start

```bash
# Clone the repo
git clone https://github.com/rbutinar/ade-core.git
cd ade-core

# Install dependencies
pip install -r requirements.txt

# Run with demo environment
python -m ade_app.mcp_server.server
```

## Use with Claude Code

ADE is designed to work with [Claude Code](https://claude.ai/claude-code). Once the MCP server is running, Claude Code can:

- Query your data catalog
- Trace lineage upstream and downstream
- Analyze impact of changes
- Generate documentation

```
# In Claude Code
"What tables feed into the sales_summary report?"
"Show me the impact of changing the customer_id column"
```

## Documentation

- [Getting Started](docs/getting-started/README.md)
- [Platform Guides](docs/platforms/)
- [MCP Server Reference](docs/mcp-server.md)

## The Autonomous Data Engineer

This project powers **The Autonomous Data Engineer** video series on YouTube, where we show real agentic workflows for Databricks, Power BI, and complex data platforms.

- [YouTube Channel](https://youtube.com/@autonomous-data-engineer) *(coming soon)*
- [Episode Scripts](docs/podcast/)

## License

Apache 2.0 — See [LICENSE](LICENSE)

## Author

**Roberto Butinar** — Data Engineer & AI Automation Specialist

- [LinkedIn](https://linkedin.com/in/rbutinar)
- [GitHub](https://github.com/rbutinar)
