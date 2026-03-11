# 🗺️ Brownfield Cartographer

> **Multi-agent codebase intelligence system for rapid FDE onboarding in production environments.**
> Point it at any GitHub repo or local path. Get a living, queryable map of the system's architecture, data flows, and semantic structure in under 60 seconds.

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/Meseretbolled/brownfield-cartographer.git
cd brownfield-cartographer

# 2. Create virtual environment and install
uv sync
source .venv/bin/activate

# 3. Set up your environment
cp .env.example .env
# Edit .env and add your OpenRouter API key

# 4. Run analysis on any repo
cartographer analyze /path/to/repo

# 5. Run analysis on a GitHub URL (auto-clones)
cartographer analyze https://github.com/dbt-labs/jaffle_shop

# 6. Launch interactive query interface
cartographer query /path/to/repo

# 7. Print summary of existing analysis
cartographer summary /path/to/repo
```

---

## Verify It Works

```bash
# 1. Check the CLI is installed and responds
cartographer --help

# 2. Run against the included jaffle_shop artefacts (no cloning needed)
cartographer query . --cartography-dir cartography-artifacts/jaffle_shop

# 3. Inside the query interface, try:
navigator> sources
navigator> sinks
navigator> blast_radius orders
navigator> hubs
navigator> quit

# 4. Run a fresh analysis against jaffle_shop
git clone https://github.com/dbt-labs/jaffle_shop /tmp/jaffle_shop
cartographer analyze /tmp/jaffle_shop

# 5. Inspect generated artefacts
ls /tmp/jaffle_shop/.cartography/
cat /tmp/jaffle_shop/.cartography/analysis_summary.md
```

---

## What It Does

The Cartographer runs four agents in sequence against any codebase:

| Agent | Role | Output |
| --- | --- | --- |
| **Surveyor** | Static AST analysis — module graph, PageRank, git velocity, dead code | `module_graph.json` |
| **Hydrologist** | Data lineage — Python dataflow, SQL (sqlglot), YAML/DAG configs, notebooks | `lineage_graph.json` |
| **Semanticist** | LLM purpose statements, doc drift detection, domain clustering, Day-One answers | `semanticist_trace.json` |
| **Archivist** | Produces all final artefacts — CODEBASE.md, onboarding brief, audit log | `CODEBASE.md`, `onboarding_brief.md` |

The **Navigator** agent provides an interactive query interface over the generated knowledge graph.

---

## Commands

### `analyze` — Full pipeline

```bash
cartographer analyze <repo>

# Options:
#   --output, -o        Custom output directory (default: <repo>/.cartography/)
#   --incremental, -i   Only re-analyse files changed since last run
#   --git-days          Days of git history for velocity (default: 30)

# Examples:
cartographer analyze /tmp/jaffle_shop
cartographer analyze https://github.com/dbt-labs/jaffle_shop
cartographer analyze /tmp/jaffle_shop --output ./my-output --git-days 60
cartographer analyze /tmp/jaffle_shop --incremental
```

### `query` — Interactive Navigator

```bash
cartographer query <repo>

# Inside the navigator:
blast_radius <node>          # All downstream dependents
lineage <dataset>            # Upstream sources of a dataset
module <path>                # Full detail on a module
sources                      # All data ingestion entry points
sinks                        # All data output endpoints
hubs                         # Top modules by PageRank
quit                         # Exit
```

### `summary` — Quick summary

```bash
cartographer summary <repo>
```

---

## Generated Artefacts

Every analysis run produces these files in `.cartography/`:

| File | Description |
| --- | --- |
| `module_graph.json` | Full module import graph with PageRank scores |
| `lineage_graph.json` | Data lineage DAG (datasets + transformations) |
| `analysis_summary.md` | Human-readable run summary |
| `CODEBASE.md` | Living context file — inject into any AI coding agent |
| `onboarding_brief.md` | Five FDE Day-One questions answered with evidence |
| `cartography_trace.jsonl` | Audit log of every agent action |

---

## Architecture

![Brownfield Cartographer — Four-Agent Pipeline](assets/Language%20Analyzer%20Ecosystem.png)

> Four agents run in sequence: **Surveyor** (static structure) → **Hydrologist** (data lineage) → **Semanticist** (LLM semantic layer) → **Archivist** (living artefacts). The **Navigator** provides interactive querying over the generated knowledge graph.

---

## Project Structure

```
brownfield-cartographer/
├── src/
│   ├── cli.py                          # Entry point: analyze, query, summary
│   ├── orchestrator.py                 # Pipeline wiring + incremental mode
│   ├── models/__init__.py              # Pydantic schemas (all node/edge types)
│   ├── graph/knowledge_graph.py        # NetworkX wrapper + serialization
│   ├── analyzers/
│   │   ├── tree_sitter_analyzer.py     # Multi-language AST parsing
│   │   ├── sql_lineage.py              # sqlglot SQL dependency extraction
│   │   └── dag_config_parser.py        # Airflow/dbt YAML config parsing
│   └── agents/
│       ├── surveyor.py                 # Module graph, PageRank, git velocity
│       ├── hydrologist.py              # Data lineage graph
│       ├── semanticist.py              # LLM purpose statements, doc drift
│       ├── archivist.py                # CODEBASE.md, onboarding brief
│       └── navigator.py               # Interactive query agent
├── assets/
│   └── Language Analyzer Ecosystem.png  # Architecture diagram
├── cartography-artifacts/
│   └── jaffle_shop/                    # Pre-generated artefacts
│       ├── module_graph.json
│       ├── lineage_graph.json
│       └── analysis_summary.md
├── .env.example                        # Environment variable template
├── pyproject.toml
└── README.md
```

---

## Supported Languages & Patterns

| Language | What's Extracted |
| --- | --- |
| **Python** | Imports, functions, classes, pandas/PySpark/SQLAlchemy dataflow |
| **SQL / dbt** | Table dependencies, CTEs, JOINs, `ref()` calls |
| **YAML** | Airflow DAG topology, dbt `schema.yml` sources and models |
| **Jupyter** | `.ipynb` cell source — read/write data references |
| **JavaScript/TypeScript** | AST parsing (imports, exports) |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```dotenv
# OpenRouter — free LLM API (https://openrouter.ai/keys)
OPENROUTER_API_KEY=your-openrouter-key-here
OPENROUTER_URL=https://openrouter.ai/api/v1
MODEL_NAME=openrouter/auto:free

# Semanticist tuning
CARTOGRAPHER_DOMAIN_K=6
CARTOGRAPHER_TOKEN_BUDGET=200000
CARTOGRAPHER_GIT_DAYS=30

# Logging
LOG_LEVEL=INFO
```

> **LLM features are optional.** All static analysis (Surveyor + Hydrologist) works with no API key at all.

---

## Target Codebases Tested

| Repo | Modules | Datasets | Transformations | Sources | Sinks |
| --- | --- | --- | --- | --- | --- |
| [dbt jaffle_shop](https://github.com/dbt-labs/jaffle_shop) | 3 | 9 | 5 | 4 | 3 |

---

## Dependencies

Key dependencies (see `pyproject.toml` for full list):

- `tree-sitter` — multi-language AST parsing
- `sqlglot` — SQL parsing and lineage extraction
- `networkx` — graph construction, PageRank, BFS
- `pydantic` — schema validation
- `typer` + `rich` — CLI and terminal output
- `gitpython` — git history analysis