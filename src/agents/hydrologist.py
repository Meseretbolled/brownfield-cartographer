"""
Hydrologist Agent — Data Flow & Lineage Analyst.

Responsibilities:
  - Python dataflow: pandas read/write, SQLAlchemy, PySpark
  - SQL lineage: sqlglot-parsed .sql and dbt model files
  - YAML/config: Airflow DAG definitions, dbt schema.yml
  - Jupyter notebook .ipynb data references
  - Merges all analyzers into DataLineageGraph
  - blast_radius(node): downstream dependents via BFS
  - find_sources() / find_sinks(): ingestion/egress nodes
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from src.analyzers.dag_config_parser import analyze_configs
from src.analyzers.sql_lineage import analyze_sql_directory
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import (
    DataLineageGraph,
    DatasetNode,
    StorageType,
    TransformationNode,
    TransformationType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Python dataflow analysis (tree-sitter-free fallback with regex for speed)
# ---------------------------------------------------------------------------

# Patterns that read data  →  (regex_pattern, TransformationType)
_READ_PATTERNS: list[tuple[re.Pattern, TransformationType]] = [
    (re.compile(r'pd\.read_csv\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_READ),
    (re.compile(r'pd\.read_parquet\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_READ),
    (re.compile(r'pd\.read_sql\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_READ),
    (re.compile(r'spark\.read\.[a-z]+\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PYSPARK_READ),
    (re.compile(r'spark\.table\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PYSPARK_READ),
    (re.compile(r'session\.execute\s*\(\s*["\']([^"\']+)["\']'), TransformationType.SQLALCHEMY),
]

# Patterns that write data
_WRITE_PATTERNS: list[tuple[re.Pattern, TransformationType]] = [
    (re.compile(r'\.to_csv\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_WRITE),
    (re.compile(r'\.to_parquet\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_WRITE),
    (re.compile(r'\.to_sql\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PANDAS_WRITE),
    (re.compile(r'\.write\.[a-z]+\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PYSPARK_WRITE),
    (re.compile(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']'), TransformationType.PYSPARK_WRITE),
]


def _analyze_python_dataflow(
    repo_path: Path,
) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: dict[str, DatasetNode] = {}
    transformations: list[TransformationNode] = []

    for py_file in repo_path.rglob("*.py"):
        if any(p.startswith(".") or p in {"__pycache__", ".venv", "venv"} for p in py_file.parts):
            continue
        try:
            source = py_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        sources_found: list[str] = []
        targets_found: list[str] = []
        t_type = TransformationType.UNKNOWN

        for pattern, ptype in _READ_PATTERNS:
            for m in pattern.finditer(source):
                name = m.group(1).strip().lower()
                if name:
                    sources_found.append(name)
                    t_type = ptype

        for pattern, ptype in _WRITE_PATTERNS:
            for m in pattern.finditer(source):
                name = m.group(1).strip().lower()
                if name:
                    targets_found.append(name)
                    if t_type == TransformationType.UNKNOWN:
                        t_type = ptype

        if sources_found or targets_found:
            node_id = f"py::{py_file}"
            transformations.append(TransformationNode(
                id=node_id,
                source_datasets=list(set(sources_found)),
                target_datasets=list(set(targets_found)),
                transformation_type=t_type,
                source_file=str(py_file),
                line_range=(1, len(source.splitlines())),
            ))
            for name in sources_found + targets_found:
                if name not in datasets:
                    datasets[name] = DatasetNode(
                        name=name,
                        storage_type=StorageType.FILE,
                        source_file=str(py_file),
                    )

    return list(datasets.values()), transformations


# ---------------------------------------------------------------------------
# Notebook (.ipynb) analysis
# ---------------------------------------------------------------------------

def _analyze_notebooks(
    repo_path: Path,
) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: dict[str, DatasetNode] = {}
    transformations: list[TransformationNode] = []

    for nb_file in repo_path.rglob("*.ipynb"):
        if any(p.startswith(".") for p in nb_file.parts):
            continue
        try:
            nb = json.loads(nb_file.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue

        all_source = []
        for cell in nb.get("cells", []):
            if cell.get("cell_type") == "code":
                all_source.extend(cell.get("source", []))

        combined = "".join(all_source)
        sources: list[str] = []
        targets: list[str] = []

        for pattern, _ in _READ_PATTERNS:
            for m in pattern.finditer(combined):
                name = m.group(1).strip().lower()
                if name:
                    sources.append(name)
        for pattern, _ in _WRITE_PATTERNS:
            for m in pattern.finditer(combined):
                name = m.group(1).strip().lower()
                if name:
                    targets.append(name)

        if sources or targets:
            node_id = f"nb::{nb_file}"
            transformations.append(TransformationNode(
                id=node_id,
                source_datasets=list(set(sources)),
                target_datasets=list(set(targets)),
                transformation_type=TransformationType.PANDAS_READ,
                source_file=str(nb_file),
                line_range=(1, 1),
            ))
            for name in sources + targets:
                if name not in datasets:
                    datasets[name] = DatasetNode(
                        name=name,
                        storage_type=StorageType.FILE,
                        source_file=str(nb_file),
                    )

    return list(datasets.values()), transformations


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class Hydrologist:
    """
    Builds the DataLineageGraph by merging Python dataflow analysis,
    SQL lineage (via sqlglot), YAML/DAG config parsing, and notebook parsing.
    """

    def __init__(self, repo_path: Path, kg: KnowledgeGraph) -> None:
        self.repo_path = repo_path
        self.kg = kg

    def run(self) -> DataLineageGraph:
        logger.info(f"[Hydrologist] Building data lineage for {self.repo_path} ...")

        all_datasets: dict[str, DatasetNode] = {}
        all_transformations: list[TransformationNode] = []

        # --- Python dataflow ---
        py_datasets, py_transforms = _analyze_python_dataflow(self.repo_path)
        logger.info(f"[Hydrologist] Python: {len(py_transforms)} transformations, {len(py_datasets)} datasets")
        for d in py_datasets:
            all_datasets[d.name] = d
        all_transformations.extend(py_transforms)

        # --- SQL lineage ---
        sql_datasets, sql_transforms = analyze_sql_directory(self.repo_path)
        logger.info(f"[Hydrologist] SQL: {len(sql_transforms)} transformations, {len(sql_datasets)} datasets")
        for d in sql_datasets:
            all_datasets[d.name] = d
        all_transformations.extend(sql_transforms)

        # --- YAML / config ---
        cfg_datasets, cfg_transforms = analyze_configs(self.repo_path)
        logger.info(f"[Hydrologist] Config: {len(cfg_transforms)} ops, {len(cfg_datasets)} datasets")
        for d in cfg_datasets:
            all_datasets.setdefault(d.name, d)
        all_transformations.extend(cfg_transforms)

        # --- Notebooks ---
        nb_datasets, nb_transforms = _analyze_notebooks(self.repo_path)
        logger.info(f"[Hydrologist] Notebooks: {len(nb_transforms)} transformations, {len(nb_datasets)} datasets")
        for d in nb_datasets:
            all_datasets.setdefault(d.name, d)
        all_transformations.extend(nb_transforms)

        # --- Populate knowledge graph ---
        for dataset in all_datasets.values():
            self.kg.add_dataset(dataset)
        for transform in all_transformations:
            self.kg.add_transformation(transform)

        lineage_graph = self.kg.to_lineage_graph_schema(str(self.repo_path))
        logger.info(
            f"[Hydrologist] Done — {len(lineage_graph.dataset_nodes)} datasets, "
            f"{len(lineage_graph.transformation_nodes)} transformations, "
            f"{len(lineage_graph.sources)} sources, "
            f"{len(lineage_graph.sinks)} sinks"
        )
        return lineage_graph

    def blast_radius(self, node_id: str) -> list[str]:
        """All nodes downstream of node_id (BFS over lineage graph)."""
        return self.kg.blast_radius(node_id)

    def save(self, output_dir: Path, lineage_graph: DataLineageGraph) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "lineage_graph.json").write_text(
            lineage_graph.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"[Hydrologist] Saved lineage_graph.json → {output_dir}")