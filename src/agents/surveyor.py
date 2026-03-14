"""
Surveyor Agent — Static Structure Analyst.

Responsibilities:
  - Multi-language AST parsing via tree-sitter (Python, SQL, YAML, JS/TS)
  - Module import graph construction (NetworkX DiGraph)
  - PageRank to identify architectural hubs
  - Git velocity: change frequency per file over N days
  - Strongly-connected-component detection (circular dependencies)
  - Dead-code-candidate flagging (exported symbols never imported)
"""
from __future__ import annotations

import logging
import subprocess
from collections import defaultdict
from pathlib import Path

from src.analyzers.tree_sitter_analyzer import analyze_directory
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import Language as Lang
from src.models import ModuleGraph, ModuleNode

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Git velocity helpers
# ---------------------------------------------------------------------------

def _git_change_counts(repo_path: Path, days: int = 30) -> dict[str, int]:
    """Return {relative_file_path: commit_count} for commits in last N days."""
    counts: dict[str, int] = defaultdict(int)
    try:
        result = subprocess.run(
            ["git", "log", f"--since={days} days ago", "--name-only", "--pretty=format:"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if line:
                counts[line] += 1
    except Exception as e:
        logger.warning(f"git log failed (non-fatal): {e}")
    return dict(counts)


def _annotate_git_velocity(
    nodes: list[ModuleNode], repo_path: Path, days: int = 30
) -> list[ModuleNode]:
    counts = _git_change_counts(repo_path, days)
    for node in nodes:
        try:
            rel = str(Path(node.path).relative_to(repo_path))
        except ValueError:
            rel = node.path
        node.change_velocity_30d = counts.get(rel, 0)
    return nodes


# ---------------------------------------------------------------------------
# Dead-code detection
# ---------------------------------------------------------------------------

def _detect_dead_code(nodes: list[ModuleNode], kg: KnowledgeGraph) -> list[ModuleNode]:
    """
    Flag a module as dead-code candidate when it has exported symbols but has
    no inbound graph edges. This works better for mixed-language repos than
    Python-only string matching.
    """
    inbound_counts: dict[str, int] = {node.path: 0 for node in nodes}

    for source, target in kg.module_graph.edges():
        if target in inbound_counts:
            inbound_counts[target] += 1

    for node in nodes:
        has_exports = bool(node.exported_functions or node.exported_classes)
        if has_exports and inbound_counts.get(node.path, 0) == 0:
            node.is_dead_code_candidate = True
        else:
            node.is_dead_code_candidate = False

    return nodes


# ---------------------------------------------------------------------------
# Import graph construction
# ---------------------------------------------------------------------------

def _resolve_python_import_to_path(
    imp: str, repo_path: Path, all_paths: set[str]
) -> str | None:
    candidate = imp.replace(".", "/")
    for suffix in (".py", "/__init__.py"):
        full = str((repo_path / (candidate + suffix)).resolve())
        if full in all_paths:
            return full
    return None


def _build_dbt_model_index(nodes: list[ModuleNode]) -> dict[str, str]:
    """
    Map exported dbt model names to the SQL file path that defines them.

    Example:
      "stg_orders" -> ".../models/staging/stg_orders.sql"
    """
    index: dict[str, str] = {}
    for node in nodes:
        if node.language != Lang.SQL:
            continue
        for exported in node.exported_functions:
            if exported:
                index[exported] = node.path
    return index


def _build_import_graph(
    nodes: list[ModuleNode], kg: KnowledgeGraph, repo_path: Path
) -> None:
    all_paths = {str(Path(n.path).resolve()) for n in nodes}
    dbt_index = _build_dbt_model_index(nodes)

    # Add all nodes first
    for node in nodes:
        kg.add_module(node)

    # Add edges
    for node in nodes:
        for imp in node.imports:
            target: str | None = None

            # Python dotted imports
            if not imp.startswith("dbt_ref:") and not imp.startswith("dbt_source:"):
                target = _resolve_python_import_to_path(imp, repo_path, all_paths)

            # dbt refs -> SQL model file
            elif imp.startswith("dbt_ref:"):
                model_name = imp.split("dbt_ref:", 1)[1].strip()
                target = dbt_index.get(model_name)

            # dbt sources do not map to repo module files directly
            elif imp.startswith("dbt_source:"):
                target = None

            if target and target != node.path:
                kg.add_module_edge(node.path, target)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class Surveyor:
    """
    Orchestrates static analysis of a repo and populates KnowledgeGraph with
    ModuleNodes, import edges, PageRank scores, and structural metadata.
    """

    def __init__(self, repo_path: Path, kg: KnowledgeGraph) -> None:
        self.repo_path = repo_path.resolve()
        self.kg = kg

    def run(self, git_days: int = 30, changed_files: list[Path] | None = None) -> ModuleGraph:
        if changed_files:
            logger.info(
                f"[Surveyor] Incremental mode — analysing {len(changed_files)} "
                f"changed file(s) in {self.repo_path} ..."
            )
            from src.analyzers.tree_sitter_analyzer import analyze_module
            nodes = []
            for f in changed_files:
                node = analyze_module(f)
                if node:
                    nodes.append(node)
            logger.info(f"[Surveyor] Incremental: parsed {len(nodes)} changed module(s)")
        else:
            logger.info(f"[Surveyor] Analysing {self.repo_path} ...")
            nodes = analyze_directory(self.repo_path)
            logger.info(f"[Surveyor] Parsed {len(nodes)} module files")

        nodes = _annotate_git_velocity(nodes, self.repo_path, git_days)

        _build_import_graph(nodes, self.kg, self.repo_path)

        nodes = _detect_dead_code(nodes, self.kg)

        if len(self.kg.module_graph) > 0:
            self.kg.compute_pagerank()

        circular = self.kg.find_circular_dependencies()
        if circular:
            logger.info(f"[Surveyor] {len(circular)} circular dependency group(s) found")

        module_graph = self.kg.to_module_graph_schema(str(self.repo_path))
        logger.info(
            f"[Surveyor] Done — {len(module_graph.nodes)} modules, "
            f"{len(module_graph.edges)} edges, "
            f"{len(module_graph.circular_dependencies)} cycles"
        )
        return module_graph

    def save(self, output_dir: Path, module_graph: ModuleGraph) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "module_graph.json").write_text(
            module_graph.model_dump_json(indent=2),
            encoding="utf-8",
        )
        logger.info(f"[Surveyor] Saved module_graph.json → {output_dir}")