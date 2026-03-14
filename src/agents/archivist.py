from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from src.models import DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)

GIT_DAYS = int(os.getenv("CARTOGRAPHER_GIT_DAYS", "30"))


def _safe_rel(path: str, repo_path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except Exception:
        return path


def _detect_repo_name(repo_path: Path) -> str:
    """Try git remote first, fall back to directory name."""
    try:
        r = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_path, capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0 and r.stdout.strip():
            url  = r.stdout.strip().rstrip(".git")
            name = url.split("/")[-1]
            if name:
                return name
    except Exception:
        pass
    return repo_path.name


def _detect_system_type(module_graph: ModuleGraph, lineage_graph: DataLineageGraph) -> str:
    """Infer the kind of system from file extensions and domain clusters."""
    paths      = " ".join(module_graph.nodes.keys()).lower()
    domains    = " ".join(
        n.domain_cluster or "" for n in module_graph.nodes.values()
    ).lower()
    datasets   = " ".join(lineage_graph.dataset_nodes.keys()).lower()

    if "dbt_project" in paths or ".sql" in paths and "ref(" in datasets:
        return "dbt data transformation project"
    if "airflow" in paths or "dag" in paths:
        return "Apache Airflow pipeline"
    if "dagster" in paths or "asset" in domains:
        return "Dagster data platform"
    if "spark" in paths or "pyspark" in paths:
        return "PySpark data processing system"
    if any(k in paths for k in ("fastapi", "django", "flask")):
        return "Python web / API service"
    if "notebook" in paths or ".ipynb" in paths:
        return "Jupyter notebook-based analytics project"
    return "Python data engineering codebase"


class Archivist:
    def __init__(
        self,
        repo_path: Path,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        semanticist: Any | None = None,
        agent_trace_records: list[dict[str, Any]] | None = None,
    ) -> None:
        self.repo_path    = repo_path.resolve()
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.semanticist  = semanticist
        # Pre-populated trace entries from Surveyor + Hydrologist passed in by orchestrator
        self._upstream_trace: list[dict[str, Any]] = agent_trace_records or []

    def run(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "CODEBASE.md").write_text(self.generate_CODEBASE_md(),        encoding="utf-8")
        (output_dir / "onboarding_brief.md").write_text(self.generate_onboarding_brief_md(), encoding="utf-8")
        self.write_cartography_trace(output_dir / "cartography_trace.jsonl")
        self._write_semantic_index(output_dir)
        logger.info("[Archivist] Wrote CODEBASE.md, onboarding_brief.md, cartography_trace.jsonl, semantic_index/")

    # ------------------------------------------------------------------
    # semantic_index/ — vector-ready per-module purpose index
    # ------------------------------------------------------------------

    def _write_semantic_index(self, output_dir: Path) -> None:
        """
        Write semantic_index/ directory: one JSON per module + manifest.json.
        Each entry contains the purpose statement and metadata needed for
        semantic search and vector embedding by downstream tools.
        """
        index_dir = output_dir / "semantic_index"
        index_dir.mkdir(parents=True, exist_ok=True)

        manifest: list[dict[str, Any]] = []

        sorted_nodes = sorted(
            self.module_graph.nodes.items(),
            key=lambda kv: kv[1].pagerank_score,
            reverse=True,
        )

        for path, node in sorted_nodes:
            entry: dict[str, Any] = {
                "path": path,
                "relative_path": _safe_rel(path, self.repo_path),
                "language": str(node.language),
                "purpose_statement": node.purpose_statement or "",
                "domain_cluster": node.domain_cluster or "unclassified",
                "exported_functions": node.exported_functions[:15],
                "exported_classes": node.exported_classes[:10],
                "pagerank_score": round(node.pagerank_score, 6),
                "complexity_score": round(node.complexity_score, 2),
                "change_velocity_30d": node.change_velocity_30d,
                "loc": node.loc,
                "is_dead_code_candidate": node.is_dead_code_candidate,
            }

            # Safe filename: replace path separators
            safe_name = (
                _safe_rel(path, self.repo_path)
                .replace("/", "__")
                .replace("\\", "__")
                .replace(":", "__")
            ) + ".json"

            (index_dir / safe_name).write_text(
                json.dumps(entry, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            manifest.append(entry)

        # Write manifest — the entry point for semantic search tools
        manifest_meta: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "repo": _detect_repo_name(self.repo_path),
            "total_modules": len(manifest),
            "index_type": "purpose_statement_keyword",
            "note": (
                "Each .json file contains the purpose statement and metadata for one module. "
                "Embed purpose_statement fields for vector similarity search, "
                "or use keyword matching on exported_functions and domain_cluster."
            ),
            "modules": manifest,
        }
        (index_dir / "manifest.json").write_text(
            json.dumps(manifest_meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(
            f"[Archivist] semantic_index/ written — {len(manifest)} module entries"
        )

    # ------------------------------------------------------------------
    # CODEBASE.md
    # ------------------------------------------------------------------

    def generate_CODEBASE_md(self) -> str:
        repo_name     = _detect_repo_name(self.repo_path)
        system_type   = _detect_system_type(self.module_graph, self.lineage_graph)
        hubs          = self.module_graph.architectural_hubs[:5]
        high_velocity = self.module_graph.high_velocity_files[:10]
        sources       = self.lineage_graph.sources[:15]
        sinks         = self.lineage_graph.sinks[:15]
        cycles        = self.module_graph.circular_dependencies[:10]

        drift_flags: dict[str, str] = {}
        if self.semanticist and hasattr(self.semanticist, "drift_flags"):
            drift_flags = self.semanticist.drift_flags or {}

        purpose_nodes = sorted(
            self.module_graph.nodes.values(),
            key=lambda n: (n.domain_cluster or "", -n.pagerank_score, n.path),
        )

        # Build high-velocity set for badge rendering in purpose index
        high_vel_set = set(self.module_graph.high_velocity_files[:20])

        lines: list[str] = [
            f"# CODEBASE.md — `{repo_name}`",
            "",
            f"_Generated: {datetime.now(UTC).isoformat()}_",
            f"_System type: {system_type}_",
            "",
            "## Architecture Overview",
            "",
            self._architecture_overview_paragraph(repo_name, system_type),
            "",
            "## Critical Path",
            "",
            "Top modules by PageRank (highest structural influence):",
            "",
        ]

        if hubs:
            for i, hub in enumerate(hubs, 1):
                node = self.module_graph.nodes.get(hub)
                lines.append(f"{i}. `{_safe_rel(hub, self.repo_path)}`")
                if node and node.purpose_statement:
                    lines.append(f"   - **Purpose:** {node.purpose_statement}")
                if node:
                    lines.append(
                        f"   - **PageRank:** `{node.pagerank_score:.5f}` "
                        f"| **Domain:** `{node.domain_cluster or 'unclassified'}` "
                        f"| **Velocity:** `{node.change_velocity_30d}` commits ({GIT_DAYS}d) "
                        f"| **LOC:** `{node.loc}` "
                        f"| **Evidence:** static analysis (tree-sitter AST + PageRank)"
                    )
        else:
            lines.append("- No architectural hubs detected.")

        lines += ["", "## Data Sources & Sinks", "", "### Sources"]
        lines += [f"- `{_safe_rel(s, self.repo_path)}`" for s in sources] if sources else ["- None detected"]
        lines += ["", "### Sinks"]
        lines += [f"- `{_safe_rel(s, self.repo_path)}`" for s in sinks] if sinks else ["- None detected"]

        lines += ["", "## Known Debt", "", "### Circular Dependencies"]
        if cycles:
            for cycle in cycles:
                rendered = " ↔ ".join(f"`{_safe_rel(p, self.repo_path)}`" for p in cycle)
                lines.append(f"- {rendered}")
        else:
            lines.append("- No circular dependencies detected")

        lines += ["", "### Documentation Drift"]
        if drift_flags:
            for path, drift in sorted(drift_flags.items()):
                lines.append(f"- `{_safe_rel(path, self.repo_path)}` — {drift}")
        else:
            lines.append("- No documentation drift flags recorded")

        lines += [
            "",
            "## High-Velocity Files",
            "",
            f"Files with most commits in the last {GIT_DAYS} days (likely pain points):",
        ]
        if high_velocity:
            for rank, p in enumerate(high_velocity, 1):
                node = self.module_graph.nodes.get(p)
                vel  = node.change_velocity_30d if node else 0
                lines.append(f"{rank}. `{_safe_rel(p, self.repo_path)}` — `{vel}` commits")
        else:
            lines.append("- No git velocity data available (shallow clone or no git history)")

        lines += ["", "## Module Purpose Index", ""]

        current_domain = None
        for node in purpose_nodes:
            domain = node.domain_cluster or "unclassified"
            if domain != current_domain:
                current_domain = domain
                lines += [f"### {domain}", ""]

            vel_badge = f" 🔥`{node.change_velocity_30d}`" if node.path in high_vel_set and node.change_velocity_30d > 0 else ""
            dead_badge = " ⚠️dead-code-candidate" if node.is_dead_code_candidate else ""
            lines.append(f"- `{_safe_rel(node.path, self.repo_path)}`{vel_badge}{dead_badge}")
            if node.purpose_statement:
                lines.append(f"  - {node.purpose_statement}")
            lines.append(
                f"  - LOC: `{node.loc}` | PageRank: `{node.pagerank_score:.5f}` | "
                f"Complexity: `{node.complexity_score:.1f}`"
            )

        return "\n".join(lines).strip() + "\n"

    def _architecture_overview_paragraph(self, repo_name: str, system_type: str) -> str:
        top_hubs     = self.module_graph.architectural_hubs[:3]
        n_modules    = len(self.module_graph.nodes)
        n_edges      = len(self.module_graph.edges)
        n_datasets   = len(self.lineage_graph.dataset_nodes)
        n_transforms = len(self.lineage_graph.transformation_nodes)
        n_cycles     = len(self.module_graph.circular_dependencies)
        dead_count   = sum(1 for n in self.module_graph.nodes.values() if n.is_dead_code_candidate)
        n_sources    = len(self.lineage_graph.sources)
        n_sinks      = len(self.lineage_graph.sinks)

        hub_str = ", ".join(f"`{_safe_rel(h, self.repo_path)}`" for h in top_hubs) or "none detected"

        lines = [
            f"- **System:** {system_type}",
            f"- **Modules:** `{n_modules}` files | `{n_edges}` import edges | `{dead_count}` dead-code candidates",
            f"- **Data layer:** `{n_datasets}` datasets | `{n_transforms}` transformations | `{n_sources}` sources | `{n_sinks}` sinks",
            f"- **Architectural hubs** _(PageRank, static analysis):_ {hub_str}",
            f"- **Circular dependencies:** {'none' if n_cycles == 0 else f'⚠ {n_cycles} group(s) — see Known Debt'}",
            f"- **Evidence method:** tree-sitter AST + git log + sqlglot SQL parsing",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # onboarding_brief.md
    # ------------------------------------------------------------------

    def generate_onboarding_brief_md(self) -> str:
        answers = {}
        if self.semanticist and hasattr(self.semanticist, "day_one_answers"):
            answers = self.semanticist.day_one_answers or {}

        repo_name   = _detect_repo_name(self.repo_path)
        system_type = _detect_system_type(self.module_graph, self.lineage_graph)

        lines: list[str] = [
            f"# FDE Day-One Onboarding Brief — `{repo_name}`",
            "",
            f"_Generated: {datetime.now(UTC).isoformat()}_",
            f"_System: {system_type}_",
            "",
            "## Five FDE Day-One Answers",
            "",
        ]

        if answers:
            for i, (question, answer) in enumerate(answers.items(), 1):
                lines.append(f"### Q{i}. {question}")
                lines.append("")
                lines.append(answer)
                lines.append("")
        else:
            lines += self._static_day_one_answers()

        lines += [
            "## Evidence Summary",
            "",
            f"- Repository: `{repo_name}`",
            f"- System type: {system_type}",
            f"- Module graph nodes: `{len(self.module_graph.nodes)}`",
            f"- Module graph edges: `{len(self.module_graph.edges)}`",
            f"- Lineage datasets: `{len(self.lineage_graph.dataset_nodes)}`",
            f"- Lineage transformations: `{len(self.lineage_graph.transformation_nodes)}`",
            f"- Git analysis window: `{GIT_DAYS}` days",
            f"- LLM-generated answers: `{'yes' if answers else 'no — static fallback used'}`",
            "",
            "## Immediate Next Actions",
            "",
            "1. Verify the top architectural hubs by navigating to their source files.",
            "2. Validate upstream lineage for the highest-value sink datasets.",
            "3. Inspect high-velocity files first — they're the most likely source of instability.",
            "4. Review documentation drift flags before trusting any existing comments/docstrings.",
            "5. Run `cartographer query <repo> --cartography-dir .cartography` to interactively explore.",
            "",
        ]
        return "\n".join(lines)

    def _static_day_one_answers(self) -> list[str]:
        hubs     = self.module_graph.architectural_hubs[:5]
        sources  = self.lineage_graph.sources[:5]
        sinks    = self.lineage_graph.sinks[:5]
        velocity = self.module_graph.high_velocity_files[:5]

        def _fmt(items: list[str], rel: bool = False) -> str:
            if not items:
                return "none detected"
            if rel:
                return ", ".join(f"`{_safe_rel(p, self.repo_path)}`" for p in items)
            return ", ".join(f"`{x}`" for x in items)

        return [
            "### Q1. What is the primary data ingestion path?",
            "",
            f"Static analysis detected these entry points (nodes with no upstream dependencies): {_fmt(sources)}.",
            "",
            "### Q2. What are the 3-5 most critical output datasets/endpoints?",
            "",
            f"Terminal sink datasets (no downstream dependents in lineage graph): {_fmt(sinks)}.",
            "",
            "### Q3. What is the blast radius if the most critical module fails?",
            "",
            f"Highest-centrality modules by PageRank: {_fmt(hubs, rel=True)}. "
            f"Changes to these propagate to the most downstream dependents.",
            "",
            "### Q4. Where is the business logic concentrated vs distributed?",
            "",
            f"Business logic is concentrated in the top-PageRank modules: {_fmt(hubs[:3], rel=True)}. "
            f"These are the most-imported files and therefore define the core interfaces.",
            "",
            f"### Q5. What has changed most frequently in the last {GIT_DAYS} days?",
            "",
            f"Highest-velocity files: {_fmt(velocity, rel=True)}. "
            f"These represent active pain points and likely sources of instability.",
            "",
        ]

    # ------------------------------------------------------------------
    # cartography_trace.jsonl — every agent action logged
    # ------------------------------------------------------------------

    def write_cartography_trace(self, path: Path) -> None:
        records: list[dict[str, Any]] = []

        # 1. Upstream records from Surveyor + Hydrologist (passed in by orchestrator)
        records.extend(self._upstream_trace)

        # 2. Per-module Semanticist records
        if self.semanticist and hasattr(self.semanticist, "budget"):
            for entry in self.semanticist.budget.call_log:
                records.append({
                    "timestamp":       datetime.now(UTC).isoformat(),
                    "agent":           "semanticist",
                    "action":          "llm_call",
                    "confidence":      "medium",
                    "evidence_method": "llm inference",
                    "details":         entry,
                })

        # 3. Documentation drift flags
        if self.semanticist and getattr(self.semanticist, "drift_flags", None):
            for module_path, drift in self.semanticist.drift_flags.items():
                records.append({
                    "timestamp":       datetime.now(UTC).isoformat(),
                    "agent":           "semanticist",
                    "action":          "documentation_drift_flag",
                    "confidence":      "medium",
                    "evidence_method": "llm inference over code vs docstring",
                    "module_path":     module_path,
                    "detail":          drift,
                })

        # 4. Domain classification results
        if self.semanticist and getattr(self.semanticist, "domain_map", None):
            records.append({
                "timestamp":       datetime.now(UTC).isoformat(),
                "agent":           "semanticist",
                "action":          "domain_classification",
                "confidence":      "medium",
                "evidence_method": "llm classification",
                "domain_counts":   {
                    d: sum(1 for v in self.semanticist.domain_map.values() if v == d)
                    for d in set(self.semanticist.domain_map.values())
                },
            })

        # 5. Budget summary
        if self.semanticist and hasattr(self.semanticist, "budget"):
            records.append({
                "timestamp":       datetime.now(UTC).isoformat(),
                "agent":           "semanticist",
                "action":          "budget_summary",
                "confidence":      "high",
                "evidence_method": "llm call accounting",
                "details":         self.semanticist.budget.summary(),
            })

        # 6. Archivist artifact generation records
        records.append({
            "timestamp":       datetime.now(UTC).isoformat(),
            "agent":           "archivist",
            "action":          "generate_CODEBASE_md",
            "confidence":      "high",
            "evidence_method": "static analysis + semantic synthesis",
            "evidence_counts": {
                "modules":         len(self.module_graph.nodes),
                "module_edges":    len(self.module_graph.edges),
                "datasets":        len(self.lineage_graph.dataset_nodes),
                "transformations": len(self.lineage_graph.transformation_nodes),
                "drift_flags":     len(self.semanticist.drift_flags) if self.semanticist else 0,
            },
        })

        records.append({
            "timestamp":       datetime.now(UTC).isoformat(),
            "agent":           "archivist",
            "action":          "generate_onboarding_brief",
            "confidence":      "medium",
            "evidence_method": "semantic synthesis" if self.semanticist else "static fallback",
            "llm_answers_used": bool(
                self.semanticist and getattr(self.semanticist, "day_one_answers", None)
            ),
        })

        with path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info(f"[Archivist] cartography_trace.jsonl — {len(records)} records written")