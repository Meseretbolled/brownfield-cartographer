"""
Archivist Agent — Living Context Maintainer.

Produces:
  - CODEBASE.md
  - onboarding_brief.md
  - cartography_trace.jsonl

The goal is to create living artifacts that can be injected into an AI coding
agent or used by humans for Day-One brownfield onboarding.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from src.models import DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)


def _safe_rel(path: str, repo_path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except Exception:
        return path


class Archivist:
    def __init__(
        self,
        repo_path: Path,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        semanticist: Any | None = None,
    ) -> None:
        self.repo_path = repo_path.resolve()
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.semanticist = semanticist

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)

        codebase_md = self.generate_CODEBASE_md()
        onboarding_md = self.generate_onboarding_brief_md()

        (output_dir / "CODEBASE.md").write_text(codebase_md, encoding="utf-8")
        (output_dir / "onboarding_brief.md").write_text(onboarding_md, encoding="utf-8")

        self.write_cartography_trace(output_dir / "cartography_trace.jsonl")

        logger.info("[Archivist] Wrote CODEBASE.md, onboarding_brief.md, cartography_trace.jsonl")

    # ------------------------------------------------------------------
    # CODEBASE.md
    # ------------------------------------------------------------------

    def generate_CODEBASE_md(self) -> str:
        hubs = self.module_graph.architectural_hubs[:5]
        high_velocity = self.module_graph.high_velocity_files[:10]
        sources = self.lineage_graph.sources[:15]
        sinks = self.lineage_graph.sinks[:15]
        cycles = self.module_graph.circular_dependencies[:10]

        drift_flags: dict[str, str] = {}
        if self.semanticist and hasattr(self.semanticist, "drift_flags"):
            drift_flags = self.semanticist.drift_flags or {}

        purpose_nodes = sorted(
            self.module_graph.nodes.values(),
            key=lambda n: (
                n.domain_cluster or "",
                -n.pagerank_score,
                n.path,
            ),
        )

        overview = self._architecture_overview_paragraph()

        lines: list[str] = [
            "# CODEBASE.md",
            "",
            f"_Generated: {datetime.now(UTC).isoformat()}_",
            "",
            "## Architecture Overview",
            "",
            overview,
            "",
            "## Critical Path",
            "",
            "Top modules by PageRank (highest structural influence):",
            "",
        ]

        if hubs:
            for i, hub in enumerate(hubs, 1):
                node = self.module_graph.nodes.get(hub)
                purpose = node.purpose_statement if node else None
                lines.append(f"{i}. `{_safe_rel(hub, self.repo_path)}`")
                if purpose:
                    lines.append(f"   - Purpose: {purpose}")
                if node:
                    lines.append(f"   - PageRank: `{node.pagerank_score:.5f}`")
                    lines.append(f"   - Change velocity (30d): `{node.change_velocity_30d}`")
        else:
            lines.append("- No architectural hubs detected.")

        lines += [
            "",
            "## Data Sources & Sinks",
            "",
            "### Sources",
        ]
        if sources:
            for s in sources:
                lines.append(f"- `{s}`")
        else:
            lines.append("- None detected")

        lines += [
            "",
            "### Sinks",
        ]
        if sinks:
            for s in sinks:
                lines.append(f"- `{s}`")
        else:
            lines.append("- None detected")

        lines += [
            "",
            "## Known Debt",
            "",
            "### Circular Dependencies",
        ]
        if cycles:
            for cycle in cycles:
                rendered = " ↔ ".join(f"`{_safe_rel(p, self.repo_path)}`" for p in cycle)
                lines.append(f"- {rendered}")
        else:
            lines.append("- No circular dependencies detected")

        lines += [
            "",
            "### Documentation Drift",
        ]
        if drift_flags:
            for path, drift in sorted(drift_flags.items()):
                lines.append(f"- `{_safe_rel(path, self.repo_path)}` — {drift}")
        else:
            lines.append("- No documentation drift flags recorded")

        lines += [
            "",
            "## High-Velocity Files",
            "",
            "Files changing most frequently in recent git history:",
        ]
        if high_velocity:
            for p in high_velocity:
                node = self.module_graph.nodes.get(p)
                vel = node.change_velocity_30d if node else 0
                lines.append(f"- `{_safe_rel(p, self.repo_path)}` — `{vel}` changes")
        else:
            lines.append("- No git velocity data available")

        lines += [
            "",
            "## Module Purpose Index",
            "",
        ]

        current_domain = None
        for node in purpose_nodes:
            domain = node.domain_cluster or "unclassified"
            if domain != current_domain:
                current_domain = domain
                lines += [
                    f"### {domain}",
                    "",
                ]
            lines.append(f"- `{_safe_rel(node.path, self.repo_path)}`")
            if node.purpose_statement:
                lines.append(f"  - {node.purpose_statement}")
            lines.append(f"  - LOC: `{node.loc}` | PageRank: `{node.pagerank_score:.5f}`")

        return "\n".join(lines).strip() + "\n"

    def _architecture_overview_paragraph(self) -> str:
        module_count = len(self.module_graph.nodes)
        import_edges = len(self.module_graph.edges)
        dataset_count = len(self.lineage_graph.dataset_nodes)
        transform_count = len(self.lineage_graph.transformation_nodes)

        top_hubs = ", ".join(
            f"`{_safe_rel(h, self.repo_path)}`" for h in self.module_graph.architectural_hubs[:3]
        ) or "no clear hubs"

        return (
            f"This repository was analyzed as a mixed-codebase system with "
            f"`{module_count}` modules, `{import_edges}` import dependencies, "
            f"`{dataset_count}` datasets, and `{transform_count}` lineage transformations. "
            f"The structural center of gravity is around {top_hubs}. "
            f"The data layer appears to flow from discovered source nodes into downstream "
            f"transformations and sink datasets captured in the lineage graph."
        )

    # ------------------------------------------------------------------
    # onboarding_brief.md
    # ------------------------------------------------------------------

    def generate_onboarding_brief_md(self) -> str:
        answers = {}
        if self.semanticist and hasattr(self.semanticist, "day_one_answers"):
            answers = self.semanticist.day_one_answers or {}

        lines: list[str] = [
            "# onboarding_brief.md",
            "",
            f"_Generated: {datetime.now(UTC).isoformat()}_",
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
            f"- Module graph nodes: `{len(self.module_graph.nodes)}`",
            f"- Module graph edges: `{len(self.module_graph.edges)}`",
            f"- Lineage datasets: `{len(self.lineage_graph.dataset_nodes)}`",
            f"- Lineage transformations: `{len(self.lineage_graph.transformation_nodes)}`",
            "",
            "## Immediate Next Actions",
            "",
            "1. Verify the top architectural hubs in code.",
            "2. Validate upstream lineage for the highest-value sink datasets.",
            "3. Inspect high-velocity files first for likely instability or debt.",
            "4. Review documentation drift flags before trusting comments/docstrings.",
            "",
        ]
        return "\n".join(lines)

    def _static_day_one_answers(self) -> list[str]:
        hubs = self.module_graph.architectural_hubs[:5]
        sources = self.lineage_graph.sources[:5]
        sinks = self.lineage_graph.sinks[:5]
        velocity = self.module_graph.high_velocity_files[:5]

        return [
            "### Q1. What is the primary data ingestion path?",
            "",
            f"Static analysis detected these likely entry points: {', '.join(f'`{s}`' for s in sources) or 'none detected'}.",
            "",
            "### Q2. What are the 3-5 most critical output datasets/endpoints?",
            "",
            f"Likely critical sinks: {', '.join(f'`{s}`' for s in sinks) or 'none detected'}.",
            "",
            "### Q3. What is the blast radius if the most critical module fails?",
            "",
            f"Highest-risk modules by structural centrality: {', '.join(f'`{_safe_rel(h, self.repo_path)}`' for h in hubs) or 'none detected'}.",
            "",
            "### Q4. Where is the business logic concentrated vs distributed?",
            "",
            f"Business logic appears concentrated around the top hubs and their surrounding import neighborhoods: {', '.join(f'`{_safe_rel(h, self.repo_path)}`' for h in hubs[:3]) or 'unknown'}.",
            "",
            "### Q5. What has changed most frequently in the last 30 days?",
            "",
            f"High-velocity files: {', '.join(f'`{_safe_rel(v, self.repo_path)}`' for v in velocity) or 'no git data available'}.",
            "",
        ]

    # ------------------------------------------------------------------
    # cartography_trace.jsonl
    # ------------------------------------------------------------------

    def write_cartography_trace(self, path: Path) -> None:
        records: list[dict[str, Any]] = []

        records.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "agent": "archivist",
            "action": "generate_CODEBASE_md",
            "confidence": "high",
            "evidence_method": "static analysis + semantic synthesis",
            "evidence_counts": {
                "modules": len(self.module_graph.nodes),
                "module_edges": len(self.module_graph.edges),
                "datasets": len(self.lineage_graph.dataset_nodes),
                "transformations": len(self.lineage_graph.transformation_nodes),
            },
        })

        records.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "agent": "archivist",
            "action": "generate_onboarding_brief",
            "confidence": "medium",
            "evidence_method": "semantic synthesis" if self.semanticist else "static fallback",
        })

        if self.semanticist and hasattr(self.semanticist, "budget"):
            records.append({
                "timestamp": datetime.now(UTC).isoformat(),
                "agent": "semanticist",
                "action": "budget_summary",
                "confidence": "high",
                "evidence_method": "llm call accounting",
                "details": self.semanticist.budget.summary(),
            })

        if self.semanticist and getattr(self.semanticist, "drift_flags", None):
            for module_path, drift in self.semanticist.drift_flags.items():
                records.append({
                    "timestamp": datetime.now(UTC).isoformat(),
                    "agent": "semanticist",
                    "action": "documentation_drift_flag",
                    "confidence": "medium",
                    "evidence_method": "llm inference over code vs docstring",
                    "module_path": module_path,
                    "detail": drift,
                })

        with path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")