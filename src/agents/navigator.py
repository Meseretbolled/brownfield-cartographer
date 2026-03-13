"""
Navigator Agent — query interface over the knowledge graph.

Required tools:
  - find_implementation(concept)
  - trace_lineage(dataset, direction)
  - blast_radius(module_path)
  - explain_module(path)

This implementation is lightweight and does not require LangGraph to run.
If LangGraph is added later, these methods can be wrapped as tools directly.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import networkx as nx

from src.graph.knowledge_graph import KnowledgeGraph
from src.models import DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)


class Navigator:
    def __init__(
        self,
        repo_path: Path,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        kg: KnowledgeGraph | None = None,
        semanticist: Any | None = None,
    ) -> None:
        self.repo_path = repo_path.resolve()
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.semanticist = semanticist
        self.kg = kg or self._rebuild_kg()

    def _rebuild_kg(self) -> KnowledgeGraph:
        kg = KnowledgeGraph()

        for node in self.module_graph.nodes.values():
            kg.add_module(node)
        for edge in self.module_graph.edges:
            kg.add_module_edge(edge.source, edge.target)

        for node in self.lineage_graph.dataset_nodes.values():
            kg.add_dataset(node)
        for node in self.lineage_graph.transformation_nodes.values():
            kg.add_transformation(node)

        return kg

    def _rel(self, path: str) -> str:
        try:
            return str(Path(path).resolve().relative_to(self.repo_path))
        except Exception:
            return path

    # ------------------------------------------------------------------
    # Tool 1
    # ------------------------------------------------------------------

    def find_implementation(self, concept: str, limit: int = 10) -> dict[str, Any]:
        """
        Semantic-ish lookup using purpose statements, domain names, exports, and paths.
        """
        concept_l = concept.lower().strip()
        scored: list[tuple[float, str, dict[str, Any]]] = []

        for path, node in self.module_graph.nodes.items():
            score = 0.0
            text_parts = [
                path.lower(),
                (node.purpose_statement or "").lower(),
                (node.domain_cluster or "").lower(),
                " ".join(node.exported_functions).lower(),
                " ".join(node.exported_classes).lower(),
            ]
            blob = " ".join(text_parts)

            if concept_l in blob:
                score += 5.0
            for token in concept_l.split():
                if token and token in blob:
                    score += 1.0

            if score > 0:
                scored.append((
                    score,
                    path,
                    {
                        "path": self._rel(path),
                        "purpose_statement": node.purpose_statement,
                        "domain_cluster": node.domain_cluster,
                        "analysis_method": "static metadata + semantic purpose matching",
                        "evidence": {
                            "exports": node.exported_functions[:8],
                            "classes": node.exported_classes[:8],
                        },
                    },
                ))

        scored.sort(key=lambda x: (-x[0], x[1]))
        return {
            "query": concept,
            "matches": [item[2] for item in scored[:limit]],
        }

    # ------------------------------------------------------------------
    # Tool 2
    # ------------------------------------------------------------------

    def trace_lineage(self, dataset: str, direction: str = "upstream") -> dict[str, Any]:
        """
        Trace lineage around a dataset.
        direction ∈ {"upstream", "downstream", "both"}
        """
        if dataset not in self.kg.lineage_graph:
            return {
                "dataset": dataset,
                "direction": direction,
                "analysis_method": "graph traversal",
                "error": "dataset not found in lineage graph",
            }

        if direction not in {"upstream", "downstream", "both"}:
            return {
                "dataset": dataset,
                "direction": direction,
                "analysis_method": "graph traversal",
                "error": "direction must be one of: upstream, downstream, both",
            }

        result: dict[str, Any] = {
            "dataset": dataset,
            "direction": direction,
            "analysis_method": "graph traversal",
        }

        if direction in {"upstream", "both"}:
            upstream = sorted(nx.ancestors(self.kg.lineage_graph, dataset))
            result["upstream"] = upstream

        if direction in {"downstream", "both"}:
            downstream = sorted(nx.descendants(self.kg.lineage_graph, dataset))
            result["downstream"] = downstream

        return result

    # ------------------------------------------------------------------
    # Tool 3
    # ------------------------------------------------------------------

    def blast_radius(self, module_path: str) -> dict[str, Any]:
        """
        Blast radius over the module import graph.
        """
        candidates = [p for p in self.module_graph.nodes if module_path in p]
        if not candidates:
            return {
                "module_path": module_path,
                "analysis_method": "module dependency traversal",
                "error": "module not found",
            }

        resolved = candidates[0]
        downstream = sorted(nx.descendants(self.kg.module_graph, resolved))
        return {
            "module_path": self._rel(resolved),
            "analysis_method": "module dependency traversal",
            "downstream_dependents": [self._rel(p) for p in downstream],
            "count": len(downstream),
        }

    # ------------------------------------------------------------------
    # Tool 4
    # ------------------------------------------------------------------

    def explain_module(self, path: str) -> dict[str, Any]:
        """
        Explain a module using the already-computed node metadata.
        """
        candidates = [p for p in self.module_graph.nodes if path in p]
        if not candidates:
            return {
                "path": path,
                "analysis_method": "static analysis + semantic synthesis",
                "error": "module not found",
            }

        resolved = candidates[0]
        node = self.module_graph.nodes[resolved]

        imported_by = sorted([
            edge.source for edge in self.module_graph.edges
            if edge.target == resolved
        ])
        imports = sorted([
            edge.target for edge in self.module_graph.edges
            if edge.source == resolved
        ])

        return {
            "path": self._rel(resolved),
            "analysis_method": "static analysis + semantic synthesis",
            "language": str(node.language),
            "purpose_statement": node.purpose_statement,
            "domain_cluster": node.domain_cluster,
            "pagerank_score": node.pagerank_score,
            "change_velocity_30d": node.change_velocity_30d,
            "exports": {
                "functions": node.exported_functions,
                "classes": node.exported_classes,
            },
            "dependencies": {
                "imports": [self._rel(p) for p in imports],
                "imported_by": [self._rel(p) for p in imported_by],
            },
        }