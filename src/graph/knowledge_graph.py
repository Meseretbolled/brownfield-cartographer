from __future__ import annotations
from pathlib import Path
from typing import Any
import networkx as nx

from src.models import (
    DataLineageGraph,
    DatasetNode,
    Edge,
    EdgeType,
    ModuleGraph,
    ModuleNode,
    TransformationNode,
)


class KnowledgeGraph:
    def __init__(self) -> None:
        self.module_graph: nx.DiGraph = nx.DiGraph()
        self.lineage_graph: nx.DiGraph = nx.DiGraph()
        self._module_nodes: dict[str, ModuleNode] = {}
        self._dataset_nodes: dict[str, DatasetNode] = {}
        self._transformation_nodes: dict[str, TransformationNode] = {}

    def add_module(self, node: ModuleNode) -> None:
        self._module_nodes[node.path] = node
        self.module_graph.add_node(node.path, **node.model_dump())

    def add_module_edge(self, source: str, target: str) -> None:
        if self.module_graph.has_edge(source, target):
            self.module_graph[source][target]["weight"] += 1
        else:
            self.module_graph.add_edge(source, target, weight=1, edge_type=EdgeType.IMPORTS)

    def add_dataset(self, node: DatasetNode) -> None:
        self._dataset_nodes[node.name] = node
        self.lineage_graph.add_node(node.name, node_type="dataset", **node.model_dump())

    def add_transformation(self, node: TransformationNode) -> None:
        self._transformation_nodes[node.id] = node
        self.lineage_graph.add_node(node.id, node_type="transformation", **node.model_dump())
        for src in node.source_datasets:
            self.lineage_graph.add_edge(src, node.id, edge_type=EdgeType.CONSUMES)
        for tgt in node.target_datasets:
            self.lineage_graph.add_edge(node.id, tgt, edge_type=EdgeType.PRODUCES)

    def compute_pagerank(self) -> dict[str, float]:
        if len(self.module_graph) == 0:
            return {}
        scores = nx.pagerank(self.module_graph, alpha=0.85)
        for path, score in scores.items():
            self.module_graph.nodes[path]["pagerank_score"] = score
            if path in self._module_nodes:
                self._module_nodes[path].pagerank_score = score
        return scores

    def find_circular_dependencies(self) -> list[list[str]]:
        return [sorted(c) for c in nx.strongly_connected_components(self.module_graph) if len(c) > 1]

    def blast_radius(self, node_id: str) -> list[str]:
        graph = self.lineage_graph if node_id in self.lineage_graph else self.module_graph
        if node_id not in graph:
            return []
        return list(nx.descendants(graph, node_id))

    def find_sources(self) -> list[str]:
        return [n for n, d in self.lineage_graph.in_degree() if d == 0]

    def find_sinks(self) -> list[str]:
        return [n for n, d in self.lineage_graph.out_degree() if d == 0]

    def get_architectural_hubs(self, top_n: int = 5) -> list[str]:
        if not self.module_graph:
            return []
        scores = nx.pagerank(self.module_graph, alpha=0.85)
        return sorted(scores, key=scores.get, reverse=True)[:top_n]

    def to_module_graph_schema(self, target_repo: str = "") -> ModuleGraph:
        edges = [
            Edge(source=u, target=v, edge_type=EdgeType.IMPORTS, weight=d.get("weight", 1.0))
            for u, v, d in self.module_graph.edges(data=True)
        ]
        return ModuleGraph(
            nodes=self._module_nodes,
            edges=edges,
            circular_dependencies=self.find_circular_dependencies(),
            high_velocity_files=sorted(
                self._module_nodes,
                key=lambda p: self._module_nodes[p].change_velocity_30d,
                reverse=True,
            )[:10],
            architectural_hubs=self.get_architectural_hubs(),
            target_repo=target_repo,
        )

    def to_lineage_graph_schema(self, target_repo: str = "") -> DataLineageGraph:
        edges = [
            Edge(
                source=u,
                target=v,
                edge_type=d.get("edge_type", EdgeType.PRODUCES),
                weight=d.get("weight", 1.0),
            )
            for u, v, d in self.lineage_graph.edges(data=True)
        ]
        return DataLineageGraph(
            dataset_nodes=self._dataset_nodes,
            transformation_nodes=self._transformation_nodes,
            edges=edges,
            sources=self.find_sources(),
            sinks=self.find_sinks(),
            target_repo=target_repo,
        )

    def serialize(self, output_dir: Path, target_repo: str = "") -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        module_graph = self.to_module_graph_schema(target_repo)
        lineage_graph = self.to_lineage_graph_schema(target_repo)
        (output_dir / "module_graph.json").write_text(module_graph.model_dump_json(indent=2), encoding="utf-8")
        (output_dir / "lineage_graph.json").write_text(lineage_graph.model_dump_json(indent=2), encoding="utf-8")

    def visualize_module_graph(self, output_path: Path) -> None:
        """
        Preferred output: PNG drawn with NetworkX + matplotlib.
        Fallback: HTML via pyvis.
        """
        if len(self.module_graph.nodes) == 0:
            return

        try:
            import matplotlib.pyplot as plt

            fig_w = 14
            fig_h = 10

            pos = nx.spring_layout(self.module_graph, seed=42, k=1.2)
            scores = nx.pagerank(self.module_graph, alpha=0.85) if self.module_graph.nodes else {}
            node_sizes = [
                500 + (scores.get(node, 0.0) * 12000)
                for node in self.module_graph.nodes
            ]
            labels = {node: Path(node).name for node in self.module_graph.nodes}

            plt.figure(figsize=(fig_w, fig_h))
            nx.draw_networkx_nodes(self.module_graph, pos, node_size=node_sizes)
            nx.draw_networkx_edges(self.module_graph, pos, arrows=True, alpha=0.5)
            nx.draw_networkx_labels(self.module_graph, pos, labels=labels, font_size=8)

            plt.title("Brownfield Cartographer - Module Import Graph")
            plt.axis("off")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            plt.tight_layout()
            plt.savefig(output_path, dpi=200, bbox_inches="tight")
            plt.close()
            return
        except Exception:
            pass

        if output_path.suffix.lower() != ".html":
            output_path = output_path.with_suffix(".html")

        from pyvis.network import Network

        net = Network(height="750px", width="100%", directed=True)
        for node, data in self.module_graph.nodes(data=True):
            size = 10 + data.get("pagerank_score", 0) * 500
            net.add_node(node, label=Path(node).name, title=node, size=size)
        for u, v, data in self.module_graph.edges(data=True):
            net.add_edge(u, v, value=data.get("weight", 1))
        net.save_graph(str(output_path))