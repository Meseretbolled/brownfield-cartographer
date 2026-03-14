from __future__ import annotations

from pathlib import Path

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
            if src not in self.lineage_graph:
                self.lineage_graph.add_node(src, node_type="dataset")
            self.lineage_graph.add_edge(src, node.id, edge_type=EdgeType.CONSUMES)
        for tgt in node.target_datasets:
            if tgt not in self.lineage_graph:
                self.lineage_graph.add_node(tgt, node_type="dataset")
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
        return [
            sorted(c)
            for c in nx.strongly_connected_components(self.module_graph)
            if len(c) > 1
        ]

    def blast_radius(self, node_id: str) -> list[str]:
        if node_id in self.lineage_graph:
            return list(nx.descendants(self.lineage_graph, node_id))
        if node_id in self.module_graph:
            return list(nx.descendants(self.module_graph, node_id))
        for nid in list(self.lineage_graph.nodes) + list(self.module_graph.nodes):
            if node_id in nid or Path(nid).name == node_id or Path(nid).stem == node_id:
                graph = self.lineage_graph if nid in self.lineage_graph else self.module_graph
                return list(nx.descendants(graph, nid))
        return []

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
                source=u, target=v,
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
        mg = self.to_module_graph_schema(target_repo)
        lg = self.to_lineage_graph_schema(target_repo)
        (output_dir / "module_graph.json").write_text(mg.model_dump_json(indent=2), encoding="utf-8")
        (output_dir / "lineage_graph.json").write_text(lg.model_dump_json(indent=2), encoding="utf-8")

    def visualize_module_graph(self, output_path: Path) -> None:
        if not self.module_graph.nodes:
            return

        output_path = output_path.with_suffix(".html")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        from pyvis.network import Network

        net = Network(height="800px", width="100%", directed=True, bgcolor="#0e1117", font_color="#e0e0e0")
        net.set_options("""
        {
          "physics": {
            "barnesHut": {
              "gravitationalConstant": -8000,
              "centralGravity": 0.3,
              "springLength": 120,
              "springConstant": 0.04,
              "damping": 0.09
            },
            "minVelocity": 0.75
          },
          "nodes": {
            "shape": "dot",
            "font": { "size": 11, "color": "#e0e0e0" }
          },
          "edges": {
            "arrows": { "to": { "enabled": true, "scaleFactor": 0.5 } },
            "color": { "color": "rgba(140,150,170,0.3)" },
            "smooth": { "type": "continuous" }
          }
        }
        """)

        scores = nx.pagerank(self.module_graph, alpha=0.85) if self.module_graph.nodes else {}

        domain_colors = [
            "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
            "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
        ]
        domain_cache: dict[str, str] = {}

        for node_id, data in self.module_graph.nodes(data=True):
            score = scores.get(node_id, 0.0)
            size = max(8, min(50, 10 + score * 12000))
            label = Path(node_id).name
            domain = data.get("domain_cluster") or "unclassified"
            if domain not in domain_cache:
                domain_cache[domain] = domain_colors[len(domain_cache) % len(domain_colors)]
            color = domain_cache[domain]
            title = (
                f"<b>{label}</b><br>"
                f"Domain: {domain}<br>"
                f"PageRank: {score:.6f}<br>"
                f"Velocity 30d: {data.get('change_velocity_30d', 0)}<br>"
                f"LOC: {data.get('loc', 0)}<br>"
                f"Language: {data.get('language', '')}"
            )
            net.add_node(node_id, label=label, title=title, size=size, color=color)

        for u, v, data in self.module_graph.edges(data=True):
            net.add_edge(u, v, value=data.get("weight", 1))

        net.save_graph(str(output_path))