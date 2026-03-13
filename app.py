from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.agents.navigator import Navigator
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import DataLineageGraph, ModuleGraph
from src.orchestrator import Orchestrator


# -----------------------------------------------------------------------------
# Page setup
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Brownfield Cartographer Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Brownfield Cartographer Dashboard")
st.caption("Surveyor • Hydrologist • Semanticist • Archivist • Navigator")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _is_git_url(value: str) -> bool:
    value = value.strip()
    return value.startswith("http://") or value.startswith("https://") or value.startswith("git@")


def _clone_repo(url: str) -> tuple[Path, bool]:
    tmp_dir = Path(tempfile.mkdtemp(prefix="cartographer_repo_"))
    clone_dir = tmp_dir / "repo"

    result = subprocess.run(
        ["git", "clone", "--depth=1", url, str(clone_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git clone failed")

    return clone_dir, True


def _resolve_repo(repo_input: str) -> tuple[Path, bool]:
    repo_input = repo_input.strip()
    if not repo_input:
        raise ValueError("Repository input is empty")

    if _is_git_url(repo_input):
        return _clone_repo(repo_input)

    local_path = Path(repo_input).expanduser().resolve()
    if not local_path.exists():
        raise FileNotFoundError(f"Path does not exist: {local_path}")
    return local_path, False


def _cartography_dir(repo_path: Path) -> Path:
    return repo_path / ".cartography"


def _load_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_module_graph(path: Path) -> ModuleGraph | None:
    if not path.exists():
        return None
    try:
        return ModuleGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e:
        st.warning(f"Could not load module graph: {e}")
        return None


def _load_lineage_graph(path: Path) -> DataLineageGraph | None:
    if not path.exists():
        return None
    try:
        return DataLineageGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e:
        st.warning(f"Could not load lineage graph: {e}")
        return None


def _rebuild_kg(
    module_graph: ModuleGraph | None,
    lineage_graph: DataLineageGraph | None,
) -> KnowledgeGraph:
    kg = KnowledgeGraph()

    if module_graph:
        for node in module_graph.nodes.values():
            kg.add_module(node)
        for edge in module_graph.edges:
            kg.add_module_edge(edge.source, edge.target)

    if lineage_graph:
        for node in lineage_graph.dataset_nodes.values():
            kg.add_dataset(node)
        for node in lineage_graph.transformation_nodes.values():
            kg.add_transformation(node)

    return kg


def _safe_rel(path: str, repo_path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except Exception:
        return path


def _cleanup_temp_repo(repo_path: Path, is_temp: bool) -> None:
    if is_temp:
        shutil.rmtree(repo_path.parent, ignore_errors=True)


def _find_repo_label(repo_input: str, repo_path: Path) -> str:
    if _is_git_url(repo_input):
        return repo_input.rstrip("/").split("/")[-2] + "/" + repo_input.rstrip("/").split("/")[-1].replace(".git", "")
    return repo_path.name


def _build_module_network_figure(
    module_graph: ModuleGraph,
    repo_path: Path,
    max_nodes: int = 300,
) -> go.Figure:
    G = nx.DiGraph()

    ranked_nodes = sorted(
        module_graph.nodes.values(),
        key=lambda n: n.pagerank_score,
        reverse=True,
    )

    selected_nodes = ranked_nodes[:max_nodes]
    selected_ids = {n.path for n in selected_nodes}

    for node in selected_nodes:
        G.add_node(
            node.path,
            label=_safe_rel(node.path, repo_path),
            pagerank=node.pagerank_score,
            velocity=node.change_velocity_30d,
            purpose=node.purpose_statement or "",
            domain=node.domain_cluster or "unclassified",
            loc=node.loc,
            language=str(node.language),
        )

    for edge in module_graph.edges:
        if edge.source in selected_ids and edge.target in selected_ids:
            G.add_edge(edge.source, edge.target, weight=edge.weight)

    if G.number_of_nodes() == 0:
        return go.Figure()

    pos = nx.spring_layout(G, seed=42, k=1.1)

    edge_x = []
    edge_y = []
    for source, target in G.edges():
        x0, y0 = pos[source]
        x1, y1 = pos[target]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode="lines",
        line=dict(width=0.7, color="#9aa4b2"),
        hoverinfo="none",
    )

    node_x = []
    node_y = []
    node_text = []
    node_size = []
    node_color = []

    domain_palette = {}
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]

    def color_for_domain(domain: str) -> str:
        if domain not in domain_palette:
            domain_palette[domain] = palette[len(domain_palette) % len(palette)]
        return domain_palette[domain]

    for node_id, attrs in G.nodes(data=True):
        x, y = pos[node_id]
        node_x.append(x)
        node_y.append(y)

        pagerank = float(attrs.get("pagerank", 0.0) or 0.0)
        velocity = int(attrs.get("velocity", 0) or 0)
        size = 12 + pagerank * 12000
        size = max(10, min(size, 60))
        node_size.append(size)

        domain = attrs.get("domain", "unclassified")
        node_color.append(color_for_domain(domain))

        label = Path(node_id).name
        hover = (
            f"<b>{label}</b><br>"
            f"path: {attrs.get('label', '')}<br>"
            f"domain: {domain}<br>"
            f"pagerank: {pagerank:.5f}<br>"
            f"velocity_30d: {velocity}<br>"
            f"loc: {attrs.get('loc', 0)}<br>"
            f"language: {attrs.get('language', '')}<br>"
            f"purpose: {attrs.get('purpose', '')[:300]}"
        )
        node_text.append(hover)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        hoverinfo="text",
        text=node_text,
        marker=dict(
            size=node_size,
            color=node_color,
            line=dict(width=0.5, color="#ffffff"),
            opacity=0.88,
        ),
    )

    fig = go.Figure(data=[edge_trace, node_trace])
    fig.update_layout(
        title="Module Dependency Graph (NetworkX layout)",
        showlegend=False,
        hovermode="closest",
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=780,
    )
    return fig


def _module_metrics_df(module_graph: ModuleGraph, repo_path: Path) -> pd.DataFrame:
    rows = []
    for node in module_graph.nodes.values():
        rows.append(
            {
                "module": _safe_rel(node.path, repo_path),
                "language": str(node.language),
                "loc": node.loc,
                "pagerank": round(node.pagerank_score, 6),
                "velocity_30d": node.change_velocity_30d,
                "domain_cluster": node.domain_cluster or "",
                "purpose_statement": node.purpose_statement or "",
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["pagerank", "velocity_30d"], ascending=[False, False])
    return df


def _dataset_df(lineage_graph: DataLineageGraph) -> pd.DataFrame:
    rows = []
    for ds in lineage_graph.dataset_nodes.values():
        rows.append(
            {
                "dataset": ds.name,
                "kind": getattr(ds, "kind", ""),
                "location": getattr(ds, "location", ""),
                "format": getattr(ds, "format", ""),
                "owner": getattr(ds, "owner", ""),
            }
        )
    return pd.DataFrame(rows)


def _show_overview(module_graph: ModuleGraph | None, lineage_graph: DataLineageGraph | None) -> None:
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    c1.metric("Modules", len(module_graph.nodes) if module_graph else 0)
    c2.metric("Module edges", len(module_graph.edges) if module_graph else 0)
    c3.metric("Datasets", len(lineage_graph.dataset_nodes) if lineage_graph else 0)
    c4.metric("Transforms", len(lineage_graph.transformation_nodes) if lineage_graph else 0)
    c5.metric("Sources", len(lineage_graph.sources) if lineage_graph else 0)
    c6.metric("Sinks", len(lineage_graph.sinks) if lineage_graph else 0)


def _load_trace_rows(trace_path: Path) -> list[dict[str, Any]]:
    rows = []
    if not trace_path.exists():
        return rows

    for line in trace_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _render_navigator(navigator: Navigator) -> None:
    st.subheader("Navigator")

    nav_mode = st.selectbox(
        "Choose a query tool",
        [
            "find_implementation",
            "trace_lineage",
            "blast_radius",
            "explain_module",
        ],
    )

    if nav_mode == "find_implementation":
        concept = st.text_input("Concept", value="staging customers")
        if st.button("Run find_implementation"):
            result = navigator.find_implementation(concept)
            st.json(result)

    elif nav_mode == "trace_lineage":
        dataset = st.text_input("Dataset name", value="customers")
        direction = st.selectbox("Direction", ["upstream", "downstream", "both"])
        if st.button("Run trace_lineage"):
            result = navigator.trace_lineage(dataset, direction)
            st.json(result)

    elif nav_mode == "blast_radius":
        module_path = st.text_input("Module path", value="models/staging/stg_customers.sql")
        if st.button("Run blast_radius"):
            result = navigator.blast_radius(module_path)
            st.json(result)

    elif nav_mode == "explain_module":
        module_path = st.text_input("Module path", value="models/staging/stg_customers.sql")
        if st.button("Run explain_module"):
            result = navigator.explain_module(module_path)
            st.json(result)


# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------

with st.sidebar:
    st.header("Repository")

    repo_input = st.text_input(
        "Local path or GitHub URL",
        value=st.session_state.get("repo_input", "https://github.com/dbt-labs/jaffle_shop"),
    )
    st.session_state["repo_input"] = repo_input

    run_incremental = st.checkbox("Incremental mode", value=False)
    max_nodes = st.slider("Max graph nodes", min_value=50, max_value=800, value=300, step=50)

    analyze_clicked = st.button("Analyze / Refresh", use_container_width=True)

    st.divider()
    st.caption("Recommended test repos")
    st.code("https://github.com/dbt-labs/jaffle_shop", language="text")
    st.code("https://github.com/mitodl/ol-data-platform", language="text")


# -----------------------------------------------------------------------------
# Main app flow
# -----------------------------------------------------------------------------

if "repo_path" not in st.session_state:
    st.session_state["repo_path"] = None
if "repo_is_temp" not in st.session_state:
    st.session_state["repo_is_temp"] = False
if "analysis_ok" not in st.session_state:
    st.session_state["analysis_ok"] = False
if "last_error" not in st.session_state:
    st.session_state["last_error"] = ""


if analyze_clicked:
    try:
        if st.session_state.get("repo_path") and st.session_state.get("repo_is_temp"):
            try:
                _cleanup_temp_repo(Path(st.session_state["repo_path"]), True)
            except Exception:
                pass

        with st.spinner("Resolving repository..."):
            repo_path, is_temp = _resolve_repo(repo_input)

        with st.spinner("Running Brownfield Cartographer analysis..."):
            orchestrator = Orchestrator(
                repo_path=repo_path,
                incremental=run_incremental,
            )
            result = orchestrator.run()

        st.session_state["repo_path"] = str(repo_path)
        st.session_state["repo_is_temp"] = is_temp
        st.session_state["analysis_ok"] = True
        st.session_state["last_error"] = ""

        st.success(
            f"Analysis completed for {repo_path.name} "
            f"({result.analysis_duration_seconds:.1f}s)"
        )

    except Exception as e:
        st.session_state["analysis_ok"] = False
        st.session_state["last_error"] = str(e)
        st.error(f"Analysis failed: {e}")


repo_path_str = st.session_state.get("repo_path")
analysis_ok = st.session_state.get("analysis_ok", False)

if not repo_path_str:
    st.info("Enter a local repo path or GitHub URL, then click Analyze / Refresh.")
    st.stop()

repo_path = Path(repo_path_str)
cart_dir = _cartography_dir(repo_path)

if not cart_dir.exists():
    st.warning(f"No .cartography directory found at: {cart_dir}")
    st.stop()

module_graph = _load_module_graph(cart_dir / "module_graph.json")
lineage_graph = _load_lineage_graph(cart_dir / "lineage_graph.json")
kg = _rebuild_kg(module_graph, lineage_graph)

navigator = Navigator(
    repo_path=repo_path,
    module_graph=module_graph or ModuleGraph(target_repo=str(repo_path)),
    lineage_graph=lineage_graph or DataLineageGraph(target_repo=str(repo_path)),
    kg=kg,
)

repo_label = _find_repo_label(repo_input, repo_path)

# -----------------------------------------------------------------------------
# Top dashboard header
# -----------------------------------------------------------------------------

st.subheader(repo_label)

header_c1, header_c2, header_c3, header_c4 = st.columns([2, 1, 1, 1])
with header_c1:
    st.write(f"**Repository:** `{repo_label}`")
    st.write(f"**Location:** `{repo_path}`")
with header_c2:
    st.write(f"**Branch:** `main/unknown`")
with header_c3:
    artifact_count = len(list(cart_dir.glob("*"))) if cart_dir.exists() else 0
    st.write(f"**Artifacts:** `{artifact_count}`")
with header_c4:
    st.write(f"**Status:** {'Loaded' if analysis_ok or cart_dir.exists() else 'Not loaded'}")

_show_overview(module_graph, lineage_graph)

tabs = st.tabs(
    [
        "Overview",
        "Surveyor",
        "Hydrologist",
        "Semanticist",
        "Archivist",
        "Navigator",
        "Raw Artifacts",
    ]
)

# -----------------------------------------------------------------------------
# Overview
# -----------------------------------------------------------------------------

with tabs[0]:
    st.markdown("### Repository Overview")

    summary_path = cart_dir / "analysis_summary.md"
    if summary_path.exists():
        st.markdown(_load_text(summary_path))
    else:
        st.info("No analysis summary found yet.")

    if module_graph:
        st.markdown("### Top Architectural Hubs")
        hub_rows = []
        for hub in module_graph.architectural_hubs[:10]:
            node = module_graph.nodes.get(hub)
            if node:
                hub_rows.append(
                    {
                        "module": _safe_rel(hub, repo_path),
                        "pagerank": round(node.pagerank_score, 6),
                        "velocity_30d": node.change_velocity_30d,
                        "domain_cluster": node.domain_cluster or "",
                    }
                )
        if hub_rows:
            st.dataframe(pd.DataFrame(hub_rows), use_container_width=True)

    if lineage_graph:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### Sources")
            st.dataframe(pd.DataFrame({"source": sorted(lineage_graph.sources)}), use_container_width=True)
        with c2:
            st.markdown("### Sinks")
            st.dataframe(pd.DataFrame({"sink": sorted(lineage_graph.sinks)}), use_container_width=True)

# -----------------------------------------------------------------------------
# Surveyor
# -----------------------------------------------------------------------------

with tabs[1]:
    st.markdown("### Surveyor")
    st.caption("Module topology, centrality, dependency structure, hotspots.")

    if module_graph is None:
        st.warning("No module graph loaded.")
    else:
        fig = _build_module_network_figure(module_graph, repo_path, max_nodes=max_nodes)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Module Metrics")
        df = _module_metrics_df(module_graph, repo_path)
        st.dataframe(df, use_container_width=True, height=500)

        if module_graph.circular_dependencies:
            st.markdown("### Circular Dependencies")
            for cycle in module_graph.circular_dependencies[:20]:
                st.code("  <->  ".join(_safe_rel(p, repo_path) for p in cycle), language="text")

# -----------------------------------------------------------------------------
# Hydrologist
# -----------------------------------------------------------------------------

with tabs[2]:
    st.markdown("### Hydrologist")
    st.caption("Data lineage across SQL, YAML, transformations, and datasets.")

    if lineage_graph is None:
        st.warning("No lineage graph loaded.")
    else:
        st.markdown("### Dataset Inventory")
        ds_df = _dataset_df(lineage_graph)
        st.dataframe(ds_df, use_container_width=True, height=400)

        st.markdown("### Source and Sink Summary")
        c1, c2 = st.columns(2)
        with c1:
            st.write("**Sources**")
            st.dataframe(pd.DataFrame({"dataset": sorted(lineage_graph.sources)}), use_container_width=True)
        with c2:
            st.write("**Sinks**")
            st.dataframe(pd.DataFrame({"dataset": sorted(lineage_graph.sinks)}), use_container_width=True)

        st.markdown("### Transformation Count")
        st.metric("Transformations", len(lineage_graph.transformation_nodes))

# -----------------------------------------------------------------------------
# Semanticist
# -----------------------------------------------------------------------------

with tabs[3]:
    st.markdown("### Semanticist")
    st.caption("Purpose statements, domain clusters, documentation drift signals.")

    if module_graph is None:
        st.warning("No module graph loaded.")
    else:
        search_term = st.text_input("Search module purpose or concept", value="")
        semantic_rows = []

        for node in module_graph.nodes.values():
            row = {
                "module": _safe_rel(node.path, repo_path),
                "purpose_statement": node.purpose_statement or "",
                "domain_cluster": node.domain_cluster or "",
                "pagerank": round(node.pagerank_score, 6),
                "velocity_30d": node.change_velocity_30d,
            }
            semantic_rows.append(row)

        sem_df = pd.DataFrame(semantic_rows)

        if search_term.strip():
            q = search_term.lower()
            sem_df = sem_df[
                sem_df["module"].str.lower().str.contains(q, na=False)
                | sem_df["purpose_statement"].str.lower().str.contains(q, na=False)
                | sem_df["domain_cluster"].str.lower().str.contains(q, na=False)
            ]

        st.dataframe(sem_df, use_container_width=True, height=550)

        if not sem_df.empty and "domain_cluster" in sem_df.columns:
            st.markdown("### Domain Clusters")
            cluster_counts = (
                sem_df["domain_cluster"]
                .fillna("")
                .replace("", "unclassified")
                .value_counts()
                .reset_index()
            )
            cluster_counts.columns = ["domain_cluster", "count"]
            st.dataframe(cluster_counts, use_container_width=True)

# -----------------------------------------------------------------------------
# Archivist
# -----------------------------------------------------------------------------

with tabs[4]:
    st.markdown("### Archivist")
    st.caption("Living artifacts for onboarding and context injection.")

    codebase_path = cart_dir / "CODEBASE.md"
    onboarding_path = cart_dir / "onboarding_brief.md"
    trace_path = cart_dir / "cartography_trace.jsonl"

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("#### CODEBASE.md")
        if codebase_path.exists():
            st.markdown(_load_text(codebase_path))
        else:
            st.info("CODEBASE.md not found")

    with c2:
        st.markdown("#### onboarding_brief.md")
        if onboarding_path.exists():
            st.markdown(_load_text(onboarding_path))
        else:
            st.info("onboarding_brief.md not found")

    st.markdown("#### cartography_trace.jsonl")
    trace_rows = _load_trace_rows(trace_path)
    if trace_rows:
        st.dataframe(pd.DataFrame(trace_rows), use_container_width=True, height=300)
    else:
        st.info("cartography_trace.jsonl not found or empty")

# -----------------------------------------------------------------------------
# Navigator
# -----------------------------------------------------------------------------

with tabs[5]:
    _render_navigator(navigator)

# -----------------------------------------------------------------------------
# Raw Artifacts
# -----------------------------------------------------------------------------

with tabs[6]:
    st.markdown("### Raw Artifact Files")

    artifact_files = [
        "module_graph.json",
        "lineage_graph.json",
        "analysis_summary.md",
        "CODEBASE.md",
        "onboarding_brief.md",
        "cartography_trace.jsonl",
    ]

    selected_artifact = st.selectbox("Choose artifact", artifact_files)

    artifact_path = cart_dir / selected_artifact
    if artifact_path.exists():
        if artifact_path.suffix == ".json":
            data = _load_json(artifact_path)
            st.json(data)
        else:
            st.code(_load_text(artifact_path), language="markdown")
    else:
        st.info(f"{selected_artifact} not found")