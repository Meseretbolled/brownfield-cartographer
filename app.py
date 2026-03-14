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

GIT_DAYS = int(os.getenv("CARTOGRAPHER_GIT_DAYS", "30"))

st.set_page_config(
    page_title="Brownfield Cartographer",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.block-container{padding-top:0.8rem;padding-bottom:1rem}
[data-testid="stMetricValue"]{font-size:1.9rem;font-weight:800;color:#e0e0e0}
[data-testid="stMetricLabel"]{font-size:0.72rem;color:#9aa4b2;text-transform:uppercase;letter-spacing:.05em}
[data-testid="stSidebar"]{background:#0a0a14;border-right:1px solid #1e1e2e}
[data-testid="stTabs"] button{font-size:.82rem;font-weight:600;letter-spacing:.03em}
[data-testid="stTabs"] button[aria-selected="true"]{color:#ff4b4b;border-bottom-color:#ff4b4b}
[data-testid="stButton"]>button[kind="primary"]{background:#ff4b4b;border:none;font-weight:700;letter-spacing:.05em}
code{background:#12121f;color:#7ec8e3;border-radius:3px;padding:1px 5px}
</style>
""", unsafe_allow_html=True)

BASE_DIR = Path.home() / "Desktop" / "brownfield-cartographer"

QUICK_REPOS = {
    "— select a repo —": "",
    "jaffle_shop  (dbt + SQL)": str(BASE_DIR / "jaffle_shop"),
    "ol-data-platform  (Dagster)": str(BASE_DIR / "ol-data-platform"),
    "data-engineering-zoomcamp": str(BASE_DIR / "data-engineering-zoomcamp"),
}

DOMAIN_PALETTE = [
    "#4e79a7","#f28e2b","#e15759","#76b7b2","#59a14f",
    "#edc948","#b07aa1","#ff9da7","#9c755f","#bab0ac",
]

TRANSFORM_COLORS = {
    "dbt_model":"#4e79a7","sql_select":"#76b7b2",
    "pandas_read":"#59a14f","pandas_write":"#f28e2b",
    "pyspark_read":"#edc948","pyspark_write":"#e15759",
    "airflow_operator":"#b07aa1","unknown":"#9aa4b2",
}


def _is_git_url(v: str) -> bool:
    return v.strip().startswith(("http://","https://","git@"))

def _clone_repo(url: str) -> tuple[Path, bool]:
    tmp = Path(tempfile.mkdtemp(prefix="cartographer_"))
    dest = tmp / "repo"
    r = subprocess.run(["git","clone","--depth=1",url,str(dest)],capture_output=True,text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "git clone failed")
    return dest, True

def _resolve_repo(repo_input: str) -> tuple[Path, bool]:
    repo_input = repo_input.strip()
    if not repo_input:
        raise ValueError("No repository path entered")
    if _is_git_url(repo_input):
        return _clone_repo(repo_input)
    p = Path(repo_input).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"Path not found: {p}")
    return p, False

def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""

def _load_json(path: Path) -> Any:
    if not path.exists(): return None
    try: return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return None

def _load_module_graph(path: Path) -> ModuleGraph | None:
    if not path.exists(): return None
    try: return ModuleGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e: st.warning(f"module_graph error: {e}"); return None

def _load_lineage_graph(path: Path) -> DataLineageGraph | None:
    if not path.exists(): return None
    try: return DataLineageGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e: st.warning(f"lineage_graph error: {e}"); return None

def _rebuild_kg(mg: ModuleGraph | None, lg: DataLineageGraph | None) -> KnowledgeGraph:
    kg = KnowledgeGraph()
    if mg:
        for n in mg.nodes.values(): kg.add_module(n)
        for e in mg.edges: kg.add_module_edge(e.source, e.target)
    if lg:
        for n in lg.dataset_nodes.values(): kg.add_dataset(n)
        for n in lg.transformation_nodes.values(): kg.add_transformation(n)
    return kg

def _safe_rel(path: str, repo_path: Path) -> str:
    try: return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except Exception: return Path(path).name

def _repo_label(repo_input: str, repo_path: Path) -> str:
    if _is_git_url(repo_input):
        parts = repo_input.rstrip("/").split("/")
        return f"{parts[-2]}/{parts[-1].replace('.git','')}"
    return repo_path.name

def _color_domain(domain: str, cache: dict[str, str]) -> str:
    if domain not in cache:
        cache[domain] = DOMAIN_PALETTE[len(cache) % len(DOMAIN_PALETTE)]
    return cache[domain]

def _load_trace_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists(): return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try: rows.append(json.loads(line))
            except Exception: pass
    return rows


def _build_module_figure(mg, repo_path, max_nodes=300, domain_filter="All", highlight=None, layout="kamada_kawai"):
    nodes = sorted(mg.nodes.values(), key=lambda n: n.pagerank_score, reverse=True)
    if domain_filter != "All":
        nodes = [n for n in nodes if (n.domain_cluster or "unclassified") == domain_filter]
    nodes = nodes[:max_nodes]
    ids = {n.path for n in nodes}

    G = nx.DiGraph()
    for n in nodes:
        G.add_node(n.path, label=_safe_rel(n.path, repo_path),
                   pagerank=n.pagerank_score, velocity=n.change_velocity_30d,
                   purpose=n.purpose_statement or "", domain=n.domain_cluster or "unclassified",
                   loc=n.loc, language=str(n.language), dead=n.is_dead_code_candidate)
    for e in mg.edges:
        if e.source in ids and e.target in ids:
            G.add_edge(e.source, e.target, weight=e.weight)

    if not G.nodes:
        fig = go.Figure()
        fig.update_layout(title="No modules", height=600, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117")
        return fig

    layout_fns = {
        "kamada_kawai": nx.kamada_kawai_layout,
        "circular": nx.circular_layout,
        "shell": nx.shell_layout,
    }
    try:
        pos = layout_fns.get(layout, nx.spring_layout)(G)
    except Exception:
        pos = nx.spring_layout(G, seed=42)

    domain_cache: dict[str, str] = {}
    traces: list[Any] = []

    ex, ey = [], []
    for s, t in G.edges():
        x0,y0=pos[s]; x1,y1=pos[t]
        ex+=[x0,x1,None]; ey+=[y0,y1,None]
    traces.append(go.Scatter(x=ex, y=ey, mode="lines",
        line=dict(width=0.4, color="rgba(140,150,170,0.2)"),
        hoverinfo="none", showlegend=False))

    nx_, ny_, nt_, ns_, nc_, nb_, nlbl = [], [], [], [], [], [], []
    for nid, a in G.nodes(data=True):
        x, y = pos[nid]; nx_.append(x); ny_.append(y)
        pr = float(a.get("pagerank") or 0)
        sz = max(10, min(65, 12 + pr*18000)); ns_.append(sz)
        dom = a.get("domain", "unclassified")
        col = _color_domain(dom, domain_cache)
        if highlight and nid in highlight:
            nc_.append("#ff4b4b"); nb_.append("#ffffff")
        elif a.get("dead"):
            nc_.append("rgba(70,70,70,0.3)"); nb_.append("rgba(70,70,70,0.3)")
        else:
            nc_.append(col); nb_.append("rgba(255,255,255,0.35)")
        nlbl.append(Path(nid).name if pr > 0.001 else "")
        nt_.append(
            f"<b>{Path(nid).name}</b><br><i>{a.get('label','')}</i><br><br>"
            f"<b>Domain:</b> {dom}<br><b>Language:</b> {a.get('language','')}<br>"
            f"<b>PageRank:</b> {pr:.6f}<br>"
            f"<b>Velocity ({GIT_DAYS}d):</b> {a.get('velocity',0)}<br>"
            f"<b>LOC:</b> {a.get('loc',0)}<br><b>Dead code?</b> {a.get('dead',False)}<br><br>"
            f"<b>Purpose:</b><br>{(a.get('purpose','') or '')[:280]}")
    traces.append(go.Scatter(x=nx_, y=ny_, mode="markers+text",
        hoverinfo="text", hovertext=nt_,
        text=nlbl, textposition="top center",
        textfont=dict(size=8, color="rgba(210,210,210,.85)"),
        marker=dict(size=ns_, color=nc_, line=dict(width=1.0, color=nb_), opacity=0.92),
        showlegend=False))
    for dom, col in domain_cache.items():
        traces.append(go.Scatter(x=[None], y=[None], mode="markers",
            marker=dict(size=10, color=col), name=dom, showlegend=True))

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=dict(
            text=f"<b>Module Dependency Graph</b>  ·  {G.number_of_nodes()} nodes  ·  {G.number_of_edges()} edges",
            font=dict(size=14, color="#e0e0e0")),
        showlegend=True,
        legend=dict(orientation="v", x=1.01, y=1,
                    bgcolor="rgba(10,10,20,.9)", bordercolor="rgba(100,100,100,.2)", borderwidth=1,
                    font=dict(color="#c0c0c0", size=10),
                    title=dict(text="Domain", font=dict(color="#9aa4b2", size=10))),
        hovermode="closest", margin=dict(l=10, r=200, t=55, b=10),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=720, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117")
    return fig


def _build_lineage_figure(lg, highlight=None):
    G = nx.DiGraph()
    for name in lg.dataset_nodes:
        G.add_node(name, node_type="dataset")
    for tid, t in lg.transformation_nodes.items():
        ttype = str(t.transformation_type).replace("TransformationType.", "")
        G.add_node(tid, node_type="transformation", ttype=ttype, source_file=t.source_file)
    for e in lg.edges:
        if e.source not in G: G.add_node(e.source, node_type="dataset")
        if e.target not in G: G.add_node(e.target, node_type="dataset")
        G.add_edge(e.source, e.target)

    if not G.nodes:
        fig = go.Figure()
        fig.update_layout(title="No lineage data", height=500, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117")
        return fig

    try: pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    except Exception:
        try: pos = nx.planar_layout(G)
        except Exception: pos = nx.spring_layout(G, seed=7, k=3.0/max(1,len(G)**.5))

    traces: list[Any] = []
    ex, ey = [], []
    for s, t in G.edges():
        x0,y0=pos[s]; x1,y1=pos[t]; ex+=[x0,x1,None]; ey+=[y0,y1,None]
    traces.append(go.Scatter(x=ex, y=ey, mode="lines",
        line=dict(width=0.8, color="rgba(140,160,190,.3)"),
        hoverinfo="none", showlegend=False))

    ds_x,ds_y,ds_t,ds_lbl = [],[],[],[]
    tr_x,tr_y,tr_t,tr_lbl,tr_c = [],[],[],[],[]
    for nid, a in G.nodes(data=True):
        x, y = pos[nid]
        ntype = a.get("node_type", "dataset")
        label = nid if len(nid) <= 26 else nid[-26:]
        is_hl = highlight and nid in highlight
        if ntype == "dataset":
            ds_x.append(x); ds_y.append(y); ds_lbl.append(label)
            tip = (f"<b>{nid}</b><br>dataset<br>"
                   f"Source: {nid in lg.sources}<br>Sink: {nid in lg.sinks}")
            if is_hl: tip += "<br><b style='color:#ff4b4b'>⚠ BLAST RADIUS</b>"
            ds_t.append(tip)
        else:
            ttype = a.get("ttype", "unknown")
            col = "#ff4b4b" if is_hl else TRANSFORM_COLORS.get(ttype, "#9aa4b2")
            tr_x.append(x); tr_y.append(y); tr_lbl.append(label[:22]); tr_c.append(col)
            sf = Path(a.get("source_file", "") or "").name
            tr_t.append(f"<b>{nid}</b><br>{ttype}<br>{sf}")

    if ds_x:
        traces.append(go.Scatter(x=ds_x, y=ds_y, mode="markers+text",
            hoverinfo="text", hovertext=ds_t, text=ds_lbl, textposition="top center",
            textfont=dict(size=8, color="rgba(180,210,255,.85)"),
            marker=dict(symbol="circle", size=16, color="rgba(78,121,167,.85)",
                        line=dict(width=1.5, color="#7ec8e3")),
            name="dataset", showlegend=True))
    if tr_x:
        traces.append(go.Scatter(x=tr_x, y=tr_y, mode="markers+text",
            hoverinfo="text", hovertext=tr_t, text=tr_lbl, textposition="top center",
            textfont=dict(size=7, color="rgba(255,220,160,.85)"),
            marker=dict(symbol="diamond", size=13, color=tr_c,
                        line=dict(width=1.0, color="rgba(255,255,255,.3)")),
            name="transformation", showlegend=True))

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=dict(
            text=f"<b>Data Lineage Graph</b>  ·  {len(lg.dataset_nodes)} datasets  ·  {len(lg.transformation_nodes)} transforms",
            font=dict(size=14, color="#e0e0e0")),
        showlegend=True,
        legend=dict(bgcolor="rgba(10,10,20,.9)", bordercolor="rgba(100,100,100,.2)",
                    borderwidth=1, font=dict(color="#c0c0c0", size=11)),
        hovermode="closest", margin=dict(l=10, r=10, t=55, b=10),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=680, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117")
    return fig


def _build_velocity_figure(mg, repo_path, top_n=20):
    rows = [{"module": _safe_rel(n.path, repo_path), "velocity": n.change_velocity_30d}
            for n in mg.nodes.values() if n.change_velocity_30d > 0]
    if not rows: return go.Figure()
    df = pd.DataFrame(rows).sort_values("velocity", ascending=True).tail(top_n)
    fig = go.Figure(go.Bar(x=df["velocity"], y=df["module"], orientation="h",
        marker=dict(color=df["velocity"], colorscale="Reds", showscale=False),
        text=df["velocity"], textposition="outside",
        hovertemplate="<b>%{y}</b><br>Commits: %{x}<extra></extra>"))
    fig.update_layout(
        title=dict(text=f"<b>Git Velocity</b> — Top Changed Files ({GIT_DAYS}d)",
                   font=dict(size=13, color="#e0e0e0")),
        xaxis=dict(title="Commits", color="#9aa4b2", gridcolor="#1e1e2e"),
        yaxis=dict(color="#9aa4b2", tickfont=dict(size=9)),
        margin=dict(l=10, r=50, t=45, b=30),
        height=max(300, top_n*26),
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117", font=dict(color="#e0e0e0"))
    return fig


def _build_domain_pie(mg):
    counts: dict[str, int] = {}
    for n in mg.nodes.values():
        d = n.domain_cluster or "unclassified"
        counts[d] = counts.get(d, 0) + 1
    if not counts: return go.Figure()
    labels, values = list(counts.keys()), list(counts.values())
    fig = go.Figure(go.Pie(labels=labels, values=values, hole=0.5,
        marker=dict(colors=DOMAIN_PALETTE[:len(labels)]),
        textfont=dict(size=10),
        hovertemplate="<b>%{label}</b><br>%{value} modules (%{percent})<extra></extra>"))
    fig.update_layout(
        title=dict(text="<b>Domain Distribution</b>", font=dict(size=13, color="#e0e0e0")),
        margin=dict(l=10, r=10, t=45, b=10), height=360, paper_bgcolor="#0e1117",
        legend=dict(font=dict(color="#c0c0c0", size=10)))
    return fig


def _build_complexity_scatter(mg, repo_path):
    rows = [{"module": _safe_rel(n.path, repo_path), "loc": n.loc,
             "complexity": n.complexity_score, "pagerank": n.pagerank_score,
             "domain": n.domain_cluster or "unclassified"}
            for n in mg.nodes.values() if n.loc > 0]
    if not rows: return go.Figure()
    df = pd.DataFrame(rows)
    cache: dict[str, str] = {}
    colors = [_color_domain(d, cache) for d in df["domain"]]
    fig = go.Figure(go.Scatter(x=df["loc"], y=df["complexity"], mode="markers",
        marker=dict(size=8+df["pagerank"]*5000, color=colors, opacity=0.8,
                    line=dict(width=0.5, color="rgba(255,255,255,.3)")),
        text=df["module"],
        hovertemplate="<b>%{text}</b><br>LOC: %{x}<br>Complexity: %{y}<extra></extra>"))
    fig.update_layout(
        title=dict(text="<b>Complexity vs LOC</b>  (bubble = PageRank)",
                   font=dict(size=13, color="#e0e0e0")),
        xaxis=dict(title="Lines of Code", color="#9aa4b2", gridcolor="#1e1e2e"),
        yaxis=dict(title="Complexity", color="#9aa4b2", gridcolor="#1e1e2e"),
        margin=dict(l=10, r=10, t=45, b=30), height=400,
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117", font=dict(color="#e0e0e0"))
    return fig


def _show_metrics(mg, lg):
    cols = st.columns(6)
    cols[0].metric("Modules", len(mg.nodes) if mg else 0)
    cols[1].metric("Import Edges", len(mg.edges) if mg else 0)
    cols[2].metric("Datasets", len(lg.dataset_nodes) if lg else 0)
    cols[3].metric("Transforms", len(lg.transformation_nodes) if lg else 0)
    cols[4].metric("Sources", len(lg.sources) if lg else 0)
    cols[5].metric("Sinks", len(lg.sinks) if lg else 0)


def _render_navigator(nav, repo_path):
    st.markdown("### Navigator — Interactive Query Interface")
    col1, col2 = st.columns([1, 2])
    with col1:
        mode = st.radio("Tool", [
            "🔍 find_implementation", "🔗 trace_lineage",
            "💥 blast_radius", "📖 explain_module"],
            label_visibility="collapsed")
    with col2:
        if "find_implementation" in mode:
            st.markdown("**Find where a concept is implemented**")
            concept = st.text_input("Concept", value="staging customers", key="nav_c")
            if st.button("Search", key="nav_f"):
                with st.spinner("Searching..."): result = nav.find_implementation(concept)
                st.json(result)
        elif "trace_lineage" in mode:
            st.markdown("**Trace data flow for a dataset**")
            c1, c2 = st.columns(2)
            dataset = c1.text_input("Dataset", value="orders", key="nav_d")
            direction = c2.selectbox("Direction", ["upstream", "downstream", "both"], key="nav_dir")
            if st.button("Trace", key="nav_l"):
                with st.spinner("Tracing..."): result = nav.trace_lineage(dataset, direction)
                st.json(result)
        elif "blast_radius" in mode:
            st.markdown("**Find everything affected by a change**")
            module_path = st.text_input("Module or dataset", value="orders", key="nav_b")
            if st.button("Calculate", key="nav_br"):
                with st.spinner("Calculating..."): result = nav.blast_radius(module_path)
                st.json(result)
                affected = result.get("affected_nodes", [])
                if affected:
                    st.session_state["blast_highlight"] = set(affected)
                    st.success(f"⚠ {len(affected)} nodes affected — highlighted red in graphs")
        elif "explain_module" in mode:
            st.markdown("**Get full explanation of a module**")
            mp = st.text_input("Module path", value="models/staging/stg_customers.sql", key="nav_e")
            if st.button("Explain", key="nav_ex"):
                with st.spinner("Explaining..."): result = nav.explain_module(mp)
                st.json(result)


for key, default in [
    ("repo_path", None), ("repo_is_temp", False),
    ("analysis_ok", False), ("last_error", ""),
    ("blast_highlight", set()), ("manual_input", ""),
]:
    if key not in st.session_state:
        st.session_state[key] = default


with st.sidebar:
    st.markdown("## 🗺 Brownfield Cartographer")
    st.caption("Surveyor · Hydrologist · Semanticist · Archivist · Navigator")
    st.divider()

    st.markdown("**Quick repos (local)**")
    quick_selection = st.selectbox("", list(QUICK_REPOS.keys()),
        label_visibility="collapsed", key="quick_select")

    if quick_selection and QUICK_REPOS.get(quick_selection):
        repo_input = QUICK_REPOS[quick_selection]
        st.caption(f"📂 `{repo_input}`")
    else:
        st.markdown("**Or enter path manually**")
        repo_input = st.text_input("Path or GitHub URL",
            value=st.session_state.get("manual_input", ""),
            placeholder="/home/user/my-repo  or  https://github.com/...",
            label_visibility="collapsed", key="manual_repo_input")
        st.session_state["manual_input"] = repo_input

    st.divider()
    run_incremental = st.checkbox("Incremental mode", value=False,
        help="Only re-analyze files changed since last run")
    max_nodes = st.slider("Max graph nodes", 50, 800, 300, 50)
    graph_layout = st.selectbox("Graph layout", ["kamada_kawai", "spring", "circular", "shell"])
    st.caption(f"Git window: **{GIT_DAYS} days**")
    st.divider()
    analyze_clicked = st.button("▶  Analyze / Refresh", use_container_width=True, type="primary")

    if st.session_state.get("analysis_ok"):
        st.success("✓ Analysis loaded")
    if st.session_state.get("last_error"):
        st.error(st.session_state["last_error"][:300])


if analyze_clicked:
    if not repo_input or not repo_input.strip():
        st.error("Please select a repo or enter a path.")
        st.stop()
    if st.session_state.get("repo_path") and st.session_state.get("repo_is_temp"):
        try: shutil.rmtree(Path(st.session_state["repo_path"]).parent, ignore_errors=True)
        except Exception: pass
    try:
        with st.spinner("Resolving repository..."):
            repo_path, is_temp = _resolve_repo(repo_input)
        with st.spinner(f"Running Cartographer on `{repo_path.name}`..."):
            result = Orchestrator(repo_path=repo_path, incremental=run_incremental).run()
        st.session_state.update({
            "repo_path": str(repo_path), "repo_is_temp": is_temp,
            "analysis_ok": True, "last_error": "", "blast_highlight": set(),
        })
        st.success(f"✓ Done in {result.analysis_duration_seconds:.1f}s — {len(result.module_graph.nodes)} modules")
        st.rerun()
    except Exception as e:
        st.session_state.update({"analysis_ok": False, "last_error": str(e)})
        st.error(f"Analysis failed: {e}")
        st.stop()


repo_path_str = st.session_state.get("repo_path")
if not repo_path_str:
    st.markdown("""
## Welcome to Brownfield Cartographer
**Select a repo from the sidebar and click ▶ Analyze / Refresh.**

This system automatically:
- Parses Python, SQL, YAML files using tree-sitter AST analysis
- Builds a module dependency graph with PageRank scoring
- Extracts full data lineage across SQL, dbt, Airflow, and Dagster
- Generates LLM purpose statements for every module
- Produces `CODEBASE.md` and onboarding brief for Day-One FDE use
""")
    st.stop()

repo_path = Path(repo_path_str)
cart_dir = repo_path / ".cartography"

if not cart_dir.exists():
    st.warning(f"No `.cartography` at `{cart_dir}`. Click **▶ Analyze / Refresh**.")
    st.stop()

mg = _load_module_graph(cart_dir / "module_graph.json")
lg = _load_lineage_graph(cart_dir / "lineage_graph.json")
kg = _rebuild_kg(mg, lg)
nav = Navigator(
    repo_path=repo_path,
    module_graph=mg or ModuleGraph(target_repo=str(repo_path)),
    lineage_graph=lg or DataLineageGraph(target_repo=str(repo_path)),
    kg=kg,
)

label = _repo_label(repo_input or repo_path_str, repo_path)
blast_hl = st.session_state.get("blast_highlight", set())

st.markdown(f"## 🗺  {label}")
_show_metrics(mg, lg)
st.divider()

tabs = st.tabs([
    "📊 Overview", "🔵 Surveyor", "🌊 Hydrologist",
    "🧠 Semanticist", "📁 Archivist", "🧭 Navigator",
    "🌐 NetworkX", "📄 Raw",
])

with tabs[0]:
    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown("### Architecture Overview")
        txt = _load_text(cart_dir / "CODEBASE.md")
        if txt:
            st.markdown("\n".join([l for l in txt.splitlines() if l.strip()][:20]))
        else:
            st.info("Run analysis to generate CODEBASE.md")
        if mg and mg.architectural_hubs:
            st.markdown("### Top Architectural Hubs")
            hub_rows = []
            for hub in mg.architectural_hubs[:8]:
                n = mg.nodes.get(hub)
                if n:
                    hub_rows.append({
                        "module": _safe_rel(hub, repo_path),
                        "pagerank": round(n.pagerank_score, 6),
                        f"velocity ({GIT_DAYS}d)": n.change_velocity_30d,
                        "domain": n.domain_cluster or "",
                        "loc": n.loc,
                    })
            st.dataframe(pd.DataFrame(hub_rows), use_container_width=True)
    with c2:
        if mg: st.plotly_chart(_build_domain_pie(mg), use_container_width=True)
        if lg:
            st.markdown("### Sources & Sinks")
            ca, cb = st.columns(2)
            with ca:
                st.markdown("**Sources**")
                for s in sorted(lg.sources)[:10]: st.markdown(f"- `{s}`")
            with cb:
                st.markdown("**Sinks**")
                for s in sorted(lg.sinks)[:10]: st.markdown(f"- `{s}`")

with tabs[1]:
    st.markdown("### Surveyor — Module Dependency Graph")
    st.caption("Node size = PageRank · Color = domain cluster · Hover for full details")
    if mg is None:
        st.warning("No module graph. Run analysis first.")
    else:
        domains = ["All"] + sorted({n.domain_cluster or "unclassified" for n in mg.nodes.values()})
        col1, col2 = st.columns([3, 1])
        domain_filter = col1.selectbox("Filter by domain", domains, key="surveyor_domain")
        show_dead = col2.checkbox("Show dead code", value=True)

        st.plotly_chart(_build_module_figure(mg, repo_path, max_nodes=max_nodes,
            domain_filter=domain_filter, highlight=blast_hl, layout=graph_layout),
            use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"### Git Velocity ({GIT_DAYS}d)")
            fig_vel = _build_velocity_figure(mg, repo_path)
            if fig_vel.data:
                st.plotly_chart(fig_vel, use_container_width=True)
            else:
                st.info(f"No commits found in {GIT_DAYS}-day window")
            st.markdown("### Complexity vs LOC")
            fig_cx = _build_complexity_scatter(mg, repo_path)
            if fig_cx.data:
                st.plotly_chart(fig_cx, use_container_width=True)
        with col2:
            st.markdown("### Circular Dependencies")
            if mg.circular_dependencies:
                for cycle in mg.circular_dependencies[:15]:
                    st.code(" ↔ ".join(_safe_rel(p, repo_path) for p in cycle))
            else:
                st.success("✓ No circular dependencies")
            if show_dead:
                dead = [n for n in mg.nodes.values() if n.is_dead_code_candidate]
                st.markdown(f"### Dead Code Candidates ({len(dead)})")
                if dead:
                    st.dataframe(pd.DataFrame([{
                        "module": _safe_rel(n.path, repo_path),
                        "loc": n.loc, "language": str(n.language),
                    } for n in dead[:30]]), use_container_width=True)
                else:
                    st.success("✓ No dead code candidates")

        st.markdown("### Full Module Metrics")
        df = pd.DataFrame([{
            "module": _safe_rel(n.path, repo_path),
            "language": str(n.language),
            "loc": n.loc,
            "complexity": round(n.complexity_score, 1),
            "pagerank": round(n.pagerank_score, 6),
            f"velocity ({GIT_DAYS}d)": n.change_velocity_30d,
            "domain": n.domain_cluster or "",
            "dead": n.is_dead_code_candidate,
            "purpose": (n.purpose_statement or "")[:100],
        } for n in mg.nodes.values()]).sort_values("pagerank", ascending=False)
        st.dataframe(df, use_container_width=True, height=400)

with tabs[2]:
    st.markdown("### Hydrologist — Data Lineage Graph")
    st.caption("○ Circles = datasets  ◆ Diamonds = transformations  Color = transformation type")
    if lg is None:
        st.warning("No lineage graph. Run analysis first.")
    else:
        st.plotly_chart(_build_lineage_figure(lg, highlight=blast_hl), use_container_width=True)

        st.markdown("### Transformation Legend")
        legend_cols = st.columns(len(TRANSFORM_COLORS))
        for i, (ttype, col) in enumerate(TRANSFORM_COLORS.items()):
            legend_cols[i].markdown(
                f"<div style='background:{col};padding:3px 6px;border-radius:4px;"
                f"font-size:10px;text-align:center;color:white'>{ttype}</div>",
                unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### Datasets")
            st.dataframe(pd.DataFrame([{
                "dataset": ds.name,
                "storage": str(ds.storage_type),
                "owner": ds.owner or "",
                "source_of_truth": ds.is_source_of_truth,
                "file": Path(ds.source_file).name if ds.source_file else "",
            } for ds in lg.dataset_nodes.values()]), use_container_width=True, height=380)
        with c2:
            st.markdown("### Transformations")
            tr_rows = [{
                "id": Path(t.id).name,
                "type": str(t.transformation_type).replace("TransformationType.", ""),
                "sources": ", ".join(t.source_datasets[:4]),
                "targets": ", ".join(t.target_datasets[:4]),
                "file": Path(t.source_file).name if t.source_file else "",
            } for t in lg.transformation_nodes.values()]
            if tr_rows:
                st.dataframe(pd.DataFrame(tr_rows), use_container_width=True, height=380)

with tabs[3]:
    st.markdown("### Semanticist — Semantic Knowledge Index")
    st.caption("Purpose statements derived from actual code. Domain clusters from semantic similarity.")
    if mg is None:
        st.warning("No module graph.")
    else:
        search = st.text_input("Search concept / purpose / domain",
            placeholder="e.g. revenue  |  customer  |  ingestion")
        sem_df = pd.DataFrame([{
            "module": _safe_rel(n.path, repo_path),
            "purpose": n.purpose_statement or "",
            "domain": n.domain_cluster or "unclassified",
            "pagerank": round(n.pagerank_score, 6),
            f"velocity ({GIT_DAYS}d)": n.change_velocity_30d,
            "language": str(n.language),
        } for n in mg.nodes.values()])
        if search.strip():
            q = search.lower()
            sem_df = sem_df[
                sem_df["module"].str.lower().str.contains(q, na=False) |
                sem_df["purpose"].str.lower().str.contains(q, na=False) |
                sem_df["domain"].str.lower().str.contains(q, na=False)]
        st.dataframe(sem_df.sort_values("pagerank", ascending=False),
            use_container_width=True, height=480)

        trace_path = cart_dir / "semanticist_trace.json"
        if trace_path.exists():
            try:
                td = json.loads(trace_path.read_text())
                st.markdown("### Documentation Drift")
                drift = td.get("drift_flags", {})
                if drift:
                    for pk, flag in list(drift.items())[:20]:
                        st.warning(f"`{_safe_rel(pk, repo_path)}` — {flag}")
                else:
                    st.success("✓ No documentation drift detected")
                st.markdown("### Day-One FDE Answers")
                answers = td.get("day_one_answers", {})
                if answers:
                    for i, (q, a) in enumerate(answers.items(), 1):
                        with st.expander(f"**Q{i}:** {q}", expanded=(i == 1)):
                            st.markdown(a)
                else:
                    st.info("No LLM answers generated")
            except Exception as e:
                st.warning(f"Could not load trace: {e}")

with tabs[4]:
    st.markdown("### Archivist — Living Context Artifacts")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### CODEBASE.md")
        txt = _load_text(cart_dir / "CODEBASE.md")
        st.markdown(txt) if txt else st.info("Not generated yet")
    with c2:
        st.markdown("#### onboarding_brief.md")
        txt = _load_text(cart_dir / "onboarding_brief.md")
        st.markdown(txt) if txt else st.info("Not generated yet")
    st.markdown("#### cartography_trace.jsonl")
    trace_rows = _load_trace_rows(cart_dir / "cartography_trace.jsonl")
    if trace_rows:
        st.dataframe(pd.DataFrame(trace_rows), use_container_width=True, height=260)
    else:
        st.info("No trace records")

with tabs[5]:
    _render_navigator(nav, repo_path)

with tabs[6]:
    st.markdown("### 🌐 NetworkX Interactive Graph")
    st.caption("Physics-based interactive visualization — drag nodes, zoom, hover for details.")
    html_path = cart_dir / "module_graph_networkx.html"
    if html_path.exists():
        st.components.v1.html(html_path.read_text(encoding="utf-8"), height=820, scrolling=False)
    else:
        st.info("NetworkX graph not found. Generating...")
        if mg:
            try:
                kg.visualize_module_graph(html_path)
                st.components.v1.html(html_path.read_text(encoding="utf-8"), height=820, scrolling=False)
                st.success("✓ NetworkX graph generated")
            except Exception as e:
                st.error(f"Could not generate: {e}")

with tabs[7]:
    st.markdown("### Raw Artifact Files")
    files = ["module_graph.json", "lineage_graph.json", "CODEBASE.md",
             "onboarding_brief.md", "cartography_trace.jsonl",
             "semanticist_trace.json", "analysis_summary.md"]
    available = [f for f in files if (cart_dir / f).exists()]
    if not available:
        st.info("No artifacts found. Run analysis first.")
    else:
        sel = st.selectbox("Select artifact", available)
        p = cart_dir / sel
        if p.suffix == ".json":
            st.json(_load_json(p))
        elif p.suffix == ".jsonl":
            rows = _load_trace_rows(p)
            if rows: st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.code(_load_text(p), language="markdown")