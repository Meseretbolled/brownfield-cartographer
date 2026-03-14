"""
Brownfield Cartographer — FastAPI backend for web/index.html

Run:  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
Then: open http://localhost:8000
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.models import DataLineageGraph, ModuleGraph
from src.orchestrator import Orchestrator

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="Brownfield Cartographer", version="1.0.0")

# Root of the project (where main.py lives)
PROJECT_ROOT = Path(__file__).parent.resolve()

# Bundled cartography artifacts shipped with the repo
BUNDLED_ARTIFACTS = PROJECT_ROOT / "cartography-artifacts"

# Where user repos live on disk — can be overridden via env var
BASE_DIR = Path(os.getenv("CARTOGRAPHER_BASE_DIR", str(Path.home() / "Desktop" / "brownfield-cartographer")))

GIT_DAYS = int(os.getenv("CARTOGRAPHER_GIT_DAYS", "30"))

# ---------------------------------------------------------------------------
# Known repos: bundled artifacts + local repos if they exist
# ---------------------------------------------------------------------------

def _build_known_repos() -> dict[str, dict[str, Any]]:
    """
    Build the repo registry.
    Priority: local repo on disk (for fresh analysis) + bundled artifacts (for viewing).
    """
    repos: dict[str, dict[str, Any]] = {}

    # Bundled artifact repos (always available for viewing)
    if BUNDLED_ARTIFACTS.exists():
        for artifact_dir in sorted(BUNDLED_ARTIFACTS.iterdir()):
            if artifact_dir.is_dir():
                name = artifact_dir.name
                # Try to find a matching local repo
                local_path = BASE_DIR / name
                repos[name] = {
                    "name": name,
                    "local_path": local_path if local_path.exists() else None,
                    "artifact_dir": artifact_dir,  # bundled, always readable
                }

    # Any additional local repos not in bundled artifacts
    if BASE_DIR.exists():
        for d in sorted(BASE_DIR.iterdir()):
            if d.is_dir() and d.name not in repos:
                cart = d / ".cartography"
                if cart.exists():
                    repos[d.name] = {
                        "name": d.name,
                        "local_path": d,
                        "artifact_dir": cart,
                    }

    return repos


def _get_cart_dir(repo_name: str) -> Path:
    """Return the best available cartography directory for a repo."""
    repos = _build_known_repos()
    if repo_name not in repos:
        raise HTTPException(404, f"Unknown repo: {repo_name}")
    info = repos[repo_name]

    # Prefer local .cartography if the repo exists on disk with fresh analysis
    if info["local_path"]:
        local_cart = info["local_path"] / ".cartography"
        if local_cart.exists():
            return local_cart

    # Fall back to bundled artifacts
    artifact_dir = info["artifact_dir"]
    if artifact_dir and artifact_dir.exists():
        return artifact_dir

    raise HTTPException(404, f"No artifacts found for {repo_name}. Run analysis first.")


def _load_mg(cart_dir: Path) -> ModuleGraph | None:
    p = cart_dir / "module_graph.json"
    if not p.exists():
        return None
    try:
        return ModuleGraph.model_validate_json(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_lg(cart_dir: Path) -> DataLineageGraph | None:
    p = cart_dir / "lineage_graph.json"
    if not p.exists():
        return None
    try:
        return DataLineageGraph.model_validate_json(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _safe_rel(path: str, repo_path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except Exception:
        return Path(path).name


def _last_analysis(cart_dir: Path) -> str:
    """Return a human-readable timestamp for the last analysis."""
    summary = cart_dir / "analysis_summary.md"
    if summary.exists():
        for line in summary.read_text(encoding="utf-8").splitlines():
            if "Generated:" in line or "generated:" in line.lower():
                try:
                    raw = line.split("**Generated:**")[-1].strip() if "**" in line else line.split(":")[-1].strip()
                    dt = datetime.fromisoformat(raw[:19])
                    return dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    return raw[:20]
    sha_file = cart_dir / "last_run_sha.txt"
    if sha_file.exists():
        return sha_file.read_text().strip()[:8]
    return "—"


# ---------------------------------------------------------------------------
# Pydantic models for request bodies
# ---------------------------------------------------------------------------

class AnalyzeRequest(BaseModel):
    repo_name: str
    incremental: bool = False
    repo_url: str = ""   # optional GitHub URL


class BlastRequest(BaseModel):
    repo_name: str
    node_id: str


# ---------------------------------------------------------------------------
# Serve index.html
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    f = PROJECT_ROOT / "web" / "index.html"
    if not f.exists():
        return HTMLResponse("<h1>web/index.html not found</h1>", status_code=404)
    return HTMLResponse(f.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# GET /api/repos  — repo list for sidebar dropdown
# ---------------------------------------------------------------------------

@app.get("/api/repos")
async def get_repos():
    repos = _build_known_repos()
    result = []
    for name, info in repos.items():
        cart = info["artifact_dir"]
        if info["local_path"]:
            local_cart = info["local_path"] / ".cartography"
            if local_cart.exists():
                cart = local_cart

        analyzed = cart.exists() if cart else False
        artifact_count = len(list(cart.glob("*"))) if (cart and cart.exists()) else 0
        last = _last_analysis(cart) if (cart and cart.exists()) else "—"

        result.append({
            "name": name,
            "path": str(info["local_path"]) if info["local_path"] else f"bundled:{name}",
            "analyzed": analyzed,
            "artifacts": artifact_count,
            "last_analysis": last,
        })
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# GET /api/repo/{name}/summary  — metrics bar + overview text
# ---------------------------------------------------------------------------

@app.get("/api/repo/{repo_name}/summary")
async def get_summary(repo_name: str):
    cart_dir = _get_cart_dir(repo_name)
    mg = _load_mg(cart_dir)
    lg = _load_lg(cart_dir)

    # Try to find repo root for relative paths
    repos = _build_known_repos()
    info = repos.get(repo_name, {})
    rp = info.get("local_path") or Path(repo_name)

    day_one_answers: dict[str, str] = {}
    drift_flags: dict[str, str] = {}
    st = cart_dir / "semanticist_trace.json"
    if st.exists():
        try:
            td = json.loads(st.read_text(encoding="utf-8"))
            day_one_answers = td.get("day_one_answers", {})
            drift_flags = td.get("drift_flags", {})
        except Exception:
            pass

    hubs = []
    if mg:
        for h in mg.architectural_hubs[:5]:
            node = mg.nodes.get(h)
            hubs.append({
                "id": h,
                "label": Path(_safe_rel(h, rp)).name,
                "pagerank": round(node.pagerank_score, 6) if node else 0,
                "velocity": node.change_velocity_30d if node else 0,
            })

    return JSONResponse({
        "repo": repo_name,
        "modules": len(mg.nodes) if mg else 0,
        "edges": len(mg.edges) if mg else 0,
        "datasets": len(lg.dataset_nodes) if lg else 0,
        "transformations": len(lg.transformation_nodes) if lg else 0,
        "sources": len(lg.sources) if lg else 0,
        "sinks": len(lg.sinks) if lg else 0,
        "hubs": [h["id"] for h in hubs],
        "hubs_detail": hubs,
        "circular_deps": len(mg.circular_dependencies) if mg else 0,
        "high_velocity": mg.high_velocity_files[:10] if mg else [],
        "day_one_answers": day_one_answers,
        "drift_flags": len(drift_flags),
        "last_analysis": _last_analysis(cart_dir),
        "artifacts": len(list(cart_dir.glob("*"))) if cart_dir.exists() else 0,
        "analyzed": cart_dir.exists(),
    })


# ---------------------------------------------------------------------------
# GET /api/repo/{name}/module_graph  — Cytoscape node/edge data
# ---------------------------------------------------------------------------

@app.get("/api/repo/{repo_name}/module_graph")
async def get_module_graph(repo_name: str):
    cart_dir = _get_cart_dir(repo_name)
    mg = _load_mg(cart_dir)
    if not mg:
        raise HTTPException(404, "module_graph.json not found — run analysis first")

    repos = _build_known_repos()
    info = repos.get(repo_name, {})
    rp = info.get("local_path") or Path(repo_name)

    nodes = []
    for node_path, n in mg.nodes.items():
        rel = _safe_rel(node_path, rp)
        nodes.append({
            "id": node_path,
            "label": Path(rel).name,
            "path": rel,
            "language": str(n.language).replace("Language.", ""),
            "domain": n.domain_cluster or "unclassified",
            "pagerank": n.pagerank_score,
            "velocity": n.change_velocity_30d,
            "loc": n.loc,
            "complexity": round(n.complexity_score, 2),
            "dead": n.is_dead_code_candidate,
            "purpose": n.purpose_statement or "",
            "exports": n.exported_functions[:8],
        })

    edges = [
        {"source": e.source, "target": e.target, "weight": e.weight}
        for e in mg.edges
    ]

    return JSONResponse({
        "nodes": nodes,
        "edges": edges,
        "hubs": mg.architectural_hubs[:10],
        "circular_deps": mg.circular_dependencies[:10],
        "high_velocity": mg.high_velocity_files[:10],
    })


# ---------------------------------------------------------------------------
# GET /api/repo/{name}/lineage_graph  — lineage nodes/edges/sources/sinks
# ---------------------------------------------------------------------------

@app.get("/api/repo/{repo_name}/lineage_graph")
async def get_lineage_graph(repo_name: str):
    cart_dir = _get_cart_dir(repo_name)
    lg = _load_lg(cart_dir)
    if not lg:
        raise HTTPException(404, "lineage_graph.json not found — run analysis first")

    nodes: list[dict[str, Any]] = []

    for name, ds in lg.dataset_nodes.items():
        nodes.append({
            "id": name,
            "label": name,
            "type": "dataset",
            "storage_type": str(ds.storage_type).replace("StorageType.", ""),
            "owner": ds.owner or "",
            "is_source": name in lg.sources,
            "is_sink": name in lg.sinks,
            "source_of_truth": ds.is_source_of_truth,
            "description": ds.description or "",
        })

    for tid, t in lg.transformation_nodes.items():
        ttype = str(t.transformation_type).replace("TransformationType.", "")
        nodes.append({
            "id": tid,
            "label": Path(tid.replace("py::", "").replace("nb::", "")).name[:32],
            "type": "transformation",
            "ttype": ttype,
            "source_file": Path(t.source_file).name if t.source_file else "",
            "sources": t.source_datasets,
            "targets": t.target_datasets,
        })

    edges = [
        {"source": e.source, "target": e.target, "edge_type": str(e.edge_type).replace("EdgeType.", "")}
        for e in lg.edges
    ]

    return JSONResponse({
        "nodes": nodes,
        "edges": edges,
        "sources": lg.sources,
        "sinks": lg.sinks,
    })


# ---------------------------------------------------------------------------
# GET /api/repo/{name}/semanticist  — domain map, drift flags, day-one answers
# ---------------------------------------------------------------------------

@app.get("/api/repo/{repo_name}/semanticist")
async def get_semanticist(repo_name: str):
    cart_dir = _get_cart_dir(repo_name)
    st = cart_dir / "semanticist_trace.json"

    if not st.exists():
        # Return empty but valid structure — no LLM run yet
        return JSONResponse({
            "domain_map": {},
            "drift_flags": {},
            "day_one_answers": {},
            "budget_summary": {},
            "models": {},
        })

    try:
        td = json.loads(st.read_text(encoding="utf-8"))
        return JSONResponse({
            "domain_map": td.get("domain_map", {}),
            "drift_flags": td.get("drift_flags", {}),
            "day_one_answers": td.get("day_one_answers", {}),
            "budget_summary": td.get("budget_summary", {}),
            "models": td.get("models", {}),
        })
    except Exception as e:
        raise HTTPException(500, f"Could not parse semanticist_trace.json: {e}")


# ---------------------------------------------------------------------------
# GET /api/repo/{name}/artifacts/{filename}  — raw artifact files
# ---------------------------------------------------------------------------

ALLOWED_ARTIFACTS = {
    "CODEBASE.md",
    "onboarding_brief.md",
    "analysis_summary.md",
    "cartography_trace.jsonl",
    "semanticist_trace.json",
    "module_graph_networkx.html",
}

@app.get("/api/repo/{repo_name}/artifacts/{artifact}")
async def get_artifact(repo_name: str, artifact: str):
    if artifact not in ALLOWED_ARTIFACTS:
        raise HTTPException(400, f"Artifact '{artifact}' not in allowed list")
    cart_dir = _get_cart_dir(repo_name)
    f = cart_dir / artifact
    if not f.exists():
        raise HTTPException(404, f"{artifact} not found — run analysis first")
    return JSONResponse({"content": f.read_text(encoding="utf-8")})


# ---------------------------------------------------------------------------
# POST /api/analyze  — trigger full analysis pipeline
# ---------------------------------------------------------------------------

@app.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    repos = _build_known_repos()
    info = repos.get(req.repo_name)

    # Resolve repo path
    if info and info.get("local_path"):
        repo_path = info["local_path"]
    elif req.repo_url:
        # Clone GitHub URL into a temp dir under BASE_DIR
        target = BASE_DIR / req.repo_name
        target.parent.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(
            ["git", "clone", "--depth=100", req.repo_url, str(target)],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0:
            raise HTTPException(400, f"Git clone failed: {r.stderr.strip()}")
        repo_path = target
    else:
        raise HTTPException(404, f"Repo '{req.repo_name}' not found on disk. "
                                  f"Provide repo_url to clone it first.")

    if not repo_path.exists():
        raise HTTPException(404, f"Repo path not found: {repo_path}")

    try:
        result = Orchestrator(
            repo_path=repo_path,
            incremental=req.incremental,
        ).run()

        return JSONResponse({
            "success": True,
            "duration": round(result.analysis_duration_seconds, 1),
            "modules": len(result.module_graph.nodes),
            "datasets": len(result.lineage_graph.dataset_nodes),
            "errors": result.errors,
            "warnings": result.warnings,
        })
    except Exception as e:
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# POST /api/blast_radius  — blast radius from Navigator
# ---------------------------------------------------------------------------

@app.post("/api/blast_radius")
async def blast_radius(req: BlastRequest):
    cart_dir = _get_cart_dir(req.repo_name)
    mg = _load_mg(cart_dir)
    lg = _load_lg(cart_dir)

    if not mg and not lg:
        raise HTTPException(404, "No graphs loaded — run analysis first")

    from src.graph.knowledge_graph import KnowledgeGraph
    kg = KnowledgeGraph()
    if mg:
        for node in mg.nodes.values():
            kg.add_module(node)
        for edge in mg.edges:
            kg.add_module_edge(edge.source, edge.target)
    if lg:
        for node in lg.dataset_nodes.values():
            kg.add_dataset(node)
        for node in lg.transformation_nodes.values():
            kg.add_transformation(node)

    affected = kg.blast_radius(req.node_id)

    repos = _build_known_repos()
    info = repos.get(req.repo_name, {})
    rp = info.get("local_path") or Path(req.repo_name)

    return JSONResponse({
        "node_id": req.node_id,
        "affected_nodes": affected,
        "count": len(affected),
        "analysis_method": "BFS over module/lineage graph",
        "risk": "HIGH" if len(affected) > 10 else "MEDIUM" if len(affected) > 3 else "LOW",
    })


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return JSONResponse({"status": "ok", "project_root": str(PROJECT_ROOT)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print(f"\n🗺  Brownfield Cartographer starting on http://localhost:{port}")
    print(f"   Project root : {PROJECT_ROOT}")
    print(f"   Bundled artifacts : {BUNDLED_ARTIFACTS}")
    print(f"   Local repos dir   : {BASE_DIR}\n")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)