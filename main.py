from __future__ import annotations
import json, os, shutil, subprocess, tempfile
from pathlib import Path
from typing import Any
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from src.models import DataLineageGraph, ModuleGraph
from src.orchestrator import Orchestrator

app = FastAPI(title="Brownfield Cartographer")
BASE_DIR = Path.home() / "Desktop" / "brownfield-cartographer"
KNOWN_REPOS = {
    "jaffle_shop": str(BASE_DIR / "jaffle_shop"),
    "ol-data-platform": str(BASE_DIR / "ol-data-platform"),
    "data-engineering-zoomcamp": str(BASE_DIR / "data-engineering-zoomcamp"),
}

class AnalyzeRequest(BaseModel):
    repo_path: str
    incremental: bool = False

def _load_mg(cart_dir):
    p = cart_dir / "module_graph.json"
    if not p.exists(): return None
    try: return ModuleGraph.model_validate_json(p.read_text(encoding="utf-8"))
    except: return None

def _load_lg(cart_dir):
    p = cart_dir / "lineage_graph.json"
    if not p.exists(): return None
    try: return DataLineageGraph.model_validate_json(p.read_text(encoding="utf-8"))
    except: return None

def _safe_rel(path, repo_path):
    try: return str(Path(path).resolve().relative_to(repo_path.resolve()))
    except: return Path(path).name

@app.get("/", response_class=HTMLResponse)
async def root():
    f = Path(__file__).parent / "web" / "index.html"
    return HTMLResponse(f.read_text(encoding="utf-8") if f.exists() else "<h1>web/index.html not found</h1>")

@app.get("/api/repos")
async def get_repos():
    repos = []
    for name, path in KNOWN_REPOS.items():
        p = Path(path); cd = p / ".cartography"
        repos.append({"name": name, "path": path, "exists": p.exists(),
            "analyzed": cd.exists(), "artifacts": len(list(cd.glob("*"))) if cd.exists() else 0})
    return JSONResponse(repos)

@app.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    rp = Path(req.repo_path).expanduser().resolve()
    if not rp.exists(): raise HTTPException(404, f"Path not found: {rp}")
    try:
        result = Orchestrator(repo_path=rp, incremental=req.incremental).run()
        return JSONResponse({"status": "ok", "duration": result.analysis_duration_seconds,
            "modules": len(result.module_graph.nodes), "datasets": len(result.lineage_graph.dataset_nodes), "errors": result.errors})
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/module-graph/{repo_name}")
async def get_module_graph(repo_name: str):
    path = KNOWN_REPOS.get(repo_name)
    if not path: raise HTTPException(404, f"Unknown repo: {repo_name}")
    rp = Path(path); mg = _load_mg(rp / ".cartography")
    if not mg: raise HTTPException(404, "Not analyzed yet — run analysis first")
    nodes = [{"id": np, "label": Path(_safe_rel(np, rp)).name, "path": _safe_rel(np, rp),
        "language": str(n.language), "domain": n.domain_cluster or "unclassified",
        "pagerank": n.pagerank_score, "velocity": n.change_velocity_30d, "loc": n.loc,
        "complexity": n.complexity_score, "dead": n.is_dead_code_candidate, "purpose": n.purpose_statement or ""}
        for np, n in mg.nodes.items()]
    edges = [{"source": e.source, "target": e.target, "weight": e.weight} for e in mg.edges]
    return JSONResponse({"nodes": nodes, "edges": edges, "hubs": mg.architectural_hubs[:10],
        "circular_deps": mg.circular_dependencies[:10], "high_velocity": mg.high_velocity_files[:10]})

@app.get("/api/lineage-graph/{repo_name}")
async def get_lineage_graph(repo_name: str):
    path = KNOWN_REPOS.get(repo_name)
    if not path: raise HTTPException(404, f"Unknown repo: {repo_name}")
    rp = Path(path); lg = _load_lg(rp / ".cartography")
    if not lg: raise HTTPException(404, "Not analyzed yet")
    nodes = []
    for name, ds in lg.dataset_nodes.items():
        nodes.append({"id": name, "label": name, "type": "dataset", "storage": str(ds.storage_type),
            "is_source": name in lg.sources, "is_sink": name in lg.sinks, "source_of_truth": ds.is_source_of_truth})
    for tid, t in lg.transformation_nodes.items():
        ttype = str(t.transformation_type).replace("TransformationType.", "")
        nodes.append({"id": tid, "label": Path(tid).name[:30], "type": "transformation", "ttype": ttype,
            "source_file": Path(t.source_file).name if t.source_file else "",
            "sources": t.source_datasets, "targets": t.target_datasets})
    edges = [{"source": e.source, "target": e.target, "edge_type": str(e.edge_type)} for e in lg.edges]
    return JSONResponse({"nodes": nodes, "edges": edges, "sources": lg.sources, "sinks": lg.sinks})

@app.get("/api/summary/{repo_name}")
async def get_summary(repo_name: str):
    path = KNOWN_REPOS.get(repo_name)
    if not path: raise HTTPException(404, "Unknown repo")
    rp = Path(path); cd = rp / ".cartography"
    mg = _load_mg(cd); lg = _load_lg(cd)
    day_one = {}; drift = {}
    st = cd / "semanticist_trace.json"
    if st.exists():
        try: td = json.loads(st.read_text()); day_one = td.get("day_one_answers", {}); drift = td.get("drift_flags", {})
        except: pass
    return JSONResponse({"repo": repo_name, "modules": len(mg.nodes) if mg else 0,
        "edges": len(mg.edges) if mg else 0, "datasets": len(lg.dataset_nodes) if lg else 0,
        "transforms": len(lg.transformation_nodes) if lg else 0,
        "sources": len(lg.sources) if lg else 0, "sinks": len(lg.sinks) if lg else 0,
        "hubs": [_safe_rel(h, rp) for h in (mg.architectural_hubs[:5] if mg else [])],
        "circular_deps": len(mg.circular_dependencies) if mg else 0,
        "day_one_answers": day_one, "drift_flags": len(drift), "analyzed": cd.exists()})

@app.get("/api/artifact/{repo_name}/{artifact}")
async def get_artifact(repo_name: str, artifact: str):
    path = KNOWN_REPOS.get(repo_name)
    if not path: raise HTTPException(404, "Unknown repo")
    allowed = ["CODEBASE.md", "onboarding_brief.md", "analysis_summary.md", "cartography_trace.jsonl", "semanticist_trace.json"]
    if artifact not in allowed: raise HTTPException(400, "Not allowed")
    f = Path(path) / ".cartography" / artifact
    if not f.exists(): raise HTTPException(404, "Not found")
    return JSONResponse({"content": f.read_text(encoding="utf-8")})

if __name__ == "__main__":
    Path("web").mkdir(exist_ok=True)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)