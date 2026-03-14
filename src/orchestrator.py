from __future__ import annotations

import logging
import os
import subprocess
import time
from datetime import datetime, UTC
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.agents.archivist import Archivist
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.surveyor import Surveyor
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import CartographyResult, DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(levelname)s  %(name)s  %(message)s",
)

CARTOGRAPHY_DIR = ".cartography"
LAST_RUN_FILE   = "last_run_sha.txt"
GIT_DAYS        = int(os.getenv("CARTOGRAPHER_GIT_DAYS", "30"))
TOKEN_BUDGET    = int(os.getenv("CARTOGRAPHER_TOKEN_BUDGET", "200000"))


def _get_current_sha(repo_path: Path) -> str | None:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() or None if r.returncode == 0 else None
    except Exception:
        return None


def _get_changed_files_since(repo_path: Path, since_sha: str) -> list[str]:
    try:
        r = subprocess.run(
            ["git", "diff", "--name-only", since_sha, "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        )
        return [l.strip() for l in r.stdout.splitlines() if l.strip()] if r.returncode == 0 else []
    except Exception:
        return []


class Orchestrator:
    def __init__(
        self,
        repo_path: Path,
        output_dir: Path | None = None,
        incremental: bool = False,
    ) -> None:
        self.repo_path  = repo_path.resolve()
        self.output_dir = (output_dir or (self.repo_path / CARTOGRAPHY_DIR)).resolve()
        self.incremental = incremental
        self.kg = KnowledgeGraph()

    def _load_last_sha(self) -> str | None:
        f = self.output_dir / LAST_RUN_FILE
        return f.read_text(encoding="utf-8").strip() or None if f.exists() else None

    def _save_current_sha(self) -> None:
        sha = _get_current_sha(self.repo_path)
        if sha:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            (self.output_dir / LAST_RUN_FILE).write_text(sha, encoding="utf-8")

    def _should_skip_incremental(self) -> bool:
        if not self.incremental:
            return False
        last_sha = self._load_last_sha()
        if not last_sha:
            logger.info("[Orchestrator] No previous run — running full analysis")
            return False
        changed = _get_changed_files_since(self.repo_path, last_sha)
        if not changed:
            logger.info("[Orchestrator] No changes since last run — skipping")
            return True
        logger.info(f"[Orchestrator] {len(changed)} changed file(s) since last run")
        return False

    def run(self) -> CartographyResult:
        start    = time.time()
        errors:   list[str] = []
        warnings: list[str] = []

        # Accumulated trace records from all agents — passed to Archivist
        agent_trace: list[dict] = []

        if self._should_skip_incremental():
            return self._load_cached_result()

        logger.info(f"[Orchestrator] Starting analysis for {self.repo_path}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        module_graph   = ModuleGraph(target_repo=str(self.repo_path))
        lineage_graph  = DataLineageGraph(target_repo=str(self.repo_path))
        semanticist: Semanticist | None = None

        # ------------------------------------------------------------------
        # Surveyor
        # ------------------------------------------------------------------
        try:
            surveyor     = Surveyor(repo_path=self.repo_path, kg=self.kg)
            module_graph = surveyor.run(git_days=GIT_DAYS)
            surveyor.save(self.output_dir, module_graph)

            agent_trace.append({
                "timestamp":        datetime.now(UTC).isoformat(),
                "agent":            "surveyor",
                "action":           "static_analysis_complete",
                "confidence":       "high",
                "evidence_method":  "tree-sitter AST + git log",
                "evidence_counts":  {
                    "modules_parsed":         len(module_graph.nodes),
                    "import_edges":           len(module_graph.edges),
                    "circular_dependencies":  len(module_graph.circular_dependencies),
                    "architectural_hubs":     len(module_graph.architectural_hubs),
                    "high_velocity_files":    len(module_graph.high_velocity_files),
                },
            })
            # One trace record per parsed module (evidence citations)
            for path, node in list(module_graph.nodes.items())[:50]:  # cap at 50 to keep file readable
                agent_trace.append({
                    "timestamp":       datetime.now(UTC).isoformat(),
                    "agent":           "surveyor",
                    "action":          "module_parsed",
                    "confidence":      "high",
                    "evidence_method": "tree-sitter AST",
                    "module_path":     path,
                    "language":        str(node.language),
                    "loc":             node.loc,
                    "exports":         node.exported_functions[:5],
                    "pagerank_score":  round(node.pagerank_score, 6),
                    "velocity_30d":    node.change_velocity_30d,
                    "is_dead_code_candidate": node.is_dead_code_candidate,
                })

        except Exception as e:
            msg = f"Surveyor failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # ------------------------------------------------------------------
        # Hydrologist
        # ------------------------------------------------------------------
        try:
            hydrologist   = Hydrologist(repo_path=self.repo_path, kg=self.kg)
            lineage_graph = hydrologist.run()
            hydrologist.save(self.output_dir, lineage_graph)

            agent_trace.append({
                "timestamp":       datetime.now(UTC).isoformat(),
                "agent":           "hydrologist",
                "action":          "lineage_analysis_complete",
                "confidence":      "high",
                "evidence_method": "regex + sqlglot + YAML parsing",
                "evidence_counts": {
                    "datasets":        len(lineage_graph.dataset_nodes),
                    "transformations": len(lineage_graph.transformation_nodes),
                    "sources":         len(lineage_graph.sources),
                    "sinks":           len(lineage_graph.sinks),
                },
            })
            # Trace each transformation node
            for node in list(lineage_graph.transformation_nodes.values())[:30]:
                agent_trace.append({
                    "timestamp":         datetime.now(UTC).isoformat(),
                    "agent":             "hydrologist",
                    "action":            "transformation_detected",
                    "confidence":        "high",
                    "evidence_method":   "static analysis",
                    "source_file":       node.source_file,
                    "transformation_type": str(node.transformation_type),
                    "source_datasets":   node.source_datasets,
                    "target_datasets":   node.target_datasets,
                })

        except Exception as e:
            msg = f"Hydrologist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # ------------------------------------------------------------------
        # Semanticist
        # ------------------------------------------------------------------
        try:
            semanticist = Semanticist(
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                repo_path=self.repo_path,
                budget_tokens=TOKEN_BUDGET,
            )
            semanticist.run()
            semanticist.save_trace(self.output_dir)
            # Re-save module_graph now that purpose statements + domains are populated
            (self.output_dir / "module_graph.json").write_text(
                module_graph.model_dump_json(indent=2), encoding="utf-8"
            )
        except Exception as e:
            msg = f"Semanticist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # ------------------------------------------------------------------
        # Archivist — receives all upstream trace records
        # ------------------------------------------------------------------
        try:
            archivist = Archivist(
                repo_path=self.repo_path,
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                semanticist=semanticist,
                agent_trace_records=agent_trace,
            )
            archivist.run(self.output_dir)
        except Exception as e:
            msg = f"Archivist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # ------------------------------------------------------------------
        # Visualization
        # ------------------------------------------------------------------
        try:
            html_path = self.output_dir / "module_graph_networkx.html"
            self.kg.visualize_module_graph(html_path)
        except Exception as e:
            warnings.append(f"Visualization failed: {e}")

        elapsed = time.time() - start
        logger.info(f"[Orchestrator] Done in {elapsed:.2f}s")
        self._save_current_sha()

        result = CartographyResult(
            target_repo=str(self.repo_path),
            module_graph=module_graph,
            lineage_graph=lineage_graph,
            analysis_duration_seconds=elapsed,
            errors=errors,
            warnings=warnings,
        )
        self._save_result_summary(result)
        return result

    def _save_result_summary(self, result: CartographyResult) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        mg = result.module_graph
        lg = result.lineage_graph
        lines = [
            "# Cartography Analysis Summary",
            "",
            f"**Target:** `{result.target_repo}`",
            f"**Generated:** {result.generated_at.isoformat()}",
            f"**Duration:** {result.analysis_duration_seconds:.1f}s",
            "",
            "## Module Graph",
            f"- Modules: {len(mg.nodes)}",
            f"- Import edges: {len(mg.edges)}",
            f"- Architectural hubs: {', '.join(mg.architectural_hubs[:5]) or 'none'}",
            f"- Circular dependencies: {len(mg.circular_dependencies)}",
            f"- High-velocity files: {', '.join(mg.high_velocity_files[:5]) or 'none'}",
            "",
            "## Lineage Graph",
            f"- Datasets: {len(lg.dataset_nodes)}",
            f"- Transformations: {len(lg.transformation_nodes)}",
            f"- Sources: {len(lg.sources)}",
            f"- Sinks: {len(lg.sinks)}",
        ]
        if result.errors:
            lines += ["", "## Errors"] + [f"- {e}" for e in result.errors]
        if result.warnings:
            lines += ["", "## Warnings"] + [f"- {w}" for w in result.warnings]
        (self.output_dir / "analysis_summary.md").write_text("\n".join(lines), encoding="utf-8")

    def _load_cached_result(self) -> CartographyResult:
        mg = ModuleGraph(target_repo=str(self.repo_path))
        lg = DataLineageGraph(target_repo=str(self.repo_path))
        mg_file = self.output_dir / "module_graph.json"
        lg_file = self.output_dir / "lineage_graph.json"
        if mg_file.exists():
            try:
                mg = ModuleGraph.model_validate_json(mg_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"[Orchestrator] Could not load cached module_graph: {e}")
        if lg_file.exists():
            try:
                lg = DataLineageGraph.model_validate_json(lg_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"[Orchestrator] Could not load cached lineage_graph: {e}")
        return CartographyResult(target_repo=str(self.repo_path), module_graph=mg, lineage_graph=lg)