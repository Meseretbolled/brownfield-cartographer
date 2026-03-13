from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

"""
Pipeline Orchestrator.

Wires:
  Surveyor -> Hydrologist -> Semanticist -> Archivist

Outputs are written under:
  <repo>/.cartography/

Supports:
  - full analysis
  - incremental mode (basic SHA-based skip)
"""

import logging
import subprocess
import time
from pathlib import Path

from src.agents.archivist import Archivist
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.surveyor import Surveyor
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import CartographyResult, DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)

CARTOGRAPHY_DIR = ".cartography"
LAST_RUN_FILE = "last_run_sha.txt"


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _get_current_sha(repo_path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return None
        return result.stdout.strip() or None
    except Exception:
        return None


def _get_changed_files_since(repo_path: Path, since_sha: str) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", since_sha, "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    def __init__(
        self,
        repo_path: Path,
        output_dir: Path | None = None,
        incremental: bool = False,
    ) -> None:
        self.repo_path = repo_path.resolve()
        self.output_dir = (output_dir or (self.repo_path / CARTOGRAPHY_DIR)).resolve()
        self.incremental = incremental
        self.kg = KnowledgeGraph()

    # -----------------------------------------------------------------------
    # Incremental helpers
    # -----------------------------------------------------------------------

    def _load_last_sha(self) -> str | None:
        sha_file = self.output_dir / LAST_RUN_FILE
        if sha_file.exists():
            return sha_file.read_text(encoding="utf-8").strip() or None
        return None

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
            logger.info("[Orchestrator] No previous run found — running full analysis")
            return False

        changed = _get_changed_files_since(self.repo_path, last_sha)
        if not changed:
            logger.info("[Orchestrator] No changes since last run — skipping")
            return True

        logger.info(f"[Orchestrator] {len(changed)} changed file(s) since last run")
        return False

    # -----------------------------------------------------------------------
    # Main run
    # -----------------------------------------------------------------------

    def run(self) -> CartographyResult:
        start = time.time()
        errors: list[str] = []
        warnings: list[str] = []

        if self._should_skip_incremental():
            return self._load_cached_result()

        logger.info(f"[Orchestrator] Starting analysis for {self.repo_path}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        module_graph: ModuleGraph = ModuleGraph(target_repo=str(self.repo_path))
        lineage_graph: DataLineageGraph = DataLineageGraph(target_repo=str(self.repo_path))
        semanticist: Semanticist | None = None

        # -------------------------------------------------------------------
        # Surveyor
        # -------------------------------------------------------------------
        try:
            surveyor = Surveyor(repo_path=self.repo_path, kg=self.kg)
            module_graph = surveyor.run()
            surveyor.save(self.output_dir, module_graph)
        except Exception as e:
            msg = f"Surveyor failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # -------------------------------------------------------------------
        # Hydrologist
        # -------------------------------------------------------------------
        try:
            hydrologist = Hydrologist(repo_path=self.repo_path, kg=self.kg)
            lineage_graph = hydrologist.run()
            hydrologist.save(self.output_dir, lineage_graph)
        except Exception as e:
            msg = f"Hydrologist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # -------------------------------------------------------------------
        # Semanticist
        # -------------------------------------------------------------------
        try:
            semanticist = Semanticist(
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                repo_path=self.repo_path,
            )
            semanticist.run()

            if hasattr(semanticist, "save_trace"):
                semanticist.save_trace(self.output_dir)

            # Save the enriched module graph again after Semanticist mutates it
            (self.output_dir / "module_graph.json").write_text(
                module_graph.model_dump_json(indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            msg = f"Semanticist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # -------------------------------------------------------------------
        # Archivist
        # -------------------------------------------------------------------
        try:
            archivist = Archivist(
                repo_path=self.repo_path,
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                semanticist=semanticist,
            )
            archivist.run(self.output_dir)
        except Exception as e:
            msg = f"Archivist failed: {e}"
            logger.exception(msg)
            errors.append(msg)

        # -------------------------------------------------------------------
        # Visualization
        # -------------------------------------------------------------------
        try:
            self.kg.visualize_module_graph(self.output_dir / "module_graph_networkx.png")
        except Exception as e:
            warnings.append(f"Module graph visualization failed: {e}")
            logger.warning(f"[Orchestrator] Visualization warning: {e}")

        elapsed = time.time() - start
        logger.info(f"[Orchestrator] Analysis complete in {elapsed:.2f}s")

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

    # -----------------------------------------------------------------------
    # Summary / cache
    # -----------------------------------------------------------------------

    def _save_result_summary(self, result: CartographyResult) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        lines = [
            "# Cartography Analysis Summary",
            "",
            f"**Target:** `{result.target_repo}`",
            f"**Generated:** {result.generated_at.isoformat()}",
            f"**Duration:** {result.analysis_duration_seconds:.1f}s",
            "",
            "## Module Graph",
            f"- Modules analysed: {len(result.module_graph.nodes)}",
            f"- Import edges: {len(result.module_graph.edges)}",
            f"- Architectural hubs: {', '.join(result.module_graph.architectural_hubs[:5]) or 'none'}",
            f"- Circular dependencies: {len(result.module_graph.circular_dependencies)}",
            f"- High-velocity files (top 5): {', '.join(result.module_graph.high_velocity_files[:5]) or 'none'}",
            "",
            "## Lineage Graph",
            f"- Datasets: {len(result.lineage_graph.dataset_nodes)}",
            f"- Transformations: {len(result.lineage_graph.transformation_nodes)}",
            f"- Sources (in-degree=0): {len(result.lineage_graph.sources)}",
            f"- Sinks (out-degree=0): {len(result.lineage_graph.sinks)}",
        ]

        if result.errors:
            lines.extend(["", "## Errors"])
            lines.extend(f"- {err}" for err in result.errors)

        if result.warnings:
            lines.extend(["", "## Warnings"])
            lines.extend(f"- {warn}" for warn in result.warnings)

        (self.output_dir / "analysis_summary.md").write_text(
            "\n".join(lines),
            encoding="utf-8",
        )

    def _load_cached_result(self) -> CartographyResult:
        module_graph = ModuleGraph(target_repo=str(self.repo_path))
        lineage_graph = DataLineageGraph(target_repo=str(self.repo_path))

        mg_file = self.output_dir / "module_graph.json"
        lg_file = self.output_dir / "lineage_graph.json"

        if mg_file.exists():
            try:
                module_graph = ModuleGraph.model_validate_json(
                    mg_file.read_text(encoding="utf-8")
                )
            except Exception as e:
                logger.warning(f"[Orchestrator] Could not load cached module_graph: {e}")

        if lg_file.exists():
            try:
                lineage_graph = DataLineageGraph.model_validate_json(
                    lg_file.read_text(encoding="utf-8")
                )
            except Exception as e:
                logger.warning(f"[Orchestrator] Could not load cached lineage_graph: {e}")

        return CartographyResult(
            target_repo=str(self.repo_path),
            module_graph=module_graph,
            lineage_graph=lineage_graph,
        )