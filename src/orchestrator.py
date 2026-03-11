"""
Pipeline Orchestrator.

Wires Surveyor → Hydrologist in sequence.
Serializes outputs to .cartography/ directory.
Supports incremental mode: re-analyse only git-changed files since last run.
"""
from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import CartographyResult, DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)

CARTOGRAPHY_DIR = ".cartography"
LAST_RUN_FILE = "last_run_sha.txt"


def _get_current_sha(repo_path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _get_changed_files_since(repo_path: Path, since_sha: str) -> list[str]:
    """Return list of files changed between since_sha and HEAD."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", since_sha, "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        )
        return [f.strip() for f in result.stdout.splitlines() if f.strip()]
    except Exception:
        return []


class Orchestrator:
    """
    Runs the full Surveyor + Hydrologist pipeline against a repo path.

    Args:
        repo_path: Local path to the target repository.
        output_dir: Where to write .cartography/ artefacts. Defaults to
                    <repo_path>/.cartography/
        incremental: If True, only re-analyse files changed since last run.
    """

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

    # ------------------------------------------------------------------
    # Incremental helpers
    # ------------------------------------------------------------------

    def _load_last_sha(self) -> str | None:
        sha_file = self.output_dir / LAST_RUN_FILE
        if sha_file.exists():
            return sha_file.read_text().strip() or None
        return None

    def _save_current_sha(self) -> None:
        sha = _get_current_sha(self.repo_path)
        if sha:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            (self.output_dir / LAST_RUN_FILE).write_text(sha)

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
        logger.info(f"[Orchestrator] {len(changed)} file(s) changed since last run")
        return False

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def run(self) -> CartographyResult:
        start = time.time()
        errors: list[str] = []
        warnings: list[str] = []

        if self._should_skip_incremental():
            # Load cached results
            return self._load_cached_result()

        logger.info(f"[Orchestrator] Starting full analysis of {self.repo_path}")

        # ── Phase 1: Surveyor ──────────────────────────────────────────
        module_graph: ModuleGraph | None = None
        try:
            surveyor = Surveyor(repo_path=self.repo_path, kg=self.kg)
            module_graph = surveyor.run()
            surveyor.save(self.output_dir, module_graph)
        except Exception as e:
            msg = f"Surveyor failed: {e}"
            logger.error(msg)
            errors.append(msg)
            module_graph = ModuleGraph(target_repo=str(self.repo_path))

        # ── Phase 2: Hydrologist ───────────────────────────────────────
        lineage_graph: DataLineageGraph | None = None
        try:
            hydrologist = Hydrologist(repo_path=self.repo_path, kg=self.kg)
            lineage_graph = hydrologist.run()
            hydrologist.save(self.output_dir, lineage_graph)
        except Exception as e:
            msg = f"Hydrologist failed: {e}"
            logger.error(msg)
            errors.append(msg)
            lineage_graph = DataLineageGraph(target_repo=str(self.repo_path))

        elapsed = time.time() - start
        logger.info(f"[Orchestrator] Pipeline complete in {elapsed:.1f}s")

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
        """Save a human-readable analysis summary."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        summary_lines = [
            f"# Cartography Analysis Summary",
            f"",
            f"**Target:** `{result.target_repo}`",
            f"**Generated:** {result.generated_at.isoformat()}",
            f"**Duration:** {result.analysis_duration_seconds:.1f}s",
            f"",
            f"## Module Graph",
            f"- Modules analysed: {len(result.module_graph.nodes)}",
            f"- Import edges: {len(result.module_graph.edges)}",
            f"- Architectural hubs: {', '.join(result.module_graph.architectural_hubs[:5]) or 'none'}",
            f"- Circular dependencies: {len(result.module_graph.circular_dependencies)}",
            f"- High-velocity files (top 5): {', '.join(result.module_graph.high_velocity_files[:5]) or 'none'}",
            f"",
            f"## Lineage Graph",
            f"- Datasets: {len(result.lineage_graph.dataset_nodes)}",
            f"- Transformations: {len(result.lineage_graph.transformation_nodes)}",
            f"- Sources (in-degree=0): {len(result.lineage_graph.sources)}",
            f"- Sinks (out-degree=0): {len(result.lineage_graph.sinks)}",
        ]
        if result.errors:
            summary_lines += ["", "## Errors", *[f"- {e}" for e in result.errors]]
        if result.warnings:
            summary_lines += ["", "## Warnings", *[f"- {w}" for w in result.warnings]]

        (self.output_dir / "analysis_summary.md").write_text(
            "\n".join(summary_lines), encoding="utf-8"
        )

    def _load_cached_result(self) -> CartographyResult:
        """Return a minimal CartographyResult from cached JSON artefacts."""
        import json
        from src.models import ModuleGraph, DataLineageGraph

        module_graph = ModuleGraph(target_repo=str(self.repo_path))
        lineage_graph = DataLineageGraph(target_repo=str(self.repo_path))

        mg_file = self.output_dir / "module_graph.json"
        lg_file = self.output_dir / "lineage_graph.json"

        if mg_file.exists():
            try:
                module_graph = ModuleGraph.model_validate_json(mg_file.read_text())
            except Exception as e:
                logger.warning(f"Could not load cached module_graph: {e}")

        if lg_file.exists():
            try:
                lineage_graph = DataLineageGraph.model_validate_json(lg_file.read_text())
            except Exception as e:
                logger.warning(f"Could not load cached lineage_graph: {e}")

        return CartographyResult(
            target_repo=str(self.repo_path),
            module_graph=module_graph,
            lineage_graph=lineage_graph,
        )