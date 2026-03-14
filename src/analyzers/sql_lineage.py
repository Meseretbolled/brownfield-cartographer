from __future__ import annotations

import logging
import re
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp

from src.models import DatasetNode, StorageType, TransformationNode, TransformationType

logger = logging.getLogger(__name__)

SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages"}
DBT_REF_RE = re.compile(r"""ref\s*\(\s*['"](\w+)['"]\s*\)""", re.IGNORECASE)
DIALECTS = ["duckdb", "bigquery", "snowflake", "postgres"]


def _should_skip(path: Path) -> bool:
    return any(p.startswith(".") or p in SKIP_DIRS for p in path.parts)


def _extract_tables(sql: str) -> tuple[list[str], list[str]]:
    sources: list[str] = []
    targets: list[str] = []
    cte_names: set[str] = set()

    for dialect in DIALECTS:
        try:
            statements = sqlglot.parse(sql, dialect=dialect)
            if not statements:
                continue
            for stmt in statements:
                if stmt is None:
                    continue
                # Extract CTE names — these are intermediate, not real sources
                for cte in stmt.find_all(exp.CTE):
                    cte_names.add(cte.alias.lower())
                # Extract all table references (SELECT/FROM/JOIN)
                for table in stmt.find_all(exp.Table):
                    name = table.name.lower()
                    if name and name not in cte_names:
                        sources.append(name)
                # Extract explicit write targets (CREATE TABLE / INSERT INTO)
                if isinstance(stmt, (exp.Create, exp.Insert)):
                    tgt = stmt.find(exp.Table)
                    if tgt:
                        targets.append(tgt.name.lower())
                # Also detect explicit JOIN targets as sources
                for join in stmt.find_all(exp.Join):
                    joined = join.find(exp.Table)
                    if joined:
                        name = joined.name.lower()
                        if name and name not in cte_names:
                            sources.append(name)
            break
        except Exception:
            continue

    final_sources = list(set(sources) - set(targets))
    return final_sources, targets


def _is_dbt_model(path: Path) -> bool:
    return "models" in path.parts and path.suffix == ".sql"


def analyze_sql_file(path: Path) -> TransformationNode | None:
    try:
        sql = path.read_text(encoding="utf-8", errors="replace")
        is_dbt = _is_dbt_model(path)

        if is_dbt:
            ref_tables = DBT_REF_RE.findall(sql)
            clean_sql = DBT_REF_RE.sub(lambda m: m.group(1), sql)
            sources, targets = _extract_tables(clean_sql)
            sources = list(set(sources + ref_tables))
            targets = [path.stem.lower()]
        else:
            sources, targets = _extract_tables(sql)

        if not sources and not targets:
            return None

        return TransformationNode(
            id=str(path),
            source_datasets=sources,
            target_datasets=targets,
            transformation_type=TransformationType.DBT_MODEL if is_dbt else TransformationType.SQL_SELECT,
            source_file=str(path),
            line_range=(1, len(sql.splitlines())),
            sql_query=sql[:500],
        )
    except Exception as e:
        logger.warning(f"Failed to analyze SQL file {path}: {e}")
        return None


def analyze_sql_directory(repo_path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: dict[str, DatasetNode] = {}
    transformations: list[TransformationNode] = []

    for path in repo_path.rglob("*.sql"):
        if _should_skip(path):
            continue
        node = analyze_sql_file(path)
        if not node:
            continue
        transformations.append(node)
        for name in node.source_datasets + node.target_datasets:
            if name and name not in datasets:
                datasets[name] = DatasetNode(
                    name=name,
                    storage_type=StorageType.TABLE,
                    source_file=str(path),
                )

    logger.info(
        f"[sql_lineage] {len(datasets)} datasets, "
        f"{len(transformations)} transformations from SQL"
    )
    return list(datasets.values()), transformations