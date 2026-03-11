from __future__ import annotations
import logging
from pathlib import Path
import sqlglot
import sqlglot.expressions as exp
from src.models import DatasetNode, StorageType, TransformationNode, TransformationType

logger = logging.getLogger(__name__)


def _extract_tables(sql: str, dialect: str = "duckdb") -> tuple[list[str], list[str]]:
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except Exception as e:
        logger.warning(f"Failed to parse SQL: {e}")
        return [], []

    sources: list[str] = []
    targets: list[str] = []
    cte_names: set[str] = set()

    for stmt in statements:
        if stmt is None:
            continue
        for cte in stmt.find_all(exp.CTE):
            cte_names.add(cte.alias.lower())
        for table in stmt.find_all(exp.Table):
            name = table.name.lower()
            if name and name not in cte_names:
                sources.append(name)
        if isinstance(stmt, (exp.Create, exp.Insert)):
            tgt = stmt.find(exp.Table)
            if tgt:
                targets.append(tgt.name.lower())

    return list(set(sources) - set(targets)), targets


def _is_dbt_model(path: Path) -> bool:
    return "models" in path.parts and path.suffix == ".sql"


def _extract_dbt_ref_tables(sql: str) -> list[str]:
    import re
    return re.findall(r"ref\(['\"](\w+)['\"]\)", sql)


def analyze_sql_file(path: Path) -> TransformationNode | None:
    try:
        sql = path.read_text(encoding="utf-8", errors="replace")
        is_dbt = _is_dbt_model(path)
        if is_dbt:
            ref_tables = _extract_dbt_ref_tables(sql)
            clean_sql = __import__("re").sub(r"ref\(['\"](\w+)['\"]\)", r"\1", sql)
            sources, targets = _extract_tables(clean_sql)
            sources = list(set(sources + ref_tables))
            target_name = path.stem.lower()
            targets = [target_name]
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
        if any(p.startswith(".") for p in path.parts):
            continue
        node = analyze_sql_file(path)
        if not node:
            continue
        transformations.append(node)
        for name in node.source_datasets + node.target_datasets:
            if name not in datasets:
                datasets[name] = DatasetNode(
                    name=name,
                    storage_type=StorageType.TABLE,
                    source_file=str(path),
                )

    return list(datasets.values()), transformations