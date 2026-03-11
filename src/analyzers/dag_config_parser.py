from __future__ import annotations
import logging
from pathlib import Path
import yaml
from src.models import DatasetNode, StorageType, TransformationNode, TransformationType

logger = logging.getLogger(__name__)


def _parse_dbt_schema(path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: list[DatasetNode] = []
    transformations: list[TransformationNode] = []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return [], []
        for source in data.get("sources", []):
            for table in source.get("tables", []):
                datasets.append(DatasetNode(
                    name=table.get("name", "").lower(),
                    storage_type=StorageType.TABLE,
                    owner=source.get("name"),
                    is_source_of_truth=True,
                    source_file=str(path),
                    description=table.get("description"),
                ))
        for model in data.get("models", []):
            name = model.get("name", "").lower()
            if name:
                datasets.append(DatasetNode(
                    name=name,
                    storage_type=StorageType.TABLE,
                    source_file=str(path),
                    description=model.get("description"),
                ))
    except Exception as e:
        logger.warning(f"Failed to parse dbt schema {path}: {e}")
    return datasets, transformations


def _parse_airflow_dag(path: Path) -> list[TransformationNode]:
    transformations: list[TransformationNode] = []
    try:
        source = path.read_text(encoding="utf-8")
        import re
        dag_ids = re.findall(r'dag_id\s*=\s*[\'"]([^\'"]+)[\'"]', source)
        task_ids = re.findall(r'task_id\s*=\s*[\'"]([^\'"]+)[\'"]', source)
        for dag_id in dag_ids:
            transformations.append(TransformationNode(
                id=f"airflow::{dag_id}",
                transformation_type=TransformationType.AIRFLOW_OPERATOR,
                source_file=str(path),
                line_range=(1, len(source.splitlines())),
                description=f"Airflow DAG: {dag_id} with tasks: {', '.join(task_ids[:5])}",
            ))
    except Exception as e:
        logger.warning(f"Failed to parse Airflow DAG {path}: {e}")
    return transformations


def _parse_dbt_project(path: Path) -> list[DatasetNode]:
    datasets: list[DatasetNode] = []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return []
        project_name = data.get("name", "")
        if project_name:
            datasets.append(DatasetNode(
                name=project_name,
                storage_type=StorageType.UNKNOWN,
                source_file=str(path),
                description=f"dbt project: {project_name}",
            ))
    except Exception as e:
        logger.warning(f"Failed to parse dbt_project.yml {path}: {e}")
    return datasets


def analyze_configs(repo_path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: list[DatasetNode] = []
    transformations: list[TransformationNode] = []

    for path in repo_path.rglob("*.yml"):
        if any(p.startswith(".") for p in path.parts):
            continue
        if path.name == "dbt_project.yml":
            datasets.extend(_parse_dbt_project(path))
        elif path.name in {"schema.yml", "sources.yml"}:
            d, t = _parse_dbt_schema(path)
            datasets.extend(d)
            transformations.extend(t)

    for path in repo_path.rglob("*.yaml"):
        if any(p.startswith(".") for p in path.parts):
            continue
        if path.name == "dbt_project.yaml":
            datasets.extend(_parse_dbt_project(path))
        elif path.name in {"schema.yaml", "sources.yaml"}:
            d, t = _parse_dbt_schema(path)
            datasets.extend(d)
            transformations.extend(t)

    for path in repo_path.rglob("*.py"):
        if any(p.startswith(".") for p in path.parts):
            continue
        if "dag" in path.name.lower():
            transformations.extend(_parse_airflow_dag(path))

    return datasets, transformations