from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from src.models import DatasetNode, StorageType, TransformationNode, TransformationType

logger = logging.getLogger(__name__)

SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages"}


def _should_skip(path: Path) -> bool:
    return any(p.startswith(".") or p in SKIP_DIRS for p in path.parts)


def _parse_dbt_schema(path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: list[DatasetNode] = []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return [], []
        for source in data.get("sources", []):
            for table in source.get("tables", []):
                name = table.get("name", "").lower()
                if name:
                    datasets.append(DatasetNode(
                        name=name,
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
    return datasets, []


def _parse_dbt_project(path: Path) -> list[DatasetNode]:
    datasets: list[DatasetNode] = []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            name = data.get("name", "")
            if name:
                datasets.append(DatasetNode(
                    name=name,
                    storage_type=StorageType.UNKNOWN,
                    source_file=str(path),
                    description=f"dbt project: {name}",
                ))
    except Exception as e:
        logger.warning(f"Failed to parse dbt_project.yml {path}: {e}")
    return datasets


def _parse_airflow_dag(path: Path) -> list[TransformationNode]:
    transforms: list[TransformationNode] = []
    try:
        source = path.read_text(encoding="utf-8", errors="replace")
        dag_ids  = re.findall(r'dag_id\s*=\s*[\'"]([^\'"]+)[\'"]', source)
        task_ids = re.findall(r'task_id\s*=\s*[\'"]([^\'"]+)[\'"]', source)
        for dag_id in dag_ids:
            transforms.append(TransformationNode(
                id=f"airflow::{dag_id}",
                transformation_type=TransformationType.AIRFLOW_OPERATOR,
                source_file=str(path),
                line_range=(1, len(source.splitlines())),
                source_datasets=[],
                target_datasets=[],
                sql_query=None,
            ))
            logger.debug(f"Airflow DAG: {dag_id} tasks={task_ids[:5]}")
    except Exception as e:
        logger.warning(f"Failed to parse Airflow DAG {path}: {e}")
    return transforms


def _parse_dagster_definitions(path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: list[DatasetNode] = []
    transforms: list[TransformationNode] = []
    try:
        source = path.read_text(encoding="utf-8", errors="replace")

        asset_names = re.findall(r'@(?:asset|multi_asset)\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)', source)
        job_names   = re.findall(r'@job\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)', source)
        op_names    = re.findall(r'@op\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)', source)
        resource_names = re.findall(r'@resource\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)', source)

        for name in asset_names:
            datasets.append(DatasetNode(
                name=name,
                storage_type=StorageType.TABLE,
                source_file=str(path),
                description=f"Dagster asset: {name}",
            ))

        all_ops = job_names + op_names
        if all_ops:
            transforms.append(TransformationNode(
                id=f"dagster::{path.stem}",
                transformation_type=TransformationType.UNKNOWN,
                source_file=str(path),
                line_range=(1, len(source.splitlines())),
                source_datasets=[],
                target_datasets=asset_names,
                sql_query=None,
            ))

        partitioned = re.findall(r'PartitionedConfig|DailyPartitionsDefinition|MonthlyPartitionsDefinition', source)
        if partitioned:
            logger.debug(f"Dagster partitioned assets in {path.name}")

    except Exception as e:
        logger.warning(f"Failed to parse Dagster file {path}: {e}")
    return datasets, transforms


def _parse_prefect_flow(path: Path) -> list[TransformationNode]:
    transforms: list[TransformationNode] = []
    try:
        source = path.read_text(encoding="utf-8", errors="replace")
        flow_names = re.findall(r'@flow\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)', source)
        for name in flow_names:
            transforms.append(TransformationNode(
                id=f"prefect::{name}",
                transformation_type=TransformationType.UNKNOWN,
                source_file=str(path),
                line_range=(1, len(source.splitlines())),
                source_datasets=[],
                target_datasets=[],
            ))
    except Exception as e:
        logger.warning(f"Failed to parse Prefect flow {path}: {e}")
    return transforms


def analyze_configs(repo_path: Path) -> tuple[list[DatasetNode], list[TransformationNode]]:
    datasets: list[DatasetNode] = []
    transforms: list[TransformationNode] = []

    for ext in ("*.yml", "*.yaml"):
        for path in repo_path.rglob(ext):
            if _should_skip(path):
                continue
            if path.name in ("dbt_project.yml", "dbt_project.yaml"):
                datasets.extend(_parse_dbt_project(path))
            elif path.name in ("schema.yml", "schema.yaml", "sources.yml", "sources.yaml"):
                d, t = _parse_dbt_schema(path)
                datasets.extend(d)
                transforms.extend(t)

    for path in repo_path.rglob("*.py"):
        if _should_skip(path):
            continue
        name_lower = path.name.lower()
        source = ""
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        if "dag" in name_lower or "airflow" in name_lower:
            transforms.extend(_parse_airflow_dag(path))

        if (
            "@asset" in source or "@multi_asset" in source
            or "@job" in source or "@op" in source
            or "dagster" in source.lower()
        ):
            d, t = _parse_dagster_definitions(path)
            datasets.extend(d)
            transforms.extend(t)

        if "@flow" in source or "prefect" in source.lower():
            transforms.extend(_parse_prefect_flow(path))

    seen_ds: set[str] = set()
    seen_tr: set[str] = set()
    unique_ds = [d for d in datasets if not (d.name in seen_ds or seen_ds.add(d.name))]
    unique_tr = [t for t in transforms if not (t.id in seen_tr or seen_tr.add(t.id))]

    logger.info(
        f"[dag_config_parser] {len(unique_ds)} datasets, "
        f"{len(unique_tr)} transformations from configs"
    )
    return unique_ds, unique_tr