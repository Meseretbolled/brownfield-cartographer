from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class Language(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    YAML = "yaml"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    NOTEBOOK = "notebook"
    UNKNOWN = "unknown"


class StorageType(str, Enum):
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    UNKNOWN = "unknown"


class TransformationType(str, Enum):
    SQL_SELECT = "sql_select"
    PANDAS_READ = "pandas_read"
    PANDAS_WRITE = "pandas_write"
    PYSPARK_READ = "pyspark_read"
    PYSPARK_WRITE = "pyspark_write"
    SQLALCHEMY = "sqlalchemy"
    DBT_MODEL = "dbt_model"
    AIRFLOW_OPERATOR = "airflow_operator"
    UNKNOWN = "unknown"


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class ModuleNode(BaseModel):
    path: str
    language: Language = Language.UNKNOWN
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: float = 0.0
    change_velocity_30d: int = 0
    is_dead_code_candidate: bool = False
    last_modified: Optional[datetime] = None
    loc: int = 0
    comment_ratio: float = 0.0
    imports: list[str] = Field(default_factory=list)
    exported_functions: list[str] = Field(default_factory=list)
    exported_classes: list[str] = Field(default_factory=list)
    pagerank_score: float = 0.0


class DatasetNode(BaseModel):
    name: str
    storage_type: StorageType = StorageType.UNKNOWN
    schema_snapshot: Optional[dict[str, Any]] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    source_file: Optional[str] = None
    description: Optional[str] = None


class FunctionNode(BaseModel):
    qualified_name: str
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False
    line_start: int = 0
    line_end: int = 0


class TransformationNode(BaseModel):
    id: str
    source_datasets: list[str] = Field(default_factory=list)
    target_datasets: list[str] = Field(default_factory=list)
    transformation_type: TransformationType = TransformationType.UNKNOWN
    source_file: str = ""
    line_range: tuple[int, int] = (0, 0)
    sql_query: Optional[str] = None
    description: Optional[str] = None


class Edge(BaseModel):
    source: str
    target: str
    edge_type: EdgeType
    weight: float = 1.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModuleGraph(BaseModel):
    nodes: dict[str, ModuleNode] = Field(default_factory=dict)
    edges: list[Edge] = Field(default_factory=list)
    circular_dependencies: list[list[str]] = Field(default_factory=list)
    high_velocity_files: list[str] = Field(default_factory=list)
    architectural_hubs: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    target_repo: str = ""


class DataLineageGraph(BaseModel):
    dataset_nodes: dict[str, DatasetNode] = Field(default_factory=dict)
    transformation_nodes: dict[str, TransformationNode] = Field(default_factory=dict)
    edges: list[Edge] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    sinks: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    target_repo: str = ""


class CartographyResult(BaseModel):
    target_repo: str
    module_graph: ModuleGraph
    lineage_graph: DataLineageGraph
    analysis_duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)