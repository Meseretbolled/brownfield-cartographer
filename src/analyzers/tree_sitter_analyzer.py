from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

import tree_sitter_javascript as tsjavascript
import tree_sitter_python as tspython
import tree_sitter_yaml as tsyaml
from tree_sitter import Language, Node, Parser

from src.models import ModuleNode
from src.models import Language as Lang

logger = logging.getLogger(__name__)

# Optional SQL parser
try:
    import tree_sitter_sql as tssql  # type: ignore
    SQL_LANGUAGE = Language(tssql.language())
    HAS_SQL = True
except Exception:
    SQL_LANGUAGE = None
    HAS_SQL = False
    logger.warning("tree_sitter_sql not installed; SQL parsing will use regex fallback")

PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
YAML_LANGUAGE = Language(tsyaml.language())

EXTENSION_MAP: dict[str, tuple[Language | None, Lang]] = {
    ".py": (PY_LANGUAGE, Lang.PYTHON),
    ".js": (JS_LANGUAGE, Lang.JAVASCRIPT),
    ".ts": (JS_LANGUAGE, Lang.TYPESCRIPT),
    ".yaml": (YAML_LANGUAGE, Lang.YAML),
    ".yml": (YAML_LANGUAGE, Lang.YAML),
    ".sql": (SQL_LANGUAGE, Lang.SQL),
}


# -----------------------------------------------------------------------------
# Parser selection
# -----------------------------------------------------------------------------

def _get_parser(path: Path) -> tuple[Parser | None, Lang] | None:
    entry = EXTENSION_MAP.get(path.suffix.lower())
    if not entry:
        return None

    ts_lang, model_lang = entry
    if ts_lang is None:
        return None, model_lang

    parser = Parser(ts_lang)
    return parser, model_lang


# -----------------------------------------------------------------------------
# Generic tree helpers
# -----------------------------------------------------------------------------

def _walk(node: Node):
    yield node
    for child in node.children:
        yield from _walk(child)


def _compute_complexity(root: Node, lang: Lang) -> float:
    if lang == Lang.PYTHON:
        complexity_nodes = {
            "if_statement", "for_statement", "while_statement",
            "try_statement", "except_clause", "with_statement",
            "conditional_expression",
        }
    elif lang == Lang.SQL:
        complexity_nodes = {
            "select_statement", "join_clause", "where_clause",
            "group_by_clause", "order_by_clause", "case_expression",
            "cte",
        }
    else:
        complexity_nodes = {
            "if_statement", "for_statement", "while_statement",
        }

    count = sum(1 for node in _walk(root) if node.type in complexity_nodes)
    return float(count)


def _compute_comment_ratio(source: bytes, lang: Lang) -> float:
    text = source.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not lines:
        return 0.0

    if lang == Lang.PYTHON or lang == Lang.YAML:
        comment_lines = sum(1 for l in lines if l.strip().startswith("#"))
    elif lang == Lang.SQL:
        comment_lines = sum(
            1 for l in lines
            if l.strip().startswith("--") or l.strip().startswith("{#")
        )
    else:
        comment_lines = sum(
            1 for l in lines
            if l.strip().startswith("//") or l.strip().startswith("/*")
        )

    return comment_lines / len(lines)


# -----------------------------------------------------------------------------
# Python extraction
# -----------------------------------------------------------------------------

def _extract_python_imports(root: Node) -> list[str]:
    imports: list[str] = []
    for node in _walk(root):
        if node.type == "import_statement":
            for child in node.children:
                if child.type == "dotted_name":
                    imports.append(child.text.decode("utf-8"))
        elif node.type == "import_from_statement":
            for child in node.children:
                if child.type == "dotted_name":
                    imports.append(child.text.decode("utf-8"))
                    break
    return imports


def _extract_python_functions(root: Node) -> list[str]:
    functions: list[str] = []
    for node in _walk(root):
        if node.type == "function_definition":
            for child in node.children:
                if child.type == "identifier":
                    name = child.text.decode("utf-8")
                    if not name.startswith("_"):
                        functions.append(name)
                    break
    return functions


def _extract_python_classes(root: Node) -> list[str]:
    classes: list[str] = []
    for node in _walk(root):
        if node.type == "class_definition":
            for child in node.children:
                if child.type == "identifier":
                    classes.append(child.text.decode("utf-8"))
                    break
    return classes


# -----------------------------------------------------------------------------
# SQL / dbt extraction
# -----------------------------------------------------------------------------

DBT_REF_RE = re.compile(r"""ref\s*\(\s*['"]([^'"]+)['"]\s*\)""", re.IGNORECASE)
DBT_SOURCE_RE = re.compile(
    r"""source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""",
    re.IGNORECASE,
)

CREATE_MODEL_RE = re.compile(
    r"""(?i)\b(create\s+or\s+replace\s+table|create\s+table|create\s+view)\s+([a-zA-Z0-9_."]+)"""
)


def _extract_sql_imports_from_text(source: bytes) -> list[str]:
    text = source.decode("utf-8", errors="replace")
    imports: list[str] = []

    for match in DBT_REF_RE.findall(text):
        imports.append(f"dbt_ref:{match}")

    for src_name, table_name in DBT_SOURCE_RE.findall(text):
        imports.append(f"dbt_source:{src_name}.{table_name}")

    return sorted(set(imports))


def _extract_sql_exports(path: Path, source: bytes) -> list[str]:
    """
    For dbt SQL models, export the model name from the filename.
    Example: models/staging/stg_orders.sql -> stg_orders
    """
    exports = [path.stem]

    text = source.decode("utf-8", errors="replace")
    for _, obj_name in CREATE_MODEL_RE.findall(text):
        clean = obj_name.strip('"')
        exports.append(clean.split(".")[-1])

    return sorted(set(exports))


# -----------------------------------------------------------------------------
# Main analysis
# -----------------------------------------------------------------------------

def analyze_module(path: Path) -> ModuleNode | None:
    result = _get_parser(path)
    if not result:
        return None

    parser, lang = result

    try:
        source = path.read_bytes()
        lines = source.decode("utf-8", errors="replace").splitlines()

        imports: list[str] = []
        functions: list[str] = []
        classes: list[str] = []
        complexity_score = 0.0

        if parser is not None:
            tree = parser.parse(source)
            root = tree.root_node
            complexity_score = _compute_complexity(root, lang)

            if lang == Lang.PYTHON:
                imports = _extract_python_imports(root)
                functions = _extract_python_functions(root)
                classes = _extract_python_classes(root)

            elif lang == Lang.SQL:
                imports = _extract_sql_imports_from_text(source)
                functions = _extract_sql_exports(path, source)

        else:
            # Fallback path, mainly for SQL if tree_sitter_sql is absent
            if lang == Lang.SQL:
                imports = _extract_sql_imports_from_text(source)
                functions = _extract_sql_exports(path, source)
                complexity_score = float(
                    len(re.findall(r"(?i)\b(select|join|case|with)\b", source.decode("utf-8", errors="replace")))
                )

        return ModuleNode(
            path=str(path),
            language=lang,
            loc=len(lines),
            complexity_score=complexity_score,
            comment_ratio=_compute_comment_ratio(source, lang),
            imports=imports,
            exported_functions=functions,
            exported_classes=classes,
            last_modified=datetime.fromtimestamp(path.stat().st_mtime),
        )

    except Exception as e:
        logger.warning(f"Failed to analyze {path}: {e}")
        return None


def analyze_directory(repo_path: Path) -> list[ModuleNode]:
    nodes: list[ModuleNode] = []
    extensions = set(EXTENSION_MAP.keys())

    skip_dirs = {
        ".git",
        "node_modules",
        "__pycache__",
        ".venv",
        "venv",
        "dist",
        "build",
        ".mypy_cache",
        ".pytest_cache",
        ".next",
        ".tox",
        "site-packages",
    }

    for file_path in repo_path.rglob("*"):
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in extensions:
            continue

        if any(part.startswith(".") or part in skip_dirs for part in file_path.parts):
            continue

        node = analyze_module(file_path)
        if node:
            nodes.append(node)

    logger.info(f"[tree_sitter_analyzer] Returning {len(nodes)} module nodes")
    return nodes