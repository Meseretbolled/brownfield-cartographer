from __future__ import annotations

import logging
import re
from collections import deque
from datetime import datetime
from pathlib import Path

import tree_sitter_javascript as tsjavascript
import tree_sitter_python as tspython
import tree_sitter_yaml as tsyaml
from tree_sitter import Language, Node, Parser

from src.models import ModuleNode
from src.models import Language as Lang

logger = logging.getLogger(__name__)

try:
    import tree_sitter_sql as tssql
    SQL_LANGUAGE = Language(tssql.language())
    HAS_SQL = True
except Exception:
    SQL_LANGUAGE = None
    HAS_SQL = False
    logger.debug("tree_sitter_sql not installed — SQL AST uses regex fallback")

PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
YAML_LANGUAGE = Language(tsyaml.language())

EXTENSION_MAP: dict[str, tuple[Language | None, Lang]] = {
    ".py":   (PY_LANGUAGE,   Lang.PYTHON),
    ".js":   (JS_LANGUAGE,   Lang.JAVASCRIPT),
    ".ts":   (JS_LANGUAGE,   Lang.TYPESCRIPT),
    ".yaml": (YAML_LANGUAGE, Lang.YAML),
    ".yml":  (YAML_LANGUAGE, Lang.YAML),
    ".sql":  (SQL_LANGUAGE,  Lang.SQL),
}

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    "dist", "build", ".mypy_cache", ".pytest_cache", ".tox",
    "site-packages", ".eggs", ".cache",
}

DBT_REF_RE    = re.compile(r"""ref\s*\(\s*['"]([^'"]+)['"]\s*\)""", re.IGNORECASE)
DBT_SOURCE_RE = re.compile(r"""source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""", re.IGNORECASE)
CREATE_RE     = re.compile(r"""(?i)\b(create\s+or\s+replace\s+table|create\s+table|create\s+view)\s+([a-zA-Z0-9_."]+)""")


def _walk(node: Node):
    stack = deque([node])
    while stack:
        current = stack.popleft()
        yield current
        stack.extend(current.children)


def _compute_complexity(root: Node, lang: Lang) -> float:
    if lang == Lang.PYTHON:
        targets = {
            "if_statement", "for_statement", "while_statement",
            "try_statement", "except_clause", "with_statement",
            "conditional_expression",
        }
    elif lang == Lang.SQL:
        targets = {
            "select_statement", "join_clause", "where_clause",
            "group_by_clause", "case_expression", "cte",
        }
    else:
        targets = {"if_statement", "for_statement", "while_statement"}
    return float(sum(1 for n in _walk(root) if n.type in targets))


def _comment_ratio(source: bytes, lang: Lang) -> float:
    lines = source.decode("utf-8", errors="replace").splitlines()
    if not lines:
        return 0.0
    if lang in (Lang.PYTHON, Lang.YAML):
        count = sum(1 for l in lines if l.strip().startswith("#"))
    elif lang == Lang.SQL:
        count = sum(1 for l in lines if l.strip().startswith("--") or l.strip().startswith("{#"))
    else:
        count = sum(1 for l in lines if l.strip().startswith("//") or l.strip().startswith("/*"))
    return count / len(lines)


def _python_imports(root: Node) -> list[str]:
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


def _python_functions(root: Node) -> list[str]:
    fns: list[str] = []
    for node in _walk(root):
        if node.type == "function_definition":
            for child in node.children:
                if child.type == "identifier":
                    name = child.text.decode("utf-8")
                    if not name.startswith("_"):
                        fns.append(name)
                    break
    return fns


def _python_classes(root: Node) -> list[str]:
    cls: list[str] = []
    for node in _walk(root):
        if node.type == "class_definition":
            for child in node.children:
                if child.type == "identifier":
                    cls.append(child.text.decode("utf-8"))
                    break
    return cls


def _sql_imports(source: bytes) -> list[str]:
    text = source.decode("utf-8", errors="replace")
    imports: list[str] = []
    for m in DBT_REF_RE.findall(text):
        imports.append(f"dbt_ref:{m}")
    for src, tbl in DBT_SOURCE_RE.findall(text):
        imports.append(f"dbt_source:{src}.{tbl}")
    return sorted(set(imports))


def _sql_exports(path: Path, source: bytes) -> list[str]:
    exports = [path.stem]
    text = source.decode("utf-8", errors="replace")
    for _, name in CREATE_RE.findall(text):
        exports.append(name.strip('"').split(".")[-1])
    return sorted(set(exports))


def analyze_module(path: Path) -> ModuleNode | None:
    entry = EXTENSION_MAP.get(path.suffix.lower())
    if not entry:
        return None
    ts_lang, lang = entry

    try:
        source = path.read_bytes()
        lines = source.decode("utf-8", errors="replace").splitlines()
        imports: list[str] = []
        functions: list[str] = []
        classes: list[str] = []
        complexity = 0.0

        if ts_lang is not None:
            parser = Parser(ts_lang)
            root = parser.parse(source).root_node
            complexity = _compute_complexity(root, lang)
            if lang == Lang.PYTHON:
                imports   = _python_imports(root)
                functions = _python_functions(root)
                classes   = _python_classes(root)
            elif lang == Lang.SQL:
                imports   = _sql_imports(source)
                functions = _sql_exports(path, source)
        else:
            if lang == Lang.SQL:
                imports   = _sql_imports(source)
                functions = _sql_exports(path, source)
                complexity = float(len(re.findall(
                    r"(?i)\b(select|join|case|with)\b",
                    source.decode("utf-8", errors="replace"),
                )))

        return ModuleNode(
            path=str(path),
            language=lang,
            loc=len(lines),
            complexity_score=complexity,
            comment_ratio=_comment_ratio(source, lang),
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

    for file_path in repo_path.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in extensions:
            continue
        if any(p.startswith(".") or p in SKIP_DIRS for p in file_path.parts):
            continue
        node = analyze_module(file_path)
        if node:
            nodes.append(node)

    logger.info(f"[tree_sitter_analyzer] {len(nodes)} modules parsed from {repo_path.name}")
    return nodes