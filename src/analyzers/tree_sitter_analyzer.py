from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime
import tree_sitter_python as tspython
import tree_sitter_javascript as tsjavascript
import tree_sitter_yaml as tsyaml
from tree_sitter import Language, Parser, Node
from src.models import ModuleNode
from src.models import Language as Lang

logger = logging.getLogger(__name__)

PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
YAML_LANGUAGE = Language(tsyaml.language())

EXTENSION_MAP = {
    ".py": (PY_LANGUAGE, Lang.PYTHON),
    ".js": (JS_LANGUAGE, Lang.JAVASCRIPT),
    ".ts": (JS_LANGUAGE, Lang.TYPESCRIPT),
    ".yaml": (YAML_LANGUAGE, Lang.YAML),
    ".yml": (YAML_LANGUAGE, Lang.YAML),
}


def _get_parser(path: Path) -> tuple[Parser, Lang] | None:
    entry = EXTENSION_MAP.get(path.suffix.lower())
    if not entry:
        return None
    ts_lang, model_lang = entry
    parser = Parser(ts_lang)
    return parser, model_lang


def _extract_python_imports(root: Node, source: bytes) -> list[str]:
    imports = []
    for node in root.children:
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
    functions = []
    for node in root.children:
        if node.type == "function_definition":
            for child in node.children:
                if child.type == "identifier":
                    name = child.text.decode("utf-8")
                    if not name.startswith("_"):
                        functions.append(name)
                    break
    return functions


def _extract_python_classes(root: Node) -> list[str]:
    classes = []
    for node in root.children:
        if node.type == "class_definition":
            for child in node.children:
                if child.type == "identifier":
                    classes.append(child.text.decode("utf-8"))
                    break
    return classes


def _compute_complexity(root: Node) -> float:
    complexity_nodes = {
        "if_statement", "for_statement", "while_statement",
        "try_statement", "except_clause", "with_statement",
    }
    count = sum(1 for node in root.children if node.type in complexity_nodes)
    return float(count)


def _compute_comment_ratio(source: bytes) -> float:
    lines = source.decode("utf-8", errors="replace").splitlines()
    if not lines:
        return 0.0
    comment_lines = sum(1 for l in lines if l.strip().startswith("#"))
    return comment_lines / len(lines)


def analyze_module(path: Path) -> ModuleNode | None:
    result = _get_parser(path)
    if not result:
        return None
    parser, lang = result
    try:
        source = path.read_bytes()
        tree = parser.parse(source)
        root = tree.root_node
        imports: list[str] = []
        functions: list[str] = []
        classes: list[str] = []
        if lang == Lang.PYTHON:
            imports = _extract_python_imports(root, source)
            functions = _extract_python_functions(root)
            classes = _extract_python_classes(root)
        lines = source.decode("utf-8", errors="replace").splitlines()
        return ModuleNode(
            path=str(path),
            language=lang,
            loc=len(lines),
            complexity_score=_compute_complexity(root),
            comment_ratio=_compute_comment_ratio(source),
            imports=imports,
            exported_functions=functions,
            exported_classes=classes,
            last_modified=datetime.fromtimestamp(path.stat().st_mtime),
        )
    except Exception as e:
        logger.warning(f"Failed to analyze {path}: {e}")
        return None


def analyze_directory(repo_path: Path) -> list[ModuleNode]:
    nodes = []
    extensions = set(EXTENSION_MAP.keys())
    for file_path in repo_path.rglob("*"):
        if file_path.suffix.lower() not in extensions:
            continue
        if any(part.startswith(".") or part in {"node_modules", "__pycache__", ".venv", "venv"} for part in file_path.parts):
            continue
        node = analyze_module(file_path)
        if node:
            nodes.append(node)
    return nodes