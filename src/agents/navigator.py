"""
Navigator Agent — LangGraph-powered query interface over the knowledge graph.

Four tools registered as proper LangGraph/LangChain tools:
  - find_implementation(concept)      Semantic lookup by purpose / exports / domain
  - trace_lineage(dataset, direction) Upstream / downstream graph traversal
  - blast_radius(module_path)         Downstream dependents of a module
  - explain_module(path)              Full module metadata + dependency context

LangGraph is used when OPENROUTER_API_KEY or ANTHROPIC_API_KEY is present.
Falls back to direct method dispatch (no LLM) so the CLI query command always
works even without an API key.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
from pathlib import Path
from typing import Any, Annotated

import networkx as nx
import numpy as np

from src.graph.knowledge_graph import KnowledgeGraph
from src.models import DataLineageGraph, ModuleGraph

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rel(path: str, repo_path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(repo_path))
    except Exception:
        return path


def _as_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


# ---------------------------------------------------------------------------
# Vector index — TF-IDF cosine similarity for semantic search
# ---------------------------------------------------------------------------

def _tfidf_vectorize(texts: list[str]) -> np.ndarray:
    """Build L2-normalised TF-IDF matrix using numpy only (no sklearn required)."""
    tokenised = [re.findall(r"[a-z]{2,}", t.lower()) for t in texts]
    vocab: dict[str, int] = {}
    for tokens in tokenised:
        for tok in tokens:
            if tok not in vocab:
                vocab[tok] = len(vocab)

    V, N = len(vocab), len(texts)
    if V == 0 or N == 0:
        return np.zeros((N, max(V, 1)), dtype=np.float32)

    tf = np.zeros((N, V), dtype=np.float32)
    for i, tokens in enumerate(tokenised):
        for tok in tokens:
            tf[i, vocab[tok]] += 1
    row_sums = tf.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    tf /= row_sums

    df  = (tf > 0).sum(axis=0)
    idf = np.log((N + 1) / (df + 1)) + 1
    mat = tf * idf

    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return mat / norms


class VectorIndex:
    """
    In-memory vector index over module purpose statements.
    Supports cosine-similarity semantic search without external dependencies.
    """

    def __init__(self, module_graph: ModuleGraph) -> None:
        self.paths: list[str] = list(module_graph.nodes.keys())
        self.nodes = module_graph.nodes

        # Build corpus: purpose + path + exports for each module
        corpus = [
            f"{node.purpose_statement or ''} {path} "
            f"{' '.join(node.exported_functions[:10])} "
            f"{' '.join(node.exported_classes[:5])} "
            f"{node.domain_cluster or ''}"
            for path, node in module_graph.nodes.items()
        ]
        self.vectors = _tfidf_vectorize(corpus)   # (N, V)
        logger.info(
            f"[Navigator] VectorIndex built — {len(self.paths)} modules, "
            f"embedding dim={self.vectors.shape[1]}"
        )

    def search(self, query: str, limit: int = 10) -> list[tuple[float, str]]:
        """Return [(cosine_score, path)] sorted descending."""
        q_vec = _tfidf_vectorize([query] + [
            # expand query with synonyms from domain labels
            "ingest transform serve monitor orchestrate configure test utility"
        ])[:1]  # take only the query vector

        # cosine similarity: (1, V) × (V, N) → (1, N)
        sims = (q_vec @ self.vectors.T)[0]
        top_idx = np.argsort(sims)[::-1][:limit]
        return [(float(sims[i]), self.paths[i]) for i in top_idx if sims[i] > 0]


# ---------------------------------------------------------------------------
# Pure graph-query functions (no LLM, always available)
# ---------------------------------------------------------------------------

def _find_implementation(
    concept: str,
    module_graph: ModuleGraph,
    repo_path: Path,
    limit: int = 10,
    vector_index: VectorIndex | None = None,
) -> dict[str, Any]:
    """
    Two-stage semantic search:
    1. Vector cosine similarity over TF-IDF embeddings of purpose statements
    2. Keyword boost for exact token matches
    Returns results with file:line citations and analysis_method label.
    """
    concept_l = concept.lower().strip()
    scores: dict[str, float] = {}

    # Stage 1 — vector similarity
    if vector_index is not None:
        for sim_score, path in vector_index.search(concept, limit=limit * 2):
            scores[path] = scores.get(path, 0.0) + sim_score * 10.0

    # Stage 2 — keyword boost
    for path, node in module_graph.nodes.items():
        text_parts = [
            path.lower(),
            (node.purpose_statement or "").lower(),
            (node.domain_cluster or "").lower(),
            " ".join(node.exported_functions).lower(),
            " ".join(node.exported_classes).lower(),
        ]
        blob = " ".join(text_parts)
        if concept_l in blob:
            scores[path] = scores.get(path, 0.0) + 5.0
        for token in concept_l.split():
            if token and len(token) > 2 and token in blob:
                scores[path] = scores.get(path, 0.0) + 1.0

    # Build ranked results
    ranked = sorted(scores.items(), key=lambda x: -x[1])[:limit]
    matches = []
    for path, score in ranked:
        if path not in module_graph.nodes:
            continue
        node = module_graph.nodes[path]
        matches.append({
            "path": _rel(path, repo_path),
            "file": _rel(path, repo_path),
            "line_start": 1,
            "line_end": node.loc,
            "purpose_statement": node.purpose_statement,
            "domain_cluster": node.domain_cluster,
            "complexity_score": node.complexity_score,
            "pagerank_score": round(node.pagerank_score, 6),
            "similarity_score": round(score, 4),
            "analysis_method": "vector cosine similarity (TF-IDF) + keyword boost",
            "evidence": {
                "exports": node.exported_functions[:8],
                "classes": node.exported_classes[:8],
                "loc": node.loc,
            },
        })

    return {
        "query": concept,
        "search_method": "TF-IDF vector index + keyword scoring",
        "matches": matches,
    }


def _trace_lineage(
    dataset: str,
    direction: str,
    kg: KnowledgeGraph,
) -> dict[str, Any]:
    if direction not in {"upstream", "downstream", "both"}:
        return {
            "dataset": dataset,
            "direction": direction,
            "analysis_method": "graph traversal",
            "error": "direction must be one of: upstream, downstream, both",
        }

    if dataset not in kg.lineage_graph:
        # Fuzzy match: try partial name
        candidates = [n for n in kg.lineage_graph.nodes() if dataset.lower() in n.lower()]
        if not candidates:
            return {
                "dataset": dataset,
                "direction": direction,
                "analysis_method": "graph traversal",
                "error": f"dataset '{dataset}' not found in lineage graph",
                "available_datasets": list(kg.lineage_graph.nodes())[:20],
            }
        dataset = candidates[0]
        logger.info(f"[Navigator] Fuzzy matched dataset to: {dataset}")

    result: dict[str, Any] = {
        "dataset": dataset,
        "direction": direction,
        "analysis_method": "graph traversal (BFS/DFS via NetworkX)",
    }

    if direction in {"upstream", "both"}:
        upstream = sorted(nx.ancestors(kg.lineage_graph, dataset))
        result["upstream"] = upstream
        result["upstream_count"] = len(upstream)

    if direction in {"downstream", "both"}:
        downstream = sorted(nx.descendants(kg.lineage_graph, dataset))
        result["downstream"] = downstream
        result["downstream_count"] = len(downstream)

    return result


def _blast_radius(
    module_path: str,
    module_graph: ModuleGraph,
    kg: KnowledgeGraph,
    repo_path: Path,
) -> dict[str, Any]:
    candidates = [p for p in module_graph.nodes if module_path in p]
    if not candidates:
        return {
            "module_path": module_path,
            "analysis_method": "module dependency traversal (PageRank + BFS)",
            "error": "module not found in graph",
            "hint": "Use a partial path, e.g. 'revenue' instead of 'src/transforms/revenue.py'",
        }

    resolved = candidates[0]
    node = module_graph.nodes[resolved]

    downstream = sorted(nx.descendants(kg.module_graph, resolved))
    upstream = sorted(nx.ancestors(kg.module_graph, resolved))

    return {
        "module_path": _rel(resolved, repo_path),
        "analysis_method": "module dependency traversal (PageRank + BFS)",
        "pagerank_score": round(node.pagerank_score, 6),
        "domain_cluster": node.domain_cluster,
        "change_velocity_30d": node.change_velocity_30d,
        "blast_radius": {
            "downstream_dependents": [_rel(p, repo_path) for p in downstream],
            "downstream_count": len(downstream),
            "upstream_dependencies": [_rel(p, repo_path) for p in upstream],
            "upstream_count": len(upstream),
        },
        "risk_assessment": (
            "HIGH — architectural hub, many dependents" if len(downstream) > 10
            else "MEDIUM — moderate downstream impact" if len(downstream) > 3
            else "LOW — few direct dependents"
        ),
    }


def _explain_module(
    path: str,
    module_graph: ModuleGraph,
    repo_path: Path,
) -> dict[str, Any]:
    candidates = [p for p in module_graph.nodes if path in p]
    if not candidates:
        return {
            "path": path,
            "analysis_method": "static analysis + semantic synthesis",
            "error": "module not found",
        }

    resolved = candidates[0]
    node = module_graph.nodes[resolved]

    imported_by = sorted([
        edge.source for edge in module_graph.edges if edge.target == resolved
    ])
    imports = sorted([
        edge.target for edge in module_graph.edges if edge.source == resolved
    ])

    return {
        "path": _rel(resolved, repo_path),
        "analysis_method": "static analysis + semantic synthesis",
        "language": str(node.language),
        "purpose_statement": node.purpose_statement,
        "domain_cluster": node.domain_cluster,
        "metrics": {
            "pagerank_score": round(node.pagerank_score, 6),
            "complexity_score": round(node.complexity_score, 2),
            "change_velocity_30d": node.change_velocity_30d,
            "loc": node.loc,
            "comment_ratio": round(node.comment_ratio, 3),
            "is_dead_code_candidate": node.is_dead_code_candidate,
        },
        "exports": {
            "functions": node.exported_functions,
            "classes": node.exported_classes,
        },
        "dependencies": {
            "imports": [_rel(p, repo_path) for p in imports],
            "imported_by": [_rel(p, repo_path) for p in imported_by],
        },
    }


# ---------------------------------------------------------------------------
# LangGraph agent (optional — only constructed when LangChain is available
# and an API key is set)
# ---------------------------------------------------------------------------

def _build_langgraph_agent(
    module_graph: ModuleGraph,
    lineage_graph: DataLineageGraph,
    kg: KnowledgeGraph,
    repo_path: Path,
    vector_index: VectorIndex | None = None,
) -> Any | None:
    """
    Build a LangGraph ReAct agent with all four tools registered.
    Returns None if LangGraph / LangChain is not installed or no API key set.
    """
    try:
        from langchain_core.tools import tool
        from langgraph.prebuilt import create_react_agent
    except ImportError:
        logger.info("[Navigator] LangGraph not installed — using direct dispatch")
        return None

    # Build LLM (OpenRouter via OpenAI-compat or Anthropic)
    llm = None
    openrouter_key = os.getenv("OPENROUTER_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    model_name = os.getenv("STRONG_MODEL", os.getenv("MODEL_NAME", "google/gemini-2.0-flash-exp:free"))

    if openrouter_key:
        try:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(
                model=model_name,
                openai_api_key=openrouter_key,
                openai_api_base="https://openrouter.ai/api/v1",
                default_headers={
                    "HTTP-Referer": "https://github.com/brownfield-cartographer",
                    "X-Title": "Brownfield Cartographer",
                },
                temperature=0,
            )
            logger.info(f"[Navigator] LangGraph LLM: OpenRouter / {model_name}")
        except ImportError:
            logger.info("[Navigator] langchain_openai not installed")

    if llm is None and anthropic_key:
        try:
            from langchain_anthropic import ChatAnthropic
            llm = ChatAnthropic(
                model="claude-haiku-4-5-20251001",
                anthropic_api_key=anthropic_key,
                temperature=0,
            )
            logger.info("[Navigator] LangGraph LLM: Anthropic Haiku")
        except ImportError:
            logger.info("[Navigator] langchain_anthropic not installed")

    if llm is None:
        logger.info("[Navigator] No LLM available — using direct dispatch")
        return None

    # Use provided vector index or build one
    _vector_idx = vector_index or VectorIndex(module_graph)

    # ----------------------------------------------------------------
    # Register the four tools
    # ----------------------------------------------------------------

    @tool
    def find_implementation(concept: str) -> str:
        """
        Find which files implement a given concept, feature, or business function.
        Uses TF-IDF vector cosine similarity over purpose statement embeddings,
        boosted by keyword matching. Returns file:line citations.

        Example: find_implementation("revenue calculation")
        """
        result = _find_implementation(concept, module_graph, repo_path, vector_index=_vector_idx)
        return _as_json(result)

    @tool
    def trace_lineage(dataset: str, direction: str = "upstream") -> str:
        """
        Trace data lineage around a dataset node in the lineage graph.
        direction must be one of: 'upstream', 'downstream', 'both'.
        Upstream = what produces this dataset.
        Downstream = what this dataset feeds into.

        Example: trace_lineage("daily_active_users", "upstream")
        """
        result = _trace_lineage(dataset, direction, kg)
        return _as_json(result)

    @tool
    def blast_radius(module_path: str) -> str:
        """
        Show every module that would break if the given module changed its interface.
        Uses BFS over the module import graph.
        Returns downstream dependents, upstream dependencies, and a risk assessment.

        Example: blast_radius("src/transforms/revenue.py")
        """
        result = _blast_radius(module_path, module_graph, kg, repo_path)
        return _as_json(result)

    @tool
    def explain_module(path: str) -> str:
        """
        Explain what a module does, its purpose statement, domain cluster, metrics,
        exported symbols, and full dependency context (what it imports and what imports it).

        Example: explain_module("src/ingestion/kafka_consumer.py")
        """
        result = _explain_module(path, module_graph, repo_path)
        return _as_json(result)

    tools = [find_implementation, trace_lineage, blast_radius, explain_module]

    system_prompt = (
        "You are the Navigator — a codebase intelligence agent for a brownfield FDE engagement. "
        "You have four tools to query a pre-analysed codebase knowledge graph:\n"
        "  • find_implementation — locate where a concept or feature is implemented\n"
        "  • trace_lineage — trace data flow upstream or downstream from a dataset\n"
        "  • blast_radius — find everything that breaks if a module changes\n"
        "  • explain_module — get full metadata, purpose, and dependencies for a module\n\n"
        "Always cite evidence: file paths, line ranges, and whether the answer comes from "
        "static analysis (certain) or LLM inference (likely). "
        "Be concise and specific — this is a production debugging context."
    )

    try:
        agent = create_react_agent(llm, tools, prompt=system_prompt)
        logger.info("[Navigator] LangGraph ReAct agent built successfully")
        return agent
    except TypeError:
        # Older langgraph versions use 'messages_modifier' instead of 'prompt'
        try:
            from langchain_core.messages import SystemMessage
            agent = create_react_agent(
                llm,
                tools,
                messages_modifier=SystemMessage(content=system_prompt),
            )
            logger.info("[Navigator] LangGraph ReAct agent built (legacy API)")
            return agent
        except Exception as e:
            logger.warning(f"[Navigator] LangGraph agent build failed: {e}")
            return None


# ---------------------------------------------------------------------------
# Public Navigator class
# ---------------------------------------------------------------------------

class Navigator:
    """
    Query interface over the codebase knowledge graph.

    When LangGraph + an LLM API key are available, natural-language queries
    are handled by a ReAct agent that decides which tools to call and synthesises
    an answer. When unavailable, direct method dispatch is used instead — all
    four tools still work, they just don't have LLM reasoning on top.
    """

    def __init__(
        self,
        repo_path: Path,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        kg: KnowledgeGraph | None = None,
        semanticist: Any | None = None,
    ) -> None:
        self.repo_path = repo_path.resolve()
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.semanticist = semanticist
        self.kg = kg or self._rebuild_kg()
        # Build vector index over purpose statements for semantic search
        self.vector_index = VectorIndex(module_graph)
        self._agent = _build_langgraph_agent(
            module_graph, lineage_graph, self.kg, self.repo_path
        )
        self.has_llm = self._agent is not None

    def _rebuild_kg(self) -> KnowledgeGraph:
        kg = KnowledgeGraph()
        for node in self.module_graph.nodes.values():
            kg.add_module(node)
        for edge in self.module_graph.edges:
            kg.add_module_edge(edge.source, edge.target)
        for node in self.lineage_graph.dataset_nodes.values():
            kg.add_dataset(node)
        for node in self.lineage_graph.transformation_nodes.values():
            kg.add_transformation(node)
        return kg

    # ------------------------------------------------------------------
    # Natural-language query (uses LangGraph agent when available)
    # ------------------------------------------------------------------

    def query(self, question: str) -> str:
        """
        Answer a free-form natural language question about the codebase.
        Uses the LangGraph ReAct agent if available, otherwise returns a
        hint to use the structured tool methods directly.
        """
        if self._agent is None:
            return (
                "LangGraph agent not available (no API key or package missing). "
                "Use the structured tools directly:\n"
                "  find_implementation(concept)\n"
                "  trace_lineage(dataset, direction)\n"
                "  blast_radius(module_path)\n"
                "  explain_module(path)"
            )

        try:
            from langchain_core.messages import HumanMessage
            result = self._agent.invoke({"messages": [HumanMessage(content=question)]})
            messages = result.get("messages", [])
            # Return the last AI message content
            for msg in reversed(messages):
                if hasattr(msg, "content") and msg.content:
                    return str(msg.content)
            return "No response generated."
        except Exception as e:
            logger.warning(f"[Navigator] LangGraph query failed: {e}")
            return f"LangGraph query failed: {e}"

    # ------------------------------------------------------------------
    # Tool 1 — find_implementation
    # ------------------------------------------------------------------

    def find_implementation(self, concept: str, limit: int = 10) -> dict[str, Any]:
        """
        Vector semantic search using TF-IDF cosine similarity over purpose statements,
        boosted by keyword matching. Returns file:line citations.
        """
        return _find_implementation(
            concept, self.module_graph, self.repo_path, limit,
            vector_index=self.vector_index
        )

    # ------------------------------------------------------------------
    # Tool 2 — trace_lineage
    # ------------------------------------------------------------------

    def trace_lineage(self, dataset: str, direction: str = "upstream") -> dict[str, Any]:
        """
        Trace lineage around a dataset node. direction ∈ {upstream, downstream, both}.
        """
        return _trace_lineage(dataset, direction, self.kg)

    # ------------------------------------------------------------------
    # Tool 3 — blast_radius
    # ------------------------------------------------------------------

    def blast_radius(self, module_path: str) -> dict[str, Any]:
        """
        Show every module that would break if the given module's interface changed.
        """
        return _blast_radius(module_path, self.module_graph, self.kg, self.repo_path)

    # ------------------------------------------------------------------
    # Tool 4 — explain_module
    # ------------------------------------------------------------------

    def explain_module(self, path: str) -> dict[str, Any]:
        """
        Full metadata + dependency context for a module.
        """
        return _explain_module(path, self.module_graph, self.repo_path)