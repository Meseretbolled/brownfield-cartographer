"""
Semanticist Agent — LLM-Powered Purpose Analyst.

Responsibilities:
  - Generate Purpose Statements for every module (from CODE, not docstrings)
  - Detect Documentation Drift: flag when docstring contradicts implementation
  - Cluster modules into inferred Business Domains via embedding + k-means
  - Answer the Five FDE Day-One Questions with evidence citations
  - ContextWindowBudget: track token spend, route cheap vs expensive tasks
    to appropriate models (fast model for bulk, strong model for synthesis)

LLM routing:
  - Bulk module summaries  → gemini-1.5-flash  (or env-configured fast model)
  - Domain clustering      → embeddings via same fast model
  - Day-One synthesis      → claude-3-5-haiku  (or env-configured strong model)

All calls require ANTHROPIC_API_KEY or GOOGLE_API_KEY in environment.
Graceful degradation: if no API key is present, purpose statements are
populated with a static fallback so the rest of the pipeline still runs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any

from src.models import DataLineageGraph, ModuleGraph, ModuleNode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Model — reads MODEL_NAME from .env, falls back to openrouter/auto:free
FAST_MODEL = os.getenv("MODEL_NAME", "openrouter/auto:free")
STRONG_MODEL = os.getenv("MODEL_NAME", "openrouter/auto:free")
OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
MAX_CODE_CHARS = 3000          # truncate large files before sending to LLM
MAX_BULK_TOKENS_PER_CALL = 800
DOMAIN_K = int(os.getenv("CARTOGRAPHER_DOMAIN_K", "6"))

FDE_QUESTIONS = [
    "What is the primary data ingestion path? (trace from raw sources to first transformation)",
    "What are the 3-5 most critical output datasets or endpoints?",
    "What is the blast radius if the most critical module fails? (which downstream systems break)",
    "Where is the business logic concentrated vs distributed? (which modules/files own the core rules)",
    "What has changed most frequently in the last 30 days? (git velocity map — likely pain points)",
]


# ---------------------------------------------------------------------------
# ContextWindowBudget
# ---------------------------------------------------------------------------

class ContextWindowBudget:
    """
    Tracks estimated token consumption across all LLM calls.
    Provides model-routing logic: cheap calls go to FAST_MODEL,
    synthesis calls go to STRONG_MODEL.
    """

    # Very rough estimate: 1 token ≈ 4 chars
    CHARS_PER_TOKEN = 4

    def __init__(self, budget_tokens: int = 200_000) -> None:
        self.budget_tokens = budget_tokens
        self.used_tokens = 0
        self.call_log: list[dict[str, Any]] = []

    def estimate(self, text: str) -> int:
        return math.ceil(len(text) / self.CHARS_PER_TOKEN)

    def record(self, model: str, prompt_chars: int, completion_chars: int) -> None:
        tokens = self.estimate(" " * (prompt_chars + completion_chars))
        self.used_tokens += tokens
        self.call_log.append({
            "model": model,
            "prompt_chars": prompt_chars,
            "completion_chars": completion_chars,
            "tokens_estimated": tokens,
            "cumulative_tokens": self.used_tokens,
        })

    def remaining(self) -> int:
        return max(0, self.budget_tokens - self.used_tokens)

    def can_afford(self, text: str) -> bool:
        return self.estimate(text) < self.remaining()

    def summary(self) -> dict[str, Any]:
        return {
            "calls": len(self.call_log),
            "used_tokens_estimated": self.used_tokens,
            "budget_tokens": self.budget_tokens,
            "remaining_tokens": self.remaining(),
        }


# ---------------------------------------------------------------------------
# LLM client — OpenRouter (free models) with Anthropic fallback
# ---------------------------------------------------------------------------

def _call_openrouter(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    """
    Call OpenRouter API using env vars:
      OPENROUTER_API_KEY  — your key from openrouter.ai/keys
      OPENROUTER_URL      — https://openrouter.ai/api/v1
      MODEL_NAME          — openrouter/auto:free (auto-selects best free model)
    """
    import urllib.request
    import json as _json

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    base_url = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
    url = f"{base_url.rstrip('/')}/chat/completions"

    payload = _json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/Meseretbolled/brownfield-cartographer",
            "X-Title": "Brownfield Cartographer",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = _json.loads(resp.read().decode("utf-8"))

    return data["choices"][0]["message"]["content"].strip()


def _call_anthropic_fallback(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    """Fallback to Anthropic API if ANTHROPIC_API_KEY is set."""
    try:
        import anthropic  # type: ignore
    except ImportError:
        raise RuntimeError("anthropic package not installed")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def _call_llm(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    """
    Route to OpenRouter (preferred, free) or Anthropic (fallback).
    Priority: OPENROUTER_API_KEY → ANTHROPIC_API_KEY → raise
    """
    if os.getenv("OPENROUTER_API_KEY"):
        return _call_openrouter(prompt, model, max_tokens, system)
    elif os.getenv("ANTHROPIC_API_KEY"):
        return _call_anthropic_fallback(prompt, model, max_tokens, system)
    else:
        raise RuntimeError("No LLM API key set. Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY.")


def _llm_call(prompt: str, model: str, budget: ContextWindowBudget, max_tokens: int = 512) -> str | None:
    """
    Wrapper around LLM call with budget tracking and graceful degradation.
    Returns None if no API key available or budget exhausted.
    """
    if not budget.can_afford(prompt):
        logger.warning("[Semanticist] Budget exhausted — skipping LLM call")
        return None

    try:
        start = time.time()
        result = _call_llm(prompt, model=model, max_tokens=max_tokens)
        elapsed = time.time() - start
        budget.record(model, len(prompt), len(result))
        logger.debug(f"[Semanticist] LLM call {model} took {elapsed:.1f}s")
        return result
    except RuntimeError as e:
        logger.warning(f"[Semanticist] LLM unavailable ({e}) — using static fallback")
        return None
    except Exception as e:
        logger.warning(f"[Semanticist] LLM call failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Purpose statement extraction
# ---------------------------------------------------------------------------

def _extract_existing_docstring(code: str) -> str | None:
    """Pull the module-level docstring from raw Python source."""
    match = re.match(r'\s*(?:"""(.*?)"""|\'\'\'(.*?)\'\'\')', code, re.DOTALL)
    if match:
        return (match.group(1) or match.group(2) or "").strip()
    return None


def _purpose_prompt(module_path: str, code: str) -> str:
    truncated = code[:MAX_CODE_CHARS]
    return (
        f"You are analysing a file from a production data engineering codebase.\n"
        f"File: {module_path}\n\n"
        f"```\n{truncated}\n```\n\n"
        f"Write a 2-3 sentence PURPOSE STATEMENT for this module. "
        f"Focus on BUSINESS FUNCTION (what business problem it solves), not implementation details. "
        f"Do NOT paraphrase the docstring — derive meaning from the actual code. "
        f"Reply with only the purpose statement, no preamble."
    )


def _drift_prompt(module_path: str, docstring: str, purpose: str) -> str:
    return (
        f"Compare these two descriptions of the same file: {module_path}\n\n"
        f"DOCSTRING (what developer wrote):\n{docstring}\n\n"
        f"INFERRED PURPOSE (from code analysis):\n{purpose}\n\n"
        f"Do they contradict each other? Reply with one of:\n"
        f"  CONSISTENT: <one sentence why>\n"
        f"  DRIFT: <one sentence describing the specific contradiction>"
    )


def generate_purpose_statement(
    node: ModuleNode,
    budget: ContextWindowBudget,
    repo_path: Path,
) -> tuple[str, str | None]:
    """
    Returns (purpose_statement, drift_flag_or_None).
    drift_flag is a string like 'DRIFT: docstring says X but code does Y'.
    """
    try:
        code = (Path(node.path)).read_text(encoding="utf-8", errors="replace")
    except Exception:
        code = f"# Could not read {node.path}"

    existing_docstring = _extract_existing_docstring(code)
    prompt = _purpose_prompt(node.path, code)

    purpose = _llm_call(prompt, model=FAST_MODEL, budget=budget, max_tokens=200)

    if purpose is None:
        # Static fallback: derive from exports
        if node.exported_functions:
            purpose = (
                f"Module providing: {', '.join(node.exported_functions[:5])}. "
                f"Contains {node.loc} lines of {node.language} code."
            )
        else:
            purpose = f"{node.language} module with {node.loc} lines. No exported symbols detected."

    drift_flag: str | None = None
    if existing_docstring and len(existing_docstring) > 20:
        drift_result = _llm_call(
            _drift_prompt(node.path, existing_docstring, purpose),
            model=FAST_MODEL, budget=budget, max_tokens=100,
        )
        if drift_result and drift_result.startswith("DRIFT:"):
            drift_flag = drift_result

    return purpose, drift_flag


# ---------------------------------------------------------------------------
# Domain clustering (simple centroid-based, no sklearn required)
# ---------------------------------------------------------------------------

def _cosine_sim(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _embed_text_simple(text: str) -> list[float]:
    """
    Lightweight deterministic pseudo-embedding using character n-gram hashing.
    Used when no embedding API is available. Not semantic — but stable and
    allows clustering by lexical similarity of purpose statements.
    Produces a 64-dimensional vector.
    """
    dims = 64
    vec = [0.0] * dims
    text = text.lower()
    for i in range(len(text) - 2):
        trigram = text[i:i+3]
        h = int(hashlib.md5(trigram.encode()).hexdigest(), 16)
        vec[h % dims] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def _kmeans_cluster(embeddings: list[list[float]], k: int, iterations: int = 20) -> list[int]:
    """Minimal k-means returning cluster label per item."""
    if not embeddings or k <= 0:
        return []
    k = min(k, len(embeddings))
    # Initialise centroids from first k embeddings
    centroids = [emb[:] for emb in embeddings[:k]]

    labels = [0] * len(embeddings)
    for _ in range(iterations):
        # Assignment
        for i, emb in enumerate(embeddings):
            labels[i] = max(range(k), key=lambda c: _cosine_sim(emb, centroids[c]))
        # Update centroids
        for c in range(k):
            members = [embeddings[i] for i, l in enumerate(labels) if l == c]
            if members:
                dims = len(members[0])
                new_centroid = [sum(m[d] for m in members) / len(members) for d in range(dims)]
                centroids[c] = new_centroid
    return labels


_DOMAIN_LABELS = [
    "ingestion", "transformation", "serving", "monitoring",
    "orchestration", "configuration", "testing", "utilities",
]


def cluster_into_domains(
    nodes: list[ModuleNode],
    budget: ContextWindowBudget,
    k: int = DOMAIN_K,
) -> dict[str, str]:
    """
    Returns {module_path: domain_label}.
    Uses purpose_statement embeddings + k-means.
    Falls back to path-based heuristics if purpose statements are empty.
    """
    if not nodes:
        return {}

    texts = [
        node.purpose_statement or " ".join(node.exported_functions) or node.path
        for node in nodes
    ]
    embeddings = [_embed_text_simple(t) for t in texts]
    labels = _kmeans_cluster(embeddings, k=k)

    # Name each cluster by asking LLM with top-3 member purposes
    cluster_to_name: dict[int, str] = {}
    for cluster_id in range(k):
        members = [nodes[i] for i, l in enumerate(labels) if l == cluster_id]
        if not members:
            cluster_to_name[cluster_id] = _DOMAIN_LABELS[cluster_id % len(_DOMAIN_LABELS)]
            continue

        sample_purposes = "\n".join(
            f"- {m.purpose_statement or m.path}" for m in members[:3]
        )
        name_prompt = (
            f"These modules belong to the same architectural domain:\n{sample_purposes}\n\n"
            f"Give this domain a single-word name (e.g. ingestion, transformation, serving, "
            f"monitoring, orchestration, utilities). Reply with ONLY the single word."
        )
        name = _llm_call(name_prompt, model=FAST_MODEL, budget=budget, max_tokens=10)
        cluster_to_name[cluster_id] = (name or _DOMAIN_LABELS[cluster_id % len(_DOMAIN_LABELS)]).strip().lower()

    return {nodes[i].path: cluster_to_name[label] for i, label in enumerate(labels)}


# ---------------------------------------------------------------------------
# Day-One question answering
# ---------------------------------------------------------------------------

def _build_synthesis_context(
    module_graph: ModuleGraph,
    lineage_graph: DataLineageGraph,
) -> str:
    """Compress structural findings into a context string for the LLM."""
    hubs = module_graph.architectural_hubs[:5]
    high_vel = module_graph.high_velocity_files[:5]
    sources = lineage_graph.sources[:8]
    sinks = lineage_graph.sinks[:8]
    cycles = module_graph.circular_dependencies[:3]

    lines = [
        "=== CODEBASE STRUCTURAL SUMMARY ===",
        f"Total modules: {len(module_graph.nodes)}",
        f"Import edges: {len(module_graph.edges)}",
        f"Architectural hubs (PageRank): {', '.join(hubs) or 'none'}",
        f"High-velocity files (30d): {', '.join(high_vel) or 'none'}",
        f"Data sources (in-degree=0): {', '.join(sources) or 'none'}",
        f"Data sinks (out-degree=0): {', '.join(sinks) or 'none'}",
        f"Circular dependencies: {len(cycles)} groups",
        "",
        "=== TOP MODULES BY PAGERANK ===",
    ]
    sorted_nodes = sorted(
        module_graph.nodes.values(),
        key=lambda n: n.pagerank_score,
        reverse=True,
    )[:10]
    for node in sorted_nodes:
        purpose = (node.purpose_statement or "no purpose statement")[:120]
        lines.append(f"  {node.path}  [{node.language}] vel={node.change_velocity_30d}  — {purpose}")

    lines += ["", "=== DATA LINEAGE SUMMARY ==="]
    for t in list(lineage_graph.transformation_nodes.values())[:15]:
        lines.append(
            f"  {t.source_file}  {t.source_datasets} → {t.target_datasets}  [{t.transformation_type}]"
        )

    return "\n".join(lines)


def answer_day_one_questions(
    module_graph: ModuleGraph,
    lineage_graph: DataLineageGraph,
    budget: ContextWindowBudget,
) -> dict[str, str]:
    """
    Returns {question: answer_with_evidence_citations}.
    Uses STRONG_MODEL for synthesis.
    Falls back to static structural answers if LLM unavailable.
    """
    context = _build_synthesis_context(module_graph, lineage_graph)

    questions_block = "\n".join(f"{i+1}. {q}" for i, q in enumerate(FDE_QUESTIONS))

    prompt = (
        f"{context}\n\n"
        f"You are an FDE (Forward Deployed Engineer) who has just been given this structural "
        f"analysis of an unfamiliar codebase. Answer these five onboarding questions:\n\n"
        f"{questions_block}\n\n"
        f"For each answer:\n"
        f"  - Be specific: cite actual file paths and dataset names from the summary above\n"
        f"  - Distinguish between STATIC ANALYSIS (certain) and INFERENCE (likely)\n"
        f"  - Keep each answer to 2-4 sentences\n\n"
        f"Format your response as:\n"
        f"Q1: <answer>\nQ2: <answer>\nQ3: <answer>\nQ4: <answer>\nQ5: <answer>"
    )

    raw = _llm_call(prompt, model=STRONG_MODEL, budget=budget, max_tokens=1000)

    if raw:
        answers: dict[str, str] = {}
        for i, q in enumerate(FDE_QUESTIONS, 1):
            match = re.search(rf"Q{i}:\s*(.*?)(?=Q{i+1}:|$)", raw, re.DOTALL)
            answers[q] = match.group(1).strip() if match else "See structural summary."
        return answers

    # Static fallback — answer from raw graph data
    hubs = module_graph.architectural_hubs[:3]
    sources = lineage_graph.sources[:5]
    sinks = lineage_graph.sinks[:5]
    high_vel = module_graph.high_velocity_files[:5]

    return {
        FDE_QUESTIONS[0]: f"[STATIC] Primary ingestion sources detected: {', '.join(sources) or 'none found'}.",
        FDE_QUESTIONS[1]: f"[STATIC] Critical output sinks: {', '.join(sinks) or 'none found'}.",
        FDE_QUESTIONS[2]: f"[STATIC] Architectural hubs (highest blast radius): {', '.join(hubs) or 'none'}.",
        FDE_QUESTIONS[3]: f"[STATIC] Business logic concentrated in top PageRank modules: {', '.join(hubs) or 'unknown'}.",
        FDE_QUESTIONS[4]: f"[STATIC] Highest-velocity files (30d): {', '.join(high_vel) or 'none found'}.",
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class Semanticist:
    """
    Adds LLM-derived semantic understanding to the KnowledgeGraph:
    purpose statements, documentation drift flags, domain clusters,
    and the Five FDE Day-One Answers.
    """

    def __init__(
        self,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        repo_path: Path,
        budget_tokens: int = 200_000,
    ) -> None:
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.repo_path = repo_path
        self.budget = ContextWindowBudget(budget_tokens)
        self.drift_flags: dict[str, str] = {}
        self.domain_map: dict[str, str] = {}
        self.day_one_answers: dict[str, str] = {}

    def run(self) -> None:
        nodes = list(self.module_graph.nodes.values())
        logger.info(f"[Semanticist] Generating purpose statements for {len(nodes)} modules …")

        # 1. Purpose statements + drift detection
        for node in nodes:
            purpose, drift = generate_purpose_statement(node, self.budget, self.repo_path)
            node.purpose_statement = purpose
            if drift:
                self.drift_flags[node.path] = drift
                logger.info(f"[Semanticist] Doc drift: {node.path} — {drift}")

        logger.info(
            f"[Semanticist] Purpose statements done. "
            f"Drift flags: {len(self.drift_flags)}. "
            f"Budget used: ~{self.budget.used_tokens:,} tokens"
        )

        # 2. Domain clustering
        logger.info("[Semanticist] Clustering modules into business domains …")
        self.domain_map = cluster_into_domains(nodes, self.budget)
        for node in nodes:
            node.domain_cluster = self.domain_map.get(node.path)

        # 3. Day-One questions
        logger.info("[Semanticist] Synthesising Day-One FDE answers …")
        self.day_one_answers = answer_day_one_questions(
            self.module_graph, self.lineage_graph, self.budget
        )

        logger.info(
            f"[Semanticist] Done. "
            f"Final budget: {self.budget.summary()}"
        )

    def save_trace(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        trace = {
            "drift_flags": self.drift_flags,
            "domain_map": self.domain_map,
            "day_one_answers": self.day_one_answers,
            "budget_summary": self.budget.summary(),
            "call_log": self.budget.call_log,
        }
        (output_dir / "semanticist_trace.json").write_text(
            json.dumps(trace, indent=2), encoding="utf-8"
        )