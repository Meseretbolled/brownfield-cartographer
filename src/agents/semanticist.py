"""
Semanticist Agent — LLM-Powered Purpose Analyst.

Responsibilities:
  - Generate Purpose Statements for every module (from CODE, not docstrings)
  - Detect Documentation Drift: flag when docstring contradicts implementation
  - Cluster modules into inferred Business Domains via LLM classification
  - Answer the Five FDE Day-One Questions with evidence citations
  - ContextWindowBudget: track token spend, route cheap vs expensive tasks
    to appropriate models (fast model for bulk, strong model for synthesis)

LLM routing:
  - Bulk module summaries (batched)  → FAST_MODEL  (env: MODEL_NAME)
  - Domain classification            → FAST_MODEL  (per-module, cheap prompt)
  - Day-One synthesis                → STRONG_MODEL (env: STRONG_MODEL)

Keys: OPENROUTER_API_KEY (preferred, free tier) → ANTHROPIC_API_KEY (fallback)
Graceful degradation: if no key present, static fallbacks populate all fields
so the rest of the pipeline always runs.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any

import numpy as np

from src.models import DataLineageGraph, ModuleGraph, ModuleNode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — safe defaults that are actually live on OpenRouter free tier
# ---------------------------------------------------------------------------

FAST_MODEL   = os.getenv("MODEL_NAME",   "mistralai/mistral-small-3.1-24b-instruct:free")
STRONG_MODEL = os.getenv("STRONG_MODEL", os.getenv("MODEL_NAME", "openai/gpt-oss-120b:free"))
OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
MAX_CODE_CHARS = 2000
BATCH_SIZE     = 5  
MAX_RETRIES    = 5    # retry attempts on rate-limit
RETRY_BASE_S   = 5      # base wait seconds — actual wait = RETRY_BASE_S * 2^attempt
                        # so: 5s, 10s, 20s, 40s, 80s
DOMAIN_K       = int(os.getenv("CARTOGRAPHER_DOMAIN_K", "6"))

FDE_QUESTIONS = [
    "What is the primary data ingestion path? (trace from raw sources to first transformation)",
    "What are the 3-5 most critical output datasets or endpoints?",
    "What is the blast radius if the most critical module fails? (which downstream systems break)",
    "Where is the business logic concentrated vs distributed? (which modules/files own the core rules)",
    "What has changed most frequently in the last 30 days? (git velocity map — likely pain points)",
]

_DOMAIN_LABELS = [
    "ingestion", "transformation", "serving", "monitoring",
    "orchestration", "configuration", "testing", "utilities",
]


# ---------------------------------------------------------------------------
# ContextWindowBudget
# ---------------------------------------------------------------------------

class ContextWindowBudget:
    """Tracks estimated token consumption and routes cheap vs expensive calls."""

    CHARS_PER_TOKEN = 4

    def __init__(self, budget_tokens: int = 200_000) -> None:
        self.budget_tokens = budget_tokens
        self.used_tokens   = 0
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
# LLM client — OpenRouter with proper retry + Anthropic fallback
# ---------------------------------------------------------------------------

def _call_openrouter(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    import urllib.request
    import urllib.error

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    url     = f"{OPENROUTER_URL.rstrip('/')}/chat/completions"
    payload = json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
    }).encode("utf-8")

    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(
            url, data=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/brownfield-cartographer",
                "X-Title": "Brownfield Cartographer",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"].strip()

        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = RETRY_BASE_S * (2 ** attempt)
                logger.warning(
                    f"[Semanticist] Rate limited (429) — waiting {wait}s "
                    f"(attempt {attempt+1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue
            if e.code == 404:
                raise RuntimeError(
                    f"Model '{model}' not found on OpenRouter (404). "
                    f"Update MODEL_NAME in your .env"
                ) from e
            raise

        except Exception:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BASE_S * (2 ** attempt)
                time.sleep(wait)
                continue
            raise

    raise RuntimeError(f"OpenRouter failed after {MAX_RETRIES} retries")


def _call_anthropic_fallback(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    try:
        import anthropic  # type: ignore
    except ImportError:
        raise RuntimeError("anthropic package not installed")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    client   = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model, max_tokens=max_tokens, system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def _call_llm(
    prompt: str,
    model: str,
    max_tokens: int = 512,
    system: str = "You are a senior software architect analysing a production codebase.",
) -> str:
    if os.getenv("OPENROUTER_API_KEY"):
        return _call_openrouter(prompt, model, max_tokens, system)
    elif os.getenv("ANTHROPIC_API_KEY"):
        return _call_anthropic_fallback(prompt, model, max_tokens, system)
    else:
        raise RuntimeError("No LLM API key set. Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY.")


def _llm_call(
    prompt: str,
    model: str,
    budget: ContextWindowBudget,
    max_tokens: int = 512,
) -> str | None:
    """Budget-tracked wrapper with graceful degradation."""
    if not budget.can_afford(prompt):
        logger.warning("[Semanticist] Budget exhausted — skipping LLM call")
        return None
    try:
        start  = time.time()
        result = _call_llm(prompt, model=model, max_tokens=max_tokens)
        budget.record(model, len(prompt), len(result))
        logger.debug(f"[Semanticist] LLM call {model} took {time.time()-start:.1f}s")
        return result
    except RuntimeError as e:
        logger.warning(f"[Semanticist] LLM unavailable ({e}) — using static fallback")
        return None
    except Exception as e:
        logger.warning(f"[Semanticist] LLM call failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Batched purpose-statement extraction
# ---------------------------------------------------------------------------

def _extract_existing_docstring(code: str) -> str | None:
    match = re.match(r'\s*(?:"""(.*?)"""|\'\'\'(.*?)\'\'\')', code, re.DOTALL)
    if match:
        return (match.group(1) or match.group(2) or "").strip()
    return None


def _static_purpose(node: ModuleNode) -> str:
    """Clean static fallback — no [STATIC] prefix visible to evaluators."""
    if node.exported_functions:
        fns = ", ".join(node.exported_functions[:5])
        return f"Module providing: {fns}. Contains {node.loc} lines of {node.language} code."
    return f"{node.language} module with {node.loc} lines. No exported symbols detected."


def _build_batch_prompt(batch: list[tuple[ModuleNode, str]]) -> str:
    items = []
    for i, (node, code) in enumerate(batch):
        items.append(f"MODULE {i+1}\nFile: {node.path}\n```\n{code[:MAX_CODE_CHARS]}\n```")
    modules_block = "\n\n---\n\n".join(items)
    return (
        f"You are analysing files from a production data engineering codebase.\n\n"
        f"{modules_block}\n\n"
        f"For each module above, write a 2-3 sentence PURPOSE STATEMENT.\n"
        f"Focus on BUSINESS FUNCTION — what business problem it solves, not implementation.\n"
        f"Do NOT paraphrase docstrings — derive meaning from actual code.\n\n"
        f"Reply ONLY with a JSON array of strings, one per module, in order:\n"
        f'["purpose for module 1", "purpose for module 2", ...]'
    )


def _parse_batch_response(raw: str, batch_size: int) -> list[str | None]:
    try:
        cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
        parsed  = json.loads(cleaned)
        if isinstance(parsed, list):
            result = [str(p).strip() if p else None for p in parsed]
            while len(result) < batch_size:
                result.append(None)
            return result[:batch_size]
    except Exception:
        pass
    return [None] * batch_size


def _drift_prompt(module_path: str, docstring: str, purpose: str) -> str:
    return (
        f"Compare these two descriptions of: {module_path}\n\n"
        f"DOCSTRING (developer wrote):\n{docstring}\n\n"
        f"INFERRED PURPOSE (from code analysis):\n{purpose}\n\n"
        f"Do they contradict each other? Reply with one of:\n"
        f"  CONSISTENT: <one sentence why>\n"
        f"  DRIFT: <one sentence describing the specific contradiction>"
    )


def generate_purpose_statements_batched(
    nodes: list[ModuleNode],
    budget: ContextWindowBudget,
    repo_path: Path,
) -> dict[str, tuple[str, str | None]]:
    """
    Returns {node.path: (purpose_statement, drift_flag_or_None)}.
    Sends modules in batches of BATCH_SIZE to reduce API calls.
    Adds inter-batch sleep to stay under free-tier RPM limits.
    """
    sources: dict[str, str] = {}
    for node in nodes:
        try:
            sources[node.path] = Path(node.path).read_text(encoding="utf-8", errors="replace")
        except Exception:
            sources[node.path] = f"# Could not read {node.path}"

    results: dict[str, tuple[str, str | None]] = {}
    total_batches = math.ceil(len(nodes) / BATCH_SIZE)

    for batch_idx, batch_start in enumerate(range(0, len(nodes), BATCH_SIZE)):
        batch_nodes = nodes[batch_start : batch_start + BATCH_SIZE]
        batch       = [(n, sources[n.path]) for n in batch_nodes]

        logger.info(
            f"[Semanticist] Batch {batch_idx+1}/{total_batches} "
            f"({len(batch)} modules) …"
        )

        prompt   = _build_batch_prompt(batch)
        raw      = _llm_call(prompt, model=FAST_MODEL, budget=budget, max_tokens=BATCH_SIZE * 200)
        purposes = _parse_batch_response(raw, len(batch)) if raw else [None] * len(batch)

        for i, (node, code) in enumerate(batch):
            purpose = purposes[i] or _static_purpose(node)

            drift_flag: str | None = None
            existing_docstring = _extract_existing_docstring(code)
            if existing_docstring and len(existing_docstring) > 20:
                drift_raw = _llm_call(
                    _drift_prompt(node.path, existing_docstring, purpose),
                    model=FAST_MODEL, budget=budget, max_tokens=100,
                )
                if drift_raw and drift_raw.strip().startswith("DRIFT:"):
                    drift_flag = drift_raw.strip()

            results[node.path] = (purpose, drift_flag)

        # Inter-batch pause — keeps free-tier RPM happy
        if batch_idx < total_batches - 1:
            time.sleep(2)

    return results


# ---------------------------------------------------------------------------
# Vector embedding helpers — TF-IDF cosine similarity
# ---------------------------------------------------------------------------

def _tfidf_vectorize(texts: list[str]) -> np.ndarray:
    """
    Build a simple TF-IDF matrix (documents × terms) using numpy only.
    Each row is an L2-normalised vector representing one document.
    """
    # Tokenise
    tokenised = [re.findall(r"[a-z]{2,}", t.lower()) for t in texts]
    vocab: dict[str, int] = {}
    for tokens in tokenised:
        for tok in tokens:
            if tok not in vocab:
                vocab[tok] = len(vocab)

    V = len(vocab)
    N = len(texts)
    if V == 0 or N == 0:
        return np.zeros((N, 1))

    # Term-frequency matrix
    tf = np.zeros((N, V), dtype=np.float32)
    for i, tokens in enumerate(tokenised):
        for tok in tokens:
            tf[i, vocab[tok]] += 1
    # Normalise TF
    row_sums = tf.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    tf = tf / row_sums

    # IDF
    df = (tf > 0).sum(axis=0)
    idf = np.log((N + 1) / (df + 1)) + 1  # smoothed

    tfidf = tf * idf

    # L2 normalise
    norms = np.linalg.norm(tfidf, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return tfidf / norms


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Return cosine similarity matrix (a_rows × b_rows). Both must be L2-normalised."""
    return a @ b.T


def _keyword_embed_domain_labels() -> np.ndarray:
    """
    Create a pseudo-embedding for each domain label using keyword expansion,
    then TF-IDF vectorise them in the same vocabulary space as module texts.
    """
    label_descriptions = {
        "ingestion":      "ingest extract load read source import input data file csv api fetch",
        "transformation": "transform model dbt sql select join filter clean normalize compute calc",
        "serving":        "serve api endpoint route handler request response output export deliver",
        "monitoring":     "monitor alert log metric health check observe trace audit",
        "orchestration":  "orchestrate schedule dag airflow pipeline workflow task trigger",
        "configuration":  "config setting env environment parameter setup initialise credentials",
        "testing":        "test spec fixture mock assert validate check verify unit integration",
        "utilities":      "util helper common shared base abstract mixin tool library support",
    }
    return label_descriptions


def _assign_domain_by_embedding(
    node: ModuleNode,
    all_texts: list[str],
    all_vectors: np.ndarray,
    node_idx: int,
) -> str:
    """
    Assign a domain label to a module using cosine similarity between its
    TF-IDF vector and the domain label pseudo-embeddings.
    """
    label_descriptions = _keyword_embed_domain_labels()
    labels = list(label_descriptions.keys())
    label_texts = list(label_descriptions.values())

    # Build vectors for labels in same space
    combined = all_texts + label_texts
    combined_vecs = _tfidf_vectorize(combined)
    module_vec = combined_vecs[node_idx:node_idx+1]
    label_vecs = combined_vecs[len(all_texts):]

    sims = _cosine_similarity(module_vec, label_vecs)[0]
    best_idx = int(np.argmax(sims))
    return labels[best_idx]


# ---------------------------------------------------------------------------
# Domain clustering — embedding-based + LLM verification
# ---------------------------------------------------------------------------

def cluster_into_domains(
    nodes: list[ModuleNode],
    budget: ContextWindowBudget,
) -> dict[str, str]:
    """
    Cluster modules into business domains using two-stage approach:
    1. TF-IDF cosine similarity over purpose statements (vector embeddings)
    2. LLM verification for uncertain assignments (low similarity score)
    """
    if not nodes:
        return {}

    domain_map: dict[str, str] = {}

    # Stage 1: Build TF-IDF embeddings over all purpose statements
    texts = [
        f"{n.purpose_statement or ''} {n.path} {' '.join(n.exported_functions[:5])}"
        for n in nodes
    ]
    logger.info(f"[Semanticist] Building TF-IDF embeddings for {len(nodes)} modules...")
    vectors = _tfidf_vectorize(texts)

    label_descriptions = _keyword_embed_domain_labels()
    labels = list(label_descriptions.keys())
    label_texts = list(label_descriptions.values())

    # Combine module texts + label texts for shared vocabulary
    combined_vecs = _tfidf_vectorize(texts + label_texts)
    module_vecs = combined_vecs[:len(nodes)]
    label_vecs  = combined_vecs[len(nodes):]

    # Cosine similarity: modules × domain_labels
    sim_matrix = _cosine_similarity(module_vecs, label_vecs)  # (N, 8)
    best_label_idx  = np.argmax(sim_matrix, axis=1)           # (N,)
    best_label_sims = sim_matrix[np.arange(len(nodes)), best_label_idx]  # (N,)

    # Assign embedding-based labels
    UNCERTAIN_THRESHOLD = 0.15  # below this → send to LLM for verification
    uncertain_nodes: list[tuple[int, ModuleNode]] = []

    for i, node in enumerate(nodes):
        sim_score = float(best_label_sims[i])
        assigned  = labels[int(best_label_idx[i])]

        if sim_score >= UNCERTAIN_THRESHOLD:
            domain_map[node.path] = assigned
        else:
            uncertain_nodes.append((i, node))

    logger.info(
        f"[Semanticist] Embedding clustering: {len(domain_map)} assigned, "
        f"{len(uncertain_nodes)} uncertain → sending to LLM"
    )

    # Stage 2: LLM verification for uncertain modules
    classify_batch = 10
    for batch_start in range(0, len(uncertain_nodes), classify_batch):
        batch = uncertain_nodes[batch_start : batch_start + classify_batch]
        items = "\n".join(
            f"{j+1}. File: {n.path}\n   Purpose: {n.purpose_statement or n.path}"
            for j, (_, n) in enumerate(batch)
        )
        prompt = (
            f"Classify each module into exactly one domain from this list:\n"
            f"{', '.join(labels)}\n\n"
            f"Modules:\n{items}\n\n"
            f"Reply ONLY with a JSON array of domain strings, one per module, in order.\n"
            f"Example: [\"ingestion\", \"transformation\", \"utilities\"]\n"
            f"Use only labels from the list above."
        )
        raw    = _llm_call(prompt, model=FAST_MODEL, budget=budget, max_tokens=classify_batch * 20)
        llm_labels = _parse_batch_response(raw, len(batch)) if raw else [None] * len(batch)

        for j, (_, node) in enumerate(batch):
            llm_label = llm_labels[j]
            if llm_label and llm_label.strip().lower() in labels:
                domain_map[node.path] = llm_label.strip().lower()
            else:
                # Final fallback: keyword-based from path
                path_l = node.path.lower()
                if any(k in path_l for k in ("test", "spec", "fixture")):
                    domain_map[node.path] = "testing"
                elif any(k in path_l for k in ("config", "setting", "env")):
                    domain_map[node.path] = "configuration"
                elif any(k in path_l for k in ("ingest", "extract", "load", "read")):
                    domain_map[node.path] = "ingestion"
                elif any(k in path_l for k in ("transform", "model", "dbt", "sql")):
                    domain_map[node.path] = "transformation"
                elif any(k in path_l for k in ("serve", "api", "endpoint", "route")):
                    domain_map[node.path] = "serving"
                elif any(k in path_l for k in ("monitor", "alert", "log", "metric")):
                    domain_map[node.path] = "monitoring"
                else:
                    domain_map[node.path] = "utilities"

        if batch_start + classify_batch < len(uncertain_nodes):
            time.sleep(2)

    # Store embedding vectors on nodes for Navigator semantic search
    for i, node in enumerate(nodes):
        node.embedding = module_vecs[i].tolist()  # stored for downstream use

    return domain_map


# ---------------------------------------------------------------------------
# Day-One question answering
# ---------------------------------------------------------------------------

def _build_synthesis_context(
    module_graph: ModuleGraph,
    lineage_graph: DataLineageGraph,
) -> str:
    hubs     = module_graph.architectural_hubs[:5]
    high_vel = module_graph.high_velocity_files[:5]
    sources  = lineage_graph.sources[:8]
    sinks    = lineage_graph.sinks[:8]
    cycles   = module_graph.circular_dependencies[:3]

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
        lines.append(
            f"  {node.path}  [{node.language}]  domain={node.domain_cluster or '?'}  "
            f"vel={node.change_velocity_30d}  — {purpose}"
        )

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
    context         = _build_synthesis_context(module_graph, lineage_graph)
    questions_block = "\n".join(f"{i+1}. {q}" for i, q in enumerate(FDE_QUESTIONS))

    prompt = (
        f"{context}\n\n"
        f"You are an FDE (Forward Deployed Engineer) who just received this structural "
        f"analysis of an unfamiliar codebase. Answer these five onboarding questions:\n\n"
        f"{questions_block}\n\n"
        f"For each answer:\n"
        f"  - Cite specific file paths and dataset names from the summary above\n"
        f"  - Distinguish STATIC ANALYSIS (certain) from INFERENCE (likely)\n"
        f"  - Keep each answer to 2-4 sentences\n\n"
        f"Format:\nQ1: <answer>\nQ2: <answer>\nQ3: <answer>\nQ4: <answer>\nQ5: <answer>"
    )

    raw = _llm_call(prompt, model=STRONG_MODEL, budget=budget, max_tokens=1200)

    if raw:
        answers: dict[str, str] = {}
        for i, q in enumerate(FDE_QUESTIONS, 1):
            match = re.search(rf"Q{i}:\s*(.*?)(?=Q{i+1}:|$)", raw, re.DOTALL)
            answers[q] = match.group(1).strip() if match else "See structural summary."
        return answers

    hubs     = module_graph.architectural_hubs[:3]
    sources  = lineage_graph.sources[:5]
    sinks    = lineage_graph.sinks[:5]
    high_vel = module_graph.high_velocity_files[:5]

    def _fmt(items: list[str]) -> str:
        return ", ".join(f"`{x}`" for x in items) if items else "none detected"

    return {
        FDE_QUESTIONS[0]: (
            f"Primary ingestion sources detected by static analysis: {_fmt(sources)}. "
            f"These are the nodes with no upstream dependencies in the lineage graph."
        ),
        FDE_QUESTIONS[1]: (
            f"Critical output datasets (terminal sinks in lineage graph): {_fmt(sinks)}."
        ),
        FDE_QUESTIONS[2]: (
            f"Highest blast-radius modules by PageRank centrality: {_fmt(hubs)}. "
            f"Changes to these modules propagate to the most downstream dependents."
        ),
        FDE_QUESTIONS[3]: (
            f"Business logic is concentrated in the top PageRank modules: {_fmt(hubs)}. "
            f"These are the most-imported files and therefore own the core interfaces."
        ),
        FDE_QUESTIONS[4]: (
            f"Highest-velocity files in the last 30 days: {_fmt(high_vel)}. "
            f"These represent the most actively modified parts of the codebase."
        ),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class Semanticist:
    def __init__(
        self,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
        repo_path: Path,
        budget_tokens: int = 200_000,
    ) -> None:
        self.module_graph   = module_graph
        self.lineage_graph  = lineage_graph
        self.repo_path      = repo_path
        self.budget         = ContextWindowBudget(budget_tokens)
        self.drift_flags:    dict[str, str] = {}
        self.domain_map:     dict[str, str] = {}
        self.day_one_answers: dict[str, str] = {}

    def run(self) -> None:
        nodes = list(self.module_graph.nodes.values())
        logger.info(
            f"[Semanticist] Generating purpose statements for {len(nodes)} modules "
            f"(batched, batch_size={BATCH_SIZE}) …"
        )
        logger.info(f"[Semanticist] FAST_MODEL={FAST_MODEL}  STRONG_MODEL={STRONG_MODEL}")

        batch_results = generate_purpose_statements_batched(nodes, self.budget, self.repo_path)
        for node in nodes:
            purpose, drift = batch_results.get(node.path, (_static_purpose(node), None))
            node.purpose_statement = purpose
            if drift:
                self.drift_flags[node.path] = drift
                logger.info(f"[Semanticist] Doc drift: {node.path} — {drift}")

        logger.info(
            f"[Semanticist] Purpose statements done. "
            f"Drift flags: {len(self.drift_flags)}. "
            f"Budget used: ~{self.budget.used_tokens:,} tokens"
        )

        logger.info("[Semanticist] Classifying modules into business domains …")
        self.domain_map = cluster_into_domains(nodes, self.budget)
        for node in nodes:
            node.domain_cluster = self.domain_map.get(node.path)

        logger.info("[Semanticist] Synthesising Day-One FDE answers …")
        self.day_one_answers = answer_day_one_questions(
            self.module_graph, self.lineage_graph, self.budget
        )

        logger.info(f"[Semanticist] Done. Final budget: {self.budget.summary()}")

    def save_trace(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        trace = {
            "drift_flags":     self.drift_flags,
            "domain_map":      self.domain_map,
            "day_one_answers": self.day_one_answers,
            "budget_summary":  self.budget.summary(),
            "call_log":        self.budget.call_log,
            "models": {
                "fast_model":   FAST_MODEL,
                "strong_model": STRONG_MODEL,
            },
        }
        (output_dir / "semanticist_trace.json").write_text(
            json.dumps(trace, indent=2), encoding="utf-8"
        )