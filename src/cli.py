from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from src.agents.navigator import Navigator
from src.graph.knowledge_graph import KnowledgeGraph
from src.models import DataLineageGraph, ModuleGraph
from src.orchestrator import Orchestrator

app = typer.Typer(
    name="cartographer",
    help="Brownfield Cartographer — Codebase Intelligence System",
    add_completion=False,
)
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clone_repo(url: str, target: Path) -> Path:
    """Shallow-clone a GitHub URL into the target directory and return the path."""
    console.print(f"[cyan]Cloning[/cyan] {url} ...")
    result = subprocess.run(
        ["git", "clone", "--depth=1", url, str(target)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print(f"[red]Git clone failed:[/red] {result.stderr.strip()}")
        raise typer.Exit(1)
    return target


def _resolve_repo(repo: str) -> tuple[Path, bool]:
    """
    Accept either a local filesystem path or a GitHub URL.
    Returns (local_path, is_temp).
    """
    if repo.startswith(("http://", "https://", "git@")):
        tmp = Path(tempfile.mkdtemp(prefix="cartographer_"))
        clone_path = tmp / "repo"
        _clone_repo(repo, clone_path)
        return clone_path, True

    path = Path(repo).expanduser().resolve()
    if not path.exists():
        console.print(f"[red]Path not found:[/red] {path}")
        raise typer.Exit(1)
    return path, False


def _load_module_graph(path: Path) -> ModuleGraph | None:
    if not path.exists():
        return None
    try:
        return ModuleGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e:
        console.print(f"[yellow]Warning: could not load {path.name}: {e}[/yellow]")
        return None


def _load_lineage_graph(path: Path) -> DataLineageGraph | None:
    if not path.exists():
        return None
    try:
        return DataLineageGraph.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as e:
        console.print(f"[yellow]Warning: could not load {path.name}: {e}[/yellow]")
        return None


def _rebuild_kg(
    module_graph: ModuleGraph | None,
    lineage_graph: DataLineageGraph | None,
) -> KnowledgeGraph:
    kg = KnowledgeGraph()

    if module_graph:
        for node in module_graph.nodes.values():
            kg.add_module(node)
        for edge in module_graph.edges:
            kg.add_module_edge(edge.source, edge.target)

    if lineage_graph:
        for node in lineage_graph.dataset_nodes.values():
            kg.add_dataset(node)
        for node in lineage_graph.transformation_nodes.values():
            kg.add_transformation(node)

    return kg


def _print_analyze_summary(result, output_dir: Path) -> None:
    console.print()
    console.print(
        Panel.fit(
            f"[green]✓ Analysis complete[/green]  ({result.analysis_duration_seconds:.1f}s)\n"
            f"Artifacts → [bold]{output_dir}[/bold]",
            title="Done",
        )
    )

    mg = result.module_graph
    lg = result.lineage_graph

    mg_table = Table(title="Module Graph", show_header=True, header_style="bold magenta")
    mg_table.add_column("Metric", style="cyan")
    mg_table.add_column("Value", justify="right")
    mg_table.add_row("Modules parsed", str(len(mg.nodes)))
    mg_table.add_row("Import edges", str(len(mg.edges)))
    mg_table.add_row("Circular deps", str(len(mg.circular_dependencies)))
    mg_table.add_row("Architectural hubs", str(len(mg.architectural_hubs)))
    mg_table.add_row("High-velocity files", str(len(mg.high_velocity_files)))
    console.print(mg_table)

    if mg.architectural_hubs:
        hub_tree = Tree("[bold]Architectural Hubs (top PageRank)[/bold]")
        for hub in mg.architectural_hubs[:5]:
            score = mg.nodes[hub].pagerank_score if hub in mg.nodes else 0.0
            hub_tree.add(f"{hub} [dim]({score:.5f})[/dim]")
        console.print(hub_tree)

    if mg.circular_dependencies:
        console.print("[yellow]⚠ Circular dependencies detected:[/yellow]")
        for cycle in mg.circular_dependencies[:5]:
            console.print(f"  → {' ↔ '.join(cycle)}")

    lg_table = Table(title="Data Lineage Graph", show_header=True, header_style="bold magenta")
    lg_table.add_column("Metric", style="cyan")
    lg_table.add_column("Value", justify="right")
    lg_table.add_row("Datasets", str(len(lg.dataset_nodes)))
    lg_table.add_row("Transformations", str(len(lg.transformation_nodes)))
    lg_table.add_row("Sources", str(len(lg.sources)))
    lg_table.add_row("Sinks", str(len(lg.sinks)))
    console.print(lg_table)

    if lg.sources:
        console.print(f"[green]Data sources:[/green] {', '.join(sorted(lg.sources)[:10])}")
    if lg.sinks:
        console.print(f"[blue]Data sinks:[/blue] {', '.join(sorted(lg.sinks)[:10])}")

    expected_files = [
        "module_graph.json",
        "lineage_graph.json",
        "CODEBASE.md",
        "onboarding_brief.md",
        "cartography_trace.jsonl",
        "analysis_summary.md",
    ]
    present = [name for name in expected_files if (output_dir / name).exists()]
    if present:
        console.print(f"[bold]Generated:[/bold] {', '.join(present)}")

    if result.warnings:
        console.print(f"\n[yellow]Warnings ({len(result.warnings)}):[/yellow]")
        for warning in result.warnings:
            console.print(f"  • {warning}")

    if result.errors:
        console.print(f"\n[red]Errors ({len(result.errors)}):[/red]")
        for err in result.errors:
            console.print(f"  • {err}")


def _render_json(data: object, title: str = "Result") -> None:
    formatted = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    console.print(Panel.fit(formatted, title=title))


def _print_help() -> None:
    console.print(
        "[bold]Commands:[/bold] "
        "[cyan]find[/cyan] <concept> | "
        "[cyan]lineage[/cyan] <dataset> [upstream|downstream|both] | "
        "[cyan]blast_radius[/cyan] <module> | "
        "[cyan]module[/cyan] <path> | "
        "[cyan]sources[/cyan] | "
        "[cyan]sinks[/cyan] | "
        "[cyan]hubs[/cyan] | "
        "[cyan]help[/cyan] | "
        "[cyan]quit[/cyan]"
    )


# ---------------------------------------------------------------------------
# analyze command
# ---------------------------------------------------------------------------

@app.command()
def analyze(
    repo: str = typer.Argument(..., help="Local path or GitHub URL to analyse"),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output directory for .cartography artifacts (default: <repo>/.cartography/)",
    ),
    incremental: bool = typer.Option(
        False,
        "--incremental",
        "-i",
        help="Only re-analyse files changed since last run",
    ),
) -> None:
    """Run the full analysis pipeline."""
    repo_path, is_temp = _resolve_repo(repo)

    try:
        console.rule("[bold cyan]Brownfield Cartographer — Analysis[/bold cyan]")
        console.print(f"[bold]Target:[/bold] {repo_path}")

        orchestrator = Orchestrator(
            repo_path=repo_path,
            output_dir=output,
            incremental=incremental,
        )
        result = orchestrator.run()
        _print_analyze_summary(result, orchestrator.output_dir)

    finally:
        if is_temp:
            shutil.rmtree(repo_path.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# query command
# ---------------------------------------------------------------------------

@app.command()
def query(
    repo: str = typer.Argument(..., help="Local path to an already-analysed repo"),
    cartography_dir: Optional[Path] = typer.Option(
        None,
        "--cartography-dir",
        "-c",
        help="Path to .cartography output (default: <repo>/.cartography/)",
    ),
) -> None:
    """Interactive query interface over the knowledge graph."""
    repo_path = Path(repo).expanduser().resolve()
    cart_dir = cartography_dir or (repo_path / ".cartography")

    if not cart_dir.exists():
        console.print(f"[red]No .cartography directory found at {cart_dir}[/red]")
        console.print("Run [bold]cartographer analyze[/bold] first.")
        raise typer.Exit(1)

    module_graph = _load_module_graph(cart_dir / "module_graph.json")
    lineage_graph = _load_lineage_graph(cart_dir / "lineage_graph.json")

    if module_graph is None and lineage_graph is None:
        console.print("[red]Could not load module_graph.json or lineage_graph.json[/red]")
        raise typer.Exit(1)

    kg = _rebuild_kg(module_graph, lineage_graph)

    navigator = Navigator(
        repo_path=repo_path,
        module_graph=module_graph or ModuleGraph(target_repo=str(repo_path)),
        lineage_graph=lineage_graph or DataLineageGraph(target_repo=str(repo_path)),
        kg=kg,
    )

    console.rule("[bold cyan]Navigator — Interactive Query[/bold cyan]")
    _print_help()
    console.print()

    while True:
        try:
            raw = console.input("[bold cyan]navigator> [/bold cyan]").strip()
        except (EOFError, KeyboardInterrupt):
            console.print()
            break

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        if cmd in {"quit", "exit", "q"}:
            break

        if cmd == "help":
            _print_help()
            continue

        if cmd == "find":
            arg = raw[len(parts[0]):].strip()
            if not arg:
                console.print("[red]Usage: find <concept>[/red]")
                continue
            result = navigator.find_implementation(arg)
            _render_json(result, title="find_implementation")
            continue

        if cmd == "lineage":
            if len(parts) < 2:
                console.print("[red]Usage: lineage <dataset> [upstream|downstream|both][/red]")
                continue

            dataset = parts[1]
            direction = parts[2].lower() if len(parts) >= 3 else "upstream"
            result = navigator.trace_lineage(dataset, direction)
            _render_json(result, title="trace_lineage")
            continue

        if cmd == "blast_radius":
            arg = raw[len(parts[0]):].strip()
            if not arg:
                console.print("[red]Usage: blast_radius <module_path>[/red]")
                continue
            result = navigator.blast_radius(arg)
            _render_json(result, title="blast_radius")
            continue

        if cmd == "module":
            arg = raw[len(parts[0]):].strip()
            if not arg:
                console.print("[red]Usage: module <path>[/red]")
                continue
            result = navigator.explain_module(arg)
            _render_json(result, title="explain_module")
            continue

        if cmd == "sources":
            sources = sorted((lineage_graph.sources if lineage_graph else []))
            tree = Tree(f"[bold]Sources ({len(sources)})[/bold]")
            for item in sources:
                tree.add(item)
            console.print(tree)
            continue

        if cmd == "sinks":
            sinks = sorted((lineage_graph.sinks if lineage_graph else []))
            tree = Tree(f"[bold]Sinks ({len(sinks)})[/bold]")
            for item in sinks:
                tree.add(item)
            console.print(tree)
            continue

        if cmd == "hubs":
            hubs = (module_graph.architectural_hubs if module_graph else [])[:10]
            tree = Tree(f"[bold]Architectural Hubs ({len(hubs)})[/bold]")
            for hub in hubs:
                score = (
                    module_graph.nodes[hub].pagerank_score
                    if module_graph and hub in module_graph.nodes
                    else 0.0
                )
                tree.add(f"{hub} [dim]({score:.5f})[/dim]")
            console.print(tree)
            continue

        console.print(f"[red]Unknown command:[/red] {cmd}")
        _print_help()


# ---------------------------------------------------------------------------
# summary command
# ---------------------------------------------------------------------------

@app.command()
def summary(
    repo: str = typer.Argument(..., help="Local path to an already-analysed repo"),
    cartography_dir: Optional[Path] = typer.Option(
        None,
        "--cartography-dir",
        "-c",
        help="Path to .cartography output (default: <repo>/.cartography/)",
    ),
) -> None:
    """Print a quick summary of existing artifacts."""
    repo_path = Path(repo).expanduser().resolve()
    cart_dir = cartography_dir or (repo_path / ".cartography")

    if not cart_dir.exists():
        console.print(f"[red]No .cartography directory found at {cart_dir}[/red]")
        raise typer.Exit(1)

    summary_file = cart_dir / "analysis_summary.md"
    if summary_file.exists():
        console.print(summary_file.read_text(encoding="utf-8"))
        return

    module_graph = _load_module_graph(cart_dir / "module_graph.json")
    lineage_graph = _load_lineage_graph(cart_dir / "lineage_graph.json")

    if module_graph is None and lineage_graph is None:
        console.print("[yellow]No summary or graph artifacts found.[/yellow]")
        raise typer.Exit(1)

    table = Table(title="Cartography Summary", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    if module_graph:
        table.add_row("Modules", str(len(module_graph.nodes)))
        table.add_row("Import edges", str(len(module_graph.edges)))
        table.add_row("Circular dependencies", str(len(module_graph.circular_dependencies)))
        table.add_row("Architectural hubs", str(len(module_graph.architectural_hubs)))

    if lineage_graph:
        table.add_row("Datasets", str(len(lineage_graph.dataset_nodes)))
        table.add_row("Transformations", str(len(lineage_graph.transformation_nodes)))
        table.add_row("Sources", str(len(lineage_graph.sources)))
        table.add_row("Sinks", str(len(lineage_graph.sinks)))

    console.print(table)


def main() -> None:
    app()


if __name__ == "__main__":
    main()