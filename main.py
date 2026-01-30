import shlex
from pathlib import Path

import pytest
import typer
import yaml

from generate_pytest_command import REPORT_PATH, build_pytest_args

app = typer.Typer(name="infrawork", help="Infrawork CLI for running testinfra from playbooks.")

@app.command("test")
def test_command(
    playbook: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False),
    report: bool = typer.Option(
        False,
        "--report",
        help="Generate an HTML report in reports/report.html",
    ),
):
    """Run testinfra/pytest for each role/host defined in the playbook."""
    if report:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with playbook.open() as f:
        plays = yaml.safe_load(f) or []

    if not isinstance(plays, list):
        typer.echo("Playbook root should be a list of plays.", err=True)
        raise typer.Exit(code=1)

    pytest_args = build_pytest_args(plays, include_report=report)
    if not any(arg == "--test" for arg in pytest_args):
        typer.echo("No roles found in playbook; nothing to run.")
        raise typer.Exit(code=0)

    typer.echo(f"Running: pytest {' '.join(shlex.quote(a) for a in pytest_args)}")
    result = pytest.main(pytest_args)
    raise typer.Exit(code=result)

@app.command("run")
def run_command():
    """Dummy run command."""
    typer.echo("Run command invoked.")

if __name__ == "__main__":
    app()
