import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

import ansible_runner
import typer
import yaml

from generate_pytest_command import collect_tests
from testinfra_utils import find_ansible_cfg, resolve_inventory_path


def _render_play(role: str, host: str, vars_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build a minimal playbook for a single role/host combo."""
    role_entry: Dict[str, Any] = {"role": role}
    if vars_dict:
        role_entry["vars"] = vars_dict
    return [
        {
            "hosts": host,
            "gather_facts": False,
            "roles": [role_entry],
        }
    ]


def _run_role_host(
    role: str,
    host: str,
    vars_dict: Dict,
    cfg_path: Path,
    inventory_path: Path,
    log_path: Path,
    role_path: Path,
) -> Tuple[str, str, int]:
    """Execute a single role/host combo with ansible-runner."""
    with tempfile.TemporaryDirectory(prefix=f"{role}-{host}-") as tmpdir:
        tmp_path = Path(tmpdir)
        playbook_path = tmp_path / "playbook.yml"
        yaml.safe_dump(_render_play(role, host, vars_dict), playbook_path.open("w"))

        env_vars = {
            "ANSIBLE_CONFIG": str(cfg_path),
            "ANSIBLE_FORCE_COLOR": "0",
            "ANSIBLE_NOCOLOR": "1",
        }

        r = ansible_runner.run(
            private_data_dir=tmpdir,
            playbook=str(playbook_path),
            inventory=str(inventory_path),
            envvars=env_vars,
            quiet=True,
        )
        stdout_data = r.stdout
        if hasattr(stdout_data, "read"):
            try:
                stdout_data = stdout_data.read()
            except Exception:
                stdout_data = ""
        if stdout_data is None:
            stdout_text = ""
        elif isinstance(stdout_data, bytes):
            stdout_text = stdout_data.decode("utf-8", errors="ignore")
        else:
            stdout_text = str(stdout_data)
        # Strip ANSI escape sequences for clean logs
        stdout_text = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", stdout_text)
        summary_lines = [
            f"Role   : {role}",
            f"Path   : {role_path}",
            f"Host   : {host}",
            f"Vars   : {vars_dict if vars_dict else '{}'}",
            "-" * 60,
            "",
        ]
        with log_path.open("w", encoding="utf-8") as fh:
            fh.write("\n".join(summary_lines))
            fh.write(stdout_text)
        return role, host, r.rc or 0


def run_playbook(playbook: Path, max_parallel: int = 5, parallel: bool = False) -> int:
    """Execute roles/hosts from the playbook. Parallel or single-process ansible-runner."""
    cfg_path = Path(find_ansible_cfg())
    inventory_path = Path(resolve_inventory_path())
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    with playbook.open() as f:
        plays = yaml.safe_load(f) or []

    if not isinstance(plays, list):
        typer.echo("Playbook root should be a list of plays.", err=True)
        return 1

    jobs = [
        {"role": role, "host": host, "vars": vars_dict}
        for play in plays
        for role, host, vars_dict in collect_tests(play)
    ]

    # Validate roles exist before running
    roles_root = Path("roles")
    missing_roles = sorted(
        {job["role"] for job in jobs if not (roles_root / job["role"]).exists()}
    )
    if missing_roles:
        typer.echo(
            "Missing roles:\n" + "\n".join(f"- {r}" for r in missing_roles),
            err=True,
        )
        return 1

    if not jobs:
        typer.echo("No roles found in playbook; nothing to run.")
        return 0

    # Resolve role path once for summary logging
    roles_root = Path("roles")

    # Produce unique, file-safe log names for each job (handles duplicate role/host with different vars)
    job_logs = {}
    job_labels = []
    job_role_paths = {}
    for idx, job in enumerate(jobs):
        base_label = f"{job['role']}@{job['host']}#{idx}"
        safe = "".join(c if c.isalnum() or c in "-_@" else "_" for c in base_label)
        job_logs[idx] = log_dir / f"{safe}.log"
        job_labels.append(base_label)
        candidate = roles_root / job["role"]
        job_role_paths[idx] = candidate if candidate.exists() else Path(f"(not found: {candidate})")

    if not parallel:
        # Single ansible-runner invocation using the original playbook
        typer.echo("Running playbook sequentially with ansible-runner...")
        pdir = playbook.parent.resolve()
        r = ansible_runner.run(
            private_data_dir=str(pdir),
            playbook=playbook.name,
            inventory=str(inventory_path),
            envvars={"ANSIBLE_CONFIG": str(cfg_path)},
            quiet=False,
        )
        return r.rc or 0

    typer.echo(f"Launching {len(jobs)} tasks in parallel. Logs dir: {log_dir}")

    results = []
    running = set(job_labels)

    def print_running():
        if running:
            typer.echo(f"Running ({len(running)}): {', '.join(sorted(running))}")
        else:
            typer.echo("Running: none")

    worker_count = max(1, min(max_parallel, len(jobs)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _run_role_host,
                job["role"],
                job["host"],
                job["vars"],
                cfg_path,
                inventory_path,
                job_logs[idx],
                job_role_paths[idx],
            ): (idx, job)
            for idx, job in enumerate(jobs)
        }
        print_running()
        for future in as_completed(future_map):
            job_idx, job = future_map[future]
            label = job_labels[job_idx]
            try:
                _, _, rc = future.result()
                results.append((label, rc))
                status = "ok" if rc == 0 else f"failed (rc={rc})"
                typer.echo(f"{label}: {status} (log: {job_logs[job_idx]})")
            except Exception as exc:  # noqa: BLE001
                results.append((label, 1))
                typer.echo(f"{label}: error {exc}", err=True)
            running.discard(label)
            print_running()

    failed = [(label, rc) for label, rc in results if rc != 0]
    if failed:
        typer.echo(
            "Failures:\n" + "\n".join(f"- {label} rc={rc}" for label, rc in failed),
            err=True,
        )
        return 1

    typer.echo("All role/host tasks completed successfully.")
    return 0
