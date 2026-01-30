import json
import re
import tempfile
from collections import deque
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


def run_playbook(
    playbook: Path, max_parallel: int = 5, parallel: bool = False, deps_file: Path | None = None
) -> int:
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

    dep_path = deps_file or playbook.with_suffix(playbook.suffix + ".deps.yml")
    deps_raw = {}
    if dep_path.exists():
        try:
            deps_raw = yaml.safe_load(dep_path.read_text()) or {}
        except Exception:
            deps_raw = {}

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

    # Resolve role path once for summary logging and prepare labels
    job_logs: Dict[int, Path] = {}
    job_role_paths: Dict[int, Path] = {}
    display_labels: Dict[int, str] = {}
    base_labels: Dict[int, str] = {}
    log_label_to_id: Dict[str, int] = {}
    base_to_ids: Dict[str, List[int]] = {}
    counts: Dict[str, int] = {}

    for idx, job in enumerate(jobs):
        base_label = f"{job['role']}@{job['host']}"
        counts[base_label] = counts.get(base_label, 0) + 1
        run_idx = counts[base_label] - 1
        log_label = f"{base_label}#{run_idx}"
        display_label = base_label if counts[base_label] == 1 else f"{base_label} (run {counts[base_label]})"

        safe = "".join(c if c.isalnum() or c in "-_@" else "_" for c in log_label)
        job_logs[idx] = log_dir / f"{safe}.log"
        candidate = roles_root / job["role"]
        job_role_paths[idx] = candidate if candidate.exists() else Path(f"(not found: {candidate})")

        display_labels[idx] = display_label
        base_labels[idx] = base_label
        log_label_to_id[log_label] = idx
        base_to_ids.setdefault(base_label, []).append(idx)

    def parse_target(raw: str) -> Tuple[str, Dict[str, Any]]:
        if not isinstance(raw, str):
            return str(raw), {}
        if ":vars=" in raw:
            base, var_part = raw.split(":vars=", 1)
            try:
                vars_filter = json.loads(var_part)
            except Exception:
                vars_filter = {}
            return base, vars_filter if isinstance(vars_filter, dict) else {}
        return raw, {}

    missing_deps = []
    in_degree: Dict[int, int] = {idx: 0 for idx in range(len(jobs))}
    edges: Dict[int, List[int]] = {idx: [] for idx in range(len(jobs))}

    # Build dependency graph if provided
    deps_map: Dict[str, List[str]] = {}
    if isinstance(deps_raw, dict):
        deps_map = {str(k): v if isinstance(v, list) else [] for k, v in deps_raw.items()}

    # Iterate over dependency declarations and apply to matching jobs
    for raw_key, raw_deps in deps_map.items():
        key_base, key_vars = parse_target(raw_key)
        target_ids = [
            idx
            for idx, job in enumerate(jobs)
            if base_labels[idx] == key_base
            and (not key_vars or key_vars == job["vars"])
        ]
        if not target_ids:
            missing_deps.append(raw_key)
            continue

        for dep_raw in raw_deps:
            dep_base, dep_vars = parse_target(dep_raw)
            dep_ids: List[int] = [
                idx
                for idx, job in enumerate(jobs)
                if base_labels[idx] == dep_base
                and (not dep_vars or dep_vars == job["vars"])
            ]
            if not dep_ids:
                missing_deps.append(dep_raw)
                continue

            for target_id in target_ids:
                for dep_id in dep_ids:
                    edges[dep_id].append(target_id)
                    in_degree[target_id] += 1

    if missing_deps:
        typer.echo(
            "Missing dependency targets:\n" + "\n".join(f"- {d}" for d in sorted(set(missing_deps))),
            err=True,
        )
        return 1

    ready = deque([idx for idx, deg in in_degree.items() if deg == 0])
    if not ready:
        typer.echo("Dependency graph has cycles or no starting nodes.", err=True)
        return 1

    typer.echo(f"Launching {len(jobs)} tasks in parallel. Logs dir: {log_dir}")

    results = []
    running: set[int] = set()

    def print_running():
        if running:
            names = [display_labels[i] for i in sorted(running)]
            typer.echo(f"Running ({len(running)}): {', '.join(names)}")
        else:
            typer.echo("Running: none")

    worker_count = max(1, min(max_parallel, len(jobs)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map: Dict[Any, int] = {}

        def submit_job(idx: int):
            job = jobs[idx]
            future = executor.submit(
                _run_role_host,
                job["role"],
                job["host"],
                job["vars"],
                cfg_path,
                inventory_path,
                job_logs[idx],
                job_role_paths[idx],
            )
            future_map[future] = idx

        while ready or future_map:
            while ready and len(future_map) < worker_count:
                idx = ready.popleft()
                submit_job(idx)
                running.add(idx)
            print_running()

            if not future_map:
                break

            done_future = next(as_completed(future_map))
            idx = future_map.pop(done_future)
            running.discard(idx)
            try:
                _, _, rc = done_future.result()
                results.append((display_labels[idx], rc))
                status = "ok" if rc == 0 else f"failed (rc={rc})"
                typer.echo(f"{display_labels[idx]}: {status} (log: {job_logs[idx]})")
            except Exception as exc:  # noqa: BLE001
                results.append((display_labels[idx], 1))
                typer.echo(f"{display_labels[idx]}: error {exc}", err=True)

            for dep in edges.get(idx, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    ready.append(dep)

    failed = [(label, rc) for label, rc in results if rc != 0]
    if failed:
        typer.echo(
            "Failures:\n" + "\n".join(f"- {label} rc={rc}" for label, rc in failed),
            err=True,
        )
        return 1

    typer.echo("All role/host tasks completed successfully.")
    return 0
