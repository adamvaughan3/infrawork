import json
import re
import tempfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
import time

import ansible_runner
import typer
import yaml

from generate_pytest_command import collect_tests
from testinfra_utils import find_ansible_cfg, resolve_inventory_path


def _load_plays(playbook: Path) -> List[dict] | None:
    with playbook.open() as f:
        plays = yaml.safe_load(f) or []
    if not isinstance(plays, list):
        typer.echo("Playbook root should be a list of plays.", err=True)
        return None
    return plays


def _build_jobs(plays: List[dict]) -> List[Dict[str, Any]]:
    return [
        {"role": role, "host": host, "vars": vars_dict}
        for play in plays
        for role, host, vars_dict in collect_tests(play)
    ]


def _load_deps(dep_path: Path) -> Dict[str, List[str]]:
    if not dep_path.exists():
        return {}
    try:
        data = yaml.safe_load(dep_path.read_text()) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _validate_roles(jobs: List[Dict[str, Any]]) -> int | None:
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
    return None


def _prepare_job_metadata(jobs: List[Dict[str, Any]], log_dir: Path):
    roles_root = Path("roles")
    job_logs: Dict[int, Path] = {}
    job_role_paths: Dict[int, Path] = {}
    display_labels: Dict[int, str] = {}
    base_labels: Dict[int, str] = {}
    role_to_ids: Dict[str, List[int]] = {}
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
        role_to_ids.setdefault(job["role"], []).append(idx)

    return job_logs, job_role_paths, display_labels, base_labels, role_to_ids


def _parse_target(raw: str) -> Tuple[str, Dict[str, Any]]:
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


def _vars_match(job_vars: Dict[str, Any], filter_vars: Dict[str, Any]) -> bool:
    if not filter_vars:
        return True
    for k, v in filter_vars.items():
        if job_vars.get(k) != v:
            return False
    return True


def _build_dependency_graph(
    jobs: List[Dict[str, Any]],
    deps_raw: Dict[str, List[str]],
    base_labels: Dict[int, str],
    role_to_ids: Dict[str, List[int]],
    dep_path: Path,
):
    job_vars_list = [job["vars"] for job in jobs]
    missing_targets: List[str] = []
    missing_deps: List[str] = []
    in_degree: Dict[int, int] = {idx: 0 for idx in range(len(jobs))}
    edges: Dict[int, List[int]] = {idx: [] for idx in range(len(jobs))}

    deps_map: Dict[str, List[str]] = {}
    if isinstance(deps_raw, dict):
        deps_map = {str(k): v if isinstance(v, list) else [] for k, v in deps_raw.items()}

    for raw_key, raw_deps in deps_map.items():
        key_base, key_vars = _parse_target(raw_key)
        if "@" in key_base:
            target_pool = [idx for idx in range(len(jobs)) if base_labels[idx] == key_base]
        else:
            target_pool = role_to_ids.get(key_base, [])

        target_ids = [idx for idx in target_pool if _vars_match(jobs[idx]["vars"], key_vars)]
        if not target_ids:
            missing_targets.append(raw_key)
            continue

        for dep_raw in raw_deps:
            dep_base, dep_vars = _parse_target(dep_raw)
            if "@" in dep_base:
                dep_pool = [idx for idx in range(len(jobs)) if base_labels[idx] == dep_base]
                dep_role = dep_base.split("@", 1)[0]
                if not dep_pool and dep_role in role_to_ids:
                    missing_targets.append(dep_raw)
                    continue
            else:
                dep_pool = role_to_ids.get(dep_base, [])

            if not dep_pool:
                missing_deps.append(dep_raw)
                continue

            dep_ids: List[int] = [idx for idx in dep_pool if _vars_match(jobs[idx]["vars"], dep_vars)]
            if dep_vars and not dep_ids:
                missing_deps.append(dep_raw)  # specific dependency (role/host+vars) absent
                continue
            if not dep_vars and not dep_ids:
                missing_deps.append(dep_raw)
                continue

            for target_id in target_ids:
                for dep_id in dep_ids:
                    edges[dep_id].append(target_id)
                    in_degree[target_id] += 1

    if missing_targets:
        typer.echo(
            typer.style("Warning: dependency targets not present in this run:", fg=typer.colors.YELLOW)
            + "\n"
            + "\n".join(f"- {d}" for d in sorted(set(missing_targets))),
            err=True,
        )
        typer.echo("-" * 60)

    if missing_deps:
        typer.echo(
            typer.style(f"Dependencies not found (deps: {dep_path}):", fg=typer.colors.RED, bold=True)
            + "\n"
            + "\n".join(f"- {d}" for d in sorted(set(missing_deps))),
            err=True,
        )
        typer.echo("-" * 60)
        return None, None

    return in_degree, edges


def _run_role_host(
    role: str,
    host: str,
    vars_dict: Dict,
    cfg_path: Path,
    inventory_path: Path,
    log_path: Path,
    role_path: Path,
    dry_run: bool = False,
) -> Tuple[str, str, int]:
    """Execute a single role/host combo with ansible-runner."""
    if dry_run:
        time.sleep(1)
        summary_lines = [
            f"Role   : {role}",
            f"Path   : {role_path}",
            f"Host   : {host}",
            f"Vars   : {vars_dict if vars_dict else '{}'}",
            "Dry run: task not executed, simulated 1s delay.",
            "-" * 60,
            "",
        ]
        with log_path.open("w", encoding="utf-8") as fh:
            fh.write("\n".join(summary_lines))
        return role, host, 0

    with tempfile.TemporaryDirectory(prefix=f"{role}-{host}-") as tmpdir:
        tmp_path = Path(tmpdir)
        playbook_path = tmp_path / "playbook.yml"
        yaml.safe_dump(
            [
                {
                    "hosts": host,
                    "gather_facts": False,
                    "roles": [{"role": role, "vars": vars_dict} if vars_dict else {"role": role}],
                }
            ],
            playbook_path.open("w"),
        )

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


def _run_sequential(playbook: Path, cfg_path: Path, inventory_path: Path) -> int:
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


def _run_parallel(
    jobs: List[Dict[str, Any]],
    deps_raw: Dict[str, List[str]],
    cfg_path: Path,
    inventory_path: Path,
    log_dir: Path,
    max_parallel: int,
    dep_path: Path,
    dry_run: bool = False,
):
    job_logs, job_role_paths, display_labels, base_labels, role_to_ids = _prepare_job_metadata(
        jobs, log_dir
    )

    typer.echo("Planned parallel tasks:")
    for idx in range(len(jobs)):
        typer.echo(f"- {display_labels[idx]} vars={jobs[idx]['vars']}")
    typer.echo("-" * 60)

    graph = _build_dependency_graph(jobs, deps_raw, base_labels, role_to_ids, dep_path)
    if graph == (None, None):
        return 1
    in_degree, edges = graph

    ready = deque([idx for idx, deg in in_degree.items() if deg == 0])
    if not ready:
        typer.echo("Dependency graph has cycles or no starting nodes.", err=True)
        return 1

    typer.echo(f"Launching up to {len(jobs)} tasks in parallel. Logs dir: {log_dir}")

    results = []
    running: set[int] = set()
    launched: set[int] = set()
    stop_submissions = False

    def print_running():
        if running:
            names = [display_labels[i] for i in sorted(running)]
            typer.echo(f"Running ({len(running)}): {', '.join(names)}")
        else:
            typer.echo("Running: none")

    worker_count = max(1, min(max_parallel, len(jobs)))
    started_at = datetime.now()
    timings: Dict[int, Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map: Dict[Any, int] = {}

        def submit_job(idx: int):
            job = jobs[idx]
            timings[idx] = {"start": datetime.now(), "end": None, "duration": None}
            future = executor.submit(
                _run_role_host,
                job["role"],
                job["host"],
                job["vars"],
                cfg_path,
                inventory_path,
                job_logs[idx],
                job_role_paths[idx],
                dry_run,
            )
            future_map[future] = idx
            launched.add(idx)

        while ready or future_map:
            while ready and len(future_map) < worker_count and not stop_submissions:
                idx = ready.popleft()
                submit_job(idx)
                running.add(idx)
            print_running()

            if not future_map:
                break

            done_future = next(as_completed(future_map))
            idx = future_map.pop(done_future)
            running.discard(idx)
            end_time = datetime.now()
            try:
                _, _, rc = done_future.result()
                results.append((display_labels[idx], rc))
                timings[idx]["end"] = end_time
                timings[idx]["duration"] = (
                    timings[idx]["end"] - timings[idx]["start"]
                ).total_seconds()
                if rc != 0:
                    status = typer.style(f"failed (rc={rc})", fg=typer.colors.RED)
                    typer.echo(f"{display_labels[idx]}: {status} (log: {job_logs[idx]})")
                    stop_submissions = True
            except Exception as exc:  # noqa: BLE001
                results.append((display_labels[idx], 1))
                timings[idx]["end"] = end_time
                timings[idx]["duration"] = (
                    timings[idx]["end"] - timings[idx]["start"]
                ).total_seconds()
                typer.echo(
                    f"{display_labels[idx]}: "
                    + typer.style(f"error {exc}", fg=typer.colors.RED, bold=True),
                    err=True,
                )
                stop_submissions = True

            for dep in edges.get(idx, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    ready.append(dep)

    if stop_submissions and len(results) < len(jobs):
        completed = {label for label, _ in results}
        for idx in range(len(jobs)):
            if display_labels[idx] in completed:
                continue
            results.append((f"{display_labels[idx]} (not run)", 1))

    failed = [(label, rc) for label, rc in results if rc != 0]
    succeeded = [label for label, rc in results if rc == 0]

    typer.echo("-" * 60)
    total_duration = (datetime.now() - started_at).total_seconds()
    summary_lines: List[str] = []
    if succeeded:
        summary_lines.append(typer.style("Succeeded:", fg=typer.colors.GREEN))
        summary_lines.extend(
            typer.style(
                f"- {label} ({timings[idx]['duration']:.2f}s)", fg=typer.colors.GREEN
            )
            for idx, label in sorted(
                ((i, lbl) for i, (lbl, rc) in enumerate(results) if rc == 0),
                key=lambda x: x[0],
            )
            if timings.get(idx, {}).get("duration") is not None
        )
    if failed:
        summary_lines.append(typer.style("Failures:", fg=typer.colors.RED, bold=True))
        summary_lines.extend(
            typer.style(f"- {label} rc={rc}", fg=typer.colors.RED) for label, rc in failed
        )
    summary_lines.append(typer.style(f"Total wall time: {total_duration:.2f}s", fg=typer.colors.BLUE))
    typer.echo("\n".join(summary_lines))

    if failed:
        return 1

    typer.echo(typer.style("All role/host tasks completed successfully.", fg=typer.colors.GREEN))
    return 0


def run_playbook(
    playbook: Path,
    max_parallel: int = 5,
    parallel: bool = False,
    deps_file: Path | None = None,
    dry_run: bool = False,
) -> int:
    """Execute roles/hosts from the playbook. Parallel or single-process ansible-runner."""
    cfg_path = Path(find_ansible_cfg())
    inventory_path = Path(resolve_inventory_path())
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    plays = _load_plays(playbook)
    if plays is None:
        return 1

    jobs = _build_jobs(plays)

    dep_path = deps_file or playbook.with_suffix(playbook.suffix + ".deps.yml")
    deps_raw = _load_deps(dep_path)

    role_check = _validate_roles(jobs)
    if role_check:
        return role_check

    if not jobs:
        typer.echo("No roles found in playbook; nothing to run.")
        return 0

    if dry_run:
        parallel = True

    if not parallel:
        return _run_sequential(playbook, cfg_path, inventory_path)

    return _run_parallel(
        jobs,
        deps_raw,
        cfg_path,
        inventory_path,
        log_dir,
        max_parallel,
        dep_path,
        dry_run,
    )
