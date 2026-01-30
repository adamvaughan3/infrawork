#!/usr/bin/env python3
"""
Generate a pytest command from an Ansible playbook that exercises each role on
its target hosts with any provided vars.

Usage:
    python generate_pytest_command.py playbooks/main.yml
"""
import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml

REPORT_DIR = Path("reports")
REPORT_PATH = REPORT_DIR / "report.html"


def normalize_hosts(hosts_field) -> List[str]:
    """Turn the play's hosts field into a list of host patterns."""
    if hosts_field is None:
        return []
    if isinstance(hosts_field, str):
        return [h.strip() for h in hosts_field.split(",") if h.strip()]
    if isinstance(hosts_field, list):
        hosts: List[str] = []
        for item in hosts_field:
            hosts.extend(normalize_hosts(item))
        return hosts
    return []


def extract_role(entry) -> Tuple[str, Dict]:
    """Extract role name and role vars from a roles entry."""
    if isinstance(entry, str):
        return entry, {}
    if not isinstance(entry, dict):
        return "", {}

    role_name = entry.get("role") or entry.get("name")
    if not role_name and len(entry) == 1:
        role_name = next(iter(entry))
    vars_dict = entry.get("vars") or {}
    return role_name or "", vars_dict


def collect_tests(play: dict) -> Iterable[Tuple[str, str, Dict]]:
    """Yield (role, host, vars) tuples for a play."""
    play_vars = play.get("vars") or {}
    hosts = normalize_hosts(play.get("hosts"))
    roles = play.get("roles") or []

    for role_entry in roles:
        role_name, role_vars = extract_role(role_entry)
        if not role_name:
            continue
        merged_vars = {**play_vars, **role_vars}
        for host in hosts:
            yield role_name, host, merged_vars


def build_pytest_args(
    plays: List[dict], include_pytest_executable: bool = False, include_report: bool = False
) -> List[str]:
    args = []
    if include_pytest_executable:
        args.append("pytest")
    args.extend(["-v", "-n", "auto", "--connection=ansible", "--force-ansible"])

    if include_report:
        args.extend(
            [
                "--html",
                str(REPORT_PATH),
                "--self-contained-html",
            ]
        )

    for play in plays:
        for role_name, host, vars_dict in collect_tests(play):
            arg = f"{role_name}:{host}"
            if vars_dict:
                arg += f":vars={json.dumps(vars_dict, separators=(',', ':'))}"
            args.extend(["--test", arg])

    return args


def build_command(plays: List[dict], include_report: bool = False) -> List[str]:
    """Backward-compatible helper that includes 'pytest' as the first element."""
    return build_pytest_args(
        plays, include_pytest_executable=True, include_report=include_report
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate pytest command from an Ansible playbook"
    )
    parser.add_argument(
        "playbook",
        type=Path,
        help="Path to playbook YAML (e.g. playbooks/main.yml)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Include HTML report (reports/report.html)",
    )
    args = parser.parse_args()

    if not args.playbook.exists():
        print(f"Playbook not found: {args.playbook}", file=sys.stderr)
        return 1

    with args.playbook.open() as f:
        plays = yaml.safe_load(f) or []

    if not isinstance(plays, list):
        print("Playbook root should be a list of plays", file=sys.stderr)
        return 1

    cmd_parts = build_command(plays, include_report=args.report)
    if not any(part == "--test" for part in cmd_parts):
        print("No roles found in playbook; nothing to run.")
        return 0

    printable = " ".join(shlex.quote(part) for part in cmd_parts)
    print(printable)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
