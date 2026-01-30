import json
import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path
from typing import Dict

import testinfra.utils.ansible_runner

# Nested mapping: role -> host -> vars
HOST_VARS: Dict[str, Dict[str, dict]] = {}


@lru_cache(maxsize=1)
def find_ansible_cfg() -> str:
    """Locate ansible.cfg upward from this file."""
    for parent in Path(__file__).resolve().parents:
        cfg = parent / "ansible.cfg"
        if cfg.exists():
            return str(cfg)
    raise FileNotFoundError("ansible.cfg not found in parent directories")


@lru_cache(maxsize=1)
def resolve_inventory_path() -> str:
    """Find ansible.cfg upward from here and return absolute inventory path."""
    cfg_path = Path(find_ansible_cfg())
    parent = cfg_path.parent
    parser = ConfigParser()
    parser.read(cfg_path)
    inv = "inventory"
    for section in ("defaults", "default"):
        if parser.has_section(section):
            inv = parser.get(section, "inventory", fallback=inv)
            break
    return str((parent / inv).resolve())


def _load_yaml(path: Path) -> dict:
    import yaml

    if not path.exists():
        return {}
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        return {}
    return data


@lru_cache(maxsize=None)
def load_role_defaults(role_name: str) -> dict:
    role_dir = Path("roles") / role_name
    return _load_yaml(role_dir / "defaults" / "main.yml")


@lru_cache(maxsize=None)
def load_role_vars(role_name: str) -> dict:
    role_dir = Path("roles") / role_name
    return _load_yaml(role_dir / "vars" / "main.yml")


def hosts_for_role(role_name: str):
    """Return host list for a role using the mapping provided via --test.

    If --test was provided and the role is not included, no hosts are returned
    so those tests are effectively skipped.
    """
    mapping = json.loads(os.environ.get("TESTINFRA_ROLE_TARGETS", "{}"))
    if mapping and role_name not in mapping:
        return []

    runner = testinfra.utils.ansible_runner.AnsibleRunner(resolve_inventory_path())
    entries = mapping.get(role_name, [{"hosts": ["all"], "vars": {}}])

    # Normalize legacy string form to dict
    normalized = []
    for entry in entries:
        if isinstance(entry, str):
            normalized.append({"hosts": [entry], "vars": {}})
        else:
            normalized.append(entry)

    base_vars = {**load_role_defaults(role_name), **load_role_vars(role_name)}

    hosts = []
    for entry in normalized:
        patterns = entry.get("hosts") or [entry.get("host", "all")]
        if isinstance(patterns, str):
            patterns = [patterns]
        vars_dict = entry.get("vars") or {}

        for pattern in patterns:
            matched = runner.get_hosts(pattern)
            if not matched:
                raise ValueError(f"No hosts matched pattern '{pattern}' for role '{role_name}'")

            for host_name in matched:
                merged = {**base_vars}
                existing = HOST_VARS.get(role_name, {}).get(host_name, {})
                merged.update(existing)
                merged.update(vars_dict)

                role_entry = HOST_VARS.setdefault(role_name, {})
                role_entry[host_name] = merged
                hosts.append(host_name)

    if not hosts:
        raise ValueError(f"No hosts matched for role '{role_name}'")

    return hosts


def vars_for_target(role_name: str, hostname: str) -> dict:
    """Return vars for a given role/host combination."""
    return HOST_VARS.get(role_name, {}).get(hostname, {})
