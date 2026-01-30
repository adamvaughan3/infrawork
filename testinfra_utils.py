import json
import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path

import testinfra.utils.ansible_runner

HOST_VARS = {}


@lru_cache(maxsize=1)
def resolve_inventory_path() -> str:
    """Find ansible.cfg upward from here and return absolute inventory path."""
    for parent in Path(__file__).resolve().parents:
        cfg = parent / "ansible.cfg"
        if cfg.exists():
            parser = ConfigParser()
            parser.read(cfg)
            inv = "inventory"
            for section in ("defaults", "default"):
                if parser.has_section(section):
                    inv = parser.get(section, "inventory", fallback=inv)
                    break
            return str((parent / inv).resolve())
    raise FileNotFoundError("ansible.cfg not found in parent directories")


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
                if vars_dict:
                    existing = HOST_VARS.get(host_name, {})
                    merged = {**existing, **vars_dict}
                    HOST_VARS[host_name] = merged
                hosts.append(host_name)

    if not hosts:
        raise ValueError(f"No hosts matched for role '{role_name}'")

    return hosts


def vars_for_host(hostname: str) -> dict:
    """Return extra vars provided for the given host (if any)."""
    return HOST_VARS.get(hostname, {})
