import json
import os
from pathlib import Path
from typing import Optional

import pytest
from testinfra_utils import vars_for_target


def pytest_addoption(parser):
    parser.addoption(
        "--test",
        action="append",
        dest="test_targets",
        default=[],
        metavar="ROLE:HOST",
        help="Specify role:host mapping (e.g. --test role1:mac1). Can be used multiple times.",
    )


def pytest_configure(config):
    raw_mappings = config.getoption("test_targets") or []
    mapping = {}
    for entry in raw_mappings:
        parts = entry.split(":", 2)
        if len(parts) < 2:
            raise ValueError(f"Invalid --test entry '{entry}', expected ROLE:HOST format")

        role, host_part = parts[0].strip(), parts[1].strip()
        if not role or not host_part:
            raise ValueError(f"Invalid --test entry '{entry}', expected ROLE:HOST format")

        hosts = [h.strip() for h in host_part.split(",") if h.strip()]
        if not hosts:
            raise ValueError(f"Invalid host list in --test entry '{entry}'")

        vars_dict = {}
        if len(parts) == 3 and parts[2].startswith("vars="):
            raw_vars = parts[2][5:]
            try:
                vars_dict = json.loads(raw_vars)
                if not isinstance(vars_dict, dict):
                    raise ValueError
            except ValueError:
                raise ValueError(
                    f"Invalid vars payload in --test entry '{entry}', expected JSON object"
                )

        mapping.setdefault(role, []).append({"hosts": hosts, "vars": vars_dict})

    os.environ["TESTINFRA_ROLE_TARGETS"] = json.dumps(mapping)
    config._test_targets_mapping = mapping


def _role_from_path(path: str) -> Optional[str]:
    parts = Path(path).parts
    if "roles" not in parts:
        return None
    idx = parts.index("roles")
    if idx + 1 < len(parts):
        return parts[idx + 1]
    return None


def pytest_collection_modifyitems(config, items):
    mapping = getattr(config, "_test_targets_mapping", {})
    if not mapping:
        return

    requested_roles = set(mapping.keys())
    kept = []
    deselected = []
    for item in items:
        role = _role_from_path(str(item.path))
        if role and role not in requested_roles:
            deselected.append(item)
        else:
            kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept


@pytest.fixture
def host_vars(host, request):
    """Vars provided via --test for the current host."""
    role = _role_from_path(str(request.node.path)) or ""
    return vars_for_target(role, host.backend.get_hostname())
