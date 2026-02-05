from pathlib import Path

from conftest import _role_from_path
from testinfra_utils import _resolve_role_dir


def test_role_from_path_supports_nested_roles():
    path = Path("roles/nested/role3/tests/test_role3.py")
    assert _role_from_path(str(path)) == "role3"


def test_resolve_role_dir_finds_nested_role():
    role_dir = _resolve_role_dir("role3")
    assert role_dir.as_posix().endswith("roles/nested/role3")
