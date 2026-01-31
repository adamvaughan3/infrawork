from pathlib import Path
from types import SimpleNamespace

import pytest

from playbook_runner import run_playbook


class DummyRunnerResult:
    def __init__(self, rc=0, stdout="ok"):
        self.rc = rc
        self.stdout = stdout


def write_playbook(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "play.yml"
    path.write_text(content)
    return path


def write_deps(tmp_path: Path, deps: str) -> Path:
    path = tmp_path / "play.yml.deps.yml"
    path.write_text(deps)
    return path


@pytest.fixture
def fake_runner(monkeypatch):
    calls = []

    def _run(**kwargs):
        calls.append(kwargs)
        return DummyRunnerResult()

    monkeypatch.setattr("ansible_runner.run", _run)
    return calls


@pytest.fixture
def ordered_runner(monkeypatch):
    calls = []

    def _run(**kwargs):
        # Capture role/host from the temp private_data_dir name: role-host-<suffix>
        pdir = Path(kwargs.get("private_data_dir", ""))
        label = pdir.name.split("-")[0:2]
        calls.append("-".join(label))
        return DummyRunnerResult()

    monkeypatch.setattr("ansible_runner.run", _run)
    return calls


PLAYBOOK_CONTENT = """\
- hosts: mac1
  roles:
    - role1
    - role2
  vars:
    test1: "test1"
"""


def test_parallel_missing_dep_causes_error(tmp_path, fake_runner, capsys):
    playbook = write_playbook(tmp_path, PLAYBOOK_CONTENT)
    deps = write_deps(
        tmp_path,
        "role2:\n"
        "  - missingrole\n",
    )

    rc = run_playbook(playbook, parallel=True, deps_file=deps)

    captured = capsys.readouterr()
    assert "Dependencies not found" in captured.err
    assert rc == 1
    assert fake_runner == []  # never invoked


def test_parallel_missing_target_warns_but_runs(tmp_path, fake_runner, capsys):
    playbook = write_playbook(tmp_path, PLAYBOOK_CONTENT)
    deps = write_deps(
        tmp_path,
        "role2:\n"
        "  - role1@mac2\n",
    )

    rc = run_playbook(playbook, parallel=True, deps_file=deps)

    captured = capsys.readouterr()
    assert "Warning: dependency targets not present" in captured.err
    assert rc == 0
    # role1 and role2 jobs should run
    assert len(fake_runner) == 2


def test_parallel_logs_and_summary(tmp_path, fake_runner):
    playbook = write_playbook(tmp_path, PLAYBOOK_CONTENT)

    rc = run_playbook(playbook, parallel=True, deps_file=None)

    assert rc == 0
    # Two roles -> two calls
    assert len(fake_runner) == 2
    # Logs created for each job
    logs_dir = Path("logs")
    assert (logs_dir / "role1@mac1_0.log").exists()
    assert (logs_dir / "role2@mac1_0.log").exists()


def test_dependency_order_respected(tmp_path, ordered_runner):
    playbook = write_playbook(
        tmp_path,
        """\
- hosts: mac1
  roles:
    - role1
    - role2
""",
    )
    deps = write_deps(
        tmp_path,
        "role2:\n"
        "  - role1\n",
    )

    rc = run_playbook(playbook, parallel=True, deps_file=deps)

    assert rc == 0
    # role1 should be invoked before role2 due to dependency
    assert ordered_runner == ["role1-mac1", "role2-mac1"]
