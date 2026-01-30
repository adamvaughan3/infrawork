import pytest

from testinfra_utils import hosts_for_role


testinfra_hosts = hosts_for_role("role1")


def test_hosts_file(host):
    hosts_file = host.file("/etc/hosts")
    assert hosts_file.exists
    assert hosts_file.is_file
    assert hosts_file.user == "root"


def test_cli_vars_pass_through(host_vars):
    """Demonstrate vars passed via --test role:host:vars=... are available."""
    if not host_vars:
        pytest.skip("No vars provided via --test for this host")

    # Assert the vars passed via --test are visible to the test context
    assert host_vars.get("test1") == "test1"
    assert host_vars.get("test2") == "test2"
