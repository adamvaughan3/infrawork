from testinfra_utils import hosts_for_role


testinfra_hosts = hosts_for_role("role2")


def test_hosts_file(host):
    hosts_file = host.file("/etc/hosts")
    assert hosts_file.exists
    assert hosts_file.is_file
    assert hosts_file.user == "root"
