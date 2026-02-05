from generate_pytest_command import build_pytest_args


def test_build_pytest_args_sets_default_parallel_to_auto():
    plays = [
        {"hosts": "mac1", "roles": ["role1"], "vars": {"foo": "bar"}},
    ]
    args = build_pytest_args(plays)
    assert "-n" in args
    assert args[args.index("-n") + 1] == "auto"


def test_build_pytest_args_contains_tests_entries():
    plays = [
        {"hosts": "mac1,mac2", "roles": ["role1", "role2"], "vars": {"k": "v"}},
    ]
    args = build_pytest_args(plays)
    test_args = [a for a in args if a.startswith("role") and ":" in a]
    assert len(test_args) == 4  # 2 roles * 2 hosts


def test_build_pytest_args_normalizes_nested_role_name():
    plays = [
        {"hosts": "mac1", "roles": ["nested/role3"]},
    ]
    args = build_pytest_args(plays)
    test_args = [a for a in args if a.startswith("role") and ":" in a]
    assert test_args == ["role3:mac1"]
