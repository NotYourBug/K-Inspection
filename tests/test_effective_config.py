import pytest
from k8s_inspector.core import config


def test_cli_default_does_not_override_file_config():
    cfg = config.resolve_config({'cluster_id': 'file-cluster', 'profile': 'hourly'},
                                {'INSPECTOR_CLUSTER': 'env-cluster'}, {})
    assert cfg.cluster_id == 'env-cluster' and cfg.profile == 'hourly'


def test_explicit_cli_wins():
    cfg = config.resolve_config({'cluster_id': 'file'}, {'INSPECTOR_CLUSTER': 'env'},
                                {'cluster_id': 'cli', 'profile': 'dataplane'})
    assert cfg.cluster_id == 'cli' and cfg.profile == '5m'


@pytest.mark.parametrize('data', [
    {'timeouts': {'connect': -1}}, {'deadlines': {'5m': float('nan')}},
    {'profile': 'typo'}, {'cluster_id': ' '}, {1: 1, 'bad': 2},
])
def test_invalid_effective_configuration_is_friendly_error(data):
    with pytest.raises(ValueError):
        config.resolve_config(data, {}, {})


def test_cli_defaults_leave_file_profile_effective():
    from k8s_inspector.cli import build_parser
    args = build_parser().parse_args(['run'])
    cfg = config.resolve_config({'profile': '5m'}, {}, {'profile': args.profile})
    assert cfg.profile == '5m'
