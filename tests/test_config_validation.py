"""Invalid configuration must fail before any cluster connection."""
import json

import pytest

from k8s_inspector.config import load_config
from k8s_inspector.cli import main


@pytest.fixture
def config_file(tmp_path):
    def write(data):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path
    return write


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1, 1.1, True, "0.8"])
def test_invalid_cpu_ratio_is_rejected(value, config_file):
    with pytest.raises(ValueError, match="node_cpu_utilization"):
        load_config(str(config_file({"thresholds": {"node_cpu_utilization": value}})))


def test_unknown_config_key_is_rejected(config_file):
    with pytest.raises(ValueError, match="pod_unready_grace_second"):
        load_config(str(config_file({"pod_unready_grace_second": 120})))


@pytest.mark.parametrize("data", [
    [], None, {"thresholds": []}, {"thresholds": {"pod_restart_count": 1.5}},
    {"thresholds": {"warning_events_max": True}},
    {"thresholds": {"pod_restart_count": -1}},
    {"thresholds": {"typo": 1}}, {"control_plane_namespace": ""},
    {"allow_noschedule_taints": "one"}, {"allow_noschedule_taints": [1]},
    {"pod_unready_grace_seconds": -1}, {"pvc_pending_grace_seconds": float("inf")},
])
def test_invalid_shapes_and_values_are_rejected(data, config_file):
    with pytest.raises(ValueError):
        load_config(str(config_file(data)))


def test_grace_defaults_and_overrides(config_file):
    cfg = load_config(None)
    assert (cfg.pod_unready_grace_seconds, cfg.pod_pending_grace_seconds,
            cfg.pvc_pending_grace_seconds) == (120, 300, 300)
    cfg = load_config(str(config_file({"pod_unready_grace_seconds": 60})))
    assert cfg.pod_unready_grace_seconds == 60


def test_cli_invalid_config_does_not_initialize_kubernetes(config_file, monkeypatch, capsys):
    def forbidden_connection(**kwargs):
        pytest.fail("invalid config must not initialize Kubernetes")
    monkeypatch.setattr("k8s_inspector.cli.load_kube", forbidden_connection)
    result = main(["run", "--config", str(config_file({"thresholds": {"pod_restart_count": -1}}))])
    assert result == 1
    assert "pod_restart_count" in capsys.readouterr().err
