"""Tests for k8s_inspector.core.config."""

import json
import tempfile
from pathlib import Path

from k8s_inspector.core.config import InspectorConfig, Thresholds, load_config


class TestDefaults:
    def test_default_thresholds(self):
        cfg = load_config(None)
        assert cfg.thresholds.node_cpu_utilization == 0.80
        assert cfg.thresholds.node_memory_utilization == 0.85
        assert cfg.thresholds.pod_restart_count == 3
        assert cfg.thresholds.warning_events_max == 50

    def test_default_namespace(self):
        cfg = load_config(None)
        assert cfg.control_plane_namespace == "kube-system"

    def test_default_allowlist(self):
        cfg = load_config(None)
        assert cfg.allow_noschedule_taints == []

    def test_thresholds_are_independent_instances(self):
        cfg1 = InspectorConfig()
        cfg2 = InspectorConfig()
        assert cfg1.thresholds is not cfg2.thresholds


class TestLoadConfigJson:
    def test_override_thresholds(self):
        data = {"thresholds": {"node_cpu_utilization": 0.90, "pod_restart_count": 5}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name
        try:
            cfg = load_config(path)
            assert cfg.thresholds.node_cpu_utilization == 0.90
            assert cfg.thresholds.pod_restart_count == 5
            # Unspecified thresholds keep defaults
            assert cfg.thresholds.node_memory_utilization == 0.85
            assert cfg.thresholds.warning_events_max == 50
        finally:
            Path(path).unlink()

    def test_override_namespace_and_taints(self):
        data = {
            "control_plane_namespace": "kube-cp",
            "allow_noschedule_taints": ["node-role.kubernetes.io/control-plane"],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name
        try:
            cfg = load_config(path)
            assert cfg.control_plane_namespace == "kube-cp"
            assert cfg.allow_noschedule_taints == ["node-role.kubernetes.io/control-plane"]
        finally:
            Path(path).unlink()

    def test_empty_file_uses_defaults(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{}")
            path = f.name
        try:
            cfg = load_config(path)
            assert cfg.thresholds.node_cpu_utilization == 0.80
            assert cfg.control_plane_namespace == "kube-system"
        finally:
            Path(path).unlink()
