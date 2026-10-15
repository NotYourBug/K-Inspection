"""
Configuration loading.

Provides sensible defaults so the inspector runs without any config file.
Pass --config with a JSON or YAML file to override thresholds or namespaces.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .common import deep_get


@dataclass
class Thresholds:
    """Inspection thresholds (overridable via config file)."""
    node_cpu_utilization: float = 0.80
    node_memory_utilization: float = 0.85
    pod_restart_count: int = 3
    warning_events_max: int = 50


@dataclass
class InspectorConfig:
    """
    Top-level inspection config.

    control_plane_namespace:
      Control plane components typically live in kube-system, but may differ.

    allow_noschedule_taints:
      NoSchedule taint keys that are considered normal (e.g. control-plane node taint).
      These will not trigger a WARN during node inspection.
    """
    thresholds: Thresholds = field(default_factory=Thresholds)
    control_plane_namespace: str = "kube-system"
    allow_noschedule_taints: list[str] = field(default_factory=list)


def load_config(path: str | None) -> InspectorConfig:
    """Load config from JSON/YAML file and merge with defaults."""
    cfg = InspectorConfig()
    if not path:
        return cfg

    p = Path(path)
    raw = p.read_text(encoding="utf-8")
    data: dict[str, Any]

    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError("PyYAML is required to read YAML config") from e
        data = yaml.safe_load(raw) or {}
    else:
        data = json.loads(raw) if raw.strip() else {}

    cfg.control_plane_namespace = deep_get(
        data, ["control_plane_namespace"], cfg.control_plane_namespace
    )
    cfg.allow_noschedule_taints = deep_get(
        data, ["allow_noschedule_taints"], cfg.allow_noschedule_taints
    )

    t = cfg.thresholds
    t.node_cpu_utilization = float(
        deep_get(data, ["thresholds", "node_cpu_utilization"], t.node_cpu_utilization)
    )
    t.node_memory_utilization = float(
        deep_get(data, ["thresholds", "node_memory_utilization"], t.node_memory_utilization)
    )
    t.pod_restart_count = int(
        deep_get(data, ["thresholds", "pod_restart_count"], t.pod_restart_count)
    )
    t.warning_events_max = int(
        deep_get(data, ["thresholds", "warning_events_max"], t.warning_events_max)
    )

    return cfg
