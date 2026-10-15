"""
Configuration loading.

Provides sensible defaults so the inspector runs without any config file.
Pass --config with a JSON or YAML file to override thresholds or namespaces.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

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
    pod_unready_grace_seconds: float = 120
    pod_pending_grace_seconds: float = 300
    pvc_pending_grace_seconds: float = 300
    output_policy: str = "best-effort"


def _object(value: Any, allowed: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(f"{label}: unknown fields {sorted(unknown)}")
    return value


def _number(value: Any, label: str, *, ratio: bool = False, integer: bool = False) -> None:
    if (type(value) not in (int, float) or (integer and type(value) is not int)
            or not math.isfinite(value) or value < 0 or (ratio and value > 1)):
        raise ValueError(f"{label} must be a finite non-negative "
                         + ("integer" if integer else "number")
                         + (" between 0 and 1" if ratio else ""))


def load_config(path: str | None) -> InspectorConfig:
    """Load config from JSON/YAML file and merge with defaults."""
    cfg = InspectorConfig()
    if not path:
        return cfg

    p = Path(path)
    raw = p.read_text(encoding="utf-8-sig")
    data: dict[str, Any]

    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError("PyYAML is required to read YAML config") from e
        try:
            data = yaml.safe_load(raw) if raw.strip() else {}
        except yaml.YAMLError as exc:
            raise ValueError("Invalid YAML configuration") from exc
    else:
        data = json.loads(raw) if raw.strip() else {}

    data = _object(data, {f.name for f in fields(InspectorConfig)}, "configuration")
    thresholds = _object(data.get("thresholds", {}),
                         {f.name for f in fields(Thresholds)}, "thresholds")
    for key, value in thresholds.items():
        _number(value, f"thresholds.{key}", ratio=key.endswith("_utilization"),
                integer=key in {"pod_restart_count", "warning_events_max"})
        setattr(cfg.thresholds, key, value)
    for key in ("pod_unready_grace_seconds", "pod_pending_grace_seconds",
                "pvc_pending_grace_seconds"):
        if key in data:
            _number(data[key], key)
            setattr(cfg, key, data[key])
    namespace = data.get("control_plane_namespace", cfg.control_plane_namespace)
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("control_plane_namespace must be a non-empty string")
    cfg.control_plane_namespace = namespace.strip()
    taints = data.get("allow_noschedule_taints", [])
    if (not isinstance(taints, list)
            or any(not isinstance(x, str) or not x.strip() for x in taints)):
        raise ValueError("allow_noschedule_taints must be an array of non-empty strings")
    cfg.allow_noschedule_taints = [x.strip() for x in taints]
    policy = data.get("output_policy", cfg.output_policy)
    if policy not in ("best-effort", "strict"):
        raise ValueError("output_policy must be best-effort or strict")
    cfg.output_policy = policy

    return cfg
