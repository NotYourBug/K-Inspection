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
from typing import Any, Mapping
from k8s_inspector.core.profiles import PROFILES, normalize_profile

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
    cluster_id: str | None = None
    profile: str = 'full'
    timeouts: dict[str, float] = field(default_factory=lambda: {'connect': 3.0, 'read': 10.0})
    deadlines: dict[str, float] = field(default_factory=lambda: {
        key: value.deadline_seconds for key, value in PROFILES.items()})


def _object(value: Any, allowed: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(f"{label}: unknown fields {sorted(map(str, unknown))}")
    return value


def _number(value: Any, label: str, *, ratio: bool = False, integer: bool = False) -> None:
    if (type(value) not in (int, float) or (integer and type(value) is not int)
            or not math.isfinite(value) or value < 0 or (ratio and value > 1)):
        raise ValueError(f"{label} must be a finite non-negative "
                         + ("integer" if integer else "number")
                         + (" between 0 and 1" if ratio else ""))


def read_config(path: str | None) -> dict[str, Any]:
    """Read syntax only; validation and priority are applied by resolve_config."""
    if not path:
        return {}

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

    return data


def resolve_config(file_config: dict[str, object], env: Mapping[str, str],
                   cli_overrides: Mapping[str, object]) -> InspectorConfig:
    cfg = InspectorConfig()
    allowed = {f.name for f in fields(InspectorConfig)}
    data = dict(_object(file_config, allowed, 'configuration'))
    environment = {'INSPECTOR_CLUSTER': 'cluster_id', 'INSPECTOR_PROFILE': 'profile',
                   'INSPECTOR_OUTPUT_POLICY': 'output_policy'}
    for name, key in environment.items():
        if name in env:
            data[key] = env[name]
    explicit = {key: value for key, value in cli_overrides.items() if value is not None}
    data.update(_object(explicit, allowed, 'CLI configuration'))
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
    cluster = data.get('cluster_id')
    if cluster is not None and (not isinstance(cluster, str) or not cluster.strip()
                                or any(ord(c) < 32 for c in cluster)):
        raise ValueError('cluster_id must be a non-empty printable string')
    cfg.cluster_id = cluster.strip() if cluster is not None else None
    cfg.profile = normalize_profile(data.get('profile', cfg.profile))
    for key in ('timeouts', 'deadlines'):
        current = getattr(cfg, key)
        overrides = _object(data.get(key, {}), set(current), key)
        for name, value in overrides.items():
            _number(value, f'{key}.{name}')
            if value == 0:
                raise ValueError(f'{key}.{name} must be positive')
            current[name] = value
    return cfg


def load_config(path: str | None) -> InspectorConfig:
    """Legacy file-only entry point; CLI resolves env and explicit overrides."""
    return resolve_config(read_config(path), {}, {})
