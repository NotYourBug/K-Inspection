"""
Shared utilities used across the k8s_inspector package.

Includes K8s quantity parsing, Pod issue detection, config helpers,
and resource formatting functions — so mcp_server.py and the main
inspection modules share a single implementation.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# K8s resource quantity parsing
# ---------------------------------------------------------------------------

# Decimal unit multipliers for CPU quantities (base unit: cores).
_DECIMAL: dict[str, float] = {
    "n": 1e-9,
    "u": 1e-6,
    "m": 1e-3,
    "": 1.0,
    "k": 1e3,
    "M": 1e6,
    "G": 1e9,
    "T": 1e12,
    "P": 1e15,
    "E": 1e18,
}

# Binary unit multipliers for memory/storage quantities (base unit: bytes).
_BINARY: dict[str, float] = {
    "Ki": 1024.0,
    "Mi": 1024.0 ** 2,
    "Gi": 1024.0 ** 3,
    "Ti": 1024.0 ** 4,
    "Pi": 1024.0 ** 5,
    "Ei": 1024.0 ** 6,
}


def parse_quantity(q: str) -> float:
    """
    Parse a K8s resource quantity string into a numeric value.

    Examples:
      cpu: "250m" -> 0.25  (cores)
      memory: "128Mi" -> 134217728  (bytes)
    """
    s = (q or "").strip()
    if not s:
        return 0.0
    for suf, mult in _BINARY.items():
        if s.endswith(suf):
            return float(s[:-len(suf)]) * mult
    for suf, mult in _DECIMAL.items():
        if suf and s.endswith(suf):
            return float(s[:-len(suf)]) * mult
    return float(s)


def cpu_to_millicores(quantity: str) -> int:
    """Convert a K8s CPU quantity (e.g. "250m", "1") to millicores."""
    return int(round(parse_quantity(quantity) * 1000))


def memory_to_bytes(quantity: str) -> int:
    """Convert a K8s memory quantity (e.g. "128Mi", "1Gi") to bytes."""
    return int(round(parse_quantity(quantity)))


def format_cpu_m(cpu_m: int) -> str:
    """Format millicores as a human-readable string."""
    return f"{cpu_m}m"


def format_bytes(num_bytes: int) -> str:
    """Format bytes as a human-readable string (Ki/Mi/Gi etc.)."""
    if num_bytes < 1024:
        return f"{num_bytes}B"
    units = ["Ki", "Mi", "Gi", "Ti", "Pi"]
    value = float(num_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f}{unit}"
    return f"{value:.1f}Ei"


def ratio(numerator: int, denominator: int) -> float:
    """Compute a ratio, returning 0.0 when denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


# ---------------------------------------------------------------------------
# Pod issue detection
# ---------------------------------------------------------------------------

BAD_POD_REASONS: set[str] = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerConfigError",
    "CreateContainerError",
    "RunContainerError",
    "InvalidImageName",
    "OOMKilled",
}


def pod_issue(container_statuses: list[Any], phase: str, restart_threshold: int) -> str | None:
    """
    Check a Pod for obvious anomaly signals. Returns an issue string or None.

    container_statuses: pod.status.container_statuses (list of V1ContainerStatus)
    phase: pod.status.phase string
    restart_threshold: restart_count threshold to flag as an issue
    """
    if phase in {"Failed", "Unknown"}:
        return f"phase={phase}"

    for cs in container_statuses:
        if cs.state and cs.state.waiting and cs.state.waiting.reason in BAD_POD_REASONS:
            return f"waiting={cs.state.waiting.reason}"
        if cs.state and cs.state.terminated and cs.state.terminated.reason in BAD_POD_REASONS:
            return f"terminated={cs.state.terminated.reason}"

    if restart_threshold > 0:
        for cs in container_statuses:
            if (cs.restart_count or 0) >= restart_threshold:
                return f"restart_count>={restart_threshold}"
    return None


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def deep_get(d: dict[str, Any], path: list[str], default: Any) -> Any:
    """Safely read a nested dict field; return default if any level is missing."""
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur
