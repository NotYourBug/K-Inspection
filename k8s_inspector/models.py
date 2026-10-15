"""
Domain models for inspection results.

Status / CheckResult / Report form the canonical data structures.
All output writers (JSON, CSV, HTML) and notifications consume these models,
never the raw check function internals.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class Status(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    """
    Result of a single inspection check.

    - name: Check identifier (e.g. "nodes", "pods", "control-plane").
    - status: PASS / WARN / FAIL / SKIP.
    - summary: Human-readable short description.
    - data: Structured details (for JSON export / downstream processing).
    - duration_ms: Wall-clock execution time in milliseconds.
    """
    name: str
    status: Status
    summary: str
    data: dict[str, Any] = field(default_factory=dict)
    duration_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class Report:
    """
    Aggregated inspection report.

    generated_at: ISO8601 timestamp of report generation.
    cluster: Cluster identifier (--cluster / INSPECTOR_CLUSTER, falls back to kubeconfig context).
    results: List of check results.
    """
    generated_at: str
    cluster: str
    results: list[CheckResult]

    @property
    def has_fail(self) -> bool:
        return any(r.status == Status.FAIL for r in self.results)

    @property
    def has_warn(self) -> bool:
        return any(r.status == Status.WARN for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "cluster": self.cluster,
            "summary": {
                "total": len(self.results),
                "pass": sum(1 for r in self.results if r.status == Status.PASS),
                "warn": sum(1 for r in self.results if r.status == Status.WARN),
                "fail": sum(1 for r in self.results if r.status == Status.FAIL),
                "skip": sum(1 for r in self.results if r.status == Status.SKIP),
            },
            "results": [r.to_dict() for r in self.results],
        }
