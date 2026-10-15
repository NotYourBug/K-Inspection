"""Independent output attempts with safe, inspectable outcomes."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from .models import Report
from .report import atomic_write_text


@dataclass(frozen=True)
class SinkResult:
    name: str
    success: bool
    required: bool
    error_kind: str | None = None
    detail: str | None = None


def run_sinks(actions: dict[str, Callable[[], object]],
              required: frozenset[str]) -> tuple[SinkResult, ...]:
    outcomes = []
    for name, action in actions.items():
        try:
            action()
            outcomes.append(SinkResult(name, True, name in required))
        except Exception as exc:
            # Provider exceptions can contain webhook tokens, credentials or payloads.
            outcomes.append(SinkResult(name, False, name in required,
                                       type(exc).__name__, "Output operation failed"))
    return tuple(outcomes)


def execution_success(report: Report, results: tuple[SinkResult, ...] = ()) -> bool:
    """Business FAIL can be a successful inspection; unknown collection cannot."""
    return (all(r.success for r in results if r.required)
            and not any(r.data.get("error") or r.data.get("collection_errors")
                        or r.data.get("unknown") for r in report.results))


def write_run_status(report: Report, profile: str, results: tuple[SinkResult, ...],
                     path: Path) -> Path:
    # This status file does not claim to describe its own write outcome.
    data = {"generated_at": report.generated_at, "cluster": report.cluster,
            "profile": profile, "run_success": execution_success(report, results),
            "sinks": [asdict(r) for r in results]}
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))
    return path
