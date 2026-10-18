"""Independent output attempts with safe, inspectable outcomes."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from k8s_inspector.core.models import Report
from k8s_inspector.reporting.writers import atomic_write_text


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
    if report.schema_version == 2:
        from k8s_inspector.core.observations import ExecutionStatus
        return (all(r.success for r in results if r.required)
                and all(e.execution_status in (ExecutionStatus.OK, ExecutionStatus.UNSUPPORTED, ExecutionStatus.SKIPPED)
                        for e in report.executions))
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


def write_run_manifest(report: Report, sink_results: tuple[SinkResult, ...], out_dir: Path) -> Path:
    """Describe completed attempts without recursively rewriting report.json."""
    data = {'schema_version': report.schema_version or 1, 'run_id': report.run_id,
            'cluster': report.cluster, 'profile': report.profile, 'generated_at': report.generated_at,
            'run_success': execution_success(report, sink_results),
            'sinks': [asdict(r) for r in sink_results]}
    path = Path(out_dir) / 'manifest.json'
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))
    return path
