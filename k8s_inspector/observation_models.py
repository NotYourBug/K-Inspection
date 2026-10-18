"""Trusted inspection evidence, independent of SDK objects and output sinks."""
from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any


class ExecutionStatus(str, Enum):
    OK = 'OK'
    PARTIAL = 'PARTIAL'
    ERROR = 'ERROR'
    UNSUPPORTED = 'UNSUPPORTED'
    SKIPPED = 'SKIPPED'


class ObservationHealth(str, Enum):
    HEALTHY = 'HEALTHY'
    UNHEALTHY = 'UNHEALTHY'
    UNKNOWN = 'UNKNOWN'


class Severity(str, Enum):
    INFO = 'INFO'
    WARN = 'WARN'
    FAIL = 'FAIL'


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('observation time requires a timezone')
    return value.astimezone(timezone.utc)


def to_json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if is_dataclass(value):
        return {f.name: to_json_value(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, dict):
        return {k: to_json_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [to_json_value(v) for v in value]
    return value


@dataclass(frozen=True)
class ResourceRef:
    kind: str
    name: str
    namespace: str | None = None
    uid: str | None = None
    container: str | None = None
    owner_uid: str | None = None
    node: str | None = None

    def __post_init__(self):
        for key in ('kind', 'name'):
            if not isinstance(getattr(self, key), str) or not getattr(self, key):
                raise ValueError(f'{key} must be a non-empty string')
        for key in ('namespace', 'uid', 'container', 'owner_uid', 'node'):
            v = getattr(self, key)
            if v is not None and (not isinstance(v, str) or not v):
                raise ValueError(f'{key} must be a non-empty string or None')


@dataclass(frozen=True)
class Coverage:
    expected: int | None
    observed: int
    complete: bool

    def __post_init__(self):
        if (type(self.observed) is not int or self.observed < 0
                or (self.expected is not None and
                    (type(self.expected) is not int or self.expected < self.observed))
                or type(self.complete) is not bool):
            raise ValueError('invalid coverage')


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: Severity
    resource: ResourceRef
    reason: str
    summary: str
    evidence: dict[str, object]
    observed_at: datetime
    fingerprint: str

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))
        try:
            # Do not stringify SDK instances, non-finite numbers or non-string keys.
            def validate(v):
                if isinstance(v, dict):
                    if any(not isinstance(k, str) for k in v):
                        raise ValueError('JSON evidence keys must be strings')
                    for x in v.values():
                        validate(x)
                elif isinstance(v, list):
                    for x in v:
                        validate(x)
                elif v is not None and type(v) not in (str, int, float, bool):
                    raise ValueError('evidence must contain JSON values')
            validate(self.evidence)
            # Copy to detach mutable caller evidence.
            object.__setattr__(self, 'evidence', json.loads(json.dumps(self.evidence, allow_nan=False)))
        except (TypeError, ValueError) as exc:
            raise ValueError('evidence must contain finite JSON values') from exc


@dataclass(frozen=True)
class CheckExecution:
    rule_id: str
    execution_status: ExecutionStatus
    observed_at: datetime
    duration_ms: int
    coverage: Coverage
    error_kind: str | None = None
    reason: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))


@dataclass(frozen=True)
class RuleObservation:
    rule_id: str
    cluster_id: str
    scope: str
    resource_uid: str | None
    health: ObservationHealth
    observed_at: datetime
    complete: bool
    run_id: str

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))
        if not self.scope or not self.cluster_id or not self.run_id:
            raise ValueError('observation identity must not be empty')


@dataclass(frozen=True)
class CheckEvaluation:
    execution: CheckExecution
    findings: tuple[Finding, ...] = ()
    observations: tuple[RuleObservation, ...] = ()


def make_finding(rule_id, cluster_id, resource, severity, reason, summary, evidence, observed_at):
    if not cluster_id:
        raise ValueError('cluster_id must not be empty')
    identity = [rule_id, cluster_id, resource.kind, resource.namespace,
                ['uid', resource.uid] if resource.uid else ['weak-name', resource.name], resource.container]
    fingerprint = hashlib.sha256(json.dumps(identity, ensure_ascii=False,
        separators=(',', ':')).encode('utf-8')).hexdigest()
    evidence = dict(evidence)
    if resource.uid is None:
        evidence['identity_quality'] = 'weak-name'
    return Finding(rule_id, severity, resource, reason, summary, evidence, observed_at, fingerprint)


def to_legacy_result(evaluation):
    from .models import CheckResult, Status
    e = evaluation.execution
    if any(f.severity == Severity.FAIL for f in evaluation.findings):
        status = Status.FAIL
    elif any(f.severity == Severity.WARN for f in evaluation.findings):
        status = Status.WARN
    elif e.execution_status in (ExecutionStatus.UNSUPPORTED, ExecutionStatus.SKIPPED):
        status = Status.SKIP
    elif (e.execution_status == ExecutionStatus.OK and e.coverage.complete
          and any(o.resource_uid is None and o.complete and o.health == ObservationHealth.HEALTHY
                  for o in evaluation.observations)):
        status = Status.PASS
    else:
        status = Status.WARN
    return CheckResult(e.rule_id, status, e.reason or f'{e.rule_id}: {status.value}',
                       to_json_value(evaluation), e.duration_ms)
