"""Pure current Pod state and historical stability evidence."""
from __future__ import annotations
from datetime import datetime, timezone
from dataclasses import replace
from typing import Any
from .common import BAD_POD_REASONS
from .config import InspectorConfig
from .observation_models import ObservationHealth as H, Severity, make_finding
from .rule_evaluation import evaluation, resource_ref

def _elapsed(timestamp: datetime | None, now: datetime) -> float | None:
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        return None
    seconds = (now - timestamp).total_seconds()
    return seconds if seconds >= 0 else None


def evaluate_pod_state(pod: client.V1Pod, cfg: InspectorConfig,
                       now: datetime, *, include_history: bool = True) -> dict[str, Any]:
    """Separate present failures, historical instability and unknown readiness."""
    if now.tzinfo is None:
        raise ValueError("Observation time must be timezone-aware")
    current: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "current_issues": current, "historical_issues": history,
        "readiness": "UNKNOWN", "observed_at": now.astimezone(timezone.utc).isoformat(),
    }
    status, metadata = pod.status, pod.metadata
    if not status:
        return result
    if metadata and metadata.deletion_timestamp:
        result["readiness"] = "TERMINATING"
        return result
    phase = status.phase or ""
    if phase == "Succeeded":
        result["readiness"] = "COMPLETED"
        return result
    if phase in {"Failed", "Unknown"}:
        current.append({"reason": f"Pod{phase}", "phase": phase})

    for is_init, statuses in ((False, status.container_statuses or []),
                              (True, status.init_container_statuses or [])):
        for cs in statuses:
            state = cs.state
            if state and state.waiting and state.waiting.reason in BAD_POD_REASONS:
                current.append({"reason": state.waiting.reason, "container": cs.name,
                                "init": is_init})
            if state and state.terminated and state.terminated.exit_code != 0:
                current.append({"reason": "InitContainerFailed" if is_init else "ContainerFailed",
                                "container": cs.name, "exit_code": state.terminated.exit_code,
                                "termination_reason": state.terminated.reason})
            if include_history:
                last = cs.last_state.terminated if cs.last_state else None
                if last and last.reason:
                    history.append({"reason": last.reason, "container": cs.name,
                                    "init": is_init,
                                    "finished_at": last.finished_at.isoformat()
                                    if last.finished_at else None})
                if cfg.thresholds.pod_restart_count > 0 and (
                        cs.restart_count or 0) >= cfg.thresholds.pod_restart_count:
                    history.append({"reason": "RestartCountHigh", "container": cs.name,
                                    "init": is_init, "restart_count": cs.restart_count})

    if current:
        result["readiness"] = "UNHEALTHY"
        return result
    if phase == "Pending":
        age = _elapsed(metadata.creation_timestamp if metadata else None, now)
        if age is None:
            return result
        if age < cfg.pod_pending_grace_seconds:
            result["readiness"] = "GRACE"
            return result
        scheduled = next((c for c in status.conditions or []
                          if c.type == "PodScheduled" and c.status == "False"), None)
        current.append({"reason": (scheduled.reason or "PodPending") if scheduled else "PodPending",
                        "message": scheduled.message if scheduled else None})
        result["readiness"] = "UNHEALTHY"
        return result
    ready = next((c for c in status.conditions or [] if c.type == "Ready"), None)
    if phase == "Running" and ready and ready.status == "True":
        result["readiness"] = "HEALTHY"
    elif phase == "Running" and ready and ready.status == "False":
        elapsed = _elapsed(ready.last_transition_time, now)
        if elapsed is not None:
            if elapsed >= cfg.pod_unready_grace_seconds:
                current.append({"reason": "PodNotReady", "message": ready.message,
                                "unready_seconds": elapsed})
                result["readiness"] = "UNHEALTHY"
            else:
                result["readiness"] = "GRACE"
    return result



def _evaluate_pods(snapshot, cfg, *, history):
    collection = snapshot.collections.get('pods')
    findings, resources, unknown = [], [], False
    rule_id = 'pods.history' if history else 'pods.current'
    for pod in collection.items if collection else ():
        ref = resource_ref(pod, 'Pod')
        state = evaluate_pod_state(pod, cfg, snapshot.started_at, include_history=history)
        issues = state['historical_issues' if history else 'current_issues']
        health = (H.UNHEALTHY if issues else H.HEALTHY if history
                  or state['readiness'] in ('HEALTHY','COMPLETED') else H.UNKNOWN)
        unknown |= health==H.UNKNOWN
        resources.append((ref,health))
        for issue in issues:
            findings.append(make_finding(rule_id,snapshot.cluster_id,replace(ref,container=issue.get('container')),
                Severity.WARN if history else Severity.FAIL,issue['reason'],
                'Historical container instability' if history else 'Current Pod failure',issue,collection.observed_at))
    return evaluation(snapshot,rule_id,('pods',),findings,unknown=unknown,resources=resources)


def evaluate_current(snapshot,cfg):
    return _evaluate_pods(snapshot,cfg,history=False)


def evaluate_history(snapshot,cfg):
    return _evaluate_pods(snapshot,cfg,history=True)


EVALUATORS = {'pods.current':evaluate_current,'pods.history':evaluate_history}


def evaluate_pods(snapshot,cfg):
    return evaluate_current(snapshot,cfg),evaluate_history(snapshot,cfg)
