"""
Inspection rule functions.

Conventions:
- Every check returns a CheckResult (never raises, to avoid aborting the whole run).
- When environment/permission issues prevent a check, return WARN with error details.
- run_all(profile=...) dispatches by frequency tier to control API pressure.
"""

from __future__ import annotations

import time
import math
from datetime import datetime, timezone
from typing import Any

from kubernetes import client

from .common import BAD_POD_REASONS, parse_quantity, pod_issue
from .config import InspectorConfig
from .kube import apiserver_healthz, get_node_metrics
from .models import CheckResult, Status


def _duration_ms(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


def check_nodes(core: client.CoreV1Api, custom: client.CustomObjectsApi,
                cfg: InspectorConfig, include_metrics: bool = True) -> CheckResult:
    """Preserve Node conditions even when optional usage collection fails."""
    t0 = time.perf_counter()
    data: dict[str, Any] = {"not_ready": [], "noschedule_taints": [],
                            "over_cpu": [], "over_memory": [], "utilization": [],
                            "collection_errors": []}
    errors = data["collection_errors"]
    try:
        nodes = core.list_node().items
    except Exception as exc:
        data["error"] = type(exc).__name__
        errors.append("Node list unavailable")
        return CheckResult("nodes", Status.WARN, "Node collection unknown", data, _duration_ms(t0))
    capacity: dict[str, dict[str, str]] = {}
    for n in nodes:
        name = n.metadata.name if n.metadata else ""
        ready = next((c for c in (n.status.conditions or []) if c.type == "Ready"), None) if n.status else None
        if ready and ready.status in {"False", "Unknown"}:
            data["not_ready"].append(name)
        elif not ready or ready.status != "True":
            errors.append(f"Node readiness unknown: {name}")
        for taint in (n.spec.taints or []) if n.spec else []:
            if taint.effect == "NoSchedule" and taint.key and taint.key not in cfg.allow_noschedule_taints:
                data["noschedule_taints"].append(f"{name}:{taint.key}")
        if n.status:
            available = n.status.allocatable
            capacity[name] = available if isinstance(available, dict) else (n.status.capacity or {})
    metrics = None
    if include_metrics:
        try:
            metrics = get_node_metrics(custom)
        except Exception as exc:
            errors.append(f"Node metrics unavailable: {type(exc).__name__}")
        if not metrics:
            errors.append("Node metrics incomplete or unavailable")
        for name in capacity:
            usage = (metrics or {}).get(name)
            if not usage:
                if metrics:
                    errors.append(f"Node metrics missing: {name}")
                continue
            row: dict[str, Any] = {"node": name}
            for resource, key, threshold, over_key in (
                ("cpu", "cpu_utilization", cfg.thresholds.node_cpu_utilization, "over_cpu"),
                ("memory", "mem_utilization", cfg.thresholds.node_memory_utilization, "over_memory")):
                row[key] = None
                try:
                    numerator = parse_quantity(usage[resource])
                    denominator = parse_quantity(capacity[name][resource])
                    if not math.isfinite(numerator) or not math.isfinite(denominator) or numerator < 0 or denominator <= 0:
                        raise ValueError("invalid usage or capacity")
                    value = numerator / denominator
                    row[key] = round(value, 4)
                    if value >= threshold:
                        data[over_key].append(name)
                except (ValueError, TypeError, KeyError, OverflowError):
                    errors.append(f"Node {resource} utilization unknown: {name}")
            data["utilization"].append(row)
    data["count"] = len(nodes)
    if not nodes:
        errors.append("No Nodes observed")
    status = Status.FAIL if data["not_ready"] else (Status.WARN if errors or any(
        data[k] for k in ("noschedule_taints", "over_cpu", "over_memory")) else Status.PASS)
    return CheckResult("nodes", status,
                       f"Nodes: total={len(nodes)} not_ready={len(data['not_ready'])} collection_errors={len(errors)}",
                       data, _duration_ms(t0))


def check_pending_pvcs(core: client.CoreV1Api, cfg: InspectorConfig | None = None,
                       storage: client.StorageV1Api | None = None, *,
                       now: datetime | None = None) -> CheckResult:
    """Distinguish Lost, stale Pending, and expected delayed binding."""
    t0 = time.perf_counter()
    name = "pvcs"
    cfg = cfg or InspectorConfig()
    now = now or datetime.now(timezone.utc)
    try:
        pvcs = core.list_persistent_volume_claim_for_all_namespaces().items
        pending: list[dict[str, Any]] = []
        lost: list[dict[str, str]] = []
        unknown: list[dict[str, str]] = []
        candidates: list[Any] = []
        errors: list[str] = []
        within_grace = 0
        waiting = 0
        for pvc in pvcs:
            phase = pvc.status.phase if pvc.status else ""
            identity = {"namespace": pvc.metadata.namespace if pvc.metadata else "",
                        "name": pvc.metadata.name if pvc.metadata else ""}
            if phase == "Lost":
                lost.append(identity)
            if phase == "Pending":
                age = _elapsed(pvc.metadata.creation_timestamp if pvc.metadata else None, now)
                if age is None:
                    unknown.append(identity)
                elif age < cfg.pvc_pending_grace_seconds:
                    within_grace += 1
                else:
                    candidates.append(pvc)
            elif phase not in {"Bound", "Lost"}:
                unknown.append(identity)
        classes: dict[str, Any] = {}
        consumers: dict[tuple[str, str], list[str]] = {}
        associations_ok = storage is not None
        if candidates and storage is not None:
            try:
                classes = {sc.metadata.name: sc for sc in storage.list_storage_class().items}
                pods = core.list_pod_for_all_namespaces(watch=False).items
                for p in pods:
                    if not p.metadata or not p.spec or p.metadata.deletion_timestamp:
                        continue
                    if p.status and p.status.phase in {"Succeeded", "Failed"}:
                        continue
                    for volume in p.spec.volumes or []:
                        claim = volume.persistent_volume_claim
                        if claim:
                            consumers.setdefault((p.metadata.namespace, claim.claim_name), []).append(
                                p.metadata.name)
            except Exception as exc:
                errors.append(f"PVC association unavailable: {type(exc).__name__}")
                associations_ok = False
        elif candidates:
            errors.append("PVC association unavailable: storage client not supplied")
        for pvc in candidates:
            identity = {"namespace": pvc.metadata.namespace, "name": pvc.metadata.name}
            sc_name = pvc.spec.storage_class_name if pvc.spec else None
            sc = classes.get(sc_name)
            if not associations_ok or (sc is None and sc_name != ""):
                unknown.append(identity)
                continue
            mode = (sc.volume_binding_mode or "Immediate") if sc else "Static"
            pod_names = consumers.get((pvc.metadata.namespace, pvc.metadata.name), [])
            if mode == "WaitForFirstConsumer" and not pod_names:
                waiting += 1
                continue
            pending.append({**identity, "binding_mode": mode,
                            "storage_class": sc_name, "consumers": sorted(set(pod_names))})
        status = Status.FAIL if lost or pending else (
            Status.WARN if unknown or errors else Status.PASS)
        return CheckResult(
            name=name,
            status=status,
            summary=(f"PVCs: lost={len(lost)} stuck_pending={len(pending)} "
                     f"waiting_for_consumer={waiting} unknown={len(unknown)}"),
            data={"count": len(pvcs), "pending": pending, "lost": lost,
                  "lost_count": len(lost), "waiting_for_consumer": waiting,
                  "within_grace": within_grace, "unknown": unknown,
                  "collection_errors": errors},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="PVC inspection error (degraded)",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def _pod_issue_from_obj(pod: client.V1Pod, restart_threshold: int) -> str | None:
    """Check a V1Pod for anomaly signals. Thin V1Pod wrapper around common.pod_issue."""
    if not pod.status:
        return "missing_status"
    return pod_issue(
        container_statuses=pod.status.container_statuses or [],
        phase=pod.status.phase or "",
        restart_threshold=restart_threshold,
    )


def _elapsed(timestamp: datetime | None, now: datetime) -> float | None:
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        return None
    seconds = (now - timestamp).total_seconds()
    return seconds if seconds >= 0 else None


def evaluate_pod_state(pod: client.V1Pod, cfg: InspectorConfig,
                       now: datetime) -> dict[str, Any]:
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
    if current:
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


def check_pods(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    Pod inspection:
    - FAIL on Failed/Unknown phase or bad waiting reasons.
    - WARN on restart count exceeding threshold.
    """
    t0 = time.perf_counter()
    name = "pods"
    try:
        pods = core.list_pod_for_all_namespaces(watch=False).items
        bad: list[dict[str, str]] = []
        restart_heavy: list[dict[str, str]] = []
        history: list[dict[str, Any]] = []
        unknown: list[dict[str, str]] = []
        observing: list[dict[str, str]] = []
        now = datetime.now(timezone.utc)
        for p in pods:
            ns = p.metadata.namespace if p.metadata else ""
            pn = p.metadata.name if p.metadata else ""
            identity = {"namespace": ns, "name": pn}
            state = evaluate_pod_state(p, cfg, now)
            if state["current_issues"]:
                bad.append({**identity,
                            "issue": "; ".join(x["reason"] for x in state["current_issues"])})
            if state["historical_issues"]:
                history.append({**identity, "issues": state["historical_issues"]})
            if any(x["reason"] == "RestartCountHigh" for x in state["historical_issues"]):
                restart_heavy.append({**identity,
                                      "issue": f"restart_count>={cfg.thresholds.pod_restart_count}"})
            if state["readiness"] == "UNKNOWN":
                unknown.append(identity)
            elif state["readiness"] in {"GRACE", "TERMINATING"}:
                observing.append(identity)
        status = Status.FAIL if bad else (
            Status.WARN if restart_heavy or history or unknown else Status.PASS)
        return CheckResult(
            name=name,
            status=status,
            summary=(f"Pods: current_failures={len(bad)} historical={len(history)} "
                     f"unknown={len(unknown)} observing={len(observing)} total={len(pods)}"),
            data={"count": len(pods), "bad": bad, "restart_heavy": restart_heavy,
                  "restart_threshold": cfg.thresholds.pod_restart_count,
                  "history": history, "unknown": unknown, "observing": observing},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Pod inspection error (degraded)",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_warning_events(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    Warning Event inspection:
    - Samples the first 200 type=Warning events to avoid oversized reports.
    - WARN if the total count exceeds the configured threshold.
    """
    t0 = time.perf_counter()
    name = "events"
    try:
        evs = core.list_event_for_all_namespaces(field_selector="type=Warning").items
        items: list[dict[str, str]] = []
        for e in evs[:200]:
            ns = e.metadata.namespace if e.metadata else ""
            n = e.involved_object.name if e.involved_object else ""
            kind = e.involved_object.kind if e.involved_object else ""
            reason = e.reason or ""
            msg = (e.message or "")[:200]
            items.append({"namespace": ns, "kind": kind, "name": n, "reason": reason, "message": msg})

        if len(evs) > cfg.thresholds.warning_events_max:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Warning Events elevated: {len(evs)} (showing first 200)",
                data={"count": len(evs), "sample": items, "threshold": cfg.thresholds.warning_events_max},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"Warning Events normal: {len(evs)}",
            data={"count": len(evs), "sample": items},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Event inspection error (degraded)",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_control_plane(core: client.CoreV1Api, api_client: client.ApiClient,
                        cfg: InspectorConfig) -> CheckResult:
    """API health and component visibility are independent observations."""
    t0 = time.perf_counter()
    errors: list[str] = []
    try:
        health = apiserver_healthz(api_client, timeout_s=2.0)
    except Exception as exc:
        health = {"ok": None, "error_kind": type(exc).__name__}
        errors.append("API health collection unavailable")
    cp: dict[str, Any] = {"apiserver_healthz": health, "components": {},
                          "collection_errors": errors, "capability": "COMPLETE"}
    unhealthy = health.get("ok") is False
    warning = health.get("ok") is not True
    now = datetime.now(timezone.utc)
    for component in ("kube-apiserver", "kube-controller-manager", "kube-scheduler"):
        try:
            pods = core.list_namespaced_pod(namespace=cfg.control_plane_namespace,
                                             label_selector=f"component={component}").items
            states = [(p, evaluate_pod_state(p, cfg, now)) for p in pods]
            bad = [{"name": p.metadata.name, "issues": state["current_issues"]}
                   for p, state in states if state["current_issues"]]
            historical = sum(bool(s["historical_issues"]) for _, s in states)
            ready = sum(s["readiness"] == "HEALTHY" for _, s in states)
            cp["components"][component] = {"found": len(pods), "ready": ready, "bad": bad}
            unhealthy = unhealthy or bool(bad)
            warning = warning or not pods or ready < len(pods) or historical > 0
            if not pods:
                cp["capability"] = "PARTIAL"
        except Exception as exc:
            errors.append(f"{component} collection unavailable: {type(exc).__name__}")
            cp["components"][component] = {"found": None, "ready": None}
            cp["capability"] = "PARTIAL"
            warning = True
    latency = health.get("latency_ms")
    warning = warning or (isinstance(latency, (int, float)) and latency >= 500)
    status = Status.FAIL if unhealthy else (Status.WARN if warning else Status.PASS)
    return CheckResult("control-plane", status,
                       "Control plane observations" if not warning else "Control plane degraded or capability limited",
                       cp, _duration_ms(t0))


def check_kube_system_addons(core: client.CoreV1Api, cfg: InspectorConfig,
                             apps: client.AppsV1Api | None = None) -> CheckResult:
    """Check observed Ready Pods and controller expectations for known addons."""
    t0 = time.perf_counter()
    errors: list[str] = []
    out: dict[str, Any] = {"namespace": cfg.control_plane_namespace, "addons": {},
                           "collection_errors": errors}
    try:
        apps = apps if apps is not None else client.AppsV1Api(core.api_client)
    except Exception as exc:
        errors.append(f"Addon controller API unavailable: {type(exc).__name__}")
    warning = False
    for addon, selector in (("kube-proxy", "k8s-app=kube-proxy"),
                            ("cni-calico", "k8s-app=calico-node"),
                            ("coredns", "k8s-app=kube-dns")):
        item: dict[str, Any] = {"found": None, "ready": None, "expected_replicas": None, "bad": []}
        out["addons"][addon] = item
        try:
            pods = core.list_namespaced_pod(namespace=cfg.control_plane_namespace,
                                             label_selector=selector).items
            states = [(p, evaluate_pod_state(p, cfg, datetime.now(timezone.utc))) for p in pods]
            item["found"] = len(pods)
            item["ready"] = sum(state["readiness"] == "HEALTHY" for _, state in states)
            item["bad"] = [{"name": p.metadata.name, "readiness": state["readiness"],
                            "issues": state["current_issues"]}
                           for p, state in states if state["readiness"] != "HEALTHY"]
            warning = warning or not pods or bool(item["bad"])
        except Exception as exc:
            errors.append(f"{addon} Pods unavailable: {type(exc).__name__}")
            warning = True
        try:
            if apps is None:
                raise RuntimeError("controller client unavailable")
            deployments = apps.list_namespaced_deployment(namespace=cfg.control_plane_namespace,
                                                           label_selector=selector).items
            daemonsets = apps.list_namespaced_daemon_set(namespace=cfg.control_plane_namespace,
                                                          label_selector=selector).items
            if not deployments and not daemonsets:
                warning = True
                continue
            expected = sum(1 if d.spec.replicas is None else d.spec.replicas for d in deployments)
            expected += sum(d.status.desired_number_scheduled or 0 for d in daemonsets)
            item["expected_replicas"] = expected
            warning = warning or item["ready"] is None or item["ready"] < expected
        except Exception as exc:
            errors.append(f"{addon} controller expectations unavailable: {type(exc).__name__}")
            warning = True
    return CheckResult("kube-system-addons", Status.WARN if warning or errors else Status.PASS,
                       "Addon Ready and controller observations", out, _duration_ms(t0))


def run_all(
    core: client.CoreV1Api,
    storage: client.StorageV1Api,
    custom: client.CustomObjectsApi,
    api_client: client.ApiClient,
    cfg: InspectorConfig,
    profile: str = "full",
) -> list[CheckResult]:
    """
    Dispatch inspection by profile (frequency tier).

    - 5m: Data plane essentials (Node Ready/taints, Pod anomalies, Pending PVCs). No metrics.
    - 10m: Control plane health only.
    - hourly: Infrastructure essentials (control plane + Nodes with metrics + Pods + PVCs).
    - daily/full: Full inspection including Warning Events and kube-system addons.
    """
    p = (profile or "full").strip().lower()
    if p in {"5m", "dataplane-5m", "dataplane"}:
        return [
            check_nodes(core, custom, cfg, include_metrics=False),
            check_pods(core, cfg),
            check_pending_pvcs(core, cfg, storage),
        ]
    if p in {"10m", "controlplane-10m", "controlplane"}:
        return [check_control_plane(core, api_client, cfg)]
    if p in {"hourly", "1h"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core, cfg, storage),
        ]
    if p in {"daily", "1d", "full"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core, cfg, storage),
            check_warning_events(core, cfg),
            check_kube_system_addons(core, cfg),
        ]
    return [
        CheckResult(
            name="profile",
            status=Status.WARN,
            summary=f"Unknown profile: {profile}, falling back to full",
            data={"profile": profile},
            duration_ms=0,
        ),
        check_control_plane(core, api_client, cfg),
        check_nodes(core, custom, cfg, include_metrics=True),
        check_pods(core, cfg),
        check_pending_pvcs(core, cfg, storage),
        check_warning_events(core, cfg),
        check_kube_system_addons(core, cfg),
    ]
