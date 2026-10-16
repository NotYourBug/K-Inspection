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
                    if usage[resource] is None or not str(usage[resource]).strip():
                        raise ValueError("missing usage")
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
        blocked_by_consumers: list[dict[str, str]] = []
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
        consumers: dict[tuple[str, str], list[Any]] = {}
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
                            consumers.setdefault((p.metadata.namespace, claim.claim_name), []).append(p)
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
            consumer_pods = consumers.get((pvc.metadata.namespace, pvc.metadata.name), [])
            pod_names = [p.metadata.name for p in consumer_pods]
            if mode == "WaitForFirstConsumer":
                binding_expired = False
                binding_unknown = False
                in_grace = False
                for p in consumer_pods:
                    age = _elapsed(p.metadata.creation_timestamp, now)
                    scheduled = next((c for c in (p.status.conditions or [])
                                      if c.type == "PodScheduled"), None) if p.status else None
                    if age is not None and age < cfg.pvc_pending_grace_seconds:
                        in_grace = True
                    elif scheduled and scheduled.status == "False":
                        blocked_by_consumers.append({
                            **identity, "pod": p.metadata.name,
                            "reason": scheduled.reason or "NotScheduled"})
                    elif scheduled and scheduled.status == "True":
                        since_scheduled = _elapsed(scheduled.last_transition_time, now)
                        if since_scheduled is None or age is None:
                            binding_unknown = True
                        elif min(since_scheduled, age) < cfg.pvc_pending_grace_seconds:
                            in_grace = True
                        else:
                            binding_expired = True
                    else:
                        binding_unknown = True
                if not binding_expired:
                    if binding_unknown:
                        unknown.append(identity)
                    else:
                        waiting += 1
                        within_grace += int(in_grace)
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
                  "blocked_by_consumers": blocked_by_consumers,
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


from .rules_pods import _elapsed, evaluate_pod_state


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
                          "collection_errors": errors, "capability": "COMPLETE",
                          "unknown": []}
    unhealthy = health.get("ok") is False
    warning = health.get("ok") is not True
    now = datetime.now(timezone.utc)
    for component in ("kube-apiserver", "kube-controller-manager", "kube-scheduler"):
        try:
            pods = core.list_namespaced_pod(namespace=cfg.control_plane_namespace,
                                             label_selector=f"component={component}").items
            states = [(p, evaluate_pod_state(p, cfg, now)) for p in pods]
            bad = [{"name": p.metadata.name, "issues": state["current_issues"]}
                   for p, state in states if state["current_issues"]
                   or state["readiness"] == "COMPLETED"]
            historical = sum(bool(s["historical_issues"]) for _, s in states)
            ready = sum(s["readiness"] == "HEALTHY" for _, s in states)
            cp["components"][component] = {"found": len(pods), "ready": ready, "bad": bad}
            unhealthy = unhealthy or bool(bad)
            warning = warning or not pods or ready < len(pods) or historical > 0
            if not pods or any(state["readiness"] == "UNKNOWN" for _, state in states):
                cp["capability"] = "PARTIAL"
                cp["unknown"].append({"component": component,
                                      "reason": "NotVisible" if not pods else "UnknownReadiness"})
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
                           "collection_errors": errors, "unknown": []}
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
            if not pods or any(s["readiness"] == "UNKNOWN" for _, s in states):
                out["unknown"].append({"addon": addon,
                                       "reason": "NotVisible" if not pods else "UnknownReadiness"})
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
                out["unknown"].append({"addon": addon, "reason": "ControllerExpectationsUnavailable"})
                continue
            expected = sum(1 if d.spec.replicas is None else d.spec.replicas for d in deployments)
            if any(not d.status or d.status.desired_number_scheduled is None for d in daemonsets):
                out["unknown"].append({"addon": addon, "reason": "DesiredReplicasUnknown"})
                warning = True
                continue
            expected += sum(d.status.desired_number_scheduled for d in daemonsets)
            item["expected_replicas"] = expected
            warning = warning or item["ready"] is None or item["ready"] < expected
        except Exception as exc:
            errors.append(f"{addon} controller expectations unavailable: {type(exc).__name__}")
            warning = True
    return CheckResult("kube-system-addons", Status.WARN if warning or errors else Status.PASS,
                       "Addon Ready and controller observations", out, _duration_ms(t0))


def run_all(core, storage, custom, api_client, cfg, profile="full") -> list[CheckResult]:
    """Legacy grouped result facade over the canonical runner (no fallback profile)."""
    from types import SimpleNamespace
    from .runner import run_inspection
    report = run_inspection(SimpleNamespace(core=core, storage=storage, custom=custom,
        api_client=api_client), cfg, profile, cluster_id=cfg.cluster_id or 'legacy',
        now=datetime.now(timezone.utc), monotonic=time.monotonic)
    groups = {'nodes': ('nodes.',), 'pods': ('pods.',), 'pvcs': ('pvc.',),
              'control-plane': ('control.',), 'events': ('events.',), 'kube-system-addons': ('addons.',)}
    results = []
    for name, prefixes in groups.items():
        members = [r for r in report.results if r.name.startswith(prefixes)]
        if not members:
            continue
        status = next((st for st in (Status.FAIL, Status.WARN, Status.PASS, Status.SKIP)
                       if any(r.status == st for r in members)), Status.SKIP)
        results.append(CheckResult(name, status, '; '.join(r.summary for r in members),
            {'rules': [r.to_dict() for r in members]}, sum(r.duration_ms or 0 for r in members)))
    return results
