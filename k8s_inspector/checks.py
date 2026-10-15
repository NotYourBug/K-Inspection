"""
Inspection rule functions.

Conventions:
- Every check returns a CheckResult (never raises, to avoid aborting the whole run).
- When environment/permission issues prevent a check, return WARN with error details.
- run_all(profile=...) dispatches by frequency tier to control API pressure.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from kubernetes import client

from .common import BAD_POD_REASONS, parse_quantity, pod_issue
from .config import InspectorConfig
from .kube import apiserver_healthz, get_node_metrics
from .models import CheckResult, Status


def _duration_ms(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


def check_nodes(
    core: client.CoreV1Api,
    custom: client.CustomObjectsApi,
    cfg: InspectorConfig,
    include_metrics: bool = True,
) -> CheckResult:
    """
    Node inspection:
    - Ready status: FAIL if any node is NotReady.
    - NoSchedule taints: WARN if a taint key is not in the allowlist.
    - Resource utilization: WARN if CPU/memory exceeds thresholds (requires metrics-server).

    include_metrics=False is used for high-frequency profiles (e.g. 5m) to avoid
    hammering the Metrics API.
    """
    t0 = time.perf_counter()
    name = "nodes"
    try:
        nodes = core.list_node().items
        not_ready: list[str] = []
        noschedule: list[str] = []
        capacity: dict[str, dict[str, str]] = {}

        for n in nodes:
            n_name = n.metadata.name if n.metadata else ""

            ready = False
            for c in n.status.conditions or []:
                if c.type == "Ready":
                    ready = (c.status == "True")
                    break
            if not ready:
                not_ready.append(n_name)

            for t in n.spec.taints or []:
                if t.effect == "NoSchedule":
                    key = t.key or ""
                    if key and key not in cfg.allow_noschedule_taints:
                        noschedule.append(f"{n_name}:{key}")

            if n.status and n.status.capacity:
                capacity[n_name] = {
                    "cpu": str(n.status.capacity.get("cpu", "")),
                    "memory": str(n.status.capacity.get("memory", "")),
                }

        metrics = get_node_metrics(custom) if include_metrics else {}
        util: list[dict[str, Any]] = []
        over_cpu: list[str] = []
        over_mem: list[str] = []

        if metrics:
            for n_name, usage in metrics.items():
                c = capacity.get(n_name)
                if not c:
                    continue
                cpu_u = parse_quantity(usage.get("cpu", "0"))
                cpu_c = parse_quantity(c.get("cpu", "0"))
                mem_u = parse_quantity(usage.get("memory", "0"))
                mem_c = parse_quantity(c.get("memory", "0"))
                cpu_p = (cpu_u / cpu_c) if cpu_c else 0.0
                mem_p = (mem_u / mem_c) if mem_c else 0.0
                util.append(
                    {
                        "node": n_name,
                        "cpu_utilization": round(cpu_p, 4),
                        "mem_utilization": round(mem_p, 4),
                    }
                )
                if cpu_p >= cfg.thresholds.node_cpu_utilization:
                    over_cpu.append(n_name)
                if mem_p >= cfg.thresholds.node_memory_utilization:
                    over_mem.append(n_name)

        # NotReady is the highest-severity finding; include noschedule data alongside it
        # so the report does not lose information.
        if not_ready:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"NotReady nodes: {len(not_ready)}",
                data={"not_ready": not_ready, "noschedule_taints": noschedule, "utilization": util},
                duration_ms=_duration_ms(t0),
            )

        if over_cpu or over_mem:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Resource usage over threshold: CPU={len(over_cpu)} Memory={len(over_mem)}",
                data={
                    "over_cpu": over_cpu,
                    "over_memory": over_mem,
                    "noschedule_taints": noschedule,
                    "utilization": util,
                    "thresholds": {
                        "cpu": cfg.thresholds.node_cpu_utilization,
                        "memory": cfg.thresholds.node_memory_utilization,
                    },
                },
                duration_ms=_duration_ms(t0),
            )

        if noschedule:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"NoSchedule taints outside allowlist: {len(noschedule)}",
                data={"noschedule_taints": noschedule, "utilization": util},
                duration_ms=_duration_ms(t0),
            )

        status = Status.PASS
        summary = f"Nodes Ready: {len(nodes)}"
        if include_metrics and metrics is None:
            status = Status.WARN
            summary = "Nodes Ready (Metrics API unavailable, skipped CPU/memory check)"

        return CheckResult(
            name=name,
            status=status,
            summary=summary,
            data={"count": len(nodes), "utilization": util},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Node inspection error (degraded)",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


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


def check_control_plane(core: client.CoreV1Api, api_client: client.ApiClient, cfg: InspectorConfig) -> CheckResult:
    """
    Control plane inspection:
    - Hits /healthz to verify apiserver health and latency.
    - Checks that kube-apiserver/kube-controller-manager/kube-scheduler Pods are Running.

    Some clusters use static Pods or external control planes, so missing Pods
    produces a WARN rather than FAIL.
    """
    t0 = time.perf_counter()
    name = "control-plane"
    ns = cfg.control_plane_namespace
    try:
        health = apiserver_healthz(api_client, timeout_s=2.0)
        selector_keys = [
            ("kube-apiserver", "component=kube-apiserver"),
            ("kube-controller-manager", "component=kube-controller-manager"),
            ("kube-scheduler", "component=kube-scheduler"),
        ]
        cp: dict[str, Any] = {"apiserver_healthz": health, "components": {}}
        any_missing = False
        any_bad = False

        for comp, sel in selector_keys:
            pods = core.list_namespaced_pod(namespace=ns, label_selector=sel).items
            if not pods:
                any_missing = True
                cp["components"][comp] = {"found": 0}
                continue
            bad: list[dict[str, str]] = []
            for p in pods:
                phase = p.status.phase if p.status else ""
                if phase != "Running":
                    bad.append(
                        {
                            "namespace": ns,
                            "name": p.metadata.name if p.metadata else "",
                            "phase": phase,
                        }
                    )
                    continue
                for cs in p.status.container_statuses or []:
                    if (cs.restart_count or 0) >= cfg.thresholds.pod_restart_count:
                        bad.append(
                            {
                                "namespace": ns,
                                "name": p.metadata.name if p.metadata else "",
                                "phase": phase,
                                "issue": f"restart_count>={cfg.thresholds.pod_restart_count}",
                            }
                        )
                        break
            if bad:
                any_bad = True
            cp["components"][comp] = {"found": len(pods), "bad": bad}

        if not health.get("ok", False):
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary="apiserver /healthz failed",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        if any_bad:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary="Control plane component Pods unhealthy",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        if any_missing:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Some control plane Pods not found in {ns} (may be static Pods or external)",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        latency = health.get("latency_ms")
        if isinstance(latency, int) and latency >= 500:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"apiserver /healthz latency high: {latency}ms",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary="Control plane healthy",
            data=cp,
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Control plane inspection error (degraded)",
            data={"namespace": ns, "error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_kube_system_addons(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    kube-system addon inspection (by label selector).

    Default checks: kube-proxy, calico-node, coredns.
    Adjust selectors if your cluster uses different CNI/DNS.
    """
    t0 = time.perf_counter()
    name = "kube-system-addons"
    ns = cfg.control_plane_namespace
    try:
        checks = [
            ("kube-proxy", "k8s-app=kube-proxy"),
            ("cni-calico", "k8s-app=calico-node"),
            ("coredns", "k8s-app=kube-dns"),
        ]
        out: dict[str, Any] = {"namespace": ns, "addons": {}}
        any_bad = False
        any_missing = False

        for addon, selector in checks:
            pods = core.list_namespaced_pod(namespace=ns, label_selector=selector).items
            if not pods:
                any_missing = True
                out["addons"][addon] = {"found": 0}
                continue
            bad: list[dict[str, str]] = []
            for p in pods:
                phase = p.status.phase if p.status else ""
                if phase != "Running":
                    bad.append({"name": p.metadata.name if p.metadata else "", "phase": phase})
            if bad:
                any_bad = True
                out["addons"][addon] = {"found": len(pods), "bad": bad}

        if any_bad:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary="kube-system addon Pods unhealthy",
                data=out,
                duration_ms=_duration_ms(t0),
            )
        if any_missing:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary="Some kube-system addons not found (check CNI/DNS selectors)",
                data=out,
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary="kube-system addons healthy",
            data=out,
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="kube-system addon inspection error (degraded)",
            data={"namespace": ns, "error": str(e)},
            duration_ms=_duration_ms(t0),
        )


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
