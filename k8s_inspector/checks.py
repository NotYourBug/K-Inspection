"""
Inspection rule functions.

Conventions:
- Every check returns a CheckResult (never raises, to avoid aborting the whole run).
- When environment/permission issues prevent a check, return WARN with error details.
- run_all(profile=...) dispatches by frequency tier to control API pressure.
"""

from __future__ import annotations

import time
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


def check_pending_pvcs(core: client.CoreV1Api) -> CheckResult:
    """PVC inspection: FAIL if any PVC is stuck in Pending phase."""
    t0 = time.perf_counter()
    name = "pvcs"
    try:
        pvcs = core.list_persistent_volume_claim_for_all_namespaces().items
        pending: list[dict[str, str]] = []
        for pvc in pvcs:
            phase = pvc.status.phase if pvc.status else ""
            if phase == "Pending":
                ns = pvc.metadata.namespace if pvc.metadata else ""
                n = pvc.metadata.name if pvc.metadata else ""
                pending.append({"namespace": ns, "name": n})
        if pending:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"Pending PVCs: {len(pending)}",
                data={"pending": pending},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"PVCs bound: {len(pvcs)}",
            data={"count": len(pvcs)},
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
        for p in pods:
            ns = p.metadata.namespace if p.metadata else ""
            pn = p.metadata.name if p.metadata else ""
            issue = _pod_issue_from_obj(p, restart_threshold=0)
            if issue:
                bad.append({"namespace": ns, "name": pn, "issue": issue})
                continue
            issue2 = _pod_issue_from_obj(p, restart_threshold=cfg.thresholds.pod_restart_count)
            if issue2 == f"restart_count>={cfg.thresholds.pod_restart_count}":
                restart_heavy.append({"namespace": ns, "name": pn, "issue": issue2})

        if bad:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"Abnormal Pods: {len(bad)}",
                data={"bad": bad},
                duration_ms=_duration_ms(t0),
            )
        if restart_heavy:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Pod restart count high: {len(restart_heavy)}",
                data={"restart_heavy": restart_heavy, "restart_threshold": cfg.thresholds.pod_restart_count},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"Pods healthy: {len(pods)}",
            data={"count": len(pods)},
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
            check_pending_pvcs(core),
        ]
    if p in {"10m", "controlplane-10m", "controlplane"}:
        return [check_control_plane(core, api_client, cfg)]
    if p in {"hourly", "1h"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core),
        ]
    if p in {"daily", "1d", "full"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core),
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
        check_pending_pvcs(core),
        check_warning_events(core, cfg),
        check_kube_system_addons(core, cfg),
    ]
