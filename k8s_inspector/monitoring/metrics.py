"""
Prometheus metrics export via Pushgateway.

Since the inspector runs as a short-lived CronJob, it cannot expose a long-lived
/metrics endpoint for Prometheus to scrape. Instead, each run converts its Report
into Prometheus Gauge metrics and pushes them to a Pushgateway. Prometheus then
scrapes the Pushgateway.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from k8s_inspector.core.models import Report, Status
from k8s_inspector.reporting.sinks import SinkResult, execution_success


@dataclass
class PushGatewayConfig:
    url: str
    job: str = "k8s-inspector"
    timeout_s: float = 5.0


def push_report_to_gateway(
    report: Report,
    profile: str,
    cfg: PushGatewayConfig,
    extra_grouping: dict[str, str] | None = None,
    *,
    sink_results: tuple[SinkResult, ...] = (),
    last_valid: tuple = (),
    confirmed: tuple = (),
) -> None:
    """Convert a Report to Prometheus metrics and push to Pushgateway."""
    prometheus_client = _import_prometheus_client()

    registry = prometheus_client.CollectorRegistry()
    populate_registry(report=report, profile=profile, registry=registry, sink_results=sink_results, last_valid=last_valid, confirmed=confirmed)

    grouping_key = dict(extra_grouping or {})
    grouping_key.setdefault("cluster", report.cluster)
    grouping_key.setdefault("profile", profile)

    prometheus_client.push_to_gateway(
        gateway=cfg.url,
        job=cfg.job,
        registry=registry,
        grouping_key=grouping_key,
        timeout=cfg.timeout_s,
    )


def populate_registry(report: Report, profile: str, registry: Any, *,
                      sink_results: tuple[SinkResult, ...] = (), last_valid: tuple = (), confirmed: tuple = ()) -> None:
    """
    Fill a Prometheus CollectorRegistry from a Report.

    Defines business gauges plus run/sink execution quality:
    - k8s_inspector_last_run_timestamp
    - k8s_inspector_report_summary
    - k8s_inspector_check_status
    - k8s_inspector_check_duration_ms
    - k8s_inspector_check_issue_count
    - k8s_inspector_node_utilization
    """
    prometheus_client = _import_prometheus_client()
    _populate_quality(report, profile, registry, last_valid, sink_results, confirmed)
    run_success = prometheus_client.Gauge(
        "k8s_inspector_run_success",
        "Inspection and required output success before this metrics delivery.",
        ["cluster", "profile"], registry=registry)
    run_success.labels(report.cluster, profile).set(int(execution_success(report, sink_results)))
    sink_success = prometheus_client.Gauge(
        "k8s_inspector_sink_success", "Completed output attempt success.",
        ["cluster", "profile", "sink"], registry=registry)
    for outcome in sink_results:
        sink_success.labels(report.cluster, profile, outcome.name).set(int(outcome.success))

    g_last_run = prometheus_client.Gauge(
        "k8s_inspector_last_run_timestamp",
        "Last run time of k8s-inspector (unix epoch seconds).",
        labelnames=["cluster", "profile"],
        registry=registry,
    )
    g_summary = prometheus_client.Gauge(
        "k8s_inspector_report_summary",
        "Summary counters of k8s-inspector report.",
        labelnames=["cluster", "profile", "status"],
        registry=registry,
    )
    g_check_status = prometheus_client.Gauge(
        "k8s_inspector_check_status",
        "Check status (0=PASS,1=WARN,2=FAIL,3=SKIP).",
        labelnames=["cluster", "profile", "check"],
        registry=registry,
    )
    g_check_duration = prometheus_client.Gauge(
        "k8s_inspector_check_duration_ms",
        "Check execution duration in milliseconds.",
        labelnames=["cluster", "profile", "check"],
        registry=registry,
    )
    g_issue = prometheus_client.Gauge(
        "k8s_inspector_check_issue_count",
        "Issue counters extracted from check data.",
        labelnames=["cluster", "profile", "check", "issue"],
        registry=registry,
    )
    g_node_util = prometheus_client.Gauge(
        "k8s_inspector_node_utilization",
        "Node utilization (ratio).",
        labelnames=["cluster", "profile", "node", "resource"],
        registry=registry,
    )

    ts = _parse_iso_to_epoch(report.generated_at)
    g_last_run.labels(report.cluster, profile).set(ts)

    from k8s_inspector.inspection.compatibility import legacy_results
    legacy = legacy_results(report)
    summary = dict(total=len(legacy), **{status.value.lower():sum(r.status==status for r in legacy) for status in Status})
    for k in ["total", "pass", "warn", "fail", "skip"]:
        v = summary.get(k)
        if isinstance(v, int):
            g_summary.labels(report.cluster, profile, k).set(v)

    for r in legacy:
        g_check_status.labels(report.cluster, profile, r.name).set(_status_to_number(r.status))
        if r.duration_ms is not None:
            g_check_duration.labels(report.cluster, profile, r.name).set(r.duration_ms)

        for issue_name, count in _extract_issue_counts(r.name, r.data).items():
            g_issue.labels(report.cluster, profile, r.name, issue_name).set(count)

        if r.name == "nodes":
            for it in _extract_node_utilization(r.data):
                g_node_util.labels(
                    report.cluster,
                    profile,
                    it["node"],
                    it["resource"],
                ).set(it["value"])


def _extract_issue_counts(check_name: str, data: dict[str, Any]) -> dict[str, int]:
    """Extract countable issues from check data for metric export."""
    out: dict[str, int] = {}
    if not isinstance(data, dict):
        return out

    if '_issue_counts' in data:
        return {k:v for k,v in data['_issue_counts'].items() if type(v) is int and v>=0}
    if check_name == "nodes":
        out["not_ready"] = len(data.get("not_ready") or [])
        out["noschedule_taints"] = len(data.get("noschedule_taints") or [])
        out["over_cpu"] = len(data.get("over_cpu") or [])
        out["over_memory"] = len(data.get("over_memory") or [])
    elif check_name == "pods":
        out["bad"] = len(data.get("bad") or [])
        out["restart_heavy"] = len(data.get("restart_heavy") or [])
    elif check_name == "pvcs":
        out["pending"] = len(data.get("pending") or [])
    elif check_name == "events":
        c = data.get("count")
        if isinstance(c, int):
            out["warning"] = c
    elif check_name == "control-plane":
        health = data.get("apiserver_healthz") or {}
        ok = health.get("ok")
        if ok is False:
            out["healthz_not_ok"] = 1
    elif check_name == "kube-system-addons":
        addons = (data.get("addons") or {}) if isinstance(data.get("addons"), dict) else {}
        missing = 0
        bad = 0
        for v in addons.values():
            if not isinstance(v, dict):
                continue
            found = v.get("found")
            if found == 0:
                missing += 1
            b = v.get("bad") or []
            if isinstance(b, list) and b:
                bad += 1
        out["addons_missing"] = missing
        out["addons_bad"] = bad
    return {k: int(v) for k, v in out.items() if isinstance(v, int)}


def _extract_node_utilization(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract per-node cpu/memory utilization ratios for metric export."""
    util = data.get("utilization") if isinstance(data, dict) else None
    if not isinstance(util, list):
        return []
    out: list[dict[str, Any]] = []
    for it in util:
        if not isinstance(it, dict):
            continue
        node = it.get("node")
        cpu = it.get("cpu_utilization")
        mem = it.get("mem_utilization")
        if isinstance(node, str) and isinstance(cpu, (int, float)):
            out.append({"node": node, "resource": "cpu", "value": float(cpu)})
        if isinstance(node, str) and isinstance(mem, (int, float)):
            out.append({"node": node, "resource": "memory", "value": float(mem)})
    return out


def _parse_iso_to_epoch(s: str) -> float:
    try:
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        return 0.0


def _status_to_number(s: Status) -> int:
    if s == Status.PASS:
        return 0
    if s == Status.WARN:
        return 1
    if s == Status.FAIL:
        return 2
    return 3


def _import_prometheus_client():
    """Lazy-import prometheus_client so a missing dependency doesn't break core inspection."""
    try:
        import prometheus_client  # type: ignore
        from prometheus_client.exposition import push_to_gateway  # type: ignore

        prometheus_client.push_to_gateway = push_to_gateway  # type: ignore[attr-defined]
        return prometheus_client
    except Exception as e:
        raise RuntimeError("prometheus-client is required for metrics export") from e


def _populate_quality(report, profile, registry, last_valid, sink_results, confirmed):
    from k8s_inspector.core.observations import ExecutionStatus as E, ObservationHealth as H
    prometheus = _import_prometheus_client()
    labels = ['cluster', 'profile']
    prometheus.Gauge('inspector_run_success', 'Collection and required sink execution success.', labels,
        registry=registry).labels(report.cluster, profile).set(int(execution_success(report, sink_results)))
    prometheus.Gauge('inspector_last_run_timestamp_seconds', 'Run completion UTC time.', labels,
        registry=registry).labels(report.cluster, profile).set(_parse_iso_to_epoch(report.generated_at))
    sink_gauge = prometheus.Gauge('inspector_sink_success', 'Completed sink result before metrics push.',
                                 labels + ['sink'], registry=registry)
    for result in sink_results:
        sink_gauge.labels(report.cluster, profile, result.name).set(int(result.success))
    rule_labels = labels + ['rule_id', 'scope']
    complete = prometheus.Gauge('inspector_collection_complete', 'Rule collection coverage complete.', rule_labels, registry=registry)
    valid = prometheus.Gauge('inspector_rule_observation_valid', 'Current complete known aggregate observation.', rule_labels, registry=registry)
    timestamp = prometheus.Gauge('inspector_last_valid_observation_timestamp_seconds', 'Last durable valid observation time.', rule_labels, registry=registry)
    failures = prometheus.Gauge('inspector_last_valid_failures', 'Last durable valid aggregate finding count; unknown has no default zero.', rule_labels, registry=registry)
    for execution in report.executions:
        args = (report.cluster, profile, execution.rule_id, 'cluster')
        complete.labels(*args).set(int(execution.coverage.complete))
        is_valid = (execution.execution_status == E.OK and execution.coverage.complete and
            any(o.rule_id == execution.rule_id and o.cluster_id == report.cluster and o.resource_uid is None
                and o.scope == 'cluster' and o.complete and o.health != H.UNKNOWN for o in report.observations))
        valid.labels(*args).set(int(is_valid))
    active_rules = {e.rule_id for e in report.executions}
    for state in last_valid:
        if state.cluster_id == report.cluster and state.profile == profile and state.scope == 'cluster' and state.rule_id in active_rules:
            args = (report.cluster, profile, state.rule_id, state.scope)
            timestamp.labels(*args).set(state.observed_at.timestamp())
            failures.labels(*args).set(state.failure_count)

    positive = prometheus.Gauge('inspector_confirmed_failures',
        'Durable positive finding evidence; zero requires valid complete healthy observation.', rule_labels, registry=registry)
    from k8s_inspector.core.observations import Severity
    for rule_id in active_rules:
        valid_states = [s for s in last_valid if s.cluster_id==report.cluster and s.profile==profile
                        and s.rule_id==rule_id and s.scope=='cluster']
        healthy_after = max((s.observed_at for s in valid_states if s.health==H.HEALTHY),default=None)
        candidates = [c.failure_count for c in confirmed if c.cluster_id==report.cluster and c.profile==profile
            and c.rule_id==rule_id and c.scope=='cluster' and (healthy_after is None or c.observed_at>=healthy_after)]
        current = {f.fingerprint for f in report.findings if f.rule_id==rule_id
            and f.severity in (Severity.FAIL,Severity.WARN) and (healthy_after is None or f.observed_at>=healthy_after)}
        if current:
            candidates.append(len(current))
        candidates += [s.failure_count for s in valid_states]
        if candidates:
            positive.labels(report.cluster,profile,rule_id,'cluster').set(max(candidates))
