"""Bounded collection followed by isolated pure rule evaluation."""
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from k8s_inspector.inspection.collector import KubeCollector
from k8s_inspector.core.models import Report
from k8s_inspector.core.observations import to_legacy_result, utc, ExecutionStatus as E, ObservationHealth as H
from k8s_inspector.core.profiles import normalize_profile
from k8s_inspector.inspection.catalog import CATALOG
from k8s_inspector.inspection.rules.evaluation import evaluation


def run_inspection(context, cfg, profile, *, cluster_id, now, monotonic):
    profile = normalize_profile(profile)  # Before any API access.
    now = utc(now)
    run_id = str(uuid4())
    collector = KubeCollector(context, monotonic() + cfg.deadlines[profile], monotonic)
    collector.connect_timeout, collector.read_timeout = cfg.timeouts['connect'], cfg.timeouts['read']
    collector.namespace = cfg.control_plane_namespace
    collector.pod_namespace = cfg.control_plane_namespace if profile == '10m' else None
    rules = tuple(r for r in CATALOG if profile in r.profiles)
    required = frozenset(k for r in rules for k in r.collection_keys)
    snapshot = replace(collector.snapshot(required, run_id, cluster_id), started_at=now)
    baseline = getattr(context, 'event_baseline', ())
    if baseline:
        from k8s_inspector.inspection.snapshot import Collection
        from k8s_inspector.core.observations import ExecutionStatus
        snapshot = replace(snapshot, collections=dict(snapshot.collections, event_baseline=Collection(
            tuple(baseline), ExecutionStatus.OK, now, True)))
    evaluations = []
    for rule in rules:
        started = monotonic()
        try:
            result = rule.evaluate(snapshot, cfg)
            if result.execution.rule_id != rule.rule_id:
                raise ValueError('rule identity mismatch')
        except Exception:
            result = evaluation(snapshot, rule.rule_id, (), unknown=True, reason='Rule evaluation failed')
        ended = monotonic()
        if ended >= collector.deadline:
            result = replace(result, execution=replace(result.execution,
                execution_status=E.PARTIAL if result.findings else E.ERROR,
                coverage=replace(result.execution.coverage, expected=None, complete=False),
                error_kind='DEADLINE', reason='Rule completed after run deadline'),
                observations=tuple(replace(o, health=H.UNKNOWN, complete=False)
                    if o.resource_uid is None or o.health==H.HEALTHY else o for o in result.observations))
        duration = max(0, int((ended - started) * 1000))
        evaluations.append(replace(result, execution=replace(result.execution, duration_ms=duration)))
    report = Report(datetime.now(timezone.utc).isoformat(), cluster_id,
        [to_legacy_result(e) for e in evaluations], schema_version=2, run_id=run_id, profile=profile,
        executions=tuple(e.execution for e in evaluations),
        findings=tuple(f for e in evaluations for f in e.findings),
        observations=tuple(o for e in evaluations for o in e.observations))

    from k8s_inspector.inspection.compatibility import legacy_results
    report.results = legacy_results(report)
    return report
