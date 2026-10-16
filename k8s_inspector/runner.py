"""Bounded collection followed by isolated pure rule evaluation."""
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from .collector import KubeCollector
from .models import Report
from .observation_models import to_legacy_result, utc
from .profiles import normalize_profile
from .rule_catalog import CATALOG
from .rule_evaluation import evaluation


def run_inspection(context, cfg, profile, *, cluster_id, now, monotonic):
    profile = normalize_profile(profile)  # Before any API access.
    now = utc(now)
    run_id = str(uuid4())
    collector = KubeCollector(context, monotonic() + cfg.deadlines[profile], monotonic)
    collector.connect_timeout, collector.read_timeout = cfg.timeouts['connect'], cfg.timeouts['read']
    rules = tuple(r for r in CATALOG if profile in r.profiles)
    required = frozenset(k for r in rules for k in r.collection_keys)
    snapshot = replace(collector.snapshot(required, run_id, cluster_id), started_at=now)
    evaluations = []
    for rule in rules:
        started = monotonic()
        try:
            result = rule.evaluate(snapshot, cfg)
            if result.execution.rule_id != rule.rule_id:
                raise ValueError('rule identity mismatch')
        except Exception:
            result = evaluation(snapshot, rule.rule_id, (), unknown=True, reason='Rule evaluation failed')
        duration = max(0, int((monotonic() - started) * 1000))
        evaluations.append(replace(result, execution=replace(result.execution, duration_ms=duration)))
    return Report(datetime.now(timezone.utc).isoformat(), cluster_id,
        [to_legacy_result(e) for e in evaluations], schema_version=2, run_id=run_id, profile=profile,
        executions=tuple(e.execution for e in evaluations),
        findings=tuple(f for e in evaluations for f in e.findings),
        observations=tuple(o for e in evaluations for o in e.observations))
