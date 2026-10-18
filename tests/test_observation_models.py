"""Identity, trust and legacy compatibility at the report boundary."""
from dataclasses import replace
from datetime import datetime, timezone
import json

import pytest

from k8s_inspector import models


def api():
    # Keep a missing feature a focused assertion instead of a collection error.
    from k8s_inspector import observation_models as o
    return o


def test_observation_contract_exists():
    import importlib.util
    assert importlib.util.find_spec('k8s_inspector.observation_models') is not None


@pytest.fixture
def o():
    return api()


def finding(o, **changes):
    args = dict(rule_id='pods.current', cluster_id='prod',
                resource=o.ResourceRef('Pod', 'api', namespace='apps', uid='uid-1'),
                severity=o.Severity.FAIL, reason='NotReady', summary='not ready',
                evidence={}, observed_at=datetime(2026, 10, 16, tzinfo=timezone.utc))
    args.update(changes)
    return o.make_finding(**args)


def test_resource_uid_changes_finding_identity(o):
    first = finding(o)
    assert first.fingerprint != finding(o, resource=replace(first.resource, uid='uid-2')).fingerprint
    assert first.fingerprint != finding(o, cluster_id='stage').fingerprint
    assert first.fingerprint != finding(o, resource=replace(first.resource, container='sidecar')).fingerprint
    assert first.fingerprint == finding(o, summary='changed display').fingerprint


def test_partial_with_failure_keeps_fail(o):
    execution = o.CheckExecution('pods.current', o.ExecutionStatus.PARTIAL,
        datetime.now(timezone.utc), 12, o.Coverage(None, 1, False), 'FORBIDDEN', 'page two')
    evaluation = o.CheckEvaluation(execution, (finding(o),), ())
    assert o.to_legacy_result(evaluation).status == models.Status.FAIL
    assert o.to_legacy_result(replace(evaluation, findings=())).status == models.Status.WARN


def test_unknown_coverage_denominator_stays_null(o):
    assert o.to_json_value(o.Coverage(None, 2, False)) == {'expected': None, 'observed': 2, 'complete': False}


def test_naive_observation_time_is_rejected(o):
    with pytest.raises(ValueError, match='timezone'):
        finding(o, observed_at=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match='timezone'):
        o.RuleObservation('pods.current', 'prod', 'cluster', None,
            o.ObservationHealth.HEALTHY, datetime(2026, 1, 1), True, 'run-1')


def test_evidence_rejects_non_json_sdk_objects(o):
    with pytest.raises(ValueError, match='JSON'):
        finding(o, evidence={'sdk': object()})
    with pytest.raises(ValueError, match='JSON'):
        finding(o, evidence={'ratio': float('nan')})


def test_only_complete_healthy_observation_is_pass(o):
    from tests.observation_factories import healthy_observation, error_evaluation
    ev = error_evaluation()
    assert o.to_legacy_result(ev).status == models.Status.WARN
    execution = replace(ev.execution, execution_status=o.ExecutionStatus.OK,
                        coverage=o.Coverage(0, 0, True), error_kind=None)
    assert o.to_legacy_result(replace(ev, execution=execution)).status == models.Status.WARN
    ev = replace(ev, execution=execution, observations=(healthy_observation(),))
    assert o.to_legacy_result(ev).status == models.Status.PASS


def test_legacy_report_root_fields_are_preserved():
    assert set(models.Report('2026-10-16T00:00:00Z', 'prod', []).to_dict()) == {
        'generated_at', 'cluster', 'summary', 'results'}


def test_explicit_v2_serializes_observations_and_utc(o):
    from tests.observation_factories import failed_evaluation
    ev = failed_evaluation()
    report = models.Report('2026-10-16T00:00:00Z', 'prod', [o.to_legacy_result(ev)],
        schema_version=2, run_id='run-1', profile='5m', executions=(ev.execution,),
        findings=ev.findings, observations=ev.observations)
    data = json.loads(json.dumps(report.to_dict(), allow_nan=False))
    assert data['schema_version'] == 2
    assert data['findings'][0]['severity'] == 'FAIL'
    assert data['observations'][0]['observed_at'].endswith('+00:00')
    assert data['summary']['fail'] == 1
