"""Shared C/D/F fixtures. Production modules must not import tests."""
from datetime import datetime, timezone
from k8s_inspector.observation_models import (
    CheckEvaluation, CheckExecution, Coverage, ExecutionStatus, ObservationHealth,
    ResourceRef, RuleObservation, Severity, make_finding,
)

NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


def healthy_observation(rule_id='pods.current', *, now=NOW, run_id='run-healthy', cluster='prod'):
    return RuleObservation(rule_id, cluster, 'cluster', None, ObservationHealth.HEALTHY, now, True, run_id)


def failed_evaluation(rule_id='pods.current', *, now=NOW, run_id='run-failed'):
    f = make_finding(rule_id, 'prod', ResourceRef('Pod', 'api', 'apps', 'pod-uid-1'),
                     Severity.FAIL, 'NotReady', 'not ready', {}, now)
    return CheckEvaluation(CheckExecution(rule_id, ExecutionStatus.OK, now, 1, Coverage(1, 1, True)),
        (f,), (RuleObservation(rule_id, 'prod', 'cluster', None, ObservationHealth.UNHEALTHY,
                              now, True, run_id),))


def error_evaluation(rule_id='pods.current', *, now=NOW, run_id='run-error'):
    return CheckEvaluation(CheckExecution(rule_id, ExecutionStatus.ERROR, now, 1,
                           Coverage(None, 0, False), 'FORBIDDEN', 'collection unavailable'), (),
        (RuleObservation(rule_id, 'prod', 'cluster', None, ObservationHealth.UNKNOWN, now, False, run_id),))
