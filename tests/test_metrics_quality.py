from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import pytest
from prometheus_client import CollectorRegistry
from k8s_inspector.monitoring.metrics import populate_registry
from k8s_inspector.core.observations import CheckEvaluation, CheckExecution, Coverage, ExecutionStatus as E
from tests.test_report_v2 import report
from tests.observation_factories import NOW, failed_evaluation, error_evaluation, healthy_observation


def sample(registry, name, *, rule_id='pods.current'):
    return registry.get_sample_value(name, {'cluster': 'prod', 'profile': '5m', 'rule_id': rule_id, 'scope': 'cluster'})


def pass_report():
    return report(CheckEvaluation(CheckExecution('pods.current', E.OK, NOW+timedelta(minutes=2), 1,
        Coverage(1, 1, True)), (), (healthy_observation(now=NOW+timedelta(minutes=2)),)))


def test_error_does_not_clear_last_valid_failure(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    store = ObservationStateStore(tmp_path/'state.json')
    store.update(report())
    last_valid = store.update(report(error_evaluation(now=NOW+timedelta(minutes=1))))
    registry = CollectorRegistry()
    populate_registry(report(error_evaluation()), '5m', registry, last_valid=last_valid)
    assert sample(registry, 'inspector_last_valid_failures') == 1
    assert sample(registry, 'inspector_rule_observation_valid') == 0
    assert sample(registry, 'inspector_last_valid_observation_timestamp_seconds') == NOW.timestamp()


def test_valid_healthy_observation_clears_previous_failure(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    store = ObservationStateStore(tmp_path/'state.json')
    store.update(report())
    current = store.update(pass_report())
    assert current[0].health.value == 'HEALTHY' and current[0].failure_count == 0
    # Restart and an older run cannot undo the newer healthy evidence.
    reopened = ObservationStateStore(tmp_path/'state.json')
    assert reopened.update(report())[0].failure_count == 0


def test_unknown_without_history_has_no_healthy_sample(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    r = report(error_evaluation())
    states = ObservationStateStore(tmp_path/'state.json').update(r)
    reg = CollectorRegistry()
    populate_registry(r, '5m', reg, last_valid=states)
    assert sample(reg, 'inspector_last_valid_failures') is None
    assert sample(reg, 'inspector_rule_observation_valid') == 0
    assert reg.get_sample_value('inspector_run_success', {'cluster': 'prod', 'profile': '5m'}) == 0


def test_state_loss_is_visible(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    path = tmp_path/'state.json'
    ObservationStateStore(path).update(report())
    path.unlink()
    with pytest.raises(ValueError, match='state'):
        ObservationStateStore(path).update(report(error_evaluation()))
    path.write_text('{broken', encoding='utf-8')
    with pytest.raises(ValueError, match='state'):
        ObservationStateStore(path).update(pass_report())


def test_profile_lock_is_exclusive_and_recovers_after_release(tmp_path):
    from k8s_inspector.inspection.state import profile_run_lock
    with profile_run_lock(tmp_path):
        with pytest.raises(OSError):
            with profile_run_lock(tmp_path):
                pytest.fail('second run acquired a held profile')
    with profile_run_lock(tmp_path):
        pass


def test_metrics_labels_have_bounded_cardinality(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    r = report()
    states = ObservationStateStore(tmp_path/'state.json').update(r)
    reg = CollectorRegistry()
    populate_registry(r, '5m', reg, last_valid=states)
    for metric in reg.collect():
        if metric.name.startswith('inspector_'):
            for value in metric.samples:
                assert not {'uid', 'pod', 'name', 'fingerprint', 'run_id'} & set(value.labels)


def test_state_write_failure_is_required_sink_and_keeps_failure(tmp_path, monkeypatch):
    from k8s_inspector.inspection.state import ObservationStateStore
    from k8s_inspector.inspection import state as observation_state
    from k8s_inspector.reporting.sinks import run_sinks, execution_success
    store = ObservationStateStore(tmp_path/'state.json')
    previous = store.update(report())
    monkeypatch.setattr(observation_state, 'atomic_write_text', lambda *a: (_ for _ in ()).throw(OSError('full')))
    outcomes = run_sinks({'observation_state': lambda: store.update(pass_report())}, frozenset({'observation_state'}))
    assert not execution_success(pass_report(), outcomes)
    reg = CollectorRegistry()
    populate_registry(pass_report(), '5m', reg, last_valid=previous, sink_results=outcomes)
    assert sample(reg, 'inspector_last_valid_failures') == 1


def test_always0_still_exports_run_failure(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from k8s_inspector import cli
    r = report(error_evaluation())
    monkeypatch.setattr(cli, 'load_kube', lambda **kw: SimpleNamespace(display_name='prod'))
    monkeypatch.setattr(cli, 'run_inspection', lambda *a, **kw: r)
    for key in ('FEISHU_WEBHOOK', 'PUSHGATEWAY_URL', 'INSPECTOR_PROFILE', 'INSPECTOR_CLUSTER'):
        monkeypatch.delenv(key, raising=False)
    values = []
    def push(**kwargs):
        reg = CollectorRegistry()
        populate_registry(kwargs['report'], kwargs['profile'], reg,
                          last_valid=kwargs.get('last_valid', ()), sink_results=kwargs['sink_results'])
        values.append(reg.get_sample_value('inspector_run_success', {'cluster': 'prod', 'profile': '5m'}))
    monkeypatch.setattr(cli, 'push_report_to_gateway', push)
    code = cli.main(['run', '--profile', '5m', '--output-dir', str(tmp_path),
        '--pushgateway-url', 'http://example.invalid', '--exit-mode', 'always0'])
    assert code == 0 and values == [0]
    assert json.loads((tmp_path/'latest/5m/run-status.json').read_text(encoding='utf-8'))['run_success'] is False


def test_state_rejects_cross_cluster_observations(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    r = report()
    r.observations = (replace(r.observations[0], cluster_id='other'),)
    with pytest.raises(ValueError, match='identity'):
        ObservationStateStore(tmp_path/'state.json').update(r)


def test_registry_replacement_drops_removed_rule_labelsets(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    store = ObservationStateStore(tmp_path/'state.json')
    old = store.update(report())
    ev = CheckEvaluation(CheckExecution('nodes.ready', E.OK, NOW, 1, Coverage(1, 1, True)), (),
                          (healthy_observation('nodes.ready'),))
    r = report(ev)
    reg = CollectorRegistry()
    populate_registry(r, '5m', reg, last_valid=old)
    assert sample(reg, 'inspector_last_valid_failures') is None


def test_event_baseline_is_cluster_and_profile_bound(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    from k8s_inspector.inspection.rules.events import evaluate_events
    from k8s_inspector.core.config import InspectorConfig
    from tests.test_rule_observations import snapshot, event
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 0
    r = report(evaluate_events(snapshot(events=[event(series=True)]), cfg)[0])
    r.profile = 'daily'
    store = ObservationStateStore(tmp_path/'state.json')
    store.update(r)
    assert store.event_baseline(cluster_id='prod', profile='daily')[0]['count'] == 21
    assert store.event_baseline(cluster_id='other', profile='daily') == ()
    assert store.event_baseline(cluster_id='prod', profile='5m') == ()


def test_cli_lock_contention_stops_before_cluster_access(tmp_path, monkeypatch):
    from k8s_inspector import cli
    from k8s_inspector.inspection.state import profile_run_lock
    monkeypatch.setattr(cli, 'load_kube', lambda **kw: pytest.fail('held lock reached cluster'))
    with profile_run_lock(tmp_path/'latest/5m'):
        assert cli.main(['run', '--profile', '5m', '--output-dir', str(tmp_path), '--exit-mode', 'always0']) == 1


def test_partial_finding_survives_following_error_without_claiming_valid_coverage(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    from k8s_inspector.core.observations import ObservationHealth as H
    ev = failed_evaluation()
    ev = replace(ev, execution=replace(ev.execution, execution_status=E.PARTIAL, coverage=Coverage(None, 1, False)),
        observations=tuple(replace(o, health=H.UNKNOWN, complete=False) for o in ev.observations))
    store = ObservationStateStore(tmp_path/'state.json')
    assert store.update(report(ev)) == ()
    r = report(error_evaluation(now=NOW+timedelta(minutes=1)))
    last_valid = store.update(r)
    reg = CollectorRegistry()
    populate_registry(r, '5m', reg, last_valid=last_valid, confirmed=store.read_confirmed())
    assert sample(reg, 'inspector_confirmed_failures') == 1
    assert sample(reg, 'inspector_last_valid_failures') is None
    assert sample(reg, 'inspector_rule_observation_valid') == 0
    store.update(pass_report())
    assert store.read_confirmed() == ()


def test_error_cannot_reuse_older_healthy_state_to_clear_partial_finding(tmp_path):
    from k8s_inspector.inspection.state import ObservationStateStore
    from k8s_inspector.core.observations import ObservationHealth as H
    store = ObservationStateStore(tmp_path/'state.json')
    healthy = pass_report()
    store.update(healthy)
    ev = failed_evaluation(now=NOW+timedelta(minutes=2))
    ev = replace(ev, execution=replace(ev.execution, execution_status=E.PARTIAL, coverage=Coverage(None,1,False)),
        observations=tuple(replace(o,health=H.UNKNOWN,complete=False) for o in ev.observations))
    store.update(report(ev))
    store.update(report(error_evaluation(now=NOW+timedelta(minutes=3))))
    assert store.read_confirmed()[0].failure_count == 1
