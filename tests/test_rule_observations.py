from datetime import datetime, timezone
from types import SimpleNamespace as NS
from k8s_inspector.core.config import InspectorConfig
from k8s_inspector.inspection.snapshot import Collection, RunSnapshot
from k8s_inspector.core.observations import ExecutionStatus as E, ObservationHealth as H, Severity

NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


def snapshot(**collections):
    return RunSnapshot('run-1', 'prod', NOW, {
        key: value if isinstance(value, Collection) else Collection(tuple(value), E.OK, NOW, True)
        for key, value in collections.items()})


def node(ready='False', uid='uid-node-1'):
    return NS(metadata=NS(name='n1', uid=uid, namespace=None),
              status=NS(conditions=[NS(type='Ready', status=ready)]))


def test_partial_collection_can_fail_but_cannot_prove_recovery():
    from k8s_inspector.inspection.rules.nodes import evaluate_nodes
    snap = snapshot(nodes=Collection((node(),), E.PARTIAL, NOW, False, 'FORBIDDEN'))
    result = evaluate_nodes(snap, InspectorConfig())[0]
    assert any(f.severity == Severity.FAIL for f in result.findings)
    aggregate = next(x for x in result.observations if x.resource_uid is None)
    assert (aggregate.health, aggregate.complete) == (H.UNKNOWN, False)


def test_new_uid_does_not_resolve_old_resource():
    from k8s_inspector.inspection.rules.nodes import evaluate_nodes
    first = evaluate_nodes(snapshot(nodes=[node()]), InspectorConfig())[0]
    second = evaluate_nodes(snapshot(nodes=[node('True', 'uid-node-2')]), InspectorConfig())[0]
    assert first.findings[0].resource.uid == 'uid-node-1'
    assert all(o.resource_uid != 'uid-node-1' for o in second.observations)


def test_node_pressure_and_missing_usage_are_separate_quality():
    from tests.factories import node as sdk_node
    from k8s_inspector.inspection.rules.nodes import evaluate_nodes
    n = sdk_node('n1')
    from kubernetes.client import V1NodeCondition
    n.status.conditions.append(V1NodeCondition(type='MemoryPressure', status='True'))
    results = {r.execution.rule_id: r for r in evaluate_nodes(snapshot(nodes=[n], node_metrics=[]), InspectorConfig())}
    assert results['nodes.pressure'].findings[0].reason == 'MemoryPressure'
    assert results['nodes.utilization'].observations[0].health == H.UNKNOWN


def test_node_zero_usage_is_valid_and_bad_metric_preserves_other_finding():
    from tests.factories import node as sdk_node
    from k8s_inspector.inspection.rules.nodes import evaluate_nodes
    n = sdk_node('n1')
    metric = {'metadata': {'name': 'n1'}, 'usage': {'cpu': '0', 'memory': '0'}}
    r = {x.execution.rule_id: x for x in evaluate_nodes(snapshot(nodes=[n], node_metrics=[metric]), InspectorConfig())}
    assert r['nodes.utilization'].observations[0].health == H.HEALTHY
    metric['usage'] = {'cpu': '4', 'memory': ''}
    r = {x.execution.rule_id: x for x in evaluate_nodes(snapshot(nodes=[n], node_metrics=[metric]), InspectorConfig())}
    assert r['nodes.utilization'].findings and r['nodes.utilization'].observations[0].health == H.UNKNOWN


def test_historical_oom_is_separate_rule():
    from tests.factories import pod, container_status
    from k8s_inspector.inspection.rules.pods import evaluate_pods
    p = pod('api', container_statuses=[container_status(old_oom=True, restarts=4)])
    results = {r.execution.rule_id: r for r in evaluate_pods(snapshot(pods=[p]), InspectorConfig())}
    assert results['pods.current'].observations[0].health == H.HEALTHY
    assert not results['pods.current'].findings
    assert results['pods.history'].findings
    assert results['pods.history'].findings[0].resource.uid == 'pod-1'


def test_pod_grace_does_not_prove_healthy_recovery():
    from tests.factories import pod, NOW as pod_now
    from dataclasses import replace
    from k8s_inspector.inspection.rules.pods import evaluate_pods
    p = pod('api', ready=False, age_seconds=30)
    result = evaluate_pods(replace(snapshot(pods=[p]), started_at=pod_now), InspectorConfig())[0]
    assert not result.findings and result.observations[0].health == H.UNKNOWN


def test_storage_delayed_binding_and_lost_survive_association_failure():
    from tests.factories import pvc, storage_class
    from k8s_inspector.inspection.rules.storage import evaluate_storage
    snap = snapshot(pvcs=[pvc('data', phase='Pending', storage_class='delayed')], pods=[],
                    storage_classes=[storage_class('delayed', 'WaitForFirstConsumer')])
    result = evaluate_storage(snap, InspectorConfig())[0]
    assert not result.findings and result.observations[0].health == H.HEALTHY
    snap = snapshot(pvcs=[pvc('lost', phase='Lost'), pvc('data', phase='Pending', storage_class='fast')],
        pods=[], storage_classes=Collection((), E.ERROR, NOW, False, 'FORBIDDEN'))
    result = evaluate_storage(snap, InspectorConfig())[0]
    assert result.findings[0].reason == 'PVCLost' and result.observations[0].health == H.UNKNOWN


def test_scheduled_consumer_uses_transition_grace_and_unknown_time():
    from datetime import timedelta
    from dataclasses import replace
    from kubernetes import client
    from tests.factories import pvc, pod, storage_class
    from k8s_inspector.inspection.rules.storage import evaluate_storage
    p = pod('api', conditions=[client.V1PodCondition(type='PodScheduled', status='True',
        last_transition_time=NOW - timedelta(seconds=20))], volumes=[client.V1Volume(name='data',
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name='data'))])
    snap = snapshot(pvcs=[pvc('data', phase='Pending', storage_class='delayed')], pods=[p],
                    storage_classes=[storage_class('delayed', 'WaitForFirstConsumer')])
    result = evaluate_storage(snap, InspectorConfig())[0]
    assert not result.findings
    p.status.conditions[0].last_transition_time = None
    result = evaluate_storage(snap, InspectorConfig())[0]
    assert not result.findings and result.observations[0].health == H.UNKNOWN


def test_managed_component_absence_is_unsupported():
    from k8s_inspector.inspection.rules.control import evaluate_control
    results = {r.execution.rule_id: r for r in evaluate_control(snapshot(pods=[], api_health=[{'ok': True}]), InspectorConfig())}
    assert results['control.components'].execution.execution_status == E.UNSUPPORTED
    assert results['control.api'].observations[0].health == H.HEALTHY


def test_addon_requires_ready_replicas():
    from kubernetes import client
    from tests.factories import pod
    from k8s_inspector.inspection.rules.control import evaluate_control
    p = pod('dns', namespace='kube-system')
    p.metadata.labels = {'k8s-app': 'kube-dns'}
    deployment = client.V1Deployment(metadata=client.V1ObjectMeta(name='dns', namespace='kube-system', uid='deploy-dns',
        labels={'k8s-app': 'kube-dns'}), spec=client.V1DeploymentSpec(replicas=3,
        selector=client.V1LabelSelector(match_labels={'k8s-app': 'kube-dns'}), template=client.V1PodTemplateSpec()))
    results = {r.execution.rule_id: r for r in evaluate_control(snapshot(pods=[p], deployments=[deployment], daemonsets=[]), InspectorConfig())}
    r = results['addons.ready']
    assert r.findings and r.findings[0].evidence['expected_replicas'] == 3
    assert r.observations[0].health == H.UNHEALTHY


def event(age_seconds=0, series=False):
    from datetime import timedelta
    from kubernetes import client
    ev = client.CoreV1Event(metadata=client.V1ObjectMeta(name='warning', namespace='apps', uid='event-uid'),
        involved_object=client.V1ObjectReference(kind='Pod', name='api', namespace='apps', uid='pod-uid'),
        type='Warning', reason='FailedScheduling', message='test', count=20,
        last_timestamp=NOW-timedelta(seconds=age_seconds))
    if series:
        ev.series = client.CoreV1EventSeries(count=21, last_observed_time=NOW)
    return ev


def test_old_event_is_not_current_warning():
    from k8s_inspector.inspection.rules.events import evaluate_events
    assert not evaluate_events(snapshot(events=[event(86400)]), InspectorConfig())[0].findings


def test_event_series_uses_last_observed_time_and_missing_time_is_unknown():
    from k8s_inspector.inspection.rules.events import evaluate_events
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 0
    r = evaluate_events(snapshot(events=[event(86400, series=True)]), cfg)[0]
    assert r.findings[0].evidence['count'] == 21
    ev = event()
    ev.last_timestamp = None
    r = evaluate_events(snapshot(events=[ev]), cfg)[0]
    assert r.observations[0].health == H.UNKNOWN


def test_event_count_increment_is_not_a_replayed_series():
    from k8s_inspector.inspection.rules.events import evaluate_events
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 0
    baseline = [{'uid': 'event-uid', 'count': 21, 'observed_at': NOW.isoformat()}]
    ev = event(series=True)
    result = evaluate_events(snapshot(events=[ev], event_baseline=baseline), cfg)[0]
    assert not [f for f in result.findings if f.severity in (Severity.WARN, Severity.FAIL)]
    ev.series.count = 22
    result = evaluate_events(snapshot(events=[ev], event_baseline=baseline), cfg)[0]
    assert result.findings[0].evidence['occurrence_delta'] == 1


def test_malformed_optional_metric_does_not_hide_node_failure():
    from tests.factories import node as sdk_node
    from k8s_inspector.inspection.rules.nodes import evaluate_nodes
    result = evaluate_nodes(snapshot(nodes=[sdk_node('n1', ready=False)],
        node_metrics=[{'metadata': {'name': 'n1'}, 'usage': None}]), InspectorConfig())
    assert result[0].findings[0].reason == 'NodeNotReady'
    assert result[1].observations[0].health == H.UNKNOWN
