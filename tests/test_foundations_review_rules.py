"""Review regressions: isolation, membership, recovery, Event windows, compatibility."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from kubernetes import client
from prometheus_client import CollectorRegistry
from k8s_inspector.config import InspectorConfig
from k8s_inspector.observation_models import ExecutionStatus as E, ObservationHealth as H, Severity
from k8s_inspector.snapshot import Collection
from k8s_inspector.rule_catalog import CATALOG
from k8s_inspector.metrics import populate_registry
from k8s_inspector.observation_state import ObservationStateStore
from tests.test_rule_observations import NOW, snapshot, event
from tests.factories import node, pod, pvc
from tests.test_collector import page
from tests.test_report_v2 import report
from tests.test_metrics_quality import pass_report, sample


def evaluate(rule_id, snap, cfg=None):
    return next(r for r in CATALOG if r.rule_id == rule_id).evaluate(snap, cfg or InspectorConfig())


def deployment(replicas=1):
    return client.V1Deployment(metadata=client.V1ObjectMeta(name='dns', namespace='kube-system', uid='dns-controller',
        labels={'k8s-app': 'kube-dns'}), spec=client.V1DeploymentSpec(replicas=replicas,
        selector=client.V1LabelSelector(match_labels={'k8s-app': 'kube-dns'}), template=client.V1PodTemplateSpec()))


def healthy_snapshot():
    n = node('n1')
    n.status.conditions.extend(client.V1NodeCondition(type=kind, status='False')
        for kind in ('MemoryPressure', 'DiskPressure', 'PIDPressure'))
    pods = []
    for name in ('kube-apiserver', 'kube-controller-manager', 'kube-scheduler'):
        p = pod(name, namespace='kube-system', uid=name+'-uid')
        p.metadata.labels = {'component': name}
        pods.append(p)
    dns = pod('dns', namespace='kube-system', uid='dns-pod')
    dns.metadata.labels = {'k8s-app': 'kube-dns'}
    pods.append(dns)
    return snapshot(nodes=[n], node_metrics=[{'metadata': {'name':'n1'}, 'usage': {'cpu':'1','memory':'2Gi'}}],
        pods=pods, deployments=[deployment()], daemonsets=[])


def test_r4_malformed_sibling_metric_cannot_hide_node_not_ready(monkeypatch):
    from k8s_inspector import runner
    from tests.test_runner import context
    ctx = context()
    ctx.custom.list_cluster_custom_object.return_value = page([{'metadata': {'name': []}, 'usage': {}}])
    monkeypatch.setattr(runner, 'CATALOG', tuple(r for r in CATALOG if r.rule_id.startswith('nodes.')))
    r = runner.run_inspection(ctx, InspectorConfig(), 'hourly', cluster_id='prod', now=NOW, monotonic=lambda:0)
    assert any(f.reason == 'NodeNotReady' for f in r.findings)
    assert next(e for e in r.executions if e.rule_id=='nodes.ready').execution_status == E.OK


def test_r5_partial_pod_membership_cannot_prove_addon_shortage():
    snap = snapshot(pods=Collection((pod('unrelated', namespace='kube-system'),), E.PARTIAL, NOW, False, 'FORBIDDEN'),
        deployments=[deployment()], daemonsets=[])
    r = evaluate('addons.ready', snap)
    assert not any(f.severity in (Severity.WARN, Severity.FAIL) for f in r.findings)
    assert r.observations[0].health == H.UNKNOWN
    uid = next(o for o in r.observations if o.resource_uid=='dns-controller')
    assert uid.health == H.UNKNOWN and not uid.complete


def test_r6_unrelated_namespace_pod_does_not_prove_addon_capability():
    r = evaluate('addons.ready', snapshot(pods=[pod('unrelated', namespace='kube-system')], deployments=[], daemonsets=[]))
    assert r.execution.execution_status == E.UNSUPPORTED
    assert r.observations[0].health == H.UNKNOWN and not r.observations[0].complete


@pytest.mark.parametrize('rule_id,uid', [('nodes.utilization','node-n1'), ('nodes.pressure','node-n1'),
    ('control.components','kube-apiserver-uid'), ('addons.ready','dns-controller')])
def test_r8_seen_healthy_resource_emits_uid_recovery_evidence(rule_id, uid):
    r = evaluate(rule_id, healthy_snapshot())
    assert any(o.resource_uid==uid and o.health==H.HEALTHY and o.complete for o in r.observations)
    assert not any(o.resource_uid=='absent-old-uid' for o in r.observations)


def test_r7_older_failure_cannot_resurrect_newer_healthy_state_or_metric(tmp_path):
    store = ObservationStateStore(tmp_path/'state.json')
    store.update(report())
    store.update(pass_report())
    store.update(report())
    assert store.read_confirmed() == ()
    r = pass_report()
    reg = CollectorRegistry()
    populate_registry(r, '5m', reg, last_valid=store.read(), confirmed=store.read_confirmed())
    assert sample(reg,'inspector_confirmed_failures') == 0


def warning_findings(result):
    return [f for f in result.findings if f.severity in (Severity.WARN, Severity.FAIL)]


def test_r9_one_series_with_21_new_occurrences_exceeds_threshold10():
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 10
    previous = [{'uid':'event-uid','count':0,'observed_at':(NOW-timedelta(seconds=60)).isoformat(),
                 'sampled_at':(NOW-timedelta(seconds=60)).isoformat()}]
    r = evaluate('events.warning', snapshot(events=[event(series=True)],event_baseline=previous),cfg)
    assert warning_findings(r) and warning_findings(r)[0].evidence['occurrence_delta']==21
    assert r.observations[0].health == H.UNHEALTHY


def test_r9_below_threshold_cursor_is_durable_and_next_delta_is_counted(tmp_path):
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 10
    ev = event()
    ev.count = 1
    ev.first_timestamp = NOW-timedelta(seconds=30)
    r = report(evaluate('events.warning',snapshot(events=[ev]),cfg))
    r.profile = 'daily'
    store = ObservationStateStore(tmp_path/'state.json')
    store.update(r)
    cursor = store.event_baseline(cluster_id='prod',profile='daily')
    assert len(cursor)==1 and cursor[0]['count']==1
    assert not warning_findings(evaluate('events.warning',snapshot(events=[ev]),cfg))
    ev.count = 12
    ev.last_timestamp = NOW+timedelta(seconds=30)
    snap = snapshot(events=Collection((ev,), E.OK, NOW+timedelta(seconds=30),True),event_baseline=cursor)
    result = evaluate('events.warning',snap,cfg)
    assert warning_findings(result)[0].evidence['occurrence_delta']==11


def test_r9_unknown_initial_cumulative_count_cannot_prove_healthy():
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 10
    r = evaluate('events.warning',snapshot(events=[event()]),cfg)
    assert r.observations[0].health == H.UNKNOWN and not r.observations[0].complete


def test_r10_event_created_during_collection_is_not_future_or_lost():
    cfg = InspectorConfig()
    cfg.thresholds.warning_events_max = 0
    ev = event()
    ev.count = 1
    ev.first_timestamp = ev.last_timestamp = NOW+timedelta(seconds=1)
    r = evaluate('events.warning',snapshot(events=Collection((ev,),E.OK,NOW+timedelta(seconds=2),True)),cfg)
    assert warning_findings(r) and r.observations[0].health == H.UNHEALTHY


def test_r3_default_v2_keeps_active_legacy_selectors_and_node_measurements():
    from k8s_inspector.runner import run_inspection
    snap = healthy_snapshot()
    core = NS(list_node=Mock(return_value=page(snap.collections.get('nodes').items)),
        list_pod_for_all_namespaces=Mock(return_value=page(snap.collections.get('pods').items)),
        list_persistent_volume_claim_for_all_namespaces=Mock(return_value=page([pvc('pending',phase='Pending',storage_class='')])))
    ctx = NS(core=core,storage=NS(list_storage_class=Mock(return_value=page())),
        custom=NS(list_cluster_custom_object=Mock(return_value=page(snap.collections.get('node_metrics').items))),
        api_client=NS(call_api=lambda *a,**kw:('ok',200,{})))
    r = run_inspection(ctx,InspectorConfig(),'hourly',cluster_id='prod',now=NOW,monotonic=lambda:0)
    assert {x.name for x in r.results} == {'nodes','pods','pvcs','control-plane'}
    reg = CollectorRegistry()
    populate_registry(r,'hourly',reg)
    labels = {'cluster':'prod','profile':'hourly','check':'pvcs','issue':'pending'}
    assert reg.get_sample_value('k8s_inspector_check_issue_count',labels)==1
    assert reg.get_sample_value('k8s_inspector_node_utilization',
        {'cluster':'prod','profile':'hourly','node':'n1','resource':'cpu'})==0.25
    assert reg.get_sample_value('k8s_inspector_node_utilization',
        {'cluster':'prod','profile':'hourly','node':'n1','resource':'memory'})==0.25


def test_r7_older_current_report_cannot_export_resurrected_fault(tmp_path):
    store=ObservationStateStore(tmp_path/'state.json')
    store.update(pass_report())
    old=report()
    store.update(old)
    reg=CollectorRegistry()
    populate_registry(old,'5m',reg,last_valid=store.read(),confirmed=store.read_confirmed())
    assert sample(reg,'inspector_confirmed_failures')==0


def test_r7_multi_time_partial_fault_waits_for_health_after_latest_evidence(tmp_path):
    from k8s_inspector.observation_models import CheckEvaluation,Coverage
    from tests.observation_factories import failed_evaluation
    ev=failed_evaluation()
    ev=replace(ev,execution=replace(ev.execution,execution_status=E.PARTIAL,coverage=Coverage(None,1,False)),
        observations=tuple(replace(o,health=H.UNKNOWN,complete=False) for o in ev.observations),
        findings=ev.findings+(replace(ev.findings[0],observed_at=NOW+timedelta(minutes=3)),))
    store=ObservationStateStore(tmp_path/'state.json')
    store.update(report(ev))
    store.update(pass_report())
    assert store.read_confirmed()[0].observed_at==NOW+timedelta(minutes=3)


def test_r3_unknown_rule_does_not_export_fake_legacy_issue_zero():
    from tests.observation_factories import error_evaluation
    r=report(error_evaluation())
    reg=CollectorRegistry()
    populate_registry(r,'5m',reg)
    # Status remains visible under the legacy name, but unknown is no zero.
    assert reg.get_sample_value('k8s_inspector_check_status',{'cluster':'prod','profile':'5m','check':'pods'})==1
    assert reg.get_sample_value('k8s_inspector_check_issue_count',
        {'cluster':'prod','profile':'5m','check':'pods','issue':'bad'}) is None


def test_r3_csv_keeps_rule_identity_and_uid_when_root_results_use_legacy_groups(tmp_path):
    import csv
    from k8s_inspector.report import write_csv
    r=report()
    from k8s_inspector.models import CheckResult,Status
    r.results=[CheckResult('pods',Status.FAIL,'Pod failure')]
    rows=list(csv.DictReader(write_csv(r,tmp_path).open(encoding='utf-8')))
    assert rows[0]['rule_id']=='pods.current'
    assert rows[0]['resource_uid']=='pod-uid-1'


def test_r4_invalid_history_evidence_cannot_hide_current_pod_failure():
    from tests.factories import container_status
    cs=container_status(waiting='CrashLoopBackOff')
    # Bad optional evidence at the rule boundary must not invalidate a sibling.
    object.__setattr__(cs,'last_state',NS(terminated=NS(reason=object(),finished_at=None)))
    p=pod('api',container_statuses=[cs])
    r=evaluate('pods.current',snapshot(pods=[p]))
    assert any(f.reason=='CrashLoopBackOff' for f in r.findings)
