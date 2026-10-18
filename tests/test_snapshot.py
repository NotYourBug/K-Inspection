from types import SimpleNamespace as NS
from unittest.mock import Mock
from tests.test_collector import page


def context():
    return NS(core=NS(list_node=Mock(return_value=page(['node'])),
                      list_pod_for_all_namespaces=Mock(return_value=page())),
              custom=NS(list_cluster_custom_object=Mock(return_value={'items': [], 'metadata': {}})))


def test_snapshot_reuses_node_metrics():
    from k8s_inspector.inspection.collector import KubeCollector
    ctx = context()
    collector = KubeCollector(ctx, 30, lambda: 0)
    a = collector.snapshot(frozenset({'nodes', 'node_metrics'}), 'run-1', 'prod')
    b = collector.snapshot(frozenset({'nodes', 'node_metrics'}), 'run-1', 'prod')
    assert a.collections['nodes'].items == ('node',)
    assert ctx.core.list_node.call_count == ctx.custom.list_cluster_custom_object.call_count == 1
    assert b.collections['nodes'] is a.collections['nodes']


def test_5m_does_not_collect_metrics():
    from k8s_inspector.inspection.collector import KubeCollector
    ctx = context()
    collector = KubeCollector(ctx, 30, lambda: 0)
    snap = collector.snapshot(frozenset({'nodes', 'pods'}), 'run-1', 'prod')
    assert set(snap.collections) == {'nodes', 'pods'}
    assert not ctx.custom.list_cluster_custom_object.called


def test_api_health_uses_transport_timeout_and_namespace_scoped_addons():
    from k8s_inspector.inspection.collector import KubeCollector
    ctx = NS(api_client=NS(call_api=Mock(return_value=('ok', 200, {}))),
             apps=NS(list_namespaced_deployment=Mock(return_value=page()),
                     list_namespaced_daemon_set=Mock(return_value=page())))
    collector = KubeCollector(ctx, 2, lambda: 0)
    collector.namespace = 'system'
    snap = collector.snapshot(frozenset({'api_health', 'deployments'}), 'run', 'prod')
    assert snap.collections['api_health'].items[0]['ok'] is True
    timeout = ctx.api_client.call_api.call_args.kwargs['_request_timeout']
    assert sum(timeout) <= 2
    assert ctx.apps.list_namespaced_deployment.call_args.kwargs['namespace'] == 'system'
