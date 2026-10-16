from types import SimpleNamespace as NS
from unittest.mock import Mock
from tests.test_collector import page


def context():
    return NS(core=NS(list_node=Mock(return_value=page(['node'])),
                      list_pod_for_all_namespaces=Mock(return_value=page())),
              custom=NS(list_cluster_custom_object=Mock(return_value={'items': [], 'metadata': {}})))


def test_snapshot_reuses_node_metrics():
    from k8s_inspector.collector import KubeCollector
    ctx = context()
    collector = KubeCollector(ctx, 30, lambda: 0)
    a = collector.snapshot(frozenset({'nodes', 'node_metrics'}), 'run-1', 'prod')
    b = collector.snapshot(frozenset({'nodes', 'node_metrics'}), 'run-1', 'prod')
    assert a.collections['nodes'].items == ('node',)
    assert ctx.core.list_node.call_count == ctx.custom.list_cluster_custom_object.call_count == 1
    assert b.collections['nodes'] is a.collections['nodes']


def test_5m_does_not_collect_metrics():
    from k8s_inspector.collector import KubeCollector
    ctx = context()
    collector = KubeCollector(ctx, 30, lambda: 0)
    snap = collector.snapshot(frozenset({'nodes', 'pods'}), 'run-1', 'prod')
    assert set(snap.collections) == {'nodes', 'pods'}
    assert not ctx.custom.list_cluster_custom_object.called
