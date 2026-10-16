from datetime import datetime, timezone
from types import SimpleNamespace as NS
from k8s_inspector.config import InspectorConfig
from k8s_inspector.snapshot import Collection, RunSnapshot
from k8s_inspector.observation_models import ExecutionStatus as E, ObservationHealth as H, Severity

NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


def snapshot(**collections):
    return RunSnapshot('run-1', 'prod', NOW, {
        key: value if isinstance(value, Collection) else Collection(tuple(value), E.OK, NOW, True)
        for key, value in collections.items()})


def node(ready='False', uid='uid-node-1'):
    return NS(metadata=NS(name='n1', uid=uid, namespace=None),
              status=NS(conditions=[NS(type='Ready', status=ready)]))


def test_partial_collection_can_fail_but_cannot_prove_recovery():
    from k8s_inspector.rules_nodes import evaluate_nodes
    snap = snapshot(nodes=Collection((node(),), E.PARTIAL, NOW, False, 'FORBIDDEN'))
    result = evaluate_nodes(snap, InspectorConfig())[0]
    assert any(f.severity == Severity.FAIL for f in result.findings)
    aggregate = next(x for x in result.observations if x.resource_uid is None)
    assert (aggregate.health, aggregate.complete) == (H.UNKNOWN, False)


def test_new_uid_does_not_resolve_old_resource():
    from k8s_inspector.rules_nodes import evaluate_nodes
    first = evaluate_nodes(snapshot(nodes=[node()]), InspectorConfig())[0]
    second = evaluate_nodes(snapshot(nodes=[node('True', 'uid-node-2')]), InspectorConfig())[0]
    assert first.findings[0].resource.uid == 'uid-node-1'
    assert all(o.resource_uid != 'uid-node-1' for o in second.observations)
