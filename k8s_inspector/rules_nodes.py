"""Pure Node observations from one shared list snapshot."""
from .observation_models import ObservationHealth as H, Severity, make_finding
from .rule_evaluation import evaluation, resource_ref


def evaluate_nodes(snapshot, cfg):
    findings, resources, unknown = [], [], False
    collection = snapshot.collections.get('nodes')
    nodes = collection.items if collection else ()
    for node in nodes:
        ref = resource_ref(node, 'Node')
        ready = next((c for c in getattr(getattr(node, 'status', None), 'conditions', None) or []
                      if c.type == 'Ready'), None)
        value = getattr(ready, 'status', None)
        health = H.HEALTHY if value == 'True' else H.UNHEALTHY if value in ('False', 'Unknown') else H.UNKNOWN
        unknown |= health == H.UNKNOWN
        resources.append((ref, health))
        if health == H.UNHEALTHY:
            findings.append(make_finding('nodes.ready', snapshot.cluster_id, ref, Severity.FAIL,
                'NodeNotReady', 'Node Ready is not True', {'ready': value}, collection.observed_at))
    return (evaluation(snapshot, 'nodes.ready', ('nodes',), findings,
                       unknown=unknown or not nodes, resources=resources),)
