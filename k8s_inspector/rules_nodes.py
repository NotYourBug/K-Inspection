"""Pure Node observations from one shared list snapshot."""
import math
from .common import parse_quantity
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
    ready_result = evaluation(snapshot, 'nodes.ready', ('nodes',), findings,
                              unknown=unknown or not nodes, resources=resources)
    pressure, pressure_unknown = [], not nodes
    utilization, usage_unknown = [], not nodes
    metrics_collection = snapshot.collections.get('node_metrics')
    metrics = {m.get('metadata', {}).get('name'): m.get('usage', {})
               for m in metrics_collection.items if isinstance(m, dict) and isinstance(m.get('metadata'), dict)} if metrics_collection else {}
    for node in nodes:
        ref = resource_ref(node, 'Node')
        conditions = {c.type: c.status for c in getattr(node.status, 'conditions', None) or []}
        for kind in ('MemoryPressure', 'DiskPressure', 'PIDPressure'):
            if conditions.get(kind) == 'True':
                pressure.append(make_finding('nodes.pressure', snapshot.cluster_id, ref, Severity.WARN,
                    kind, f'Node {kind}', {'condition': kind}, collection.observed_at))
            elif conditions.get(kind) != 'False':
                pressure_unknown = True
        for taint in getattr(getattr(node, 'spec', None), 'taints', None) or []:
            if taint.effect == 'NoSchedule' and taint.key not in cfg.allow_noschedule_taints:
                pressure.append(make_finding('nodes.pressure', snapshot.cluster_id, ref, Severity.WARN,
                    'NoScheduleTaint', 'Node scheduling restricted', {'taint': taint.key}, collection.observed_at))
        capacity = getattr(node.status, 'allocatable', None)
        if not isinstance(capacity, dict):
            capacity = getattr(node.status, 'capacity', None) or {}
        usage = metrics.get(ref.name, {})
        if not isinstance(usage, dict):
            usage = {}
            usage_unknown = True
        for name, threshold in (('cpu', cfg.thresholds.node_cpu_utilization),
                                ('memory', cfg.thresholds.node_memory_utilization)):
            try:
                raw = usage.get(name)
                if raw is None or not str(raw).strip():
                    raise ValueError('missing usage')
                numerator, denominator = parse_quantity(raw), parse_quantity(capacity[name])
                if not math.isfinite(numerator) or not math.isfinite(denominator) or numerator < 0 or denominator <= 0:
                    raise ValueError('invalid usage')
                ratio = numerator / denominator
                if ratio >= threshold:
                    utilization.append(make_finding('nodes.utilization', snapshot.cluster_id, ref, Severity.WARN,
                        'HighUtilization', f'Node {name} utilization high',
                        {'resource': name, 'ratio': ratio, 'threshold': threshold},
                        min(collection.observed_at, metrics_collection.observed_at)))
            except (KeyError, ValueError, TypeError, OverflowError):
                usage_unknown = True
    return (ready_result,
        evaluation(snapshot, 'nodes.utilization', ('nodes', 'node_metrics'), utilization, unknown=usage_unknown),
        evaluation(snapshot, 'nodes.pressure', ('nodes',), pressure, unknown=pressure_unknown))
