"""Independent pure Node rules over a shared list snapshot."""
import math
from k8s_inspector.core.resources import parse_quantity
from k8s_inspector.core.observations import ObservationHealth as H, Severity, make_finding
from k8s_inspector.inspection.rules.evaluation import evaluation, resource_ref


def evaluate_ready(snapshot, cfg):
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
    return evaluation(snapshot, 'nodes.ready', ('nodes',), findings,
                      unknown=unknown or not nodes, resources=resources)


def evaluate_pressure(snapshot, cfg):
    collection = snapshot.collections.get('nodes')
    nodes = collection.items if collection else ()
    findings, resources, unknown = [], [], not nodes
    for node in nodes:
        ref = resource_ref(node, 'Node')
        conditions = {c.type: c.status for c in getattr(getattr(node, 'status', None), 'conditions', None) or []}
        node_findings, node_unknown = [], False
        for kind in ('MemoryPressure', 'DiskPressure', 'PIDPressure'):
            if conditions.get(kind) == 'True':
                node_findings.append(make_finding('nodes.pressure', snapshot.cluster_id, ref, Severity.WARN,
                    kind, f'Node {kind}', {'condition': kind}, collection.observed_at))
            elif conditions.get(kind) != 'False':
                node_unknown = True
        for taint in getattr(getattr(node, 'spec', None), 'taints', None) or []:
            if taint.effect == 'NoSchedule' and taint.key not in cfg.allow_noschedule_taints:
                node_findings.append(make_finding('nodes.pressure', snapshot.cluster_id, ref, Severity.WARN,
                    'NoScheduleTaint', 'Node scheduling restricted', {'taint': taint.key}, collection.observed_at))
        unknown |= node_unknown
        resources.append((ref, H.UNHEALTHY if node_findings else H.UNKNOWN if node_unknown else H.HEALTHY))
        findings.extend(node_findings)
    return evaluation(snapshot, 'nodes.pressure', ('nodes',), findings, unknown=unknown, resources=resources)


def evaluate_utilization(snapshot, cfg):
    collection = snapshot.collections.get('nodes')
    nodes = collection.items if collection else ()
    metrics_collection = snapshot.collections.get('node_metrics')
    metrics = {}
    for metric in metrics_collection.items if metrics_collection else ():
        if not isinstance(metric, dict) or not isinstance(metric.get('metadata'), dict):
            continue
        name = metric['metadata'].get('name')
        if isinstance(name, str) and name:
            metrics[name] = metric.get('usage')
    findings, measurements, resources, unknown = [], [], [], not nodes
    for node in nodes:
        ref = resource_ref(node, 'Node')
        status = getattr(node, 'status', None)
        capacity = getattr(status, 'allocatable', None)
        if not isinstance(capacity, dict):
            capacity = getattr(status, 'capacity', None) or {}
        usage = metrics.get(ref.name)
        node_unknown, node_bad, ratios = False, False, {}
        for name, threshold in (('cpu', cfg.thresholds.node_cpu_utilization),
                                ('memory', cfg.thresholds.node_memory_utilization)):
            try:
                raw = usage.get(name) if isinstance(usage, dict) else None
                if raw is None or not str(raw).strip():
                    raise ValueError('missing usage')
                numerator, denominator = parse_quantity(raw), parse_quantity(capacity[name])
                if not math.isfinite(numerator) or not math.isfinite(denominator) or numerator < 0 or denominator <= 0:
                    raise ValueError('invalid usage')
                ratio = numerator / denominator
                ratios[name] = ratio
                if ratio >= threshold:
                    node_bad = True
                    findings.append(make_finding('nodes.utilization', snapshot.cluster_id, ref, Severity.WARN,
                        'HighUtilization', f'Node {name} utilization high',
                        {'resource': name, 'ratio': ratio, 'threshold': threshold},
                        min(collection.observed_at, metrics_collection.observed_at)))
            except (KeyError, ValueError, TypeError, OverflowError):
                node_unknown = True
        # INFO retains healthy measurements without making them business faults.
        if ratios:
            measurements.append(make_finding('nodes.utilization', snapshot.cluster_id, ref, Severity.INFO,
                'NodeUtilization', 'Observed Node utilization',
                {'cpu_utilization': ratios.get('cpu'), 'mem_utilization': ratios.get('memory')},
                min(collection.observed_at, metrics_collection.observed_at)))
        unknown |= node_unknown
        metrics_complete = bool(metrics_collection and metrics_collection.complete)
        resources.append((ref, H.UNHEALTHY if node_bad else H.UNKNOWN
                          if node_unknown or not metrics_complete else H.HEALTHY))
    return evaluation(snapshot, 'nodes.utilization', ('nodes', 'node_metrics'), findings + measurements,
                      unknown=unknown, resources=resources)


EVALUATORS = {'nodes.ready': evaluate_ready, 'nodes.utilization': evaluate_utilization,
              'nodes.pressure': evaluate_pressure}


def evaluate_nodes(snapshot, cfg):
    # Compatibility entry point; production catalog calls just its own evaluator.
    return tuple(fn(snapshot, cfg) for fn in EVALUATORS.values())
