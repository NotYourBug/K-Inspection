"""Independent pure API, component and addon observations."""
import math

from .observation_models import ObservationHealth as H, Severity, ResourceRef, make_finding
from .rule_evaluation import evaluation, resource_ref
from .rules_pods import evaluate_pod_state


def evaluate_api(snapshot, cfg):
    collection = snapshot.collections.get('api_health')
    findings, unknown = [], True
    for health in collection.items if collection else ():
        latency = health.get('latency_ms')
        evidence = {'status_code': health.get('status_code'), 'latency_ms': latency}
        if health.get('ok') is False:
            findings.append(make_finding('control.api', snapshot.cluster_id,
                ResourceRef('APIServer', 'healthz'), Severity.FAIL, 'APIUnhealthy',
                'API health endpoint is unhealthy', evidence, collection.observed_at))
        elif health.get('ok') is True and type(latency) in (int, float) and math.isfinite(latency) and latency >= 500:
            findings.append(make_finding('control.api', snapshot.cluster_id,
                ResourceRef('APIServer', 'healthz'), Severity.WARN, 'APISlow',
                'API health endpoint response took at least 500 ms', evidence, collection.observed_at))
        unknown = health.get('ok') not in (True, False)
    return evaluation(snapshot, 'control.api', ('api_health',), findings, unknown=unknown)


def _pods(snapshot, cfg):
    collection = snapshot.collections.get('pods')
    return [p for p in collection.items if p.metadata.namespace == cfg.control_plane_namespace] if collection else []


def evaluate_components(snapshot, cfg):
    collection = snapshot.collections.get('pods')
    pods = _pods(snapshot, cfg)
    findings, missing, resources, visible, unknown = [], [], [], 0, False
    for name in ('kube-apiserver', 'kube-controller-manager', 'kube-scheduler'):
        matches = [p for p in pods if (p.metadata.labels or {}).get('component') == name]
        if not matches:
            missing.append(name)
        for pod in matches:
            visible += 1
            ref = resource_ref(pod, 'Pod')
            state = evaluate_pod_state(pod, cfg, collection.observed_at)
            bad = bool(state['current_issues']) or state['readiness'] == 'COMPLETED'
            health = H.UNHEALTHY if bad else H.HEALTHY if state['readiness']=='HEALTHY' else H.UNKNOWN
            resources.append((ref, health))
            unknown |= health==H.UNKNOWN
            if bad:
                findings.append(make_finding('control.components', snapshot.cluster_id, ref,
                    Severity.FAIL, 'ComponentUnhealthy', 'Control component unhealthy',
                    {'component': name, 'readiness': state['readiness'], 'issues': state['current_issues']}, collection.observed_at))
    return evaluation(snapshot, 'control.components', ('pods',), findings, resources=resources,
        unknown=unknown or bool(missing), unsupported=bool(collection and collection.complete and not visible),
        reason='Components not visible' if missing else None)


def _selected(pod, selector):
    # Controller selectors, rather than the broad addon label, define membership.
    if selector is None:
        return None
    labels = pod.metadata.labels or {}
    if any(labels.get(k)!=v for k,v in (selector.match_labels or {}).items()):
        return False
    for expression in selector.match_expressions or []:
        key, op, values = expression.key, expression.operator, expression.values or []
        if op=='In' and labels.get(key) not in values: return False
        if op=='NotIn' and labels.get(key) in values: return False
        if op=='Exists' and key not in labels: return False
        if op=='DoesNotExist' and key in labels: return False
        if op not in ('In','NotIn','Exists','DoesNotExist'): return None
    return True


def evaluate_addons(snapshot, cfg):
    collection = snapshot.collections.get('pods')
    deployments = snapshot.collections.get('deployments')
    daemonsets = snapshot.collections.get('daemonsets')
    controllers = [(d, 'Deployment') for d in deployments.items] if deployments else []
    controllers += [(d, 'DaemonSet') for d in daemonsets.items] if daemonsets else []
    pods = _pods(snapshot, cfg)
    findings, resources, unknown, supported = [], [], False, 0
    for controller, kind in controllers:
        label = (controller.metadata.labels or {}).get('k8s-app')
        if controller.metadata.namespace != cfg.control_plane_namespace or label not in ('kube-proxy','calico-node','kube-dns'):
            continue
        supported += 1
        ref = resource_ref(controller, kind)
        spec = getattr(controller, 'spec', None)
        desired = (spec.replicas if spec and spec.replicas is not None else 1) if kind=='Deployment' else getattr(controller.status,'desired_number_scheduled',None)
        membership = [(p, _selected(p, getattr(spec, 'selector', None))) for p in pods]
        matching = [p for p, selected in membership if selected is True]
        states = [evaluate_pod_state(p, cfg, collection.observed_at) for p in matching]
        ready = sum(s['readiness']=='HEALTHY' for s in states)
        complete = (bool(collection and collection.complete) and all(selected is not None for _,selected in membership)
                    and getattr(spec,'selector',None) is not None and type(desired) is int and desired>=0)
        health = H.UNKNOWN
        if complete:
            if ready < desired:
                # Known grace/unknown Pod states do not prove a stable shortage.
                uncertain = any(s['readiness'] in ('UNKNOWN','GRACE','TERMINATING') for s in states)
                if not uncertain:
                    health = H.UNHEALTHY
                    findings.append(make_finding('addons.ready', snapshot.cluster_id, ref,
                        Severity.WARN, 'AddonNotReady', 'Addon Ready replicas below expectation',
                        {'addon':label,'expected_replicas':desired,'ready':ready}, collection.observed_at))
            else:
                health = H.HEALTHY
        unknown |= health==H.UNKNOWN
        resources.append((ref,health))
    for label in ('kube-proxy','calico-node','kube-dns'):
        if any((p.metadata.labels or {}).get('k8s-app')==label for p in pods) and not any(
            c.metadata.namespace==cfg.control_plane_namespace and (c.metadata.labels or {}).get('k8s-app')==label
            for c,_ in controllers):
            unknown = True
    visible_complete = bool(collection and collection.complete and deployments and deployments.complete
                            and daemonsets and daemonsets.complete)
    return evaluation(snapshot, 'addons.ready', ('pods','deployments','daemonsets'), findings, resources=resources,
        unknown=unknown, unsupported=not supported and visible_complete)


EVALUATORS = {'control.api':evaluate_api,'control.components':evaluate_components,'addons.ready':evaluate_addons}


def evaluate_control(snapshot,cfg):
    return tuple(fn(snapshot,cfg) for fn in EVALUATORS.values())
