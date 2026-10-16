"""Pure API, visible control components, and known addon readiness."""
from .observation_models import ObservationHealth as H, Severity, make_finding
from .rule_evaluation import evaluation, resource_ref
from .rules_pods import evaluate_pod_state


def evaluate_control(snapshot, cfg):
    api_collection = snapshot.collections.get('api_health')
    api_findings, api_unknown = [], True
    for health in api_collection.items if api_collection else ():
        if health.get('ok') is False:
            from .observation_models import ResourceRef
            api_findings.append(make_finding('control.api', snapshot.cluster_id,
                ResourceRef('APIServer', 'healthz'), Severity.FAIL, 'APIUnhealthy',
                'API health endpoint is unhealthy', {'status_code': health.get('status_code')}, api_collection.observed_at))
        api_unknown = health.get('ok') not in (True, False)
    api_result = evaluation(snapshot, 'control.api', ('api_health',), api_findings, unknown=api_unknown)
    collection = snapshot.collections.get('pods')
    pods = [p for p in collection.items if p.metadata.namespace == cfg.control_plane_namespace] if collection else []
    components, missing, visible, unknown = [], [], 0, False
    for name in ('kube-apiserver', 'kube-controller-manager', 'kube-scheduler'):
        matches = [p for p in pods if (p.metadata.labels or {}).get('component') == name]
        if not matches:
            missing.append(name)
        for pod in matches:
            visible += 1
            state = evaluate_pod_state(pod, cfg, snapshot.started_at)
            if state['current_issues'] or state['readiness'] == 'COMPLETED':
                components.append(make_finding('control.components', snapshot.cluster_id, resource_ref(pod, 'Pod'),
                    Severity.FAIL, 'ComponentUnhealthy', 'Control component unhealthy',
                    {'component': name, 'readiness': state['readiness'], 'issues': state['current_issues']}, collection.observed_at))
            elif state['readiness'] != 'HEALTHY':
                unknown = True
    component_result = evaluation(snapshot, 'control.components', ('pods',), components,
        unknown=unknown or bool(missing), unsupported=bool(collection and collection.complete and not visible),
        reason='Components not visible' if missing else None)
    deployments = snapshot.collections.get('deployments')
    daemonsets = snapshot.collections.get('daemonsets')
    controllers = [(d, 'Deployment') for d in deployments.items] if deployments else []
    controllers += [(d, 'DaemonSet') for d in daemonsets.items] if daemonsets else []
    addon_findings, addon_unknown, supported = [], False, 0
    for label in ('kube-proxy', 'calico-node', 'kube-dns'):
        matches = [(c, kind) for c, kind in controllers if c.metadata.namespace == cfg.control_plane_namespace
                   and (c.metadata.labels or {}).get('k8s-app') == label]
        matching_pods = [p for p in pods if (p.metadata.labels or {}).get('k8s-app') == label]
        if not matches:
            addon_unknown |= bool(matching_pods)
            continue
        supported += 1
        expected = 0
        for controller, kind in matches:
            desired = (controller.spec.replicas if controller.spec.replicas is not None else 1) if kind == 'Deployment' else getattr(controller.status, 'desired_number_scheduled', None)
            if type(desired) is not int or desired < 0:
                addon_unknown = True
            else:
                expected += desired
        ready = sum(evaluate_pod_state(p, cfg, snapshot.started_at)['readiness'] == 'HEALTHY' for p in matching_pods)
        if ready < expected:
            controller, kind = matches[0]
            addon_findings.append(make_finding('addons.ready', snapshot.cluster_id, resource_ref(controller, kind),
                Severity.WARN, 'AddonNotReady', 'Addon Ready replicas below expectation',
                {'addon': label, 'expected_replicas': expected, 'ready': ready}, snapshot.started_at))
        elif any(evaluate_pod_state(p, cfg, snapshot.started_at)['readiness'] == 'UNKNOWN' for p in matching_pods):
            addon_unknown = True
    addon_result = evaluation(snapshot, 'addons.ready', ('pods', 'deployments', 'daemonsets'), addon_findings,
        unknown=addon_unknown, unsupported=not supported and not pods and bool(collection and collection.complete))
    return api_result, component_result, addon_result
