"""Generate deterministic shadow monitoring artifacts from the rule catalog."""
from dataclasses import asdict
import argparse
import hashlib
from pathlib import Path
import yaml

from k8s_inspector.monitoring.inventory import load_expected_targets
from k8s_inspector.inspection.catalog import CATALOG


def render_monitoring(catalog, targets, output: Path, *, shadow=True, quality_receiver_url=None):
    if not shadow and not quality_receiver_url:
        raise ValueError('active monitoring requires an independent quality receiver URL')
    output=Path(output)
    output.mkdir(parents=True,exist_ok=True)
    common={'shadow':'true'} if shadow else {}
    rules=[]
    def quality(name,expr,summary,description,wait='2m'):
        rules.append({'alert':name,'expr':expr,'for':wait,
            'labels':dict(common,severity='critical',category='quality'),
            'annotations':{'summary':summary,'description':description}})
    for r in catalog:
        selector='{profile="'+r.primary_profile+'",rule_id="'+r.rule_id+'",scope="cluster"}'
        # A new positive partial finding is evidence too. Unknown runs emit no zero.
        expression='max by (cluster, profile, rule_id, scope) ((last_over_time(inspector_last_valid_failures'+selector+'[30d]) > 0) or (last_over_time(inspector_confirmed_failures'+selector+'[30d]) > 0))'
        rules.append({'alert':'K8sInspectorBusinessFailure','expr':expression,'for':'1m',
            'labels':dict(common,category='business',severity='critical' if r.primary_profile in ('5m','10m') else 'warning',
                          rule_id=r.rule_id,profile=r.primary_profile,scope='cluster'),
            'annotations':{'summary':'Inspection finding: '+r.rule_id,
                'description':'Last valid unhealthy observation; verify recovery using fresh evidence.'}})
    quality('K8sInspectorTargetNeverRan',
        'inspector_expected_target unless on (cluster, profile) count_over_time(inspector_last_run_timestamp_seconds[30d])',
        'Expected inspection target has never run','Expected inventory has no run history for this target.','5m')
    quality('K8sInspectorTelemetryMissing',
        '(inspector_expected_target unless on (cluster, profile) inspector_last_run_timestamp_seconds) and on (cluster, profile) count_over_time(inspector_last_run_timestamp_seconds[30d])',
        'Inspection telemetry missing','Target ran before but its current telemetry is absent.','5m')
    quality('K8sInspectorRunStale',
        '(time() - max by (cluster, profile) (inspector_last_run_timestamp_seconds)) > on (cluster, profile) inspector_expected_max_run_age_seconds',
        'Inspection run is stale','Run completion time exceeded the expected target budget.')
    quality('K8sInspectorExpectedObservationMissing',
        'inspector_expected_rule unless on (cluster, profile, rule_id, scope) inspector_rule_observation_valid',
        'Expected rule observation missing','Expected rule has no current quality sample.')
    quality('K8sInspectorObservationInvalid',
        'max by (cluster, profile, rule_id, scope) (inspector_rule_observation_valid == 0) and on (cluster, profile, rule_id, scope) inspector_expected_rule',
        'Current observation is unknown','Incomplete or invalid collection cannot prove recovery.')
    quality('K8sInspectorObservationStale',
        '((time() - max by (cluster, profile, rule_id, scope) (inspector_last_valid_observation_timestamp_seconds)) > on (cluster, profile) group_left inspector_expected_max_run_age_seconds) and on (cluster, profile, rule_id, scope) inspector_expected_rule',
        'Last valid observation is stale','Stale evidence does not verify business recovery.')
    quality('K8sInspectorRunFailed',
        'max by (cluster, profile) (inspector_run_success == 0) and on (cluster, profile) inspector_expected_target',
        'Inspection execution or required output failed','Check collection quality and sink results in the run manifest.')
    quality('K8sInspectorSinkFailed',
        'max by (cluster, profile, sink) (inspector_sink_success == 0) and on (cluster, profile) inspector_expected_target',
        'Inspection output failed','A completed sink attempt failed; inspect local manifests and logs.')
    quality('K8sInspectorInventoryUnavailable',
        'max by (job) (up{job="inspector-inventory"} == 0) or absent(up{job="inspector-inventory"})',
        'Expected target inventory unavailable','Restore independent inventory; missing-target detection requires it.')
    quality('K8sInspectorPushgatewayUnavailable',
        'max by (job) (up{job="pushgateway"} == 0) or absent(up{job="pushgateway"})',
        'Inspection Pushgateway unavailable','Restore the telemetry path; absent metrics do not prove recovery.')
    inventory={'targets':[dict(asdict(t),rule_ids=list(t.rule_ids)) for t in sorted(targets,key=lambda t:(t.cluster_id,t.profile))]}
    inventory_text=yaml.safe_dump(inventory,sort_keys=False)
    config={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'k8s-inspector-expected-targets','namespace':'kube-system'},
            'data':{'expected-targets.yaml':inventory_text}}
    labels={'app':'k8s-inspector-inventory'}
    deployment={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'k8s-inspector-inventory','namespace':'kube-system'},
        'spec':{'replicas':1,'selector':{'matchLabels':labels},'template':{'metadata':{'labels':labels,
            'annotations':{'inspector/config-sha256':hashlib.sha256(inventory_text.encode()).hexdigest()}},
            'spec':{'automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001},
                'containers':[{'name':'inventory','image':'your-registry/k8s-inspector:0.1.0',
                    'command':['python','-m','k8s_inspector.expected_targets'],
                    'args':['--config','/etc/inventory/expected-targets.yaml'],
                    'env':[{'name':'PYTHONDONTWRITEBYTECODE','value':'1'}],
                    'ports':[{'containerPort':9400}],
                    'securityContext':{'readOnlyRootFilesystem':True,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},
                    'resources':{'requests':{'cpu':'10m','memory':'32Mi'},'limits':{'cpu':'100m','memory':'128Mi'}},
                    'readinessProbe':{'httpGet':{'path':'/readyz','port':9400}},
                    'livenessProbe':{'httpGet':{'path':'/healthz','port':9400}},
                    'volumeMounts':[{'name':'inventory','mountPath':'/etc/inventory','readOnly':True}]}],
                'volumes':[{'name':'inventory','configMap':{'name':'k8s-inspector-expected-targets'}}]}}}}
    service={'apiVersion':'v1','kind':'Service','metadata':{'name':'k8s-inspector-inventory','namespace':'kube-system'},
        'spec':{'selector':labels,'ports':[{'port':9400,'targetPort':9400}]}}
    scrape={'scrape_configs':[{'job_name':'inspector-inventory','static_configs':[{'targets':['k8s-inspector-inventory.kube-system.svc:9400']}]}]}
    receivers=[{'name':'shadow-null'},{'name':'feishu-bridge','webhook_configs':[{'url':'http://k8s-inspector-alert-bridge.kube-system.svc:8000/alert','send_resolved':True}]}]
    if quality_receiver_url:
        receivers.append({'name':'independent-quality','webhook_configs':[{'url':quality_receiver_url,'send_resolved':True}]})
    am={'route':{'receiver':'feishu-bridge','group_by':['cluster','profile','rule_id'],'group_wait':'30s',
        'group_interval':'5m','repeat_interval':'2h','routes':[
            {'matchers':['shadow="true"'],'receiver':'shadow-null'},
            {'matchers':['category="quality"'],'receiver':'independent-quality' if quality_receiver_url else 'shadow-null'}]},
        'receivers':receivers}
    artifacts={
        'inspection-rules.yaml':yaml.safe_dump({'groups':[{'name':'k8s-inspector-v2','rules':rules}]},sort_keys=False),
        'inventory.yaml':yaml.safe_dump_all([config,deployment,service],sort_keys=False),
        'scrape-snippet.yaml':yaml.safe_dump(scrape,sort_keys=False),
        'alertmanager-snippet.yaml':yaml.safe_dump(am,sort_keys=False),
    }
    paths=[]
    for name,content in artifacts.items():
        path=output/name
        with path.open('w',encoding='utf-8',newline='') as handle:
            handle.write(content)
        paths.append(path)
    return tuple(paths)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--activate',action='store_true')
    parser.add_argument('--quality-receiver-url')
    args=parser.parse_args(argv)
    render_monitoring(CATALOG,load_expected_targets(args.targets),args.output,
        shadow=not args.activate,quality_receiver_url=args.quality_receiver_url)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
