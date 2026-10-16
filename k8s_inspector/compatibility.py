"""Project v2 evidence into the legacy groups used by shipped monitoring."""
from .models import CheckResult, Status
from .observation_models import CheckEvaluation, ExecutionStatus as E, Severity, to_legacy_result


GROUPS = {'nodes':('nodes.',), 'pods':('pods.',), 'pvcs':('pvc.',),
          'control-plane':('control.',), 'events':('events.',), 'kube-system-addons':('addons.',)}


def legacy_results(report):
    if report.schema_version!=2 or not report.executions:
        return report.results
    executions = {e.rule_id:e for e in report.executions}
    faults = [f for f in report.findings if f.severity in (Severity.WARN,Severity.FAIL)]
    def known(rule_id):
        e = executions.get(rule_id)
        return bool(e and e.execution_status==E.OK and e.coverage.complete)
    def records(rule_id, predicate=lambda f:True):
        matching = [f for f in faults if f.rule_id==rule_id and predicate(f)]
        if not matching and not known(rule_id):
            return None
        return [dict(f.evidence, name=f.resource.name, node=f.resource.name,
                     namespace=f.resource.namespace, uid=f.resource.uid, reason=f.reason) for f in matching]
    def count(items):
        return len(items) if items is not None else None
    per_rule = [to_legacy_result(CheckEvaluation(e,
        tuple(f for f in report.findings if f.rule_id==e.rule_id),
        tuple(o for o in report.observations if o.rule_id==e.rule_id))) for e in report.executions]
    results, assigned = [], set()
    for name,prefixes in GROUPS.items():
        members = [r for r in per_rule if r.name.startswith(prefixes)]
        if not members:
            continue
        assigned.update(r.name for r in members)
        data, counts = {}, {}
        if name=='nodes':
            for key,rule,predicate in (
                ('not_ready','nodes.ready',lambda f:True),
                ('noschedule_taints','nodes.pressure',lambda f:f.reason=='NoScheduleTaint'),
                ('over_cpu','nodes.utilization',lambda f:f.evidence.get('resource')=='cpu'),
                ('over_memory','nodes.utilization',lambda f:f.evidence.get('resource')=='memory')):
                data[key]=records(rule,predicate)
                counts[key]=count(data[key])
            data['utilization'] = [dict(f.evidence,node=f.resource.name) for f in report.findings
                if f.rule_id=='nodes.utilization' and f.reason=='NodeUtilization' and f.severity==Severity.INFO]
        elif name=='pods':
            data['bad']=records('pods.current')
            data['restart_heavy']=records('pods.history',lambda f:f.reason=='RestartCountHigh')
            data['history']=records('pods.history')
            counts={'bad':count(data['bad']),'restart_heavy':count(data['restart_heavy'])}
        elif name=='pvcs':
            data['pending']=records('pvc.binding',lambda f:f.reason=='PVCBindingStalled')
            data['lost']=records('pvc.binding',lambda f:f.reason=='PVCLost')
            counts={'pending':count(data['pending'])}
        elif name=='events':
            cursors={f.evidence['event_uid']:f.evidence for f in report.findings
                if f.rule_id=='events.warning' and f.reason=='EventCursor'}
            data['count']=sum(c['activity_count'] for c in cursors.values()) if cursors else 0 if known('events.warning') else None
            data['sample']=list(cursors.values())
            data['count_is_lower_bound']=not known('events.warning')
            counts={'warning':data['count']}
        elif name=='control-plane':
            bad=records('control.api')
            ok=False if bad else True if known('control.api') else None
            data['apiserver_healthz']={'ok':ok,'status_code':bad[0].get('status_code') if bad else 200 if ok else None}
            data['components']=records('control.components')
            counts={'healthz_not_ok':int(not ok) if ok is not None else None}
        elif name=='kube-system-addons':
            bad=records('addons.ready')
            data['addons']={r.get('addon',r['name']):{'bad':[r],
                'expected_replicas':r.get('expected_replicas'),'ready':r.get('ready')} for r in bad or []}
            counts={'addons_bad':len(data['addons']) if bad is not None else None,
                    'addons_missing':0 if known('addons.ready') else None}
        # Explicit counters omit unknowns rather than mapping absent detail to 0.
        data['_issue_counts']={k:v for k,v in counts.items() if v is not None}
        data['collection_errors']=[{'rule_id':r.name,'error_kind':executions[r.name].error_kind}
            for r in members if executions[r.name].error_kind]
        status=next(st for st in (Status.FAIL,Status.WARN,Status.PASS,Status.SKIP) if any(r.status==st for r in members))
        results.append(CheckResult(name,status,'; '.join(r.summary for r in members),data,
                                   sum(r.duration_ms or 0 for r in members)))
    results.extend(r for r in per_rule if r.name not in assigned)
    return results
