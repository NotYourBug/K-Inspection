"""Shared pure evidence construction; never collects resources."""
from .observation_models import (
    CheckEvaluation, CheckExecution, Coverage, ExecutionStatus as E,
    ObservationHealth as H, ResourceRef, RuleObservation, Severity,
)


def resource_ref(obj, kind):
    md = getattr(obj, 'metadata', None)
    owners = getattr(md, 'owner_references', None) or []
    return ResourceRef(kind, getattr(md, 'name', None) or '<unknown>',
        getattr(md, 'namespace', None), getattr(md, 'uid', None),
        owner_uid=getattr(owners[0], 'uid', None) if owners else None,
        node=getattr(getattr(obj, 'spec', None), 'node_name', None))


def evaluation(snapshot, rule_id, keys, findings=(), *, unknown=False, unsupported=False,
               resources=(), reason=None):
    collections = [snapshot.collections[k] for k in keys if k in snapshot.collections]
    complete = (len(collections) == len(keys) and all(c.complete and c.execution_status == E.OK
                for c in collections) and not unknown and not unsupported)
    observed = len(collections[0].items) if collections else 0
    timestamp = min((c.observed_at for c in collections), default=snapshot.started_at)
    errors = [c.error_kind for c in collections if c.error_kind]
    if unsupported or (collections and all(c.execution_status == E.UNSUPPORTED for c in collections)):
        status = E.UNSUPPORTED
    elif complete:
        status = E.OK
    else:
        status = E.PARTIAL if any(c.items for c in collections) else E.ERROR
    faults = tuple(f for f in findings if f.severity in (Severity.WARN, Severity.FAIL))
    health = H.UNHEALTHY if faults else H.HEALTHY
    observations = [RuleObservation(rule_id, snapshot.cluster_id, 'cluster', None,
        health if complete else H.UNKNOWN, timestamp, complete, snapshot.run_id)]
    resources = resources or tuple((f.resource, H.UNHEALTHY) for f in faults)
    seen_resources = set()
    for ref, resource_health in resources:
        if ref.uid and (ref.namespace, ref.uid) not in seen_resources:
            seen_resources.add((ref.namespace, ref.uid))
            observations.append(RuleObservation(rule_id, snapshot.cluster_id,
                f'namespace:{ref.namespace}' if ref.namespace else 'cluster', ref.uid,
                resource_health, timestamp, resource_health != H.UNKNOWN, snapshot.run_id))
    execution = CheckExecution(rule_id, status, timestamp, 0,
        Coverage(observed if complete else None, observed, complete),
        errors[0] if errors else ('INCOMPLETE' if not complete and not unsupported else None),
        reason or (f'{rule_id}: {len(findings)} findings' if complete else f'{rule_id}: coverage unknown'))
    return CheckEvaluation(execution, tuple(findings), tuple(observations))
