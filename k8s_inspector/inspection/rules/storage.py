"""Pure PVC binding evaluation with consumer and StorageClass evidence."""
from k8s_inspector.core.observations import ObservationHealth as H, Severity, make_finding
from k8s_inspector.inspection.rules.evaluation import evaluation, resource_ref
from k8s_inspector.inspection.rules.pods import _elapsed


def evaluate_storage(snapshot, cfg):
    collection = snapshot.collections.get('pvcs')
    classes_collection = snapshot.collections.get('storage_classes')
    pods_collection = snapshot.collections.get('pods')
    classes = {s.metadata.name: s for s in classes_collection.items} if classes_collection else {}
    pods = pods_collection.items if pods_collection else ()
    findings, resources, unknown = [], [], False
    keys = {'pvcs'}
    for pvc in collection.items if collection else ():
        ref = resource_ref(pvc, 'PersistentVolumeClaim')
        phase = getattr(pvc.status, 'phase', None)
        reason, evidence, health = None, {}, H.UNKNOWN
        if phase == 'Lost':
            reason, health = 'PVCLost', H.UNHEALTHY
        elif phase == 'Bound':
            health = H.HEALTHY
        elif phase == 'Pending':
            age = _elapsed(pvc.metadata.creation_timestamp, snapshot.started_at)
            if age is not None and age < cfg.pvc_pending_grace_seconds:
                # Known expected binding grace, not a claim of recovered old UID.
                health = H.UNKNOWN
            elif age is not None:
                keys.add('storage_classes')
                sc_name = pvc.spec.storage_class_name if pvc.spec else None
                sc = classes.get(sc_name)
                association_ok = classes_collection and classes_collection.complete
                if association_ok and (sc is not None or sc_name == ''):
                    mode = (sc.volume_binding_mode or 'Immediate') if sc else 'Static'
                    evidence = {'binding_mode': mode, 'storage_class': sc_name, 'consumers': []}
                    if mode != 'WaitForFirstConsumer':
                        reason, health = 'PVCBindingStalled', H.UNHEALTHY
                    else:
                        keys.add('pods')
                        consumers = [p for p in pods if p.metadata.namespace == ref.namespace
                            and not p.metadata.deletion_timestamp and p.spec
                            and (not p.status or p.status.phase not in ('Succeeded', 'Failed'))
                            and any(v.persistent_volume_claim and
                                v.persistent_volume_claim.claim_name == ref.name for v in p.spec.volumes or [])]
                        evidence['consumers'] = sorted(p.metadata.name for p in consumers)
                        health = H.HEALTHY if pods_collection and pods_collection.complete else H.UNKNOWN
                        for pod in consumers:
                            scheduled = next((c for c in pod.status.conditions or [] if c.type == 'PodScheduled'), None) if pod.status else None
                            pod_age = _elapsed(pod.metadata.creation_timestamp, snapshot.started_at)
                            if pod_age is not None and pod_age < cfg.pvc_pending_grace_seconds:
                                health = H.UNKNOWN
                            elif scheduled and scheduled.status == 'True':
                                scheduled_age = _elapsed(scheduled.last_transition_time, snapshot.started_at)
                                if pod_age is not None and scheduled_age is not None and min(pod_age, scheduled_age) >= cfg.pvc_pending_grace_seconds:
                                    reason, health = 'PVCBindingStalled', H.UNHEALTHY
                                    break
                                health = H.UNKNOWN
                            elif scheduled and scheduled.status == 'False':
                                # Scheduling failure belongs to the Pod rule; PVC waits normally.
                                pass
                            else:
                                health = H.UNKNOWN
        unknown |= health == H.UNKNOWN
        resources.append((ref, health))
        if reason:
            findings.append(make_finding('pvc.binding', snapshot.cluster_id, ref, Severity.FAIL,
                reason, 'PVC binding failure', evidence, collection.observed_at))
    return (evaluation(snapshot, 'pvc.binding', tuple(sorted(keys)), findings, unknown=unknown,
                       resources=resources),)
