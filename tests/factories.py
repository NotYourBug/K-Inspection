"""Real Kubernetes objects for state semantics tests; no API connections."""
from datetime import datetime, timedelta, timezone

from kubernetes import client

NOW = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)


def container_status(name="app", *, waiting=None, exit_code=None, old_oom=False,
                     restarts=0, ready=True):
    if waiting:
        state = client.V1ContainerState(waiting=client.V1ContainerStateWaiting(reason=waiting))
    elif exit_code is not None:
        state = client.V1ContainerState(
            terminated=client.V1ContainerStateTerminated(exit_code=exit_code))
    else:
        state = client.V1ContainerState(running=client.V1ContainerStateRunning())
    last = (client.V1ContainerState(terminated=client.V1ContainerStateTerminated(
        exit_code=137, reason="OOMKilled", finished_at=NOW - timedelta(hours=1)))
        if old_oom else None)
    return client.V1ContainerStatus(name=name, image="test", image_id="test-id",
                                    ready=ready, restart_count=restarts,
                                    state=state, last_state=last)


def pod(name, phase="Running", ready=True, age_seconds=600, uid="pod-1", **overrides):
    namespace = overrides.pop("namespace", "ns")
    conditions = overrides.pop("conditions", [
        client.V1PodCondition(type="Ready", status="True" if ready else "False",
                             last_transition_time=NOW - timedelta(seconds=age_seconds))])
    statuses = overrides.pop("container_statuses", [container_status()])
    init_statuses = overrides.pop("init_container_statuses", [])
    volumes = overrides.pop("volumes", [])
    init_containers = overrides.pop("init_containers", [])
    deletion = overrides.pop("deletion_timestamp", None)
    owners = overrides.pop("owner_references", [])
    if overrides:
        raise TypeError(f"Unknown pod overrides: {sorted(overrides)}")
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace=namespace, uid=uid,
                                   creation_timestamp=NOW - timedelta(seconds=age_seconds),
                                   deletion_timestamp=deletion, owner_references=owners),
        spec=client.V1PodSpec(containers=[client.V1Container(name="app")],
                              init_containers=init_containers, volumes=volumes),
        status=client.V1PodStatus(phase=phase, conditions=conditions,
                                  container_statuses=statuses,
                                  init_container_statuses=init_statuses))


def pvc(name, phase="Bound", age_seconds=600, **overrides):
    namespace = overrides.pop("namespace", "ns")
    storage_class_name = overrides.pop("storage_class", None)
    if overrides:
        raise TypeError(f"Unknown PVC overrides: {sorted(overrides)}")
    return client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=name, namespace=namespace, uid=f"pvc-{name}",
                                   creation_timestamp=NOW - timedelta(seconds=age_seconds)),
        spec=client.V1PersistentVolumeClaimSpec(storage_class_name=storage_class_name),
        status=client.V1PersistentVolumeClaimStatus(phase=phase))


def storage_class(name, binding_mode="Immediate"):
    return client.V1StorageClass(metadata=client.V1ObjectMeta(name=name),
                                provisioner="test", volume_binding_mode=binding_mode)


def node(name, ready=True, **overrides):
    capacity = overrides.pop("capacity", {"cpu": "4", "memory": "8Gi"})
    if overrides:
        raise TypeError(f"Unknown node overrides: {sorted(overrides)}")
    return client.V1Node(
        metadata=client.V1ObjectMeta(name=name, uid=f"node-{name}"),
        spec=client.V1NodeSpec(taints=[]),
        status=client.V1NodeStatus(capacity=capacity, allocatable=capacity,
                                  conditions=[client.V1NodeCondition(
                                      type="Ready", status="True" if ready else "False")]))
