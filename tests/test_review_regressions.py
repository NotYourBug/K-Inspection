"""Concrete failures found in the fresh whole-batch review."""
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes import client
from prometheus_client import CollectorRegistry

from k8s_inspector.inspection import checks
from k8s_inspector.core.config import InspectorConfig
from k8s_inspector.monitoring.metrics import populate_registry
from k8s_inspector.core.models import Report, Status
from k8s_inspector.reporting.sinks import execution_success
from tests.factories import NOW, container_status, node, pod, pvc, storage_class
from tests.test_failure_preservation import addon_clients
from tests.test_pvc_semantics import apis, inspect


@pytest.mark.parametrize("cpu", [None, "", "   "])
def test_missing_node_usage_is_unknown_not_zero(cpu):
    core, custom = MagicMock(), MagicMock()
    core.list_node.return_value.items = [node("n1")]
    usage = {"memory": "1Gi"}
    if cpu is not None:
        usage["cpu"] = cpu
    custom.list_cluster_custom_object.return_value = {
        "items": [{"metadata": {"name": "n1"}, "usage": usage}]}
    result = checks.check_nodes(core, custom, InspectorConfig())
    assert result.status == Status.WARN
    assert result.data["utilization"][0]["cpu_utilization"] is None
    assert result.data["utilization"][0]["mem_utilization"] == 0.125
    assert result.data["collection_errors"]
    report = Report(NOW.isoformat(), "prod", [result])
    assert execution_success(report) is False
    registry = CollectorRegistry()
    populate_registry(report, "hourly", registry)
    assert registry.get_sample_value("k8s_inspector_node_utilization",
        {"cluster": "prod", "profile": "hourly", "node": "n1", "resource": "cpu"}) is None


def test_real_zero_node_usage_remains_valid():
    core, custom = MagicMock(), MagicMock()
    core.list_node.return_value.items = [node("n1")]
    custom.list_cluster_custom_object.return_value = {
        "items": [{"metadata": {"name": "n1"}, "usage": {"cpu": "0", "memory": "0"}}]}
    result = checks.check_nodes(core, custom, InspectorConfig())
    assert result.status == Status.PASS
    assert result.data["utilization"][0]["cpu_utilization"] == 0
    assert not result.data["collection_errors"]


def delayed_claim_with_consumer(*, consumer_age, scheduled_age=None, unschedulable=False):
    value = pod("app", phase="Pending", ready=False, age_seconds=consumer_age, volumes=[
        client.V1Volume(name="data", persistent_volume_claim=
                        client.V1PersistentVolumeClaimVolumeSource(claim_name="data"))])
    if scheduled_age is not None:
        value.spec.node_name = "n1"
        value.status.conditions.append(client.V1PodCondition(
            type="PodScheduled", status="True",
            last_transition_time=NOW - timedelta(seconds=scheduled_age)))
    if unschedulable:
        value.status.conditions.append(client.V1PodCondition(
            type="PodScheduled", status="False", reason="Unschedulable",
            message="insufficient CPU", last_transition_time=NOW - timedelta(seconds=consumer_age)))
    core, storage = apis([pvc("data", phase="Pending", age_seconds=86400,
                              storage_class="delayed")],
                         [value], [storage_class("delayed", "WaitForFirstConsumer")])
    return inspect(core, storage)


def test_old_delayed_claim_with_new_consumer_does_not_fail():
    result = delayed_claim_with_consumer(consumer_age=1)
    assert result.status == Status.PASS
    assert result.data["pending"] == []
    assert result.data["within_grace"] == 1


def test_old_consumer_just_scheduled_gets_binding_grace():
    result = delayed_claim_with_consumer(consumer_age=86400, scheduled_age=1)
    assert result.status == Status.PASS
    assert result.data["pending"] == []


def test_unschedulable_consumer_does_not_blame_delayed_storage():
    result = delayed_claim_with_consumer(consumer_age=3600, unschedulable=True)
    assert result.status == Status.PASS
    assert result.data["pending"] == []
    assert result.data["blocked_by_consumers"][0]["reason"] == "Unschedulable"


def test_delayed_claim_after_confirmed_binding_grace_is_failure():
    result = delayed_claim_with_consumer(consumer_age=3600, scheduled_age=600)
    assert result.status == Status.FAIL
    assert result.data["pending"][0]["consumers"] == ["app"]


def test_old_consumer_without_scheduling_evidence_is_unknown():
    result = delayed_claim_with_consumer(consumer_age=3600)
    assert result.status == Status.WARN
    assert result.data["unknown"]


@pytest.mark.parametrize("visible", [True, False])
def test_control_plane_unknown_is_not_success(monkeypatch, visible):
    monkeypatch.setattr(checks, "apiserver_healthz", lambda *a, **kw: {"ok": True})
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = [pod("cp", conditions=[])] if visible else []
    result = checks.check_control_plane(core, None, InspectorConfig())
    assert result.status == Status.WARN
    assert result.data["capability"] == "PARTIAL"
    assert result.data["unknown"]
    assert execution_success(Report(NOW.isoformat(), "prod", [result])) is False


def test_addon_unknown_readiness_is_not_success():
    core, apps = addon_clients(pod("addon", conditions=[]))
    result = checks.check_kube_system_addons(core, InspectorConfig(), apps)
    assert result.data["unknown"]
    assert execution_success(Report(NOW.isoformat(), "prod", [result])) is False


def test_addon_unknown_expectations_are_not_success():
    core, apps = addon_clients(pod("addon"))
    apps.list_namespaced_deployment.return_value.items = []
    apps.list_namespaced_daemon_set.return_value.items = []
    result = checks.check_kube_system_addons(core, InspectorConfig(), apps)
    assert result.data["unknown"]
    assert execution_success(Report(NOW.isoformat(), "prod", [result])) is False


def test_control_plane_completed_component_is_current_failure(monkeypatch):
    monkeypatch.setattr(checks, "apiserver_healthz", lambda *a, **kw: {"ok": True})
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = [pod("cp", phase="Succeeded")]
    assert checks.check_control_plane(core, None, InspectorConfig()).status == Status.FAIL


def test_restartable_init_crash_is_unhealthy_even_in_pending_grace():
    value = pod("p", phase="Pending", ready=False, age_seconds=10,
                 init_containers=[client.V1Container(name="sidecar", restart_policy="Always")],
                 init_container_statuses=[
                     container_status("sidecar", waiting="CrashLoopBackOff", ready=False)])
    state = checks.evaluate_pod_state(value, InspectorConfig(), NOW)
    assert state["current_issues"][0]["reason"] == "CrashLoopBackOff"
    assert state["readiness"] == "UNHEALTHY"
