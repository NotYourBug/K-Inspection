from types import SimpleNamespace
from unittest.mock import MagicMock

from kubernetes import client

from k8s_inspector import checks
from k8s_inspector.config import InspectorConfig
from k8s_inspector.models import Status
from tests.factories import node, pod


def test_node_failure_survives_metrics_error(monkeypatch):
    core = SimpleNamespace(list_node=lambda: SimpleNamespace(items=[node("n1", ready=False)]))
    monkeypatch.setattr(checks, "get_node_metrics",
                        lambda custom: {"n1": {"cpu": "invalid", "memory": "1Gi"}})
    result = checks.check_nodes(core, None, InspectorConfig())
    assert result.status == Status.FAIL
    assert result.data["not_ready"] == ["n1"]
    assert result.data["collection_errors"]


def test_zero_allocatable_does_not_report_zero_usage(monkeypatch):
    value = node("n1", capacity={"cpu": "0", "memory": "0"})
    core = SimpleNamespace(list_node=lambda: SimpleNamespace(items=[value]))
    monkeypatch.setattr(checks, "get_node_metrics",
                        lambda custom: {"n1": {"cpu": "1", "memory": "1Gi"}})
    result = checks.check_nodes(core, None, InspectorConfig())
    assert result.status == Status.WARN
    assert result.data["utilization"][0]["cpu_utilization"] is None
    assert result.data["utilization"][0]["mem_utilization"] is None


def test_bad_metrics_for_one_node_preserves_other_node_usage(monkeypatch):
    core = SimpleNamespace(list_node=lambda: SimpleNamespace(items=[node("a"), node("b")]))
    monkeypatch.setattr(checks, "get_node_metrics", lambda custom: {
        "a": {"cpu": "4", "memory": "1Gi"}, "b": {"cpu": "invalid", "memory": "1Gi"}})
    result = checks.check_nodes(core, None, InspectorConfig())
    assert result.status == Status.WARN
    assert result.data["over_cpu"] == ["a"]
    assert result.data["collection_errors"]


def test_control_plane_health_failure_survives_component_forbidden(monkeypatch):
    monkeypatch.setattr(checks, "apiserver_healthz", lambda *a, **kw: {"ok": False})
    core = MagicMock()
    core.list_namespaced_pod.side_effect = PermissionError("forbidden")
    result = checks.check_control_plane(core, None, InspectorConfig())
    assert result.status == Status.FAIL
    assert result.data["apiserver_healthz"]["ok"] is False
    assert result.data["collection_errors"]


def test_managed_control_plane_absence_is_not_healthy(monkeypatch):
    monkeypatch.setattr(checks, "apiserver_healthz", lambda *a, **kw: {"ok": True})
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = []
    result = checks.check_control_plane(core, None, InspectorConfig())
    assert result.status == Status.WARN
    assert result.data["capability"] == "PARTIAL"


def addon_clients(value, desired=1):
    core, apps = MagicMock(), MagicMock()
    core.list_namespaced_pod.return_value.items = [value]
    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(name="addon", uid="addon-id"),
        spec=client.V1DeploymentSpec(replicas=desired, selector=client.V1LabelSelector(),
                                    template=client.V1PodTemplateSpec()),
        status=client.V1DeploymentStatus(ready_replicas=1))
    apps.list_namespaced_deployment.return_value.items = [deployment]
    apps.list_namespaced_daemon_set.return_value.items = []
    return core, apps


def test_addon_running_but_unready_is_not_pass():
    core, apps = addon_clients(pod("addon", ready=False))
    result = checks.check_kube_system_addons(core, InspectorConfig(), apps=apps)
    assert result.status == Status.WARN
    assert result.data["addons"]["coredns"]["ready"] == 0


def test_addon_desired_replicas_exceed_ready_pods_is_warn():
    core, apps = addon_clients(pod("addon"), desired=3)
    result = checks.check_kube_system_addons(core, InspectorConfig(), apps=apps)
    assert result.status == Status.WARN
    assert result.data["addons"]["coredns"]["expected_replicas"] == 3


def test_5m_never_requests_metrics():
    core, custom = MagicMock(), MagicMock()
    core.list_node.return_value.items = [node("n1")]
    core.list_pod_for_all_namespaces.return_value.items = [pod("p")]
    core.list_persistent_volume_claim_for_all_namespaces.return_value.items = []
    checks.run_all(core, MagicMock(), custom, None, InspectorConfig(), "5m")
    assert custom.mock_calls == []
