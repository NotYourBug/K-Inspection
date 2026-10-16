"""Tests for k8s_inspector.checks with mocked K8s API."""

from unittest.mock import MagicMock, patch

import pytest

from k8s_inspector.checks import (
    _pod_issue_from_obj,
    check_control_plane,
    check_kube_system_addons,
    check_nodes,
    check_pending_pvcs,
    check_pods,
    check_warning_events,
    run_all,
)
from k8s_inspector.config import InspectorConfig
from k8s_inspector.models import Status
from tests.factories import pod as make_sdk_pod, container_status, pvc as make_pvc, storage_class


@pytest.fixture
def cfg():
    return InspectorConfig()


# ---------------------------------------------------------------------------
# check_nodes
# ---------------------------------------------------------------------------


def _make_node(name, ready=True, taints=None, capacity_cpu="4", capacity_mem="8Gi"):
    node = MagicMock()
    node.metadata.name = name
    node.metadata.uid = f"uid-{name}"

    ready_cond = MagicMock()
    ready_cond.type = "Ready"
    ready_cond.status = "True" if ready else "False"
    node.status.conditions = [ready_cond]

    node.spec.taints = taints or []

    node.status.capacity = {"cpu": capacity_cpu, "memory": capacity_mem}
    return node


class TestCheckNodes:
    def test_all_ready_no_metrics(self, cfg):
        core = MagicMock()
        core.list_node.return_value.items = [_make_node("n1"), _make_node("n2")]
        custom = MagicMock()

        with patch("k8s_inspector.checks.get_node_metrics", return_value={}):
            result = check_nodes(core, custom, cfg, include_metrics=False)

        assert result.status == Status.PASS
        assert "2" in result.summary

    def test_not_ready_node_is_fail(self, cfg):
        core = MagicMock()
        core.list_node.return_value.items = [
            _make_node("n1", ready=True),
            _make_node("n2", ready=False),
        ]
        custom = MagicMock()

        with patch("k8s_inspector.checks.get_node_metrics", return_value={}):
            result = check_nodes(core, custom, cfg, include_metrics=False)

        assert result.status == Status.FAIL
        assert "n2" in result.data["not_ready"]

    def test_noschedule_taint_warns(self, cfg):
        taint = MagicMock()
        taint.effect = "NoSchedule"
        taint.key = "test.io/maintenance"
        core = MagicMock()
        core.list_node.return_value.items = [_make_node("n1", taints=[taint])]
        custom = MagicMock()

        with patch("k8s_inspector.checks.get_node_metrics", return_value={}):
            result = check_nodes(core, custom, cfg, include_metrics=False)

        assert result.status == Status.WARN
        assert len(result.data["noschedule_taints"]) == 1

    def test_allowed_noschedule_taint_passes(self, cfg):
        cfg.allow_noschedule_taints = ["node-role.kubernetes.io/control-plane"]
        taint = MagicMock()
        taint.effect = "NoSchedule"
        taint.key = "node-role.kubernetes.io/control-plane"
        core = MagicMock()
        core.list_node.return_value.items = [_make_node("n1", taints=[taint])]
        custom = MagicMock()

        with patch("k8s_inspector.checks.get_node_metrics", return_value={}):
            result = check_nodes(core, custom, cfg, include_metrics=False)

        assert result.status == Status.PASS

    def test_high_cpu_warns(self, cfg):
        core = MagicMock()
        core.list_node.return_value.items = [_make_node("n1", capacity_cpu="4")]
        custom = MagicMock()

        # 3.6 / 4 = 0.9 > 0.80 threshold
        metrics = {"n1": {"cpu": "3600m", "memory": "1Gi"}}
        with patch("k8s_inspector.checks.get_node_metrics", return_value=metrics):
            result = check_nodes(core, custom, cfg, include_metrics=True)

        assert result.status == Status.WARN
        assert "n1" in result.data["over_cpu"]

    def test_metrics_unavailable_warns(self, cfg):
        core = MagicMock()
        core.list_node.return_value.items = [_make_node("n1")]
        custom = MagicMock()

        with patch("k8s_inspector.checks.get_node_metrics", return_value=None):
            result = check_nodes(core, custom, cfg, include_metrics=True)

        # include_metrics=True but metrics unavailable
        assert result.status == Status.WARN

    def test_exception_is_graceful(self, cfg):
        core = MagicMock()
        core.list_node.side_effect = RuntimeError("boom")
        custom = MagicMock()

        result = check_nodes(core, custom, cfg)
        assert result.status == Status.WARN
        assert "error" in result.data


# ---------------------------------------------------------------------------
# check_pending_pvcs
# ---------------------------------------------------------------------------


class TestCheckPendingPvcs:
    def test_all_bound(self, cfg):
        core = MagicMock()
        pvc1 = MagicMock()
        pvc1.status.phase = "Bound"
        pvc1.metadata.namespace = "ns"
        pvc1.metadata.name = "pvc1"
        core.list_persistent_volume_claim_for_all_namespaces.return_value.items = [pvc1]

        result = check_pending_pvcs(core)
        assert result.status == Status.PASS

    def test_pending_pvc_is_fail(self, cfg):
        core = MagicMock()
        pvc = make_pvc("stuck-pvc", phase="Pending", storage_class="immediate")
        core.list_persistent_volume_claim_for_all_namespaces.return_value.items = [pvc]
        core.list_pod_for_all_namespaces.return_value.items = []
        storage = MagicMock()
        storage.list_storage_class.return_value.items = [storage_class("immediate")]
        result = check_pending_pvcs(core, cfg, storage)
        assert result.status == Status.FAIL
        assert len(result.data["pending"]) == 1

    def test_exception_is_graceful(self, cfg):
        core = MagicMock()
        core.list_persistent_volume_claim_for_all_namespaces.side_effect = RuntimeError("boom")
        result = check_pending_pvcs(core)
        assert result.status == Status.WARN


# ---------------------------------------------------------------------------
# check_pods
# ---------------------------------------------------------------------------


def _make_pod(name, ns, phase="Running", container_statuses=None):
    return make_sdk_pod(name, namespace=ns, phase=phase,
                        container_statuses=container_statuses or [])


def _make_cs(waiting_reason=None, restart_count=0):
    return container_status(waiting=waiting_reason, restarts=restart_count)


class TestCheckPods:
    def test_all_running(self, cfg):
        core = MagicMock()
        core.list_pod_for_all_namespaces.return_value.items = [
            _make_pod("p1", "ns", "Running", [_make_cs()]),
            _make_pod("p2", "ns", "Running", [_make_cs()]),
        ]
        result = check_pods(core, cfg)
        assert result.status == Status.PASS

    def test_failed_phase_is_fail(self, cfg):
        core = MagicMock()
        core.list_pod_for_all_namespaces.return_value.items = [
            _make_pod("p1", "ns", "Failed"),
        ]
        result = check_pods(core, cfg)
        assert result.status == Status.FAIL

    def test_crash_loop_is_fail(self, cfg):
        core = MagicMock()
        core.list_pod_for_all_namespaces.return_value.items = [
            _make_pod("p1", "ns", "Running", [_make_cs(waiting_reason="CrashLoopBackOff")]),
        ]
        result = check_pods(core, cfg)
        assert result.status == Status.FAIL

    def test_high_restart_is_warn(self, cfg):
        core = MagicMock()
        core.list_pod_for_all_namespaces.return_value.items = [
            _make_pod("p1", "ns", "Running", [_make_cs(restart_count=5)]),
        ]
        result = check_pods(core, cfg)
        assert result.status == Status.WARN

    def test_exception_is_graceful(self, cfg):
        core = MagicMock()
        core.list_pod_for_all_namespaces.side_effect = RuntimeError("boom")
        result = check_pods(core, cfg)
        assert result.status == Status.WARN


# ---------------------------------------------------------------------------
# check_warning_events
# ---------------------------------------------------------------------------


class TestCheckWarningEvents:
    def test_normal_event_count(self, cfg):
        core = MagicMock()
        core.list_event_for_all_namespaces.return_value.items = []
        result = check_warning_events(core, cfg)
        assert result.status == Status.PASS

    def test_high_event_count_warns(self, cfg):
        events = [MagicMock() for _ in range(100)]
        core = MagicMock()
        core.list_event_for_all_namespaces.return_value.items = events
        result = check_warning_events(core, cfg)
        assert result.status == Status.WARN

    def test_exception_is_graceful(self, cfg):
        core = MagicMock()
        core.list_event_for_all_namespaces.side_effect = RuntimeError("boom")
        result = check_warning_events(core, cfg)
        assert result.status == Status.WARN


# ---------------------------------------------------------------------------
# check_control_plane
# ---------------------------------------------------------------------------


class TestCheckControlPlane:
    def test_healthy(self, cfg):
        core = MagicMock()
        api_client = MagicMock()

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": True, "latency_ms": 50}):
            # Mock healthy component pods
            pod = make_sdk_pod("apiserver-1")
            core.list_namespaced_pod.return_value.items = [pod]

            result = check_control_plane(core, api_client, cfg)
            assert result.status == Status.PASS

    def test_healthz_not_ok_is_fail(self, cfg):
        core = MagicMock()
        api_client = MagicMock()

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": False, "latency_ms": 100, "error": "timeout"}):
            pod = make_sdk_pod("apiserver-1")
            core.list_namespaced_pod.return_value.items = [pod]

            result = check_control_plane(core, api_client, cfg)
            assert result.status == Status.FAIL

    def test_missing_pods_is_warn(self, cfg):
        core = MagicMock()
        api_client = MagicMock()

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": True, "latency_ms": 50}):
            core.list_namespaced_pod.return_value.items = []

            result = check_control_plane(core, api_client, cfg)
            assert result.status == Status.WARN

    def test_high_latency_is_warn(self, cfg):
        core = MagicMock()
        api_client = MagicMock()

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": True, "latency_ms": 600}):
            pod = make_sdk_pod("apiserver-1")
            core.list_namespaced_pod.return_value.items = [pod]

            result = check_control_plane(core, api_client, cfg)
            assert result.status == Status.WARN


# ---------------------------------------------------------------------------
# check_kube_system_addons
# ---------------------------------------------------------------------------


class TestCheckKubeSystemAddons:
    def test_all_healthy(self, cfg):
        from tests.test_failure_preservation import addon_clients
        core, apps = addon_clients(make_sdk_pod("addon-1"))
        result = check_kube_system_addons(core, cfg, apps=apps)
        assert result.status == Status.PASS

    def test_missing_addon_is_warn(self, cfg):
        core = MagicMock()
        core.list_namespaced_pod.return_value.items = []
        result = check_kube_system_addons(core, cfg)
        assert result.status == Status.WARN

    def test_exception_is_graceful(self, cfg):
        core = MagicMock()
        core.list_namespaced_pod.side_effect = RuntimeError("boom")
        result = check_kube_system_addons(core, cfg)
        assert result.status == Status.WARN


# ---------------------------------------------------------------------------
# run_all profile dispatch
# ---------------------------------------------------------------------------


class TestRunAll:
    def _mocks(self):
        return MagicMock(), MagicMock(), MagicMock(), MagicMock()

    def test_5m_profile_returns_dataplane_checks(self, cfg):
        core, storage, custom, api = self._mocks()
        core.list_node.return_value.items = []
        core.list_pod_for_all_namespaces.return_value.items = []
        core.list_persistent_volume_claim_for_all_namespaces.return_value.items = []

        results = run_all(core, storage, custom, api, cfg, profile="5m")
        names = {r.name for r in results}
        assert names == {"nodes", "pods", "pvcs"}

    def test_10m_profile_returns_control_plane_only(self, cfg):
        core, storage, custom, api = self._mocks()

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": True, "latency_ms": 10}):
            pod = MagicMock()
            pod.status.phase = "Running"
            pod.status.container_statuses = []
            pod.metadata.name = "cp-1"
            core.list_namespaced_pod.return_value.items = [pod]

            results = run_all(core, storage, custom, api, cfg, profile="10m")
            names = {r.name for r in results}
            assert names == {"control-plane"}

    def test_full_profile_returns_all_checks(self, cfg):
        core, storage, custom, api = self._mocks()
        core.list_node.return_value.items = []
        core.list_pod_for_all_namespaces.return_value.items = []
        core.list_persistent_volume_claim_for_all_namespaces.return_value.items = []
        core.list_event_for_all_namespaces.return_value.items = []

        with patch("k8s_inspector.checks.apiserver_healthz",
                   return_value={"ok": True, "latency_ms": 10}):
            core.list_namespaced_pod.return_value.items = [MagicMock()]

            results = run_all(core, storage, custom, api, cfg, profile="full")
            names = {r.name for r in results}
            assert names == {"control-plane", "nodes", "pods", "pvcs", "events", "kube-system-addons"}

    def test_unknown_profile_rejected_without_api(self, cfg):
        core, storage, custom, api = self._mocks()
        with pytest.raises(ValueError, match='profile'):
            run_all(core, storage, custom, api, cfg, profile="bogus")
        core.list_node.assert_not_called()
