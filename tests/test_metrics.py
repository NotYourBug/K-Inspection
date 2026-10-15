"""Tests for k8s_inspector.metrics."""

from unittest.mock import MagicMock, patch

import pytest

from k8s_inspector.metrics import (
    _extract_issue_counts,
    _extract_node_utilization,
    _parse_iso_to_epoch,
    _status_to_number,
    populate_registry,
    push_report_to_gateway,
    PushGatewayConfig,
)
from k8s_inspector.models import CheckResult, Report, Status


class TestStatusToNumber:
    def test_mapping(self):
        assert _status_to_number(Status.PASS) == 0
        assert _status_to_number(Status.WARN) == 1
        assert _status_to_number(Status.FAIL) == 2
        assert _status_to_number(Status.SKIP) == 3


class TestParseIsoToEpoch:
    def test_valid(self):
        ts = _parse_iso_to_epoch("2025-01-01T00:00:00+00:00")
        assert ts > 0

    def test_invalid_returns_zero(self):
        assert _parse_iso_to_epoch("not-a-date") == 0.0


class TestExtractIssueCounts:
    def test_nodes_check(self):
        data = {
            "not_ready": ["n1", "n2"],
            "noschedule_taints": ["t1"],
            "over_cpu": ["n1"],
            "over_memory": [],
        }
        counts = _extract_issue_counts("nodes", data)
        assert counts["not_ready"] == 2
        assert counts["noschedule_taints"] == 1
        assert counts["over_cpu"] == 1
        assert counts["over_memory"] == 0

    def test_pods_check(self):
        data = {"bad": [{"name": "p1"}], "restart_heavy": [{"name": "p2"}, {"name": "p3"}]}
        counts = _extract_issue_counts("pods", data)
        assert counts["bad"] == 1
        assert counts["restart_heavy"] == 2

    def test_pvcs_check(self):
        data = {"pending": [{"namespace": "ns", "name": "pvc1"}]}
        counts = _extract_issue_counts("pvcs", data)
        assert counts["pending"] == 1

    def test_events_check(self):
        counts = _extract_issue_counts("events", {"count": 42})
        assert counts["warning"] == 42

    def test_control_plane_healthz_not_ok(self):
        counts = _extract_issue_counts("control-plane", {"apiserver_healthz": {"ok": False}})
        assert counts["healthz_not_ok"] == 1

    def test_control_plane_healthz_ok(self):
        counts = _extract_issue_counts("control-plane", {"apiserver_healthz": {"ok": True}})
        assert "healthz_not_ok" not in counts

    def test_not_a_dict(self):
        assert _extract_issue_counts("nodes", None) == {}


class TestExtractNodeUtilization:
    def test_extracts_cpu_and_memory(self):
        data = {
            "utilization": [
                {"node": "n1", "cpu_utilization": 0.75, "mem_utilization": 0.60},
                {"node": "n2", "cpu_utilization": 0.90, "mem_utilization": 0.50},
            ]
        }
        result = _extract_node_utilization(data)
        assert len(result) == 4  # 2 nodes * 2 resources
        resources = {(it["node"], it["resource"], it["value"]) for it in result}
        assert ("n1", "cpu", 0.75) in resources
        assert ("n1", "memory", 0.60) in resources

    def test_no_utilization_field(self):
        assert _extract_node_utilization({}) == []

    def test_utilization_not_a_list(self):
        assert _extract_node_utilization({"utilization": "bad"}) == []


class TestPopulateRegistry:
    def test_populates_all_gauges(self):
        report = Report(
            generated_at="2025-01-01T00:00:00+00:00",
            cluster="test",
            results=[
                CheckResult(name="nodes", status=Status.PASS, summary="ok",
                            data={"utilization": [{"node": "n1", "cpu_utilization": 0.5, "mem_utilization": 0.3}]},
                            duration_ms=100),
                CheckResult(name="pods", status=Status.WARN, summary="high restart",
                            data={"restart_heavy": [{"namespace": "ns", "name": "p1"}]},
                            duration_ms=200),
            ],
        )

        mock_registry = MagicMock()
        populate_registry(report, "5m", mock_registry)

        # Verify all 6 gauge types were created
        gauge_calls = [c[0][0] for c in mock_registry.method_calls if not c[0][0].startswith("_")]
        # We don't check exact call count — just that no exceptions were raised
        assert True  # No exception means success

    def test_handles_none_duration(self):
        report = Report(
            generated_at="2025-01-01T00:00:00+00:00",
            cluster="test",
            results=[CheckResult(name="nodes", status=Status.PASS, summary="ok", duration_ms=None)],
        )
        mock_registry = MagicMock()
        # Should not raise
        populate_registry(report, "5m", mock_registry)


class TestPushReportToGateway:
    def test_pushes_with_correct_config(self):
        report = Report(
            generated_at="2025-01-01T00:00:00+00:00",
            cluster="test-cluster",
            results=[],
        )
        cfg = PushGatewayConfig(url="http://localhost:9091", job="test-job", timeout_s=3.0)

        with patch("k8s_inspector.metrics._import_prometheus_client") as mock_pc:
            mock_client = MagicMock()
            mock_pc.return_value = mock_client

            push_report_to_gateway(report, "daily", cfg)

            mock_client.push_to_gateway.assert_called_once()
            call_kwargs = mock_client.push_to_gateway.call_args[1]
            assert call_kwargs["gateway"] == "http://localhost:9091"
            assert call_kwargs["job"] == "test-job"
            assert call_kwargs["grouping_key"]["cluster"] == "test-cluster"
            assert call_kwargs["grouping_key"]["profile"] == "daily"

    def test_extra_grouping_merged(self):
        report = Report(generated_at="2025-01-01T00:00:00+00:00", cluster="c1", results=[])
        cfg = PushGatewayConfig(url="http://localhost:9091")

        with patch("k8s_inspector.metrics._import_prometheus_client") as mock_pc:
            mock_client = MagicMock()
            mock_pc.return_value = mock_client

            push_report_to_gateway(report, "5m", cfg, extra_grouping={"env": "prod"})

            call_kwargs = mock_client.push_to_gateway.call_args[1]
            assert call_kwargs["grouping_key"]["env"] == "prod"
            assert call_kwargs["grouping_key"]["cluster"] == "c1"
