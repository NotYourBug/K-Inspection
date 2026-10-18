import json

import pytest
from prometheus_client import CollectorRegistry

from k8s_inspector import cli
from k8s_inspector.monitoring.metrics import populate_registry
from k8s_inspector.core.models import CheckResult, Report, Status
from tests.test_report_persistence import run_cli


def denied():
    raise OSError("cannot write; secret=https://private-token@example.invalid")


def test_report_failure_does_not_block_metrics_or_notification():
    from k8s_inspector.reporting.sinks import run_sinks
    calls = []
    results = run_sinks({"json": denied, "metrics": lambda: calls.append("metrics"),
                         "notification": lambda: calls.append("notification")},
                        required=frozenset({"json", "metrics"}))
    assert calls == ["metrics", "notification"]
    assert [(x.name, x.success) for x in results] == [
        ("json", False), ("metrics", True), ("notification", True)]
    assert "private-token" not in repr(results)


def test_cli_report_error_still_attempts_all_other_outputs(run_cli, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "write_json", lambda *a, **kw: denied())
    monkeypatch.setattr(cli, "push_report_to_gateway", lambda **kw: calls.append("metrics"))
    monkeypatch.setattr(cli, "send_feishu_text", lambda *a, **kw: calls.append("notification"))
    result = run_cli(status=Status.FAIL, output_dir=tmp_path, formats="json,csv,html",
                     extra=("--pushgateway-url", "http://example.invalid",
                            "--feishu-webhook", "https://example.invalid",
                            "--output-policy", "strict"))
    assert result == 1
    assert set(calls) == {"metrics", "notification"}
    assert (tmp_path / "report.csv").exists()
    assert (tmp_path / "report.html").exists()


def test_strict_required_sink_failure_returns_one(run_cli, tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "write_json", lambda *a, **kw: denied())
    assert run_cli(output_dir=tmp_path, extra=("--output-policy", "strict")) == 1


def test_always0_keeps_failed_sink_observable(run_cli, tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "write_json", lambda *a, **kw: denied())
    assert run_cli(output_dir=tmp_path, extra=("--output-policy", "strict",
                                             "--exit-mode", "always0")) == 0
    state = json.loads((tmp_path / "latest/5m/run-status.json").read_text())
    assert state["run_success"] is False
    assert any(x["name"] == "latest_json" and not x["success"] for x in state["sinks"])
    assert "private-token" not in json.dumps(state)


@pytest.mark.parametrize("status,fail_on,expected", [
    (Status.PASS, "fail", 0), (Status.WARN, "fail", 0),
    (Status.WARN, "warn", 1), (Status.FAIL, "fail", 2), (Status.FAIL, "warn", 2)])
def test_warn_fail_exit_codes_remain_compatible(run_cli, tmp_path, status, fail_on, expected):
    assert run_cli(status=status, output_dir=tmp_path, extra=("--fail-on", fail_on)) == expected


def test_failed_sink_is_exported_without_claiming_business_recovery():
    from k8s_inspector.reporting.sinks import SinkResult
    report = Report("2026-10-15T00:00:00Z", "prod",
                    [CheckResult("nodes", Status.FAIL, "Node not ready")])
    registry = CollectorRegistry()
    populate_registry(report, "5m", registry, sink_results=(
        SinkResult("latest_json", False, True, "OSError", "unavailable"),))
    assert registry.get_sample_value("k8s_inspector_run_success",
                                     {"cluster": "prod", "profile": "5m"}) == 0
    assert registry.get_sample_value("k8s_inspector_check_status",
             {"cluster": "prod", "profile": "5m", "check": "nodes"}) == 2


def test_business_fail_with_successful_collection_is_successful_execution():
    registry = CollectorRegistry()
    populate_registry(Report("2026-10-15T00:00:00Z", "prod",
                              [CheckResult("nodes", Status.FAIL, "not ready")]),
                       "5m", registry)
    assert registry.get_sample_value("k8s_inspector_run_success",
                                     {"cluster": "prod", "profile": "5m"}) == 1


def test_invalid_output_format_is_rejected_before_cluster_access(tmp_path, monkeypatch):
    def unexpected(**kwargs):
        pytest.fail("invalid format must not initialize Kubernetes")
    monkeypatch.setattr(cli, "load_kube", unexpected)
    assert cli.main(["run", "--formats", "jsno", "--output-dir", str(tmp_path)]) == 1
