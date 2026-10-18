"""Tests for k8s_inspector.models."""

import pytest

from k8s_inspector.models import CheckResult, Report, Status


class TestStatus:
    def test_enum_values(self):
        assert Status.PASS.value == "PASS"
        assert Status.WARN.value == "WARN"
        assert Status.FAIL.value == "FAIL"
        assert Status.SKIP.value == "SKIP"

    def test_is_string_enum(self):
        assert isinstance(Status.PASS, str)


class TestCheckResult:
    def test_basic_construction(self):
        r = CheckResult(name="nodes", status=Status.PASS, summary="all good")
        assert r.name == "nodes"
        assert r.status == Status.PASS
        assert r.summary == "all good"
        assert r.data == {}
        assert r.duration_ms is None

    def test_with_data_and_duration(self):
        r = CheckResult(
            name="pods",
            status=Status.FAIL,
            summary="bad pods found",
            data={"bad": [{"namespace": "ns", "name": "p"}]},
            duration_ms=123,
        )
        assert r.data["bad"] == [{"namespace": "ns", "name": "p"}]
        assert r.duration_ms == 123

    def test_to_dict(self):
        r = CheckResult(name="events", status=Status.WARN, summary="many events",
                        data={"count": 100}, duration_ms=50)
        d = r.to_dict()
        assert d["name"] == "events"
        assert d["status"] == "WARN"
        assert d["summary"] == "many events"
        assert d["data"]["count"] == 100
        assert d["duration_ms"] == 50


class TestReport:
    def test_empty_results(self):
        report = Report(generated_at="2025-01-01T00:00:00", cluster="test", results=[])
        assert not report.has_fail
        assert not report.has_warn

    def test_has_fail(self):
        report = Report(
            generated_at="2025-01-01T00:00:00",
            cluster="test",
            results=[CheckResult(name="nodes", status=Status.FAIL, summary="bad")],
        )
        assert report.has_fail
        assert not report.has_warn

    def test_has_warn(self):
        report = Report(
            generated_at="2025-01-01T00:00:00",
            cluster="test",
            results=[CheckResult(name="pods", status=Status.WARN, summary="elevated")],
        )
        assert not report.has_fail
        assert report.has_warn

    def test_mixed_results(self):
        report = Report(
            generated_at="2025-01-01T00:00:00",
            cluster="test",
            results=[
                CheckResult(name="nodes", status=Status.PASS, summary="ok"),
                CheckResult(name="pods", status=Status.FAIL, summary="bad"),
                CheckResult(name="events", status=Status.WARN, summary="many"),
                CheckResult(name="addons", status=Status.SKIP, summary="n/a"),
            ],
        )
        assert report.has_fail
        assert report.has_warn

    def test_to_dict_summary(self):
        report = Report(
            generated_at="2025-01-01T00:00:00",
            cluster="prod",
            results=[
                CheckResult(name="a", status=Status.PASS, summary="ok"),
                CheckResult(name="b", status=Status.PASS, summary="ok"),
                CheckResult(name="c", status=Status.WARN, summary="w"),
                CheckResult(name="d", status=Status.FAIL, summary="f"),
                CheckResult(name="e", status=Status.SKIP, summary="s"),
            ],
        )
        d = report.to_dict()
        assert d["generated_at"] == "2025-01-01T00:00:00"
        assert d["cluster"] == "prod"
        assert d["summary"]["total"] == 5
        assert d["summary"]["pass"] == 2
        assert d["summary"]["warn"] == 1
        assert d["summary"]["fail"] == 1
        assert d["summary"]["skip"] == 1
        assert len(d["results"]) == 5
