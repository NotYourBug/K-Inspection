"""Tests for k8s_inspector.core.resources."""

from unittest.mock import MagicMock

import pytest

from k8s_inspector.core.resources import (
    BAD_POD_REASONS,
    cpu_to_millicores,
    deep_get,
    format_bytes,
    format_cpu_m,
    memory_to_bytes,
    parse_quantity,
    pod_issue,
    ratio,
)


class TestParseQuantity:
    def test_empty_and_zero(self):
        assert parse_quantity("") == 0.0
        assert parse_quantity("0") == 0.0

    def test_cpu_units(self):
        assert parse_quantity("250m") == 0.25
        assert parse_quantity("1000m") == 1.0
        assert parse_quantity("1") == 1.0
        assert parse_quantity("500n") == pytest.approx(5e-7)

    def test_memory_units(self):
        assert parse_quantity("128Mi") == 128 * 1024 * 1024
        assert parse_quantity("1Gi") == 1024 ** 3
        assert parse_quantity("2Ki") == 2048

    def test_unknown_format_falls_through(self):
        # bare numbers are treated as decimal
        assert parse_quantity("500") == 500.0


class TestCpuToMillicores:
    def test_conversion(self):
        assert cpu_to_millicores("250m") == 250
        assert cpu_to_millicores("1") == 1000
        assert cpu_to_millicores("2.5") == 2500
        assert cpu_to_millicores("0") == 0


class TestMemoryToBytes:
    def test_conversion(self):
        assert memory_to_bytes("128Mi") == 128 * 1024 * 1024
        assert memory_to_bytes("1Gi") == 1024 ** 3
        assert memory_to_bytes("0") == 0


class TestFormatCpuM:
    def test_format(self):
        assert format_cpu_m(250) == "250m"
        assert format_cpu_m(1000) == "1000m"
        assert format_cpu_m(0) == "0m"


class TestFormatBytes:
    def test_format(self):
        assert format_bytes(0) == "0B"
        assert format_bytes(1023) == "1023B"
        assert format_bytes(1024) == "1.0Ki"
        assert format_bytes(1024 ** 2) == "1.0Mi"
        assert format_bytes(1024 ** 3) == "1.0Gi"


class TestRatio:
    def test_normal(self):
        assert ratio(50, 100) == 0.5
        assert ratio(0, 100) == 0.0
        assert ratio(100, 100) == 1.0

    def test_zero_denominator(self):
        assert ratio(100, 0) == 0.0
        assert ratio(0, 0) == 0.0


class TestDeepGet:
    def test_shallow(self):
        d = {"a": 1, "b": 2}
        assert deep_get(d, ["a"], None) == 1
        assert deep_get(d, ["c"], None) is None

    def test_nested(self):
        d = {"a": {"b": {"c": 42}}}
        assert deep_get(d, ["a", "b", "c"], None) == 42
        assert deep_get(d, ["a", "x"], None) is None
        assert deep_get(d, ["x", "y"], None) is None

    def test_default_value(self):
        assert deep_get({}, ["a"], 99) == 99


class TestPodIssue:
    def _cs(self, waiting_reason=None, terminated_reason=None, restart_count=0):
        cs = MagicMock()
        cs.name = "test-container"
        cs.restart_count = restart_count
        if waiting_reason:
            cs.state = MagicMock()
            cs.state.waiting = MagicMock()
            cs.state.waiting.reason = waiting_reason
            cs.last_state = None
        elif terminated_reason:
            cs.state = None
            cs.last_state = MagicMock()
            cs.last_state.terminated = MagicMock()
            cs.last_state.terminated.reason = terminated_reason
        else:
            cs.state = None
            cs.last_state = None
        return cs

    def test_phase_failed(self):
        assert pod_issue([], "Failed", 0) == "phase=Failed"
        assert pod_issue([], "Unknown", 0) == "phase=Unknown"

    def test_running_no_issue(self):
        cs = self._cs()
        assert pod_issue([cs], "Running", 0) is None

    def test_crash_loop_backoff(self):
        cs = self._cs(waiting_reason="CrashLoopBackOff")
        assert pod_issue([cs], "Running", 0) == "waiting=CrashLoopBackOff"

    def test_image_pull_backoff(self):
        cs = self._cs(waiting_reason="ImagePullBackOff")
        assert pod_issue([cs], "Running", 0) == "waiting=ImagePullBackOff"

    def test_terminated_oom_killed(self):
        cs = self._cs(terminated_reason="OOMKilled")
        # The current termination is a fault; last_state alone is historical.
        cs.state, cs.last_state = cs.last_state, None
        assert pod_issue([cs], "Running", 0) == "terminated=OOMKilled"

    def test_restart_threshold(self):
        cs = self._cs(restart_count=5)
        assert pod_issue([cs], "Running", 3) == "restart_count>=3"
        # Below threshold, no issue
        assert pod_issue([cs], "Running", 10) is None

    def test_waiting_takes_priority_over_restart(self):
        cs = self._cs(waiting_reason="CrashLoopBackOff", restart_count=5)
        assert pod_issue([cs], "Running", 1) == "waiting=CrashLoopBackOff"


class TestBadPodReasons:
    def test_includes_critical_reasons(self):
        assert "CrashLoopBackOff" in BAD_POD_REASONS
        assert "ImagePullBackOff" in BAD_POD_REASONS
        assert "ErrImagePull" in BAD_POD_REASONS
        assert "OOMKilled" in BAD_POD_REASONS
        assert "CreateContainerConfigError" in BAD_POD_REASONS
