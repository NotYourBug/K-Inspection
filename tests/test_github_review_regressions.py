"""Regressions found by the repository's first integration PR review."""
from types import SimpleNamespace as NS
from unittest.mock import Mock
import ctypes
import os
import stat

import pytest

from k8s_inspector.collector import KubeCollector
from k8s_inspector.config import InspectorConfig
from k8s_inspector.observation_models import ObservationHealth as H, Severity
from k8s_inspector.runner import run_inspection
from tests.observation_factories import NOW
from tests.test_collector import page


def timed_context(seconds, status=200, body='ok'):
    tick = [0.0]

    def health(*args, **kwargs):
        tick[0] += seconds
        return body, status, {}

    return NS(api_client=NS(call_api=Mock(side_effect=health)),
              core=NS(list_namespaced_pod=Mock(return_value=page()))), lambda: tick[0]


def test_health_snapshot_retains_request_latency():
    context, clock = timed_context(0.6)
    result = KubeCollector(context, 30, clock).snapshot(frozenset({'api_health'}), 'run', 'prod')
    assert result.collections['api_health'].items[0]['latency_ms'] == pytest.approx(600)


@pytest.mark.parametrize('seconds,slow', [(0.1, False), (0.499, False), (0.5, True), (0.6, True)])
def test_canonical_runner_preserves_slow_api_warning_without_marking_endpoint_failed(seconds, slow):
    context, clock = timed_context(seconds)
    report = run_inspection(context, InspectorConfig(), '10m', cluster_id='prod', now=NOW, monotonic=clock)
    api_findings = [f for f in report.findings if f.rule_id == 'control.api']
    assert bool(api_findings) == slow
    if slow:
        assert [(f.severity, f.reason) for f in api_findings] == [(Severity.WARN, 'APISlow')]
        assert api_findings[0].evidence['latency_ms'] == pytest.approx(seconds * 1000)
    observation = next(o for o in report.observations if o.rule_id == 'control.api')
    assert observation.complete and observation.health == (H.UNHEALTHY if slow else H.HEALTHY)
    legacy = next(r for r in report.results if r.name == 'control-plane')
    assert legacy.data['apiserver_healthz']['ok'] is True
    assert legacy.data['_issue_counts']['healthz_not_ok'] == 0
    if slow:
        assert legacy.data['apiserver_healthz']['latency_ms'] == pytest.approx(seconds * 1000)


def test_slow_unhealthy_api_retains_confirmed_failure():
    context, clock = timed_context(0.6, status=503, body='failed')
    report = run_inspection(context, InspectorConfig(), '10m', cluster_id='prod', now=NOW, monotonic=clock)
    api_findings = [f for f in report.findings if f.rule_id == 'control.api']
    assert [(f.severity, f.reason) for f in api_findings] == [(Severity.FAIL, 'APIUnhealthy')]
    assert api_findings[0].evidence['latency_ms'] == pytest.approx(600)
    legacy = next(r for r in report.results if r.name == 'control-plane')
    assert legacy.data['apiserver_healthz']['ok'] is False
    assert legacy.data['_issue_counts']['healthz_not_ok'] == 1


def test_posix_publication_syncs_file_then_replace_then_parent(tmp_path, monkeypatch):
    from k8s_inspector import report as reports
    path = tmp_path / 'state.json'
    events = []
    real_fsync, real_replace = os.fsync, os.replace

    def sync_file(fd):
        events.append('file')
        real_fsync(fd)

    def replace(source, destination):
        events.append('replace')
        real_replace(source, destination)

    def sync_directory(directory):
        assert directory == path.parent
        assert path.read_text(encoding='utf-8') == 'new fault'
        events.append('directory')

    monkeypatch.setattr(reports, '_WINDOWS', False, raising=False)
    monkeypatch.setattr(os, 'fsync', sync_file)
    monkeypatch.setattr(os, 'replace', replace)
    monkeypatch.setattr(reports, '_sync_directory', sync_directory, raising=False)
    reports.atomic_write_text(path, 'new fault')
    assert events == ['file', 'replace', 'directory']


def test_new_state_directory_entries_are_synced(tmp_path, monkeypatch):
    from k8s_inspector import report as reports
    from k8s_inspector.observation_state import profile_run_lock
    synced = []
    monkeypatch.setattr(reports, '_WINDOWS', False, raising=False)
    monkeypatch.setattr(reports, '_sync_directory', synced.append, raising=False)
    directory = tmp_path / 'new' / 'profile'
    with profile_run_lock(directory):
        reports.atomic_write_text(directory / 'state.json', 'fault')
    assert synced == [tmp_path, tmp_path / 'new', directory]


def test_directory_sync_failure_is_required_state_sink_failure(tmp_path, monkeypatch):
    from k8s_inspector import report as reports
    from k8s_inspector.observation_state import ObservationStateStore
    from k8s_inspector.sinks import run_sinks, execution_success
    from tests.test_report_v2 import report

    def fail(directory):
        raise OSError('directory sync failed')

    monkeypatch.setattr(reports, '_WINDOWS', False, raising=False)
    monkeypatch.setattr(reports, '_sync_directory', fail, raising=False)
    store = ObservationStateStore(tmp_path / 'state.json')
    value = report()
    outcomes = run_sinks({'observation_state': lambda: store.update(value)}, frozenset({'observation_state'}))
    assert not outcomes[0].success and outcomes[0].error_kind == 'OSError'
    assert not execution_success(value, outcomes)
    assert not store.marker.exists()


@pytest.mark.parametrize('succeeds', [True, False])
def test_windows_replacement_requests_write_through_and_propagates_errors(tmp_path, monkeypatch, succeeds):
    from k8s_inspector import report as reports
    path = tmp_path / 'state.json'
    path.write_text('old', encoding='utf-8')
    calls = []

    def move(source, destination, flags):
        calls.append(flags)
        if succeeds:
            os.replace(source, destination)
        return int(succeeds)

    monkeypatch.setattr(reports, '_WINDOWS', True, raising=False)
    monkeypatch.setattr(ctypes, 'WinDLL', lambda *a, **kw: NS(MoveFileExW=move), raising=False)
    monkeypatch.setattr(ctypes, 'get_last_error', lambda: 5, raising=False)
    monkeypatch.setattr(ctypes, 'WinError', lambda code: OSError(code, 'move failed'), raising=False)
    if succeeds:
        reports.atomic_write_text(path, 'new')
    else:
        with pytest.raises(OSError):
            reports.atomic_write_text(path, 'new')
    assert calls == [0x1 | 0x8]
    assert path.read_text(encoding='utf-8') == ('new' if succeeds else 'old')
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.skipif(os.name == 'nt', reason='POSIX directory descriptors are validated by Linux CI')
def test_native_posix_publication_fsyncs_directory_descriptor(tmp_path, monkeypatch):
    from k8s_inspector import report as reports
    synced = []
    real_fsync = os.fsync

    def fsync(fd):
        synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
        real_fsync(fd)

    monkeypatch.setattr(os, 'fsync', fsync)
    reports.atomic_write_text(tmp_path / 'state.json', 'fault')
    assert synced == [False, True]
