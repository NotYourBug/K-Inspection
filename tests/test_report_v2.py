import csv
import json
from dataclasses import replace
from datetime import timedelta
import pytest
from k8s_inspector.models import Report
from k8s_inspector.observation_models import to_legacy_result
from k8s_inspector import report as output
from tests.observation_factories import NOW, failed_evaluation, error_evaluation


def report(evaluation=None, **kwargs):
    ev = evaluation or failed_evaluation()
    return Report(max((o.observed_at for o in ev.observations), default=NOW).isoformat(), 'prod', [to_legacy_result(ev)], schema_version=2,
        run_id='fb7e6d3a-fadf-4010-a98a-50a2b3be1f61', profile='5m', executions=(ev.execution,),
        findings=ev.findings, observations=tuple(replace(o, run_id='fb7e6d3a-fadf-4010-a98a-50a2b3be1f61') for o in ev.observations), **kwargs)


def test_v2_preserves_v1_root_fields_and_csv_finding_evidence(tmp_path):
    r = report()
    data = json.loads(output.write_json(r, tmp_path).read_text(encoding='utf-8'))
    assert {'generated_at', 'cluster', 'summary', 'results'} <= set(data)
    rows = list(csv.DictReader(output.write_csv(r, tmp_path).open(encoding='utf-8')))
    assert rows[0]['resource_uid'] == 'pod-uid-1'
    assert rows[0]['schema_version'] == '2' and rows[0]['execution_status'] == 'OK'


def test_html_escapes_untrusted_evidence_and_links_objects(tmp_path):
    ev = failed_evaluation()
    f = replace(ev.findings[0], summary='<script>alert(1)</script>', evidence={'message': '<script>'})
    text = output.write_html(report(replace(ev, findings=(f,))), tmp_path).read_text(encoding='utf-8')
    assert '<script>' not in text and '&lt;script&gt;' in text
    assert 'pod-uid-1' in text and 'href="#object-' in text


def test_unknown_is_visible_in_html(tmp_path):
    text = output.write_html(report(error_evaluation()), tmp_path).read_text(encoding='utf-8')
    assert 'UNKNOWN' in text and 'FORBIDDEN' in text and 'expected=unknown' in text


def test_snapshot_scope_mismatch_omits_details():
    r = report().to_dict()
    assert output.snapshot_is_compatible(r, cluster='prod', profile='5m', scope='cluster', now=NOW, max_age_seconds=900)
    for overrides in ({'scope': 'namespace:other'}, {'cluster': 'other'}, {'profile': 'hourly'},
                      {'now': NOW+timedelta(hours=1)}):
        args = dict(cluster='prod', profile='5m', scope='cluster', now=NOW, max_age_seconds=900)
        args.update(overrides)
        assert not output.snapshot_is_compatible(r, **args)


def test_manifest_failure_does_not_overwrite_report(tmp_path, monkeypatch):
    from k8s_inspector import sinks
    r = report()
    target = output.write_json(r, tmp_path)
    before = target.read_bytes()
    monkeypatch.setattr(sinks, 'atomic_write_text', lambda *a: (_ for _ in ()).throw(OSError('full')))
    with pytest.raises(OSError):
        sinks.write_run_manifest(r, (), tmp_path)
    assert target.read_bytes() == before


def test_same_second_archive_directory_contains_report_run_id(tmp_path):
    from k8s_inspector.cli import _resolve_output_dir
    a = _resolve_output_dir(str(tmp_path), 'hourly', NOW, run_id='run-one')
    b = _resolve_output_dir(str(tmp_path), 'hourly', NOW, run_id='run-two')
    assert a != b and 'run-one' in a and 'run-two' in b


def test_unknown_legacy_profile_is_rejected_before_api():
    from k8s_inspector.checks import run_all
    from k8s_inspector.config import InspectorConfig
    with pytest.raises(ValueError, match='profile'):
        run_all(None, None, None, None, InspectorConfig(), 'bogus')
