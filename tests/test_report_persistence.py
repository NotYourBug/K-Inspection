import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from k8s_inspector import cli, report as reports
from k8s_inspector.models import CheckResult, Status, Report
from tests.factories import NOW


@pytest.fixture
def run_cli(monkeypatch):
    monkeypatch.setattr(cli, "load_kube", lambda **kw: SimpleNamespace(
        core=None, storage=None, custom=None, api_client=None, display_name="test"))
    monkeypatch.setattr(cli, "_now_dt", lambda: NOW)
    for key in ("PUSHGATEWAY_URL", "FEISHU_WEBHOOK", "INSPECTOR_EXIT_MODE"):
        monkeypatch.delenv(key, raising=False)
    def run(status=Status.PASS, persist_on="all", output_dir=None,
            profile="5m", archive="none", formats="json", extra=()):
        monkeypatch.setattr(cli, "run_inspection", lambda *a, **kw: Report(
            NOW.isoformat(), 'test', [CheckResult("nodes", status, "test")],
            schema_version=2, run_id=__import__('uuid').uuid4().hex, profile=profile))
        return cli.main(["run", "--output-dir", str(output_dir), "--profile", profile,
                         "--persist-level", persist_on, "--archive", archive,
                         "--formats", formats, *extra])
    return run


def test_atomic_replace_failure_keeps_previous_report(tmp_path, monkeypatch):
    path = tmp_path / "report.json"
    path.write_text('{"status":"FAIL"}', encoding="utf-8")
    def failing_replace(*args):
        raise OSError("replace failed")
    monkeypatch.setattr(reports, "_replace_file", failing_replace)
    with pytest.raises(OSError):
        reports.atomic_write_text(path, '{"status":"PASS"}')
    assert json.loads(path.read_text())["status"] == "FAIL"
    assert list(tmp_path.iterdir()) == [path]


def test_issue_only_run_refreshes_latest_on_recovery(run_cli, tmp_path):
    run_cli(status=Status.FAIL, persist_on="issue", output_dir=tmp_path)
    run_cli(status=Status.PASS, persist_on="issue", output_dir=tmp_path)
    path = tmp_path / "latest/5m/report.json"
    assert json.loads(path.read_text())["summary"]["fail"] == 0


def test_canonical_latest_path_is_not_nested_twice(run_cli, tmp_path):
    output = tmp_path / "latest/5m"
    assert run_cli(output_dir=output, persist_on="none") == 0
    assert (output / "report.json").exists()
    assert not (output / "latest").exists()


def test_latest_json_is_written_even_for_html_only(run_cli, tmp_path):
    run_cli(output_dir=tmp_path, formats="html")
    assert (tmp_path / "latest/5m/report.json").exists()
    assert (tmp_path / "report.html").exists()


def test_canonical_latest_keeps_requested_html_in_same_directory(run_cli, tmp_path):
    output = tmp_path / "latest/5m"
    run_cli(output_dir=output, formats="json,html")
    assert (output / "report.html").exists()
    assert not (tmp_path / "archive/none").exists()


def test_same_second_archives_do_not_overwrite(run_cli, tmp_path):
    for _ in range(2):
        run_cli(output_dir=tmp_path, profile="hourly", archive="hourly")
    files = list((tmp_path / "archive/hourly").rglob("report.json"))
    assert len(files) == 2
    assert len({p.parent for p in files}) == 2


def test_legacy_archive_path_updates_canonical_latest(run_cli, tmp_path):
    run_cli(output_dir=tmp_path / "archive/hourly", profile="hourly", archive="hourly")
    assert (tmp_path / "latest/hourly/report.json").exists()


def expired_archive(tmp_path):
    root = tmp_path / "archive/hourly"
    bucket = root / "2020010100"
    bucket.mkdir(parents=True)
    (bucket / "report.json").write_text("{}")
    return root, bucket


def test_cleanup_failure_is_not_counted(tmp_path, monkeypatch):
    root, bucket = expired_archive(tmp_path)
    def denied(*a, **kw):
        raise PermissionError("denied")
    monkeypatch.setattr(cli.shutil, "rmtree", denied)
    errors = []
    assert cli._cleanup_subdirs(root, 7, "%Y%m%d%H", error_sink=errors) == 0
    assert errors
    assert bucket.exists()


def test_cleanup_dry_run_changes_nothing(tmp_path):
    root, bucket = expired_archive(tmp_path)
    assert cli._cleanup_subdirs(root, 7, "%Y%m%d%H", dry_run=True) == 0
    assert (bucket / "report.json").exists()


def test_cleanup_reparse_target_is_rejected(tmp_path, monkeypatch):
    root, bucket = expired_archive(tmp_path)
    real_lstat = Path.lstat
    def reparse_stat(path, *args, **kwargs):
        if path == bucket:
            return SimpleNamespace(st_file_attributes=0x400, st_mode=0o40755)
        return real_lstat(path, *args, **kwargs)
    monkeypatch.setattr(Path, "lstat", reparse_stat)
    errors = []
    assert cli._cleanup_subdirs(root, 7, "%Y%m%d%H", error_sink=errors) == 0
    assert errors
    assert (bucket / "report.json").exists()


def test_cleanup_failed_deletion_returns_nonzero(tmp_path, monkeypatch):
    root, _ = expired_archive(tmp_path)
    def denied(*a, **kw):
        raise PermissionError("denied")
    monkeypatch.setattr(cli.shutil, "rmtree", denied)
    assert cli.main(["cleanup", "--root", str(root.parent)]) == 1
