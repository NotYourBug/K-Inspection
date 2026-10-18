"""Fail-closed release checks; real container smoke requires an explicit local image."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
from tests.test_installed_package import installed_env


def test_release_checks_fail_when_promtool_fails(tmp_path,monkeypatch,installed_env):
    from tools import verify_release as release
    monkeypatch.setattr(release,'install_package',lambda *a,**kw:installed_env[3])
    bad=tmp_path/'bad-promtool.py';bad.write_text('print("rejected-rule");raise SystemExit(7)',encoding='utf-8')
    result=release.verify_release(Path(__file__).resolve().parents[1],python_executable=Path(sys.executable),
        output_dir=tmp_path/'release',offline=True,run_source_tests=False,
        tool_commands={'promtool':[sys.executable,str(bad)]})
    assert result.success is False
    assert result.checks['promtool'].success is False
    assert '7' in result.checks['promtool'].detail


def test_missing_mandatory_tools_and_environment_never_mean_release_success(tmp_path,monkeypatch,installed_env):
    from tools import verify_release as release
    monkeypatch.setattr(release,'install_package',lambda *a,**kw:installed_env[3])
    result=release.verify_release(Path(__file__).resolve().parents[1],python_executable=Path(sys.executable),
        output_dir=tmp_path/'offline',offline=True,run_source_tests=False)
    assert not result.success
    assert result.checks['docker'].success is False
    assert result.checks['environment_acceptance'].success is False
    assert result.checks['clean_dependency_install'].success is False
    assert 'PENDING' in result.checks['docker'].detail


@pytest.mark.parametrize('module',['k8s_inspector.cli','k8s_inspector.alertmanager_bridge'])
def test_image_cli_and_bridge_entrypoints(module):
    image=os.getenv('INSPECTOR_TEST_IMAGE')
    if not image or not shutil.which('docker'):
        pytest.skip('PENDING: requires built local INSPECTOR_TEST_IMAGE and Docker')
    result=subprocess.run(['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL',
        '--security-opt','no-new-privileges','--entrypoint','python',image,'-m',module,'--help'],
        capture_output=True,text=True,timeout=150)
    assert result.returncode==0,result.stderr
    assert 'usage:' in result.stdout
