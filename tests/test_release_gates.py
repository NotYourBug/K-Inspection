"""Release gates must catch missing bootstrap tools and broken shipped configs."""
from dataclasses import replace
from pathlib import Path
import json
import shutil
import sys

import build
from packaging.requirements import Requirement
import pytest
import yaml

from tests.test_installed_package import installed_env


ROOT = Path(__file__).resolve().parents[1]


def test_dev_bootstrap_requests_every_nonisolated_build_backend():
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    config = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    requested = {Requirement(item).name.lower() for item in
                 config['project']['dependencies'] + config['project']['optional-dependencies']['dev']}
    required = {Requirement(item).name.lower() for item in build.ProjectBuilder(ROOT).build_system_requires}
    assert required <= requested, f'Non-isolated build tools missing from bootstrap: {required - requested}'


@pytest.fixture
def release_project(tmp_path, monkeypatch, installed_env):
    from tools import verify_release as release
    project = tmp_path / 'project'
    project.mkdir()
    for folder in ('deploy', 'constraints', 'tests/prometheus'):
        shutil.copytree(ROOT / folder, project / folder)
    for name in ('pyproject.toml', 'Dockerfile'):
        shutil.copyfile(ROOT / name, project / name)
    monkeypatch.setattr(release, 'install_package', lambda *a, **kw: installed_env[3])
    commands = {name: release._tool(ROOT, name, {}) for name in ('kustomize', 'promtool', 'amtool')}
    assert all(commands.values()), 'Actual validation tools are required'
    return project, commands


def validation_result(release_project, output):
    from tools import verify_release as release
    project, commands = release_project
    result = release.verify_release(project, python_executable=Path(sys.executable), output_dir=output,
                                    offline=True, run_source_tests=False, tool_commands=commands)
    # This fixture tests config gates, not source-suite execution; package evidence
    # is real and source suite is run independently by the outer pytest process.
    checks = dict(result.checks)
    checks['source_tests'] = release.CheckOutcome(True, 'Outer pytest process')
    return replace(result, checks=checks)


def test_local_ci_gate_rejects_invalid_composed_alertmanager(release_project, tmp_path):
    from tools import verify_release as release
    good = validation_result(release_project, tmp_path / 'good')
    assert release.local_checks_success(good), {k: v.detail for k, v in good.checks.items() if not v.success}
    project, _ = release_project
    path = project / 'deploy/overlays/shadow/am-shadow.yaml'
    config = yaml.safe_load(path.read_text(encoding='utf-8'))
    content = yaml.safe_load(config['data']['alertmanager.yml'])
    content['route']['group_wait'] = 'invalid-duration'
    config['data']['alertmanager.yml'] = yaml.safe_dump(content)
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    bad = validation_result(release_project, tmp_path / 'bad')
    assert not bad.checks['amtool'].success
    assert not release.local_checks_success(bad)


def test_bridge_temporal_fixture_failure_blocks_local_ci_gate(release_project, tmp_path):
    from tools import verify_release as release
    project, _ = release_project
    fixture = project / 'tests/prometheus/bridge-rules.test.yml'
    fixture.write_text('invalid: [', encoding='utf-8')
    result = validation_result(release_project, tmp_path / 'broken-bridge')
    assert not result.checks['promtool'].success, 'Bridge fixture must be executed, not omitted'
    assert not release.local_checks_success(result)
    assert (tmp_path / 'broken-bridge/bridge-rules-check.log').is_file()
    assert (tmp_path / 'broken-bridge/bridge-rules-test.log').is_file()


def test_local_exit_gate_keeps_release_pending_and_fails_missing_checks(tmp_path, monkeypatch):
    from tools import verify_release as release
    checks = {name: release.CheckOutcome(True, 'verified') for name in release.LOCAL_REQUIRED_CHECKS}
    checks['environment_acceptance'] = release.CheckOutcome(False, 'PENDING')
    result = release.VerificationResult(False, checks, ())
    monkeypatch.setattr(release, 'verify_release', lambda *a, **kw: result)
    assert release.main(['--offline', '--local-checks-only', '--output-dir', str(tmp_path)]) == 0
    evidence = json.loads((tmp_path / 'verification.json').read_text(encoding='utf-8'))
    assert evidence['success'] is False
    assert evidence['local_checks_success'] is True
    assert evidence['exit_gate'] == 'local'
    assert release.main(['--offline', '--output-dir', str(tmp_path)]) == 1
    del checks['amtool']
    assert release.main(['--offline', '--local-checks-only', '--output-dir', str(tmp_path)]) == 1
