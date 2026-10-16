"""Wheel tests run outside the source tree; offline dependency bootstrap is explicit."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import pytest


@pytest.fixture(scope='module')
def installed_env(tmp_path_factory):
    from tools.verify_release import install_package
    root=Path(__file__).resolve().parents[1]
    task=tmp_path_factory.mktemp('installed-package')
    source=task/'project';source.mkdir()
    shutil.copytree(root/'k8s_inspector',source/'k8s_inspector',ignore=shutil.ignore_patterns('__pycache__'))
    for name in ('pyproject.toml','README.md'):
        shutil.copyfile(root/name,source/name)
    # A real nested package catches fixed-list setuptools discovery regressions.
    nested=source/'k8s_inspector/_release_canary';nested.mkdir()
    (nested/'__init__.py').write_text('VALUE=17',encoding='utf-8')
    result=install_package(source,python_executable=Path(sys.executable),output_dir=task/'artifacts',offline=True)
    assert result.success,{k:v.detail for k,v in result.checks.items()}
    python=next(p for p in result.artifacts if p.name in ('python','python.exe'))
    empty=task/'empty';empty.mkdir()
    return source,python,empty,result


def run(installed_env,code):
    source,python,empty,_=installed_env
    return subprocess.run([str(python),'-I','-c',code],cwd=empty,capture_output=True,text=True,timeout=120)


def test_wheel_installs_and_imports_outside_source_tree(installed_env):
    result=run(installed_env,'import k8s_inspector,k8s_inspector.alert_routing;print(k8s_inspector.__file__)')
    assert result.returncode==0,result.stderr
    assert str(installed_env[0]) not in result.stdout
    assert 'site-packages' in result.stdout


def test_nested_packages_are_included_in_wheel(installed_env):
    result=run(installed_env,'from k8s_inspector._release_canary import VALUE;print(VALUE)')
    assert result.returncode==0 and result.stdout.strip()=='17',result.stderr


def test_cli_help_from_installed_wheel(installed_env):
    result=run(installed_env,"from k8s_inspector.cli import main;raise SystemExit(main(['--help']))")
    assert result.returncode==0 and 'cleanup' in result.stdout


def test_declared_runtime_imports(installed_env):
    result=run(installed_env,'import kubernetes,yaml,prometheus_client;import importlib.metadata as m;print(m.version("lark-oapi"))')
    assert result.returncode==0 and '1.7.5' in result.stdout,result.stderr


def test_optional_mcp_not_required_for_cli(installed_env):
    result=run(installed_env,"import sys;from k8s_inspector.cli import main;assert 'fastmcp' not in sys.modules;raise SystemExit(main(['run','--help']))")
    assert result.returncode==0 and '--profile' in result.stdout
