"""Package boundaries keep domain code discoverable without optional services."""
import ast
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('name', [
    'core.files', 'core.config', 'inspection.runner', 'inspection.rules.nodes',
    'notifications.bridge.server', 'notifications.delivery.store',
    'notifications.providers.feishu', 'reporting.writers', 'monitoring.inventory',
])
def test_domain_modules_are_discoverable(name):
    try:
        spec = importlib.util.find_spec('k8s_inspector.' + name)
    except ModuleNotFoundError:
        spec = None
    assert spec is not None, f'Missing domain module: {name}'


def test_core_import_does_not_load_service_dependencies():
    code = '''
import sys
from k8s_inspector.core.config import InspectorConfig
from k8s_inspector.core.models import Report
from k8s_inspector.core.observations import Severity
assert InspectorConfig() and Severity.WARN.value == 'WARN'
assert not {'kubernetes','lark_oapi','fastmcp','prometheus_client'} & set(sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', code], cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_delivery_import_does_not_initialize_bridge_or_providers():
    code = '''
import sys
from k8s_inspector.notifications.delivery.store import DeliveryStore
assert 'k8s_inspector.notifications.bridge.server' not in sys.modules
assert not {'lark_oapi','fastmcp'} & set(sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', code], cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_shared_core_and_pure_rules_do_not_depend_on_adapters():
    packages = [ROOT / 'k8s_inspector/core', ROOT / 'k8s_inspector/inspection/rules']
    assert all(package.is_dir() for package in packages)
    forbidden = ('kubernetes', 'lark_oapi', 'fastmcp', 'prometheus_client',
                 'k8s_inspector.reporting', 'k8s_inspector.monitoring', 'k8s_inspector.notifications')
    for package in packages:
        for path in package.glob('*.py'):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                names = [node.module or ''] if isinstance(node, ast.ImportFrom) else (
                    [name.name for name in node.names] if isinstance(node, ast.Import) else [])
                assert not any(name == prefix or name.startswith(prefix + '.')
                               for name in names for prefix in forbidden), path
