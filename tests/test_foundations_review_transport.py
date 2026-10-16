"""Review regressions at SDK/HTTP, deadline and cluster identity boundaries."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace as NS
import json
import socket
import pytest
from k8s_inspector.collector import KubeCollector
from k8s_inspector.kube import load_kube
from k8s_inspector.config import InspectorConfig
from k8s_inspector.observation_models import ExecutionStatus as E, Severity
from tests.test_rule_observations import NOW
from tests.test_collector import page


def kubeconfig(path, server):
    path.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'shared',
        'contexts': [{'name': 'shared', 'context': {'cluster': 'shared', 'user': 'anonymous'}}],
        'clusters': [{'name': 'shared', 'cluster': {'server': server}}],
        'users': [{'name': 'anonymous', 'user': {}}]}), encoding='utf-8')
    return str(path)


@contextmanager
def endpoint(*, drop=False, status=200):
    attempts = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            attempts.append(self.path)
            if drop:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(status)
            self.end_headers()
            self.wfile.write(b'ok' if status == 200 else b'probe failed')
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', attempts
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()


def test_r1_physical_http_attempts_share_collector_retry_budget(tmp_path):
    with endpoint(drop=True) as (url, attempts):
        ctx = load_kube(kubeconfig(tmp_path/'config', url))
        try:
            result = KubeCollector(ctx, 30, lambda: 0).collect_list('nodes', ctx.core.list_node)
            assert result.execution_status == E.ERROR
            assert len(attempts) == 3
        finally:
            ctx.api_client.close()


def test_r2_final_page_after_deadline_preserves_items_without_complete_coverage():
    tick = [0]
    def late(**kwargs):
        tick[0] = 2
        return page(['observed-fault'])
    result = KubeCollector(NS(), 1, lambda: tick[0]).collect_list('nodes', late)
    assert result.items == ('observed-fault',)
    assert not result.complete and result.execution_status == E.PARTIAL
    assert result.error_kind == 'DEADLINE'


def test_r11_default_identity_binds_actual_selected_cluster(tmp_path):
    a = load_kube(kubeconfig(tmp_path/'a', 'https://cluster-a.invalid'))
    b = load_kube(kubeconfig(tmp_path/'b', 'https://cluster-b.invalid'))
    again = load_kube(str(tmp_path/'a'))
    try:
        identity = lambda ctx: getattr(ctx, 'cluster_identity', None) or ctx.display_name
        assert identity(a) != identity(b)
        assert identity(a) == identity(again)
        assert 'cluster-a.invalid' not in identity(a)  # identity never exposes connection URL/auth
    finally:
        for ctx in (a,b,again):
            ctx.api_client.close()


def test_r12_sdk_health_http500_is_confirmed_api_failure(tmp_path):
    from k8s_inspector.rule_catalog import CATALOG
    with endpoint(status=500) as (url, attempts):
        ctx = load_kube(kubeconfig(tmp_path/'config', url))
        try:
            snapshot = KubeCollector(ctx, 30, lambda: 0).snapshot(frozenset({'api_health'}), 'run', 'prod')
            result = next(r for r in CATALOG if r.rule_id == 'control.api').evaluate(snapshot, InspectorConfig())
            assert any(f.reason == 'APIUnhealthy' and f.severity == Severity.FAIL for f in result.findings)
            assert result.execution.coverage.complete
            assert len(attempts) == 1
        finally:
            ctx.api_client.close()


def test_r2_late_rule_cannot_claim_complete_healthy_coverage(monkeypatch):
    from k8s_inspector import runner
    from k8s_inspector.rule_catalog import RuleSpec
    from k8s_inspector.rules_nodes import evaluate_nodes
    from tests.test_runner import context
    tick=[0]
    def late(s,c):
        result=evaluate_nodes(s,c)[0]
        tick[0]=10000
        return result
    monkeypatch.setattr(runner,'CATALOG',(RuleSpec('nodes.ready',frozenset({'5m'}),'5m',frozenset({'nodes'}),late),))
    r=runner.run_inspection(context(),InspectorConfig(),'5m',cluster_id='prod',now=NOW,monotonic=lambda:tick[0])
    assert r.findings[0].reason=='NodeNotReady'
    assert not r.executions[0].coverage.complete
    assert r.executions[0].error_kind=='DEADLINE'


def test_r11_in_cluster_requires_stable_identity_before_loading_credentials(tmp_path,monkeypatch):
    from k8s_inspector import cli
    for key in ('INSPECTOR_CLUSTER','INSPECTOR_PROFILE'):
        monkeypatch.delenv(key,raising=False)
    monkeypatch.setattr(cli,'load_kube',lambda **kw:pytest.fail('generic in-cluster identity reached credentials'))
    assert cli.main(['run','--in-cluster','--profile','5m','--output-dir',str(tmp_path)])==1


def test_r12_sdk_health_http200_is_valid_healthy_probe(tmp_path):
    with endpoint(status=200) as (url,attempts):
        ctx=load_kube(kubeconfig(tmp_path/'config',url))
        try:
            snap=KubeCollector(ctx,30,lambda:0).snapshot(frozenset({'api_health'}),'run','prod')
            assert snap.collections['api_health'].complete
            assert snap.collections['api_health'].items[0]['ok'] is True
            assert len(attempts)==1
        finally:
            ctx.api_client.close()
