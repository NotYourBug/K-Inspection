"""Exercise real CLI -> Kubernetes SDK -> local HTTP -> Pushgateway boundary."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit
import csv
import json
from kubernetes.client import ApiClient
from prometheus_client.parser import text_string_to_metric_families
from tests.test_foundations_review_rules import healthy_snapshot
from tests.test_foundations_review_transport import kubeconfig
from tests.factories import pvc


def test_r3_real_cli_preserves_shipped_legacy_metrics_and_report_groups(tmp_path,monkeypatch):
    from k8s_inspector import cli
    for key in ('INSPECTOR_CLUSTER','INSPECTOR_PROFILE','INSPECTOR_STATE_DIR','PUSHGATEWAY_URL','FEISHU_WEBHOOK'):
        monkeypatch.delenv(key,raising=False)
    snap=healthy_snapshot()
    with ApiClient() as serializer:
        objects={
            '/api/v1/nodes':serializer.sanitize_for_serialization(snap.collections['nodes'].items),
            '/api/v1/pods':serializer.sanitize_for_serialization(snap.collections['pods'].items),
            '/api/v1/persistentvolumeclaims':serializer.sanitize_for_serialization([pvc('pending',phase='Pending',storage_class='')]),
            '/apis/storage.k8s.io/v1/storageclasses':[],
            '/apis/metrics.k8s.io/v1beta1/nodes':list(snap.collections['node_metrics'].items)}
    pushed=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path=urlsplit(self.path).path
            if path=='/healthz':
                body=b'ok'
            elif path in objects:
                body=json.dumps({'metadata':{'resourceVersion':'1'},'items':objects[path]}).encode()
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def do_PUT(self):
            pushed.append(self.rfile.read(int(self.headers['Content-Length'])).decode())
            self.send_response(200)
            self.end_headers()
        def log_message(self,*args):
            pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        url=f'http://127.0.0.1:{server.server_port}'
        config=kubeconfig(tmp_path/'config',url)
        result=cli.main(['run','--kubeconfig',config,'--cluster','prod','--profile','hourly',
            '--output-dir',str(tmp_path/'out'),'--formats','json,csv,html','--pushgateway-url',url])
        assert result==2
        assert len(pushed)==1
        samples=[s for metric in text_string_to_metric_families(pushed[0]) for s in metric.samples]
        def value(name,labels):
            return next((s.value for s in samples if s.name==name and s.labels==labels),None)
        assert value('k8s_inspector_check_issue_count',
            {'cluster':'prod','profile':'hourly','check':'pvcs','issue':'pending'})==1
        assert value('k8s_inspector_node_utilization',
            {'cluster':'prod','profile':'hourly','node':'n1','resource':'cpu'})==0.25
        assert value('k8s_inspector_node_utilization',
            {'cluster':'prod','profile':'hourly','node':'n1','resource':'memory'})==0.25
        data=json.loads((tmp_path/'out/latest/hourly/report.json').read_text(encoding='utf-8'))
        assert data['summary']['total']==4
        assert {r['name'] for r in data['results']}=={'nodes','pods','pvcs','control-plane'}
        rows=list(csv.DictReader((tmp_path/'out/report.csv').open(encoding='utf-8')))
        assert any(r['rule_id']=='pvc.binding' and r['resource_uid']=='pvc-pending' for r in rows)
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()
