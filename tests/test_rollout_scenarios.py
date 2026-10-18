"""Shadow isolation and rollback gate tests do not deploy or contact providers."""
from pathlib import Path
import pytest
from tests.deployment_helpers import render,find


def test_shadow_uses_separate_outputs_jobs_and_destination_secret():
    production=render('deploy/overlays/example')
    shadow=render('deploy/overlays/shadow')
    assert {d['metadata'].get('namespace') for d in shadow if d['kind']=='CronJob'}=={'k8s-inspector-shadow'}
    def configuration(docs):
        return next(d['data'] for d in docs if d['kind']=='ConfigMap' and 'PUSHGATEWAY_JOB' in d.get('data',{}))
    assert configuration(production)['PUSHGATEWAY_JOB'] != configuration(shadow)['PUSHGATEWAY_JOB']
    for job in [d for d in shadow if d['kind']=='CronJob']:
        container=job['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]
        if 'cleanup' in job['metadata']['name']:
            assert container['args'][container['args'].index('--root')+1]=='/data/shadow/archive'
        else:
            assert container['args'][container['args'].index('--output-dir')+1]=='/data/shadow'
        assert container['volumeMounts'][0]['mountPath']=='/data/shadow'
        assert job['spec']['suspend'] is True
    bridge=find(shadow,'Deployment','k8s-inspector-alert-bridge')['spec']['template']['spec']['containers'][0]
    chats=[e['valueFrom']['secretKeyRef']['name'] for e in bridge['env'] if e['name']=='FEISHU_CHAT_ID']
    assert chats==['k8s-inspector-shadow-feishu']
    assert not any(d['kind']=='Secret' for d in shadow)


def test_shadow_keeps_control_plane_namespace_rbac_but_binds_shadow_service_account():
    docs=render('deploy/overlays/shadow')
    role=next(d for d in docs if d['kind']=='Role')
    binding=next(d for d in docs if d['kind']=='RoleBinding')
    assert role['metadata']['namespace']=='kube-system'
    assert binding['metadata']['namespace']=='kube-system'
    assert role['metadata']['name'].startswith('shadow-')
    assert binding['roleRef']['name']==role['metadata']['name']
    assert binding['subjects'][0]['namespace']=='k8s-inspector-shadow'


def test_shadow_routes_discard_new_rules_and_keep_auth_on_legacy_delivery():
    import yaml
    docs=render('deploy/overlays/shadow')
    config=find(docs,'ConfigMap','k8s-inspector-alertmanager-config')
    am=yaml.safe_load(config['data']['alertmanager.yml'])
    assert am['route']['routes'][0]['receiver']=='shadow-null'
    assert am['route']['routes'][0]['matchers']==['shadow="true"']
    receiver=next(r for r in am['receivers'] if r['name']=='feishu-bridge')
    assert receiver['webhook_configs'][0]['http_config']['authorization']['credentials_file']=='/etc/bridge-auth/token'
    rules=find(docs,'ConfigMap','k8s-inspector-prometheus-rules')['data']
    assert 'inspection-v2-shadow.yaml' in rules


def test_rollback_guard_refuses_sync_or_older_schema_with_unfinished_outbox():
    from tools.rollout import validate_rollback
    with pytest.raises(ValueError,match='unfinished'):
        validate_rollback(current_schema=3,target_schema=3,delivery_mode='sync',unfinished_jobs=1)
    with pytest.raises(ValueError,match='schema'):
        validate_rollback(current_schema=3,target_schema=2,delivery_mode='async',unfinished_jobs=0)
    assert validate_rollback(current_schema=3,target_schema=3,delivery_mode='async',unfinished_jobs=4)['preserve_outbox'] is True
