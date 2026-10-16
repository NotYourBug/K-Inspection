"""Contracts consume actual Kustomize output, including optional compositions."""
import pytest
from tests.deployment_helpers import render,find


@pytest.fixture(scope='module')
def base(): return render('deploy')


@pytest.fixture(scope='module')
def example(): return render('deploy/overlays/example')


def test_base_does_not_install_optional_monitoring(base):
    assert not [d for d in base if d['kind'] in ('Deployment','Service','PersistentVolume')]
    assert not any(d.get('spec',{}).get('nfs') for d in base)
    assert len([d for d in base if d['kind']=='CronJob'])==5


def test_inspector_role_has_no_unneeded_or_mutating_verbs(base):
    for d in base:
        if d['kind'] in ('Role','ClusterRole'):
            assert all(set(r['verbs'])<={'get','list'} for r in d['rules'])
            assert all('pods/exec' not in r.get('resources',[]) for r in d['rules'])


def test_inspector_profiles_have_separate_block_state_claims(base):
    claims=set()
    for job in [d for d in base if d['kind']=='CronJob' and 'cleanup' not in d['metadata']['name']]:
        pod=job['spec']['jobTemplate']['spec']['template']['spec']
        volume=next(v for v in pod['volumes'] if v['name']=='state')
        claims.add(volume['persistentVolumeClaim']['claimName'])
    assert len(claims)==4
    for name in claims:
        assert find(base,'PersistentVolumeClaim',name)['spec']['accessModes']==['ReadWriteOnce']


def test_async_bridge_has_persistent_single_writer(example):
    bridge=find(example,'Deployment','k8s-inspector-alert-bridge')
    assert bridge['spec']['replicas']==1 and bridge['spec']['strategy']['type']=='Recreate'
    pod=bridge['spec']['template']['spec']
    container=pod['containers'][0]
    mount=next(m for m in container['volumeMounts'] if m['mountPath']=='/var/lib/inspector')
    volume=next(v for v in pod['volumes'] if v['name']==mount['name'])
    assert find(example,'PersistentVolumeClaim',volume['persistentVolumeClaim']['claimName'])['spec']['accessModes']==['ReadWriteOnce']
    assert all(k in container for k in ('startupProbe','readinessProbe','livenessProbe','resources'))
    assert pod['securityContext']['runAsNonRoot'] is True


def test_bridge_disables_service_account_automount(example):
    pod=find(example,'Deployment','k8s-inspector-alert-bridge')['spec']['template']['spec']
    assert pod['automountServiceAccountToken'] is False
    assert pod['containers'][0]['securityContext']['allowPrivilegeEscalation'] is False


def test_secrets_not_embedded_in_configmap(example):
    assert not [d for d in example if d['kind']=='Secret']
    bridge=find(example,'Deployment','k8s-inspector-alert-bridge')['spec']['template']['spec']['containers'][0]
    for env in bridge['env']:
        if env['name'] in ('FEISHU_APP_SECRET','FEISHU_APP_ID','FEISHU_CHAT_ID'):
            assert 'secretKeyRef' in env['valueFrom']


def test_example_is_suspended_and_has_ingress_restriction(example):
    assert all(d['spec']['suspend'] is True for d in example if d['kind']=='CronJob')
    policy=find(example,'NetworkPolicy','k8s-inspector-alert-bridge')
    assert policy['spec']['policyTypes']==['Ingress']
    assert policy['spec']['ingress']
