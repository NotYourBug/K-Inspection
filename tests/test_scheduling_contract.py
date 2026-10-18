from dataclasses import replace
from pathlib import Path
import pytest
import yaml
from k8s_inspector.core.profiles import PROFILES
from tests.deployment_helpers import render


@pytest.fixture(scope='module')
def jobs(): return [d for d in render('deploy') if d['kind']=='CronJob']


def test_job_deadline_matches_runner_catalog(jobs):
    observed={}
    for job in jobs:
        if 'cleanup' in job['metadata']['name']: continue
        c=job['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]
        profile=c['args'][c['args'].index('--profile')+1]
        observed[profile]=job['spec']['jobTemplate']['spec'].get('activeDeadlineSeconds')
    assert observed=={'5m':270,'10m':510,'hourly':930,'daily':1830}


def test_schedules_have_explicit_timezone_and_no_job_retries(jobs):
    for job in jobs:
        assert job['spec'].get('timeZone')=='Asia/Shanghai'
        assert job['spec']['concurrencyPolicy']=='Forbid'
        assert job['spec']['jobTemplate']['spec'].get('backoffLimit')==0


def test_cleanup_does_not_target_latest_or_bridge_state(jobs):
    cleanup=next(j for j in jobs if 'cleanup' in j['metadata']['name'])
    pod=cleanup['spec']['jobTemplate']['spec']['template']['spec']
    args=pod['containers'][0]['args']
    assert args[args.index('--root')+1]=='/data/out/archive'
    assert args[args.index('--hourly-days')+1]=='7' and args[args.index('--daily-days')+1]=='30'
    assert all(m['mountPath']=='/data/out' for m in pod['containers'][0]['volumeMounts'])
    assert pod['automountServiceAccountToken'] is False


def test_generator_uses_supplied_catalog_and_rejects_overlap(tmp_path):
    from tools.render_cronjobs import render_cronjobs
    path=render_cronjobs({'5m':replace(PROFILES['5m'],deadline_seconds=250)},tmp_path/'custom.yaml')
    docs=list(yaml.safe_load_all(path.read_text(encoding='utf-8')))
    assert len(docs)==1 and docs[0]['spec']['jobTemplate']['spec']['activeDeadlineSeconds']==280
    with pytest.raises(ValueError,match='interval'):
        render_cronjobs({'5m':replace(PROFILES['5m'],deadline_seconds=300)},tmp_path/'overlap.yaml')


def test_committed_jobs_match_catalog_generation(tmp_path):
    from tools.render_cronjobs import render_cronjobs
    output=render_cronjobs(PROFILES,tmp_path/'jobs.yaml')
    assert output.read_bytes()==Path('deploy/base/cronjobs-by-frequency.yaml').read_bytes()
    assert output.read_bytes()==Path('deploy/cronjobs-by-frequency.yaml').read_bytes()
