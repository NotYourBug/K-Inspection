"""Generate scheduled Jobs from the finite runner catalog; defaults stay suspended."""
from pathlib import Path
from copy import deepcopy
import argparse
import yaml
from k8s_inspector.profiles import PROFILES

NAMES={'5m':'dataplane-5m','10m':'controlplane-10m','hourly':'infra-hourly','daily':'daily'}


def _interval(schedule):
    parts=schedule.split()
    if len(parts)!=5: raise ValueError('unsupported schedule interval')
    if parts[0].startswith('*/') and parts[1:]==['*']*4:
        value=int(parts[0][2:])
        if 1<=value<=59: return value*60
    if parts==['0','*','*','*','*']: return 3600
    if parts[:2]==['0','2'] and parts[2:]==['*']*3: return 86400
    raise ValueError('unsupported schedule interval')


def render_cronjobs(profiles, output: Path) -> Path:
    template=yaml.safe_load((Path(__file__).parent/'templates/inspector-cronjob.yaml').read_text(encoding='utf-8'))
    jobs=[]
    for name,profile in profiles.items():
        if profile.schedule is None: continue
        if name!=profile.name or name not in NAMES: raise ValueError('unknown scheduled profile')
        if type(profile.deadline_seconds) is not int or profile.deadline_seconds<=0:
            raise ValueError('invalid deadline')
        if profile.deadline_seconds+30>=_interval(profile.schedule):
            raise ValueError('deadline plus shutdown margin must fit schedule interval')
        job=deepcopy(template);job['metadata']['name']='k8s-inspector-'+NAMES[name]
        spec=job['spec'];spec.update(schedule=profile.schedule,timeZone='Asia/Shanghai',concurrencyPolicy='Forbid',suspend=True,
            startingDeadlineSeconds=60,successfulJobsHistoryLimit=2,failedJobsHistoryLimit=2)
        body=spec['jobTemplate']['spec'];body.update(activeDeadlineSeconds=profile.deadline_seconds+30,backoffLimit=0,ttlSecondsAfterFinished=86400)
        pod=body['template']['spec'];pod['terminationGracePeriodSeconds']=30
        container=pod['containers'][0]
        container['args']=['run','--in-cluster','--profile',name,'--formats','json,html' if name=='daily' else 'json',
            '--output-dir','/data/out/archive/'+name if name in ('hourly','daily') else '/data/out/latest/'+name]
        container['args']+=['--archive',name] if name in ('hourly','daily') else ['--persist-level','issue']
        for volume in pod['volumes']:
            if volume['name']=='state': volume['persistentVolumeClaim']['claimName']='k8s-inspector-state-'+name
        jobs.append(job)
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('w',encoding='utf-8',newline='') as handle: yaml.safe_dump_all(jobs,handle,sort_keys=False)
    return output


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path('deploy/base/cronjobs-by-frequency.yaml'))
    args=parser.parse_args(argv)
    render_cronjobs(PROFILES,args.output)
    return 0


if __name__=='__main__': raise SystemExit(main())
