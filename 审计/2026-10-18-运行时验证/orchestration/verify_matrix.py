from pathlib import Path
import os,subprocess,json,time,sys
root=Path.cwd();work=root/'.superpowers/sdd/2026-10-02-production-release'
env=os.environ.copy()
for name in ('PYTHONPATH','PYTHONHOME','VIRTUAL_ENV','PIP_NO_COMPILE','PIP_EXTRA_INDEX_URL','PIP_FIND_LINKS','PIP_NO_INDEX','PIP_TRUSTED_HOST'):env.pop(name,None)
env.update({'PYTHONUTF8':'1','PYTHONDONTWRITEBYTECODE':'1','PIP_COMPILE':'0','PIP_CONFIG_FILE':os.devnull,'PIP_INDEX_URL':'https://pypi.org/simple','PIP_DISABLE_PIP_VERSION_CHECK':'1','PIP_CACHE_DIR':str(root/'.superpowers/pip-cache')})
print('WAITING FOR ALL CLEAN BOOTSTRAPS',flush=True)
deadline=time.monotonic()+1800
while True:
    try:
        bootstrap=json.loads((work/'bootstrap-fast-results.json').read_text(encoding='utf-8'))
    except (FileNotFoundError,json.JSONDecodeError):
        bootstrap={}
    def finished(version):
        entry=bootstrap.get(version,{})
        return 'freeze' in entry or any(isinstance(v,dict) and 'exit' in v and v['exit']!=0 for v in entry.values())
    if all(finished(version) for version in ('3.11.16','3.9.25','3.14.7')):break
    if time.monotonic()>deadline:raise SystemExit('Bootstrap wait timed out; no runtime claims made')
    time.sleep(5)
results={}
for version in ('3.11.16','3.9.25','3.14.7'):
    target=work/'matrix-corrected'/('python-'+version);target.mkdir(parents=True,exist_ok=False)
    python=root/'out/ra'/('s'+version.replace('.',''))/'Scripts/python.exe'
    proof=root/'out/ra'/('w'+version.replace('.',''))
    if bootstrap.get(version,{}).get('pip-check',{}).get('exit')!=0:
        results[version]={'status':'bootstrap_failed'};continue
    print('VERIFY START',version,flush=True);started=time.monotonic()
    try:
        with (target/'verify-command.log').open('w',encoding='utf-8') as f:
            r=subprocess.run([str(python),'-X','utf8','-m','tools.verify_release','--local-checks-only','--output-dir',str(proof)],cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=1800)
        entry={'exit':r.returncode,'seconds':round(time.monotonic()-started,2)}
    except subprocess.TimeoutExpired:
        entry={'exit':None,'timeout':True}
    report=proof/'verification.json'
    if report.exists():
        data=json.loads(report.read_text(encoding='utf-8'));entry.update({'local_checks_success':data.get('local_checks_success'),'release_success':data['success'],'failed_checks':{k:v['detail'] for k,v in data['checks'].items() if not v['success']}})
    results[version]=entry
    (work/'runtime-matrix-corrected-results.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
    print('VERIFY END',version,json.dumps(entry),flush=True)
print(json.dumps(results,indent=2),flush=True)
