from pathlib import Path
import subprocess,json,os,time
root=Path.cwd();work=root/'.superpowers/sdd/2026-10-02-production-release'
env=os.environ.copy()
for name in ('PYTHONPATH','PYTHONHOME','VIRTUAL_ENV'):env.pop(name,None)
commands=[['-m','k8s_inspector.cli','--help'],['-m','k8s_inspector.cli','run','--help'],['-m','k8s_inspector.cli','cleanup','--help'],['-m','k8s_inspector.alertmanager_bridge','--help'],['-m','k8s_inspector.bridge_admin','--help'],['-m','k8s_inspector.expected_targets','--help']]
empty=root/'out/ra/empty-corrected';empty.mkdir(exist_ok=True)
results=json.loads((work/'installed-command-results.json').read_text(encoding='utf-8')) if (work/'installed-command-results.json').exists() else {}
for version in ('3.11.16','3.9.25','3.14.7'):
    proof=root/'out/ra'/('w'+version.replace('.',''));target=work/'matrix-corrected'/('python-'+version)
    report=proof/'verification.json'
    if results.get(version,{}).get('success'):
        import hashlib
        wheel=next((proof/'package/wheel').glob('*.whl'))
        assert results[version]['wheel_sha256']==hashlib.sha256(wheel.read_bytes()).hexdigest()
        print('REUSE VERIFIED UNCHANGED WHEEL',version,flush=True)
        continue
    print('WAIT FOR INSTALLED WHEEL',version,flush=True)
    deadline=time.monotonic()+2400
    while not report.exists():
        summary=work/'runtime-matrix-corrected-results.json'
        try:
            completed=json.loads(summary.read_text(encoding='utf-8')) if summary.exists() else {}
        except json.JSONDecodeError:completed={}
        if version in completed or time.monotonic()>deadline:break
        time.sleep(5)
    if not report.exists() or not json.loads(report.read_text(encoding='utf-8'))['checks'].get('install',{}).get('success'):

        results[version]={'status':'no_confirmed_installed_wheel'};continue
    python=proof/'package/venv/Scripts/python.exe';entry=[]
    for index,arguments in enumerate(commands+[['-m','pip','--isolated','check'],['-c','import importlib.util,json,sys;assert importlib.util.find_spec("pytest") is None;assert importlib.util.find_spec("build") is None;print(json.dumps({"prefix":sys.prefix,"base_prefix":sys.base_prefix,"path":sys.path}))'],['-c','import kubernetes,yaml,prometheus_client,lark_oapi;import importlib.metadata as m;print({n:m.version(n) for n in ("kubernetes","lark-oapi","PyYAML","prometheus-client")})']]):
        print('INSTALLED COMMAND',version,index,flush=True)
        started=time.monotonic()
        r=subprocess.run([str(python),'-I','-B','-X','utf8',*arguments],cwd=empty,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=900 if index==8 else 120)
        (target/('installed-command-'+str(index)+'.log')).write_text(r.stdout+'\n'+r.stderr,encoding='utf-8')
        entry.append({'arguments':arguments,'exit':r.returncode,'seconds':round(time.monotonic()-started,2)})
    import hashlib
    wheel=next((proof/'package/wheel').glob('*.whl'))
    results[version]={'success':all(x['exit']==0 for x in entry),'commands':entry,'wheel_sha256':hashlib.sha256(wheel.read_bytes()).hexdigest()}
    (work/'installed-command-results.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
    print('INSTALLED COMMANDS DONE',version,results[version]['success'],flush=True)
(work/'installed-command-results.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
print(json.dumps(results,indent=2))
