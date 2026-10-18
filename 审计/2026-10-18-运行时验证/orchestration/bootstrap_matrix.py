from pathlib import Path
import subprocess,os,json,time,hashlib,sys
root=Path.cwd();work=root/'.superpowers/sdd/2026-10-02-production-release'
env=os.environ.copy()
for name in ('PYTHONPATH','PYTHONHOME','VIRTUAL_ENV'):env.pop(name,None)
env['PYTHONUTF8']='1'
env['PYTHONDONTWRITEBYTECODE']='1'
results={}
for version in ('3.11.16','3.9.25','3.14.7'):
    runtime=root/'.superpowers/tools/python-runtimes'/('cpython-'+version+'-windows-x86_64-none')/'python.exe'
    target=work/'bootstrap-fast'/('python-'+version);target.mkdir(parents=True,exist_ok=False)
    selected=root/'out/ra'/('s'+version.replace('.',''));python=selected/'Scripts/python.exe'
    entry={'version':version,'base_executable':str(runtime),'executable_sha256':hashlib.sha256(runtime.read_bytes()).hexdigest()}
    for name,command in [
        ('create-venv',[runtime,'-I','-m','venv',selected]),
        ('clean-before',[python,'-I','-c','import sys,site,json,importlib.metadata as m;print(json.dumps({"python":sys.version,"prefix":sys.prefix,"base_prefix":sys.base_prefix,"user_site":site.ENABLE_USER_SITE,"packages":[d.metadata["Name"] for d in m.distributions()]}))']),
        ('install-dev',[python,'-I','-m','pip','--isolated','--disable-pip-version-check','install','--no-compile','--no-input','--index-url','https://pypi.org/simple','--cache-dir',root/'.superpowers/pip-cache','-r',root/'requirements.txt']),
        ('pip-check',[python,'-I','-m','pip','--isolated','check']),
        ('freeze',[python,'-I','-m','pip','--isolated','freeze','--all'])]:
        print(version,name,flush=True);started=time.monotonic()
        try:
            with (target/(name+'.log')).open('w',encoding='utf-8') as f:
                result=subprocess.run([str(x) for x in command],cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=600)
            entry[name]={'exit':result.returncode,'seconds':round(time.monotonic()-started,2)}
        except subprocess.TimeoutExpired:
            entry[name]={'exit':None,'timeout':True}
        results[version]=entry
        (work/'bootstrap-fast-results.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
        if entry[name]['exit']!=0:
            print('FAILED',version,name,flush=True);break
    else:print('BOOTSTRAP PASSED',version,flush=True)
print(json.dumps(results,indent=2),flush=True)
