"""Local release evidence; missing mandatory checks never count as success."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import argparse
import json
import os
import subprocess
import sys
import shutil
import hashlib
import uuid


@dataclass(frozen=True)
class CheckOutcome:
    success: bool
    detail: str


@dataclass(frozen=True)
class VerificationResult:
    success: bool
    checks: dict[str, CheckOutcome]
    artifacts: tuple[Path, ...]


LOCAL_REQUIRED_CHECKS = (
    'wheel', 'venv', 'install', 'imports', 'source_tests',
    'kustomize_base', 'kustomize_example', 'kustomize_shadow',
    'kustomize', 'promtool', 'amtool', 'runtime_metadata', 'selected_constraints',
)


def local_checks_success(result: VerificationResult) -> bool:
    """Local CI evidence only; this never approves external release acceptance."""
    return all(result.checks.get(name, CheckOutcome(False, 'Missing')).success
               for name in LOCAL_REQUIRED_CHECKS)


def _command(command, *, cwd, log, timeout=180):
    try:
        result=subprocess.run([str(x) for x in command],cwd=cwd,capture_output=True,text=True,
                              encoding='utf-8',errors='replace',timeout=timeout)
        log.write_text(result.stdout+'\n'+result.stderr,encoding='utf-8')
        return CheckOutcome(result.returncode==0,f'exit={result.returncode}; evidence={log.name}')
    except (OSError,subprocess.TimeoutExpired) as exc:
        log.write_text(type(exc).__name__,encoding='utf-8')
        return CheckOutcome(False,f'{type(exc).__name__}; evidence={log.name}')


def install_package(project_dir: Path, *, python_executable: Path, output_dir: Path,
                    offline: bool=False) -> VerificationResult:
    python_executable=Path(python_executable).resolve()
    project_dir,output_dir=Path(project_dir).resolve(),Path(output_dir).resolve()
    checks,artifacts={},[]
    output_dir.mkdir(parents=True,exist_ok=False)
    wheel_dir=output_dir/'wheel'
    checks['wheel']=_command([python_executable,'-m','build','--wheel','--no-isolation','--outdir',wheel_dir],
        cwd=project_dir,log=output_dir/'wheel-build.log')
    wheels=list(wheel_dir.glob('*.whl'))
    if not checks['wheel'].success or len(wheels)!=1:
        return VerificationResult(False,checks,tuple(output_dir.glob('*.log')))
    artifacts.append(wheels[0])
    env=output_dir/'venv'
    checks['venv']=_command([python_executable,'-m','venv',env],cwd=output_dir,log=output_dir/'venv.log')
    python=env/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
    if not checks['venv'].success:
        return VerificationResult(False,checks,tuple(artifacts))
    command=[python,'-m','pip','install','--disable-pip-version-check','--no-cache-dir']
    if offline:
        command+=['--no-index','--no-deps']
    else:
        constraints=project_dir/'constraints/runtime.txt'
        if constraints.exists(): command+=['-c',constraints]
    checks['install']=_command(command+[wheels[0]],cwd=output_dir,log=output_dir/'install.log')
    if offline:
        # Source import isolation is real; dependency resolution is NOT tested
        # offline. Add preinstalled dependency directories without executing any
        # editable-source .pth files from them.
        probe=subprocess.run([str(python_executable),'-I','-c','import json,site;print(json.dumps(site.getsitepackages()))'],
            capture_output=True,text=True,check=True,timeout=30)
        parent_paths=json.loads(probe.stdout)
        child=subprocess.run([str(python),'-I','-c','import sysconfig;print(sysconfig.get_path("purelib"))'],capture_output=True,text=True,check=True,timeout=30)
        site_dir=Path(child.stdout.strip())
        site_dir.mkdir(parents=True,exist_ok=True)
        (site_dir/'offline-dependencies.pth').write_text('\n'.join(parent_paths)+'\n',encoding='utf-8')
    code='import k8s_inspector,k8s_inspector.observation_state,k8s_inspector.compatibility;print(k8s_inspector.__file__)'
    checks['imports']=_command([python,'-I','-c',code],cwd=output_dir,log=output_dir/'imports.log')
    if checks['imports'].success:
        module_path=(output_dir/'imports.log').read_text(encoding='utf-8').strip()
        checks['imports']=CheckOutcome(Path(module_path).resolve().is_relative_to(env.resolve()),
                                       'Installed module path='+module_path)
    artifacts.extend([python,*output_dir.glob('*.log')])
    return VerificationResult(all(c.success for c in checks.values()),checks,tuple(artifacts))


def _tool(project, name, overrides):
    if name in overrides: return list(overrides[name])
    found=shutil.which(name)
    if found: return [found]
    folders={'kustomize':'kustomize-5.4.3','promtool':'prometheus-2.53.0','amtool':'alertmanager-0.27.0'}
    candidate=project/'.superpowers/tools'/folders.get(name,'unavailable')/(name+'.exe')
    return [str(candidate)] if candidate.is_file() else None


def verify_release(project_dir: Path, *, python_executable: Path, output_dir: Path,
                   offline: bool=False, run_source_tests: bool=True, tool_commands=None) -> VerificationResult:
    import yaml
    python_executable=Path(python_executable).resolve()
    project,output=Path(project_dir).resolve(),Path(output_dir).resolve()
    output.mkdir(parents=True,exist_ok=False)
    result=install_package(project,python_executable=python_executable,output_dir=output/'package',offline=offline)
    checks=dict(result.checks);artifacts=list(result.artifacts);overrides=tool_commands or {}
    if run_source_tests:
        checks['source_tests']=_command([python_executable,'-m','pytest','tests/','-q'],cwd=project,
            log=output/'source-tests.log',timeout=1200)
    else:
        checks['source_tests']=CheckOutcome(False,'PENDING: source suite intentionally not run')
    kustomize=_tool(project,'kustomize',overrides)
    rendered={}
    for name,path in [('base','deploy'),('example','deploy/overlays/example'),('shadow','deploy/overlays/shadow')]:
        if not kustomize or not (project/path).exists():
            checks['kustomize_'+name]=CheckOutcome(False,'PENDING: kustomize or overlay unavailable')
            continue
        target=output/(name+'.yaml')
        checks['kustomize_'+name]=_command(kustomize+['build',str(project/path),'--output',str(target)],
            cwd=project,log=output/('kustomize-'+name+'.log'))
        if checks['kustomize_'+name].success:
            rendered[name]=list(yaml.safe_load_all(target.read_text(encoding='utf-8')))
            artifacts.append(target)
    checks['kustomize']=CheckOutcome(all(checks['kustomize_'+n].success for n in ('base','example','shadow')),
                                    'See per-overlay render checks')
    configs=output/'configs';configs.mkdir()
    promtool=_tool(project,'promtool',overrides);amtool=_tool(project,'amtool',overrides)
    prom_checks=[];am_checks=[]
    for name,docs in rendered.items():
        for document in docs:
            if not document or document['kind']!='ConfigMap': continue
            for key,value in document.get('data',{}).items():
                if key=='prometheus.yml':
                    config=yaml.safe_load(value)
                    # Host-only validation tree, not a deployed configuration rewrite.
                    rules=[]
                    for cm in docs:
                        if cm and cm['kind']=='ConfigMap' and cm['metadata']['name']=='k8s-inspector-prometheus-rules':
                            for filename,content in cm.get('data',{}).items():
                                rulepath=configs/(name+'-'+filename);rulepath.write_text(content,encoding='utf-8');rules.append(str(rulepath))
                    config['rule_files']=rules
                    file=configs/(name+'-prometheus.yml');file.write_text(yaml.safe_dump(config),encoding='utf-8')
                    if promtool:
                        prom_checks.append(_command(promtool+['check','config',str(file)],cwd=project,log=output/(name+'-promconfig.log')))
                if key=='alertmanager.yml':
                    config=yaml.safe_load(value)
                    for receiver in config.get('receivers',[]):
                        for webhook in receiver.get('webhook_configs',[]):
                            auth=webhook.get('http_config',{}).get('authorization',{})
                            if 'credentials_file' in auth:
                                token=configs/'validation-token';token.write_text('validation-only-not-a-real-token',encoding='utf-8')
                                auth['credentials_file']=str(token)
                    file=configs/(name+'-alertmanager.yml');file.write_text(yaml.safe_dump(config),encoding='utf-8')
                    if amtool:
                        am_checks.append(_command(amtool+['check-config',str(file)],cwd=project,log=output/(name+'-amconfig.log')))
    if promtool:
        prom_checks.append(_command(promtool+['check','rules',str(project/'deploy/monitoring/generated/inspection-rules.yaml')],cwd=project,log=output/'rules-check.log'))
        prom_checks.append(_command(promtool+['test','rules',str(project/'tests/prometheus/inspection-rules.test.yaml')],cwd=project,log=output/'rules-test.log'))
        prom_checks.append(_command(promtool+['check','rules',str(project/'deploy/monitoring/bridge-rules.yaml')],cwd=project,log=output/'bridge-rules-check.log'))
        prom_checks.append(_command(promtool+['test','rules',str(project/'tests/prometheus/bridge-rules.test.yml')],cwd=project,log=output/'bridge-rules-test.log'))
    checks['promtool']=CheckOutcome(bool(prom_checks) and all(c.success for c in prom_checks),
        '; '.join(c.detail for c in prom_checks) if prom_checks else 'PENDING: promtool unavailable')
    checks['amtool']=CheckOutcome(bool(am_checks) and all(c.success for c in am_checks),
        '; '.join(c.detail for c in am_checks) if am_checks else 'PENDING: amtool/config unavailable')
    if not offline and shutil.which('docker'):
        image='k8s-inspector-verify:'+uuid.uuid4().hex
        build=_command(['docker','build','-t',image,str(project)],cwd=project,log=output/'docker-build.log',timeout=1200)
        smoke=[build]
        if build.success:
            for module in ('cli','alertmanager_bridge'):
                smoke.append(_command(['docker','run','--rm','--network','none','--read-only','--entrypoint','python',image,
                    '-m','k8s_inspector.'+module,'--help'],cwd=project,log=output/('docker-'+module+'.log'),timeout=150))
        checks['docker']=CheckOutcome(all(c.success for c in smoke),'local image '+image+'; '+'; '.join(c.detail for c in smoke))
    else:
        checks['docker']=CheckOutcome(False,'PENDING: Docker unavailable or offline mode forbids image pulls/builds')
    checks['environment_acceptance']=CheckOutcome(False,'PENDING: reviewed runtime matrix, actual storage/receiver/shadow acceptance')
    checks['clean_dependency_install']=CheckOutcome(not offline and checks.get('install',CheckOutcome(False,'')).success,
        'PENDING: offline bootstrap shares installed dependency paths' if offline else 'Clean venv install result')
    # Preserve selected interpreter metadata, without exposing pip index URLs.
    probe=_command([python_executable,'-I','-c',
        'import json,platform,importlib.metadata as m;print(json.dumps({"python":platform.python_version(),"dependencies":{n:m.version(n) for n in ["kubernetes","lark-oapi","PyYAML","prometheus-client","setuptools","wheel","build","pytest"]}}))'],
        cwd=project,log=output/'runtime-metadata.log')
    checks['runtime_metadata']=probe
    if probe.success:
        from packaging.requirements import Requirement
        from packaging.markers import default_environment
        actual=json.loads((output/'runtime-metadata.log').read_text(encoding='utf-8'))
        marker_environment=default_environment();marker_environment['python_version']='.'.join(actual['python'].split('.')[:2])
        mismatches=[]
        for file in (project/'constraints/runtime.txt',project/'constraints/build.txt'):
            for line in file.read_text(encoding='utf-8').splitlines():
                if not line.strip() or line.startswith('#'): continue
                requirement=Requirement(line)
                if requirement.marker and not requirement.marker.evaluate(marker_environment): continue
                installed=actual['dependencies'].get(requirement.name)
                if installed is not None and installed not in requirement.specifier:
                    mismatches.append(requirement.name+'='+installed)
        checks['selected_constraints']=CheckOutcome(not mismatches,'Installed pins verified' if not mismatches else 'PENDING pinned clean build: '+', '.join(mismatches))
    hashes={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts if p.is_file() and p.suffix=='.whl' and p.is_relative_to(output)}
    manifest=output/'candidate-manifest.json'
    source_hashes={}
    for folder in ('k8s_inspector','tools','deploy','constraints'):
        for path in sorted((project/folder).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix in ('.py','.yaml','.yml','.txt'):
                source_hashes[path.relative_to(project).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ('pyproject.toml','Dockerfile'):
        source_hashes[name]=hashlib.sha256((project/name).read_bytes()).hexdigest()
    manifest.write_text(json.dumps({'wheel_sha256':hashes,'source_sha256':source_hashes,'schema':{'bridge':3,'report':2},'offline':offline,'release_ready':False,
        'checks':{k:vars(v) for k,v in checks.items()}},indent=2),encoding='utf-8')
    artifacts.extend([manifest,*output.glob('*.log')])
    return VerificationResult(all(c.success for c in checks.values()),checks,tuple(artifacts))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-dir',type=Path,default=Path.cwd())
    parser.add_argument('--python',type=Path,default=Path(sys.executable))
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--offline',action='store_true')
    parser.add_argument('--local-checks-only',action='store_true',
                        help='Exit on mandatory local CI checks only; release acceptance stays pending in JSON')
    args=parser.parse_args(argv)
    result=verify_release(args.project_dir,python_executable=args.python,output_dir=args.output_dir,offline=args.offline)
    local_success=local_checks_success(result)
    data={'success':result.success,'local_checks_success':local_success,
          'exit_gate':'local' if args.local_checks_only else 'release',
          'checks':{k:vars(v) for k,v in result.checks.items()},
          'artifacts':[str(p) for p in result.artifacts]}
    (args.output_dir/'verification.json').write_text(json.dumps(data,indent=2),encoding='utf-8')
    print(json.dumps(data,indent=2))
    return 0 if (local_success if args.local_checks_only else result.success) else 1


if __name__=='__main__':
    raise SystemExit(main())
