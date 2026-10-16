"""Local release evidence; missing mandatory checks never count as success."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import argparse
import json
import os
import subprocess
import sys


@dataclass(frozen=True)
class CheckOutcome:
    success: bool
    detail: str


@dataclass(frozen=True)
class VerificationResult:
    success: bool
    checks: dict[str, CheckOutcome]
    artifacts: tuple[Path, ...]


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
        checks['imports']=CheckOutcome(str(env) in module_path and str(project_dir) not in module_path,
                                       'Installed module path='+module_path)
    artifacts.extend([python,*output_dir.glob('*.log')])
    return VerificationResult(all(c.success for c in checks.values()),checks,tuple(artifacts))


def verify_release(project_dir: Path, *, python_executable: Path, output_dir: Path,
                   offline: bool=False) -> VerificationResult:
    result=install_package(project_dir,python_executable=python_executable,output_dir=output_dir,offline=offline)
    checks=dict(result.checks)
    checks['environment_acceptance']=CheckOutcome(False,'PENDING: actual runtime matrix, container, storage and shadow acceptance')
    if offline:
        checks['clean_dependency_install']=CheckOutcome(False,'PENDING: offline bootstrap shares installed dependency paths')
    return VerificationResult(False,checks,result.artifacts)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-dir',type=Path,default=Path.cwd())
    parser.add_argument('--python',type=Path,default=Path(sys.executable))
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--offline',action='store_true')
    args=parser.parse_args(argv)
    result=verify_release(args.project_dir,python_executable=args.python,output_dir=args.output_dir,offline=args.offline)
    data={'success':result.success,'checks':{k:vars(v) for k,v in result.checks.items()},
          'artifacts':[str(p) for p in result.artifacts]}
    (args.output_dir/'verification.json').write_text(json.dumps(data,indent=2),encoding='utf-8')
    print(json.dumps(data,indent=2))
    return 0 if result.success else 1


if __name__=='__main__':
    raise SystemExit(main())
