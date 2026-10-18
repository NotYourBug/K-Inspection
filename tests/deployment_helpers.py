from pathlib import Path
import os
import shutil
import subprocess
import yaml

ROOT=Path(__file__).resolve().parents[1]


def render(path):
    command=os.getenv('KUSTOMIZE') or shutil.which('kustomize')
    if not command:
        command=str(ROOT/'.superpowers/tools/kustomize-5.4.3/kustomize.exe')
    result=subprocess.run([command,'build',str(ROOT/path)],capture_output=True,text=True,
                          encoding='utf-8',timeout=45)
    assert result.returncode==0,result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def find(docs,kind,name):
    return next(d for d in docs if d['kind']==kind and d['metadata']['name']==name)
