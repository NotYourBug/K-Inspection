"""Verify raw archived bytes and their committed Git blobs, without runtimes."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
archive = Path(__file__).resolve().parent
manifest = archive / 'SHA256SUMS.json'
records = json.loads(manifest.read_text(encoding='utf-8'))['files']
sha = lambda data: hashlib.sha256(data).hexdigest()
assert records and all(sha((root / name).read_bytes()) == digest for name, digest in records.items())
summary = json.loads((archive / 'orchestration/acceptance-summary.json').read_text(encoding='utf-8'))
assert summary['local_checks_success'] is True and summary['release_success'] is False
assert summary['installed_commands_total'] == 27
assert set(summary['versions']) == {'3.9.25', '3.11.16', '3.14.7'}
for version, result in summary['versions'].items():
    wheel = archive / ('python-' + version) / 'verification/package/wheel/k8s_inspector-0.1.0-py3-none-any.whl'
    assert sha(wheel.read_bytes()) == result['wheel_sha256']
if '--working-only' not in sys.argv:
    git = 'C:/Users/23033/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/git/cmd/git.exe'
    paths = list(records) + [manifest.relative_to(root).as_posix()]
    payload = ''.join('HEAD:' + path + '\n' for path in paths).encode('utf-8')
    output = subprocess.check_output([git, 'cat-file', '--batch'], input=payload, cwd=root)
    cursor = 0
    for path in paths:
        newline = output.index(b'\n', cursor)
        header = output[cursor:newline].split()
        assert len(header) == 3 and header[1] == b'blob', (path, header)
        size = int(header[2])
        data = output[newline + 1:newline + 1 + size]
        assert data == (root / path).read_bytes(), 'Git blob differs from working bytes: ' + path
        assert sha(data) == records.get(path, sha(manifest.read_bytes())), path
        cursor = newline + size + 2
    assert cursor == len(output)
    allowed = {'.gitattributes', 'docs/superpowers/plans/2026-10-02-production-release.md',
               'docs/superpowers/plans/2026-10-02-remediation-roadmap.md',
               '审计/22-本地运行时验收-2026-10-18.md', '审计/23-本地运行时验收执行台账-2026-10-18.md'}
    changed = subprocess.check_output([git, '-c', 'core.quotepath=false', 'diff', '--name-only',
                                       summary['baseline_commit'], 'HEAD'], cwd=root).decode('utf-8').splitlines()
    assert all(path in allowed or path.startswith('审计/2026-10-18-运行时验证/') for path in changed)
    print('Git blobs equal working bytes:', len(paths), '; product source unchanged from', summary['baseline_commit'])
print('Working archive SHA256 entries verified:', len(records), '; local=true; release=false; installed commands=27/27')
