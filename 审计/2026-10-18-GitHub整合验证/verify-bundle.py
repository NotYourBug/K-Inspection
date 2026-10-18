"""Verify this review-fix snapshot's raw evidence and source bytes."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

archive = Path(__file__).resolve().parent
root = archive.parents[1]
manifest = archive / 'SHA256SUMS.json'
files = json.loads(manifest.read_text(encoding='utf-8'))['files']
for name, digest in files.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
if '--working-only' not in sys.argv:
    git = 'C:/Users/23033/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/git/cmd/git.exe'
    names = list(files) + [manifest.relative_to(root).as_posix()]
    output = subprocess.check_output([git, 'cat-file', '--batch'], cwd=root,
        input=''.join('HEAD:' + name + '\n' for name in names).encode('utf-8'))
    cursor = 0
    for name in names:
        end = output.index(b'\n', cursor)
        header = output[cursor:end].split()
        assert len(header) == 3 and header[1] == b'blob', name
        size = int(header[2])
        assert output[end + 1:end + 1 + size] == (root / name).read_bytes(), name
        cursor = end + size + 2
    assert cursor == len(output)
    print('Committed Git blobs match:', len(names))
print('Working SHA256 entries verified:', len(files))
