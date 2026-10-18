"""Archive this continuation's evidence, excluding disposable environments."""
from pathlib import Path
import json
import os
import shutil

root = Path.cwd().resolve()
work = root / '.superpowers/sdd/2026-10-02-production-release'
archive = root / '审计/2026-10-18-运行时验证'
identity = '# SDD ledger — plan: docs/superpowers/plans/2026-10-02-production-release.md'
assert (work / 'progress.md').read_text(encoding='utf-8').splitlines()[0] == identity
if archive.exists():
    # Resume this exact plan's interrupted copy only; no unrelated archive.
    assert (archive / 'orchestration/progress.md').read_text(encoding='utf-8').splitlines()[0] == identity
    assert json.loads((archive / 'orchestration/acceptance-summary.json').read_text(encoding='utf-8'))['baseline_commit'] == '44b375f70af922258a10353eb98ef6354df1c120'
summary = json.loads((work / 'acceptance-summary.json').read_text(encoding='utf-8'))
assert summary['local_checks_success'] and not summary['release_success']

def copy_evidence(source, destination):
    count = 0
    for base, dirs, files in os.walk(source, followlinks=False):
        dirs[:] = [d for d in dirs if d not in ('source-env', 'venv', '__pycache__')]
        base = Path(base)
        for name in dirs + files:
            path = base / name
            assert not path.is_symlink()
            assert not (getattr(path.stat(), 'st_file_attributes', 0) & 0x400), path
        for name in files:
            path = base / name
            target = destination / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            count += 1
    return count

counts = {'orchestration': copy_evidence(work, archive / 'orchestration')}
for version in summary['versions']:
    proof = root / 'out/ra' / ('w' + version.replace('.', ''))
    target = archive / ('python-' + version)
    counts[version] = copy_evidence(proof, target / 'verification')
    shutil.copyfile(proof / 'package/venv/pyvenv.cfg', target / 'installed-wheel-venv.cfg')
    shutil.copyfile(root / 'out/ra' / ('s' + version.replace('.', '')) / 'pyvenv.cfg', target / 'source-venv.cfg')
counts['diagnostic-first-wheel'] = copy_evidence(root / 'out/ra/v31116', archive / 'diagnostic-first-wheel')
shutil.copyfile(root / 'out/ra/OWNER.txt', archive / 'runtime-workspace-OWNER.txt')
old_config = root / 'out/ra/31116/pyvenv.cfg'
if old_config.exists():
    shutil.copyfile(old_config, archive / 'diagnostic-short-precompile-venv.cfg')
(archive / 'archive-map.json').write_text(json.dumps({
    'source_baseline': summary['baseline_commit'],
    'archived_file_groups': counts,
    'proof_mapping': {v: {'original': 'out/ra/w' + v.replace('.', ''), 'archive': 'python-' + v + '/verification'} for v in summary['versions']},
    'omitted': ['source-env and venv trees', '__pycache__', 'shared managed Python/tools/caches'],
    'record_note': 'Historical absolute paths are preserved verbatim in logs; use this mapping after owned scratch cleanup.'
}, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(json.dumps(counts, indent=2))
