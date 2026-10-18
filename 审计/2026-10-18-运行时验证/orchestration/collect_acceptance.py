"""Collect already completed local runtime evidence; no new runtime claims."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import zipfile

root = Path.cwd()
work = root / '.superpowers/sdd/2026-10-02-production-release'
git = 'C:/Users/23033/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/git/cmd/git.exe'
baseline = '44b375f70af922258a10353eb98ef6354df1c120'
assert subprocess.check_output([git, 'rev-parse', 'HEAD'], text=True).strip() == baseline
matrix = json.loads((work / 'runtime-matrix-corrected-results.json').read_text())
commands = json.loads((work / 'installed-command-results.json').read_text())
expected = {'3.9.25': (7, 510.23), '3.11.16': (0, 333.58), '3.14.7': (1, 413.50)}
result = {'date': '2026-10-18', 'baseline_commit': baseline,
          'scope': 'Windows x86_64 local runtime acceptance only',
          'local_checks_success': True, 'release_success': False,
          'product_source_changed': False,
          'pending_checks': ['docker', 'environment_acceptance'],
          'runtime_environment': {'source_install': 'pip --isolated install --no-compile -r requirements.txt',
                                  'wheel_install_environment': {'PIP_COMPILE': '0', 'PYTHONDONTWRITEBYTECODE': '1'},
                                  'managed_python': 'uv; workspace-local; no-bin/no-registry/no-config',
                                  'proof_roots': 'out/ra/w<version without dots>',
                                  'installed_commands': '-I -B -X utf8; cwd outside source import path'},
          'versions': {}}
reference_sources = None
baseline_bytes = {}
for version, (warnings, seconds) in expected.items():
    proof = root / 'out/ra' / ('w' + version.replace('.', ''))
    verification = json.loads((proof / 'verification.json').read_text())
    candidate = json.loads((proof / 'candidate-manifest.json').read_text())
    entry = matrix[version]
    assert entry['exit'] == 0 and entry['local_checks_success'] is True
    assert entry['release_success'] is False
    assert verification['local_checks_success'] is True and verification['success'] is False
    assert verification['checks']['clean_dependency_install']['success'] is True
    assert {k for k, v in verification['checks'].items() if not v['success']} == set(result['pending_checks'])
    assert candidate['offline'] is False and candidate['release_ready'] is False
    source_hashes = candidate['source_sha256']
    assert len(source_hashes) == 102
    if reference_sources is None:
        reference_sources = source_hashes
    assert reference_sources == source_hashes
    baseline_eol_differences = []
    for name, digest in source_hashes.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
        if name not in baseline_bytes:
            baseline_bytes[name] = subprocess.check_output([git, 'show', baseline + ':' + name])
        original = baseline_bytes[name]
        current = (root / name).read_bytes()
        assert original.replace(b'\r\n', b'\n') == current.replace(b'\r\n', b'\n'), 'baseline content mismatch: ' + name
        if original != current:
            baseline_eol_differences.append(name)
    test_text = (proof / 'source-tests.log').read_text(encoding='utf-8')
    last = next(line for line in reversed(test_text.splitlines()) if '476 passed' in line)
    assert re.search(r'476 passed, 2 skipped', last)
    assert f'in {seconds:.2f}s' in last
    actual_warnings = re.search(r'(\d+) warnings?', last)
    assert (int(actual_warnings.group(1)) if actual_warnings else 0) == warnings
    installed = commands[version]
    assert installed['success'] is True and len(installed['commands']) == 9
    assert all(command['exit'] == 0 for command in installed['commands'])
    assert len(candidate['wheel_sha256']) == 1
    wheel_name, wheel_hash = next(iter(candidate['wheel_sha256'].items()))
    wheel = proof / wheel_name
    assert hashlib.sha256(wheel.read_bytes()).hexdigest() == wheel_hash == installed['wheel_sha256']
    with zipfile.ZipFile(wheel) as archive:
        runtime_files = [n for n in archive.namelist() if n.startswith('k8s_inspector/') and n.endswith('.py')]
        assert len(runtime_files) == 40
        assert set(runtime_files) == {n for n in source_hashes if n.startswith('k8s_inspector/')}
        for name in runtime_files:
            assert archive.read(name) == (root / name).read_bytes(), name
    configs = {}
    for label, venv in [('source', root / 'out/ra' / ('s' + version.replace('.', ''))),
                        ('installed_wheel', proof / 'package/venv')]:
        config = (venv / 'pyvenv.cfg').read_text()
        assert 'include-system-site-packages = false' in config
        assert f'version = {version}' in config
        assert not (venv / 'Lib/site-packages/offline-dependencies.pth').exists()
        configs[label] = config
    metadata = json.loads((proof / 'runtime-metadata.log').read_text())
    assert metadata['python'] == version
    probe_logs = work / 'matrix-corrected' / ('python-' + version)
    assert 'No broken requirements found' in (probe_logs / 'installed-command-6.log').read_text()
    result['versions'][version] = {
        'local_exit': 0, 'local_checks_success': True, 'release_success': False,
        'clean_dependency_install': True,
        'source_tests': {'passed': 476, 'skipped': 2, 'warnings': warnings, 'seconds': seconds,
                         'skips': 'Docker CLI and Bridge entrypoint smoke tests; Docker unavailable'},
        'verifier_seconds': entry['seconds'], 'installed_commands_passed': 9,
        'wheel_sha256': wheel_hash, 'wheel_runtime_files_equal_current_source': 40,
        'source_files_byte_equal_current': 102,
        'source_files_equal_baseline_git_after_CRLF_normalization': 102,
        'baseline_git_raw_byte_line_ending_differences': baseline_eol_differences,
        'runtime_metadata': metadata, 'venv_configs': configs}
result['installed_commands_total'] = 27
result['source_hash_manifest_equal_across_versions'] = True
result['limits'] = [
    'Windows managed runtimes do not establish Linux CI or container behavior.',
    'No bytecode precompilation; arbitrary deep Windows install paths are not certified.',
    'Two Docker tests skipped in each source suite; no cluster or real provider acceptance.',
    'Python 3.9 compatibility passing does not guarantee continued upstream maintenance.',
    'Recorded local timings are not production or container performance evidence.',
    'Selective dependency pins are not a complete transitive lock.'
]
(work / 'acceptance-summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(json.dumps({k: v for k, v in result.items() if k in ('local_checks_success', 'release_success', 'pending_checks', 'installed_commands_total')}, indent=2))
for version, entry in result['versions'].items():
    print(version, entry['source_tests'], entry['wheel_sha256'])
