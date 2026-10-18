from pathlib import Path
import hashlib
import json
import re
import shutil
import subprocess
import sys

root = Path.cwd()
work = root / 'out/github-sync-a3d22c5'
archive = root / '审计/2026-10-18-GitHub整合验证'
baseline = '94ae59d9c09bfc36d28acb1446f31f375d8f84ac'
git = 'C:/Users/23033/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/git/cmd/git.exe'
assert subprocess.check_output([git, 'rev-parse', 'HEAD'], text=True).strip() == baseline

def text(path):
    data = path.read_bytes()
    return data.decode('utf-16') if data[:2] in (b'\xff\xfe', b'\xfe\xff') else data.decode('utf-8-sig')

suite = text(work / 'review-full-suite-elevated.log')
summary = next(line for line in reversed(suite.splitlines()) if re.search(r'\d+ passed', line))
assert re.search(r'487 passed, 3 skipped', summary), summary
assert 'failed' not in summary and 'error' not in summary.lower()
assert '30 passed' in text(work / 'latency-green.log')
assert '37 passed, 1 skipped' in text(work / 'durability-green.log')
audit = root / '审计/24-GitHub整合与审查修复-2026-10-18.md'
content = audit.read_text(encoding='utf-8')
placeholder = '完整本地回归：等待本轮完整命令退出后填写。'
assert placeholder in content
content = content.replace(placeholder, '完整本地回归：Windows CPython 3.12.14，`' + summary.strip() + '`。两个 Docker skips 与一个真实 POSIX 描述符 skip 分别待该修复提交的 GitHub 容器任务和 Linux 矩阵补验。')
audit.write_bytes(content.encode('utf-8'))
names = ['initial-review-comments.json', 'initial-pr-ci.json', 'initial-push-ci.json',
         'container-job.log', 'latency-red.log', 'latency-green.log',
         'durability-red.log', 'durability-green.log', 'review-full-suite.log',
         'review-full-suite-elevated.log', 'kustomize-unprivileged-error.log',
         'deployment-escalated.log', 'prepare-review-bundle.py']
for name in names:
    shutil.copyfile(work / name, archive / name)
for version in ('3.9', '3.11', '3.14'):
    source = work / 'push-artifacts' / ('wheel-python-' + version) / 'out/ci-release'
    destination = archive / 'initial-linux' / version
    destination.mkdir(parents=True, exist_ok=True)
    for name in ('source-tests.log', 'verification.json', 'runtime-metadata.log'):
        shutil.copyfile(source / name, destination / name)
shutil.copyfile(work / 'push-artifacts/image-metadata/image-metadata.json', archive / 'initial-image-metadata.json')
(archive / 'execution-summary.json').write_bytes((json.dumps({
    'baseline_commit': baseline, 'pr': 'https://github.com/NotYourBug/K-Inspection/pull/1',
    'review_comment_ids_fixed': [4306444704, 4306444711],
    'local_python': sys.version, 'local_full_suite': summary,
    'latency_red': '4 failed, 2 passed', 'latency_green': '30 passed',
    'durability_red': '5 failed, 1 skipped, 6 deselected', 'durability_green': '37 passed, 1 skipped',
    'first_full_suite': '4 failed, 473 passed, 3 skipped, 10 errors; Kustomize sandbox evalsymlink denied',
    'first_push_ci': 39113353394, 'first_pr_ci': 39113411196,
    'initial_ci_scope': 'Only baseline94ae59d; each Linux source476 pass/2skip and separate container2pass',
    'fixed_head_ci': 'Must use current PR head checks; baseline CI is not reused as proof of later fixes',
    'release_ready': False, 'real_cluster_storage_receiver_shadow_acceptance': 'pending'
}, ensure_ascii=False, indent=2) + '\n').encode('utf-8'))
attrs = root / '.gitattributes'
attrs.write_bytes(attrs.read_bytes().replace(b'\r\n', b'\n'))
source_names = ['k8s_inspector/collector.py', 'k8s_inspector/rules_control.py', 'k8s_inspector/compatibility.py',
                'k8s_inspector/report.py', 'k8s_inspector/observation_state.py',
                'tests/test_github_review_regressions.py', 'tests/test_report_persistence.py']
files = sorted(p for p in archive.rglob('*') if p.is_file() and p.name != 'SHA256SUMS.json')
files += [root / name for name in source_names] + [audit, attrs]
hashes = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
assert len(hashes) == len(files)
(archive / 'SHA256SUMS.json').write_bytes((json.dumps({'algorithm': 'sha256', 'baseline_commit': baseline,
    'files': hashes}, ensure_ascii=False, indent=2) + '\n').encode('utf-8'))
print(summary)
print('Prepared raw evidence/source hash entries:', len(hashes))
