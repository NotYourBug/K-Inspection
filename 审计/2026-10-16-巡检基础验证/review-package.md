# C batch whole-range review package

Base: 2f7e806
Head: 43e642b4f7bb7840b852f5c10ff6809e696feb1f
Plan: docs/superpowers/plans/2026-10-02-inspection-foundations-and-monitoring.md
Spec: docs/superpowers/specs/2026-10-02-remediation-design.md
Ledger: .superpowers/sdd/2026-10-02-inspection-foundations-and-monitoring/progress.md

Implemented C1–C7: trustworthy observation DTOs and v1 compatibility; bounded paged snapshot collector; finite profiles/catalog/config precedence; pure domains; report v2/manifest/CLI; persistent complete last-valid plus separate positive partial evidence; independent inventory and deterministic shadow monitoring.

Verification: full suite421 passed/2 upstream SDK deprecations. Official promtool2.53.0:20 new rules,11 temporal groups; legacy standalone+embedded5 each. amtool0.27.0:base/generated config validation and shadow-null routing. Kustomize5.4.3:23 base resources after read-only sandbox metadata escalation. Windows/Python3.12 only; real cluster/Linux/container/PVC/power-loss/independent receiver/daily shadow gates remain open. No deployment, external messages, remote push, merge or PR.

Read all ledger Ruling lines; judge user effect independently. Legacy check_* calculation compatibility and delayed default cutover are explicit deviations. C7 additionally persists positive partial facts without calling them complete valid observations. Complete healthy scope metrics do not create healthy observations for a missing old UID.

Review Focus verbatim:
1. 同名新 UID 继承旧故障/恢复身份：C1 的 test_resource_uid_changes_finding_identity。
2. 第二页失败或 deadline 到期仍声明覆盖完整：C2 的分页/期限测试。
3. 配置未知 profile 回落 full 造成 API 突增：C3 的 test_unknown_profile_is_rejected。
4. FAIL 后采集 ERROR 把故障计数写成0：C6 的 test_error_does_not_clear_last_valid_failure。
5. 指标从未出现/Pushgateway 被清空后所有告警消失：C7 的 inventory 缺失与质量规则测试。

Additional areas worth deliberate adversarial inspection: catalog domain isolation, partial list truth, paged collection timestamps and deadline final page, source/profile identity in persistent state, partial positive evidence lifecycle, per-UID recovery, event cursor/count/window semantics, managed/control/addon visibility, compatibility facade/current defaults, and real shadow integration boundaries. The open E gate does not excuse an actual bug in shipped default behavior.
