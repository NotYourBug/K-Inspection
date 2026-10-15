# Whole-batch review: A1–A6

Base eac3b3d; head 9d445f7e0cda1f762b9cf99be7e1462009b0e602.
Plan docs/superpowers/plans/2026-10-02-critical-inspection-fixes.md.
Spec docs/superpowers/specs/2026-10-02-remediation-design.md.
Rulings: .superpowers/sdd/2026-10-02-critical-inspection-fixes/progress.md.

Implemented strict config/graces; current/history Pod semantics; PVC Lost/delayed binding; partial failure preservation; addon Ready/controller expectations and namespaced read-only RBAC; atomic latest/archives/safe cleanup; independent sinks/quality metrics/status manifest.

Review Focus verbatim:
1. 已恢复的历史 OOM 被当成当前故障：A2 的 test_running_ready_pod_with_old_oom_is_not_current_fail。
2. Pod Running 但 init 失败或 Ready=False 被漏报：A2 的两个当前状态测试。
3. 正常等待消费者的 PVC 被误报、Lost 被漏报：A3 的状态和关联测试。
4. Node NotReady 后 metrics 出错覆盖 FAIL：A4 的 test_node_failure_survives_metrics_error。
5. 恢复后 latest 仍是旧 FAIL，或写文件失败阻断指标：A5/A6 的输出故障测试。

Author verification: baseline 163 passed; A1 184; A2 201; A3 209; A4 217; A5 228; A6 241 passed, all Python3.12.14/Windows with real Kubernetes SDK37.0.1 installed from declared constraints. Full dependencies in 审计/2026-10-15-实施基线/dependencies-py312.txt.

No real cluster/provider sends, Linux/image validation or claimed Python3.9/3.11/3.14 runtime verification. B–F not implemented; C owns structured observations, last-valid state and monitoring recovery. Avoid re-reviewing existing unmodified backlog as though A promised to implement all35 tasks; flag real A regressions regardless of whether spec enumerates the trigger.

Read-only review. Do not modify source/index/HEAD. Do not spawn agents. Use the requesting-code-review template, grade by actual impact, provide precise file:line and concrete reproduction, and list every behavior declined to judge. Return Critical/Important/Minor and readiness verdict.
