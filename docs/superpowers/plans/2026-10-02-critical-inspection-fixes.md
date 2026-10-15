# A：巡检关键缺陷修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** 在保持现有调用方式的前提下，先消除已确认的漏报、误报、陈旧报告和输出故障传播。

**Architecture:** 修复原有函数；暂不引入 C 批次的新模型。新增证据放入 CheckResult.data，C4 再迁移为结构化 Finding。latest 始终更新，归档与业务问题策略分离，各输出独立尝试。

**Tech Stack:** Python、Kubernetes SDK 的 V1 对象、pytest、标准库 tempfile/os/pathlib。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；[总计划](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/plans/2026-10-02-remediation-roadmap.md)。

## Global Constraints

- 不改变 profile 范围；5m 不调用 Metrics API。保持 CLI、旧 Report 构造和异常隔离。
- 不把采集未知输出为 PASS，不因新增采集异常删除已经确认的 FAIL。
- 用对象状态和时间判断当前故障；历史 OOM、累计重启与当前不可用分别表达。
- API 快照先按已有结构读取；本批不建设完整采集框架，也不等待该框架才修缺陷。
- 新增文件路径均为实施目标；每项完成后更新实施记录。提交前先落实 E1 的版本基线。

## Review Focus

1. 已恢复的历史 OOM 被当成当前故障：A2 的 test_running_ready_pod_with_old_oom_is_not_current_fail。
2. Pod Running 但 init 失败或 Ready=False 被漏报：A2 的两个当前状态测试。
3. 正常等待消费者的 PVC 被误报、Lost 被漏报：A3 的状态和关联测试。
4. Node NotReady 后 metrics 出错覆盖 FAIL：A4 的 test_node_failure_survives_metrics_error。
5. 恢复后 latest 仍是旧 FAIL，或写文件失败阻断指标：A5/A6 的输出故障测试。

## A1：严格校验配置，建立真实对象测试夹具

**文件与责任**

- 修改 k8s_inspector/config.py：字段、类型、范围与未知键校验。
- 修改 k8s_inspector/cli.py：清楚报告配置错误；保留现有参数。
- 新增 tests/factories.py：构造 SDK 对象与固定 UTC 时间，不触发真实集群。
- 修改 tests/test_config.py，按需增加 tests/test_cli.py 中配置入口用例。

**接口与约定**

- 保持 load_config(path: str | None) -> InspectorConfig；保留现有字段名称。
- 在 InspectorConfig 新增三个顶层字段 pod_unready_grace_seconds=120、pod_pending_grace_seconds=300、pvc_pending_grace_seconds=300；C3 沿用，不能同时出现两套配置源。
- 比例只接受有限数值且在 [0,1]；计数只接受非负整数；bool 不冒充整数；宽限期为非负有限数。
- factories 提供 pod(name, phase="Running", ready=True, age_seconds=600, uid="pod-1", **overrides)、pvc(name, phase="Bound", age_seconds=600, **overrides)、node(name, ready=True, **overrides)；now 固定为带时区 UTC。返回真实 V1Pod/V1PersistentVolumeClaim/V1Node。
- overrides 只负责替换已声明的 SDK 字段；容器状态、condition、StorageClass 的 fixture 构造必须显式可读。

**测试锚点**

~~~python
@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1, 1.1, True])
def test_invalid_cpu_ratio_is_rejected(value, config_file):
    path = config_file({"thresholds": {"node_cpu_utilization": value}})
    with pytest.raises(ValueError, match="node_cpu_utilization"):
        load_config(str(path))

def test_unknown_config_key_is_rejected(config_file):
    with pytest.raises(ValueError, match="pod_unready_grace_second"):
        load_config(str(config_file({"pod_unready_grace_second": 120})))
~~~

config_file fixture 在本任务定义，写入 JSON/YAML 到 tmp_path；对 NaN 使用 JSON 兼容输入或 YAML 明确表示，避免序列化层先挡住被测校验。

- [ ] 写上述非法值、未知键、旧最小配置及新默认值测试。
- [ ] 运行 python -m pytest tests/test_config.py -q，确认失败来自缺少校验而非夹具错误。
- [ ] 实现校验，补 YAML 缺依赖与文件错误的友好提示，邻近无效参数边界一并修正。
- [ ] 再运行配置与 CLI 相关测试；确认既有合法配置仍可加载。
- [ ] 审查配置兼容影响，记录新增字段和迁移提示，提交或保存可识别快照。

## A2：修正 Pod 当前状态、Ready 与历史稳定性

**文件与责任**

- 修改 k8s_inspector/common.py：当前容器状态与历史信息分开。
- 修改 k8s_inspector/checks.py：读取 init 状态、Ready condition、Pod age、owner、deletion_timestamp。
- 修改 tests/test_checks.py；新增 tests/test_pod_semantics.py，使用 A1 factories。

**接口与约定**

- 保持 common.pod_issue(container_statuses, phase, restart_threshold) 的兼容签名与原有返回类型；历史 last_state 不再直接生成“当前不可用”结论。
- 新增 evaluate_pod_state(pod: V1Pod, cfg: InspectorConfig, now: datetime) -> dict[str, object]，返回 current_issues、historical_issues、readiness、observed_at。本批为 checks 内部接口，C4 替换时提供迁移测试。
- Ready=False 超过宽限期形成当前异常；Ready 缺失为未知，不能仅由 phase=Running 判 PASS。
- 持续不Ready时间优先用Ready condition.last_transition_time，不能以Pod创建时间假装不Ready持续时长；时间缺失/未来时间标证据不足，不直接计算为超宽限。
- init 当前 CrashLoop/非零终止形成异常；完成的普通 init 成功不作为故障。restartable init sidecar 按目标 K8s 版本的 fixture 单独处理。
- Pending 未过宽限期不直接 FAIL；持续不可调度保存原因。终止中、成功 Job 和 Failed Job 明确分类。
- 同一 Pod 可同时保留当前故障与重启历史；不能通过早 return 丢掉其余证据。

**测试锚点**

~~~python
def test_running_ready_pod_with_old_oom_is_not_current_fail(pod_with_old_oom, cfg, now):
    state = evaluate_pod_state(pod_with_old_oom, cfg, now)
    assert state["current_issues"] == []
    assert any(x["reason"] == "OOMKilled" for x in state["historical_issues"])

def test_running_unready_pod_after_grace_is_current_issue(unready_pod, cfg, now):
    state = evaluate_pod_state(unready_pod, cfg, now)
    assert "PodNotReady" in {x["reason"] for x in state["current_issues"]}
~~~

本任务另定义 test_init_crashloop_is_current_issue、test_recent_pending_is_within_grace、test_missing_ready_is_unknown、test_bad_pod_does_not_hide_restart_evidence、test_successful_job_is_not_unready_failure。历史证据可以触发独立 WARN，但不能成为当前 OOM FAIL。

- [ ] 写上列测试，明确每个 fixture 的当前/历史状态与时间。
- [ ] 运行 python -m pytest tests/test_pod_semantics.py -q，验证现有漏报/误报确实被复现。
- [ ] 先修 common，再接 checks；保持结果名称、计数与旧报告兼容。
- [ ] 运行 Pod 语义测试与 tests/test_checks.py；检查 5m 没有新增 metrics 请求。
- [ ] 审查当前故障和稳定性证据不会混用，记录目标 K8s 版本边界，提交。

## A3：修正 PVC 状态与等待消费者语义

**文件与责任**

- 修改 k8s_inspector/checks.py 中 check_pending_pvcs 与 run_all 调用。
- 新增 tests/test_pvc_semantics.py；补 tests/test_checks.py 的 profile 兼容测试。

**接口与约定**

- 扩展 check_pending_pvcs(core, cfg=None, storage=None, *, now=None) -> CheckResult，旧只传 core 的调用继续可用；缺少关联信息时明确 UNKNOWN/WARN。
- 按需一次读取 Pod 与 StorageClass；后续由 C2 快照统一提供，不能每个 PVC 额外 list 全集群。
- Lost 明确 FAIL；Bound 正常；普通 Pending 超过宽限期检查消费者/调度原因。
- WaitForFirstConsumer 且无消费者不直接 FAIL；有消费者仍 Pending，结合 Pod 调度及存储错误判断。
- StorageClass/消费者读取 403 不把 PVC 标 PASS；报告关联数据缺失与原始状态。

~~~python
def test_wait_for_first_consumer_without_consumer_is_not_fail(core, storage, cfg, now):
    core.pvcs = [pvc("data", phase="Pending", storage_class="delayed")]
    core.pods = []
    storage.classes = [storage_class("delayed", binding_mode="WaitForFirstConsumer")]
    result = check_pending_pvcs(core, cfg, storage, now=now)
    assert result.status != Status.FAIL
    assert result.data["waiting_for_consumer"] == 1

def test_lost_pvc_is_fail(core, cfg, now):
    core.pvcs = [pvc("data", phase="Lost")]
    result = check_pending_pvcs(core, cfg, now=now)
    assert result.status == Status.FAIL
    assert result.data["lost_count"] == 1
~~~

fixture 的 storage_class 参数在本任务扩展 A1 pvc factory，映射到 spec.storage_class_name；storage_class helper 返回 V1StorageClass。

- [ ] 写上述测试及 test_old_pending_with_failed_consumer_has_evidence、test_storageclass_forbidden_is_unknown。
- [ ] 运行 python -m pytest tests/test_pvc_semantics.py -q。
- [ ] 实现按需关联和原因分类，旧 pending 列表保留，新增 lost_count/等待计数。
- [ ] 跑 PVC 与 profile 测试；断言关联 list 次数按运行而非 PVC 数量增长。
- [ ] 审查“等待”不会掩盖真正 Lost/消费者失败，提交。

## A4：保留已确认故障，明确 Node、Addon 与控制面能力

**文件与责任**

- 修改 k8s_inspector/checks.py：check_nodes、check_control_plane、check_kube_system_addons。
- 修改 k8s_inspector/kube.py：仅修本批所需 metrics 错误分类，不引入完整重试层。
- 新增 tests/test_failure_preservation.py，补 tests/test_checks.py。

**接口与约定**

- CheckResult.data 加 collection_errors 列表；阶段异常在该列表留痕，状态取已确认结果的最高严重性。
- 已有 FAIL + 后续采集失败仍 FAIL；全无有效数据为 WARN，summary 说明未知。
- capacity/allocatable 缺失或零不能计算为 0% 健康；metrics 不可用不等于 usage=0。
- API 健康与控制面 Pod Ready 分别检查；托管集群不可见组件声明能力未知/不适用。
- Addon 读取 Ready、期望与实际副本；暂不扩大到所有工作负载，完整规则归 C4/F1。

~~~python
def test_node_failure_survives_metrics_error(core, custom, cfg):
    core.nodes = [node("n1", ready=False)]
    custom.list_cluster_custom_object.side_effect = TimeoutError("metrics timed out")
    result = check_nodes(core, custom, cfg, include_metrics=True)
    assert result.status == Status.FAIL
    assert result.data["not_ready"] == ["n1"]
    assert result.data["collection_errors"]
~~~

还需 test_zero_allocatable_does_not_report_zero_usage、test_addon_running_but_unready_is_not_pass、test_managed_control_plane_absence_is_not_healthy、test_5m_never_requests_metrics；保留原 not_ready 列表字段。

- [ ] 写故障保留与能力边界用例。
- [ ] 运行 python -m pytest tests/test_failure_preservation.py -q。
- [ ] 按子阶段隔离异常，保留已收集证据；避免一个 metrics 项失败丢弃所有 Node 结果。
- [ ] 跑 checks/kube 相关测试；验证错误摘要无凭据泄漏。
- [ ] 审查严重性聚合和未知分支，提交。

## A5：原子刷新 latest，归档与清理可信

**文件与责任**

- 修改 k8s_inspector/report.py：JSON 原子写入 helper；CSV/HTML 同样采用完整文件替换。
- 修改 k8s_inspector/cli.py：latest、归档策略及 cleanup 统计分离。
- 新增 tests/test_report_persistence.py，修改现有报告/cleanup 测试。

**接口与约定**

- 新增 atomic_write_text(path: Path, text: str) -> None：目标目录同文件系统临时文件、flush、适当 fsync、os.replace；失败不破坏旧文件，清理临时文件。
- 保持 write_json/write_csv/write_html 返回 Path；latest/<profile>/report.json 每次运行更新。
- issue-only 只控制额外归档/扩展输出策略；恢复后的 PASS 仍刷新 latest。
- 归档目录使用 UTC 时间与唯一 run 标识，兼容既有 timestamp 清理识别；完整 run_id 由 C1 接入。
- cleanup 只计实际删除成功；加 dry-run，验证解析后目标仍在指定 root，排除符号链接/Windows reparse point；删除失败可见。

~~~python
def test_atomic_replace_failure_keeps_previous_report(tmp_path, monkeypatch):
    path = tmp_path / "report.json"
    path.write_text('{"status":"FAIL"}', encoding="utf-8")
    monkeypatch.setattr(os, "replace", failing_replace)
    with pytest.raises(OSError):
        atomic_write_text(path, '{"status":"PASS"}')
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "FAIL"

def test_issue_only_run_refreshes_latest_on_recovery(run_cli, tmp_path):
    run_cli(status=Status.FAIL, persist_on="issue", output_dir=tmp_path)
    run_cli(status=Status.PASS, persist_on="issue", output_dir=tmp_path)
    assert json.loads((tmp_path / "latest/5m/report.json").read_text())["summary"]["fail"] == 0
~~~

failing_replace 与 run_cli fixture 在本任务定义；run_cli 替换 load_kube/run_all，走实际 cmd_run。增加 test_cleanup_failure_is_not_counted、test_cleanup_dry_run_changes_nothing、test_cleanup_reparse_target_is_rejected。

- [ ] 写失败替换、恢复刷新和清理用例。
- [ ] 运行 python -m pytest tests/test_report_persistence.py -q。
- [ ] 实现原子文件发布与独立归档；先验证绝对路径，再执行任何递归删除。
- [ ] 跑 report/cleanup/CLI 回归；Windows 与容器 Linux 都验证替换行为。
- [ ] 记录归档目录迁移及权限要求，提交。

## A6：隔离输出故障与运行退出语义

**文件与责任**

- 修改 k8s_inspector/cli.py：每种报告、Pushgateway、直接通知独立执行。
- 新增 k8s_inspector/sinks.py：SinkResult 与输出编排，C5 沿用。
- 新增 tests/test_sink_isolation.py，补 CLI/metrics/notify 现有测试。

**接口与约定**

- SinkResult(name: str, success: bool, required: bool, error_kind: str | None, detail: str | None)；detail 必须脱敏。
- run_sinks(actions: dict[str, Callable[[], object]], required: frozenset[str]) -> tuple[SinkResult, ...]，逐项执行，单项失败不阻断其他项。
- strict 输出策略在配置中显式启用；必需输出失败时进程为 1；always0 覆盖进程退出码，SinkResult 与 run_success 仍失败。
- 业务 FAIL 与 sink 失败同时存在时，strict 的程序失败 1 优先，报告仍记录业务 FAIL；文档明确这项新增语义。保留非 strict 的旧业务退出约定。
- A 批次 sink 状态可放兼容 data/run 日志，C1/C5 再正式入 Report v2；不为“所有 sink 状态”递归触发报告重写。

~~~python
def test_report_failure_does_not_block_metrics_or_notification():
    calls = []
    results = run_sinks(
        {"json": failing_write,
         "metrics": lambda: calls.append("metrics"),
         "notification": lambda: calls.append("notification")},
        required=frozenset({"json", "metrics"}),
    )
    assert calls == ["metrics", "notification"]
    assert [(x.name, x.success) for x in results] == [
        ("json", False), ("metrics", True), ("notification", True)]
~~~

增加 test_strict_required_sink_failure_returns_one、test_always0_keeps_failed_sink_observable、test_warn_fail_exit_codes_remain_compatible。fixture 明确 required 集合与 sink 顺序，不依赖真实网络。

- [ ] 写 sink 故障和退出码测试。
- [ ] 运行 python -m pytest tests/test_sink_isolation.py -q。
- [ ] 实现独立编排与脱敏结果；保证 metrics 能表达报告失败，不把它变成业务恢复。
- [ ] 运行 python -m pytest tests/ -q；通过后不重复扩展无关测试。
- [ ] 审查输出策略、README 兼容提示和 A 批次验收，提交。

## 批次完成与回退

- [ ] A1–A6 均有真实失败复现与通过记录；现有测试没有通过削弱断言来适配。
- [ ] 对照 G1：当前/历史、未知/健康、恢复刷新和输出隔离均成立。
- [ ] 回退使用版本基线；不要把恢复后的 latest 换回旧故障报告。新字段兼容旧读取端。
- [ ] 记录仍待 C 解决的执行质量、分页、完整 run_id 和 Prometheus 恢复语义。
