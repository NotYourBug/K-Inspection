# C：巡检模型与监控治理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** 让执行质量、业务故障、覆盖范围和恢复证据分别可见，降低 API 压力和重复告警。

**Architecture:** 采集快照 → 纯规则 → 兼容报告 → 独立输出；规则/profile 注册表统一调度与主要告警来源。独立 expected inventory 检测从未运行和监控丢失；持久有效观察防止 FAIL→ERROR 被当成恢复。

**Tech Stack:** Python dataclasses、Kubernetes SDK、pytest、Prometheus/Pushgateway、promtool。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；[总计划](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/plans/2026-10-02-remediation-roadmap.md)。

## Global Constraints

- 依赖 A 批次的语义修复；按一种规则迁移并验收，不同时替换全仓。
- 保留 CheckResult/Report 旧公开接口；新增 v2 不静默改变旧 summary。
- UNKNOWN 不是 HEALTHY；UNSUPPORTED 与 ERROR 分开；有部分故障证据仍保留 Finding。
- 5m 不采 Metrics API，其他频次范围保持已有约定；主要告警 profile 与能执行规则的 profiles 是不同字段。
- 详细资源 UID、Pod 名称、证据留报告；Prometheus 不用无限基数对象标签。
- MCP 自包含约定保持，F3 通过 fixtures 对齐，不在 C 批次强制导入巡检内部实现。

## Review Focus

1. 同名新 UID 继承旧故障/恢复身份：C1 的 test_resource_uid_changes_finding_identity。
2. 第二页失败或 deadline 到期仍声明覆盖完整：C2 的分页/期限测试。
3. 配置未知 profile 回落 full 造成 API 突增：C3 的 test_unknown_profile_is_rejected。
4. FAIL 后采集 ERROR 把故障计数写成0：C6 的 test_error_does_not_clear_last_valid_failure。
5. 指标从未出现/Pushgateway 被清空后所有告警消失：C7 的 inventory 缺失与质量规则测试。

## C1：模型、身份与向后兼容

**文件与责任**

- 修改 k8s_inspector/models.py；新增 observation_models.py，保持模型责任清楚。
- 新增 tests/test_observation_models.py，修改 tests/test_models.py。
- 新增 tests/observation_factories.py：C/D/F 共用数据输入夹具，不能让 MCP 生产代码依赖本包。

**接口**

ExecutionStatus、ObservationHealth、Severity、ResourceRef、Coverage、Finding、CheckExecution、RuleObservation、CheckEvaluation 使用设计第 6 节的精确字段。补充类型：

- ResourceRef 的 kind/name 必需 str，其余 namespace/uid/container/owner_uid/node 为 str | None。
- Finding 的 evidence 为 dict[str, object]，observed_at 为 UTC datetime；fingerprint 用 rule_id + cluster + namespace + resource UID/明确的弱身份 + container。cluster 在 make_finding 入参，不遗漏集群。
- RuleObservation.scope 为稳定字符串（cluster 或 namespace:<name>）；resource_uid: str | None，health 为 ObservationHealth，complete: bool，run_id: str。
- make_finding(rule_id: str, cluster_id: str, resource: ResourceRef, severity: Severity, reason: str, summary: str, evidence: dict[str, object], observed_at: datetime) -> Finding。
- to_legacy_result(evaluation: CheckEvaluation) -> CheckResult：有 FAIL finding 就 FAIL；无finding但 ERROR/PARTIAL 就 WARN；UNSUPPORTED/SKIPPED 就 SKIP；完整健康才 PASS。
- Report 保留原前三字段；新增默认可空 schema_version/run_id/profile/executions/findings/observations/sink_results。旧构造序列化维持 v1；新 runner 显式 schema_version=2。
- JSON 输出枚举值、UTC ISO 时间；无时区输入拒绝作为可信观察。

~~~python
def test_resource_uid_changes_finding_identity(resource, now):
    old = make_finding("pods.ready", "prod", replace(resource, uid="old"),
                       Severity.FAIL, "PodNotReady", "not ready", {}, now)
    new = make_finding("pods.ready", "prod", replace(resource, uid="new"),
                       Severity.FAIL, "PodNotReady", "not ready", {}, now)
    assert old.fingerprint != new.fingerprint

def test_legacy_report_root_fields_are_preserved():
    report = Report("2026-10-02T00:00:00+00:00", "prod", [])
    assert set(report.to_dict()) == {"generated_at", "cluster", "summary", "results"}
~~~

增加 test_partial_with_failure_keeps_fail、test_unknown_coverage_denominator_stays_null、test_naive_observation_time_is_rejected。observation_factories 明确提供 healthy_observation、failed_evaluation、error_evaluation 与真实 UID/scope/run_id。

- [ ] 写身份、序列化与兼容测试。
- [ ] 运行 python -m pytest tests/test_models.py tests/test_observation_models.py -q。
- [ ] 实现模型/转换，不把 SDK 对象直接放入 evidence。
- [ ] 再跑模型测试，验证旧 JSON 消费者 fixtures 不变。
- [ ] 审查字段类型与 B/D 引用一致，提交。

## C2：有期限的采集、分页、错误分类与快照复用

**文件与责任**

- 新增 collector.py、snapshot.py；修改 kube.py 的请求包装。
- 新增 tests/test_collector.py、tests/test_snapshot.py。

**接口**

- Collection[T](items: tuple[T,...], execution_status: ExecutionStatus, observed_at: datetime, complete: bool, error_kind: str | None, reason: str | None)；使用 Python 3.9 支持的 Generic，不使用仅新版本可用类型语法。
- RunSnapshot(run_id: str, cluster_id: str, started_at: datetime, collections: Mapping[str, Collection[object]])。
- KubeCollector(context: KubeContext, deadline: float, clock: Callable[[], float])；deadline 为 monotonic 绝对期限。
- collect_list(key: str, call: Callable[..., object], **kwargs) -> Collection[object]：SDK limit/continue 分页，_request_timeout=(3, min(10, remaining))；分页各页共享 deadline。
- snapshot(required: frozenset[str], run_id: str, cluster_id: str) -> RunSnapshot：Node/Pod/PVC/StorageClass/Event/metrics 同次运行按 key 只采一次。
- 403=FORBIDDEN、404/API 不支持=UNSUPPORTED、429/5xx/timeout=临时错误；最多2次重试；410 continue 失效可在剩余期限内重新开始一次，不能将不完整页拼成完整。
- 捕获错误要保留已拿到的部分 items，但 complete=False。空列表且成功 complete=True 与失败空列表有不同语义。

~~~python
def test_second_page_error_is_partial(collector, sdk_list):
    sdk_list.pages([page(["n1"], next_token="p2"), forbidden()])
    result = collector.collect_list("nodes", sdk_list)
    assert (result.complete, result.execution_status) == (False, ExecutionStatus.PARTIAL)
    assert result.items == ("n1",)

def test_snapshot_reuses_node_metrics(collector, sdk_metrics):
    collector.snapshot(frozenset({"nodes", "node_metrics"}), "run-1", "prod")
    assert sdk_metrics.call_count == 1
~~~

page/forbidden/SDK mock factory 在本任务定义。增加 test_deadline_stops_next_page、test_forbidden_is_not_retried、test_410_does_not_mix_snapshots、test_5m_does_not_collect_metrics。不要只做线程超时测试，断言底层 timeout 参数和剩余期限。

- [ ] 写分页、期限、分类、复用测试。
- [ ] 运行 python -m pytest tests/test_collector.py tests/test_snapshot.py -q。
- [ ] 实现采集层并先接一条 Node 规则，其他保持旧路径到 C4。
- [ ] 跑 collector 与 kube 回归，断言请求数随页数而非规则数增长。
- [ ] 审查 deadline 和部分覆盖声明，提交。

## C3：规则/profile 注册表、运行编排与配置优先级

**文件与责任**

- 新增 rule_catalog.py、runner.py；修改 config.py、cli.py、checks.py 的兼容入口。
- 新增 tests/test_runner.py、tests/test_effective_config.py。

**接口**

- RuleSpec(rule_id: str, profiles: frozenset[str], primary_profile: str, collection_keys: frozenset[str], evaluate: Callable[[RunSnapshot, InspectorConfig], CheckEvaluation])。
- ProfileSpec(name: str, deadline_seconds: int, schedule: str | None)；PROFILES: Mapping[str, ProfileSpec]。evaluate由域函数结果中按rule_id取一条，catalog包装器确保恰好匹配，未匹配为执行错误。
- PROFILES 是5m/10m/hourly/daily/full五个profile的有限注册表；deadline：240/480/900/1800/1800 秒；既有合法 alias 显式映射，未知值报 ValueError，不回落 full。
- run_inspection(context: KubeContext, cfg: InspectorConfig, profile: str, *, cluster_id: str, now: datetime, monotonic: Callable[[], float]) -> Report。
- resolve_config(file_config: dict[str, object], env: Mapping[str, str], cli_overrides: Mapping[str, object]) -> InspectorConfig；仅显式 CLI 覆盖，argparse 默认值不能压掉文件/env。
- 新增 cluster_id/profile/timeouts/deadlines/output_policy 的明确 schema；A1 三个宽限字段沿用。bridge 与 MCP 有各自配置边界，不塞进一个无限大的 InspectorConfig。
- run_all 原签名保留为兼容 facade，调用新 runner 的检查部分返回 list[CheckResult]。新 CLI 使用完整 Report。

~~~python
def test_unknown_profile_is_rejected(context, cfg, now):
    with pytest.raises(ValueError, match="profile"):
        run_inspection(context, cfg, "5min-typo", cluster_id="prod",
                       now=now, monotonic=fake_monotonic)

def test_cli_default_does_not_override_file_config():
    cfg = resolve_config({"cluster_id": "file-cluster"},
                         {"INSPECTOR_CLUSTER": "env-cluster"}, {})
    assert cfg.cluster_id == "env-cluster"
~~~

增加 test_explicit_cli_wins、test_run_deadline_marks_remaining_rules_unknown、test_known_failure_survives_later_rule_error、test_profile_collections_match_existing_frequency_contract。规则主 profile 初始：nodes.ready/pods.current/pvc.binding=5m；control.api/control.components=10m；nodes.utilization/nodes.pressure=hourly；events.warning/addons.ready=daily。full 输出所有规则但不重复主告警。

- [ ] 写配置优先级/profile/截止期限测试。
- [ ] 运行 python -m pytest tests/test_runner.py tests/test_effective_config.py -q。
- [ ] 实现注册表、编排、脱敏有效配置输出与兼容 facade。
- [ ] 跑 CLI/checks 兼容测试，核对 profile 的 API 预算。
- [ ] 审查注册表是唯一来源，E3/C7 消费同一数据，提交。

## C4：逐领域迁移纯规则与可信观察

**文件与责任**

- 新增 rules_pods.py、rules_nodes.py、rules_storage.py、rules_control.py、rules_events.py。
- 修改 checks.py：保留 facade，去掉已迁移的重复业务逻辑。
- 新增 tests/test_rule_observations.py；复用 A2–A4 fixtures。

**接口**

- 每个模块 evaluate_<domain>(snapshot: RunSnapshot, cfg: InspectorConfig) -> tuple[CheckEvaluation, ...]，按 RuleSpec 的 rule_id 拆结果。
- collection 不完整时可产出已确认 Finding，aggregate observation 必须 UNKNOWN/complete=False；不能把未见到的剩余对象推断健康。
- 同时输出 scope aggregate 与已有故障资源的 UID observation；资源缺失只能说明在完整清单中未见，不能直接为删除前故障确认恢复。
- pods.current 包括 Ready/init/当前失败；历史重启/OOM 独立规则，不能混入当前失效计数。
- events.warning 使用 observed time window、Event series/count 增量与有效时间来源；旧 Event 不因仍留在 API 重复触发当前故障。无可靠事件时间为质量不足。
- control.api/组件可见性分别表达；Addons 根据 Ready 和真实期望副本，不用 Running 代替健康。

~~~python
def test_partial_collection_can_fail_but_cannot_prove_recovery(snapshot, cfg):
    result = evaluate_nodes(snapshot.with_partial_notready_node(), cfg)[0]
    assert any(f.severity == Severity.FAIL for f in result.findings)
    aggregate = next(x for x in result.observations if x.resource_uid is None)
    assert (aggregate.health, aggregate.complete) == (ObservationHealth.UNKNOWN, False)

def test_old_event_is_not_current_warning(event_snapshot, cfg):
    result = evaluate_events(event_snapshot(age_seconds=86400), cfg)[0]
    assert not result.findings
~~~

增加 test_historical_oom_is_separate_rule、test_managed_component_absence_is_unsupported、test_addon_requires_ready_replicas、test_new_uid_does_not_resolve_old_resource。先实现 Node，再 Pod/PVC，最后 control/events；每域一次独立回归，不机械搬动全部函数。

- [ ] 写当前域 failing test 并运行 python -m pytest tests/test_rule_observations.py -q。
- [ ] 实现该域纯规则并接 catalog，删除该域重复判断。
- [ ] 跑该域 A 批次 fixtures 与兼容报告测试，逐域重复以上两步。
- [ ] 完成所有既有域后跑 python -m pytest tests/ -q。
- [ ] 审查纯规则无 SDK/文件/网络调用，提交并标记 F1 尚未扩展的覆盖项。

## C5：Report v2、证据展示、run_id 与输出结果

**文件与责任**

- 修改 report.py、models.py、sinks.py、cli.py；新增 tests/test_report_v2.py。
- 更新 README 的 schema 与产物布局说明。

**接口与约定**

- write_json/write_csv/write_html 消费 Report v2，保留旧字段；HTML 展示执行质量、采集时间、覆盖、当前故障/历史证据与对象链接。
- run_id 使用 UUID/ULID 的一种稳定唯一格式，目录含 run_id；同秒运行不覆盖 archive。
- report 的 sink_results 描述当次已知结果；报告自身写入结果另外保留 manifest.json/日志，避免循环“重写报告以记录写报告成功”。
- write_run_manifest(report: Report, sink_results: tuple[SinkResult,...], out_dir: Path) -> Path；该写入也采用 A5 atomic helper。
- 快照 attachment 验证 schema、cluster/profile、生成时间与 scope；现阶段不能证明 team/namespace 匹配就不附带对象细节，避免跨团队暴露。
- CSV 为每个 Finding 一行，并另提供 execution 字段；旧 overview 列兼容，通过 schema 标识新列。

~~~python
def test_same_second_archives_have_distinct_run_ids(run_reports, tmp_path):
    paths = run_reports(now="2026-10-02T00:00:00Z", count=2, output=tmp_path)
    assert len({p.parent for p in paths}) == 2

def test_html_escapes_untrusted_evidence(report_with_script, tmp_path):
    html = write_html(report_with_script, tmp_path).read_text()
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
~~~

增加 test_v2_preserves_v1_root_fields、test_unknown_is_visible_in_html、test_snapshot_scope_mismatch_omits_details、test_manifest_failure_does_not_overwrite_report。

- [ ] 写 v2、转义、作用域、run_id 与输出 manifest 测试。
- [ ] 运行 python -m pytest tests/test_report_v2.py -q。
- [ ] 实现输出与布局，迁移已有 latest 读取端并记录 schema 兼容。
- [ ] 跑 report/sink/Bridge 快照测试。
- [ ] 审查敏感证据与归档同秒冲突，提交。

## C6：指标区分未知，持久保留最后有效故障

**文件与责任**

- 修改 metrics.py；新增 observation_state.py；新增 tests/test_metrics_quality.py。
- 修改 runner/sinks 的状态读取与提交顺序。

**接口**

- ValidRuleState(rule_id: str, cluster_id: str, profile: str, scope: str, health: ObservationHealth, failure_count: int, observed_at: datetime, run_id: str) 在 observation_state.py 定义。
- ObservationStateStore(path: Path).update(report: Report) -> tuple[ValidRuleState,...]：只以完整有效 aggregate observation 更新，ERROR/UNKNOWN 不覆盖；同 profile 输出目录持久保存，使用 A5 原子替换与运行互斥，不能在 ephemeral /tmp 假装跨 CronJob 持久。
- populate_registry(report: Report, profile: str, registry: CollectorRegistry, *, last_valid: tuple[ValidRuleState,...] = ()) -> None，旧调用兼容。
- 新指标：run_success、collection_complete、rule_observation_valid、last_valid_observation_timestamp_seconds、last_valid_failures、sink_success；保留旧指标但新规则仅消费有效语义。
- last_valid_failures=0 只来自有效完整健康观察；未知且无历史则不输出该样本。未知且历史 FAIL 则保留失败计数、旧有效时间，同时输出 invalid。
- 运行结束时间与有效采集时间分开；always0 不影响 run_success。不要用 summary PASS 计数推断采集成功。

~~~python
def test_error_does_not_clear_last_valid_failure(state_store, fail_report, error_report, registry):
    state_store.update(fail_report)
    last_valid = state_store.update(error_report)
    populate_registry(error_report, "5m", registry, last_valid=last_valid)
    assert sample(registry, "inspector_last_valid_failures") == 1
    assert sample(registry, "inspector_rule_observation_valid") == 0

def test_valid_healthy_observation_clears_previous_failure(state_store, fail_report, pass_report):
    state_store.update(fail_report)
    current = state_store.update(pass_report)
    assert current[0].health == ObservationHealth.HEALTHY
    assert current[0].failure_count == 0
~~~

sample helper 明确按 cluster/profile/rule_id/scope 选样本。增加 test_unknown_without_history_has_no_healthy_sample、test_state_loss_is_visible、test_metrics_labels_have_bounded_cardinality、test_always0_still_exports_run_failure。历史状态无法读取时报告质量故障，不能自造0。

- [ ] 写 FAIL→ERROR→PASS 与状态丢失测试。
- [ ] 运行 python -m pytest tests/test_metrics_quality.py -q。
- [ ] 实现 last-valid store 与指标，状态提交失败属于必需 sink 错误；依赖 profile CronJob 禁止并发且验证互斥。
- [ ] 跑 metrics/runner/sink 测试；核对 Pushgateway 不残留旧 label 集。
- [ ] 记录指标迁移、state 卷与清理策略，提交。

## C7：独立目标清单、单一告警来源与规则测试

**文件与责任**

- 新增 expected_targets.py、tools/render_monitoring.py、deploy/monitoring/expected-targets.example.yaml。
- 修改 deploy Prometheus rules/Alertmanager snippet，生成到明确目录。
- 新增 tests/test_monitoring_catalog.py、tests/prometheus/inspection-rules.test.yaml。

**接口**

- ExpectedTarget(cluster_id: str, profile: str, max_run_age_seconds: int, rule_ids: tuple[str,...])；来自配置/deploy，与运行时上报独立。
- load_expected_targets(path: Path) -> tuple[ExpectedTarget,...]；export_inventory(targets) 的进程通过只读 ConfigMap 暴露固定 expected 指标，不与巡检 CronJob/Pushgateway 同进程。
- render_monitoring(catalog: tuple[RuleSpec,...], targets: tuple[ExpectedTarget,...], output: Path) -> tuple[Path,...]；rule_catalog 是主 profile/规则映射唯一来源，CI 验证生成文件没有漂移。
- expected inventory 服务本身由 Prometheus up 与独立健康告警覆盖；不以“expected 指标消失”让 missing 检测静默。
- expected 对 observed 做差集，检测从未运行；last_run 检测过期；有效时间/complete 检测失明；Pushgateway up 检测链路。
- 每条业务 rule 仅主要 profile 产告警，退出 HasFail/CheckFail/计数同义重复告警；保留质量告警和必要层级抑制，质量不足不能发送“业务恢复已验证”。

~~~python
def test_rule_has_one_primary_notification_profile(catalog):
    for rule in catalog:
        assert rule.primary_profile in rule.profiles
        assert rendered_business_alert_profiles(rule.rule_id) == {rule.primary_profile}

def test_monitoring_generation_is_reproducible(catalog, targets, tmp_path):
    a = render_monitoring(catalog, targets, tmp_path / "a")
    b = render_monitoring(catalog, targets, tmp_path / "b")
    assert [p.read_text() for p in a] == [p.read_text() for p in b]
~~~

promtool fixtures 必须验证：expected 存在但 run 不存在→TargetNeverRan；Pushgateway 清空→TelemetryMissing；FAIL→ERROR→业务故障继续/质量失明可见；FAIL→完整健康→故障解除；旧有效失败过期→质量报警并保留事件待验证；hourly/full 不重复5m业务告警。样本时刻、for 时长与预计 alerts 写在 YAML，不能仅检查 PromQL 语法。

- [ ] 写 catalog 单源与 promtool 时序 fixtures。
- [ ] 运行 python -m pytest tests/test_monitoring_catalog.py -q；运行 promtool test rules tests/prometheus/inspection-rules.test.yaml。
- [ ] 实现 inventory/export/render，明确实际部署版本兼容。
- [ ] 运行 promtool check rules 与差异校验；工具缺失时保留待验，不用 YAML 解析代替。
- [ ] 审查旧 grouping key 清理与新旧规则切换：新规则 shadow，通过后停旧规则，再清理旧指标；提交。

## 批次完成与回退

- [ ] C1–C7 完成，执行质量与业务状态在 JSON/HTML/metrics 中一致。
- [ ] 验证 FAIL→ERROR、从未运行、Pushgateway 清空、完整恢复，不能只验正常样例。
- [ ] 为 D4 输出带 UID/scope/run_id 的可信证据；不得把指标 absent 当作 resolved。
- [ ] 回退新规则时保留 observation state 和 v2 报告；并行 shadow 禁止双倍正式通知。
