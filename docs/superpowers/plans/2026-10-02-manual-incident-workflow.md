# D：人工事件协作 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** 让重大事件有负责人、成员告警、协作群和可信结案依据，日常告警继续进入固定责任群。

**Architecture:** 在 B 的持久状态库中增加事件、成员、操作和审计表；人工管理接口调用事件服务。群创建是有独立授权和结果核对的外部操作，不能当成消息投递无限重试。

**Tech Stack:** Python、SQLite、现有 Feishu SDK、受保护 HTTP 管理接口、pytest。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；依赖 B1/B4/B7 与 C1/C4/C6。

## Global Constraints

- 建群默认关闭；critical/groupKey 不触发创建。人工可以绑定已有群，不强制另建群。
- 管理身份来自凭据，不能相信 JSON 内自报操作者。每次变更写入审计。
- 自动推断根因、自然语言合并、飞书按钮回调、自动解散群不在本批范围。
- resolved 通知、silence、inhibition、指标消失和消息发出均不是已验证业务恢复。
- 部分恢复不能结案；创建结果未知不盲目重试。迁移和备份沿用 B7。

## Review Focus

1. 一次通知包含恢复和仍 firing 的成员：D2/D4 的部分恢复测试。
2. 旧 startsAt 的 resolved 覆盖新发生周期：D2 的迟到事件测试。
3. 群创建成功但本地没记住 chat_id：D3 的未知结果核对测试。
4. 删除/重建同名资源被算作恢复：D4 的 UID 与覆盖验证。
5. 自报管理员、越权团队或重复升级突破限额：D5/D6 的权限和并发测试。

## D1：事件模型、持久状态与版本控制

**文件：** 新增 incident_models.py、incident_store.py、tests/test_incident_store.py；修改 B7 schema migrations。生产文件位于 k8s_inspector/。

**接口：**

- Incident、IncidentMember、Actor 使用设计第6节字段；IncidentState 为 OPEN/ACKNOWLEDGED/INVESTIGATING/RESOLVED/CLOSED/REOPENED/CANCELLED。
- Actor(principal_id: str, roles: frozenset[str])；团队授权关系由 D5 凭据配置提供，不从任意 body 传入。
- IncidentMember补充 member_id: str、incident_id: str、latest_status: str、latest_received_at: datetime、identity_quality: str；RecoveryDecision.pending_member_ids引用member_id。service fixture在D1仅为含store的简单容器，正式服务类在D3定义。
- IncidentStore.create(title: str, owner_team: str, owner: str, impact: str, actor: Actor, now: datetime) -> Incident。
- IncidentStore.get(incident_id: str) -> Incident。
- IncidentStore.transition(incident_id: str, expected_version: int, state: IncidentState, reason: str, actor: Actor, now: datetime) -> Incident。
- 表包括 incidents、incident_members、incident_operations、incident_audit、processed_inbox；状态变更和审计同一事务提交。version 用 compare-and-swap，不丢失并发编辑。
- create 本身不调用飞书；CLOSED 要有结论；CANCELLED 要有原因，不标记成健康。

~~~python
def test_creating_incident_does_not_create_chat(service, provider, actor, now):
    incident = service.store.create("API unavailable", "sre", "alice", "prod", actor, now)
    assert incident.state == IncidentState.OPEN
    assert incident.chat_id is None
    assert provider.create_count == 0

def test_stale_version_cannot_overwrite_incident(store, incident, actor, now):
    store.transition(incident.incident_id, incident.version,
                     IncidentState.ACKNOWLEDGED, "assigned", actor, now)
    with pytest.raises(VersionConflict):
        store.transition(incident.incident_id, incident.version,
                         IncidentState.CANCELLED, "mistake", actor, now)
~~~

本任务定义 service/provider/actor fixtures、VersionConflict 与 NotFound 异常；provider 是 fake，不发送外部请求。

- [ ] 写上列测试以及 test_transition_and_audit_commit_together。
- [ ] 运行 python -m pytest tests/test_incident_store.py -q，确认缺少事务/版本时失败。
- [ ] 实现模型、迁移和合法状态转换表。
- [ ] 再运行同文件测试及 B7 migration 回归，要求全部 PASS。
- [ ] 审查取消/关闭语义与备份兼容，提交或记录完整版本快照。

## D2：成员身份、发生周期、迟到通知与复开

**文件：** 新增 incident_policy.py；修改 incident_store.py；新增 tests/test_incident_members.py。

**接口：**

- IncidentStore.link_member(incident_id: str, source_id: str, alert: Alert, actor: Actor, now: datetime) -> IncidentMember：复用 B2 alert_cycle_key。
- IncidentStore.apply_batch(receipt_id: str, source_id: str, batch: AlertBatch, received_at: datetime) -> tuple[str,...]：返回受影响 incident IDs；事务记录 processed_inbox，重放不重复变更。
- IncidentStore.get_members(incident_id: str) -> tuple[IncidentMember,...]。
- 每个成员保留 source/fingerprint/startsAt/可选 UID/rule_id；groupKey 不作为事件身份。
- 同 fingerprint 的旧周期 resolved 不改变新周期 firing。同周期已 resolved 的迟到 firing 不仅凭接收时间认作新故障；新 startsAt 或可信新故障证据才开启新周期。
- 已关联逻辑成员的新故障在结案后30分钟内进入 REOPENED，episode+1；窗口外在固定群通知，由人工创建/关联新事件，不静默复用结案。
- startsAt 缺少、源截断或身份质量低时标记待核实，禁止自动结案。

~~~python
def test_old_resolved_does_not_clear_new_cycle(store, linked_member, now):
    store.apply_batch("new", "prod-am", batch("firing", starts_at="2026-10-02T01:00:00Z"), now)
    store.apply_batch("old", "prod-am", batch("resolved", starts_at="2026-10-02T00:00:00Z"), now)
    assert store.get_members(linked_member.incident_id)[-1].latest_status == "firing"
~~~

本任务填充D1定义的成员状态字段，历史记录不覆盖。另测 test_partial_resolved_keeps_active_member、test_inbox_replay_is_idempotent、test_reopen_within_30_minutes_increments_episode、test_missing_cycle_requires_review。

- [ ] 写周期/部分恢复/重放/复开测试。
- [ ] 运行 python -m pytest tests/test_incident_members.py -q。
- [ ] 实现成员投影与 inbox 消费 cursor，接收 ACK 后事件消费失败仍可重放。
- [ ] 再跑成员测试与 B2 身份测试，确认同一周期规则一致。
- [ ] 审查迟到事件不可排序时的保守策略，提交。

## D3：人工升级、绑定群与创建结果核对

**文件：** 新增 incident_service.py；修改 feishu_app.py 的创建超时结果；新增 tests/test_incident_escalation.py。

**接口：**

- IncidentService.escalate(incident_id: str, expected_version: int, actor: Actor, *, existing_chat_id: str | None, create_group: bool, request_id: str, now: datetime) -> EscalationResult。
- EscalationResult(operation_id: str, state: str, chat_id: str | None)。
- reconcile_group(operation_id: str, confirmed_chat_id: str, reason: str, actor: Actor, now: datetime) -> EscalationResult。
- group operation 状态 PREPARED/CALLING/CONFIRMED/RECONCILE_REQUIRED/REJECTED；request_id 唯一且持久保存。先事务登记，再调用外部，确认 chat_id 后事务绑定。
- 默认 create_group=False；绑定前验证机器人可访问目的地及团队配置。标题用 incident_id/episode，不能用告警分组键。
- 超时或进程在 CALLING 阶段退出后标 RECONCILE_REQUIRED；不复用 B4 消息 worker 自动重试创建。只有明确未发出请求的 PREPARED 可继续。

~~~python
def test_unknown_create_is_not_retried(service, provider, incident, actor, now):
    provider.create_then_timeout()
    first = service.escalate(incident.incident_id, incident.version, actor,
                             existing_chat_id=None, create_group=True,
                             request_id="op-1", now=now)
    again = service.escalate(incident.incident_id, incident.version, actor,
                             existing_chat_id=None, create_group=True,
                             request_id="op-1", now=now)
    assert first.state == again.state == "RECONCILE_REQUIRED"
    assert provider.create_count == 1
~~~

追加 test_existing_chat_binding_creates_no_group、test_duplicate_request_returns_same_operation、test_reconcile_requires_explicit_reason。群创建供应商幂等只有核实真实语义后才作为额外保护，不能删除未知结果人工核对路径。

- [ ] 写外部未知/重复/绑定测试。
- [ ] 运行 python -m pytest tests/test_incident_escalation.py -q。
- [ ] 实现持久操作状态，SDK 返回结构化创建结果，错误内容脱敏。
- [ ] 再跑升级与 B3 adapter 回归，明确消息和群创建的不同重试规则。
- [ ] 审查默认关闭、机器人权限和未知结果处理文档，提交。

## D4：恢复证据、观察窗口与结案

**文件：** 新增 recovery_evidence.py；修改 incident_policy.py；新增 tests/test_incident_recovery.py。

**接口：**

- RecoveryDecision(eligible: bool, pending_member_ids: tuple[str,...], reasons: tuple[str,...], observed_since: datetime | None)。
- evaluate_recovery(incident: Incident, members: tuple[IncidentMember,...], observations: tuple[RuleObservation,...], now: datetime, *, observation_seconds: int = 600) -> RecoveryDecision。
- 每个关联成员需要匹配 cluster/rule/scope/UID、完整且足够新鲜的 HEALTHY 证据；证据 freshness 沿用 C7 该规则/profile 的期限。连续有效健康观察跨越10分钟才 eligible，UNKNOWN/故障中断窗口。
- 资源删除/新 UID、源截断、未见指标不构成同资源恢复；可由人工记录替代工作负载证据与处置结论，保留“人工核验”标识。
- Alertmanager resolved 仅更新通知状态；其他来源告警缺少巡检规则映射时要求显式人工核验，不能套用无关巡检 PASS。
- resolve/close 操作仍由授权人执行并附原因，禁止自动把所有 resolved 的事件直接 CLOSED。

~~~python
def test_partial_recovery_is_not_eligible(incident, two_members, healthy_one, now):
    result = evaluate_recovery(incident, two_members, healthy_one, now)
    assert result.eligible is False
    assert len(result.pending_member_ids) == 1

def test_new_uid_does_not_prove_old_member_recovery(incident, old_member, healthy_new_uid, now):
    result = evaluate_recovery(incident, (old_member,), healthy_new_uid, now)
    assert result.eligible is False
~~~

追加 test_unknown_interrupts_observation_window、test_ten_minutes_of_valid_health_allows_resolution、test_stale_evidence_blocks_resolution、test_silence_and_truncation_do_not_resolve。

- [ ] 写部分恢复/UID/窗口/失明测试。
- [ ] 运行 python -m pytest tests/test_incident_recovery.py -q。
- [ ] 实现按成员验证与持久观察窗口；错误不给健康结论。
- [ ] 跑恢复与 C6 指标质量测试，确认 FAIL→ERROR 仍待验证。
- [ ] 审查人工核验记录不会伪装成自动健康，提交。

## D5：受保护管理 CLI、角色、审计与建群预算

**文件：** 新增 incident_api.py、incident_cli.py；新增 tests/test_incident_auth.py。

**接口：**

- authenticate_actor(headers: Mapping[str,str], credentials: tuple[AdminCredential,...]) -> Actor；AdminCredential 定义 principal/token/roles/allowed_teams，Secret 注入。
- authorize(actor: Actor, action: str, owner_team: str, credentials: tuple[AdminCredential,...]) -> None；角色 reader/operator/incident_admin，创建群另需 group_creator，团队权限按principal_id从可信credentials查找。
- CLI 子命令 create/show/link/ack/assign/escalate/reconcile/resolve/close/cancel；每个变更传 request_id 与 expected_version。
- 管理 API 与告警入口分别鉴权，不共用 AM token；默认仅集群内可达，TLS 在受信代理终止或应用提供。
- 创建预算每小时2个、活跃事件群5个，事务内检查/预留；未知 CALLING 仍占预算。触顶可绑定已有群，不无限再建。
- 每次变更保留 actor/action/reason/time/old-version/new-version；日志不含 token 和隐私成员列表。

~~~python
def test_payload_cannot_grant_admin(api, reader_token):
    reply = api.post("/incidents", token=reader_token,
                     json={"actor": "admin", "roles": ["incident_admin"]})
    assert reply.status_code == 403

def test_third_group_in_one_hour_is_rejected(service, group_creator, now):
    create_two_confirmed_operations(service, group_creator, now)
    with pytest.raises(GroupBudgetExceeded):
        create_next_operation(service, group_creator, now)
~~~

另测 test_cross_team_mutation_is_denied、test_unknown_creation_consumes_budget、test_budget_check_is_atomic、test_duplicate_request_does_not_consume_second_budget。

- [ ] 写身份/团队/预算/重复测试。
- [ ] 运行 python -m pytest tests/test_incident_auth.py -q。
- [ ] 实现 API/CLI、认证、授权和审计；先提供帮助与 dry-run，dry-run 不发飞书请求。
- [ ] 跑权限与全部事件测试；通过 CLI→API 路径验证，不能只测内部函数。
- [ ] 审查机器人创建权限只有启用事件创建时需要，提交。

## D6：事件闭环、恢复与运营验收

**文件：** 新增 tests/test_incident_workflow.py、docs/runbooks/incidents.md；修改事件指标与部署可选配置。

**验收契约：** 固定群通知始终存在；一个事件可关联多告警，但不是所有同群告警都归同事故；备份恢复包括事件、操作与审计；活跃/待核对/待验证数量为低基数指标。

~~~python
def test_end_to_end_partial_recovery_never_closes(workflow):
    incident = workflow.open_and_link_two_alerts()
    workflow.bind_existing_chat(incident)
    workflow.resolve_first_alert()
    assert workflow.incident_state(incident) != IncidentState.CLOSED
    workflow.provide_complete_health_for_both(minutes=10)
    workflow.operator_resolve_and_close(incident, conclusion="verified")
    assert workflow.incident_state(incident) == IncidentState.CLOSED
~~~

workflow fixture 使用真实 SQLite、本地 HTTP 和 fake provider。另测 test_restore_preserves_reconcile_required、test_reopened_episode_preserves_history、test_fixed_notifications_continue_when_group_budget_full。

- [ ] 写上述闭环和备份恢复测试，运行 python -m pytest tests/test_incident_workflow.py -q。
- [ ] 接通 B inbox 消费、C 证据、管理接口、操作指标与手册。
- [ ] 运行全部事件测试，要求 PASS；按 E5 授权范围在专用群演练一次绑定、部分恢复、结案和复开。
- [ ] 记录未知建群结果核对、权限撤销和 backup/restore 操作；演练前创建仍关闭。
- [ ] 对照 G4 验收并提交；回退事件功能时保留数据和未完成操作，日常固定群继续工作。
