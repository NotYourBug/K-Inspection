# B：可靠告警交付 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** Alertmanager 请求可靠落库后再确认，通知可从重启恢复，某个责任群失败不会丢掉其他群的任务。

**Architecture:** 单实例 SQLite inbox/outbox + 单 worker；保留固定责任路由和同步兼容模式。异步接收、消息重试与人工建群是不同契约。HA 环境改共享事务数据库，不能并发挂同一 SQLite。

**Tech Stack:** Python stdlib sqlite3、HTTP server、pytest、lark-oapi、Prometheus。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；[总计划](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/plans/2026-10-02-remediation-roadmap.md)。

**2026-10-15实施补充：** B1–B7离线实现完成，审查发现的有序通知、A→B→A、严格app成功码、人工处置和备份目录持久性纳入一轮TDD修复。schema3为顺序/周期成员增加迁移；DeliveryTarget增加可选cycle_keys。连续重复规则以设计补充为准。实际PVC/Pod恢复、promtool/amtool/kustomize和供应商窗口仍待E2/E5，不能将离线完成当成生产G2通过。

## Global Constraints

- 默认固定群，不按 groupKey/alertname/critical 创建、邀请或重命名群。
- 同步 200 = 发送得到成功确认；异步 202 = 本地持久提交，不能写成“飞书送达”。
- 至少一次语义；发送成功后本地崩溃存在重复边界。供应商 uuid 的窗口必须实测，不能凭参数存在宣称恰好一次。
- 数据库保存 payload/身份必要信息，不保存 app_secret/webhook token；destination_id 使用稳定配置标识，凭据在适配器侧解析。
- SQLite 只用经验证的本地/块持久卷，单实例、跨请求独立连接、显式事务与 busy timeout；网络盘不支持。
- actor 管理权限在 D5 定义；B6 的维护命令先为容器内本地运维，不能新增无鉴权公网管理入口。

## Review Focus

1. 两群仅一群成功后 Alertmanager 重投：B1/B4 的事务与独立 job 测试。
2. 输入顺序、滚动 endsAt、HA 副本、路由改变导致去重错误：B2 的身份和窗口测试。
3. 外部成功、本地 SENT 提交失败：B4 的 test_external_success_before_local_commit_is_recoverable。
4. DB 满、锁住或队列满仍返回成功：B5 的 test_storage_failure_never_acknowledges。
5. worker 已停而探针仍就绪、未处理 dead 被清理：B6 的就绪与保留测试。

## B1：持久接收与事务 outbox

**文件与责任**

- 新增 k8s_inspector/delivery_models.py：不可变 DTO。
- 新增 k8s_inspector/bridge_store.py：schema 版本、事务、inbox/outbox 查询与原子 accept。
- 新增 tests/test_bridge_store.py、tests/delivery_factories.py。

**接口**

~~~python
@dataclass(frozen=True)
class DeliveryTarget:
    destination_id: str
    content_hash: str
    payload: dict[str, object]

@dataclass(frozen=True)
class AcceptResult:
    receipt_id: str
    new_job_ids: tuple[str, ...]
    reused_job_ids: tuple[str, ...]

class DeliveryStore:
    def __init__(self, path: Path, *, max_backlog: int = 10000,
                 dedup_seconds: int = 300): ...
    def accept(self, source_id: str, batch: AlertBatch,
               targets: tuple[DeliveryTarget, ...],
               now: datetime) -> AcceptResult: ...
    def get_job(self, delivery_id: str) -> DeliveryJob: ...
    def list_jobs(self, *, state: str | None = None) -> tuple[DeliveryJob, ...]: ...
~~~

AlertBatch 复用 alert_routing.py 当前类型。DeliveryJob/DeliveryResult 按设计第 6 节定义；DeliveryResult 额外允许 retry_after_seconds: float | None。时间为 UTC，存储使用统一 UTC epoch 或 ISO 表示并在边界转回 datetime，不能混用 naive。

schema：inbox(receipt_id, source_id, received_at, normalized_payload, truncated)；outbox 记录完整 DeliveryJob；inbox_jobs 建立多对多关系；schema_version 单行记录迁移。相同内容的 pending job 可被多次请求复用，不能以 inbox 一行一 job 假设限制去重。唯一约束覆盖 source/destination/content_hash/generation；接受事务同时写全部目的地，达到 backlog 上限就整体拒绝。

delivery_factories 定义 batch(status="firing", fingerprint="f1", starts_at=固定UTC)、target(destination_id="team-a", content_hash="hash-a")，以及固定 now；store fixture 使用 tmp_path 的真实 sqlite 文件。

~~~python
def test_all_destinations_are_atomic(store, batch, targets, now, fail_second_insert):
    with pytest.raises(StorageUnavailable):
        store.accept("prod-am", batch, targets, now)
    assert store.list_jobs() == ()

def test_pending_jobs_survive_reopen(tmp_path, batch, targets, now):
    path = tmp_path / "bridge.db"
    first = DeliveryStore(path).accept("prod-am", batch, targets, now)
    reopened = DeliveryStore(path)
    assert {x.delivery_id for x in reopened.list_jobs()} == set(first.new_job_ids)
~~~

StorageUnavailable、BacklogFull、UnsupportedSchema 在 bridge_store.py 定义为不同异常；数据库原始错误不直接暴露 HTTP body。fail_second_insert 通过内部事务执行封装注入失败，不修改生产事务保证。

- [x] 写原子提交、重开恢复、并发两次 accept 只形成一组 jobs 的测试。
- [x] 运行 python -m pytest tests/test_bridge_store.py -q，确认没有事务/唯一约束时失败。
- [x] 实现 schema、accept、读取与容量计数；提交后才返回 AcceptResult。
- [x] 运行同文件测试；用真实 SQLite 并发连接检查锁与事务，不仅用 mock。
- [x] 审查多目的地部分失败和凭据存储，提交。

## B2：稳定身份、内容去重与周期提醒

**文件与责任**

- 新增 k8s_inspector/delivery_identity.py：规范化 hash、备用指纹、身份质量。
- 修改 bridge_store.py：窗口/generation 选择逻辑。
- 新增 tests/test_delivery_identity.py，补 test_bridge_store.py。

**接口**

- canonical_content_hash(batch: AlertBatch) -> str：排序 alerts；firing 不含 endsAt，resolved 含 endsAt；包含 labels、annotations、fingerprint、startsAt、truncated_alerts。group_key/receiver 不决定内容身份。
- alert_cycle_key(source_id: str, alert: Alert) -> tuple[str, str, str]：source、原指纹或规范 labels hash、starts_at。
- 请求凭据绑定逻辑 source_id；HA 两个副本共用同一 source_id，不能从不可信 body 覆盖它。
- 活动 PENDING/SENDING/RETRY_WAIT 同内容持续复用；最近 SENT 未过 300s 复用；超过窗口新 generation；DEAD 不因重复请求自动重置预算，需明确人工 requeue。
- 新目的地即使内容相同，也必须产生该目的地的新任务。

~~~python
def test_firing_rolling_end_does_not_change_hash(batch_with_end):
    a = batch_with_end(status="firing", ends_at="2026-10-02T01:00:00Z")
    b = batch_with_end(status="firing", ends_at="2026-10-02T01:05:00Z")
    assert canonical_content_hash(a) == canonical_content_hash(b)

def test_new_destination_is_not_suppressed(store, batch, now):
    first = store.accept("prod-am", batch, (target("team-a"),), now)
    second = store.accept("prod-am", batch, (target("team-b"),), now)
    assert set(first.new_job_ids).isdisjoint(second.new_job_ids)
    assert len(second.new_job_ids) == 1
~~~

追加 test_alert_order_is_irrelevant、test_annotation_change_is_new_content、test_resolved_end_changes_hash、test_sent_repeat_after_window_gets_new_generation、test_pending_repeat_reuses_job_after_window、test_ha_replicas_share_logical_source。SENT fixture 使用 B4 的 finish 接口，B2 初期以测试事务 helper 标记，不新增生产捷径。

- [x] 写 hash 与窗口边界测试，时间固定，无真实 sleep。
- [x] 运行 python -m pytest tests/test_delivery_identity.py tests/test_bridge_store.py -q。
- [x] 实现规范排序与 generation 事务；记录缺少指纹/startsAt 的质量标志。
- [x] 再运行以上测试；核对 299s/300s/301s 窗口约定：年龄 <300s 抑制，>=300s 可提醒。
- [x] 审查 route reload/source 迁移说明，提交。

## B3：统一供应商结果、超时与消息 uuid

**文件与责任**

- 新增 k8s_inspector/delivery_adapter.py。
- 修改 feishu_app.py 与 notify.py：超时、成功确认、错误分类。
- 新增 tests/test_delivery_adapter.py；保留现有 notify 测试。

**接口**

- DeliveryAdapter.send(job: DeliveryJob) -> DeliveryResult。
- FeishuAppClient.send_card_message(chat_id: str, card: dict[str, Any], message_uuid: str | None = None) -> str | None：旧双参数调用兼容；成功返回可用 message_id。
- webhook 使用现有成功判定，并设置真实 socket timeout=10s；SDK 设置可验证的 HTTP timeout，不能仅用线程 Future timeout 假装底层已取消。
- delivery_id 作为 app message uuid 的稳定输入；每个 generation 独立 UUID，重试不换 UUID。
- HTTP 429/5xx/网络临时失败可重试；无权限、无效目的地、非法内容为永久错误；传输超时可能结果不明。SDK 业务码建立基于选定版本官方文档的分类表，未知码保留且有界处理。
- DeliveryResult.success=True 必须得到供应商业务成功确认；retryable 与 result_unknown 分别表达，不用 HTTP 200 代替业务成功。

~~~python
def test_business_failure_on_http_200_is_not_success(adapter, job, provider):
    provider.reply(status=200, body={"code": 999, "msg": "rejected"})
    result = adapter.send(job)
    assert result.success is False
    assert result.error_code == "999"

def test_timeout_records_unknown_result(adapter, job, provider):
    provider.raise_timeout()
    result = adapter.send(job)
    assert (result.success, result.retryable, result.result_unknown) == (False, True, True)
~~~

provider fixture 是本地 fake server/SDK response，不发真实消息。增加 test_same_delivery_reuses_sdk_uuid、test_invalid_chat_is_permanent、test_credentials_are_redacted、test_provider_timeout_reaches_http_transport。

- [x] 写 adapter 结果与真实 timeout 配置断言。
- [x] 运行 python -m pytest tests/test_delivery_adapter.py tests/test_notify.py -q。
- [x] 实现类型化结果、可验证 timeout 和版本明确的 SDK UUID builder 调用。
- [x] 跑 adapter/notify/Bridge 旧测试，保持同步模式兼容。
- [x] 记录供应商 uuid 的未验证窗口为 E5 验收项，不宣称消除全部重复，提交。

## B4：投递 worker、重试、lease 与崩溃恢复

**文件与责任**

- 新增 k8s_inspector/delivery_worker.py。
- 修改 bridge_store.py：原子 claim、lease fencing、finish/requeue。
- 新增 tests/test_delivery_worker.py。

**接口**

- DeliveryStore.claim_due(now: datetime, lease_seconds: int = 60) -> DeliveryJob | None。
- DeliveryStore.finish(delivery_id: str, lease_token: str, result: DeliveryResult, now: datetime, *, retry_at: datetime | None = None) -> bool；过期 token 返回 False，不能改写新 lease。
- DeliveryWorker(store: DeliveryStore, adapter: DeliveryAdapter, clock: Callable[[], datetime], retry_delays: tuple[int, ...] = (5,15,45,120,300))。
- DeliveryWorker.run_once() -> bool：是否处理一个任务；worker 生命周期的 start()/stop(grace_seconds=15) 明確释放未发送任务、保留在途结果。
- claim 增加 attempt_count；首次 +5 次重试，失败总计6次后 DEAD。Retry-After 受配置总重试期限约束。
- lease 过期只恢复消息投递；D3 的建群操作不使用该重试机制。

~~~python
def test_old_lease_cannot_finish_new_claim(store, now, job_id):
    old = store.claim_due(now, lease_seconds=60)
    new = store.claim_due(now + timedelta(seconds=61), lease_seconds=60)
    assert new.delivery_id == old.delivery_id
    assert store.finish(job_id, old.lease_token, success_result(), now) is False
    assert store.get_job(job_id).lease_token == new.lease_token

def test_external_success_before_local_commit_is_recoverable(store, worker, provider, clock):
    store.fail_next_finish = True  # fixture 在事务执行层注入，非生产属性
    worker.run_once()
    assert provider.accepted_count == 1
    clock.advance(seconds=61)
    worker.run_once()
    assert store.list_jobs(state="SENT")
    assert provider.accepted_delivery_ids == {provider.last_delivery_id}
~~~

fake provider 分别模拟“支持 UUID 去重”和“webhook 不去重”，后一分支允许重复外部发送但要求稳定 delivery_id、未知结果记录与重试次数可追溯。增加 test_partial_destination_failure_does_not_block_other_job、test_permanent_error_goes_dead、test_sixth_failure_exhausts_budget、test_restart_claims_pending、test_shutdown_preserves_inflight。

- [x] 写 lease、部分失败、崩溃窗口和预算测试。
- [x] 运行 python -m pytest tests/test_delivery_worker.py -q。
- [x] 实现 claim/finish 与可注入 clock/jitter；不要在事务持锁期间调用飞书。
- [x] 跑 store/identity/adapter/worker 测试；确认没有真实时间等待。
- [x] 审查至少一次边界、在途退出和 dead 处理，提交。

## B5：鉴权、有限并发、背压与异步 HTTP 契约

**文件与责任**

- 新增 k8s_inspector/bridge_auth.py；修改 alertmanager_bridge.py。
- 修改 deploy/alertmanager/alertmanager.yml 与对应监控配置源。
- 新增 tests/test_bridge_ingress.py；继续现有 Bridge HTTP 测试。

**接口**

- authenticate(headers: Mapping[str, str], sources: tuple[SourceCredential, ...]) -> str；SourceCredential(source_id, token) 在 bridge_auth 定义，返回可信逻辑来源。使用恒定时间比较、Secret 来源、日志脱敏。
- handle_alert_request(source_id: str, payload: object, store: DeliveryStore, now: datetime) -> AcceptResult：normalize → route → B2 hash → 渲染持久 payload → B1 accept。
- --delivery-mode sync|async，默认先保留 sync，生产 overlay 在 G2 后显式切 async；async 必须指定状态库与鉴权凭据。
- 1MiB body、16 个在处理请求、10000 backlog；并发满/存储不可用为503。保留 Content-Length/Transfer-Encoding/读取超时边界。
- HTTP202 返回 receipt_id、新增/复用计数；不返回凭据或完整原始错误。不能 accept 事务前写响应。
- batch.truncated_alerts 非零仍可通知，但正文显示不完整；不能作为事件全部恢复证据。

~~~python
def test_storage_failure_never_acknowledges(http_bridge, authenticated_payload, store):
    store.fail_accept_with(StorageUnavailable("disk full"))
    response = http_bridge.post(authenticated_payload)
    assert response.status_code == 503
    assert store.list_jobs() == ()

def test_202_means_persisted_not_sent(http_bridge, authenticated_payload, store, provider):
    response = http_bridge.post(authenticated_payload)
    assert response.status_code == 202
    assert store.list_jobs(state="PENDING")
    assert provider.accepted_count == 0
~~~

增加 test_missing_token_is_401、test_body_cannot_override_source、test_concurrency_limit_returns_503、test_body_limit_is_413、test_route_jobs_commit_together。HTTP fake 用本地随机端口，不依赖外网。

- [x] 写鉴权/ACK/容量测试。
- [x] 运行 python -m pytest tests/test_bridge_ingress.py tests/test_alertmanager_bridge.py -q。
- [x] 实现受控 HTTP 入口与 async 模式；AM 通过其 http_config 传认证，按实际版本核验字段。
- [x] 再跑 HTTP 测试，确认旧 sync 成功/失败契约仍成立。
- [x] 记录切换配置、token 轮换与回退，提交。

## B6：就绪、自监控、保留与死信维护

**文件与责任**

- 新增 bridge_observability.py、bridge_admin.py；修改 Bridge 探针路由。
- 新增 tests/test_bridge_observability.py；新增 deploy/monitoring/bridge-rules.yaml。

**接口**

- DeliveryStore.stats(now: datetime) -> StoreStats：按 state 计数、最旧未完成年龄、最近 commit、unknown/dead 数。
- DeliveryStore.prune_sent(before: datetime) -> int：只删除超过30天且已完成的 inbox/outbox 关系；保留 unsent/unknown/未处置 dead。
- bridge_ready(store: DeliveryStore, worker_last_heartbeat: datetime | None, now: datetime) -> bool：可写检查、worker 心跳和接收容量；心跳超30s判不就绪。
- 本地命令 python -m k8s_inspector.bridge_admin inspect|requeue|backup；requeue 必须给 delivery_id、reason，追加维护日志并重新设置预算，不把原错误删掉。
- 指标只用低基数 source/destination/state/error_kind；不含 alert fingerprint、完整 chat_id、token、任意 annotations。

~~~python
def test_stopped_worker_is_not_ready(store, now):
    assert bridge_ready(store, now - timedelta(seconds=31), now) is False

def test_prune_retains_unhandled_dead_and_unknown(store, now):
    dead, unknown, sent = seed_old_jobs(store, days=31)
    store.prune_sent(now - timedelta(days=30))
    assert {x.delivery_id for x in store.list_jobs()} == {dead, unknown}
~~~

本任务定义 StoreStats、seed_old_jobs；对 SENT 且历史存在未知结果但未人工确认的任务，仍需保留审计事件，不能因最终 SENT 删除未知操作记录。指标异常通过独立接收渠道/值班监控接入，当前同一飞书链失效时不作为唯一兜底。

- [x] 写探针、保留和低基数测试。
- [x] 运行 python -m pytest tests/test_bridge_observability.py -q。
- [x] 实现 /healthz、/readyz、/metrics 与本地维护；不要把自监控递归写入自己的失败队列。
- [ ] 用规则测试验证 dead/backlog/worker/状态库异常，HTTP 测试验证不泄漏敏感信息。
- [x] 记录值班处理步骤、保留期限与“dead 不等于已处理”，提交。

## B7：持久部署、备份、schema 迁移与恢复演练

**文件与责任**

- 修改 deploy/alertmanager-feishu-bridge.yaml；新增 deploy/storage/bridge-state-pvc.yaml 示例。
- bridge_store.py 增加 migration/backup；新增 tests/test_bridge_migrations.py。
- 新增 docs/runbooks/bridge-delivery.md，E2 接入实际 overlay。

**接口与约定**

- DeliveryStore.backup(destination: Path) -> Path：SQLite backup API 或停写后一致快照，不能运行中只复制 db 丢掉 WAL。
- migrate(path: Path, target_version: int) -> None：迁移事务、先备份、拒绝比当前程序新的 schema；失败保留可恢复状态。
- Bridge replicas=1、Recreate 策略或能证明同一时刻只有一个 writer 的替代；挂载 /var/lib/inspector/bridge.db 与独立状态 PVC。
- 不自动导入 JsonStateStore 的 group_key→chat_id 为新 incident/route；历史 JSON 只提供只读导出，人工核对目的地。
- 恢复旧备份可能重投已经外部发送的消息，演练记录重复风险；RPO/RTO 由真实卷与备份演练给出，计划不预填“0丢失”。

~~~python
def test_backup_restore_preserves_pending_and_schema(store, tmp_path, job_id):
    backup = store.backup(tmp_path / "backup.db")
    restored = DeliveryStore(backup)
    assert restored.get_job(job_id).state == "PENDING"

def test_newer_schema_is_rejected(tmp_path):
    path = database_with_schema_version(tmp_path, 999)
    with pytest.raises(UnsupportedSchema):
        DeliveryStore(path)
~~~

- [x] 写备份/迁移/新版本拒绝测试，模拟迁移中断。
- [x] 运行 python -m pytest tests/test_bridge_migrations.py -q。
- [x] 实现一致快照与 schema 门槛；编写状态 PVC 和回退操作文档。
- [ ] 在测试环境执行 Pod 替换后 pending 恢复、备份到新卷恢复；没有真实存储时记为待验，不能用 mock 宣布生产通过。
- [x] 审查 G2 与部署前提，提交。回退 sync 前先保存并处置未完成 jobs，不能静默遗弃。

### 执行证据（2026-10-15）

离线最终343 passed，2项SDK上游DeprecationWarnings；独立审查121 passed、5项Important，作者单轮RED→GREEN修复，未复审。详见审计/13-可靠告警交付实施记录-2026-10-15.md、审计/14-可靠告警交付独立审查-2026-10-15.md 和审计/15-可靠告警交付执行台账-2026-10-15.md。

上面仍未勾选项是明确的发布验收门槛：规则fixtures已提供但promtool未运行；真实PVC/Pod替换/新卷恢复未演练。amtool/kustomize、独立监控接入、供应商UUID窗口及运行环境矩阵也仍需E2/E5，不宣称生产G2/G3通过。
