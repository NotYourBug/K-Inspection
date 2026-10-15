# Bridge 可靠投递运维手册

本批次提供离线实现。默认 `sync`；启用 `async` 前须完成 G2/E2/E5：实际持久卷、镜像、有效配置、独立监控接收器、真实飞书契约及恢复演练。当前仓库未部署到集群，也未发送真实通知。

## 接口语义和存储前提

- sync 的 HTTP 200 表示供应商业务成功已确认；失败返回500。async 的202只表示同一事务已持久接收全部目的地，不表示已发到飞书。
- async 必须设置状态库和来源凭据。无效输入400、无鉴权/错误凭据401、超限413、容量/数据库/并发不足503；传输中断也可能没有可读响应，上游应按失败重试。
- 使用单实例、一个worker、`replicas: 1`、`Recreate` 和经过断电/锁/持久性验证的本地或块存储文件系统。状态路径 `/var/lib/inspector/bridge.db`，独立于报告PVC。**不支持NFS、RWX网络文件系统或多个服务实例共享SQLite**。HA改共享事务数据库。
- `deploy/storage/bridge-state-pvc.yaml` 中 `validated-block-storage` 是待替换占位符。可选组件 `deploy/components/bridge-async` 不在默认base中，禁止未经验证直接启用。
- 应用凭据、webhook URL及入口token只在Secret/env/适配器侧，数据库不存配置凭据。告警正文可能包含业务信息，数据库和备份仍需限制本地访问及加密存储。

## 本地启用与固定群路由

Secret挂载文件 `sources.json` 内容为数组，每条含 `source_id` 和 `token`。token使用随机生成的至少32字符ASCII值，不要把真实值提交到仓库：

```json
[{"source_id":"prod-am","token":"REPLACE_WITH_A_RANDOM_32_OR_MORE_CHARACTER_TOKEN"}]
```

同一Secret的 `token` 文件只包含相同的token，供Alertmanager读取。以受保护文件创建Secret：

```bash
kubectl -n kube-system create secret generic inspector-bridge-auth --from-file=sources.json --from-file=token
```

token文件末尾换行和Alertmanager读取行为须在E2验证；建议文件无末尾换行。Bridge读取JSON字符串原值。HA Alertmanager副本共用逻辑 `source_id`；不要把副本名当作来源。也可用 `BRIDGE_SOURCE_ID`/`BRIDGE_TOKEN` 配置单条凭据，不能两种来源混用产生含糊配置；文件优先。

```bash
python -m k8s_inspector.alertmanager_bridge --mode app --feishu-chat-id oc_existing \
  --delivery-mode async --state-db /var/lib/inspector/bridge.db \
  --sources-file /etc/bridge-auth/sources.json
```

`FEISHU_APP_ID`/`FEISHU_APP_SECRET`按原方式提供。webhook模式用 `FEISHU_WEBHOOK`；`--webhook-destination-id` 是稳定的配置标识，旋转同群凭据保持标识，改接收群须用新标识并先处置旧任务。

app目的地ID由现有chat_id稳定hash生成，非秘密chat_id和已渲染卡片随任务持久化。删改路由不会取消已接收任务；重启仍投递到原责任群。身份质量缺少fingerprint/startsAt时记入payload。groupKey不决定群，也不产生自动建群、邀请或重命名操作。旧JsonStateStore映射不自动导入；仅可只读核对历史群。

## sync → async 切换与token轮换

1. 先完成存储与外部契约验收、备份、配置渲染，并准备独立兜底接收渠道。保存有效镜像/配置及回退记录。
2. **先给Alertmanager挂载Secret并启用HTTP Bearer配置**，此时Bridge仍sync且未强制鉴权，原入口允许额外Authorization头。核对token文件和sources.json一致，再reload/restart并验证通知成功。
3. 再把Bridge切为async，使用同一Secret。Recreate间隙由Alertmanager按传输错误/503重试。检查401为0、202对应receipt可查询、worker送达、readyz和独立监控正常。不能反过来先强制鉴权导致旧请求401而被上游放弃。
4. 轮换时先在sources.json中让同一source_id同时接受旧、新两个不同token，重启Bridge生效；再更换Alertmanager token并验证；最后移除旧token并重启。Secret投射和环境变量的更新时点不同，不能同时盲切。token不在日志/指标打印。

Alertmanager v0.27.0的 [`http_config.authorization.credentials_file` 文档](https://raw.githubusercontent.com/prometheus/alertmanager/v0.27.0/docs/configuration.md) 已核对；实际 `amtool check-config`/有效Secret挂载仍待E2。

## 重试、结果不明与死信

每个目的地独立任务。相同内容的活动/DEAD任务持续复用；SENT后年龄<300秒复用，>=300秒允许新generation。路由改变到新目的地产生新任务。最大backlog10000，含未处置DEAD；重复请求复用不占新job额度。

lease60秒，provider每次HTTP连接/读取超时10秒；获取token和发消息可有两次HTTP操作。首次加5次重试，延迟5/15/45/120/300秒加0–10%抖动，尊重Retry-After但不越过首次claim起900秒的持久预算。重启不重置预算；尚未首次尝试的排队时间不计入预算。永久错误立即DEAD，其他错误有界重试。

外部成功、本地SENT提交失败或超时后可能重复发送。app重试使用同一UUID，webhook正文携带delivery_id；**不承诺恰好一次**。真实UUID去重窗口待E5实测，见[供应商证据与分类](../reference/feishu-delivery-errors.md)。lease过期会留下未知事件；最终SENT仍保留这类证据。

在容器内以受控运维权限执行，管理接口不暴露HTTP：

```bash
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db inspect
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db inspect --delivery-id DELIVERY_UUID --include-payload
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db requeue --delivery-id DELIVERY_UUID --reason '已核对群成员权限并修复'
```

先核对目的地、错误码、外部message_id和操作历史，再修复凭据/权限/内容。只有DEAD可人工重投，必须说明原因；重置尝试预算但保留旧错误和每次操作事件，沿用原UUID。DEAD不等于已处理。未知结果需人工核对供应商记录，不能直接删库或把群创建也套用此重试机制。

## 监控和保留

`/healthz`仅存活；`/readyz`检查DB可写、worker心跳<=30秒且未满容量；`/metrics`仅输出汇总和固定state标签，不输出chat_id、指纹、注释或token。监控端点应由集群网络策略限制到探针/Prometheus。

E2/C5须在有效Prometheus配置增加独立 `inspector-bridge` 抓取任务（目标 `k8s-inspector-alert-bridge.kube-system.svc:8000`，默认路径/metrics），加载 `deploy/monitoring/bridge-rules.yaml`，并将 `notification_channel="independent"` 路由到**不依赖此Bridge的值班渠道**。单纯添加标签不会自动产生独立接收器。监控必须覆盖dead、积压年龄、worker停止、数据库不可写、抓取失败/从未出现和容量不足。

```bash
promtool check rules deploy/monitoring/bridge-rules.yaml
promtool test rules tests/prometheus/bridge-rules.test.yml
```

当前Windows环境无promtool/Docker，已提供正向和边界规则fixtures，**尚未实际执行PromQL规则测试**。YAML解析不算规则通过。

可由受控本地维护每天清理已确认SENT超过30天的记录：

```bash
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db prune --retention-days 30
```

保留所有unsent、DEAD以及结果曾不明但未核实的SENT审计。重投不抹旧事件。监控备份/磁盘增长，未知证据长期保留须纳入容量规划；不得用删未处理记录解决容量告警。

## 一致备份、迁移、恢复和回退

```bash
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db backup --destination /backup/bridge-UNIQUE_UTC.db
```

使用SQLite backup API包含WAL已提交内容，10秒备份期限；在目标目录先生成临时快照、校验完整性和fsync，再以不覆盖已有文件的原子hard-link发布。备份目录也必须在支持这些语义的本地/块文件系统上；不支持时备份失败。定期将已校验的关闭快照用受控备份系统复制到独立加密存储，不能运行中只拷贝bridge.db漏掉WAL。

当前schema=2。启动拒绝更新或残缺schema；v1→v2升级前创建 `bridge.db.pre-v2-UUID.bak`，迁移事务失败回滚，备份保留。**升级前停止所有Bridge writer**；首次v1→v2启动也必须满足此前提。失败先保存原库/备份/镜像，再用受支持版本核验，禁止手改schema_version冒充迁移成功。

恢复步骤：停止入口流量并留在上游/独立接收器；停止旧Pod和所有writer；备份当前库；在**新空目录/新验证卷**放入完整快照，校验schema、integrity和待投任务；启动同版本单实例，观察pending恢复和lease后的SENDING重试，再恢复入口。不要把恢复文件覆盖到仍有原WAL/SHM的活跃路径。保留旧目录以供核对。

恢复旧快照可能重发外部已成功消息；记录快照时间、丢失窗口、重投数量、RPO和RTO。离线测试只验证文件重开/WAL快照/事务中断；真实Pod替换、新卷恢复、断电/挂载故障及供应商重复行为**仍待E2/E5演练**。

回退sync前先停止新async接收，保存一致备份，drain待发送任务或登记并明确安排后续恢复（包括DEAD/unknown）；保持来源鉴权和固定路由，核验sync200实际发送。不能切换后静默遗弃outbox或自动导入旧group_key映射。
