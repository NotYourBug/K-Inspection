# K8s 集群自动化巡检（Python + Kubernetes SDK）

作为 k8s SRE，集群巡检的核心目标是**提前发现潜在故障、验证集群健康状态、确保业务稳定性**，需覆盖「基础设施层→控制平面→数据平面→业务层→可观测性」全维度，同时兼顾**自动化执行**和**人工复核**。基于以下的巡检维度（基础设施/控制平面/数据平面）实现的可落地工具，支持：

- 自动化巡检：Node / Pod / PVC / Event / 控制平面组件健康
- 报告输出：JSON / CSV / HTML
- 告警推送：飞书群机器人 Webhook（仅推送失败项，可选）
- 执行方式：本地运行 / 容器化 + CronJob

当前实现范围以代码和报告证据为准；下文巡检维度表也包含人工运维目标，磁盘/网络主动探测、etcd 深检和按日重启增量尚未实现。2026-10-15 完成首批巡检正确性整改：Pod 当前/历史状态分离、PVC 绑定语义、部分采集证据保留、原子报告与独立输出；详细计划见 docs/superpowers/plans/2026-10-02-remediation-roadmap.md。

### 一、巡检整体框架

| **<font style="color:rgb(0, 0, 0);">巡检维度</font>** | **<font style="color:rgb(0, 0, 0);">核心目标</font>**                       | **<font style="color:rgb(0, 0, 0);">执行方式</font>** | **<font style="color:rgb(0, 0, 0);">执行频率</font>**        |
| ------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------- | -------------------------------------------------------- |
| <font style="color:rgb(0, 0, 0);">基础设施层</font>    | <font style="color:rgb(0, 0, 0);">节点 / 网络 / 存储可用性</font>                | <font style="color:rgb(0, 0, 0);">自动化 </font>     | <font style="color:rgb(0, 0, 0);">每小时（关键项）、每日（全量）</font> |
| <font style="color:rgb(0, 0, 0);">控制平面</font>     | <font style="color:rgb(0, 0, 0);">apiserver/etcd/scheduler 等组件健康</font> | <font style="color:rgb(0, 0, 0);">自动化</font>      | <font style="color:rgb(0, 0, 0);">每 10 分钟</font>         |
| <font style="color:rgb(0, 0, 0);">数据平面</font>     | <font style="color:rgb(0, 0, 0);">Node/Pod / 存储卷健康</font>               | <font style="color:rgb(0, 0, 0);">自动化</font>      | <font style="color:rgb(0, 0, 0);">每 5 分钟</font>          |

### 二、核心巡检项（附检查方法 / API / 工具）

#### 基础设施层巡检

| **<font style="color:rgb(0, 0, 0);">检查项</font>** | **<font style="color:rgb(0, 0, 0);">检查标准</font>**                                                   | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>**                                                        |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| <font style="color:rgb(0, 0, 0);">节点状态</font>    | <font style="color:rgb(0, 0, 0);">所有 Node 状态为 Ready，无 NotReady/Taint（业务节点无 NoSchedule 污点）</font>    | <font style="color:rgb(0, 0, 0);">kubectl get nodes / CoreV1Api.list\_node () / 代码过滤 Ready 状态</font>          |
| <font style="color:rgb(0, 0, 0);">节点系统资源</font>  | <font style="color:rgb(0, 0, 0);">CPU 使用率 < 80%、内存使用率 < 85%、磁盘使用率 < 85%（/var/lib/kubelet 目录）</font> | <font style="color:rgb(0, 0, 0);">kubectl top nodes / Metrics API /node-exporter 监控 / df -h 检查磁盘</font>       |
| <font style="color:rgb(0, 0, 0);">节点网络</font>    | <font style="color:rgb(0, 0, 0);">节点间网络互通（6443/10250/10255 端口通），CNI 插件（calico/flux）运行正常</font>      | <font style="color:rgb(0, 0, 0);">ping/telnet / kubectl get pods -n kube-system -l k8s-app=calico-node</font> |
| <font style="color:rgb(0, 0, 0);">存储后端</font>    | <font style="color:rgb(0, 0, 0);">存储类（StorageClass）可用，PV 绑定状态正常，无 Pending PVC</font>                | <font style="color:rgb(0, 0, 0);">kubectl get sc/pv/pvc / CoreV1Api.list\_pvc () 过滤 phase=Pending</font>      |

#### 控制平面巡检

| **<font style="color:rgb(0, 0, 0);">检查项</font>**            | **<font style="color:rgb(0, 0, 0);">检查标准</font>**                                                   | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>**                                                                                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <font style="color:rgb(0, 0, 0);">apiserver</font>          | <font style="color:rgb(0, 0, 0);">副本数正常（多主集群≥2），Pod Running，API 响应时间 < 500ms，无 5xx 错误</font>        | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-apiserver / kubectl get --raw=/healthz / Prometheus 监控 apiserver\_request\_duration\_seconds</font> |
| <font style="color:rgb(0, 0, 0);">etcd 集群</font>            | <font style="color:rgb(0, 0, 0);">集群健康（member 健康数 = 副本数），磁盘使用率 < 80%，无 leader 切换频繁（<1 次 / 天）</font> | <font style="color:rgb(0, 0, 0);">kubectl -n kube-system exec etcd-master -- etcdctl endpoint health /etcd 监控（etcd\_disk\_usage\_percent）</font>                                        |
| <font style="color:rgb(0, 0, 0);">controller-manager</font> | <font style="color:rgb(0, 0, 0);">Pod Running，无重启，核心控制器（如 deployment/pod 控制器）无报错</font>             | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-controller-manager / 查看日志kubectl logs -n kube-system kube-controller-manager</font>                 |
| <font style="color:rgb(0, 0, 0);">scheduler</font>          | <font style="color:rgb(0, 0, 0);">Pod Running，无重启，调度成功率 > 99%（无 Pending Pod 因调度失败）</font>           | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-scheduler / 检查 Event 中FailedScheduling事件</font>                                                     |

#### 数据平面巡检

| **<font style="color:rgb(0, 0, 0);">检查项</font>**   | **<font style="color:rgb(0, 0, 0);">检查标准</font>**                                                       | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>**                                                                                            |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| <font style="color:rgb(0, 0, 0);">Pod 健康状态</font>  | <font style="color:rgb(0, 0, 0);">业务 Pod Running，重启次数 < 3 次 / 天，无 CrashLoopBackOff/Error 状态</font>      | <font style="color:rgb(0, 0, 0);">kubectl get pods --all-namespaces / CoreV1Api.list\_pod\_for\_all\_namespaces () 过滤 phase=Failed/Unknown</font> |
| <font style="color:rgb(0, 0, 0);">Node 组件健康</font> | <font style="color:rgb(0, 0, 0);">kubelet/kube-proxy Pod Running，节点日志无 ERROR 级报错</font>                 | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l k8s-app=kubelet / journalctl -u kubelet</font>                               |
| <font style="color:rgb(0, 0, 0);">事件（Event）</font> | <font style="color:rgb(0, 0, 0);">无大量 Warning/Error 级 Event（<5 个 / 小时），无重复报错（如 ImagePullBackOff）</font> | <font style="color:rgb(0, 0, 0);">kubectl get events --all-namespaces --field-selector type=Warning / Event API 监听</font>                         |

基于 instr的巡检维度（基础设施/控制平面/数据平面）实现的可落地工具，支持：

- 自动化巡检：Node / Pod / PVC / Event / 控制平面组件健康
- 报告输出：JSON / CSV / HTML
- 告警推送：飞书群机器人 Webhook（仅推送失败项，可选）
- 执行方式：本地运行 / 容器化 + CronJob

以下是一套可落地、可扩展的 K8s 集群巡检方案，包含巡检维度、核心检查项、工具化实现、执行频率和故障闭环。

## 目录

- [项目架构](#项目架构)
- [快速开始（本地）](#快速开始本地)
- [容器化与集群部署](#容器化与集群部署)
- [存储与留存策略（PV/PVC）](#存储与留存策略pvpvc)
- [Prometheus + Grafana（可观测性增强）](#prometheus--grafana可观测性增强)
- [Alertmanager（告警治理）](#alertmanager告警治理)
- [MCP Server（给 LLM 用的排障工具层）](#mcp-server给-llm-用的排障工具层)
- [安全建议](#安全建议)
- [常见问题](#常见问题)
- [开发与测试](#开发与测试)

## 项目架构

本项目提供两条常见落地路径：

- **轻量模式（脚本 + 报告）**：CronJob/本地运行 → 输出 JSON/CSV/HTML →（可选）飞书群机器人推送摘要
- **生产模式（可观测 + 告警治理）**：CronJob → Pushgateway → Prometheus（规则）→ Alertmanager（聚合/抑制）→ Bridge（固定责任群通知）

核心组件：

- `k8s_inspector/cli.py`：巡检任务入口（短任务）
- `k8s_inspector/checks.py`：巡检规则
- `k8s_inspector/report.py`：报告输出（JSON/CSV/HTML）
- `k8s_inspector/metrics.py`：Pushgateway 指标推送
- `k8s_inspector/alertmanager_bridge.py`：接收 Alertmanager webhook 的桥接服务（常驻）
- `deploy/`：Kustomize 部署清单（RBAC/CronJobs/监控/告警/存储）

## 目录结构

- `k8s_inspector/`：核心代码（巡检/报告/指标/告警桥接）
- `deploy/`：集群内部署（Kustomize）
- `deploy/storage/`：PV/PVC 示例（NFS RWX）
- `tests/`：单元测试
- `README-MCP.md` ：项目简介

## 快速开始（本地）

1. 安装依赖

```bash
pip install -r requirements.txt
```

1. 使用本地 kubeconfig 执行

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full
```

1. 集群内执行（Pod/CronJob 中）

```bash
INSPECTOR_CLUSTER=prod python -m k8s_inspector.cli run --in-cluster --formats json --output-dir /data/out --profile 5m
```

## 失败项推送（飞书）

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --feishu-webhook "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
```

## 配置文件

默认无需配置文件。你也可以传入 JSON 或 YAML（YAML 依赖 PyYAML）：

```bash
python -m k8s_inspector.cli run --config ./config.yaml
```

示例（config.yaml）：

```plain
thresholds:
  node_cpu_utilization: 0.80
  node_memory_utilization: 0.85
  pod_restart_count: 3
  warning_events_max: 50
control_plane_namespace: kube-system
pod_unready_grace_seconds: 120
pod_pending_grace_seconds: 300
pvc_pending_grace_seconds: 300
output_policy: strict  # 可选；默认 best-effort
allow_noschedule_taints:
  - "node-role.kubernetes.io/control-plane"
```

## 容器化与 CronJob

配置中的未知字段、错误类型、非有限数值和无效范围会在连接集群前报错。重启阈值仍为累计计数；历史 OOM 不再冒充当前故障。Ready=False 的观察期使用 condition.lastTransitionTime；时间缺失为未知。WaitForFirstConsumer 结合消费者创建与调度转换时间计算绑定宽限；消费者因CPU等原因未调度时单独记录阻塞原因，不直接归咎于存储。缺少调度时间为未知，Lost 与已确认超时绑定保留故障证据。

Addon 期望副本需要读取配置 namespace 内的 Deployment/DaemonSet；deploy/rbac.yaml 增加了 kube-system 内的只读 Role。若 control_plane_namespace 改为其他 namespace，应同步调整该 Role/RoleBinding。

部署清单在 deploy/：

- deploy/kustomization.yaml：方案A一键部署（集群内 Prometheus+Alertmanager+Pushgateway+Bridge+RBAC+CronJobs）
- deploy/storage/：PV/PVC 示例（用于巡检结果落盘归档）
- deploy/rbac.yaml：最小化 list/watch 权限 + /healthz 非资源 URL 访问
- deploy/cronjob.yaml：示例 CronJob（按需修改镜像、参数、频率、输出卷）
- deploy/cronjobs-by-frequency.yaml：按 5m/10m/hourly/daily 拆分的多 CronJob 示例

### 方案A（推荐，集群内一键部署）

部署前准备：

1. 先构建并推送镜像（示例）

```bash
docker build -t your-registry/k8s-inspector:0.1.0 .
docker push your-registry/k8s-inspector:0.1.0
```

1. 修改部署占位符（必须）

- `your-registry/k8s-inspector:0.1.0`：替换为你的镜像仓库地址
- `INSPECTOR_CLUSTER=YOUR_CLUSTER_NAME`：替换为你的真实集群名（避免多集群指标/告警混淆）
- 飞书配置：按需选择 webhook 或应用机器人模式

```bash
kubectl apply -f deploy/secret-feishu.example.yaml
kubectl apply -k deploy
```

应用前请先修改以下占位项：

- `your-registry/k8s-inspector:0.1.0`（巡检镜像地址：在 deploy/cronjobs-by-frequency.yaml 与 deploy/alertmanager-feishu-bridge.yaml 中）
- `deploy/secret-feishu.example.yaml`：
  - `app_id`
  - `app_secret`
  - `fixed_chat_id`
- `INSPECTOR_CLUSTER`：集群标识（建议必填，用于指标/告警/群名区分集群；deploy/cronjobs-by-frequency.yaml / deploy/cronjob.yaml 默认是占位符 YOUR\_CLUSTER\_NAME，部署前必须替换）

## 退出码

- 0：无 FAIL（可能包含 WARN）
- 2：存在 FAIL
- 1：配置/启动错误，或 --fail-on warn 且只有 WARN；显式 --output-policy strict 时，任何已配置输出失败也返回 1（优先于业务 FAIL 的 2）

默认 output_policy=best-effort，输出彼此独立尝试；--output-policy strict 可覆盖文件配置。INSPECTOR_EXIT_MODE=always0 / --exit-mode always0 只覆盖有效巡检运行的退出码，不抹掉失败记录，也不覆盖启动配置错误。每次运行在 canonical latest 目录写 run-status.json，列出输出成功/失败与 run_success；该状态文件不能记录自身写入失败，失败会进入日志并影响 strict 退出码。

## 执行频率（profile）

- 5m：数据平面关键项（Node Ready/污点、Pod 异常、Pending PVC），不拉取 Metrics，降低 API 压力
- 10m：控制平面（/healthz + 控制平面组件 Pod）
- hourly：基础设施关键项（控制平面 + Node（含 Metrics）+ Pod + PVC）
- daily/full：全量（hourly + Warning Event + kube-system 关键组件）

## 存储与留存策略（PV/PVC）

本项目默认通过 PV/PVC 持久化巡检产物，并按频率做分层留存，避免高频任务导致存储无限增长：

- **所有 profile**：每次原子刷新 latest/<profile>/report.json，包括有效恢复；--persist-level issue/none 仅控制额外格式/归档，不能关闭 canonical latest JSON
- **路径兼容**：--output-dir ./out 生成 ./out/latest/<profile>/；已有 --output-dir /data/out/latest/5m 不会重复嵌套。archive/hourly、archive/daily 的部署路径也会更新对应 latest
- **低频归档**：启用 --archive hourly/daily 后写 archive/<频次>/<UTC时间桶>/<UTC时间戳_唯一标识>/report.json；同秒运行不覆盖。未启用 archive 时，额外格式仍可写入原 output-dir
- **定期清理**：`cleanup` CronJob 负责删除过期归档目录（hourly 7 天，daily 30 天，默认可改）

先执行 python -m k8s_inspector.cli cleanup --root ./out/archive --dry-run 检查清理名单。数值时间桶按 UTC 解释；存量非 UTC 目录可能有时差，应先核对或迁移。清理拒绝符号链接/Windows reparse 路径，只统计实际删除成功，任何删除失败返回非零；保留天数0表示不清理。原子发布不等于多任务写同一 latest 的互斥，部署仍需 concurrencyPolicy=Forbid。

对应文件：

- PV/PVC 示例：`deploy/storage/`
- 归档与清理：`deploy/cleanup-cronjob.yaml` + `k8s-inspector cleanup` 子命令

## Prometheus + Grafana（可观测性增强）

本项目支持将巡检结果推送到 Pushgateway，再由 Prometheus 抓取，Grafana 展示大盘。

新增 k8s_inspector_run_success 和 k8s_inspector_sink_success：业务 FAIL 可以是成功执行的巡检；采集未知或必需输出失败则执行质量失败。推送内容只包含推送前已经完成的输出结果，不能提前宣称本次指标推送成功；该次推送结果见 run-status.json/日志。独立目标清单、持久最后有效观察及“失明不假恢复”的规则治理仍待 C 批次完成。

1. 部署 Pushgateway（示例）

```bash
kubectl apply -f deploy/pushgateway.yaml
```

1. 配置 Prometheus 抓取 Pushgateway

Prometheus 原生配置可参考：

- deploy/prometheus-scrape-pushgateway-snippet.yaml

1. 让巡检 CronJob 推送指标

在 CronJob 的 env 中配置：

- PUSHGATEWAY\_URL：例如 <http://pushgateway.kube-system.svc:9091>
- PUSHGATEWAY\_JOB：默认 k8s-inspector

或本地运行时传参：

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --profile full --pushgateway-url http://127.0.0.1:9091
```

1. 导入 Grafana Dashboard

Grafana 导入 JSON：

- deploy/grafana-dashboard-k8s-inspector.json

## Alertmanager（告警治理）

如果你已经使用 Prometheus + Alertmanager，可以把“脚本直推飞书”升级为“Prometheus 规则 → Alertmanager 聚合/去重/抑制 → 飞书”。

1. 部署飞书告警桥接服务（接收 Alertmanager webhook，再转发到飞书）

```bash
kubectl apply -f deploy/alertmanager-feishu-bridge.yaml
```

在 deploy/alertmanager-feishu-bridge.yaml 中：

Bridge 当前策略为 **固定责任群**，不会根据 `groupKey` 创建群、邀请成员或修改群名。`FEISHU_CHAT_POLICY` / `--chat-policy` 默认且目前仅支持 `fixed`。重大故障的人工事件升级、事件协作群和受控自动升级属于后续整改范围。

| 发送方式 | 必需配置 | 行为 |
|---|---|---|
| webhook 文本 | `FEISHU_MODE=webhook`、`FEISHU_WEBHOOK` | 发到该 webhook 所属的已有群 |
| app 卡片 | `FEISHU_MODE=app`、`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_CHAT_ID` | 发到指定已有群；应用机器人需已加入该群并具有发送权限 |

部署示例默认使用 app；Secret 增加了必需的 `fixed_chat_id`，见 `deploy/secret-feishu.example.yaml`。切换为 webhook 时移除 app 专用 Secret 引用，配置群 webhook。示例中的真实群 ID、应用凭据和镜像地址需要在部署环境填写。

本地启动示例（应用凭据通过环境变量设置）：

```bash
python -m k8s_inspector.alertmanager_bridge --mode app --feishu-chat-id oc_your_existing_chat --chat-policy fixed
python -m k8s_inspector.alertmanager_bridge --mode webhook --feishu-webhook "<webhook_url>"
```

可选的多责任群路由使用 JSON/YAML 文件，见 `deploy/feishu-routes.example.yaml`。app 模式通过 `--routes-file` 或 `FEISHU_ROUTES_FILE` 加载：

- `routes[].match` 对每条告警的标签精确匹配，按文件顺序选择第一条命中的路由，不使用正则或通配符。
- 未匹配或缺少标签时发送到必需的兜底群；同一目标群的告警合并成一张卡片。
- 兜底群优先级为 `--feishu-chat-id` → `FEISHU_CHAT_ID` → 文件中的 `default_chat_id`。所有群必须预先存在。
- 当前巡检规则尚未统一输出 `owner_team`；可以先按已有的 `cluster`、`alertname`、`check` 标签路由，补齐团队标签后再增加责任归属规则。
- 路由文件替换真实群 ID 后，可用 `kubectl create configmap k8s-inspector-feishu-routes --from-file=routes.yaml=<路由文件路径> -n kube-system` 创建 ConfigMap，再给 Bridge 增加只读挂载并设置 `FEISHU_ROUTES_FILE`。示例路由文件不是 Kubernetes 资源，不直接 `kubectl apply`。

每条通知保留来源标签，混合批次会同时显示 firing/resolved。恢复通知仅描述所列告警，不能代表整个事件已恢复；上游截断会明确提示。默认同步模式在实际发送成功后返回 HTTP200，校验错误400、超大请求413、交付失败500。可选 `--delivery-mode async` 提供逐目的地SQLite事务队列、内容去重、租约重试和重启恢复，HTTP202仅表示持久接收；鉴权和独立状态卷为必需配置，存储/容量失败返回503。发送结果不明时仍可能重复，不能宣称恰好一次。

异步模式的启用顺序、Secret、备份/迁移、死信处理和独立监控见[可靠投递运维手册](docs/runbooks/bridge-delivery.md)。`deploy/components/bridge-async` 为显式选择的示例组件，默认base保持sync；真实PVC、promtool、供应商UUID窗口和Pod替换恢复验收通过后再切换生产。

巡检背景通过 `INSPECTOR_REPORT_DIR` / `--report-dir` 配置，默认 `/data/out`。只有同一 cluster/profile、没有责任路由或团队/namespace/service 范围的通知才会附带匹配的新鲜快照；缺少身份、过期、时间无效或混合 profile 时不附带。快照仅作为背景，不作为事件恢复依据。`--report-max-results 0` 可关闭附带背景，CLI 数量参数优先于环境变量。

**从旧版升级**：先配置已有固定群及机器人权限，再替换 Bridge。旧的 `FEISHU_ALERT_USER_OPEN_IDS`、群名模板、恢复后缀和群映射路径不再控制通知；相应旧 CLI 参数已移除。已有群不会被程序修改。若需要保留旧映射用于后续人工登记，请在替换旧 Pod 前导出 `/data/state/feishu_chats.json`；旧部署的 `emptyDir` 会随 Pod 删除。新版本不读取、迁移或改写旧映射。

1. 配置 Prometheus 告警规则
   Prometheus 规则文件：deploy/prometheus-alert-rules-k8s-inspector.yaml
2. 配置 Alertmanager 路由与抑制规则
   Alertmanager 配置片段：deploy/alertmanager-config-snippet.yaml
   该配置包含：
   - 按 cluster 聚合通知，由 Bridge 根据每条告警的标签路由到固定责任群
   * 对 K8sInspectorStale 做抑制：当巡检未运行时，避免旧数据触发重复 FAIL/WARN 告警

说明：为了降低“Pushgateway 残留旧值”导致的误报，本仓库的告警规则已对关键告警增加“新鲜度约束”（last\_run 在窗口内才允许触发）。

## 安全建议

- **不要提交真实密钥到 GitHub**：`deploy/secret-feishu.example.yaml` 仅做示例，请自行在私有环境创建真实 Secret。
- **最小权限 RBAC**：优先使用 `deploy/rbac.yaml` 的只读权限；不要给 `cluster-admin`。
- **多集群强制区分**：部署前必须设置 `INSPECTOR_CLUSTER`（默认占位符 `YOUR_CLUSTER_NAME`）。
- **证书校验**：测试环境可临时关闭；生产环境建议开启 K8s/飞书 API 的证书校验与网络隔离。

## 常见问题

- `kubectl apply -f kustomization.yaml` 报 `no matches for kind "Kustomization"`
  - 用 `kubectl apply -k deploy`。
- CronJob `BackoffLimitExceeded` 且 `kubectl logs` 为空
  - 常见原因是“退出码非 0”导致 Job 标记失败；项目支持 `INSPECTOR_EXIT_MODE=always0`（推荐配合 Prometheus 告警）。
- Pushgateway/Prometheus 有数据但飞书无消息
  - 按链路排查：Prometheus 规则 → Alertmanager receiver → Bridge `/alert` 返回码与日志 → 飞书配置与权限。
- `K8sInspectorHasFail` 看似误报
  - 先看 `K8sInspectorStale` 是否同时触发；本仓库规则已增加 last\_run 新鲜度约束，避免旧值误报。

## Inspection report v2

The CLI now emits `schema_version: 2`, a UUID `run_id`, canonical `profile`, and separate `executions`, `findings`, `observations`, and known `sink_results`. The original `generated_at`, `cluster`, `summary`, and `results` keys remain. V2 summary and results retain legacy check groups, as does `run_all`. Structured executions and CSV `rule_id` identify the individual rules. Legacy metric selectors and healthy Node utilization remain available during shadow cutover; unknown counters are omitted rather than replaced by zero. Constructing `Report(generated_at, cluster, results)` still serializes exactly the v1 root.

`latest/<profile>/report.json` is refreshed on every run. Archives include the same report run ID, so runs within one second remain distinct. `manifest.json` records completed sink attempts; `run-status.json` and logs also show failures, including a failed manifest write. A report does not recursively rewrite itself to claim its own write succeeded. JSON observation timestamps are UTC; HTML also displays Asia/Shanghai time, execution quality, unknown coverage, current failures, historical evidence, and local object anchors. CSV retains overview columns and adds schema, execution and resource identity columns, with a row per finding (or an execution-only row).

Bridge background snapshots require a supported schema, matching cluster/profile, and a fresh timezone-aware timestamp. Team, namespace, service, responsibility-routed or non-cluster-scope notifications omit object details until ownership can be verified. A background snapshot is never recovery evidence.

## Durable observation state and metric migration

`observation-state.json` lives beside `latest/<profile>/report.json` by default. `--state-dir ROOT` (or `INSPECTOR_STATE_DIR`) moves it to `ROOT/<profile>/observations.json`; this root must survive CronJob/Pod replacement. The initialization marker detects a missing state file after initialization. Losing the entire volume also loses that marker; unknown observations still omit health samples, and independent expected-target monitoring must detect missing or stale valid evidence. Restore state and reports from a verified backup; never delete a marker to hide an unreadable state file.

A profile OS lock spans collection and publication. State updates have a separate OS lock and atomic replacement; older observations cannot overwrite newer ones. The deployed CronJobs already use `concurrencyPolicy: Forbid`. **Release gate:** verify cross-process locking and restart/power-loss behavior on the actual persistent volume. The existing report NFS example is not certification for observation state locking; use a verified local/block state volume and the production validation checklist before cutover. Do not put state in ephemeral `/tmp` or an `emptyDir`.

Only complete known aggregate observations replace last-valid state. FAIL → ERROR keeps the last failure count and its old timestamp; a complete valid healthy observation writes zero. Unknown without history emits no last-valid failure sample. State read/write failures are required sink failures, visible in manifests, run status and metrics. `--output-policy strict` returns 1 for required sink failure; `always0` changes process exit only. A failed state write retains readable previous samples for the metrics push.

The new bounded `inspector_*` metrics separate run success, collection completeness, current observation validity, last-valid time/failures and sink success. Object UIDs, Pod names, fingerprints and run IDs remain in reports/state, outside these metric labels. Legacy `k8s_inspector_*` metrics remain for compatibility. Each push uses a new registry and PUT replacement for the same job/cluster/profile grouping key. Remove an old job/grouping key only after validating the new shadow rules; do not broadly delete other clusters' metrics. Persistent event cursors bind cluster/profile/UID and distinguish repeated series from count increments; counts without a prior cursor remain explicitly cumulative.

## Independent expected-target monitoring

`deploy/monitoring/expected-targets.example.yaml` defines expected cluster/profile/rule targets even before their first successful run. Generate candidates with `python -m tools.render_monitoring --targets deploy/monitoring/expected-targets.example.yaml --output deploy/monitoring/generated`. The independent inventory service exposes expected metrics from a read-only ConfigMap; its own scrape failure or removal also alerts. New business rules have exactly one primary profile per catalog rule and retain confirmed failures through collection errors. Positive partial evidence is persisted separately without becoming a complete valid observation.

New rules default to `shadow="true"`; install the null shadow route before loading them. Existing active rules remain until controlled cutover. Activation requires an independent quality receiver and the storage, runtime, API and real-cluster gates in [the cutover runbook](docs/runbooks/inspection-monitoring.md). Local generation and tests do not deploy resources or send notifications.

Local kubeconfig runs derive a stable connection-bound fallback identity; production and in-cluster runs should use an explicit `--cluster` / `INSPECTOR_CLUSTER` / config `cluster_id` (mandatory in-cluster). INFO findings preserve measurements and Event cursors without counting as faults. Event totals without a trustworthy window/baseline remain UNKNOWN. See the monitoring runbook for shadow rollout and release gates.


## Offline release preparation

`pyproject.toml` owns runtime/development declarations; `requirements.txt` is the constrained development install entry point. `constraints/runtime.txt` pins selected direct dependencies (SDK29 for Python3.9, SDK37 for newer Python); `constraints/build.txt` selects compatible build/test tools. Transitive versions and the declared runtime matrix still require actual clean-install evidence.

```sh
python -m pip install -r requirements.txt
python -m tools.render_cronjobs --output deploy/base/cronjobs-by-frequency.yaml
python -m tools.verify_release --offline --output-dir ./out/release-check
kustomize build deploy
kustomize build deploy/overlays/example
kustomize build deploy/overlays/shadow
```

The verifier writes per-check evidence and returns1 while any mandatory check is failed/pending; offline mode never pulls/builds Docker images and explicitly records shared dependency bootstrap. `.github/workflows/ci.yml` is a prepared GitHub example, not a run CI result. No package/image is automatically published.

CI adds `--local-checks-only` to use the mandatory local outcomes as its exit gate, including composed Prometheus/Alertmanager configs and both inspection/Bridge rule suites. The resulting JSON separates `local_checks_success` from full `success`, which remains false while external release acceptance is pending. The default command above keeps the full release gate.

The default `deploy` now contains only suspended inspector jobs/RBAC/storage. The full example and namespace-isolated shadow monitoring/async Bridge are explicit overlays. Configure actual image digest/StorageClasses/Secrets/cluster identity and complete real release gates before unsuspending. Report storage and four per-profile state claims are separate; cleanup defaults to dry-run. Legacy root YAML files are historical standalone examples and are not the canonical deployment entry point.

See [rollout/rollback](docs/runbooks/rollout.md), [retention/scheduling](docs/runbooks/report-retention.md) and [operations/ownership](docs/runbooks/operations.md). Real environment acceptance remains pending; ordinary alert delivery continues to existing fixed groups. D/F features are outside this preparation.
