# F：覆盖扩展与 MCP 加固 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** 在巡检正确性与可靠交付成立后，扩大实际可验证的覆盖，并让可选 MCP 安全、准确地辅助诊断。

**Architecture:** 新规则接入 C 的快照/catalog；MCP 按 AGENTS 保持自包含，以同一组输入 fixtures 验证一致语义。MCP 默认本地 stdio，远程能力显式启用。

**Tech Stack:** Kubernetes SDK、FastMCP 可选 extra、Python、pytest。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；依赖 C1–C4，打包与部署消费 E1/E2。

## Global Constraints

- F 是可选后续，不阻塞最低可靠版本；若当前已开放 MCP，F2 的TLS/鉴权作为启用门槛提前。
- MCP 不强制导入 common/checks/collector；测试可共用 fixture，生产仍可独立部署。
- 2026-10-14复核：当前 mcp_server.py 已导入 .common，与 AGENTS 声明的自包含约定不一致。F2应先恢复/证明独立运行能力，再按契约fixtures对齐；不能把文档中的意图误写为当前已实现事实。
- 只读诊断，不添加 exec/delete/evict、自动修复或任意网络探测。
- 无权限、能力缺失、metrics不可用都要返回 unknown/unsupported，不能说“集群正常”。
- 按真实 K8s/API/feature gate 版本验证规则；最新文档出现的字段不默认在所有集群可用。

## Review Focus

1. Deployment replicas 看似齐全但未 Ready：F1 的 observedGeneration/副本用例。
2. MCP 默认跳过证书验证或网络入口无鉴权：F2 的TLS/transport测试。
3. 非授权namespace与日志泄漏：F2 的范围/长度/权限测试。
4. 顺序init、restartable sidecar、overhead计算错误：F3 的版本标注资源fixtures。
5. N个Node触发N次全量metrics请求：F3 的单次请求计数测试。

## F1：补充工作负载、压力与控制面能力检查

**文件：** 新增 k8s_inspector/rules_workloads.py，扩展 rules_nodes.py/rules_control.py/snapshot.py/rule_catalog.py；新增 tests/test_extended_rules.py。

**接口：**

- evaluate_workloads(snapshot: RunSnapshot, cfg: InspectorConfig) -> tuple[CheckEvaluation,...]。
- WorkloadPolicy(rollout_grace_seconds: int = 300, required_addons: tuple[str,...] = ())；配置添加显式字段，未知键校验沿用A1。
- Deployment/StatefulSet/DaemonSet 使用 observedGeneration、desired/ready/available与rollout条件；缩容到0有意配置与采集失败分开。owner UID保留到Finding。
- Node DiskPressure/MemoryPressure/PIDPressure独立规则，不因Ready=True忽略；metrics不足不会掩盖condition异常。
- 控制面/Addon的“Pod可见”与“功能可达”分别记录。DNS功能探测若需要，作为显式可选的受限目标探针另立设计，不在本批偷偷创建测试Pod/执行命令。
- 新workload规则加入hourly/daily/full，主告警profile初始hourly；不扩大5m API压力。必需Addon依据环境配置而非固定认为某CNI必然存在。

~~~python
def test_deployment_unready_after_rollout_grace_fails(snapshot, cfg):
    snapshot = snapshot.with_deployment(
        desired=3, ready=1, observed_generation=4, generation=4, age_seconds=600)
    result = evaluate_workloads(snapshot, cfg)
    assert any(f.reason == "AvailableReplicasBelowDesired"
               for evaluation in result for f in evaluation.findings)

def test_ready_node_with_pressure_still_has_finding(snapshot, cfg):
    result = evaluate_nodes(snapshot.with_node(ready=True, disk_pressure=True), cfg)
    assert any(f.reason == "DiskPressure" for e in result for f in e.findings)
~~~

另测 test_zero_desired_is_not_failure、test_unobserved_generation_is_pending_not_healthy、test_missing_required_addon_is_distinct_from_unconfigured_addon、test_managed_control_plane_is_capability_limited。

- [ ] 写当前域版本标注fixtures与上列失败断言。
- [ ] 运行 python -m pytest tests/test_extended_rules.py -q。
- [ ] 接入快照和catalog，保留证据/覆盖，不引入逐对象全量API调用。
- [ ] 跑C4规则/C7单源测试，核对新增权限与API预算。
- [ ] 记录能力表和未覆盖项，提交；功能探测/etcd深检需要独立设计和真实环境条件。

## F2：MCP 可选安装、TLS、入口与诊断范围

**文件：** 修改 pyproject.toml、k8s_inspector/mcp_server.py；新增 tests/test_mcp_security.py、docs/runbooks/mcp.md；MCP部署为E2可选组件。

**接口：**

- MCPAccessPolicy(allowed_namespaces: frozenset[str], allow_cluster_metadata: bool, allow_logs: bool, max_log_bytes: int = 16384, max_log_lines: int = 80) 在mcp_server内定义。
- authorize_namespace(namespace: str, policy: MCPAccessPolicy) -> None；namespace scope从已验证principal或本地固定配置获得。
- 默认 transport=stdio；显式 --transport http|sse（兼容）、--host 默认127.0.0.1。远程绑定必须配置经过验证的认证，不只用“在内网”代替。
- init_k8s_client 默认 verify_ssl=True；证书错误清楚报告；insecure仅显式调试参数，日志可见，不能被代码静默覆盖。
- 日志默认关闭；启用后同时限制namespace、行数、字节、timeout；当前/previous日志分开，访问pods/log有独立只读权限。截断明确标记，不输出整个日志或凭据。
- mcp extra 固定经过测试的FastMCP/Pydantic兼容范围；安装基础CLI时不必安装MCP，独立MCP安装模式和所需Python版本在能力表明确。

~~~python
def test_kubernetes_tls_verification_defaults_to_true(monkeypatch):
    monkeypatch.delenv("MCP_K8S_VERIFY_SSL", raising=False)
    kube = init_k8s_client()
    assert kube.core.api_client.configuration.verify_ssl is True

def test_namespace_outside_policy_is_denied():
    policy = MCPAccessPolicy(frozenset({"team-a"}), False, False)
    with pytest.raises(PermissionError):
        authorize_namespace("team-b", policy)
~~~

init fixture monkeypatch配置加载与客户端，不连接集群。增加 test_default_transport_is_stdio、test_remote_binding_requires_auth、test_logs_are_disabled_by_default、test_log_response_is_bounded、test_mcp_installs_without_inspector_internal_imports。

- [ ] 写TLS/transport/范围/日志断言，运行 python -m pytest tests/test_mcp_security.py -q。
- [ ] 实现自包含配置与权限，按选定FastMCP版本官方API接认证，避免复制不兼容latest接口。
- [ ] 基础包与mcp extra分别隔离安装；运行本地协议smoke，检查日志走stderr不污染stdio消息。
- [ ] 远程测试缺少认证配置时启动失败；可选部署限制网络/账户权限。
- [ ] 审查安全默认值与兼容迁移，提交。官方transport依据见[FastMCP运行文档](https://gofastmcp.com/deployment/running-server)。

## F3：MCP资源计算、metrics复用与语义契约

**文件：** 修改 mcp_server.py 的 _pod_effective_resources、_node_metric_view、_problem_reason_list、_pod_issue_summary；新增 tests/test_mcp_semantics.py、tests/fixtures/kubernetes-resources/。

**接口：**

- 保留 _pod_effective_resources(pod: V1Pod) -> MCPPodResource；声明字段request与limit分别表达，缺limit不是0上限，返回可选值/明确unbounded标志。
- 新增 load_node_metrics_once(custom: CustomObjectsApi) -> dict[str, dict[str,str]] | None；_node_metric_view_from_snapshot(metrics, node_name)返回usage或unknown。cluster_summary/find_problem_nodes每次请求只读一次metrics全集。
- 普通init按各阶段峰值而非全部相加；restartable init sidecar按执行顺序累计常驻需求，再与应用阶段比较；requests加overhead。limits缺失/overhead与Pod级预算采用目标版本明确语义。
- PodLevelResources、in-place resize/status allocation依据目标版本声明支持；字段存在但暂未支持时返回unsupported/低置信度，不能静默忽略然后给精确调度结论。
- 同A2/C4输入fixtures测试Ready/init/历史OOM：MCP诊断可以展示更多历史，但当前故障结论必须一致。
- 建议包含证据、当前/历史区别、影响范围与验证步骤；不输出“已执行修复”或把推测根因当作事实。

~~~python
def test_sequential_init_uses_max_not_sum(resource_pod_factory):
    pod = resource_pod_factory(app_cpu="500m", init_cpu=["1", "2"], overhead_cpu="100m")
    result = _pod_effective_resources(pod)
    assert result.cpu_request_m == 2100

def test_restartable_sidecar_counts_overlap(resource_pod_factory):
    pod = resource_pod_factory(app_cpu="1", sidecar_before_init_cpu="200m",
                               init_cpu=["1500m"], overhead_cpu="100m")
    result = _pod_effective_resources(pod)
    assert result.cpu_request_m == 1800

def test_cluster_scan_fetches_metrics_once(mcp_runtime, custom):
    mcp_runtime.scan_nodes(count=50)
    assert custom.list_cluster_custom_object.call_count == 1
~~~

resource_pod_factory在本任务定义，构建按顺序的V1 init对象。CPU字段沿用现有MCPPodResource.cpu_request_m，不改变用户接口名称。追加 test_ready_pod_old_oom_is_not_current_failure、test_unready_pod_is_detected、test_unknown_metrics_does_not_report_zero_usage、test_unbounded_limit_is_not_zero_capacity。

- [ ] 写上列fixtures，标注参考K8s版本和期望值；运行 python -m pytest tests/test_mcp_semantics.py -q。
- [ ] 修资源算法与metrics快照、自包含Pod语义；不让生产MCP导入C层。
- [ ] 运行MCP全部测试、资源golden fixtures与基础CLI回归。
- [ ] 对照真实目标版本的scheduler计算核对复杂sidecar/Pod级预算，未验证功能标不支持。
- [ ] 审查建议表述/API请求数/日志泄漏，提交。

资源计算依据：[Kubernetes sidecar文档](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)、[v1.34资源计算官方实现](https://github.com/kubernetes/kubernetes/blob/v1.34.0/staging/src/k8s.io/component-helpers/resource/helpers.go)。Pod级预算版本依据：[资源管理文档](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)。参考版本不等于用户集群版本，需在E1/E5落实。

## 批次完成与后续范围

- [ ] F1/F2/F3各自可独立启用，未启用MCP不会影响巡检/Bridge。
- [ ] 覆盖清单明确已验证规则、权限/版本边界和未支持能力。
- [ ] 自动关联/自动建群、自动修复、长驻采集与HA数据库另立设计，依据真实使用数据决定。
