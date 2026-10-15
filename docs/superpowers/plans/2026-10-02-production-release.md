# E：生产部署与发布 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ('- [ ]') syntax for tracking.

**Goal:** 把离线通过的代码变成可安装、可运行、可观测、可恢复的发布产物。

**Architecture:** 先建立版本和依赖基线；拆可选部署与环境 overlay；用实际构建、规则测试和影子运行逐级验收。E1 提前执行，完整发布依赖 A/B/C。

**Tech Stack:** Python packaging、pytest、Docker、Kustomize、Prometheus promtool、测试 Kubernetes 集群。

**Spec:** [设计基准](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/specs/2026-10-02-remediation-design.md)；[总计划](C:/Users/23033/Desktop/Cluster-Stability-Inspector/docs/superpowers/plans/2026-10-02-remediation-roadmap.md)。

## Global Constraints

- 当前 .git 不存在，旧整改前 ZIP 只覆盖部分文件；不能把它作为完整版本回退保证。
- 本计划所有部署/真实发送/故障演练在 E5 明确的测试范围内执行；离线计划完成不代表已经授权生产发布。
- YAML 解析不等于 Kustomize 渲染、server dry-run、真实卷挂载或 promtool 规则通过。
- SQLite 状态卷与报告卷分离；单实例不是 HA，无节点故障无损承诺。
- 保留 Python>=3.9 当前声明直到实际兼容评估；3.11 为主发布、3.14 额外回归，声明支持的版本需要运行验证。
- 可选 MCP/事件创建不进入最低可靠版本的默认暴露面。

## Review Focus

1. 仅源码目录运行正常，wheel 漏新增模块：E1/E4 的隔离安装测试。
2. 两个 Bridge Pod 同时写状态或 SQLite 落在 NFS：E2 的部署和卷语义验收。
3. CronJob 超时重叠、时区误解和 cleanup 误删：E3 的期限/清理验证。
4. 正式发布与 shadow 都通知同群形成双报：E5 的独立目的地与切换演练。
5. 回退旧程序无法读新 schema、备份漏 WAL：E4/E5 的恢复与版本拒绝测试。

## E1：版本、测试、依赖与安装基线

**文件：** 修改 pyproject.toml、requirements.txt、.gitignore；新增 tools/verify_release.py、tests/test_installed_package.py；记录到审计实施日志。

**接口与交付：**

- 保存全项目源码/config样例/部署/测试的完整版本快照与 SHA256 manifest；排除运行输出、虚拟环境、真实 Secret/凭据。核实是否另有原始 Git 仓库，不能凭空声明已提交。
- 如果该目录就是正式源码，建立本地版本历史；不添加远程、不推送。若无法建立 Git，独立快照和差异记录作为临时审查依据。
- pyproject 是依赖声明单源；requirements 从同一约束生成或显式仅为开发安装入口，不维护两个漂移版本表。
- 使用 setuptools package discovery，新增子包必须进入 wheel；选定 SDK/工具版本写约束文件，不能仅依赖 >= 的任意未来版本。
- verify_release(project_dir: Path, *, python_executable: Path, output_dir: Path) -> VerificationResult；本任务定义 CheckOutcome(success: bool, detail: str)、VerificationResult(success: bool, checks: dict[str, CheckOutcome], artifacts: tuple[Path,...])。命令支持 --offline，网络依赖不可用时明确报告待验，未完成必需检查则success=False。

~~~python
def test_wheel_installs_and_imports_outside_source_tree(installed_env):
    result = installed_env.run(
        ["-c", "import k8s_inspector; import k8s_inspector.alert_routing"],
        cwd=installed_env.empty_directory,
    )
    assert result.returncode == 0
    assert str(installed_env.source_directory) not in result.module_path
~~~

installed_env fixture 构建 wheel 并安装进隔离 venv，module_path 来自包 __file__，不是 shell 环境猜测。增加 test_cli_help_from_installed_wheel、test_declared_runtime_imports、test_optional_mcp_not_required_for_cli。

- [ ] 保存完整版本基线，记录解释器与已安装依赖；重新运行 python -m pytest tests/ -q，不沿用历史163测试作为本轮结果。
- [ ] 写隔离安装测试，运行 python -m pytest tests/test_installed_package.py -q。
- [ ] 修打包发现/依赖单源；落实3.9/3.11/3.14实际运行结果，不兼容时提出明确版本声明变更。
- [ ] 用 python -m build 构建，隔离安装验证；将构建工具纳入开发约束。
- [ ] 记录安装产物/hash/测试结果与未完成的工具验证，提交。

## E2：部署分层、状态持久化与最小权限

**文件：** 调整 deploy/kustomization.yaml，新增 deploy/base/、deploy/overlays/example/、deploy/components/bridge/ 和可选 monitoring/MCP 组件；新增 tests/test_deployment_contract.py。

**约定：**

- 默认基础只含 inspector 所需资源；不默认安装与用户已有 Prometheus/Alertmanager 重复的实例。现有一体示例保留为明确 example overlay。
- cluster_id、镜像引用、输出 StorageClass、状态 StorageClass、群配置和已有监控地址在 overlay 注入；取消硬编码 NFS服务器/IP。
- Bridge async 组件挂独立状态 PVC，replicas=1、Recreate、非 root、资源限制、startup/readiness/liveness 探针；报告挂载可只读。
- SQLite 文件系统类型必须通过实际 CSI/PV 信息和读写/重启测试核实；ReadWriteOnce 标签本身不证明文件系统适用。
- Inspector read-only RBAC，仅所需资源 get/list/watch；不赋 pods/exec/删除/cluster-admin；Bridge 默认不需要集群读取 token，关闭 automountServiceAccountToken。
- NetworkPolicy 开放 AM→Bridge、Prometheus→metrics、Bridge→必要飞书/代理；API/CSI/DNS出站需求按真实 CNI 验证，不能写一个不可部署的通用 IP 白名单。

~~~python
def test_async_bridge_has_persistent_single_writer(rendered):
    bridge = rendered.deployment("alertmanager-feishu-bridge")
    assert bridge.spec.replicas == 1
    assert bridge.spec.strategy.type == "Recreate"
    assert rendered.mounted_pvc(bridge, "/var/lib/inspector") is not None

def test_inspector_role_has_no_mutating_verbs(rendered):
    verbs = rendered.cluster_role_verbs("k8s-inspector")
    assert not (verbs & {"create", "update", "patch", "delete", "deletecollection"})
~~~

rendered fixture 解析真实 kustomize build 输出，不将原 YAML 当渲染结果。另测 test_base_does_not_install_optional_monitoring、test_secrets_not_embedded_in_configmap、test_bridge_disables_service_account_automount。

- [ ] 写部署契约测试，运行 python -m pytest tests/test_deployment_contract.py -q。
- [ ] 拆 base/component/overlay，并迁移现有样例引用，保持示例可渲染。
- [ ] 执行 kustomize build deploy/overlays/example；测试集群执行 kubectl apply --dry-run=server -k deploy/overlays/example。
- [ ] 在专用测试卷验证 Pod 替换/备份恢复/单 writer，记录 StorageClass 与实际 volume 语义。
- [ ] 工具或集群不可用项标待验，G2/G3 不通过；审查权限和卷后提交。

## E3：CronJob 期限、报告保留、时区和配置单源

**文件：** 修改 cronjobs-by-frequency.yaml、cleanup-cronjob.yaml、tools/render_monitoring.py；新增 tests/test_scheduling_contract.py、docs/runbooks/report-retention.md。

**接口与约定：**

- render_cronjobs(profiles: Mapping[str, ProfileSpec], output: Path) -> Path；消费 C3 profile catalog，不另外维护频次和 deadline 表。
- activeDeadlineSeconds 至少容纳运行 deadline +30s 停机余量：5m=270、10m=510、hourly=930、daily/full=1830；与 schedule 间隔校验。concurrencyPolicy=Forbid、backoffLimit=0/明确小值，避免业务退出2形成无限 Job 重试。
- 对支持 CronJob timeZone 的目标版本设置 Asia/Shanghai；不支持时显式使用控制器时区并调整 schedule，不能无验证宣称本地0点执行。
- hourly7天/daily30天沿用；latest不按归档删除；unknown/dead/event状态不由报告 cleanup 清理。
- cleanup 先 dry-run，真实删除失败计数和退出码可见；存储容量/权限不足有监控。

~~~python
def test_job_deadline_matches_runner_catalog(rendered, profiles):
    for profile in ("5m", "10m", "hourly", "daily"):
        job = rendered.cronjob_for(profile)
        assert job.active_deadline_seconds == profiles[profile].deadline_seconds + 30
        assert job.concurrency_policy == "Forbid"

def test_cleanup_does_not_target_latest_or_bridge_state(rendered):
    args = rendered.cleanup_args()
    assert "/data/out/archive" in args
    assert "/var/lib/inspector" not in args
~~~

- [ ] 写期限/时区/清理契约测试并运行 python -m pytest tests/test_scheduling_contract.py -q。
- [ ] 实现 catalog 生成与示例参数，明确创建失败/业务 FAIL/程序错误的 Job 策略。
- [ ] 真实测试运行一个 profile，记录耗时/API请求量/最终退出；跨时区 schedule 验证。
- [ ] cleanup dry-run 校验名单后在测试归档执行，验证删除失败不计成功。
- [ ] 审查配置漂移检测、存储互斥与报告保留说明，提交。

## E4：CI、真实产物与规则检查

**文件：** 完善 tools/verify_release.py；新增 .github/workflows/ci.yml 作为 GitHub 示例，或按实际托管平台适配；修改 Dockerfile；新增 tests/test_release_artifacts.py。

**发布检查：**

- 源码测试→wheel隔离安装→Docker镜像入口→Kustomize build→promtool check config/rules→promtool test rules；每项独立记录，失败阻止候选版本。
- 镜像使用选定Python3.11基础版本/digest、依赖约束、非root；部署按镜像digest固定。CLI与Bridge不同command覆盖必须真实运行 smoke。
- 新增模块、extras和配置样例进入相应产物；镜像不带真实kubeconfig、Secret和审计运行数据。
- Python3.9支持声明与3.14回归纳入矩阵；FastMCP extra 单独测试，不让未安装 extra 的 CLI 崩溃。
- schema 向后读与新版本拒绝沿用B7测试；回退使用匹配schema的程序或备份恢复，不直接滚回未知旧版本。

~~~python
def test_image_cli_and_bridge_entrypoints(image_runner):
    assert image_runner.run(["python", "-m", "k8s_inspector.cli", "--help"]).returncode == 0
    assert image_runner.run(["python", "-m", "k8s_inspector.alertmanager_bridge", "--help"]).returncode == 0

def test_release_checks_fail_when_promtool_fails(verification_runner):
    result = verification_runner.with_failed_check("promtool").run()
    assert result.success is False
    assert result.checks["promtool"].success is False
~~~

- image_runner.run在fixture中用Docker --entrypoint覆盖默认CLI ENTRYPOINT，执行传入命令；否则Bridge smoke会误走CLI参数解析。
- [ ] 写产物和检查失败测试，运行 python -m pytest tests/test_release_artifacts.py -q。
- [ ] 实现可本地执行的验证入口，再接托管平台CI；不自动发布镜像/包。
- [ ] 运行 python -m build、docker build、kustomize build、promtool check/test；保留版本与输出证据。
- [ ] 完成镜像smoke/隔离导入/备份恢复，不以源码pytest替代。
- [ ] 审查候选版本manifest、digest、schema和回退路径，提交。

## E5：影子运行、故障演练、灰度与回退

**文件：** 新增 docs/runbooks/rollout.md、tests/test_rollout_scenarios.py、审计发布验收记录。

**契约与步骤：**

- shadow 运行新规则，输出独立路径/job/profile命名并发专用测试目的地，不能与旧链路争抢Pushgateway grouping key或latest文件。
- 7–14天是建议观察跨度，不是睡眠自动化或无条件发布等待。对daily至少覆盖多次运行与恢复，记录误报/漏报理由、耗时、API数、backlog与dead。
- 演练限专用namespace/测试群：Pod未Ready/init失败、PVC等待消费者/Lost fixture、metrics403/timeout、FAIL→ERROR→有效PASS、Pushgateway清空、Bridge重启、供应商429/超时、DB不可写、创建未知结果（D启用时）。
- 先小范围正式通知，再替换主规则与旧 grouping，防止双通知；保留旧版本/配置/一致备份。
- 发现新漏报、错误恢复、状态无法恢复、队列持续增长就停止扩大范围；回退前导出未完成任务，旧同步模式不能直接遗弃 outbox。

~~~python
def test_shadow_uses_separate_outputs_and_destinations(shadow_config, production_config):
    assert shadow_config.output_dir != production_config.output_dir
    assert shadow_config.pushgateway_group != production_config.pushgateway_group
    assert shadow_config.destination_id != production_config.destination_id
~~~

- [ ] 写shadow隔离与回退保留任务测试，运行 python -m pytest tests/test_rollout_scenarios.py -q。
- [ ] 准备候选版本、差异、演练范围、测试群和回退步骤供上线负责人审阅。
- [ ] 在授权测试范围执行演练，记录实际现象与原因，不仅“请求成功”。
- [ ] 满足G3后再审批正式切换；发布确认来自真实负责人，不用观察期经过代替授权。
- [ ] 完成灰度记录与回退演练，确认历史163测试不是生产可靠性证明。

## E6：运营手册、责任与长期验收

**文件：** 更新 README/AGENTS，新增 docs/runbooks/operations.md，汇总各子计划的验收日志。

**交付：**

- 明确每个cluster/profile/route/team负责人、配置来源、token轮换、事件升级与响应操作。
- 手册包括unknown、dead、backlog、never-ran、旧指标清理、报告保留、schema迁移、备份恢复。
- 运行记录至少包含有效巡检覆盖、当前与历史故障、有效恢复、投递成功/未知/重试、监控失明；SLO目标根据观察数据设定，不预填为已实现。
- 索引连接真实命令/配置及版本；产品能力表区分已实现、可选和未实现。

~~~python
def test_documented_cli_commands_match_parser(documented_commands, installed_env):
    for command in documented_commands.help_commands:
        assert installed_env.run(command).returncode == 0
~~~

- [ ] 用已安装包验证手册中的help/只读命令；不为纯文案写镜像实现的测试。
- [ ] 完成配置、运维、责任、恢复和能力表。
- [ ] 核对全部G1–G4证据，缺环境验证明确列出，不能标“全部完成”。
- [ ] 交接给实际维护负责人，记录未启用D/F与后续范围。
- [ ] 评审并提交；后续迭代按实际故障数据排序。
