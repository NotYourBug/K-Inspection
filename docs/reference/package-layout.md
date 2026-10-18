# Python 包结构与导入迁移

2026-10-18 按职责整理 Python 模块。`k8s_inspector/` 根目录由 40 个 Python
文件收敛为 6 个，保留包标记、巡检 CLI、MCP 和三个稳定服务命令入口。
业务规则、配置项、退出码、Report v2 和 Bridge SQLite schema3 保持原有语义。

## 职责与边界

| 包 | 负责内容 | 依赖约束 |
| --- | --- | --- |
| `core` | 配置、模型、证据、profile、资源解析、原子文件写入 | 不导入 SDK、报告、监控或通知适配器 |
| `inspection` | Kubernetes 采集、快照、规则目录、执行、状态与兼容投影 | `rules` 中只做纯求值，不做 SDK/文件/网络 I/O |
| `notifications.bridge` | HTTP 接收、鉴权、路由、就绪指标、本地维护命令 | 组合投递和供应商能力 |
| `notifications.delivery` | SQLite inbox/outbox、身份、投递适配与重试 | 导入 store 不初始化 HTTP 服务或飞书 SDK |
| `notifications.providers` | 飞书 app/webhook 客户端与错误处理 | 处理供应商协议 |
| `reporting` | JSON/CSV/HTML、摘要、隔离输出和 run-status | 依赖共享模型与 `core.files` |
| `monitoring` | Prometheus/Pushgateway 与期望目标清单导出 | 使用共享证据与巡检目录 |

各级 `__init__.py` 只标记包，不批量重导出模块或启动服务。共享原子写入从报告模块
移入 `core.files`，使巡检状态持久化不再反向依赖报告层。重命名后，规则目录的动态
导入同步改用 `inspection.rules.<domain>`。

## 保留的命令

```bash
k8s-inspector --help
python -m k8s_inspector.cli --help
python -m k8s_inspector.alertmanager_bridge --help
python -m k8s_inspector.bridge_admin --help
python -m k8s_inspector.expected_targets --help
python -m k8s_inspector.mcp_server
```

Bridge、admin 和 inventory 根模块只转交 `main`。现有 Docker/Kustomize 命令无需
调整；不要从这些根模块导入服务实现。CLI 与 MCP 主体本次保留原位置。
MCP 已有对共享资源解析的依赖，现改为 `core.resources`；它不是可独立复制的单文件
发行物。MCP 可选依赖和独立打包仍按 F 阶段单独处理。

## 内部导入迁移

下面是完整的旧路径到新路径映射，均省略 `k8s_inspector.` 前缀。例如：

```python
from k8s_inspector.core.config import InspectorConfig
from k8s_inspector.inspection.runner import run_inspection
from k8s_inspector.notifications.delivery.store import DeliveryStore
```

旧内部模块不保留同名兼容文件。项目内源码、测试、发布工具的导入已迁移；外部扩展
脚本如果直接依赖这些内部路径，需要按表更新。历史审计记录和旧计划保留原始路径，
用于追溯当时版本。

| 旧模块 | 新模块 |
| --- | --- |
| `common` | `core.resources` |
| `config` | `core.config` |
| `models` | `core.models` |
| `observation_models` | `core.observations` |
| `profiles` | `core.profiles` |
| `collector` | `inspection.collector` |
| `snapshot` | `inspection.snapshot` |
| `kube` | `inspection.kube` |
| `checks` | `inspection.checks` |
| `compatibility` | `inspection.compatibility` |
| `observation_state` | `inspection.state` |
| `runner` | `inspection.runner` |
| `rule_catalog` | `inspection.catalog` |
| `rule_evaluation` | `inspection.rules.evaluation` |
| `rules_nodes` | `inspection.rules.nodes` |
| `rules_pods` | `inspection.rules.pods` |
| `rules_storage` | `inspection.rules.storage` |
| `rules_events` | `inspection.rules.events` |
| `rules_control` | `inspection.rules.control` |
| `alertmanager_bridge` | `notifications.bridge.server` |
| `alert_routing` | `notifications.bridge.routing` |
| `bridge_auth` | `notifications.bridge.auth` |
| `bridge_admin` | `notifications.bridge.admin` |
| `bridge_observability` | `notifications.bridge.observability` |
| `state_store` | `notifications.bridge.legacy_state` |
| `bridge_store` | `notifications.delivery.store` |
| `delivery_models` | `notifications.delivery.models` |
| `delivery_identity` | `notifications.delivery.identity` |
| `delivery_adapter` | `notifications.delivery.adapter` |
| `delivery_worker` | `notifications.delivery.worker` |
| `feishu_app` | `notifications.providers.feishu` |
| `notify` | `notifications.providers.webhook` |
| `provider_error` | `notifications.providers.errors` |
| `report` | `reporting.writers` |
| `sinks` | `reporting.sinks` |
| `metrics` | `monitoring.metrics` |
| `expected_targets` | `monitoring.inventory` |

原 `report` 中的 `atomic_write_text`、目录创建与持久化替换实现移入 `core.files`。
新代码应从 `k8s_inspector.core.files` 导入 `atomic_write_text`。

## 验证

`tests/test_package_layout.py` 验证包可发现性、共享核心的依赖隔离、store 导入隔离
和纯规则边界；`tests/test_installed_package.py` 在源码目录外构建/安装 wheel，验证
子包包含情况与原有服务命令。既有巡检、报告、投递恢复和发布测试继续运行。

运行完整测试：`python -m pytest tests/ -v`。测试证据与本次整理的审计记录一并归档；
真实集群、卷与通知接收验收仍以现有 runbook 的门槛为准。
