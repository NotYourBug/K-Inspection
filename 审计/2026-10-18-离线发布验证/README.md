# 离线发布验证证据

本目录记录 E 批次的原始验证输出，基线为 `ac2a8b9`。执行范围、审查和完整决策台账见上级目录 19/20/21 号文档。

- `baseline-source.zip` 和 `baseline-source-manifest.json`：129 项受版本控制的基线源码文件及 SHA256，逐项核对通过。
- `release-source.zip` 和 `release-source-manifest.json`：修复提交的 174 项源码/配置/测试/文档文件及 SHA256，排除审计运行产物；可供后续真实环境验收。
- 根目录 `E*.log`：各步 RED/GREEN、完整回归、源码外安装包帮助、本地备份恢复与一次性报告归档清理记录。`E4-relative-existing.log` 是原实现已经通过的行为检查。
- `actual-offline/`：第一次诊断运行；安装路径判断 false negative 导致 imports 检查失败，保留原始事实。
- `pre-review-offline/`：安装路径判断修复后的审查前完整验证。
- `final-offline/`：审查修复前的补充完整验证，尚不包括审查指出的三项门禁修复。
- `review-fixed-offline/`：审查修复后最终候选的逐项验证、部署渲染、工具输出及 wheel。
- `python-base-image.json`：官方镜像 registry manifest 与 digest 元数据，未下载容器层或运行容器。
- `validation-tools.json`：本机解释器、promtool、amtool、Kustomize 版本与可执行文件哈希。
- `backup-fixture/`：仅使用虚构告警/目的地标识的本地 SQLite 备份 fixture，不含真实凭据。

原始 JSON 中的绝对路径描述执行时的位置；计划临时目录清理后，应读取本目录对应相对位置的保留文件。虚拟环境和 Python 可执行文件不归档。所有 wheel 均为本地候选/诊断产物，未发布；最终候选由实施记录明确指定。

`success=false`、退出 1 和 PENDING 保留其原义。缺少 Docker、干净依赖安装和真实环境验收不因源码或工具通过而变为发布通过。完整传递依赖锁、Python 版本矩阵、真实 CSI/CNI、独立通知、影子运行及负责人签收仍待验。

`sha256-manifest.json` 在归档完成后生成，排除它自身；保留字节由 `.gitattributes` 的 `-text` 保护，并在提交后核对 Git blob。
