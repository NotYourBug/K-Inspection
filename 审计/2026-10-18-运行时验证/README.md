# 2026-10-18 本地运行时验收证据

本目录记录用户选择的本地运行时验收，基线为 `44b375f70af922258a10353eb98ef6354df1c120`。不修改产品源码，不表示容器、真实集群或供应商验收通过。

- `orchestration/acceptance-summary.json`：最终结构化结论，三个版本本地门槛通过、完整发布仍为 false。
- `orchestration/runtime-matrix-corrected-results.json`：最终三个校验器的实际退出码和耗时。
- `orchestration/installed-command-results.json`：独立安装包的 27 项命令、实际退出码与 wheel SHA。
- `python-*/verification/`：对应版本完整源码测试、wheel、候选哈希清单、安装和本地工具日志、组合配置。
- `python-*/source-venv.cfg` 与 `installed-wheel-venv.cfg`：清理前实际环境配置，均关闭系统 site-packages。
- `orchestration/bootstrap-fast/`：真正干净源环境的安装前包清单、安装日志、pip check、freeze；3.14 初始只有 pip。
- `orchestration/progress.md`：本轮续接台账，4 条新裁定；`previous-phase-context.md` 只是上一轮已归档背景，不能重复算成本轮决策。
- `orchestration/*first*`、`bootstrap-short/`、`python-3.11.16/`、`python-3.9.25/`、`diagnostic-first-wheel/`：首次长路径、字节码编译中止及负向 pip 环境变量误用等未成功尝试。最终结论只取 corrected 结果，不把中止算通过或版本不兼容。
- `archive-map.json`：原始短路径与归档路径映射。日志中的历史绝对路径按采集字节保留，清理后不会继续存在；日志和 wheel 可在此核对。
- `SHA256SUMS.json`：覆盖本目录其他文件、本轮 22/23 号审计、修改的计划和 `.gitattributes`，不包含清单自身；提交后再核对 Git blob 与工作区字节。

三个版本均为 Windows x86_64 uv 管理的 Astral 独立 CPython。每个完整源码测试结果为 476 passed、2 skipped；跳过项都是 Docker 入口测试。安装时关闭字节码编译、使用专属短路径；运行时实际导入与完整测试没有被跳过。3.9 的上游维护提示和 3.14 的弃用提示见 22 号审计与原始日志。

不归档源环境/安装包 venv、SDK 安装目录、Python 可执行文件、共享缓存。已下载的共享工具和 Python 留在仓库忽略的 `.superpowers/tools`，供后续授权验收复用。仅在所有证据提交且哈希核对后，删除本计划专属 scratch 和 `out/ra`；其他计划与输出目录保留。

本地用例覆盖不等于真实 CSI 持久化、CNI 出站策略、供应商成功/未知语义、Linux CI、容器构建/入口或多次 daily shadow 已验收。`release_success=false`、G2/G3 开放；后续顺序见 22 号审计和 production-release 计划。
