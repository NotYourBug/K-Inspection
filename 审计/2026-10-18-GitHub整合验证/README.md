# GitHub 首次整合与两项审查修复证据

基线为 `94ae59d9c09bfc36d28acb1446f31f375d8f84ac`，PR 为 https://github.com/NotYourBug/K-Inspection/pull/1。

本目录保留最初两条 P1 审查意见、首次整合提交 CI 记录、两个问题各自的失败/成功测试，以及修复后完整本地回归。首次 CI 通过并不能证明后续修复已通过；修复提交的 GitHub Actions 按 PR 的具体 head SHA 核对。

`SHA256SUMS.json` 同时记录本目录文件、本次修改的运行时源码/测试、24 号说明和 `.gitattributes` 的原始字节，不包含清单本身。历史日志保留采集时的路径和字节。Windows 完整回归跳过两个 Docker 测试和一个真实 POSIX 描述符测试，后两类分别由 GitHub 的容器任务和 Linux 矩阵验证。

新状态路径、文件系统和硬件的断电持久性仍需专用测试环境验收；本目录不作该项通过声明。
