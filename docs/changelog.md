# 更新日志（摘要）

本页从 `git log --oneline -30` 人工归纳，保留最近 10 条，每条一行；提交哈希链到 GitHub 的 commit 页，行内附相关源文件或文档页入口。完整提交历史见 [commits/main](https://github.com/cuihairu/jsonstream/commits/main)。本页不随每次推送自动生成——新条目在文档或站点有实质变化时手工补记。

## 2026-09-28

- [`1ffd3a5`](https://github.com/cuihairu/jsonstream/commit/1ffd3a5) 参考页措辞再打磨一轮，涉及 [protocol](/protocol)、[DESIGN](/DESIGN)、[NOTES](/NOTES)、[design-notes](/design-notes)。
- [`4bce5f0`](https://github.com/cuihairu/jsonstream/commit/4bce5f0) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 改版为角色分流导航：快速开始置顶、特性一览八条、使用者/SDK 作者/协议实现者三组目录替代平面导航表，20 条站内锚链对照构建产物逐一验证。
- [`24f4fa6`](https://github.com/cuihairu/jsonstream/commit/24f4fa6) 去 AI 味补漏：[protocol](/protocol)、[design-notes](/design-notes)、[interview-requirements](/interview-requirements) 的措辞与加粗精简。
- [`00f2099`](https://github.com/cuihairu/jsonstream/commit/00f2099) 去 AI 味第二轮：[protocol](/protocol)、[DESIGN](/DESIGN)、[NOTES](/NOTES) 等八个参考页的模板化加粗标签与口头禅清理，语义、数字、链接零改动。
- [`95cc0ac`](https://github.com/cuihairu/jsonstream/commit/95cc0ac) 去 AI 味第一轮：[tcp-and-landscape](/tcp-and-landscape) 的「机制要点/优点/缺点」标签改叙述段，首页卡片与 README 简介同步；顺带修正 §4.3 分片残留口径。
- [`18cf776`](https://github.com/cuihairu/jsonstream/commit/18cf776) [pnpm-workspace.yaml](https://github.com/cuihairu/jsonstream/blob/main/pnpm-workspace.yaml) 以 overrides 钉 vite ^6.4.3，清掉 Dependabot 4 条告警（esbuild 随依赖树一并解除），详见 [FAQ 依赖版本一节](/faq)。
- [`a70efd4`](https://github.com/cuihairu/jsonstream/commit/a70efd4) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 头部居中（logo 置顶、五徽章居中）；v0.1.0 发布后恢复 [release 链接](https://github.com/cuihairu/jsonstream/releases/tag/v0.1.0)。
- [`9ba6000`](https://github.com/cuihairu/jsonstream/commit/9ba6000) [check-links.mjs](https://github.com/cuihairu/jsonstream/blob/main/docs/.vitepress/check-links.mjs) 收编外链真实 HEAD 探测（200/301/302 通过、不可达跳过），[FAQ](/faq) 口径同步为程序化探测。
- [`d2accc9`](https://github.com/cuihairu/jsonstream/commit/d2accc9) 「不做 FIN 分片」翻案为大消息分片能力：[protocol §3.4](/protocol#_3-4-分片传输-大消息) 新增 FRAGMENT 帧型与 FlagFragmented 位，五处口径同步；Go 参考实现落地中（详见 [websocket-comparison §①](/websocket-comparison)）。
- [`8a72f33`](https://github.com/cuihairu/jsonstream/commit/8a72f33) README 与文档站一致性核对：语句覆盖计数修正为 812/812，[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 性能表与 [benchmarks](/benchmarks) 页互相注记空载/高负载口径差异。
