# 更新日志（摘要）

本页从 `git log --oneline -30` 人工归纳，保留最近 10 条，每条一行；提交哈希链到 GitHub 的 commit 页，行内附相关源文件或文档页入口。完整提交历史见 [commits/main](https://github.com/cuihairu/jsonstream/commits/main)。本页不随每次推送自动生成——新条目在文档或站点有实质变化时手工补记。

> 2026-09-30 补记轮：为保持「最近 10 条」口径一次补入七条——四条指定（`26571dd`、`6a906aa`、`3b8afdb`、`ed0e9ec`）＋两条此前漏记（`3c65a07`、本页创建提交 `7f3d566`）＋上游已推的覆盖率口径轮 `d9874c9`；按时间轮换出更早的七条：去 AI 味三轮（`1ffd3a5`、`24f4fa6`、`00f2099`）、README 角色化初版（`4bce5f0`，成果由 `6a906aa` 继承）、tcp-and-landscape 措辞轮（`95cc0ac`）、pnpm overrides 钉版（`18cf776`，结论沉淀在 [FAQ 依赖版本一节](/faq)）、README/文档站一致性核对（`8a72f33`，其 812/812 语句计数已被 `d9874c9` 的工具链无关表述取代）。

## 2026-09-30

- [`d9874c9`](https://github.com/cuihairu/jsonstream/commit/d9874c9) 覆盖率口径改为只记百分比不记分母：[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 与 [design-notes §9](/design-notes) 同步——语句计数随 Go 工具链版本变化（ci.yml 矩阵的 1.24 与 stable 两腿实测同为 100.0%），故两处不再写死 812/812。
- [`26571dd`](https://github.com/cuihairu/jsonstream/commit/26571dd) 全站链接与锚点校验轮：独立清扫脚本补上 [check-links.mjs](https://github.com/cuihairu/jsonstream/blob/main/docs/.vitepress/check-links.mjs) 不覆盖的两类（docs 各页同页锚点、[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 指向线上站的外链 fragment），全绿；顺带修复 [DESIGN](/DESIGN) 与 [interview-requirements](/interview-requirements) 里两处指称已不存在的 README 节名的陈旧引用；覆盖率 100.0% 与 212 测试总数复测成立。

## 2026-09-29

- [`6a906aa`](https://github.com/cuihairu/jsonstream/commit/6a906aa) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 重排为 30 秒上手 → 核心特性 → 快速示例 → 文档站导航 → 性能基准摘要 → 贡献指引；徽章区新增 GitHub Pages workflow 徽章、codecov 链接收紧到 branch/main（路径对照 [ci.yml](https://github.com/cuihairu/jsonstream/blob/main/.github/workflows/ci.yml) / [pages.yml](https://github.com/cuihairu/jsonstream/blob/main/.github/workflows/pages.yml) / 根 [codecov.yml](https://github.com/cuihairu/jsonstream/blob/main/codecov.yml)）；贡献指引为新增章节。
- [`3b8afdb`](https://github.com/cuihairu/jsonstream/commit/3b8afdb) 新增[协议术语速查](/glossary)：帧类型、Flags（含 opcode 对照）、错误码、配置参数四张表，口径以 [protocol](/protocol) 为单一事实源（FRAGMENT 0x10 与 bit5 Fragmented 注明 Go 实现落地中）；接入侧栏「上手」组与顶部导航，getting-started/api/faq 回链。
- [`ed0e9ec`](https://github.com/cuihairu/jsonstream/commit/ed0e9ec) changelog 接入首页（第 10 张卡片 + 文末全貌句），[FAQ](/faq) 对 README 导航的指称同步为「按读者角色导航」——侧栏、首页卡片、文末清单三处口径一致。
- [`3c65a07`](https://github.com/cuihairu/jsonstream/commit/3c65a07) （此前漏记）README 结构化改版为 简介/特性/快速开始/协议与帧格式/配置参考/性能/生态位对比：配置参考新增 13 字段全表（与 [api](/api) 逐项一致），性能表标注 [bench_test.go](https://github.com/cuihairu/jsonstream/blob/main/bench_test.go) 九个基准函数名并保留空载/高负载双窗口口径注记，9 条真 bug 清单原样保留。
- [`7f3d566`](https://github.com/cuihairu/jsonstream/commit/7f3d566) 本页创建：changelog 摘要页 + 侧栏入口。

## 2026-09-28

- [`a70efd4`](https://github.com/cuihairu/jsonstream/commit/a70efd4) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 头部居中（logo 置顶、五徽章居中）；v0.1.0 发布后恢复 [release 链接](https://github.com/cuihairu/jsonstream/releases/tag/v0.1.0)。
- [`9ba6000`](https://github.com/cuihairu/jsonstream/commit/9ba6000) [check-links.mjs](https://github.com/cuihairu/jsonstream/blob/main/docs/.vitepress/check-links.mjs) 收编外链真实 HEAD 探测（200/301/302 通过、不可达跳过），[FAQ](/faq) 口径同步为程序化探测。
- [`d2accc9`](https://github.com/cuihairu/jsonstream/commit/d2accc9) 「不做 FIN 分片」翻案为大消息分片能力：[protocol §3.4](/protocol#_3-4-分片传输-大消息) 新增 FRAGMENT 帧型与 FlagFragmented 位，五处口径同步；Go 参考实现落地中（详见 [websocket-comparison](/websocket-comparison)）。
