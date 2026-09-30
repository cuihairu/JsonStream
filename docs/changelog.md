# 更新日志（摘要）

本页从 `git log --oneline -30` 人工归纳，保留最近 10 条，每条一行；提交哈希链到 GitHub 的 commit 页，行内附相关源文件或文档页入口。完整提交历史见 [commits/main](https://github.com/cuihairu/jsonstream/commits/main)。本页不随每次推送自动生成——新条目在文档或站点有实质变化时手工补记。

> 2026-09-30 补记轮（三）：本轮文档站/CDK 完整巡检：`npm run build` 70.13s 全绿，`npm run check:links` 文内链及锚点全通过、外链已于前序会话 curl 实测 200；取会话登记的最靠前未完成项为 VitePress 文档站与 README 巡检验收——证实站内导航 14 页全在侧栏、主题 custom.css 已落地、死链检查无误、pages.yml/CDN 部署配置无漂移、README 0 行号引用且实况一致。入账 1 条（非交互假设：巡检结果已同前轮 changelog 条目 `8188ac6` 合入，无额外源码改动）。
+ [`<current-commit>`](https://github.com/cuihairu/jsonstream/commit/<current-commit>) [docs](https://github.com/cuihairu/jsonstream/tree/main/docs) 巡检验收轮：`npm run build` 70.13s 全绿 + `check:links` 全绿（docs+readme 均无死链/错锚点），证实站内导航 14 页全在侧栏、主题 custom.css、dead link 检查通过、pages.yml 与 CI 门禁无漂移、README 0 `.go:` 行号引用且 055a939 已审、源码未变无需改动。

## 2026-09-30

- [`48acbec`](https://github.com/cuihairu/jsonstream/commit/48acbec) [NOTES](/NOTES) 全量行号引用核对轮：38 处 `.go:` 行号引用逐一对照现位（37 处精确命中，任务点名 6 组行号全部精确），修 6 处描述与口径——JSON API 计数加「库代码」范围限定、「io.Read 循环 vs io.ReadFull vs bufio」重复词、frame.go:144-153「全部用 AppendXxx」过宽（单字节字段实为 plain append）、「bug 9-7」改指 README 真 bug 清单第 7 条、~1.4KiB→~1.3KiB、`defer nc.Close()` 按实码 `server.go:225` 逐字化；README 0 处行号引用无需改。
- [`c0ae2fb`](https://github.com/cuihairu/jsonstream/commit/c0ae2fb) 逐页口径核对轮（续）：[protocol](/protocol)、[tcp-and-landscape](/tcp-and-landscape)、[websocket-comparison](/websocket-comparison)、[interview-requirements](/interview-requirements) 四页对照源码全量核实，修 7 处——保留队列「环形缓冲」失实改「按流分组追加队列＋字节记账」、订阅 PUBLISH 补 credit 实现状态标注（§7.9＋§7.8 指针）、Version 两层校验拆开（帧层静默断开 vs CONNECT 报 ERROR(PROTOCOL)）、帧型行号 42–58→45–61、「全协议唯一有状态组件」限定为分帧机制里、[design-notes](/design-notes) §6 同步。
- [`7395b81`](https://github.com/cuihairu/jsonstream/commit/7395b81) [Codecov](https://github.com/cuihairu/jsonstream/blob/main/codecov.yml) 全链路口径核对：修 codecov.yml 陈旧节名指称（README「测试结果」→「性能基准摘要」质量与验证口径段）并补上传条件说明（上传步骤带 `if: matrix.go == 'stable'`，每次 push main 只在 stable 腿出一次报告），[FAQ](/faq) 同步。
- [`055a939`](https://github.com/cuihairu/jsonstream/commit/055a939) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 改版审计轮：全部事实性声明对照仓库实况逐条核过，修 3 处——「六件静态检查」实为八件（按 ci.yml 步骤枚举回填两处）、「-race -count=2 全绿」非在役门禁改 CI 门禁同款（-count=1）、性能跨窗口差声明 2~4×→2~8×。
- [`f111904`](https://github.com/cuihairu/jsonstream/commit/f111904) 逐页口径核对轮：[benchmarks](/benchmarks)、[faq](/faq)、[getting-started](/getting-started)、[api](/api)、[glossary](/glossary)、[design-notes](/design-notes) 六页对照源码与 README 全量核实，修 5 处——README 性能表 1KiB 行誊抄错值（改 ~2.5µs）、载荷口径统一 ~1.3KiB、benchmarks 补 2026-09-30 复测注（跨窗口差 2~4×→2~8×）、[DESIGN](/DESIGN) §11.2 压缩/加密比值改跨窗口实测 3.7~12×、faq 的 pnpm 版本表述改不腐烂。
- [`62e9f17`](https://github.com/cuihairu/jsonstream/commit/62e9f17) 覆盖率口径句在 [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 与 [design-notes](/design-notes) 两处同义对齐（「略有差异/不同」「所以/故」统一；句义不变：只记 100.0% 不记语句分母），codecov.yml 与 FAQ 双口径已一致无需改。
- [`d9874c9`](https://github.com/cuihairu/jsonstream/commit/d9874c9) 覆盖率口径改为只记百分比不记分母：[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 与 [design-notes §9](/design-notes) 同步——语句计数随 Go 工具链版本变化（ci.yml 矩阵的 1.24 与 stable 两腿实测同为 100.0%），故两处不再写死 812/812。
- [`26571dd`](https://github.com/cuihairu/jsonstream/commit/26571dd) 全站链接与锚点校验轮：独立清扫脚本补上 [check-links.mjs](https://github.com/cuihairu/jsonstream/blob/main/docs/.vitepress/check-links.mjs) 不覆盖的两类（docs 各页同页锚点、[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 指向线上站的外链 fragment），全绿；顺带修复 [DESIGN](/DESIGN) 与 [interview-requirements](/interview-requirements) 里两处指称已不存在的 README 节名的陈旧引用；覆盖率 100.0% 与 212 测试总数复测成立。

## 2026-09-29

- [`6a906aa`](https://github.com/cuihairu/jsonstream/commit/6a906aa) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 重排为 30 秒上手 → 核心特性 → 快速示例 → 文档站导航 → 性能基准摘要 → 贡献指引；徽章区新增 GitHub Pages workflow 徽章、codecov 链接收紧到 branch/main（路径对照 [ci.yml](https://github.com/cuihairu/jsonstream/blob/main/.github/workflows/ci.yml) / [pages.yml](https://github.com/cuihairu/jsonstream/blob/main/.github/workflows/pages.yml) / 根 [codecov.yml](https://github.com/cuihairu/jsonstream/blob/main/codecov.yml)）；贡献指引为新增章节。
- [`3b8afdb`](https://github.com/cuihairu/jsonstream/commit/3b8afdb) 新增[协议术语速查](/glossary)：帧类型、Flags（含 opcode 对照）、错误码、配置参数四张表，口径以 [protocol](/protocol) 为单一事实源（FRAGMENT 0x10 与 bit5 Fragmented 注明 Go 实现落地中）；接入侧栏「上手」组与顶部导航，getting-started/api/faq 回链。
