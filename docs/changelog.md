# 更新日志（摘要）

本页从 `git log --oneline -30` 人工归纳，保留最近 10 条，每条一行；提交哈希链到 GitHub 的 commit 页，行内附相关源文件或文档页入口。完整提交历史见 [commits/main](https://github.com/cuihairu/jsonstream/commits/main)。本页不随每次推送自动生成——新条目在文档或站点有实质变化时手工补记。

> 2026-10-01 全量链接审计轮：docs 14 页 + README 共 200 条内联链接逐一校验——140 条站内/相对链接与 31 条线上站链接（含全部 fragment）对照 dist 页面与 heading id 真值，29 条 github.com/其他外链 curl 逐一探测全 200，config 侧栏/导航与首页 frontmatter 的全部 link 目标均解析到现存页，无孤儿页（仅首页不在侧栏，属正常），含空格 URL/图片/引用式/blob 行锚点等边角类逐类排查均无——零死链、零错锚、零缺资源，无可修复项。自 `d8d52de` 起窗口内入账 1 条（`2752d56`），按时间轮换出最旧 `26571dd`（其两处陈旧指称修复在各页存续），保持最近 10 条；本轮自身不入条目（非交互假设：清扫脚本为 /tmp 临时件不入库，沿 `26571dd` 前例；单笔提交写不进自身哈希，不复用占位符写法）。

## 2026-10-01

- [`2752d56`](https://github.com/cuihairu/jsonstream/commit/2752d56) benchmarks 实测复核轮：跑 9 靶 × 10 轮取 2026-10-01 轻载窗口读数，[benchmarks](/benchmarks) 正文三表与逐项解读按实测改写（旧 09-28 高负载数据与 09-30 复测注降级「历史窗口对照」，补端到端载荷口径）、[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 性能表与跨窗口差声明同源同步（2~8×→3.5~8.5×）、[DESIGN](/DESIGN) §9.1/§11.2/§12 倍数引用随之协调（3.7~12×→3.6~12×），线速单位勘正 MiB/s→MB/s。
- [`b64bd8c`](https://github.com/cuihairu/jsonstream/commit/b64bd8c) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 对照 docs 站点现状改版：快速上手六签名（NewServer/Handle/Serve/Dial/Request/Decode）与 13 字段配置表对照源码逐段核对全精确；徽章区新增文档站直达徽章（Codecov 徽章与站点/changelog 链接核对为已存在有效项，不重复添加）；修两处口径——基准「九个基准的实跑口径」与表列七行矛盾改「摘自九靶中七靶」（压+加与加密端到端两靶无空载存档值，不跨窗口混填）、测试计数「212 个测试/基准函数」按 [interview-requirements](/interview-requirements) 口径补「/fuzz」（198 Test+9 Benchmark+5 Fuzz=212 复测精确）。

## 2026-09-30

- [`46d0970`](https://github.com/cuihairu/jsonstream/commit/46d0970) 逐页口径核对收尾轮：[DESIGN](/DESIGN) 与站点首页对照源码/README 全量核实（前两轮已覆盖其余各页，DESIGN 此前仅 §11.2 顺带勘正、index 从未核过），修 14 处——「五个动词/五个发起原语」实为六个（补 Publish，endpoint do* 恰六个，endpoint.go:349-461）、net.Conn 豁免补 Client 拨号侧、transport 片段补 log/onDead 两字段、初始栈 ~8KB→~2KB（runtime stackMin=2048，[NOTES](/NOTES) 两处同步）、nil 接收者行收窄至 ReadStream（Subscription 无 nil 防护）、flate/GCM「25×/7 倍」统一改跨窗口 3.7~12× 引 §11.2、迁移流按 bind 实码改奇偶口径（奇数流连 handler 迁、偶数流不迁）、首页 Publish 示例未定义变量补具体字面量。
- [`48acbec`](https://github.com/cuihairu/jsonstream/commit/48acbec) [NOTES](/NOTES) 全量行号引用核对轮：38 处 `.go:` 行号引用逐一对照现位（37 处精确命中，任务点名 6 组行号全部精确），修 6 处描述与口径——JSON API 计数加「库代码」范围限定、「io.Read 循环 vs io.ReadFull vs bufio」重复词、frame.go:144-153「全部用 AppendXxx」过宽（单字节字段实为 plain append）、「bug 9-7」改指 README 真 bug 清单第 7 条、~1.4KiB→~1.3KiB、`defer nc.Close()` 按实码 `server.go:225` 逐字化；README 0 处行号引用无需改。
- [`c0ae2fb`](https://github.com/cuihairu/jsonstream/commit/c0ae2fb) 逐页口径核对轮（续）：[protocol](/protocol)、[tcp-and-landscape](/tcp-and-landscape)、[websocket-comparison](/websocket-comparison)、[interview-requirements](/interview-requirements) 四页对照源码全量核实，修 7 处——保留队列「环形缓冲」失实改「按流分组追加队列＋字节记账」、订阅 PUBLISH 补 credit 实现状态标注（§7.9＋§7.8 指针）、Version 两层校验拆开（帧层静默断开 vs CONNECT 报 ERROR(PROTOCOL)）、帧型行号 42–58→45–61、「全协议唯一有状态组件」限定为分帧机制里、[design-notes](/design-notes) §6 同步。
- [`7395b81`](https://github.com/cuihairu/jsonstream/commit/7395b81) [Codecov](https://github.com/cuihairu/jsonstream/blob/main/codecov.yml) 全链路口径核对：修 codecov.yml 陈旧节名指称（README「测试结果」→「性能基准摘要」质量与验证口径段）并补上传条件说明（上传步骤带 `if: matrix.go == 'stable'`，每次 push main 只在 stable 腿出一次报告），[FAQ](/faq) 同步。
- [`055a939`](https://github.com/cuihairu/jsonstream/commit/055a939) [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 改版审计轮：全部事实性声明对照仓库实况逐条核过，修 3 处——「六件静态检查」实为八件（按 ci.yml 步骤枚举回填两处）、「-race -count=2 全绿」非在役门禁改 CI 门禁同款（-count=1）、性能跨窗口差声明 2~4×→2~8×。
- [`f111904`](https://github.com/cuihairu/jsonstream/commit/f111904) 逐页口径核对轮：[benchmarks](/benchmarks)、[faq](/faq)、[getting-started](/getting-started)、[api](/api)、[glossary](/glossary)、[design-notes](/design-notes) 六页对照源码与 README 全量核实，修 5 处——README 性能表 1KiB 行誊抄错值（改 ~2.5µs）、载荷口径统一 ~1.3KiB、benchmarks 补 2026-09-30 复测注（跨窗口差 2~4×→2~8×）、[DESIGN](/DESIGN) §11.2 压缩/加密比值改跨窗口实测 3.7~12×、faq 的 pnpm 版本表述改不腐烂。
- [`62e9f17`](https://github.com/cuihairu/jsonstream/commit/62e9f17) 覆盖率口径句在 [README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 与 [design-notes](/design-notes) 两处同义对齐（「略有差异/不同」「所以/故」统一；句义不变：只记 100.0% 不记语句分母），codecov.yml 与 FAQ 双口径已一致无需改。
- [`d9874c9`](https://github.com/cuihairu/jsonstream/commit/d9874c9) 覆盖率口径改为只记百分比不记分母：[README](https://github.com/cuihairu/jsonstream/blob/main/README.md) 与 [design-notes §9](/design-notes) 同步——语句计数随 Go 工具链版本变化（ci.yml 矩阵的 1.24 与 stable 两腿实测同为 100.0%），故两处不再写死 812/812。

