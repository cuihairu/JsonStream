# 面试题要求

> 本文归档**题面原文**：分组与条目原样保留，不混入解题思路；每一条的实现现状在文末「实现现状」一节逐条对照（附代码与文档依据）。解题思路与设计权衡见 [design-notes.md](design-notes.md)、[DESIGN.md](DESIGN.md)，本文不重复。

## 题面原文

- **传输可靠性与容错性**
  - 基于 TCP
  - 支持断线重连恢复
  - 支持心跳机制
- **便捷性**
  - 以 JSON 为交互数据
- **高性能**
  - 二进制帧
  - 支持参数化是否启用压缩
  - 支持分包传输（大数据量场景）
  - 支持合并包之后解包（大数据量场景）
  - 支持分包传输/分片（大数据量场景，最好支持分片传输）
- **安全性**
  - 支持可选是否启用加密
- **灵活性**
  - 支持请求/响应模式
  - 支持发布/订阅模式
  - 支持流式传输
  - 支持双工
  - 支持单向发送，无需返回
  - 支持返回错误
  - 支持可选背压
- **工程要求**
  - 对应的测试用例
  - 性能测试
  - 完整的使用例子
  - 相关说明文档

## 实现现状（非题面）

以下逐条对照**不是题面内容**，是本仓库对每一条的实现现状标注，口径以代码事实为准、不夸大。

### 分帧三条的详细现状（分包 / 合并包 / 分片）

题面「高性能」组最后三条围绕同一件事：TCP 字节流上如何承载大数据量。三者现状不同，分开说：

- **支持合并包之后解包 —— 已完整覆盖**。TCP 字节流的粘包/半包正是本协议分帧设计的正面战场：14B 定长头 + 32 位大端长度前缀，解码端 `io.ReadFull` 两段读（先读满头、再按长度读满载荷），`transport.readLoop` 外层再包一层 16 KiB bufio。**粘包**（多帧挤在一次读里）由缓冲批量取字节解决，**半包**（一帧分多次到达）由 ReadFull 内部循环消化——两个机制职责不同、不可互替。代码位置：frame.go:168（ReadFrame）、transport.go:175（readLoop）；专节讨论见 [NOTES.md](NOTES.md) §3、[DESIGN.md](DESIGN.md) §7。
- **支持分包传输 / 分片 —— 已识别的生产化缺口，本轮不做**。当前设计是单帧 16 MiB 上限（frame.go:31，超限直接断开防 OOM）+ 载荷 ≥ 64B 起压缩（transform.go:17）。WebSocket 式的 FIN 位分片重组**明确不做**——这是 [design-notes.md](design-notes.md) §1.2 的架构决策：16 MiB 内一帧装得下，省掉一个分片重组状态机。生产环境若确需任意大小消息，分片是必须补的能力；实现路线一句话：FIN 位 + continuation 帧的重组状态机（可比 RFC 6455 §5.4），作为协议 v2 的独立议题（扩展路径见 [protocol.md](protocol.md) §10）。本轮**不实现**，此处如实标注为缺口而非功能。
- **分包传输（大数据量场景）**——与上一条同指：当前答案「单帧 16 MiB + 压缩」覆盖了绝大多数 JSON 业务消息的体量，超出 16 MiB 的单体消息不在 v1 承诺范围内。

### 其余各条逐项对照

| 题面条目 | 实现现状 | 依据 |
| --- | --- | --- |
| 基于 TCP | 完整支持：裸 TCP 直连（`net.Dialer` / `net.Listener`） | transport.go、client.go:279 |
| 支持断线重连恢复 | 完整支持：会话保留 + 原序重放（at-least-once）、takeover、订阅自动重订 | [protocol.md](protocol.md) §8、client.go（connectLoop） |
| 支持心跳机制 | 完整支持：双向 PING/PONG，读空闲 1.5× 间隔判死 | [protocol.md](protocol.md) §7.2、transport.go:148 |
| 以 JSON 为交互数据 | 完整支持：payload 语义恒为 UTF-8 JSON；路由/主题在 Metadata | [protocol.md](protocol.md) §5 |
| 二进制帧 | 完整支持：14B 定长头 + 32 位大端长度前缀 | [protocol.md](protocol.md) §3、frame.go |
| 支持参数化是否启用压缩 | 完整支持：握手协商生效，每帧 Flags 如实标注，载荷 ≥64B 才实际压 | [protocol.md](protocol.md) §6、transform.go:74 |
| 支持可选是否启用加密 | 完整支持：AES-256-GCM + 32B PSK，先压后加，每帧独立标注 | [protocol.md](protocol.md) §6、transform.go |
| 支持请求/响应模式 | 完整支持：REQUEST→RESPONSE，响应帧自带终结语义 | [protocol.md](protocol.md) §7.4 |
| 支持发布/订阅模式 | 完整支持：SUBSCRIBE/SUBACK/PUBLISH，主题是双向命名空间 | [protocol.md](protocol.md) §7.8 |
| 支持流式传输 | 完整支持：Flags.Stream，N×RESPONSE + 恰一个终结帧 | [protocol.md](protocol.md) §7.5 |
| 支持双工 | 完整支持：Flags.Channel，同一 Stream ID 双向复用，半关闭语义 | [protocol.md](protocol.md) §7.6 |
| 支持单向发送，无需返回 | 完整支持：ONEWAY 永不产生任何响应帧（含错误） | [protocol.md](protocol.md) §7.7 |
| 支持返回错误 | 完整支持：ERROR 帧 + 十个错误码 + 「哪层错误决定是否断连」惯例 | [protocol.md](protocol.md) §7.10、errors.go |
| 支持可选背压 | 完整支持：credit 连接级窗口，默认关闭，交付应用后归还 | [protocol.md](protocol.md) §7.9、credit.go |
| 对应的测试用例 | 已交付：212 个测试/基准/fuzz 函数，库包与示例包覆盖率 100% 且有 CI 门禁 | README「测试与质量结果」 |
| 性能测试 | 已交付：bench 套件（帧编解码/变换管线/回环 RTT） | README 性能表 |
| 完整的使用例子 | 已交付：examples/client + examples/server 端到端跑全部模式 | examples/ |
| 相关说明文档 | 已交付：协议规范、设计、知识点、对照等 docs/ 全套 + 在线站点 | docs/、<https://cuihairu.github.io/jsonstream/> |
