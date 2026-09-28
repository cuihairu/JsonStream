# 与 WebSocket 的能力对照

> 基于**代码事实**的逐条对照：WebSocket（RFC 6455）有、本协议没有（或显式不做）的；WebSocket 标准没有、本协议内建的。每条给出协议文档章节或代码位置依据，不夸大、不遮短。设计动机的完整论证见 [design-notes.md](design-notes.md) §1–§2。

## WebSocket 有、JsonStream 没有（或显式不做）

### ① 分片传输（FIN / continuation，任意大小重组）

WebSocket 用 FIN 位 + continuation opcode 把一条逻辑消息拆成任意多帧，接收端重组（RFC 6455 §5.4）。JsonStream 的对应能力**已定稿、形式不同**：独立 `FRAGMENT` 帧类型（0x10）+ `FlagFragmented` 标志位（[protocol.md](protocol.md) §3.4）——变换后超过单帧上限（16 MiB）的消息自动拆成连续 chunk 运行，重组总量以 `MaxMessageSize`（默认 64 MiB）为帽。与 WebSocket 的差异是形式而非能力：续段不带元数据与变换标志（只随开帧走一次）、运行不可穿插由唯一写循环保证、接收端因此无需块序号；WebSocket 的重组状态机则为浏览器流式解析设计，两者各取所需。**如实标注**：规范已定稿，Go 参考实现的分片代码在落地中——main 分支当前版本对 `FlagFragmented` 帧按保留位违规断开，互通须等实现合入；两次决策（原「不做」与翻案）的理由都留档在 [design-notes.md](design-notes.md) §1.2，逐条现状见 [interview-requirements.md](interview-requirements.md) 的分帧三条。

### ② 浏览器原生可达

`new WebSocket(url)` 是浏览器唯一的标准全双工通道。JsonStream 跑在 raw TCP 上（client.go:279–281，`net.Dialer` 裸拨），浏览器 JS 没有裸 TCP socket API——Web 前端接入需要自建桥接网关（ws ↔ JsonStream 代理），本仓库不提供。

### ③ TLS 一等承载与 443/HTTP 栈复用

`wss://` 是 WebSocket 规范内建承载：TLS + HTTP(S) 443 复用 + 代理/防火墙友好。JsonStream 的应用层 AES-256-GCM 定位是"经过中间件仍要保密的端到端段"，明确不替代 TLS（[protocol.md](protocol.md) §10 非目标）。TLS 叠加现状（如实）：

- **服务端开箱可用**：`NewServer(ln net.Listener, cfg)`（server.go:31）直接收 `tls.Listen` 的结果；
- **客户端缺注入点**：`Dial(ctx, addr, cfg)`（client.go:40）内部裸拨 TCP，公开 API 暂无传入 `net.Conn`（如 `tls.Conn`）的入口——需要库侧补一个连接注入参数。

因此穿透企业代理/防火墙的能力弱于 wss。

### ④ 子协议/扩展协商字段

WebSocket 握手有标准的 `Sec-WebSocket-Protocol`（子协议）与 `Sec-WebSocket-Extensions`（扩展，如 permessage-deflate）协商槽。JsonStream 握手是应用层 `CONNECT`/`CONNACK` JSON（handshake.go:10–29）：`version` + 连接参数（compress/encrypt/heartbeat_ms/credit/session_id/auth），没有开放扩展协商位。能力演进走 Version 字段 + 未知类型/非零保留位断开（[protocol.md](protocol.md) §3.2、§4）。

### ⑤ 文本/二进制 opcode 区分

WebSocket 用 opcode 区分 text（0x1）/binary（0x2），文本帧附带 UTF-8 合法性校验义务。JsonStream 的 15 个帧类型全是控制/语义类型（frame.go:42–58），payload 按题面统一为 JSON——这是题面约束下的简化而非能力缺陷，但代价是协议失去承载非 JSON 载荷的官方位置（要扩展需动 Type 表或 Flags）。

### ⑥ 客户端帧 Masking —— 明确不需要

WebSocket 强制客户端→服务端帧掩码，防的是"不可信脚本借浏览器 HTTP 栈发出伪造请求污染中间代理缓存"的历史威胁（RFC 6455 §5.3）。JsonStream 是专用客户端/服务端 raw TCP 直连，链路上不存在共享的 HTTP 缓存中间盒，**威胁模型不存在，掩码明确不做**（[design-notes.md](design-notes.md) §1.2）；恶意输入的防线由 Magic 首字节判废、16 MiB 上限、解压上限承担。

## WebSocket 标准没有、JsonStream 内建

WebSocket 只有"一条连接上的无序消息"，以下六项在 WS 应用里都要**应用层自造约定**；JsonStream 把它们做进协议帧语义：

| 能力 | WebSocket 生态现状 | JsonStream 机制（依据） |
| --- | --- | --- |
| 请求/响应 | 无消息关联原语，自造请求 ID + 回调表 | `REQUEST`→`RESPONSE` 同 Stream ID 关联，响应帧自带终结语义（[protocol.md](protocol.md) §7.4） |
| 发布/订阅 | 无订阅概念，自造 topic 协议与确认 | `SUBSCRIBE`/`SUBACK`/`PUBLISH`，主题是双向命名空间（[protocol.md](protocol.md) §7.8） |
| 流式传输 | 无多帧流语义，自造序列号与 EOF 约定 | `Flags.Stream`：N×RESPONSE + 恰一个终结帧，Emitter 逐帧（[protocol.md](protocol.md) §7.5） |
| credit 背压 | 无对端流控信令（浏览器 `bufferedAmount` 只是本地发送队列提示） | `CREDIT` 帧逐条授权、交付应用后归还，握手协商生效值，默认关闭（[protocol.md](protocol.md) §7.9） |
| 断线恢复 | 无会话/重放语义，断线即状态全丢 | `session_id` + 服务端保留队列原序重放（at-least-once）+ takeover + 订阅重订（[protocol.md](protocol.md) §8） |
| 多路复用 | 一连接即一"流"，并发交互需自造 ID 复用 | 每帧必带 Stream ID（控制帧除外），发起方奇偶划分，计数器跨重连单调（[protocol.md](protocol.md) §7.3） |

## 一句话总结

WebSocket 的核心价值是**可达性**（浏览器、443、代理友好），语义全靠应用层自造；JsonStream 反过来——放弃可达性（raw TCP、无浏览器通道），把交互语义（req/res、pub/sub、流式、背压、恢复）做进协议。两者的能力集合几乎不重叠，详细定位与更多协议的横评见 [tcp-and-landscape.md](tcp-and-landscape.md)。
