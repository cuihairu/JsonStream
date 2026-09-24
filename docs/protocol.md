# JsonStream 协议规范 v1

> 状态：草案 / 参考实现随本仓库提供（Go）。
> 参考：WebSocket（RFC 6455，二进制帧与心跳惯例）、RSocket（交互模型与恢复语义）、MQTT（SUBACK 惯例）。

## 1. 概述

JsonStream 是一个基于 TCP 的二进制帧协议，承载 JSON 业务数据。目标是在"题面五个诉求"上各给一个明确的机制：

| 题面诉求 | 协议机制 |
| --- | --- |
| 传输可靠性 | 长度前缀分帧 + 心跳 + 会话保留与重连恢复 |
| 便捷性 | Payload 为 UTF-8 JSON；路由/主题放 Metadata |
| 高性能 | 固定 14 字节帧头；可选 flate 压缩 |
| 安全性 | 可选 AES-256-GCM 加密；压缩/加密按连接参数化 |
| 灵活性 | 请求/响应、流式、双工、单向、发布/订阅、错误、信用背压七种语义，共用一套帧格式 |

设计原则：**帧层只管分帧与寻址，语义层（模式）跑在 Stream 上**。所有模式复用同一种帧，靠帧类型 + Flags 区分，避免"每种模式一套帧头"的膨胀。

## 2. 术语

- **帧（Frame）**：线上最小传输单元，由定长头部 + 可变载荷组成。
- **Stream**：一次逻辑交互（一个请求、一条订阅、一个双工通道），由 StreamID 标识，生命周期可跨多帧。
- **连接（Connection）**：一条 TCP 连接，同一时刻至多承载一个**会话（Session）**。
- **会话（Session）**：逻辑状态（Stream 注册表、订阅关系、待重放队列），可跨越 TCP 断连存活，用于恢复。

## 3. 帧格式

### 3.1 布局（大端序）

```
 0                   1                   2                   3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic = 0x4A53 ("JS")         | Version (1B)  | Flags  (1B)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Type   (1B)   | Reserved (1B) |        Stream ID (4B)         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Payload Length (4B)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Meta Length (2B, 仅 Flags.HasMeta) | Metadata (JSON, 明文)    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Payload（可压缩/加密，长度 = Payload Length）                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- 无 Metadata 时帧头固定 **14 字节**；有 Metadata 时 16 字节 + Meta 内容。
- `Payload Length` 计的是**变换后**（压缩/加密后）的字节数；`Meta Length` 计明文 JSON。

### 3.2 字段说明

| 字段 | 大小 | 说明 |
| --- | --- | --- |
| Magic | 2B | 固定 `0x4A 0x53`（"JS"）。粘包/串扰时快速判废，第一个字节错了就立即断开，不必等长度字段把连接带沟里。 |
| Version | 1B | 协议版本，当前 `1`。握手时双方校验，不匹配回 `ERROR(PROTOCOL)` 并断开。 |
| Flags | 1B | bit0 `Compressed`，bit1 `Encrypted`，bit2 `HasMeta`，bit3 `Stream`（见 §7），bit4 `Channel`，bit5–7 保留（必须为 0）。 |
| Type | 1B | 帧类型，见 §4。 |
| Reserved | 1B | 对齐保留，必须为 0。将来可升级为扩展标志位。 |
| Stream ID | 4B | 连接内唯一的流标识，0 表示"不属于任何流"（仅握手/心跳/连接级错误使用）。 |
| Payload Length | 4B | 上限 `16 MiB`。解码端读到超限值必须断开（防恶意长度导致 OOM）。该上限同样约束**解压后**的逻辑载荷：压缩比不受发送方约束，flate 解压结果超过 `16 MiB` 视为协议违规（防解压炸弹，同样 OOM 防线）。 |
| Meta Length | 2B | 仅 `Flags.HasMeta` 时出现，上限 65535（64 KiB−1：长度字段是 uint16，恰 64 KiB 无法在线上表达，编码端超限即拒）。`HasMeta` 置位而长度为 0 合法——编码端 flag 置位即写段（长度可为 0），保证解析-编码互逆。 |

### 3.3 为什么是"长度前缀 + 类型字段"

TCP 是无边界的字节流，任何协议第一件事是分帧。三种主流做法：

1. **定界符**（如 Redis 的 `\r\n`、旧文本协议）：实现最简单，但二进制载荷需要转义，无法承载压缩/加密后的不透明字节——压缩后的数据里出现任何字节序列都是合法的。**JsonStream 的 payload 要压缩/加密，这条路直接被堵死。**
2. **长度前缀**（本协议、WebSocket 的 length 字段、RSocket 的 FRAME_LENGTH、HTTP/2 的 Length）：读定长头就能精确知道还差多少字节，`io.ReadFull` 两次即完成一帧；接收端可以预分配精确大小的缓冲；不需要扫描/转义。代价是发送端必须先知道长度——对"整条消息编码完成再发送"的模型毫无负担。
3. **自描述流**（如 Protobuf varint 分帧）：省几个字节，但解码状态机复杂，且不利于"读到帧头就能做准入控制"（如 §3.2 的长度上限检查）。

长度前缀里放的是**紧随其后的载荷长度**（不是整帧长度），这样接收端读完 14 字节头即可完成全部准入检查（魔数、版本、长度上限），再做两次 `ReadFull`，中途不用回改。

**类型字段**解决的是复用问题：一条 TCP 连接上要同时跑心跳、握手、多种交互模式的帧。没有类型，接收端无法区分"这 20 字节是心跳还是半个请求"。WebSocket 用 opcode、RSocket 用 frame type、MQTT 用 packet type——都是同一个答案：**帧类型是连接复用的最小前提**。本协议显式列出全部类型（§4），未知类型一律 `ERROR(PROTOCOL)` 断开，为将来升级留出"新类型 = 新能力"的演进路径。

## 4. 帧类型

| 值 | 名称 | 方向 | Stream ID | 用途 |
| --- | --- | --- | --- | --- |
| 0x01 | `CONNECT` | C→S | 0 | 握手，携带能力声明与会话恢复凭证 |
| 0x02 | `CONNACK` | S→C | 0 | 握手应答，下发会话 ID 与生效参数 |
| 0x03 | `PING` | 双向 | 0 | 心跳探测 |
| 0x04 | `PONG` | 双向 | 0 | 心跳应答 |
| 0x05 | `REQUEST` | 双向 | ≠0 | 发起一次请求（单帧响应/多帧流/双工通道，见 Flags） |
| 0x06 | `RESPONSE` | 双向 | ≠0 | 流上的一帧数据 |
| 0x07 | `COMPLETE` | 双向 | ≠0 | 流正常终止 |
| 0x08 | `CANCEL` | 双向 | ≠0 | 取消流：立即终结（状态机见 §7.3） |
| 0x09 | `ONEWAY` | 双向 | ≠0 | 单向发送，永不产生任何响应帧 |
| 0x0A | `ERROR` | 双向 | ≠0/0 | 终结某个流；Stream ID=0 表示连接级致命错误（随后必须断开） |
| 0x0B | `SUBSCRIBE` | C→S | ≠0 | 订阅主题（Stream ID 即该订阅的流标识） |
| 0x0C | `SUBACK` | S→C | ≠0 | 订阅确认 |
| 0x0D | `UNSUBSCRIBE` | C→S | ≠0 | 退订；成功不回帧，失败回 `ERROR` |
| 0x0E | `PUBLISH` | 双向 | ≠0* | C→S：发布到服务端主题；S→C：向订阅者投递（*由订阅/发布动作关联，本身不开启流） |
| 0x0F | `CREDIT` | 双向 | ≠0 | 背压授权：补 n 条发送额度，见 §7.7 |

收到任何未定义类型：回 `ERROR(PROTOCOL)` 并断开。

> 恢复不走独立帧：客户端在 `CONNECT` 里携带 `session_id`，服务端以 `CONNACK.resumed` 确认是否恢复成功（MQTT session-present 的惯例），见 §8。

## 5. Metadata 与 Payload

- **Metadata**：UTF-8 JSON 对象，承载路由与主题等"协议层键"：`{"route":"echo"}`、`{"topic":"ticks"}`。
  - `REQUEST`/`ONEWAY` 必带 `route`；`SUBSCRIBE`/`UNSUBSCRIBE`/`PUBLISH` 必带 `topic`；其余帧不带 Metadata。
  - **为什么放 Metadata 而不是 Payload**：路由信息是协议元数据，与业务数据解耦后，中间件（鉴权、限流、可观测性）不必解码 payload 就能路由；同时压缩/加密只变换 payload，路由保持明文可审计。代价见 design-notes §4。
- **Payload**：业务数据，语义上恒为 JSON（变换前）。空载荷允许（长度 0）。

## 6. 变换管线：压缩与加密的参数化

发送侧固定顺序：**JSON 编码 → flate 压缩（可选）→ AES-256-GCM 加密（可选）**。先压后加密是惯例——加密输出近似随机字节，先加密后压缩得不到任何压缩率。

### 6.1 协商

- `CONNECT` 的 payload 声明客户端**期望**的参数：

  ```json
  {
    "version": 1,
    "compress": true,
    "encrypt": true,
    "heartbeat_ms": 10000,
    "credit": 256,
    "session_id": "…（重连时携带）",
    "auth": "…（可选令牌）"
  }
  ```

- `CONNACK` 下发**实际生效**的参数与凭证：

  ```json
  {
    "session_id": "s-3fa9…",
    "resumed": false,
    "compress": true,
    "encrypt": true,
    "heartbeat_ms": 10000,
    "credit": 256,
    "retention_ms": 30000
  }
  ```

- **生效规则：以服务端配置为准**。压缩/加密按「双方都声明才启用」生效（`生效 = 服务端开启 && 客户端开启`）；服务端要求加密而客户端未声明，回 `ERROR(AUTH_DENIED)` 拒绝连接。`credit` 取双方声明中大于 0 者的**最小值**（任一方为 0 即整体关闭）。`heartbeat_ms` 与保留期由服务端权威下发。每帧 Flags 如实标注本帧是否压缩/加密，**解码端按帧内 Flags 走管线，不依赖协商结果**——这样单连接内可以混合发送（例如心跳与握手永远明文直发，大 payload 才压缩）。
- **阈值与明文范围**：Payload ≥ 64 字节才压缩（JSON 短消息压缩后常不降反升，flate 头开销吃掉收益）；`CONNECT/CONNACK` 握手帧恒为明文（它们本身就是协商凭据，且此刻变换参数尚未生效）。
- 加密密钥：本版本使用**预共享密钥（PSK, AES-256-GCM）**，由两侧配置注入，不在线上传输。每帧随机 12 字节 nonce，前置于密文。生产环境的完整答案是在 TLS 之上运行本协议（ Flags.Encrypted 留给应用层端到端加密），见 design-notes §3。

### 6.2 每帧开销

| 配置 | 开销 |
| --- | --- |
| 不压缩不加密 | 0 |
| 压缩 | flate 流内自含（preset dict 未用），0 字节额外 |
| 加密 | 12B nonce + 16B GCM tag = 28B |

## 7. 连接与交互语义

### 7.1 握手

```
C→S  CONNECT {version, compress, encrypt, heartbeat_ms, credit, [session_id], [auth]}
S→C  CONNACK  {session_id, resumed, …生效参数}
     或 ERROR(AUTH_DENIED / PROTOCOL / UNSUPPORTED) 后断开
```

- 版本不匹配 → `ERROR(PROTOCOL)` 断开。
- 携带 `session_id` 且服务端保留期未过 → `CONNACK{resumed:true}`，进入恢复流程（§8）。

### 7.2 心跳

- 双方各自以 `heartbeat_ms` 间隔发送 `PING`，对端收到任何帧（含 PING）即视为活跃并回 `PONG`。
- **读空闲超时 = `heartbeat_ms × 1.5`**：超过该时长未收到任何入站帧即断开。任何入站帧都会重置计时器——业务繁忙时心跳帧自动"免费"，这正是 WebSocket/MQTT 心跳的通行惯例。
- 心跳超时断开后：服务端为该会话启动保留期（§8）；客户端触发自动重连。

### 7.3 Stream 生命周期与 ID 分配

状态机：

```
            REQUEST/SUBSCRIBE
                 │
                 ▼
   ┌──→ ACTIVE ──┼──→ COMPLETE ──→ CLOSED
   │      │      │
   │      │      └──→ ERROR ────→ CLOSED
   │      │
   │      └──────────→ CANCEL(收到) → CLOSED
   └─ CANCEL(发出)：立即终结本流，停止收发（发送方反悔不需要它；
      双工通道的"我说完了"是半关闭，用 COMPLETE 表达，见 §7.6）
```

**ID 分配惯例：请求发起方分配，且客户端只用奇数、服务端只用偶数**，各自单调递增。双方可能同时主动发起流（双工/服务端推送），奇偶划分让两套计数器永不撞号——这是在 RSocket 惯例（双方独立递增、依赖实现自觉不复用）上加的一条硬约束。Stream ID 不得复用，直到回绕（4G 上限，实际连接生命周期内不会发生）。

### 7.4 请求/响应（Flags.Stream=0, Channel=0）

```
C→S  REQUEST (meta.route="echo", payload)
S→C  RESPONSE (同一 Stream ID)      ← 单帧即终结，隐含 COMPLETE
```

- **响应帧自带终结语义**，不追加 `COMPLETE`——一次交互 3 个帧（含握手外）降为 2 个。这是对 RSocket `REQUEST_RESPONSE`（next+complete 合一）的直接借鉴。
- 处理失败：`ERROR{code, message}` 终结流，客户端 `Request()` 以 error 返回。
- 超时由客户端 `ctx` 控制；超时后发 `CANCEL`，服务端停止处理。

### 7.5 流式（Flags.Stream=1）

```
C→S  REQUEST (Flags.Stream, meta.route="range")
S→C  RESPONSE ×N    （受背压约束，§7.7）
S→C  COMPLETE       （或 ERROR / CANCEL 提前终止）
```

- 0..N 帧 `RESPONSE` + 恰好一个终结帧（`COMPLETE` / `ERROR` / 来自客户端的 `CANCEL`）。
- 服务端 handler 以 `Emitter.Emit()` 逐帧下发；handler 正常返回即自动 `COMPLETE`，返回 error 则 `ERROR`。

### 7.6 双工（Flags.Channel=1，隐含 Stream=1）

```
A→B  REQUEST (Flags.Channel, meta.route="chat")
B→A  RESPONSE ×…          ← 双方都可发
A→B  RESPONSE ×…
任一方 COMPLETE/CANCEL → 流终结
```

- 发起方 `Channel.Send()` 与对端 handler 的 `Channel.Send()` 都走 `RESPONSE` 帧，同一 Stream ID 双向复用。
- `COMPLETE` 表示"我发完了"（半关闭）；收到对端 `COMPLETE` 且自身已发完，流才整体关闭。任一方 `CANCEL`/`ERROR` 立即双向关闭。
- 与 HTTP/2 双向流、RSocket `REQUEST_CHANNEL` 语义一致。

### 7.7 单向（`ONEWAY`）

```
A→B  ONEWAY (meta.route, payload)      ← 没有然后了
```

- 协议层**保证不产生任何响应帧**，包括错误（路由不存在也静默丢弃，留服务端日志）。一旦回帧，"无需返回"的承诺就被打破了——这与 RSocket `REQUEST_FNF` 的取舍相同：用"不可知送达"换"零等待、零反向流量"。
- 需要确认的场景请用请求/响应。

### 7.8 发布/订阅

```
C→S  SUBSCRIBE (meta.topic="ticks", Stream ID=S1)
S→C  SUBACK (S1)                      ← 确认订阅建立（MQTT 惯例）
S→C  PUBLISH (S1, payload) ×…         ← 向该订阅投递
C→S  UNSUBSCRIBE (S1)                 ← 退订；成功不回帧
```

- `SUBSCRIBE` 的 Stream ID 由客户端分配（奇数），此后服务端投递复用该 ID，订阅的生命周期即流的生命周期；断线恢复时订阅关系随会话保留。
- **反向也成立**：客户端 `PUBLISH`（新分配 Stream ID，携带 topic）→ 服务端 `HandlePublish` 处理，处理失败回 `ERROR`（该流上）。即主题是双向命名空间，方向由谁发起决定。
- 服务端广播 `Publish(topic, v)` 对每个活跃订阅复制一份 `PUBLISH`，受该连接背压约束（§7.9）。
- 订阅不存在的主题：`SUBACK` 成功（主题是"将来会有广播"的命名空间，空主题合法）；服务端 `HandlePublish` 未注册时投递静默丢弃。这与"请求打错路由必须报错"不同——订阅关心的是"我以后能不能收到"，而不是"现在有没有人生产"。

### 7.9 背压（可选，credit-based）

```
B→A  CREDIT (S2, payload={"n": 2})     ← 授权 A 再发 2 条
A→B  RESPONSE ×2 （S2）
B→A  CREDIT (S2, {"n": 2})             ← B 每交付应用一条，累计归还
```

- **模型**：接收方授权额度（credit），发送方额度归零即阻塞（不丢弃、不排队爆内存），接收方把消息交付给应用后归还额度。等价于 Reactive Streams 的 `request(n)` / gRPC 依赖的 HTTP/2 流窗口；与 RSocket `LEASE`（按时间窗口授权速率）不同，见 design-notes §5。
- **作用范围**：流式与双工通道上的多帧数据——`Flags.Stream` 的下行 `RESPONSE`、双工通道的 `RESPONSE`（**双向都受约束**：两端各自在发送前消耗、交付应用后归还）、订阅投递的下行 `PUBLISH`。请求/响应与 `ONEWAY` 单帧即终结，不纳入 credit（约束它们没有意义，反而增加一次 RTT）。
- **协商**：`CONNECT/CONNACK` 的 `credit` > 0 表示启用，生效值取双方声明中大于 0 者的**最小值**（与 §6.1 一致）；未启用时发送方无限制（诚实语义：关闭背压 = 信任 TCP 缓冲能兜住，内存风险自担）。
- **粒度**：本版本 credit 是**连接级**共享窗口（实现简单、全局内存有界）；per-stream 独立窗口列为扩展点（§10）。
- **与断线恢复的交互**：额度闸门属于连接，随连接终结——断开期间产生的下行帧改道会话保留队列（§8.1），**不消耗旧连接的额度**（否则闸门死亡会拦截本应保留的帧，破坏 at-least-once）；重放帧同样直接下发、不经发送侧闸门。恢复后的实时帧在新连接的额度下正常收发，客户端对重放帧照常归还额度。

### 7.10 错误

`ERROR` 帧 payload：`{"code": N, "message": "…"}`。携带非零 Stream ID 时终结该流；Stream ID=0 表示连接级致命错误，双方随后必须断开 TCP。

| code | 名称 | 含义 |
| --- | --- | --- |
| 1 | `INTERNAL` | 处理器内部错误（handler panic 也归并为此） |
| 2 | `NOT_FOUND` | 路由不存在 |
| 3 | `INVALID` | 载荷/参数非法（JSON 解码失败等） |
| 4 | `PROTOCOL` | 协议违规（坏魔数、未知类型、模式与 Flags 不符） |
| 5 | `CANCELLED` | 对端已取消（本地用于取消 Err） |
| 6 | `TIMEOUT` | 服务端处理超时（v1 预留：实现未内置处理超时，当前无产生路径；接收方按通用流错误处理即可） |
| 7 | `AUTH_DENIED` | 鉴权失败 / 拒绝连接 |
| 8 | `SESSION_EXPIRED` | 会话过期，恢复失败 |
| 9 | `BUSY` | 过载拒绝 |
| 10 | `UNSUPPORTED` | 对端未启用的能力被错误使用（如要求加密却明文） |

惯例：**协议错误（4）必须断开连接**——状态已不可信；**应用错误（1/2/3/6/9）只终结所在流**，连接继续服务其他流。

## 8. 断线重连恢复

### 8.1 会话保留（服务端）

`CONNACK` 下发 `session_id` 与 `retention_ms`。连接断开后，服务端将该会话保留 `retention_ms`（默认 30s），保留内容：

- 每个未终结流的**下行帧队列**（`RESPONSE`/`PUBLISH`/`SUBACK`/`COMPLETE`/`ERROR`；环形缓冲，默认 4 MiB/会话。`CREDIT` 是连接级流控信号，离开旧连接即无意义，**不保留**）。
- 订阅关系（重连后继续投递）；
- 队列继续接收新帧（如广播消息），重连时一次性重放。

**未终结流跨恢复存活**：客户端发起且未终结的流（含通道）在恢复时随会话迁移到新连接——恢复成功后客户端对这些流的上行帧（通道数据、`CANCEL`、额度回授）继续由原 handler 处理，交互双向打通，而不是只剩下行重放。服务端发起的流（偶数 Stream ID）随旧连接消亡，不迁移（§7.3 的方向对称性：被动方无法替主动方决定"继续"）——连接死亡时服务端侧挂起的发起侧调用（`Request`/`Stream`/`Channel`）立即以连接关闭失败：它们的响应是客户端→服务端的上行，断连后必然丢失，等待没有意义。会话终结（保留期到/禁用保留/溢出）时，仍在执行的流以 `SESSION_EXPIRED` 失败，handler 随之收尾；断开**期间**客户端发出的 `CANCEL` 属上行，按上一条丢失——重连后发出的 `CANCEL` 即时生效。

**溢出**：断开期间队列超过上限（默认 4 MiB）即**清空整个队列**，并将会话标记为不可恢复——重连时服务端直接回 `resumed:false`（§8.2），而不是补发一半丢了帧的历史。理由：此时订阅关系对应的投递序列已经出现空洞，"恢复成功"的承诺无法兑现；诚实降级为全新会话，让订阅重订、挂起流失败，应用以 `OnResumeFailed` 重建状态。

**上行不缓存**：客户端发向服务端的帧随断连丢失。理由：上行重试语义（幂等、去重）只有客户端应用自己能定义，服务端替它缓存既做不对语义、又把内存压力乘上连接数。这与 RSocket Resume 缓存双向流的取舍不同，见 design-notes §6。

### 8.2 恢复流程

```
（断连后客户端自动重连，指数退避 + 抖动）
C→S  TCP 连接 + CONNECT {session_id}
S→C  CONNACK {resumed:true, retention_ms, …}
S→C  （按 Stream ID 分组、原序重放保留的下行帧）
     ……此后实时帧与重放帧不再区分，按到达序发送
```

- 服务端校验会话存在且未过期、队列未溢出 → `CONNACK{resumed:true}`，把新连接绑到旧会话，冻结中的队列解冻重放。
- 会话过期/未知/**队列已溢出** → `CONNACK{resumed:false}`（不设独立失败帧），客户端：所有挂起流以 `SESSION_EXPIRED` 失败、清空订阅表、回调 `OnResumeFailed` 让应用重建状态；随后重新走全新握手。

### 8.3 语义：at-least-once

重放机制不携带 per-frame 序号，客户端无法精确判重，因此恢复语义是 **at-least-once**：**断开期间产生的下行帧**（服务端已收下、进入保留队列的）在重连时重放；而断连瞬间已在 TCP 在途、服务端已写出的帧不保证送达，也不会被重放——它们从未进入保留队列。也就是说，v1 的承诺是"重连后不丢新帧"，不是"零丢失"；残余的窗口只有断连瞬间的最后一两帧，兜底靠业务幂等。选它的理由：JSON 业务消息绝大多数天然可幂等（携带业务 ID 可去重），而精确一次（exactly-once）需要 per-stream 序号 + 双端确认窗口，复杂度堪比一个 TCP。RSocket 同样提供 `resumable`（精确）与简化两种模式，我们把简化模式作为 v1 的唯一实现，序号扩展点留在 §10。

## 9. 帧序与并发

- 同一 Stream 内帧严格有序（TCP 保证 + 单连接内按序发送）；不同 Stream 之间无顺序承诺（服务端可并发处理）。
- 一帧内先 Metadata 后 Payload；解码端在读取 Payload 前即可据 Flags 决定缓冲策略。
- 单连接上的写操作由内部写 goroutine 串行化，API 层并发调用安全。

## 10. 明确不做的（v1）与扩展点

| 取舍 | 理由 | 扩展路径 |
| --- | --- | --- |
| 不做帧分片（对照 WebSocket 的 FIN 位） | payload 上限 16 MiB 已覆盖 JSON 场景，分片状态机是纯成本 | Reserved 位升级 + 新 Flags |
| 不做 per-frame 序号与精确恢复 | at-least-once 足够（§8.3） | Metadata 增补 seq + ACK 窗口 |
| 不做 per-stream credit | 连接级窗口已防内存失控 | CREDIT 帧语义升级 |
| 不做 TLS/密钥协商 | 面试题范围；PSK 演示参数化能力 | 传输层换 TLS，Flags.Encrypted 转为端到端 |
| 不做 QoS 分级（对照 MQTT QoS 0/1/2） | pub/sub 投递随恢复机制天然是 QoS1-ish | Flags 复用 |
| 不做压缩字典（对照 HTTP/2 HPACK） | JSON 重复来自业务，收益不稳 | CONNACK 协商 zstd dict |
