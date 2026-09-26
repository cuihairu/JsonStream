# JsonStream 设计文档（架构与取舍）

本文是实现的**架构级**讲解：整体架构、模块划分、数据流与控制流、关键数据结构、并发与错误处理策略，并且每条关键决策都回答"为什么这么设计"——列出考虑过的备选方案与放弃理由。协议层的逐条权衡（帧头字段、交互原语、压缩/加密参数化、背压模型、pub/sub 边界）在 [design-notes.md](design-notes.md) 有更细的展开，本文引用不重复；帧格式的规范定义见 [protocol.md](protocol.md)。

讲解顺序刻意与面试陈述顺序一致：先给一张全景图，再沿"一帧的旅程"走数据流，然后下钻并发与错误处理，最后是逐条决策清单与实测数据。

## 1. 问题与目标

面试题要求（README「面试题要求」一节）可归纳为：基于 TCP 的自定义 JSON 帧协议，支持断线重连恢复、心跳、可选压缩/加密，五种交互模式（请求/响应、流式、双工、单向、发布/订阅），可选背压，配套测试与性能数据。翻译成工程目标：

1. **一条 TCP 连接多路复用所有交互模式**——共享握手、心跳与恢复；
2. **协议栈对应用暴露的 API 心智要小**——Request/Stream/Channel/SendOneWay/Subscribe 五个动词，模式差异藏在协议栈内；
3. **每个"可选能力"真的是可选**——关闭压缩/加密/背压/恢复时，不该为它们付任何运行时代价；
4. **对恶意与错误输入有界**——任何对端行为都不能把本端内存打爆或让 goroutine 泄漏。

非目标（同样重要，写进 protocol.md §10）：分片重组、变长头、精确一次投递、密钥分发、TLS 替代。

## 2. 整体架构

```
        应用层
  ┌────────────────────────────────────────────────────┐
  │  Client (client.go)          Server (server.go)    │
  │  重连循环/会话恢复门面        accept/握手/会话保留    │
  └───────────────┬──────────────────┬─────────────────┘
                  │      共用（唯一抽象差：ID 奇偶 + 钩子注入）
  ┌───────────────┴──────────────────┴─────────────────┐
  │  endpoint (endpoint.go)   流调度核心                 │
  │  帧分发 handleFrame / 被动流准备 / 发起 do*          │
  │     │                    │                         │
  │  routeTable (table.go)   flow (flow.go)            │
  │  路由/主题/handler 注册    单条流的本端视图与终结语义  │
  └───────────────┬────────────────────────────────── ─┘
  ┌───────────────┴───────────────────────────────────┐
  │  transport (transport.go)  一条 TCP 连接的帧收发    │
  │  读循环: 分帧→逆向变换→分发    写循环: sendCh 串行化  │
  │     │                       │                      │
  │  credit (credit.go)        transform (transform.go)│
  │  发送侧信用闸门(可选)        flate→AES-GCM(可选)     │
  └───────────────┬────────────────────────────────── ─┘
  ┌───────────────┴───────────────────────────────────┐
  │  frame (frame.go)   14B 定长头编解码（无状态）        │
  └────────────────────────────────────────────────────┘
```

分层原则只有一条：**每层只认识相邻下层**。`frame.go` 不知道连接的存在（纯 `io.Reader` 进、`*Frame` 出，因此可以单测、可以 fuzz）；`transform.go` 不知道帧头（纯字节进出）；`transport.go` 不知道交互模式（帧进帧出 + 一个分发回调）；`endpoint.go` 不知道 TCP（拿着 transport 的 send/close 接口）；`Client/Server` 是两种装配门面。这个原则的直接收益：14 个源文件里没有任何一处 `net.Conn` 出现在 endpoint 层以上（除 `Server.Serve` 的 accept 与握手）。

### 2.1 模块划分与职责

| 模块 | 关键类型 | 职责 | 不做什么（边界） |
| --- | --- | --- | --- |
| frame.go | `Frame`/`Header`/`ReadFrame` | 14B 定长头 + 长度前缀的编解码；Metadata 编解码 | 不做压缩/加密/超时（纯函数） |
| transform.go | `transformer` | payload 的 flate 压缩、AES-256-GCM 加密及逆向 | 不看帧头，不知道路由 |
| credit.go | `creditGate` | 发送侧信用令牌闸门（背压唯一机制） | 不管帧内容 |
| transport.go | `transport` | 读循环（分帧/逆向变换/分发/读空闲超时）、写循环（sendCh 串行化/心跳注入/写超时）、kill 单点收尾 | 不理解交互模式 |
| flow.go | `flow`/`ReadStream`/`Channel`/`Subscription` | 单条流的接收缓冲、终结语义（doneCh）、credit 记账、发起方读取端 | 不直接碰 socket（一律经 endpoint） |
| endpoint.go | `endpoint` | 帧分发、被动流准备（路由查找/模式校验/注册）、发起路径（do*）、流表 | 不管重连（钩子注入） |
| table.go | `routeTable` | 路由/主题/handler/鉴权注册表 | 无状态查找 |
| client.go | `Client` | Dial/握手/重连循环/退避/会话恢复/订阅重订 | 不处理被动流逻辑（共用 endpoint） |
| server.go | `Server`/`serverConn`/`serverSession`/`sessionStore` | accept/握手鉴权/协商生效参数/会话保留与重放/takeover | 不做客户端逻辑 |
| message.go | `Message`/`Request`/`emitter`/`streamContext` | 应用视图与流上下文 | — |
| handshake.go | `ConnectJSON`/`connackJSON` | 握手帧与生效参数计算 | — |
| config.go / errors.go | `Config`/`Error` | 参数归一/错误码 | — |

## 3. 数据流：一帧的旅程

### 3.1 上行（客户端 Request → 服务端 handler）

```
Client.Request(ctx, route, v)
 └─ waitEp ──► endpoint.doRequest
      ├─ allocID（奇数，idMu 保护）
      ├─ registerFlow ──► streams[sid] = flow
      └─ tr.send(REQUEST 帧) ──► sendCh(256) ──► writeLoop
                                              ├─ transformer.outbound: JSON →[flate]→[GCM]
                                              ├─ appendTo 编码 14B 头
                                              └─ conn.Write（10s 写超时）

服务端 readLoop（每帧重置读 deadline = 1.5×心跳间隔）
 └─ ReadFrame（bufio 16KiB，两次 ReadFull：头/体）
     ├─ PING → 回 PONG；PONG → 丢弃（任何帧都证明对端活着）
     ├─ transformer.inbound: [GCM Open]→[flate 解压(上限 16MiB)]
     └─ endpoint.handleFrame(REQUEST)
          ├─ prepareRequest【同步，readLoop goroutine】
          │    路由查找 → Flags 模式校验 → newFlow → registerFlow
          └─ go runRequest【异步，每流一个 goroutine】
                handler(req) → emit(RESPONSE)
                     └─ downSink（服务端）→ tr.send / 会话保留队列
```

同步/异步的分界线是**本文件最重要的一条设计**，理由见 §10-D5。

### 3.2 下行（服务端 emit → 客户端交付）

```
handler / Server.Publish
 └─ endpoint.emit(f)
      ├─ client 侧: 直达 tr.send
      └─ server 侧: downSink = serverSession.sendDown
           ├─ 连接活着 → tr.send（进 sendCh）
           └─ 断开期间 → retained[sid] 追加（字节记账，超 4MiB 失去恢复资格）

writeLoop: sendCh → outbound 变换 → conn.Write
（ticker 到期且无数据帧时注入 PING——心跳是写循环的职责，读空闲检测是读循环的职责）

客户端 readLoop → handleFrame(RESPONSE, sid)
 └─ flow.creditIn（背压记账）→ flow.deliver → frames chan(32)
      └─ ReadStream.Next / Channel.Receive / Subscription.consume 取走
           └─ flow.release(1)：累计到半窗批量回授 CREDIT 帧
```

### 3.3 控制流

- **握手**（CONNECT/CONNACK 恒明文）：双方在写循环启动**之前**用 `rawWrite`/`writeOnce` 同步直写——握手期没有并发写出者，不需要串行化设施；`bufio.Reader` 在握手与 transport 间传递（`transport.reader` 字段），避免缓冲区里已读出的帧丢失。
- **生效参数**：`effectiveConnack`（handshake.go:51）——compress/encrypt 是与（双方都开才开），credit 取 min（任一方 0 即整体关闭），心跳以服务端为权威，客户端 `bindSession` 用 CONNACK 回写的参数重建 transformer 与 endpoint。
- **连接死亡**：唯一入口 `transport.kill`（deadOnce 保证单次）：`close(dead)` → 关 credit 闸门 → `conn.Close()` → `go onDead(err)`。所有等待者（send 的 select、takeCredit、connectLoop 的 `<-tr.dead`）通过 dead channel 感知。
- **重连**（client.go:215 connectLoop）：退避 = 初始值 ×2 至上限，叠加 `0.5+0.5×rand` 抖动（防重连风暴同步化）；断开后先退避一拍再重拨——立即重连常抢在服务端感知断连之前，把本该判「会话过期」的重连误判成 takeover 恢复。
- **会话保留/恢复**（server.go:384 起）：unbind 启动保留期 timer；重连 `bind` 做 takeover（杀旧连接）→ 迁移奇数流 → 停 timer → 重放 retained；溢出（retainedBytes > 4MiB）则会话失去恢复资格。

## 4. 流式解析：三个层次的"流"，以及为什么每一层都选了不同的解法

"流式"这个词在本项目里指三件不同的事，它们的共同点是**都不把整体读进内存再处理**，但每层的最优解法不一样。把它们分开讲，是这份实现里最容易被面试官追问、也最容易答混的地方。

| 层次 | "流"的对象 | 本实现的做法 | 内存特征 |
| --- | --- | --- | --- |
| ① 传输层 | TCP 字节流 → 帧 | `ReadFrame` 拉取式两段读（定长头 + 长度前缀） | O(L)，只缓冲一帧 |
| ② 协议层 | 帧 → 交互流（一条流多帧） | `flow` + `frames chan *Frame` 逐帧交付 | O(流数 × 缓冲深度) |
| ③ 应用层 | 帧的 JSON 载荷 → 业务对象 | `json.RawMessage` **延迟解码**，应用调 `Decode` 才解析 | 不解析即 0 额外分配 |

### 4.1 传输层：定长头把状态机消掉了，于是解析器是"无状态拉取"

TCP 给的是字节流，`ReadFrame`（frame.go:168）要在任意字节偏移上恢复出帧边界。三种分帧手段的取舍已在 protocol.md §3.3 论证过，这里只讲**它对解析器结构的影响**：

```go
var head [headerSize]byte            // 14B 定长头：栈上数组，零堆分配
io.ReadFull(r, head[:])              // ① 半包在这一行被消化
// …校验魔数/版本/保留位/长度上限（全是对已读到的字节做判断）
if f.Flags&FlagHasMeta != 0 { io.ReadFull(r, ml[:]) }   // ② 变长段
io.ReadFull(r, f.Metadata) / io.ReadFull(r, f.Payload)  // ③ 变长段
```

三个设计点值得单独说：

- **头是栈数组而不是 `make([]byte, 14)`**：`ReadFrame` 在繁忙连接上每秒可能被调用上万次，14B 的堆分配会被 size class 放大并增加 GC 扫描成本。栈数组让它彻底消失——这也是基准里 64B 载荷的帧往返也只有 6 allocs/op 的原因之一。
- **先校验、后分配**：`payloadLen` 的上限判断（frame.go:191）在 `make([]byte, payloadLen)` **之前**。顺序反了就是"先按对端报的长度申请内存、再判断合不合法"，等于把 OOM 开关交给对端——这是协议实现最常见也最致命的一类错误。
- **`io.ReadFull` 而不是 `Read` + 循环**：`ReadFull` 的契约就是"读满 n 字节或出错"，半包在它内部被循环消化。`bufio` 解决的是另一个问题（减少 syscall 次数），两者不可互相替代：没有 bufio，一个带元数据的帧要走 4 次 `read(2)`（头/metaLen/元数据/载荷各一次，ReadFull 内部还可能再循环）；没有 ReadFull，`Read` 只保证"读到至少 1 字节"，一次拿到半个帧头就会把状态读乱。

**为什么解析器"无状态"是个成就而不是偷懒**：变长长度编码（WebSocket 的 7/16/64 位）要求解码端跨帧记住"上一次读到哪儿了"，于是出现了一类只能靠状态机消灭的 bug——某次读超时/半包导致状态错位，后续所有帧全部失步，而且现场极难复现。定长 4B 长度前缀把状态机的需求降为零：`ReadFrame` 是个纯函数式的 `(io.Reader) → (*Frame, error)`，可以单测、可以 fuzz（fuzz_test.go 的 `FuzzReadFrame` 就是这么做的），也可以在任意位置丢帧重入。

**代价要诚实**：无状态换来的代价是每帧固定 14B 头（design-notes §1.2 的"自付保险费"）和 16MiB 的单帧上限（>16MiB 的消息被顶回给应用层自己切块）。

### 4.2 协议层：一条交互流 = 一个 `flow` + 一个缓冲通道，逐帧交付

② 层的"流"是逻辑概念：一次请求/响应、一条流式响应、一个双工通道、一条订阅，各自可能有 0..N 帧。实现上它退化成两件东西：

- `flow`（flow.go:26）：流的身份（id/kind）、终结状态（done/err/doneCh）、背压记账（inflight/pendingCredit）、以及一个 `frames chan *Frame`（深 32）。
- 消费端三种视角：`ReadStream.Next`（拉）、`Channel.Receive`（拉）、`Subscription.consume`（推，回调逐帧串行）。

**关键取舍：缓冲深度 32 是"未启用背压时的兜底"，不是"设计出来的窗口"**。启用 credit 时在途条数被 credit 窗口（通常 ≪ 32）约束，缓冲根本积压；未启用时缓冲写满会让 `deliver` 阻塞读循环，反压一路传导到 TCP 接收窗口——也就是"关闭背压 = 信任 TCP 兜底"的字面含义。备选是"缓冲深度与 credit 窗口联动"：那要让 flow 感知协商结果，而 flow 的构造在 `newFlow` 里、协商结果在 endpoint 上，多一层耦合换来的只是省几个槽位，不值。

**另一处非显然的取舍：`frames` 存指针不存值**。一帧载荷最大 16MiB，复制进通道意味着每个中转点都付一次 L 的拷贝；存指针则"谁投递谁负责生命周期"。代价是帧的内存所有权变得微妙——保留队列直接持引用（§5.5）而不深拷贝，前提是**栈内没有任何代码修改 `Frame` 的字段**。这条不变式没有类型系统保护（`Frame` 是可变的结构体），它靠注释和 code review 维持；真正的保险是"编码发生在写循环、编码后不再有人碰那个 `Frame`"这一时序事实。

### 4.3 应用层：延迟解码——本实现对"流式 vs DOM"的取舍

③ 层是 JSON 怎么进内存。本实现**既不是纯 DOM 也不是 token 流**，而是第三种：**帧级流式 + 载荷延迟解码**。

- 不用 DOM（`json.Unmarshal` 到具体类型）立刻解析：分发路径上有大量帧的 payload 根本没人看——路由只看 Metadata、保留队列只是原样重放、订阅转发不解包。只有真正到了 handler/应用手里才需要解析。立刻解析等于为"不看"也付全额 CPU。
- 不用 token 流（`json.Decoder.Token()` 逐 token 事件）：帧边界已经是天然的、自描述的"流单元"，在它内部再套一层 token 状态机，要多一套状态（当前在对象的第几个 key、数组下标、嵌套深度）却换不来任何东西——因为**我们从来不需要"读到一半就开始处理"**：一帧最大 16MiB，已经被内存上限挡住了，"大于内存预算的流"这个 token 流唯一的典型场景在这里不存在。
- 用 `json.RawMessage`（message.go:17）做中间态：它只是 `[]byte` 的类型别名，附一个 `MarshalJSON`，`Unmarshal` 到它时只做一次合法性扫描、**不建树**。应用想要结构时才 `Decode(v)` 付解析成本。

这条不变式是 §5.1 的"栈内恒明文"在 JSON 层的延伸：**协议栈的任何一层都不假设 payload 的形状**。收益不只是性能，还有正确性——`RawMessage` 保留原始字节，转发/重放不会引入浮点精度漂移（`1e400`、`9007199254740993` 这类值过一遍 `float64` 就变了）。

**诚实的反面**：`Decode(v)` 是"要么全解、要么失败"的原子操作，无法只取对象里的一个字段。NDJSON 式"一帧一个巨大数组、只关心前几项"的场景，这里只能整体解——真要支持就得把 `Decode` 换成 `json.Decoder` 的游标式读取，那是一条明确但要付"读过头"与"状态机"代价的路（NOTES.md §2 展开这条路的两难）。

### 4.4 "读过头"问题：握手与传输层共享同一个 bufio.Reader

流式解析里最阴的一类 bug 是**读过头**：解析器为了填满自己的缓冲，把下一条消息的开头也读走了，而上层以为消息之间有干净的边界。JSON 有 `Decoder.Buffered()` 专门暴露这个"多读的余量"；本项目在帧层面对同样的问题，解法是**让两个阶段共享同一个 reader 对象，而不是各自新建**：

```go
br := bufio.NewReader(nc)          // 握手阶段
hf, _ := ReadFrame(br)             // …读 CONNECT（可能把 CONNACK 的前几字节也读进了 br 的缓冲）
tr := newTransport(nc, br, …)      // ← 关键：把 br 本身交给 transport，不新建
```

`transport` 因此有一个类型是 `io.Reader` 而不是 `*bufio.Reader` 的字段（transport.go:17，注释写明"与握手阶段共享的 bufio，避免缓冲数据丢失"）。如果这里写成 `bufio.NewReaderSize(conn, …)`（`readLoop` 里确实又包了一层，见 transport.go:156），第二层 bufio 会从 conn 重新取字节，而握手阶段的 br 缓冲里那几字节就被永久跳过——表现为"偶发丢第一帧"，且只在 CONNACK 与首个数据帧同批到达时出现，极难复现。

注意 `readLoop` 里那层 `bufio.NewReaderSize` 之所以**安全**，是因为它包的是 `t.reader`（那个共享的 br）而不是 `conn`：多一层缓冲只多一次内存拷贝，不会跨阶段抢字节。这个"看起来像冗余、实际是兜底"的写法值得在 code review 里明确说明，否则下一个人会"顺手优化掉"它。

## 5. 关键数据结构

### 5.1 Frame（frame.go:119）

```go
type Frame struct {
    Header                     // Version/Flags/Type/StreamID 定长头
    Metadata []byte            // 恒明文 JSON（路由/主题），nil 表示无
    Payload  []byte            // 栈内恒明文 JSON；只在读写 socket 边界做变换
}
```

**不变式：payload 在协议栈内部永远是明文**，压缩/加密只发生在 transport 的 outbound/inbound 两个边界函数。备选方案是让 Frame 携带"已加密"状态在栈内流转——放弃，因为每个消费点都得先判断帧是否已解密，一处遗漏就是拿密文当 JSON 解析的 bug；边界化之后，栈内所有代码（路由、分发、保留队列）天然只见明文。

### 5.2 flow（flow.go:26）——单条流的本端视图

```go
type flow struct {
    id     uint32
    kind   flowKind          // request/stream/channel/subscribe 四种
    ep     *endpoint          // 当前绑定的 endpoint（重连迁移时可换绑，mu 保护）
    frames chan *Frame        // 入站数据缓冲（32）
    mu     sync.Mutex         // 保护以下字段
    done   bool; err *Error   // 终结状态
    doneCh chan struct{}      // 终结广播（关闭即信号）
    ackCh  chan struct{}      // SUBACK 信号
    inflight, pendingCredit int // 背压记账
    reqEntry routeEntry       // 被动流 handler 载体（读循环注册、异步执行）
    chEntry  func(*Channel) error
}
```

一个结构同时表示"我发起的流"与"我在响应的流"——差异只在 kind 与谁持有 frames 的读取端。备选是 initiator/responder 两个结构：放弃，因为双工通道（Channel）本质上同时是两者，拆开会让 Channel 持有两个半流对象，状态同步（对端 CANCEL 时两边都要终结）立刻变复杂。

终结语义收敛为一个原语：`finish(e)` 在锁内写 done/err、`close(doneCh)`、注销流表，幂等。所有等待方（Next/Receive/Request 的 select、streamContext.Done）监听 doneCh。**err 必须在锁内读**（`doneState`）——曾因 `Err()` 裸读与读循环 finish 写竞争被 CI race 实锤（README bug 清单外的一条修复），此后统一收口。

### 5.3 endpoint（endpoint.go:12）——流调度核心

```go
type endpoint struct {
    clientSide bool               // ID 奇偶方向的唯一差异
    cfg Config; tr *transport; table *routeTable
    creditWindow, creditFlushAt int
    idMu sync.Mutex; nextID uint32 // 奇偶分配，跨重连单调
    streamsMu sync.RWMutex; streams map[uint32]*flow
    onSubscribe func(uint32, string) error   // ── 服务端注入的钩子
    onUnsubscribe func(uint32)               //    client 侧为 nil
    downSink func(*Frame) error              //    （下行落会话保留队列）
}
```

**Client 与 Server 共用 endpoint 是本实现最大的一条复用决策**：分发、被动流准备、五个发起原语、流表、credit 全部只写一遍；两侧差异压缩成"clientSide 决定 ID 奇偶 + 三个可空钩子"。备选方案：客户端/服务端各一套调度器——放弃，因为 pub/sub 双向语义（PUBLISH 同一帧类型两个方向）与双工通道要求两侧逻辑对称，两套实现必然漂移，测试矩阵也要翻倍。代价是 endpoint 的 API 面比"纯客户端"宽（客户端也会带着 onSubscribe==nil 的空分支），换来的是行为对称性有单一事实来源。

### 5.4 transport（transport.go:15）

```go
type transport struct {
    conn net.Conn; reader io.Reader  // 握手期共享的 bufio
    tr *transformer; cfg *Config; credit *creditGate // nil = 无背压
    sendCh chan *Frame               // 256，全部出站帧的唯一入口
    handler func(*Frame) error       // 读循环分发回调
    deadOnce sync.Once; dead chan struct{}; deadErr error
}
```

### 5.5 serverSession / sessionStore（server.go:388）

```go
type serverSession struct {
    mu sync.Mutex
    conn, lastConn *serverConn      // 当前/最近连接（迁移与终结要用 lastConn）
    subs map[uint32]string          // 订阅关系（流 ID → 主题）
    retained map[uint32][]*Frame    // 断开期间的下行帧（按流分组）
    retainedBytes int; overflowed bool
    timer *time.Timer               // 保留期
}
```

保留队列按流分组（`map[uint32][]*Frame`）而不是单一 FIFO：重放时对同一流保持顺序即可，跨流本就允许交错（多路复用的本意）。字节记账 `headerSize+metaLenSize+len(Metadata)+len(Payload)` 是近似值（不含 map/切片开销）——精确记账需要在每次 append 时遍历，收益是让上限更准一点，而上限本身是防 OOM 的粗闸门，近似足够。

## 6. 并发模型

### 6.1 goroutine 清单（每条连接）

| goroutine | 生命周期 | 职责 |
| --- | --- | --- |
| readLoop | 连接建立→kill | 分帧、逆向变换、分发、读空闲超时 |
| writeLoop | 同上 | sendCh 串行写出、心跳注入、写超时 |
| runRequest/serveOneWay/topic handler | 单帧/单流 | 应用代码执行（每流一个，天然的取消边界） |
| Subscription.consume | 订阅存活期 | 逐帧调回调（串行投递的保证者） |
| connectLoop（仅客户端） | Client 存活期 | 重连状态机 |

**设计原则：每条流最多一个执行 goroutine，每条连接恰好两个 I/O goroutine。** 备选方案对比：

- *单 goroutine per connection + 回调分发*（net/rpc 模式）：读循环直接执行 handler，实现最简单；放弃，因为任何一个慢 handler 都会停摆该连接所有流的读入（队头阻塞），心跳也随之停发——读写必须分离，执行必须与读入分离。
- *每帧一个 goroutine*：吞吐上限高；放弃，因为流式语义（handler 串行 Emit、订阅回调串行投递）要求顺序，无界并发反而要再引入序列化层。
- *固定 worker 池*（SimpleGoServer 题一的做法）：适合 CPU 密集 handler 的公平调度；本协议 handler 以 I/O 为主（Emit 受背压约束会阻塞），池化会把"阻塞在背压上的 handler"占满池子。每流一个 goroutine 的成本（初始栈 ~8KB）在交互数 ≤ 万级时可接受，且取消语义（streamContext）与 goroutine 生命周期天然对齐。

### 6.2 锁与 channel 拓扑

| 同步原语 | 保护对象 | 备注 |
| --- | --- | --- |
| transport.deadOnce/dead | 连接死亡的单次广播 | kill 的幂等性 |
| flow.mu | done/err/ep/inflight/pendingCredit | 锁内不调外部函数，无死锁面 |
| endpoint.idMu | nextID | 唯一的写竞争热点（每发起一次交互一次） |
| endpoint.streamsMu (RW) | streams 流表 | 读多写少，lookup 走 RLock |
| routeTable.mu (RW) | 注册表 | 运行中注册对新请求立即生效 |
| Client.mu / Server.mu / sessionStore.mu / serverSession.mu | 各自字段 | 锁序见下 |

锁序（获取顺序）：`store.mu → ss.mu`（不嵌套，先快照再取）、`ss.mu → ep.streamsMu`（单向）、`flow.mu` 不与任何锁嵌套。sendCh(256)/frames(32)/notify(1)/dead/doneCh/ackCh 的容量与信号语义都写在类型注释里——**cap-1 的 notify 是"信号量"而非"队列"**（credit.go:12），多路 add 只需唤醒一次，`select+default` 投递永不阻塞。

### 6.3 出站串行化：为什么是 sendCh 而不是写锁

所有出站帧（数据、错误、CANCEL、CREDIT、心跳）进同一条 sendCh，由写循环单点写出。备选：`sync.Mutex` 包住 conn.Write——更直接，但三个理由选 channel：(1) 写循环要与心跳 ticker 做 `select`，mutex 模式下心跳注入需要独立的 ticker goroutine 或定时抢锁；(2) sendCh 天然有 256 帧的缓冲，应用 goroutine 与 socket 速度解耦，等价于一层小发送窗口；(3) close(dead) 之后 send 返回 ErrClosed 的语义可以和 select 自然组合。代价是写循环 goroutine 本身与一次 channel 往返（~百 ns 级，相对网络 RTT 可忽略）。

send 的死连接预检（transport.go:78）是被真 bug 逼出来的：连接死后 sendCh 常有空位，`select` 双就绪随机选择，同一次 kill 后的 send 会不确定性地产出"帧入队但永不写出"或 ErrClosed——**Go 的 select 随机性在错误路径上是真陷阱**，消灭它的办法是给死分支优先预检。

### 6.4 确定性交付：排空语义

终结与数据帧的交付是异步的（doneCh 关闭时 frames 里可能还有余帧），而 `select` 双就绪随机选择——所以 `ReadStream.Next`/`Channel.Receive`/`doRequest` 的 doneCh 分支都要**先排空 frames 再返回终结**（flow.go:181 注释），否则会随机丢最后一帧。CANCEL/ERROR 例外：立即终结语义优先，在途帧语义上作废。

## 7. 错误处理策略

三类错误三种处置，与题目"错误处理和恢复"对应：

| 类别 | 判定 | 处置 | 例子 |
| --- | --- | --- | --- |
| 连接级（网络/协议） | ReadFrame/变换失败、handler 返回 PROTOCOL、StreamID=0 的 ERROR | `kill`：断连、广播 dead、触发重连/保留 | 坏魔数、解压超限、保留位非零 |
| 流级 | ERROR(sid≠0)、CANCEL、会话过期 | `flow.fail`：终结单条流，连接继续 | 路由不存在、handler 返回 error |
| 应用级 | handler 内部 error/panic | panic recover→INTERNAL 错误帧；error 按 `asStreamError` 归一 | 业务校验失败 |

配套纪律：

- **panic 一律 recover 在协议栈边界**（runRequest/serveOneWay/topic handler 三处），单条流的应用崩溃绝不带崩进程，也不会泄漏未终结的 flow（recover 路径同样走到 finish/错误帧）。
- **错误帧的编解码有兜底**：`errDecode` 对不可解码载荷归一为 INTERNAL，而不是把对端的畸形错误帧变成连接级失败。
- **资源释放单点化**：连接关闭只在 kill 一处（deadOnce 幂等）；`Server.handleConn` 用 defer Close 兜底握手失败路径；`Subscription.Close`/`Client.Close` 都是 `sync.Once`/幂等语义——**释放接口必须可重入**，否则使用方的 defer 链里必然出现 double-close。
- **断连即败的"无意义等待"剪除**：服务端发起的流（偶数 ID）响应是上行、不缓存，断连后等待必然落空，onDead 立即 fail（server.go:366 注释）；客户端发起的 responder 流归会话保留，两类流在同一次断连里的命运不同，这是 at-least-once 语义的直接推论。
- **一处已知的类型系统盲区**：`asStreamError`（message.go:108）用 `err.(*Error)` 做类型断言，若应用返回的是**类型为 nil 的 `*Error`**（`var e *Error; return nil, e` 这类写法），断言会成功并返回 nil，随后 `errorFrame` 把 `nil` 编码成 `null`，对端 `errDecode` 因 `code==0` 归一为 INTERNAL。结论是**降级而非崩溃**（typed-nil-in-interface 陷阱的典型形态），但错误信息会丢失。要彻底消除得写成 `if e, ok := err.(*Error); ok && e != nil`——没改的原因是它会让"应用故意返回 nil *Error"这种病态写法更难被察觉，而降级后的行为已经安全；这一点在此显式记录，作为已知边界而不是遗漏。

## 8. 边界情况清单

这份清单是"读代码时最容易看漏、线上最容易炸"的那一类输入与时序。每一行给出：**触发条件 → 处置 → 代码位置 → 为什么这么处置**。测试侧的对应关系见 design-notes.md §9（覆盖率 100% 意味着每一行都有断言，只是不在此重复）。

### 8.1 帧层：来自对端的任意字节

| 边界情况 | 处置 | 位置 | 为什么 |
| --- | --- | --- | --- |
| 帧头跨 TCP 分段（半包） | `io.ReadFull` 读满 14B 或报错 | frame.go:170 | 半包是常态不是异常；ReadFull 内部循环消化 |
| 一条连接上多帧粘包 | bufio 缓冲 + ReadFull 精确取长 | frame.go:170, transport.go:156 | 缓冲解决 syscall 次数，ReadFull 解决取字节数 |
| 坏魔数 / 版本不符 / 保留位非零 | Malformed → 断连 | frame.go:173/184/187 | 保留位非零意味着"我们不知道对端在用什么扩展语义"，猜错比断开糟 |
| `payloadLen` 超过 16MiB | Malformed，**在 `make` 之前** | frame.go:191 | 顺序即安全：先分配后校验 = 把 OOM 开关交给对端 |
| `metaLen` 恰为 64KiB | 编码端拒（上限 = `math.MaxUint16`） | frame.go:34/134 | 上界必须等于字段可表达的真值，65536 无法用 uint16 表达（gosec 实锤的 off-by-one） |
| `FlagHasMeta` 置位但元数据为空 | 仍写出 metaLen 段（长度 0） | frame.go:150 | 保证 parse∘encode = id：否则"声称有 meta 却不带段"的自相矛盾帧（fuzz 实锤） |
| 载荷恰好 0 字节 | `Payload` 保持 nil，不分配 | frame.go:207 | 0 长度帧合法（心跳/控制帧）；`make([]byte, 0)` 会返回非 nil 空切片，破坏判等 |
| 未知帧类型 | PROTOCOL → 断连且**不回帧** | endpoint.go:219 | 连类型都不认识说明对端是异版本实现，回帧的互通前提已不成立 |
| 握手期恶意慢连接 | 整体 deadline = DialTimeout | server.go:217 | 不给握手期读设上限，一个连上不发字节的连接就能占住一个 goroutine |

### 8.2 变换层：压缩与加密的对端输入

| 边界情况 | 处置 | 位置 | 为什么 |
| --- | --- | --- | --- |
| 解压炸弹（16MiB flate 膨胀三个数量级） | `io.LimitReader(limit+1)` 读满即拒 | config.go:21 | 读 `limit+1` 而不是 `limit`：读满 limit 无法区分"恰好 limit"与"超过 limit" |
| 密文短于 nonce+tag | Malformed | transform.go:108 | 长度不足时 `gcm.Open` 会越界 panic；显式判长度是防线 |
| GCM tag 校验失败 | 解密错误 → 断连 | transform.go:111 | AEAD 的认证失败就是"帧被篡改"，没有"降级接受"的余地 |
| 未启用却收到加密/压缩帧 | UNSUPPORTED → 断连 | transform.go:105/119 | 帧内 Flags 与本端配置矛盾 = 对端违约；状态已不可信 |
| 载荷 < 64B 却要压缩 | 跳过压缩（阈值） | transform.go:74 | 短 JSON 压缩后常不降反升，flate 头开销吃掉全部收益 |
| nonce 来源失败（熵源异常） | 整帧失败，绝不降级为明文 | transform.go:89 | 静默降级会把"本该保密"变成"明文上网"，是最坏的一种失败 |

### 8.3 流层：时序、竞态与生命周期

| 边界情况 | 处置 | 位置 | 为什么 |
| --- | --- | --- | --- |
| 帧到达时流已终结（迟到的 RESPONSE/COMPLETE） | 静默忽略 | endpoint.go:118 | CANCEL 与在途帧的竞态下属正常，不是协议错误 |
| 终结帧与数据帧同时就绪 | 消费端先排空 `frames` | flow.go:181/272 | `select` 双就绪随机选择，不排空会**随机**丢最后一帧 |
| 死连接上 `send` 的 select 双就绪 | `dead` 分支优先预检 | transport.go:78 | 否则同一次 kill 后的 send 不确定地产出 nil（帧入队但永不写出，静默丢失） |
| 重连后 Stream ID 撞号 | 计数器跨重连单调过继 | client.go:376 | 撞号时旧流迟到的 COMPLETE 会误杀新注册的同 ID 流（实测 bug） |
| 订阅句柄持有创建时的 endpoint | 出站一律 `currentEP()` | flow.go:229/370 | 陈旧引用让 CANCEL/UNSUBSCRIBE 发给死连接被静默吞 |
| 会话恢复时 responder 流不迁移 | `bind` 连 handler 一起搬 | server.go:444 | handler 悬空会白产帧，把保留队列撑爆并连坐整个会话 |
| 服务端发起的流在断连后 | 立即 fail，不等响应 | server.go:374 | 响应是上行、不缓存，等待必然落空 |
| credit 额度在断连时归零 | 失败改道保留队列，不拦截 | message.go:97 | 否则连接级流控泄漏进会话级语义，破坏 at-least-once（实测 bug） |
| handler panic | recover → INTERNAL 错误帧 + 流终结 | endpoint.go:273 | 应用崩溃不带崩进程，也不泄漏未终结的 flow |
| 应用返回 typed-nil `*Error` | 降级为 INTERNAL | message.go:108 | typed-nil-in-interface 陷阱；见 §7 末条 |
| 畸形错误帧载荷 | `errDecode` 归一 INTERNAL | errors.go:70 | 对端的一个坏 error 载荷不该升级成连接级失败 |
| 重复注册同一路由 | 后者覆盖前者 | table.go:40 | 幂等语义的代价是"注册顺序影响行为"，注释里写明 |
| 空载荷的 `Decode` | 直接返回 nil，不动 v | message.go:22 | 空载荷是合法的（无 body 的请求），不是错误 |

### 8.4 配置与 API 层

| 边界情况 | 处置 | 位置 | 为什么 |
| --- | --- | --- | --- |
| `Heartbeat` 配得过小（<1s） | 归一为 1s | config.go:99 | 心跳是防御参数，暴露成可调就一定会有人调到把连接打挂 |
| `Retention` 为负 | 禁用会话恢复 | server.go:476 | "不保留"要有一个显式表达，不能靠 0 撞上"取默认值" |
| `Credit ≤ 0` | 不启用背压，闸门为 nil | endpoint.go:46 | nil 闸门的 `take` 直接放行，是"关闭"而非"坏掉" |
| `Credit` 窗口为 1 | `creditFlushAt` 下限钳到 1 | endpoint.go:49 | 半窗为 0 会导致"永远攒不到回授"——额度只减不增，流必然饿死 |
| ONEWAY 路由不存在 | 静默丢弃，不回帧也不记日志 | endpoint.go:333 | ONEWAY 是高频路径，可被随机路由刷爆日志——那等于把 DoS 面开进可观测性 |
| 订阅不存在的主题 | SUBACK 成功 | endpoint.go:202 | 订阅问的是"以后能不能收到"，不是"现在有没有人生产" |
| `nil` 接收者的 `ReadStream`/`Subscription` | 全部方法 no-op 返回 | flow.go:182/216/230 | 使用方的 `defer s.Cancel()` 在出错路径上可能对着 nil 调用 |
| `Cancel`/`Close` 二次调用 | 幂等 | flow.go:230/299 | 释放接口必须可重入（§7 配套纪律） |
| 加密启用但密钥不是 32B | 构造期失败，不建连接 | transform.go:51 | 配置错误要在握手前暴露，而不是让每一帧都失败 |
| `SendOneWay`/`Publish` 无 ctx 参数 | 内部用 `context.Background()` | client.go:181/190 | **已知 API 不对称**：连接断开时这两个调用等的是重连而非调用方超时；服务端消失时会一直等。补 ctx 参数是 v1 之后的 API 演进项（见 §11.3 对应的覆盖率说明） |

## 9. 复杂度分析

记号：`L` = 载荷字节数，`N` = 单连接并发流数，`S` = 会话保留字节数（≤ `RetentionBytes`），`W` = credit 窗口条数，`R` = 帧速率。

### 9.1 单帧成本

| 操作 | 时间 | 空间 | 备注 |
| --- | --- | --- | --- |
| `appendTo` 编码 | O(L + M) | O(L + M) | 写循环复用 `buf[:0]`，稳态零分配 |
| `ReadFrame` 解码 | O(L + M) | O(L + M) | 头是栈数组；2 次堆分配（元数据、载荷） |
| flate 压缩 | O(L)（常数级 CPU 系数大） | O(L) | 实测 ~25× 慢于 GCM，是管线的主要成本 |
| GCM 加/解密 | O(L) + 28B | O(L) + 28B | 12B nonce + 16B tag；AES-NI 下 GB/s 级 |
| `deliver` 投递 | O(1) | O(1) | 通道操作，帧传指针不拷贝 |
| 路由查找 | O(1) 期望 | O(1) | map 哈希 |

**分解出的固定成本 vs 边际成本**（本机实测，见 §11 表格）：小帧 ~1µs/帧的固定开销、~0.5–1.6 ns/B 的边际开销，两者在 L ≈ 1–2KiB 附近交叉。也就是说：

- **小消息为主**（<1KiB，如 RPC 的 `{"id":…,"result":…}`）：瓶颈是每帧固定开销，与载荷大小几乎无关 → 优化方向是减少帧数/系统调用（批量写、帧头压缩），不是压缩载荷。
- **大消息为主**（>64KiB，如批量导出）：瓶颈是带宽与拷贝 → 压缩才有意义（这正是 `minCompressSize=64` 阈值与"按帧而非按连接决定是否压缩"的依据）。

### 9.2 并发与内存上界

| 结构 | 数量 | 单实例内存 | 上界由谁保证 |
| --- | --- | --- | --- |
| `flow` 结构体 | N | ~200B + 通道 32×8B | N 由应用并发决定 |
| `frames` 缓冲 | N | 32 个指针（帧体共享） | 缓冲深度常量 32 |
| `sendCh` | 1/连接 | 256 个指针 | 常量 256 |
| 读/写循环 goroutine | 2/连接 | 初始栈 ~8KB，按需增长 | 常量 |
| 保留队列 | 1/会话 | ≤ `RetentionBytes`（4MiB） | 字节记账，超限降级 |
| 单帧载荷 | — | ≤ 16MiB | 硬上限，分配前校验 |

**必须说清的一点：16MiB 上限约束的是"单帧"，不是"总量"。** 未启用 credit 时，内存的真实上界是 `32 × N × 平均帧长`——一个 N=1000、平均帧 1MiB 的连接可以合法地吃掉 32GiB。真正的总量约束由三样东西提供，缺一不可：

1. **credit 窗口**（可选，默认关）：把在途条数钉在 W 以内；
2. **32 深的通道缓冲**：写满后读循环阻塞，反压传导到 TCP 接收窗口；
3. **单帧 16MiB 上限**：防止"一条恶意大帧"打爆单点分配。

这也是"背压默认关闭"这个决策的真实代价（design-notes §5.2 第 4 条）：默认配置下的内存安全依赖 TCP 缓冲 + 应用自律，协议层不提供硬保证。要给硬保证就把 credit 打开，并在文档里说清它会阻塞 handler。

### 9.3 会话与恢复

| 操作 | 时间 | 空间 | 备注 |
| --- | --- | --- | --- |
| 会话保留（每帧） | O(1) 均摊 | O(S) | append + 字节记账，O(1) |
| 保留队列溢出清理 | O(S) 一次性 | → 0 | 整队丢弃并标记 `overflowed` |
| 重放 | O(保留帧数) | O(1) 额外 | 直接排入 `sendCh` |
| 迁移流（`bind`） | O(N) | O(1) | 遍历 `snapshot()`，单向流迁移、双向流不迁 |
| 会话终结（`terminate`） | O(N) | O(1) | 先快照后解锁，避免锁序倒置 |
| 重连期望耗时 | O(BackoffMax) | O(1) | 指数退避 × 抖动，期望 ~1.5× 上限；**最坏无界**（一直重试到 `Close`） |

### 9.4 端到端

一次请求/响应的完整成本 = 2 帧 × (编码 + 变换 + 写 syscall) + 2 × (读 + 变换 + 解码) + 2 次 JSON 编解码 + 2 次 handler goroutine 调度 + 1 个 RTT。实测 33 allocs/op（明文）、39 allocs/op（加密）——**加密的额外 6 次分配全部来自 nonce 与密文缓冲**，与"每帧多 28B 上线开销"一致。

稳态成本是每 `Heartbeat` 一次 14B 写；读侧每帧一次 `SetReadDeadline`（netpoll 的 deadline 表更新，纳秒级）。两者都远小于一次 RTT，所以"心跳开着"不会成为吞吐瓶颈——这是把心跳做成"每连接独立 ticker"而不是"全局扫描"的理由。

## 10. 设计决策清单（备选方案与放弃理由）

> 协议层决策（帧头为什么 14B、为什么不做 FIN 分片/MASK/变长长度、Flags 合并请求入口、credit vs LEASE vs 滑动窗口、Metadata 分离、先压后加）的完整论证见 [design-notes.md](design-notes.md) §1–§5，此处只列实现层决策。

- **D1 共用 endpoint 抽象**（§5.3）。备选：客户端/服务端两套调度器——放弃理由：双工与 pub/sub 要求两侧逻辑对称，两套实现必然漂移；现状代价是 client 侧带着三个 nil 钩子的空分支。
- **D2 读写双循环 + 每流执行 goroutine**（§6.1）。备选：单 goroutine 串行分发（慢 handler 队头阻塞心跳）、固定 worker 池（背压阻塞占满池）。选中方案以 goroutine 数量换取消语义的干净。
- **D3 出站统一 sendCh(256) 单点写**（§6.3）。备选：写锁（心跳注入别扭、无缓冲解耦）。
- **D4 kill 单点收尾 + dead channel 广播**。备选：各处自查 conn 状态——放弃，竞态窗口遍布；sync.Once + close(chan) 是 Go 里"一次性广播"的最简正解。
- **D5 被动流注册同步、handler 执行异步**（endpoint.go:205 注释）。`prepareRequest` 在 readLoop 的同步路径完成路由查找/校验/注册，`runRequest` 才进 goroutine。备选：全异步（注册也在 goroutine 里）——放弃，对端发起 Channel 后会立即发数据帧，注册晚于数据帧到达时 lookupFlow miss，帧被静默丢弃，双方死等（实测会发生的互锁）。同步注册的代价是 readLoop 被路由表查找（RLock）短暂占用，可忽略。
- **D6 flow 终结 = close(doneCh) 单原语**。备选：状态枚举 + 轮询——放弃，close 的广播语义让所有等待方一次感知，且 streamContext 直接把 doneCh 包装成 context.Context，handler 的 ctx 取消免费获得。
- **D7 出站一律经当前 endpoint**（flow.currentEP()）。备选：流持有构造时的 endpoint——被 bug 9-6 实锤放弃：重连后 CANCEL/UNSUBSCRIBE 发给死连接被静默吞掉，对端 handler 永远收不到取消。`currentEP()` 的锁开销换消灭一整类陈旧引用 bug。
- **D8 Stream ID 奇偶 + 跨重连单调**。奇偶（HTTP/2 同思路）让新到帧无需协商即可判归属；`bindSession` 把 `oldEp.nextID` 过继给新端点（client.go:376）——重置会让新流与迁移流撞号，旧连接迟到的 COMPLETE 误杀新流（bug 9-5）。
- **D9 会话保留 per-session 内存队列 + 字节上限 + 溢出降级**。备选：写磁盘/外部 broker（题面外）、无上限（OOM 开关）、溢出即断会话（过于激进——降级为"失去恢复资格但连接可用"既防 OOM 又不惩罚已建立的连接）。
- **D10 心跳由写循环注入、死活由读 deadline 判定**。写侧 ticker 到期发 PING，读侧每帧重置 `SetReadDeadline(1.5×间隔)`。备选：TCP keepalive（探不到对端进程死锁/GC 停顿——它测的是内核协议栈）；读侧主动探测（会把心跳职责和读职责搅在一起）。1.5× 是容忍一次丢帧抖动与判死速度的折中。
- **D11 flate 编解码器 sync.Pool 按帧复用**。实测每帧新建 writer 代价 ~1.3ms/~800KB 分配，池化 + Reset 后压缩往返 4.1 倍提速、分配降 162 倍（README 基准）。备选：每帧新建（太贵）、连接级单实例（读写循环已在单 goroutine 内，但订阅消费与 handler 并发 Emit 会竞争）。
- **D12 变换在帧级、标志在帧内自描述**。每帧 Flags 如实标注本帧是否压缩/加密，解码只看帧不看协商（design-notes §3）——实现层配套：`transformer` 无锁（纯函数式进出），栈内明文不变式（§5.1）。
- **D13 测试接缝只设五个包级 var**（jsonMarshal/aesNewCipher/gcmNew/randRead/resubAckTimeout）。这些构造在合法入参下不会失败，其错误分支是防御性死码；接缝之外覆盖率全靠并发时序构造，不动生产逻辑。备选：接口化全部依赖（过度设计）、不设接缝（防御分支永远测不到）。

## 11. 实测数据与验证

### 11.1 覆盖率与测试规模

| 包 | 语句覆盖率 | 说明 |
| --- | --- | --- |
| `jsonstream`（库） | **100.0%** | CI 有门禁（`.github/workflows/ci.yml`），跌破即失败 |
| `examples/server` | 99.0% | 1 条未覆盖，见 §11.3 |
| `examples/client` | 96.5% | 4 条未覆盖，见 §11.3 |

库包覆盖率 100% 的含义要说准：**没有任何"测不到就是死码"的托词**——防御性分支靠五个包级接缝（jsonMarshal/aesNewCipher/gcmNew/randRead/resubAckTimeout）覆盖，其余靠并发时序构造覆盖（design-notes §9）。测试方法学（确定性并发、race detector 纪律）也在那一节。

### 11.2 性能

下表是**本机实测**（i9-10880H / Go 1.24，`go test -run '^$' -bench . -benchtime 2s -count=3 -benchmem` 取中位数；与 README 表格同机）。共享容器里 ns/op 随负载明显浮动（同基准两次实测可差 ±30% 以上），**可复现的是 allocs/op 与 B/op 这两列结构性质**，时间列只当量级看：

| 基准 | ns/op（中位） | 吞吐 | allocs/op | B/op |
| --- | --- | --- | --- | --- |
| 帧往返 64B | 1 058 | ~100 MB/s | 6 | 216 |
| 帧往返 1KiB | 2 544 | ~420 MB/s | 6 | 1 176 |
| 帧往返 64KiB | 35 217 | ~1 860 MB/s | 6 | 65 688 |
| 变换·明文（1.3KiB） | 17 | — | **0** | 0 |
| 变换·flate | 15 344 | — | 8 | ~4 900 |
| 变换·AES-GCM | 2 808 | — | 3 | 3 088 |
| 变换·压+加 | 15 584 | — | 11 | ~5 100 |
| 请求/响应 RTT（明文） | 107 000 | — | 33 | ~2 130 |
| 请求/响应 RTT（加密） | 118 400 | — | 39 | ~2 320 |

从这组数字能直接读出三条结论（都已写进 §9 的取舍）：

1. **allocs/op 与载荷大小无关**（三种尺寸都是 6）——说明帧头的定长数组与元数据解码没有引入按帧的隐藏分配；大帧的 B/op 线性增长全部来自载荷本身。
2. **flate 比 GCM 贵 5 倍以上**（15.3µs vs 2.8µs，同一载荷；比值随频点与负载在 5–7× 间浮动）——所以"压缩按帧可选、加密可全开"的参数化策略在性能上是有依据的取舍，而不是口号。
3. **小帧的固定成本主导**：64B→1KiB 总耗时只涨 ~1.5µs，其中载荷本身的内存操作只占一小部分，大头是每帧固定开销——小消息场景的优化方向是减少帧数与 syscall，不是压载荷。

### 11.3 覆盖率的口径与已知例外

`examples/` 合计还有 5 条 `if err != nil { return err }` 未被覆盖，**全部属于同一类**：它们只在"连接或流恰好在这一瞬间死掉"时才失败，而示例是微秒级顺序执行的流水，测试无法把死亡稳定地插进那个窗口。逐条列在这里，免得读者以为是遗漏：

| 位置 | 分支 | 为什么覆盖不到 |
| --- | --- | --- |
| `examples/client` `Stream` | 连接在 `math.add` 响应之后、`Stream` 调用之前死掉 | 两者相隔微秒级；掐断 TCP 落在这个窗口里是概率事件 |
| `examples/client` `Channel.Send` | 流在 `Channel` 返回后、`Send` 之前被终结 | 同上。服务端要等一个来回才可能终结流，那时 `Send` 早已发出 |
| `examples/client` `SendOneWay` / `Publish` | 连接在调用前死掉 | 这两个 API **没有 ctx 参数**（§8.4 末行），内部等的是重连；服务端消失就永远等下去——构造出来的测试只会挂住，不会失败 |
| `examples/server` `chat` 的 `ch.Send` | 流在 handler 的 `Receive` 与 `Send` 之间被终结 | 同一个微秒级窗口。示例没开 credit，所以 `Channel.Send` 的失败路径只剩"流已终结"和"传输已死"两条，都要求死亡恰好插在 `Receive` 与 `Send` 之间 |

刻意不去覆盖它们，理由是：为了覆盖率给示例代码加测试钩子、加 sleep，或把双工 handler 改写成一次性收发的形态，都是在破坏示例（它同时是文档、API 范例和面试展示物）的价值。库包里所有等价分支都有确定性测试覆盖——那才是覆盖率该保证的地方；而示例的价值在于"读起来是对的"，不在于"每行都执行过"。

已为可覆盖的部分付出的真实代价（不是白得的）：示例服务端为了让 `serve` 可测而抽出 `options`/`parseOptions`/`serve(ctx)`，示例客户端为了让超时可注入而抽出 `runOptions.callTTL`；示例双工 handler 补了 `io.EOF` 判断，避免对端正常半关闭时被回一个 `ERROR(INTERNAL)` 帧——这一条是**修 bug，不是补覆盖率**。

已知边界与缺点清单（诚实版）：design-notes.md §7；未做的优化（帧缓冲池化、小帧写合并、字段对齐重排）：design-notes.md §8。测试里抓到的 9 个真缺陷（每条都是一条设计教训的实证）：README「测试里抓到的真 bug」。

### 11.4 fuzz 冒烟门禁：把"恶意输入防护"从声明变成检查

三个 fuzz 靶（帧解析 / 变换层 / 握手状态机）都在信任边界上，README bug 清单里的 1、2 两个真 bug 就是它们抓的。但"我本地跑过"会随时间腐烂成一句没人能验证的话，所以 CI 里加了一层固定预算的挖掘：

| 设计点 | 做法 | 为什么 |
| --- | --- | --- |
| 靶名发现 | `go test -list 'Fuzz.*'` 而非手写清单 | 新增 fuzz 靶不必改 CI，也就不会"静默不跑"——与覆盖率门禁同款的承诺腐烂风险，用发现机制根除 |
| 预算 | 每靶 20s（三靶 ≈ 63s 墙钟） | CI 时间是共享成本；这一步买的是"每次推送都重新看一眼不变量"，深挖（分钟级 × 千万 execs）留在本地 |
| 不带 `-race` | 变异吞吐会掉一个数量级 | 分工：竞态由全量 `-race` 跑负责，fuzz 冒烟只兜"panic 与靶内断言的不变量"（解析-编码互逆、任意字节不崩、解压不超上限） |
| 崩溃处理 | 失败即红，且 Go 自动把 crash 语料写进 `testdata/fuzz/<Target>/` | 提交这个文件就等于把一次性发现固化成永久回归用例，之后每次 `go test`（含种子语料）都会重放它 |
| 空靶保护 | 发现不到靶直接失败并报错 | 靶名写错/包路径改动的失败模式必须是红的，不能是"0 靶 0 崩溃"的假绿 |

**这一步每轮到底搜了多少空间**（本机 i9-10880H / 12 fuzz worker，取 `-fuzz` 自报的 execs 计数；同一命令两次实测如下）：

| 靶 | 重载采样（20s 预算，1 分钟负载 ~115~150 / 12 核） | 轻载采样（15s 预算） | 每 exec 的主导成本 |
| --- | --- | --- | --- |
| `FuzzReadFrame` | 12.7 万 | 38.4 万 | 纯内存解析，**成本双峰**：多数输入读满 14 字节头即被拒；一旦长度字段合法就要分配到 `MaxPayloadSize`（16MiB）量级的缓冲，突变体变大即变慢 |
| `FuzzTransformInbound` | 13.4 万 | 37.8 万 | 每 exec 都走 flate + AES-GCM 开箱，但随机数据的 AEAD 认证首块就失败、flate 头也立刻报错——**单价不高，故与帧解析同量级** |
| `FuzzRawPeer` | 1.95 万 | 4.8 万 | 每 exec 起一条真实 TCP 连接走完握手，再让一个合法客户端完整请求一次——慢 3~6 倍的差距来自连接与调度开销，不是被测代码 |

**绝对数字为什么要列两次采样而不是一个值**：本机是共享容器，测这两组数时 1 分钟负载平均在 100~150（12 核），同一命令在轻载/重载之间差到 3 倍——`go test -fuzz` 的吞吐就是 CPU 供给的函数，跨机器引用单值没有意义（与 §11.2 的 ns/op 同理，波动更大；复现时把 `/proc/loadavg` 一起记）。**跨两轮稳定的是相对关系**：`FuzzRawPeer` 比两个纯内存靶慢 3~6 倍，另两个在噪声内持平。要对外说清一个量级：单次 CI 的搜索量是"十万次变异"。

**这个门禁证明不了什么**（避免过度宣称）：20s 的随机变异没有覆盖度保证，"CI 绿"只意味着这一轮变异没撞上已知不变量。真正的防线仍是靶内断言的不变量本身——把"任意字节不得 panic""解析∘编码 = 恒等""解压结果受上限约束"写成断言，fuzz 引擎只负责替人找反例；找到的概率低，不等于断言可以放松。这与 §11.1 覆盖率门禁是同一种哲学：**约束写成可执行检查，工具只提供搜索力**。

### 11.5 一次 CI 自身的假绿：`(cached)` 让门禁不干活

上面三道门禁（覆盖率、fuzz、race）一度全是"名义上存在"的：`actions/setup-go` 默认 `cache: true`（keyed on `go.sum`），会恢复 GOCACHE；而 `go test` 的结果缓存键覆盖测试二进制、环境与被测文件——**只改 README 或工作流时键不变，测试直接复用上次结果**。实测（本机复现，无需 CI）：

```
$ go test -race ./...        # 第一次：ok  github.com/cuihairu/jsonstream  32.320s
$ go test -race ./...        # 第二次：ok  github.com/cuihairu/jsonstream  (cached)
$ go test -coverprofile=c.out .   # 第二次：ok ... (cached) coverage: 100.0% of statements
```

失败模式极其安静：绿灯照亮，什么都没跑。覆盖率门禁最危险——它会拿**上一次**的 profile 冒充这次的证据，恰好在一个"刚改了被测代码但 go.sum 没变"的提交上说"100%"（注意：改了 `.go` 文件二进制就变了，键会失效；真正的风险窗口是文档/注释/非 Go 输入变更，以及**本地**复现门禁时以为跑过其实没跑）。修法两条，都不复杂：

| 做法 | 效果 | 取舍 |
| --- | --- | --- |
| 测试/覆盖率步加 `-count=1` | 只废掉结果缓存，保留 GOMODCACHE/GOCACHE 的编译加速 | 首选：改动面最小，语义精确（我要的是"这次跑过"） |
| `setup-go` 设 `cache: false` | 彻底不恢复 GOCACHE | 连编译缓存都丢，CI 慢十几秒，为一个测试语义问题付全价 |

`go test -bench` 不受此影响（基准结果不进缓存，实测两次都真跑 14s+），所以只有 race 与覆盖率两步需要 `-count=1`。教训可以泛化：**任何"跑一下就算"的门禁，都要先问它在缓存命中的时候会做什么**——和 §11.4 的"靶名发现"是同一类防守：门禁的失败模式必须是"红"，不能是"看起来绿"。

## 12. 面试讲解的展开顺序建议

1. 一句话定位：TCP 上的 JSON 多路复用帧协议，WebSocket 的分帧 + RSocket 的交互模型 + MQTT 的会话语义，各取一截。
2. 画 §2 的分层图，强调"每层只认相邻下层"与栈内明文不变式。
3. 走 §3.1 的一帧旅程，在 prepareRequest 的同步/异步分界停下讲 D5（互锁事故）。
4. 讲 §4 的三层"流"：定长头如何把解析状态机消掉、帧如何逐帧交付、载荷为何延迟解码——这一段最能区分"抄过 WebSocket"和"理解分帧"。
5. 讲并发模型 §6：双 I/O goroutine + 每流执行 goroutine，select 随机性的两个陷阱（死连接双就绪、终结排空）。
6. 讲错误三级分类 §7，引出 kill 单点与幂等释放；顺手翻 §8 的边界清单证明每个"防御"都有具体对手。
7. 用 §9 的复杂度与 §11 的数据收尾（固定成本 vs 边际成本、flate 与 GCM 的 7 倍差），主动抛 design-notes §7 的缺点清单——先于面试官说出来。
