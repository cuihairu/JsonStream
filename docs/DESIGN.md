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

同步/异步的分界线是**本文件最重要的一条设计**，理由见 §7-D5。

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

## 4. 关键数据结构

### 4.1 Frame（frame.go:119）

```go
type Frame struct {
    Header                     // Version/Flags/Type/StreamID 定长头
    Metadata []byte            // 恒明文 JSON（路由/主题），nil 表示无
    Payload  []byte            // 栈内恒明文 JSON；只在读写 socket 边界做变换
}
```

**不变式：payload 在协议栈内部永远是明文**，压缩/加密只发生在 transport 的 outbound/inbound 两个边界函数。备选方案是让 Frame 携带"已加密"状态在栈内流转——放弃，因为每个消费点都得先判断帧是否已解密，一处遗漏就是拿密文当 JSON 解析的 bug；边界化之后，栈内所有代码（路由、分发、保留队列）天然只见明文。

### 4.2 flow（flow.go:26）——单条流的本端视图

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

### 4.3 endpoint（endpoint.go:12）——流调度核心

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

### 4.4 transport（transport.go:15）

```go
type transport struct {
    conn net.Conn; reader io.Reader  // 握手期共享的 bufio
    tr *transformer; cfg *Config; credit *creditGate // nil = 无背压
    sendCh chan *Frame               // 256，全部出站帧的唯一入口
    handler func(*Frame) error       // 读循环分发回调
    deadOnce sync.Once; dead chan struct{}; deadErr error
}
```

### 4.5 serverSession / sessionStore（server.go:388）

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

## 5. 并发模型

### 5.1 goroutine 清单（每条连接）

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

### 5.2 锁与 channel 拓扑

| 同步原语 | 保护对象 | 备注 |
| --- | --- | --- |
| transport.deadOnce/dead | 连接死亡的单次广播 | kill 的幂等性 |
| flow.mu | done/err/ep/inflight/pendingCredit | 锁内不调外部函数，无死锁面 |
| endpoint.idMu | nextID | 唯一的写竞争热点（每发起一次交互一次） |
| endpoint.streamsMu (RW) | streams 流表 | 读多写少，lookup 走 RLock |
| routeTable.mu (RW) | 注册表 | 运行中注册对新请求立即生效 |
| Client.mu / Server.mu / sessionStore.mu / serverSession.mu | 各自字段 | 锁序见下 |

锁序（获取顺序）：`store.mu → ss.mu`（不嵌套，先快照再取）、`ss.mu → ep.streamsMu`（单向）、`flow.mu` 不与任何锁嵌套。sendCh(256)/frames(32)/notify(1)/dead/doneCh/ackCh 的容量与信号语义都写在类型注释里——**cap-1 的 notify 是"信号量"而非"队列"**（credit.go:12），多路 add 只需唤醒一次，`select+default` 投递永不阻塞。

### 5.3 出站串行化：为什么是 sendCh 而不是写锁

所有出站帧（数据、错误、CANCEL、CREDIT、心跳）进同一条 sendCh，由写循环单点写出。备选：`sync.Mutex` 包住 conn.Write——更直接，但三个理由选 channel：(1) 写循环要与心跳 ticker 做 `select`，mutex 模式下心跳注入需要独立的 ticker goroutine 或定时抢锁；(2) sendCh 天然有 256 帧的缓冲，应用 goroutine 与 socket 速度解耦，等价于一层小发送窗口；(3) close(dead) 之后 send 返回 ErrClosed 的语义可以和 select 自然组合。代价是写循环 goroutine 本身与一次 channel 往返（~百 ns 级，相对网络 RTT 可忽略）。

send 的死连接预检（transport.go:76）是被真 bug 逼出来的：连接死后 sendCh 常有空位，`select` 双就绪随机选择，同一次 kill 后的 send 会不确定性地产出"帧入队但永不写出"或 ErrClosed——**Go 的 select 随机性在错误路径上是真陷阱**，消灭它的办法是给死分支优先预检。

### 5.4 确定性交付：排空语义

终结与数据帧的交付是异步的（doneCh 关闭时 frames 里可能还有余帧），而 `select` 双就绪随机选择——所以 `ReadStream.Next`/`Channel.Receive`/`doRequest` 的 doneCh 分支都要**先排空 frames 再返回终结**（flow.go:181 注释），否则会随机丢最后一帧。CANCEL/ERROR 例外：立即终结语义优先，在途帧语义上作废。

## 6. 错误处理策略

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

## 7. 设计决策清单（备选方案与放弃理由）

> 协议层决策（帧头为什么 14B、为什么不做 FIN 分片/MASK/变长长度、Flags 合并请求入口、credit vs LEASE vs 滑动窗口、Metadata 分离、先压后加）的完整论证见 [design-notes.md](design-notes.md) §1–§5，此处只列实现层决策。

- **D1 共用 endpoint 抽象**（§4.3）。备选：客户端/服务端两套调度器——放弃理由：双工与 pub/sub 要求两侧逻辑对称，两套实现必然漂移；现状代价是 client 侧带着三个 nil 钩子的空分支。
- **D2 读写双循环 + 每流执行 goroutine**（§5.1）。备选：单 goroutine 串行分发（慢 handler 队头阻塞心跳）、固定 worker 池（背压阻塞占满池）。选中方案以 goroutine 数量换取消语义的干净。
- **D3 出站统一 sendCh(256) 单点写**（§5.3）。备选：写锁（心跳注入别扭、无缓冲解耦）。
- **D4 kill 单点收尾 + dead channel 广播**。备选：各处自查 conn 状态——放弃，竞态窗口遍布；sync.Once + close(chan) 是 Go 里"一次性广播"的最简正解。
- **D5 被动流注册同步、handler 执行异步**（endpoint.go:205 注释）。`prepareRequest` 在 readLoop 的同步路径完成路由查找/校验/注册，`runRequest` 才进 goroutine。备选：全异步（注册也在 goroutine 里）——放弃，对端发起 Channel 后会立即发数据帧，注册晚于数据帧到达时 lookupFlow miss，帧被静默丢弃，双方死等（实测会发生的互锁）。同步注册的代价是 readLoop 被路由表查找（RLock）短暂占用，可忽略。
- **D6 flow 终结 = close(doneCh) 单原语**。备选：状态枚举 + 轮询——放弃，close 的广播语义让所有等待方一次感知，且 streamContext 直接把 doneCh 包装成 context.Context，handler 的 ctx 取消免费获得。
- **D7 出站一律经当前 endpoint**（flow.currentEP()）。备选：流持有构造时的 endpoint——被 bug 9-6 实锤放弃：重连后 CANCEL/UNSUBSCRIBE 发给死连接被静默吞掉，对端 handler 永远收不到取消。`currentEP()` 的锁开销换消灭一整类陈旧引用 bug。
- **D8 Stream ID 奇偶 + 跨重连单调**。奇偶（HTTP/2 同思路）让新到帧无需协商即可判归属；`bindSession` 把 `oldEp.nextID` 过继给新端点（client.go:376）——重置会让新流与迁移流撞号，旧连接迟到的 COMPLETE 误杀新流（bug 9-5）。
- **D9 会话保留 per-session 内存队列 + 字节上限 + 溢出降级**。备选：写磁盘/外部 broker（题面外）、无上限（OOM 开关）、溢出即断会话（过于激进——降级为"失去恢复资格但连接可用"既防 OOM 又不惩罚已建立的连接）。
- **D10 心跳由写循环注入、死活由读 deadline 判定**。写侧 ticker 到期发 PING，读侧每帧重置 `SetReadDeadline(1.5×间隔)`。备选：TCP keepalive（探不到对端进程死锁/GC 停顿——它测的是内核协议栈）；读侧主动探测（会把心跳职责和读职责搅在一起）。1.5× 是容忍一次丢帧抖动与判死速度的折中。
- **D11 flate 编解码器 sync.Pool 按帧复用**。实测每帧新建 writer 代价 ~1.3ms/~800KB 分配，池化 + Reset 后压缩往返 4.1 倍提速、分配降 162 倍（README 基准）。备选：每帧新建（太贵）、连接级单实例（读写循环已在单 goroutine 内，但订阅消费与 handler 并发 Emit 会竞争）。
- **D12 变换在帧级、标志在帧内自描述**。每帧 Flags 如实标注本帧是否压缩/加密，解码只看帧不看协商（design-notes §3）——实现层配套：`transformer` 无锁（纯函数式进出），栈内明文不变式（§4.1）。
- **D13 测试接缝只设五个包级 var**（jsonMarshal/aesNewCipher/gcmNew/randRead/resubAckTimeout）。这些构造在合法入参下不会失败，其错误分支是防御性死码；接缝之外覆盖率全靠并发时序构造，不动生产逻辑。备选：接口化全部依赖（过度设计）、不设接缝（防御分支永远测不到）。

## 8. 实测数据与验证

语句覆盖率 **100%**（1206/1206），155 个测试函数（含 3 个 fuzz 靶），`-race -count=3` 全绿。测试方法学的细节（确定性并发、race detector 纪律）见 design-notes.md §9。

性能（i9-10880H / Go 1.24，`go test -bench . -benchtime 2s`，量级参考）：

| 基准 | 结果 | 说明 |
| --- | --- | --- |
| 帧编解码往返 64B / 1KiB / 64KiB | ~1.1µs / ~1.1µs / ~67µs | 定长头路径，64B→64KiB 线性于载荷 |
| 变换管线（1.4KiB JSON） | 明文 ~15ns；AES-GCM ~5µs；flate ~60µs | AES-NI 加速下的 GCM；flate 含池化收益 |
| 请求/响应 RTT（本机回环） | ~0.1ms | 含两次变换与完整分发路径 |
| flate 池化收益 | 4.1× 提速 / 分配降 162× | 对比每帧新建 writer |

已知边界与缺点清单（诚实版）：design-notes.md §7；未做的优化（帧缓冲池化、小帧写合并、字段对齐重排）：design-notes.md §8。测试里抓到的 9 个真缺陷（每条都是一条设计教训的实证）：README「测试里抓到的真 bug」。

## 9. 面试讲解的展开顺序建议

1. 一句话定位：TCP 上的 JSON 多路复用帧协议，WebSocket 的分帧 + RSocket 的交互模型 + MQTT 的会话语义，各取一截。
2. 画 §2 的分层图，强调"每层只认相邻下层"与栈内明文不变式。
3. 走 §3.1 的一帧旅程，在 prepareRequest 的同步/异步分界停下讲 D5（互锁事故）。
4. 讲并发模型 §5：双 I/O goroutine + 每流执行 goroutine，select 随机性的两个陷阱（死连接双就绪、终结排空）。
5. 讲错误三级分类 §6，引出 kill 单点与幂等释放。
6. 用 §8 数据收尾，主动抛 design-notes §7 的缺点清单——先于面试官说出来。
