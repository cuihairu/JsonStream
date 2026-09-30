# TCP 流特性与市面协议横评

> 本文回答四个问题：TCP 到底给了你什么；粘包/半包怎么处理；自定义协议怎么决策；市面常见协议各自怎么取舍。事实以 RFC 与各协议规范为准，对 JsonStream 的评价以代码为准。

## 1. TCP 流的字节流特性

TCP 承诺的是字节流，不是消息流。RFC 793 给的抽象是一条双向有序字节流：不丢、不重、不乱序，仅此而已。三个直接推论：

- 无消息边界。应用调用两次 `write`，TCP 不保证对端两次 `read` 各收到一笔：发送端缓冲、Nagle 合并、MSS 分段、接收端读取时机都会重排"写入边界"。应用看到"两笔消息粘成一次读到"（粘包）或"一笔消息分两次读到"（半包），都是这条字节流语义的正常表现，不是 bug。
- 有序可靠是字节级的契约，不是消息级的。TCP 保证字节按序到达，但对"哪些字节构成一条消息"完全无知——消息语义得由应用层自己建立（分帧，见 §2）。
- 队头阻塞在字节流层的表现：接收端按序交付字节，某个段丢失后，其后已经到达的数据也必须等重传补齐空洞才能交付。应用层看到的就是一条连接上丢一个包卡住后面所有消息。这里要分两层看：传输层 HOL 是 TCP 字节流丢包阻塞整条连接；应用层 HOL 是 HTTP/1.1 同连接请求串行、或一个慢消费者拖住全连接那类问题。HTTP/2 用多路复用消灭了应用层 HOL，但所有流仍跑在一条 TCP 字节流上，传输层 HOL 原样继承；HTTP/3/QUIC 把流做进传输层，才连这层一起解决（§4）。

所以粘包不是 TCP 的 bug，是字节流语义的本意。要消息语义，唯一的路是应用层分帧。

## 2. 粘包/半包的一般处理：三种分帧手段

| 手段 | 代表 | 适用 | 失败模式 / 代价 |
| --- | --- | --- | --- |
| 定长 | 传统信令/工控二进制定长报文 | 消息长度固定且小 | 变长消息要 padding（浪费带宽）或截断；JSON 本质变长，完全不适用 |
| 分隔符 | Redis 行协议、SMTP、NDJSON | 文本协议 | 载荷含分隔符需转义（逐字节扫描状态机）；二进制载荷不可用——压缩/加密后的字节流里任何字节都合法；接收端无法提前预分配。注意：JSON 字符串里控制字符必须转义，所以"未压缩 JSON 按换行分帧"（NDJSON）其实是安全的；本项目弃它的理由与 JSON 无关，是 payload 要压缩/加密（[protocol.md](protocol.md) §3.3、[NOTES.md](NOTES.md) §1） |
| 长度前缀 | WebSocket length 字段、HTTP/2 Length、RSocket FRAME_LENGTH、本协议 | 二进制协议的通用解 | 发送端必须先知道长度（对"整条消息编码完再发"的模型无负担）；长度字段变长（WS 7/16/64 位）省字节但换解码分支状态机，定长（本协议固定 4B 大端）无条件两段读；长度上限即内存防线——对端报多大就预分配多大，上限必须存在 |

本协议选了 14B 定长头 + 32 位大端长度前缀，解码端"读完头 → 两次 `ReadFull`"成为无条件操作（[protocol.md](protocol.md) §3）。

缓冲层有两个易混的工具，职责不同，不可互替：

- `bufio`（本协议 16 KiB）管 syscall 次数：从内核批量搬字节，一次 `read(2)` 拿一小段缓冲，后续帧从用户态缓冲直接取。
- `io.ReadFull` 管取满 n 字节：契约是"读满或出错"，内部循环消化半包。

没有 bufio，一个带元数据的帧要多次 `read(2)`；没有 ReadFull，`Read` 只保证"至少 1 字节"，半个帧头就会把解析读乱（[DESIGN.md](DESIGN.md) §7 边界清单）。

## 3. 协议如何设计与取舍：决策框架

自定义二进制协议的每个决策都能放进这几个槽里：

1. 帧头开销账目。每帧固定头 = 带宽税 × 帧数，小帧最受伤。本协议 14B 对数百字节起的 JSON 是 <3% 的税；对心跳帧是实打实的浪费——所以心跳帧不带 Metadata、payload 为空，14B 就是下限（[design-notes.md](design-notes.md) §1.2、§1.3 的完整账目）。
2. 控制面/数据面分离。握手、心跳、连接级错误、credit 属控制面（Stream ID=0、握手帧恒明文）；业务交互属数据面（带流 ID、可受压缩/加密变换）。混在一起，会让"必须明文、必须低开销"的帧背上数据面的包袱。
3. 扩展位策略。保留位必须为 0、未知类型立即断开，给未来升级留一扇语义清晰的门；代价是异版本互通直接失败。本协议认为这是特性：半互通比静默错位危险。对照"保留但忽略"路线：兼容性好，但对端乱用保留位时行为不可知。
4. 背压归属。传输层窗口（TCP、HTTP/2，按字节）管的是链路；应用层 credit（按条数）管的才是"我还没消费完"；租约（RSocket LEASE，按时间）管的是速率不是在途量。本协议选 credit、连接级、默认关闭——它是全协议唯一反向影响应用并发模型的机制，不该默认强加（[design-notes.md](design-notes.md) §5）。
5. 状态机复杂度换功能。每个"功能"都要一个常驻状态机养着（分片重组、变长解码、掩码往返、恢复队列）。本协议 v1 把解码路径做成无状态两段读（定长头 + 长度前缀的直接红利：可 fuzz、可任意丢帧重入），分帧机制里唯一有状态组件是断线恢复的保留队列。

对照本项目的四个标志性取舍：14B 帧头账目（[protocol.md](protocol.md) §3.2：Magic+Version 是自付的保险费）、credit 可选背压（§7.9）、单帧 16 MiB + 大消息分片（§3.4：独立 FRAGMENT 帧型换掉 FIN+continuation，重组帽 MaxMessageSize 默认 64 MiB，实现现状在 [interview-requirements.md](interview-requirements.md) 有标注）、先压后加不可逆顺序（§6）。

## 4. 市面常见协议横评

### 4.1 MQTT

MQTT 的帧很小：1 字节类型加标志位，再跟 1–4 字节的剩余长度 varint。topic 支持 `+`/`#` 通配订阅；QoS 分三级——0 至多一次、1 至少一次（PUBACK 确认）、2 恰好一次（PUBREC/PUBREL/PUBCOMP 四步握手）；keepalive 靠 PINGREQ/PINGRESP。MQTT 5.0 又加了属性、原因码、Receive Maximum（QoS>0 时未确认在途数上限的流控）和 session expiry。拓扑上是 broker 中介：发布者与订阅者经 broker 解耦，保留消息、遗嘱这些语义也都由 broker 承接。

这套设计的好处在物联网弱网下最明显：端侧极薄，投递、保留、遗嘱一个地方全管了，QoS 分级在不可靠链路上实用。代价：没有请求/响应原语（MQTT 5 的 response topic + correlation data 是约定，不是协议语义）；payload 对 broker 不透明，想按内容路由就得破端到端；topic 树是全局命名空间，权限与运维模型复杂；broker 本身是必须养的基础设施。

适用物联网遥测、消息总线、弱网大量小消息。对 JsonStream 的参照：SUBACK 确认惯例直接借鉴（[protocol.md](protocol.md) §4）；QoS 不做分级——pub/sub 投递随会话恢复机制天然是 QoS1-ish（§10）；拓扑上反着选，JsonStream 是点对点直连协议，没有 broker，路由（Metadata.route）对中间件可读但不需要中介设备。

### 4.2 WebSocket

WebSocket 先走 HTTP/1.1 `Upgrade` 握手（101 切协议），之后是 RFC 6455 §5.2 的帧格式：2–6B 变长帧头（7/16/64 位长度）+ FIN 分片 + 客户端掩码 + opcode（text/binary/close/ping/pong）。`wss://` 是 TLS 一等承载，443 可复用。

它最大的优势是浏览器原生可达——`new WebSocket()` 是浏览器唯一的标准全双工通道，穿透代理/防火墙能力也强，全语言生态覆盖齐全。但 WS 只给了"一条连接上的无序消息"：请求/响应关联、订阅、流式、背压信令、断线恢复，全要应用层自造；一条连接也没有多路复用语义。

适用浏览器实时应用（聊天、行情、协同）。对 JsonStream 的参照：帧式心跳（PING/PONG + 空闲超时）与长度前缀分帧的惯例借鉴；变长长度、客户端掩码两点显式不做——raw TCP 直连没有掩码要防的那个威胁模型；FIN 分片换形为独立 FRAGMENT 帧型（protocol §3.4，各条理由与逐项能力对照见 [websocket-comparison.md](websocket-comparison.md)）。

### 4.3 RSocket

RSocket 是二进制协议，可跑在 TCP/WebSocket/Aeron 之上，帧头 6–10B。四个交互原语（REQUEST_RESPONSE / REQUEST_STREAM / REQUEST_FNF / REQUEST_CHANNEL），Stream ID 多路复用。背压双轨：订阅方 `request(n)`（credit）加连接级 `LEASE`（租约限速）；大 payload 自动 fragmentation 分片；还有可选的 Resume——携带 resume token 的会话恢复，可以做到精确语义。

它的强项是响应式语义最完备：背压、取消、恢复都在协议内建，且传输无关。弱点在复杂度：帧类型多，resume/lease/fragmentation 全套要养，实现门槛高，生态又以 JVM 为主，普及受限。适用服务间响应式流（Reactive Streams 的线上化）。

对 JsonStream 是最近的亲缘：四原语划分、Stream ID 多路复用、恢复语义直接借鉴。分歧点也明确：三种请求合并为一个 `REQUEST` + Flags.Stream/Channel 位（解析分支减半，代价是中间件不能从帧头预判模式，[design-notes.md](design-notes.md) §2.2）；fragmentation 走了不同路线——不用 RSocket 的自动分片，定稿为显式 FRAGMENT 帧型（protocol §3.4，实现落地中）；resume 简化为 at-least-once（精确一次需要 per-frame 序号 + 确认窗口，v1 判定复杂度不成比例，[protocol.md](protocol.md) §8.3）；背压默认关闭、只有 credit 一轨（LEASE 防滥用不防背压）。

### 4.4 HTTP/1.1 → HTTP/2 → HTTP/3(QUIC)

HTTP/1.1 是文本协议，同连接请求串行——keep-alive 只复用 TCP，应用层 HOL 还在。HTTP/2 二进制帧化 + 单 TCP 连接 Stream ID 多路复用 + HPACK 头压缩，消灭了应用层 HOL；但所有流共享一条 TCP 字节流，传输层 HOL 原样继承（§1）。HTTP/3 跑在 QUIC（UDP）上：TLS 1.3 内建进握手，流级独立重传——一条流丢包不再阻塞其他流，0-RTT 会话恢复，连接迁移（Connection ID 与四元组解耦，换网不断连）。

优点一头在生态：中间件、缓存、CDN、浏览器，HTTP 无敌；HTTP/3 在高丢包/移动网络下表现最好。缺点：HTTP/1.1 文本解析与头冗余；HTTP/2 受制于 TCP 队头，丢包全连接卡；HTTP/3 依赖 UDP（部分网络拦截/限速），用户态拥塞控制实现复杂，调试工具链还在成熟中。HTTP/2 适合 Web API 通用场景，HTTP/3 适合全球分发、移动端、高丢包环境。

对 JsonStream 的参照：Stream ID 奇偶划分与 HTTP/2 同思路（新到流无需协商即知发起方，[protocol.md](protocol.md) §7.3）；credit 按条数而非 HTTP/2 的按字节窗口——JSON 消息的业务心智单位是条不是字节。定位结论是事实，不必装饰：浏览器可达性与中间盒友好性上，裸 TCP 自定义协议让位于 wss/QUIC。JsonStream 的适用域是受控网络里的服务端间长连专用通道（专用客户端/服务端直连，无浏览器、无中间代理假设），价值在语义层（req/res + pub/sub + 背压 + 恢复内建）而非传输普适性。

### 4.5 汇总对比

| | 传输 | 分帧方式 | 背压 | 多路复用 | 浏览器可达 | TLS 承载 | 恢复机制 | 典型场景 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MQTT | TCP | 固定头 + 剩余长度 varint | v5 Receive Maximum（QoS 在途上限） | 无（无流概念） | 否（可跑 WS 上） | 可叠 TLS | 持久会话 + QoS 重传 | 物联网/消息总线 |
| WebSocket | TCP | 2–6B 变长头 + FIN 分片 | 无（应用自造） | 无（应用自造） | **是** | wss 一等 | 无（应用自造） | 浏览器实时 |
| RSocket | TCP/WS/Aeron | 6–10B 头 + fragmentation | request(n) + LEASE | Stream ID | 否（可跑 WS 上） | 随传输层 | Resume（可选精确） | 服务间响应式流 |
| HTTP/2 | TCP | 二进制帧 + HPACK | 流窗口（按字节） | Stream ID | 否 | 是（ALPN h2） | 无 | Web API 通用 |
| HTTP/3 | QUIC（UDP） | QUIC 帧 | 流级窗口 | 原生流 | 否（WebTransport 另议） | 内建 TLS 1.3 | 0-RTT 重连 + 连接迁移 | 高丢包/移动网络 |
| **JsonStream** | TCP | 14B 定长头 + 32 位长度前缀 | credit（可选、连接级、按条） | Stream ID（奇偶、跨重连单调） | 否 | 服务端开箱可叠（`tls.Listen`）；客户端缺注入点 | 会话保留 + 重放（at-least-once） | 服务端间长连专用通道 |

这条赛道上没有全能选手。MQTT 用 broker 换端侧极薄，WebSocket 用语义贫瘠换浏览器可达，RSocket 用复杂度换语义完备，HTTP/3 用 UDP 换传输层性能。JsonStream 在受控网络的小生态位里，用 raw TCP 换到了"语义内建 + 零依赖"的组合。
