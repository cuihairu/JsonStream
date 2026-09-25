# JsonStream 技术基础知识梳理（结合实现讲原理）

本文按面试考点组织：每个知识点先讲原理，再对应到本仓库的具体实现（文件:行号），最后给"面试怎么讲"的要点。与 [DESIGN.md](DESIGN.md)（架构与取舍）、[protocol.md](protocol.md)（协议规范）互为引用。

阅读顺序建议：§1–§2 是 JSON 解析本身的原理与形态选型（本项目叫 JsonStream，这两块是最直接的考点）；§3 往后是网络与 Go 侧的通用工程考点。§15 是一份可以单独拿出来复习的 Go 陷阱清单。

## 1. JSON 解析原理：从文法说起

### 1.1 文法：为什么 JSON 好解析，又为什么没那么好解析

RFC 8259 的文法极简（这里刻意省略了 RFC 允许实现自定扩展的部分）：

```
JSON = value
value = object | array | string | number | "true" | "false" | "null"
object = "{" ws (string ":" ws value ("," string ":" ws value)*)? "}" ws
array  = "[" ws (value ("," value)*)? "]" ws
string = '"' char* '"'          char = unescaped | escape (["\\\/bfnrt] | u 4HEXDIG)
number = [ "-" ] int [ frac ] [ exp ]   int = "0" / (1-9 *DIGIT)
```

结构字符只有 `{ } [ ]` 四个，字符串又用引号定界——这让"扫到引号外的括号就配对"成为可能。但有个反直觉的事实必须先说清楚：**结构字符在字符串里可以合法出现**（`{"a":"{}"}` 是合法 JSON，`{` 不需要转义）。真正保护解析器的是**字符串扫描规则**（引号闭合 + 反斜杠转义），而不是"括号不会出现在字符串里"。下面第 1 条展开这一点。

顺带说一个与本项目相关的推论：JSON 字符串里的控制字符（换行等）**必须**转义，所以"未压缩的 JSON 用换行分帧"（NDJSON）其实是安全的。本项目放弃分隔符成帧的理由与 JSON 无关——是 payload 还要压缩/加密，压缩后的字节流里任何字节都可能出现（protocol.md §3.3）。

没那么好解析的原因有三个，面试常问：

1. **转义使"扫描"不等于"数括号"**：`"a\"}b"` 里的 `}` 是字符串内容，而结构字符在字符串里又完全合法。朴素的"数 `{`/`}` 配对"必然失效——它没法区分 `{"a":"{}"}`（一个对象）里的 `{}`（字符串内容）。正确做法是完整的词法状态机：先按"字符串/字符串外"两条规则切分，再在字符串外数括号（RFC 8259 附录给了完整的 ABNF 状态机）。
2. **数字不是自描述的**：`1` 可能是整数、`1.0` 是浮点、`1e400` 是溢出。`encoding/json` 的默认策略是"能塞进 `float64` 就塞"，塞不进就报错（实测）：
   ```go
   json.Unmarshal([]byte(`1e400`), &v)  // v any
   // json: cannot unmarshal number 1e400 into Go value of type float64
   ```
   更隐蔽的是**不报错但丢精度**：`9007199254740993`（2^53+1）解到 `any` 会变成 `9007199254740992`——尾数被静默截断。要避免只有两条路：解到具体整数类型，或用 `json.RawMessage` / `Decoder.UseNumber()` 保留原始词法。
3. **嵌套深度无上限**（文法允许任意深）。这是 DoS 入口：深度 10 万的数组会让递归下降解析器爆栈。Go 的 `encoding/json` 有 `maxNestingDepth = 10000` 的硬限制（`scanner.go`），超了报 `exceeded max depth`；一百万个元素的数组则让 DOM 解析吃掉几百 MB——**这正是流式解析存在的理由**。

### 1.2 Go 的 `encoding/json` 是怎么工作的

理解三个事实，就能解释本项目绝大多数 JSON 相关的取舍：

- **`Unmarshal` 是两趟**：Go 源码 `decode.go` 的 `Unmarshal` 先 `checkValid(data, &d.scan)` 整份校验一遍（注释写明"Avoids filling out half a data structure before discovering a JSON syntax error"），再 `d.unmarshal(v)` 建值。代价是同一份字节被走两遍，收益是**语法错误在动手改目标结构之前就暴露**——顺带也意味着尾部多余字节会报错（实测 `{"a":1} garbage` → `invalid character 'g' after top-level value`）。
- **解码目标决定代价**：解到**具体类型**（struct）时，字段是按名字匹配的、反射路径短；解到 `any`/`interface{}` 时，每个值都要装箱成 `map[string]any` / `[]any` / `float64`，分配数量与嵌套深度成正比。想避免 float 精度损失要用 `json.Decoder.UseNumber()`，或者解到 `json.RawMessage`。
- **未知字段默认忽略，`DisallowUnknownFields()` 才报错**；**重复键后者覆盖前者**（实测 `{"a":1,"a":2}` → `map[a:2]`，对 struct 也是后一个字段赢）；**`<` `>` `&` 默认被转义**（`encode.go` 里 `escapeHTML: true`，实测 `{"html":"<a>&</a>"}` 编码成 `{"html":"\u003ca\u003e\u0026\u003c/a\u003e"}`，`SetEscapeHTML(false)` 可关）——最后这条在往 HTML 页面里嵌 JSON 时会咬人，也是"JSON 不是 JS 的子集"这个老问题在 Go 上的残留。

### 1.3 本实现的落点

本项目只用 `json.Marshal` / `json.Unmarshal` / `json.RawMessage`（全仓库仅这三处 JSON API，见 `json.Marshal` 10 处、`json.Unmarshal` 6 处、`json.RawMessage` 2 处），且**不用 `json.Decoder`**。这不是遗漏，是 §2 的结论：帧边界已经是天然的流单元，帧内再套 token 状态机换不到东西。

具体落点：

| 场景 | 用法 | 位置 |
| --- | --- | --- |
| 帧载荷编码 | `json.Marshal(v)`（接缝 `jsonMarshal` 可注入失败） | message.go:80, config.go:12 |
| 帧载荷延迟解码 | `Message.Payload` 是 `json.RawMessage` | message.go:17 |
| 应用按需解析 | `Message.Decode(v)`，空载荷直接返回 nil | message.go:21 |
| 路由/主题元数据 | 独立的小 struct `metaJSON`，`omitempty` | frame.go:218 |
| 握手报文 | `ConnectJSON` / `connackJSON`，`omitempty` 控制线上体积 | handshake.go:10/21 |
| 错误帧 | `Error{Code, Message}`，`errDecode` 对畸形载荷兜底 | errors.go:49/68 |

**面试怎么讲**：能背出 JSON 文法并说明"数括号不够用"的原因；能说出 `Unmarshal` 是"先全量校验再解码"两趟；能解释为什么本项目选 `RawMessage` 延迟解码而不是 `Decoder` 流式（答：帧边界已是流单元，帧内 token 流要多一套状态机却没有"提前开始处理"的收益，见 §2）。

## 2. 流式 vs DOM：三种形态与选型

### 2.1 三种形态

| 形态 | 内存 | 什么时候开始处理 | 能提前停止 | 代表 |
| --- | --- | --- | --- | --- |
| **DOM**（整树） | O(n) | 全部读完后 | 否 | `json.Unmarshal` |
| **流式 / token** | O(1)–O(深度) | 边读边处理 | 是（不读完就返回） | `json.Decoder.Token()` / SAX 解析器 |
| **延迟解码**（中间态） | O(n)（原始字节） | 由应用决定何时解析 | 取决于应用 | `json.RawMessage` |

补充两点常被忽略的差别：

- **DOM 的真正优势不是随机访问，是"先验证后使用"**。整棵树建起来意味着"这个 JSON 一定合法"已经被证明过了；而流式解析里，语法错误可能在读到一半时才暴露，此前已经基于前半段做出了副作用（写过库、发过请求）——这就是流式解析必须配合"出错要能回滚"的原因。
- **流式的真正优势是内存上界与背压友好**。DOM 的大小由对端决定（一个 10GB 的 JSON 数组直接 OOM）；流式可以让"读不完就不读"，配合 `io.Pipe` 把背压传导回生产者。DOM 做不到"读到第 100 条就停"——除非你手工切分。

### 2.2 选型表

| 需求 | 该选 | 理由 |
| --- | --- | --- |
| 配置文件、结构已知、一次性读 | DOM | 代码最短，类型安全，错误早暴露 |
| 单条消息可能大于内存预算 | 流式 | 唯一可行解（NDJSON、日志、导出） |
| 同一份字节要转发/重放/存盘 | 延迟解码 | 解析一次都不付，只在真正需要时付 |
| 只要对象里的一个字段 | 流式（token + `More()`） | DOM 必须整棵建起来 |
| 消息边界已由外层协议给出 | 延迟解码 | 见下 |

最后一行是本项目的情形，值得展开。

### 2.3 本项目为什么是"帧级流式 + 载荷延迟解码"

外层已经把消息边界切好了（14B 定长头 + 长度前缀），于是"流式的两个核心收益"各有不同的答案：

- **内存上界**：不需要靠流式来获得——16MiB 单帧上限（分配前校验）+ 32 深的通道缓冲 + 可选 credit 窗口（DESIGN.md §9.2）已经把总量钉住。
- **背压**：不需要靠解析器来获得——背压发生在帧的投递层（`flow.deliver` 阻塞读循环 → TCP 接收窗口），JSON 解析器根本不参与。

那流式还剩什么收益？**"不等完整消息就开始处理"**。而这个收益在本项目里恰好用不上：帧是"一条完整的业务消息"（一次请求、一次响应），业务上本来就要等它完整才能处理——不等完整的只有"传输层字节流"，而那已经被定长头分帧解决了。

于是结论落回 `json.RawMessage`：**帧内不流式，但也不急着建树**。它拿到"DOM 的按需性"（不解析就不付 CPU），代价是"没有提前开始处理"（这正好符合业务语义），并且额外收获了正确性——原始字节不经 `float64` 往返，重放/转发不会有精度漂移。实测这条差别是实打实的：

```go
var v any
json.Unmarshal([]byte(`{"id":9007199254740993}`), &v)   // → map[id:9007199254740992]  尾数被静默截断
var raw json.RawMessage
json.Unmarshal([]byte(`{"id":9007199254740993}`), &raw)  // → {"id":9007199254740993}  原始字节原样保留
```

本项目的保留队列会把下行帧原样存下来、等重连后重发——如果载荷在入队前被解过一遍，replay 出来的 ID 就和第一次不一样了，而上层没有任何办法察觉。**这类"重放语义要求字节级保真"的场景，是 `RawMessage` 优于 DOM 的硬理由，而不只是性能考量。**

### 2.4 "读过头"：流式解析的头号陷阱

任何带缓冲的流式解析器都会**多读**。`json.Decoder` 为此专门提供 `Buffered()` 来取回未消费的余量——如果你在同一个 `io.Reader` 上既用 `Decoder` 又手工读，就会丢掉已经读进 Decoder 缓冲的字节，症状是"偶发丢第一条记录"。

本项目在帧层遇到的是同一个问题，解法不同但同源：**不新建 reader，而是把同一个对象传下去**。

```go
br := bufio.NewReader(nc)       // 握手阶段建
hf, err := ReadFrame(br)        // 可能把 CONNACK 的前几字节读进 br 的缓冲
tr := newTransport(nc, br, …)   // ← 传的是 br 本身
```

`transport` 的字段类型因此是 `io.Reader` 而不是 `*bufio.Reader`（transport.go:17 注释写明"与握手阶段共享的 bufio，避免缓冲数据丢失"）。如果这里新建一个 `bufio.NewReader(conn)`，握手阶段缓冲里的字节就被永久跳过。

注意 `readLoop` 里**又包了一层** `bufio.NewReaderSize(t.reader, 16<<10)`（transport.go:156）却依然安全：它包的是 `t.reader`（那个共享的 `br`）而不是 `conn`。多一层缓冲只多一次内存拷贝，不跨阶段抢字节。这类"看起来冗余"的代码必须在注释里写清理由，否则下一个人会顺手优化掉它。

**面试怎么讲**：能画出三种形态的内存/时序差异；能说清"帧边界已给定 ⇒ 帧内 token 流是负收益"这条推理链（这是本题最容易被追问、也最能体现判断力的一问）；能讲"读过头"以及为什么本项目用"共享 reader"而不是 `Buffered()` 解决它。

## 3. TCP 字节流与分帧（粘包/半包）

**原理**：TCP 是字节流协议，只保证字节有序可靠，不保证"消息"边界。发送方三次 write 的内容可能被合并成一次到达（Nagle 算法、MSS 填充），也可能一次 write 被拆成多段到达（MSS/MTU 限制）。所以应用层协议必须自带分帧，手段只有三种：定长消息、分隔符、长度前缀。

- 定长：实现最简，载荷不齐要填充，浪费带宽——只适合固定尺寸的协议（如 TCP Proxy Protocol v1 之前的一些固定头）。
- 分隔符：文本协议常用（Redis RESP 的 `\r\n`、HTTP 头的 `\r\n\r\n`）。致命问题是**分隔符在载荷中出现时需要转义**，JSON 里任意转义都可能出现在字符串值中，转义方案会把解码变成变长扫描。
- 长度前缀：头里写明"接下来多长是载荷"，解码端"读头 → 按长度读体"，无条件操作。HTTP/2、WebSocket（扩展长度）、MQTT 剩余长度、RSocket 都是这类。

**本实现**：14B 定长头 + 32 位大端长度前缀（frame.go:23）。`ReadFrame`（frame.go:168）是教科书式两段读：`io.ReadFull(head[:14])` 再 `io.ReadFull(payload)`——ReadFull 的语义就是"读满或出错"，半包由它内部循环解决，粘包由 bufio 缓冲解决。上层 `transport.readLoop`（transport.go:156）再包一层 16KiB bufio，从内核批量搬字节减少 syscall。

**面试要点**：能说清"粘包不是 TCP 的 bug，是字节流语义的本意"；能推导为什么 JSON 不适合分隔符成帧；知道 `io.ReadFull` vs `io.ReadFull` 循环 vs `bufio` 的分工（ReadFull 解决半包，bufio 解决 syscall 次数，二者不可互相替代）。

## 4. 字节序与二进制编码

**原理**：网络字节序规定为大端（高位字节在低地址），历史原因是位拆解直观。Go 的 `encoding/binary` 提供 `BigEndian.Uint32`/`AppendUint32`；`AppendUint32` 是零分配写法（append 风格），比 `binary.Write(w, ...)` 走 io.Writer 接口快一个量级（接口调度 + 装箱）。

**本实现**：frame.go:144-153 全部用 `binary.BigEndian.AppendXxx`；`Frame.appendTo(dst)` 是 append 风格 API——调用方传入可复用缓冲，编码器只追加。写循环里 `buf, err = t.encodeFrame(buf[:0], f)`（transport.go:118）就是"复用上一帧的缓冲"——`buf[:0]` 重置长度保留容量。

**面试要点**：为什么大端（历史 + 可读性 + 网络惯例）；`binary.Write` 与 `AppendUint32` 的性能差距来源（接口调用、反射路径）；append 风格 API 在热路径的价值。

## 5. 状态机思维：协议状态与流状态

**原理**：协议实现里有两类状态机，分层后各自都很简单：

1. **帧解码状态机**：定长头方案下退化为"无状态两段读"——不需要跨帧记忆（对比 WebSocket 的 FIN 分片重组状态机、变长长度的分支状态机）。**砍掉状态机本身就是设计目标**：每少一个状态，解码路径就少一类"状态被污染后帧流错位"的故障。
2. **流生命周期状态机**：每条流 init → active → done（正常/错误/取消三途）。本实现收敛为一个布尔 + 一个广播 channel（flow.go done/doneCh），而不是显式枚举状态——因为所有等待方只关心一个问题"这条流结束了吗、以什么错误结束"。

**本实现**：`flow.finish`（flow.go:145）是唯一的状态迁移函数，锁内写 done/err、close(doneCh)、注销流表，幂等（二次 finish 直接返回）。`streamContext`（message.go:49）把 doneCh 包装成 `context.Context`——流的终结免费变成 ctx 取消，应用 handler 用标准库习惯写可取消逻辑。

**面试要点**：能画出流状态机并说明为什么没有 PAUSED 等中间态（背压不改变流状态，只改变发送许可——两轴正交）；能解释"用 channel close 表达一次性状态迁移"为什么是 Go 惯用替代"状态枚举 + 条件变量"。

## 6. 增量缓冲与背压

**原理**：背压的本质是**把下游的消费速度反向传导给上游生产者**。没有背压时，快速生产者 + 慢速消费者 = 无界队列 = OOM（或 TCP 缓冲满后静默丢帧，取决于缓冲在哪一层）。实现背压有三个位置：

1. **传输层自带**：TCP 滑动窗口（按字节）——发送方填满对端接收窗口就阻塞。这是"免费"的兜底，但它只约束 TCP 字节，应用层消息在多条 channel 里排队时 TCP 还没满。
2. **channel 容量**：Go 的 channel 发送在缓冲满时阻塞，天然背压——但阻塞点在哪决定了谁被拖住。
3. **显式信用（credit）**：接收方授权 N 条，发送方耗尽即停，交付后归还。Reactive Streams 的 `request(n)`、HTTP/2 的 WINDOW_UPDATE 同族。

**本实现**（三层都用上，各司其职）：

- 每流 `frames chan *Frame` 缓冲 32（flow.go:22）——未启用背压时的兜底：缓冲满则读循环阻塞在 deliver，反向传导到 TCP。
- 连接级 `creditGate`（credit.go）：`take` 取令牌、`add` 补令牌、cap-1 的 notify channel 做"信号量"（多次 add 只需一次唤醒，`select+default` 投递永不阻塞，credit.go:14）。归还在**交付应用后**（`flow.release`），不是收到帧时——早了背压失效（帧在应用层暗中堆积），晚了吞吐骤降；累计到**半窗**批量回授 CREDIT 帧（flow.go:93），摊薄控制帧开销，位置对齐 HTTP/2 WINDOW_UPDATE 的工程惯例。
- 之所以 credit 默认关闭：额度归零时 `Emit()` 阻塞 handler goroutine，是全协议唯一反向影响应用并发模型的机制；连接级窗口存在"一个慢订阅拖住同连接所有流"的队头阻塞。详细论证 design-notes §5。

**面试要点**：背压的失效模式（归还太早/太晚）；credit vs LEASE vs 滑动窗口的语义差异（约束在途量 vs 约束速率 vs 字节记账）；为什么"收到帧≠归还"。

## 7. 内存与零拷贝

**原理**：网络程序的内存账本 = 每帧的分配次数 × 帧速率。零拷贝手段从便宜到贵：(1) 复用缓冲（append 风格 + `buf[:0]`）；(2) 延迟解码（`json.RawMessage`）；(3) 池化（`sync.Pool`）；(4) 真正的零拷贝（`net.Buffers`/writev、`mmap`、eBPF/io_uring 侧通道）。前三个是纯用户态手段，最后一个要系统调用配合。

**本实现**：

- **append 风格编码**：`Frame.appendTo(dst)`（frame.go:133），写循环复用同一 `buf`。
- **延迟 JSON 解码**：`Message.Payload` 是 `json.RawMessage`（message.go:17）——帧到达时不反序列化，应用 `Decode(v)` 时才解。路由分发、保留队列、订阅转发都不碰 payload 内容，这既是性能（不解不付 CPU）也是正确性（`RawMessage` 保持原始字节，转发不产生浮点精度漂移）。
- **sync.Pool 池化 flate 编解码器**（transform.go:23）：flate.Writer 构造要建 32KB 级别内部表，实测每帧新建 ~1.3ms/~800KB 分配；池化 + `Reset` 后压缩往返 4.1× 提速、分配降 162×。取出即 Reset（transform.go:125）——上一使用者遗留的流状态（含越限中断的半流）被重新初始化，池化不跨帧泄漏状态。
- **Metadata 明文常驻**：保留队列里的帧直接持引用，不深拷贝——前提是栈内没有人改 Frame 字段（Append 语义由调用方约束）。

**面试要点**：sync.Pool 的 GC 语义（两轮 GC 后清空，池命中率取决于分配压力，不是缓存）；为什么不用 `bytes.Pool` 之类的固定池（帧大小 14B~16MiB 跨三个数量级，固定尺寸池必然浪费）；`json.RawMessage` 的边界（它只是 `[]byte` 类型别名 + MarshalJSON，不解码也不校验）。

## 8. Go channel 与 goroutine 调度

**原理**：goroutine 是 M:N 调度的用户态线程，channel 是 CSP 模型的同步原语。要点：发送/接收在缓冲未满/非空时不阻塞、阻塞时 goroutine 挂起（gopark）让出 P；`close` 是广播（所有接收方立即返回零值+ok=false）；**`select` 在多个就绪分支间随机选择**——这是公平性设计，也是错误路径的陷阱源。

**本实现的三个"被 bug 教育出来"的 channel 纪律**：

1. **死连接上的 select 双就绪**（transport.go:78）：连接死后 sendCh 常有空位，`send` 的 select 可能随机选中入队分支——帧入队但永不写出，静默丢失。修法：进 select 前先对 dead 做非阻塞预检，给"死"优先级。教训：**select 的随机性在错误路径上不是公平，是不确定**。
2. **终结排空**（flow.go:181）：doneCh 关闭时 frames 里可能还有余帧，Next 的 select 双就绪随机选择，直接返回 false 会随机丢最后一帧。修法：doneCh 分支先排空 frames。教训：**异步终结与缓冲交付的组合必须在消费端显式排空**。
3. **cap-1 信号量**（credit.go:14）：notify 容量为 1，多次 add 只保留一次唤醒信号——因为它传的是"有新令牌"这个事件而非数量，数量在 mu 保护的 n 里。这是把"事件"与"计数"分离的标准写法，避免每令牌一个 channel 元素。

**调度侧**：每连接两 I/O goroutine + 每流一执行 goroutine 的模型下，Go 的 netpoller（epoll/kqueue）把阻塞在 net.Conn.Read/Write 的 goroutine 挂起、就绪时唤醒——所以"每连接一个读 goroutine"在万级连接下依然便宜（每 goroutine 初始栈 ~8KB，1 万连接 ~80MB 栈上限但实际按需增长）。GOMAXPROCS 个 P 分摊就绪队列，多核利用不需要应用做任何事。

**面试要点**：能讲 close 的广播语义与"closed channel 恒就绪"如何被用来做取消传播（dead/doneCh/closed 都是）；能讲 select 随机性的两面性；知道 channel 不是免费的（一次往返 ~50-100ns，热路径上缓冲大小要给理由——sendCh 256 是"突发下不阻塞应用"，frames 32 是"无背压时的兜底"）。

## 9. GC 与性能取舍

**原理**：Go 的 GC 是并发三色标记，代价主要在：分配率（分配越多，GC 周期越密）、指针数量（扫描成本）、大对象（>32KB 直接进大对象槽）。网络服务的优化套路：减少每帧分配（池化/复用）、用 `[]byte` 而非 `string` 往返转换（`string(bytes)` 必拷贝）、控制大缓冲生命周期。

**本实现**：

- 分配热点实测在 flate writer（§7 已述），池化后每帧分配降到个位数。
- `Frame.Payload` 用 `[]byte` 一贯到底，Metadata 解码出的 route/topic 才转 string（map 查找键需要可比性/不可变）——`[]byte` 不能做 map 键（切片不可比较），这是选 string 的真实约束，不是偏好。
- 16MiB 载荷上限同时是 GC 防线：`ReadFrame` 对 payloadLen 先校验再 `make`（frame.go:191），恶意长度字段在分配前就被拒绝。**上限必须在分配之前检查**——先分配后校验等于把 OOM 开关交给对端。
- 明确不做的：帧缓冲池化（帧在 sendCh 排队期间生命周期归写循环，池化要审计异步生命周期，收益百 ns 级）、字段对齐重排（基准瓶颈在网络 RTT ~114µs，布局优化在噪声之下）——见 design-notes §8，"知道不做什么"也是取舍。

**面试要点**：能画出一次 GC 周期内 STW 的两个极短窗口（标记开始/结束）与并发标记阶段；能解释为什么"降低分配率"比"调 GOGC"更根本；池化的正确性条件（Reset 干净、无跨使用者状态泄漏）。

## 10. 心跳、超时与死连接检测

**原理**：连接健康分两层——内核协议栈活着（TCP keepalive 可探）与应用进程活着（只有应用层心跳能探：死锁、事件循环卡住、GC 停顿、进程僵死）。TCP keepalive 默认 2 小时不起作用，且探不到应用。应用层心跳的设计参数：间隔 P、判死阈值 T、探测方向。T/P 的权衡：太小误杀（抖动一次就断），太大判死慢（死连接占资源）。常见取 1.5~3×。

**本实现**（transport.go）：写循环 ticker 每 `Heartbeat` 发 PING（无数据帧竞争时）；读循环**每收到任何帧**都重置 `SetReadDeadline(now + 1.5×Heartbeat)`——不单等 PONG，因为任何帧都是活性证据，这把"心跳"与"流量"统一成一个判据。1.5× 的推导：容忍一次 PING 丢失/一次调度抖动，同时 2× 以上判死太慢。双向独立发，间隔经 CONNACK 协商一致（服务端权威）。

写超时硬编码 10s（transport.go:120）：写阻塞的对端通常已死（TCP 缓冲满 + 无 ACK），10s 足够穿过正常 RTT 而不至于让写 goroutine 挂太久。备选：把写超时也做成配置——v1 不做，因为它不是语义参数而是防御参数，暴露出去只会引诱用户调错。

**面试要点**：keepalive vs 应用层心跳的层次差异；为什么"任何帧都算活性"；deadline 是怎么生效的（runtime 在 epoll wait 前计算最近 deadline，超时的 netpoll 触发 read/write 返回超时错误——不是定时器线程打断读）。

## 11. 错误处理与资源释放（Go 惯用法）

**原理**：Go 没有异常，错误是值。工程纪律：错误要分类（决策"续"还是"断"）、错误链要可追（`fmt.Errorf("%w")` 包装 + `errors.Is/As`）、资源释放要幂等（defer + Once）、panic 只用于"不可恢复的程序性错误"且在边界 recover。

**本实现**：

- **哨兵错误 + 包装**：`ErrMalformed`/`ErrClosed` 是哨兵（frame.go:161），`fmt.Errorf("%w: header: %w", ErrMalformed, err)` 双重包装——调用方既能 `errors.Is(err, ErrMalformed)` 判类，又能打日志看根因。
- **分类决策表**：连接级断连（Malformed/协议违规/StreamID=0 ERROR）、流级终结（ERROR/CANCEL/过期）、应用级回错误帧——DESIGN.md §7 的表。**分类决定处置**，这是"错误处理"从字符串比赛变成工程设计的关键一步。
- **幂等释放**：`transport.kill` 的 `deadOnce`、`Client.Close` 的 `closeOnce`、`Subscription.Close` 的 `closeOnce`、`flow.finish` 的锁内幂等——释放路径可重入，使用方 defer 链才安全。
- **边界 recover**：三处 handler 调用点 recover 并转 INTERNAL 错误帧（或 ONEWAY 只记日志）——应用的 panic 不带崩进程，也不悬挂流（runRequest 的 defer 保证 finish/error 帧必达）。
- **ctx 取消传播**：`streamContext` 把流终结变成 ctx 取消；`doRequest` 的 ctx.Done 分支发 CANCEL 再 fail——取消是协议动作（要通知对端停算），不只是本地返回。

**面试要点**：哨兵 vs 自定义类型 vs errors.As 的选择依据；defer 在循环里的坑（本实现 handler goroutine 生命周期短，defer 即释放）；为什么 `Close() error` 的 error 几乎总是忽略（关闭时的错误没有处置手段，日志即可）。

## 12. 边界条件与恶意输入防护

**原理**：协议实现的攻击面 = 每一个从对端字节解释出来的整数/长度/标志。防护原则：**一切长度先验证后分配；一切上限等于字段可表达的真实上界；一切未定义输入有确定处置**。

**本实现的防线清单**：

| 攻击 | 防线 | 位置 |
| --- | --- | --- |
| 巨大长度字段打爆内存 | payloadLen > 16MiB 拒绝，**校验先于 make** | frame.go:191 |
| 解压炸弹（16MiB flate 膨胀千倍） | `readAll` 用 `io.LimitReader(limit+1)` 读满即拒 | config.go:21 |
| 元数据长度 off-by-one | 上限定 `math.MaxUint16`（64KiB-1），恰 64KiB 无法用 uint16 表达 | frame.go:34 |
| 未定义保留位 | Flags 保留位非零按 Malformed 断连 | frame.go:187 |
| 版本不符 | 握手期即拒（快速失败） | frame.go:184 |
| 加密帧打到未启用方 | UNSUPPORTED 断连（协商结果与帧标志不一致=对端违约） | transform.go inbound |
| 未知帧类型 | 立即断连（对端是异版本实现，不回帧） | endpoint.go:219 |
| 保留队列撑爆内存 | 每会话 4MiB 字节记账，溢出失去恢复资格 | server.go sendDown |
| 握手期恶意慢连接 | 握手整体 deadline = DialTimeout | server.go handleConn |
| 畸形错误帧 | errDecode 兜底归一 INTERNAL，不放大为断连 | errors.go |

其中解压炸弹与编码不自洽两条是 **fuzz 实测抓到的真 bug**（README bug 清单 1、2），不是纸面推演。off-by-one 那条是 gosec 抓的：上界必须等于字段可表达的值，不是顺手的整数——`uint16` 能表达的最大值是 65535，不是 65536。

**面试要点**：能主动列出"对端可控的输入有哪些"并逐个给出处置；知道 `io.LimitReader(n+1)` 读满 n+1 即超限的技巧（读满 n 不能区分"恰好 n"与"超过 n"）；fuzz 的价值定位（它测的是"解析-编码互逆"与"任意字节不崩溃"这类人类想不到的输入）。

## 13. 重连、会话恢复与投递语义

**原理**：分布式系统的基础是投递语义三选一：at-most-once（可能丢）、at-least-once（可能重）、exactly-once（要序号+去重+确认，代价最高）。"断线重连恢复"的完整设计要回答：断开期间的状态谁记着（服务端会话）、记多久（保留期）、怎么续（重放）、续不上怎么办（降级路径）、新旧连接打架怎么办（takeover）。

**本实现**：at-least-once——服务端按会话保留订阅关系 + 断开期间下行帧（4MiB/30s 上限），重连 CONNACK{resumed=true} 后重放；客户端指数退避+抖动重连，未终结流迁移到新连接（含 handler——见 bug 9-7：responder 流不迁移会白产帧撑爆保留队列）；恢复失败则挂起流以 SESSION_EXPIRED 失败、订阅自动重订、OnResumeFailed 交还应用。诚实的边界（design-notes §7）：断连瞬间 TCP 在途帧不保证送达，无 per-frame 序号无法判重，非幂等操作要业务 ID 幂等兜底；上行不缓存。

**面试要点**：为什么 at-least-once 而不是 exactly-once（题面不要求 + 序号/ACK 是一整层复杂度）；takeover 的必要性（半开连接复活与新连接打架）；"恢复"与"重试"的区别（恢复是协议状态迁移，重试是应用语义）。

## 14. 压缩与加密的正确顺序（常识考点）

**原理**：先压缩后加密。AES-GCM 输出在计算上不可区分于随机字节，随机字节的熵已达上限，压缩率≈0；先加密后压缩=白付 28B 密码学开销（12B nonce + 16B tag）还保留明文冗余。GCM 本身是 AEAD：加密+认证一体，tag 校验失败即拒收——所以"解密"同时防篡改。nonce 唯一性是 GCM 的安全底线：同一 key 下 nonce 重用会泄露明文异或关系。本实现每帧随机 12B nonce 前置传输（transform.go outbound `Seal(nonce, nonce, ...)`——密文连同 nonce 一起输出，接收方取前 12B 作 nonce）。

**面试要点**：AES-NI 使 GCM 吞吐达 GB/s 级（本机实测 ~5µs/1.4KiB，含池化开销）；PSK 的局限（不解决分发与轮换，前向保密无）；为什么握手帧恒明文（先有协商后有密钥，鸡生蛋）；"帧内自描述的 Flags"让单连接内明文帧与加密帧混合存在是特性不是漏洞。

## 15. Go 语言陷阱清单（每条都对应本仓库的一处代码）

这一节可以单独拿出来复习。每一行的格式是：**陷阱 → 本仓库怎么处理的 → 面试怎么讲**。

### 15.1 语言语义

| 陷阱 | 现象 | 本仓库 |
| --- | --- | --- |
| **`select` 多就绪时随机选** | 公平性设计，但在错误路径上 = 不确定性 | `transport.send` 先对 `dead` 做非阻塞预检（transport.go:78）；消费端在 `doneCh` 分支先排空 `frames`（flow.go:181）。两条都是 CI race/实测逼出来的（NOTES §8） |
| **typed-nil 装进 error 接口** | `var e *Error; return e` 的 `err != nil` 为真，调用方一解引用就 panic | `asStreamError` 用类型断言，nil `*Error` 会让错误帧载荷变成 `null`，对端归一为 INTERNAL——**降级而非崩溃**（DESIGN.md §7 末条记录了这个已知盲区） |
| **整数转换静默截断** | `uint16(65536)` = 0，凭空造出一个"长度为 0"的错帧 | `MaxMetadataSize = math.MaxUint16`（不是 64KiB），编码侧用 `min(len, Max)` 把边界显式化（frame.go:34/153）。gosec 实锤的 off-by-one |
| **切片别名** | `append` 复用底层数组会改到别人持有的切片 | `appendTo(dst)` 只追加不改已有内容；写循环用 `buf[:0]` 复用，**前提是不有人持有 buf 的切片** |
| **`[]byte` 不能做 map 键** | 切片不可比较 | 路由名/主题名一律 `string`：Metadata 解码出来就是 string，且 map 查找需要可比、可作键 |
| **map 迭代顺序随机** | 遍历结果每次都不同 | `Server.Sessions()` 显式 `sort.Strings`（对外可预期）；`Server.Publish` 遍历顺序无关紧要，故不排 |
| **`sync.Once` 不可重入** | 在 `Do` 的函数体内再次调用同一个 `Do` → 自死锁 | `kill`/`Close` 的 `Do` 体内只做"置状态 + 通知"，不回调用户代码（transport.go:185、client.go:107） |
| **结构体零值不一定可用** | 零值 map 写入 panic、零值 mutex 拷贝即错 | `Config` 零值可用（`normalized()` 补默认值）；`flow` 零值**不可用**（`doneCh` 是 nil channel，收永远阻塞），所以只能经 `newFlow` 构造 |
| **nil channel 永久阻塞** | 对 nil channel 的收发永远挂起 | `flow.doneCh`/`ackCh` 在 `newFlow` 里一律 `make`；`creditGate` 的 `notify` 同理 |
| **值接收者 vs 指针接收者的方法集** | 值接收者的方法在指针上也可调用，反之不行 | `discardLogger` 用值接收者（config.go:125），于是 `discardLogger{}` 与 `&discardLogger{}` 都满足 `Logger` |
| **min/max 是 1.21+ 内置** | 旧版本要自己写 | `frame.go:149/153` 直接用 `min`，把"边界不会溢出"写进类型检查 |

### 15.2 并发与内存

| 陷阱 | 现象 | 本仓库 |
| --- | --- | --- |
| **数据竞争 = 无 happens-before，与墙钟无关** | `sleep` 治不好竞态，只有 channel / `go` / 锁能建立边 | `flow.err` 的所有读取收口到 `doneState()`（flow.go:124）：裸读与 `finish` 的写竞争，race detector 实锤 |
| **RWMutex 不可升级** | 持读锁时申请写锁 → 死锁 | 全部按"锁外取快照、锁内只改数据"写：`Server.Publish` 先快照 sessions 再逐个取 `ss.mu`（server.go:79-98） |
| **锁序倒置** | 两把锁交叉获取 → 死锁 | 明确记在 DESIGN.md §6.2：`store.mu → ss.mu` 不嵌套；`ss.mu → ep.streamsMu` 单向；`flow.mu` 锁内不调外部函数 |
| **channel 提供 happens-before** | 收发本身就是同步边 | 全局的测试接缝（`jsonMarshal` 等）能安全替换，前提是"先写桩再 spawn"（design-notes §9 的顺序约束） |
| **`sync.Pool` 两轮 GC 后清空** | 不是缓存，命中率取决于分配压力 | flate 读写器池化（transform.go:23/27）；取出即 `Reset`，保证不跨帧泄漏流状态 |
| **`defer` 在循环里累积** | 长循环里 defer 到函数结束才执行 | handler 是一流一 goroutine，`defer` 随 goroutine 结束而执行（正确用法）；`Server.Publish` 的遍历循环内没有 defer |
| **`time.After` 在循环里** | 老版本每个都起一个 runtime timer，循环里用会堆积 | 库里的 `writeLoop` 用 `NewTicker`+`Stop`（transport.go:111）；`resubscribeLocked` 的 `time.After` 是一次性 select，可接受。Go 1.23+ 起未被引用的 timer 可被 GC 回收（go.mod 声明 1.24，所以示例里的 `time.After` 循环也是安全的） |
| **每流一 goroutine 的代价** | 初始栈 ~8KB，活跃流多了就是内存 | 换来的取消语义干净（`streamContext` 直接包 `doneCh`）。这是"goroutine 数量换语义"的有意识取舍（DESIGN.md §6.1） |
| **goroutine 泄漏的三种形态** | 阻塞在无人关闭的 channel / 阻塞在不返回的读 / 循环体永不退出 | 收尾单点化在 `transport.kill`（关 dead + 关 credit 闸门 + 关 conn）；`Server.handleConn` 用 `defer nc.Close()` 兜住握手失败路径 |

### 15.3 错误处理

| 陷阱 | 现象 | 本仓库 |
| --- | --- | --- |
| **`errors.Is` 与 `==` 的区别** | 包装后 `==` 失效 | 全部用 `%w` 包装 + `errors.Is/As`：`fmt.Errorf("%w: header: %w", ErrMalformed, err)` 双重包装，既能判类又能看根因（frame.go:171） |
| **`%w` 只能包一个** | 多错误要 Go 1.20+ 的 `errors.Join` | 帧层用"哨兵 + 描述"两段式（`%w: <细节>: %w`）代替 Join，可读性更好 |
| **`defer` 里改返回值要命名返回值** | `return nil` 之后 defer 改局部变量没用 | 全仓库没有依赖 defer 改返回值的写法（关闭类的返回值本来就是 nil） |
| **忽略 `Close()` 的错误** | 容易 lint 报警 | 释放接口约定"不返回有意义的错误"（`Client.Close`/`Subscription.Close` 恒 nil），所以要么 `_ =` 要么注释说明为什么丢弃（examples/client 的 `Cancel`/`Close`） |
| **panic 只在栈边界 recover** | 中间层 recover 会吞掉上下文 | recover 只在三处协议边界（`runRequest`/`serveOneWay`/topic handler），且都补发了错误帧或日志，不会让流悬挂 |

**面试怎么讲**：这一节能直接答"你在 Go 上踩过哪些坑"。挑三条讲透最有效——`select` 随机性在错误路径上的后果（配 DESIGN.md §6.4 的排空语义）、typed-nil 装进接口、以及"数据竞争看 happens-before 不看墙钟"（配 race detector 的向量时钟判定）。这三条都不是"知道规则"级别的题，而是要能说出**为什么规则是这样**。
