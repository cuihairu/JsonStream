<p align="center"><img src="assets/logo.svg" width="110" alt="JsonStream"></p>

<h1 align="center">JsonStream</h1>

<p align="center">
  <a href="https://github.com/cuihairu/jsonstream/actions/workflows/ci.yml"><img src="https://github.com/cuihairu/jsonstream/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="https://github.com/cuihairu/jsonstream/actions/workflows/pages.yml"><img src="https://github.com/cuihairu/jsonstream/actions/workflows/pages.yml/badge.svg" alt="pages"></a>
  <a href="https://cuihairu.github.io/jsonstream/"><img src="https://img.shields.io/badge/docs-GitHub%20Pages-blue" alt="docs"></a>
  <a href="https://codecov.io/gh/cuihairu/jsonstream/branch/main"><img src="https://codecov.io/gh/cuihairu/jsonstream/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="https://pkg.go.dev/github.com/cuihairu/jsonstream"><img src="https://pkg.go.dev/badge/github.com/cuihairu/jsonstream.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

## 简介

JsonStream 是基于 TCP 的自定义二进制帧协议，承载 JSON：一条连接跑请求/响应、流式、双工、单向、发布/订阅五种交互，心跳、断线恢复、压缩加密、credit 背压都在协议内，不靠应用层自造。Go 参考实现纯标准库、零第三方依赖（Go ≥ 1.24，[v0.1.0 发布说明](https://github.com/cuihairu/jsonstream/releases/tag/v0.1.0)）；协议规范语言无关（[docs/protocol.md](docs/protocol.md)，跨语言实现的单一事实源），其他语言实现规划中。

在线文档站随 main 自动发布：<https://cuihairu.github.io/jsonstream/>，最近改动的逐条摘要在[更新日志](https://cuihairu.github.io/jsonstream/changelog)。

## 30 秒上手

零第三方依赖：

```bash
go get github.com/cuihairu/jsonstream
```

一个路由加一次调用：

```go
ln, _ := net.Listen("tcp", "127.0.0.1:9000")
s, _ := jsonstream.NewServer(ln, jsonstream.DefaultConfig())
s.Handle("math.add", func(r *jsonstream.Request) (any, error) {
    var in map[string]int
    if err := r.Decode(&in); err != nil {
        return nil, err
    }
    return map[string]int{"sum": in["a"] + in["b"]}, nil
})
go s.Serve()

c, _ := jsonstream.Dial(context.Background(), "127.0.0.1:9000", jsonstream.DefaultConfig())
m, _ := c.Request(context.Background(), "math.add", map[string]int{"a": 2, "b": 40})
var out map[string]int
_ = m.Decode(&out) // {"sum": 42}
```

## 核心特性

- 一条 TCP 连接跑全部五种交互：请求/响应、流式、双工、单向、发布/订阅；数据帧都带 Stream ID（控制帧除外），多路复用无需协商
- 心跳在协议内：双向独立 PING/PONG，读空闲超 1.5× 间隔判死——TCP keepalive 探不出进程死锁，所以不用它
- 断线自动重连（指数退避 + 抖动）；服务端在保留期内重放订阅与下行帧（at-least-once），同会话新连接 takeover 顶替旧连接
- 压缩（flate）、加密（AES-256-GCM + 32B 预共享密钥）、背压（credit）全部可选：握手协商生效、每帧 Flags 自描述、关闭零运行时代价
- Metadata 与 Payload 分离：路由/主题恒为明文，网关不解码业务数据即可鉴权、限流、转发
- 14B 定长头 + 32 位大端长度前缀，单帧载荷上限 16 MiB，解码端先校验长度后分配内存
- 质量基线：库包语句覆盖 100%（CI 门禁）、五个 fuzz 靶持续冒烟、八件静态检查零告警、五平台交叉编译

## 快速示例

流式、双工、订阅、单向的逐方法写法见 [docs/api.md](docs/api.md)，五种交互模式的完整走查见 [docs/getting-started.md](docs/getting-started.md)。压缩/加密/背压都是参数化开关，握手协商后生效（双方都开才启用），默认全关、零开销：

```go
cfg := jsonstream.DefaultConfig()
cfg.Compress = true // ≥64B 载荷走 flate
cfg.Encrypt = true  // AES-256-GCM，先压后加
cfg.Key = key32     // 32 字节预共享密钥
cfg.Credit = 64     // 连接级信用窗口，生效值取双方最小
```

发起类 API 一律以 `ctx` 打头：`SendOneWay`/`Publish`（Client 与 Server 两侧）与 `Request`/`Stream`/`Channel` 同规，ctx 约束「等可用连接」与「等发送入队」两段等待（DESIGN §8.4）。该签名在首发（v0.1.0）前定版，不为旧签名留别名——旧行为等价于传 `context.Background()`。

可运行的端到端程序在 [examples/](examples/)：`examples/server` 与 `examples/client` 覆盖请求/响应、流式取消、双工、单向、发布/订阅、服务端主动发起六类场景。

## 文档站导航

文档站全部页面按你想做的事分流：

### 使用者：在 Go 应用里直接用库

- 上手：[环境与安装](https://cuihairu.github.io/jsonstream/getting-started#环境与安装)、[第一个请求/响应](https://cuihairu.github.io/jsonstream/getting-started#第一个请求-响应)、[五种交互模式](https://cuihairu.github.io/jsonstream/getting-started#五种交互模式)、[参数化开关](https://cuihairu.github.io/jsonstream/getting-started#参数化开关)
- 参考：[Client 与 Server 的方法](https://cuihairu.github.io/jsonstream/api#client)、[Config 全字段](https://cuihairu.github.io/jsonstream/api#config)、[错误码](https://cuihairu.github.io/jsonstream/api#错误)、[术语速查](https://cuihairu.github.io/jsonstream/glossary)
- 排障与性能：[FAQ](https://cuihairu.github.io/jsonstream/faq)、[基准实测](https://cuihairu.github.io/jsonstream/benchmarks)（[开销逐项解读](https://cuihairu.github.io/jsonstream/benchmarks#逐项解读-开销主要在哪)）

### SDK 作者：网关、代理与上层封装

- [帧层（低层）](https://cuihairu.github.io/jsonstream/api#帧层-低层)：绕过交互语义直接收发帧
- [Metadata 与 Payload 分离](https://cuihairu.github.io/jsonstream/protocol#_5-metadata-与-payload)：路由/主题对中间件可读，不解密 payload 即可路由
- [握手协商](https://cuihairu.github.io/jsonstream/protocol#_6-1-协商)与[背压（credit）](https://cuihairu.github.io/jsonstream/protocol#_7-9-背压-可选-credit-based)：网关透传与限流要处理的语义
- [并发模型速览](https://cuihairu.github.io/jsonstream/api#并发模型速览)：哪些方法可多 goroutine 并发调用

### 协议实现者：在其他语言复刻本协议

- [协议规范 v1](https://cuihairu.github.io/jsonstream/protocol) 是单一事实源，只拿这一份就应能写出可互通的实现：[帧布局](https://cuihairu.github.io/jsonstream/protocol#_3-1-布局-大端序)、[帧类型](https://cuihairu.github.io/jsonstream/protocol#_4-帧类型)、[握手](https://cuihairu.github.io/jsonstream/protocol#_7-1-握手)、[断线重连恢复](https://cuihairu.github.io/jsonstream/protocol#_8-断线重连恢复)、[分片传输](https://cuihairu.github.io/jsonstream/protocol#_3-4-分片传输-大消息)
- Go 参考实现的取舍与边界：[架构与取舍](https://cuihairu.github.io/jsonstream/DESIGN)（[边界情况清单](https://cuihairu.github.io/jsonstream/DESIGN#_8-边界情况清单)、[设计决策清单](https://cuihairu.github.io/jsonstream/DESIGN#_10-设计决策清单-备选方案与放弃理由)）、[设计笔记](https://cuihairu.github.io/jsonstream/design-notes)、[知识点梳理](https://cuihairu.github.io/jsonstream/NOTES)
- 横向对照与题面：[与 WebSocket 对照](https://cuihairu.github.io/jsonstream/websocket-comparison)、[TCP 流特性与协议横评](https://cuihairu.github.io/jsonstream/tcp-and-landscape)、[题目要求](https://cuihairu.github.io/jsonstream/interview-requirements)

## 性能基准摘要

下列数字摘自 [bench_test.go](bench_test.go) 九个基准中的七靶（`go test -run '^$' -bench . -benchmem -count=10 -benchtime=1s`，i9-10880H / Go 1.27.1，2026-10-01 轻载窗口，量级参考；压+加与加密端到端两靶未列，全量 9 靶见下文）：

| 基准（bench_test.go） | 结果 |
|---|---|
| `BenchmarkFrameRoundTrip64B` / `1KiB` / `64KiB` | ~0.6µs / ~1.8µs / ~33µs |
| `BenchmarkTransformPlain` / `Encrypt` / `Compress`（~1.3KiB JSON） | ~14ns / ~7.7µs / ~27µs |
| `BenchmarkRequestResponsePlain`（本机回环 RTT） | ~0.12ms |

跨机器只比相对关系不比绝对值：本表与 [docs/benchmarks.md](docs/benchmarks.md) 的 9 靶 × 10 轮 benchstat 聚合版同源（同一次 2026-10-01 实跑，两处口径各自注记），该页另保留 2026-09-28 高负载窗口的同套数字作对照（同靶相差 3.5~8.5× 属实测范围）；共享容器里 ns/op 浮动明显，可复现的是 allocs/op 与 B/op 这类结构性性质。压缩路径按帧复用 flate 编解码器（`sync.Pool` + `Reset`），池化后压缩往返 4.1 倍提速、分配降 162 倍（DESIGN §10-D11）。

质量与验证口径：库包语句覆盖 100.0%（CI 门禁跌破即失败；语句计数随 Go 工具链版本不同——1.24 与 stable 两腿实测均为 100.0%，故只记百分比不记分母，两个示例包同为 100.0%）；Codecov 徽章显示 100%（`codecv.yml` 行覆盖目标 99%，当前 ≈99.85% 四舍五入），不红 CI（`fail_ci_if_error: false`，与语句 100% 门禁解耦）；212 个测试/基准/fuzz 函数；`-race -count=1` 全绿（CI 门禁同款）加 goroutine 泄漏守卫；五条 fuzz 靶 CI 冒烟（长跑累计千万级 execs，实锤修复「测试里抓到的真 bug」一节的第 1、2 条）；八件静态检查（vet/gofmt/gosec/revive/staticcheck/gocritic/nilness/govulncheck）与五平台交叉编译全部落成 CI 门禁。方法学与逐项数据见 [docs/DESIGN.md](docs/DESIGN.md) §11，本地复现命令见下文「贡献指引」。

## 贡献指引

issue 与 PR 都欢迎；合入前请让本地验证与 CI 同绿（本仓库无 CONTRIBUTING.md，本节即贡献约定）。

### 本地验证

```bash
git clone https://github.com/cuihairu/jsonstream.git
cd jsonstream

# 构建 + 全量测试 + 静态检查
go build ./...
go test ./...
go vet ./...

# 覆盖率：库包 100.0%（CI 门禁），examples/server 100.0%，examples/client 100.0%
go test -cover ./...

# 性能基准
go test -bench . -benchtime 2s

# fuzz 冒烟（CI 同款：逐包自动发现全部靶，每靶 20s）
for pkg in $(go list ./...); do
  for t in $(go test -list 'Fuzz.*' "$pkg" | grep '^Fuzz'); do
    go test -run '^$' -fuzz "^${t}$" -fuzztime 20s "$pkg"
  done
done

# fuzz 深挖单靶（注意 -fuzz 只接受单个包，./... 会报错）
go test -run '^$' -fuzz FuzzReadFrame -fuzztime 300s .

# 端到端示例：终端 1 启动服务端
go run ./examples/server
# 终端 2 运行客户端（请求/响应、流式取消、双工、单向、发布/订阅、服务端主动发起）
go run ./examples/client
```

### 文档站开发

```bash
pnpm install      # 首次；Node 22 / pnpm 12
pnpm dev          # 本地开发，http://localhost:5173
pnpm build        # 构建到 docs/.vitepress/dist，自带死链检查
pnpm check:links  # 内链大小写与跨页锚点、外链真实 HEAD 探测
```

### CI 与发布

推送触发两个 workflow：[ci](https://github.com/cuihairu/jsonstream/actions/workflows/ci.yml)（构建 + 全量测试 + fuzz 冒烟 + 覆盖率门禁）与 [pages](https://github.com/cuihairu/jsonstream/actions/workflows/pages.yml)（文档站构建与发布）。改文档的 PR 绿了不代表线上已更新——pages 的 deploy 只在合入 main 后执行，连续推送还会在部署队列里串行排队；三个易踩坑的细节见 [docs/faq.md](docs/faq.md)。门禁口径（覆盖率 100%、fuzz、静态检查）的逐项含义见上文「性能基准摘要」末段与 [docs/DESIGN.md](docs/DESIGN.md) §11。

## 协议与帧格式

帧格式规范（逐字段、逐帧型）以 [docs/protocol.md](docs/protocol.md) 为准，本节是导读。

```
 0               8               16              24              32
 +---------------+---------------+---------------+---------------+
 |     Magic "JS" (0x4A53)       |    Version=1  |     Flags     |
 +---------------+---------------+---------------+---------------+
 |     Type      |   Reserved    |          Stream ID            |
 +---------------+---------------+---------------+---------------+
 |                        Payload Length                          |
 +---------------+---------------+---------------+---------------+
 |   Meta Length (2B, Flags.HasMeta 时) | Metadata (JSON, 路由/主题) |
 +---------------------------------------------------------------+
 |                     Payload (JSON)                             |
 +---------------------------------------------------------------+
```

TCP 是字节流没有消息边界，分帧手段无非三种：定长、分隔符、长度前缀。JSON 里有分隔符歧义，所以选 14B 定长头 + 32 位大端长度前缀：解码端「读完头 → 两次 `ReadFull`」是无条件操作，没有跨帧状态机，可以单测、可以 fuzz、可以在任意位置丢帧重入。Magic 给误连/端口探测一个立即判废的机会，Version 给握手期快速失败的依据；Flags 保留位必须为 0，见到非零按 Malformed 断开，给未来升级留门。

对 WebSocket（RFC 6455 §5.2）帧格式做了两处减法、一处换形（逐点依据见 [docs/design-notes.md](docs/design-notes.md) §1）：

- 分片换形，不做 FIN+continuation：16 MiB 单帧上限管住单帧内存；更大的逻辑消息拆成连续 chunk——开帧保留原类型并置 `FlagFragmented`，续段/收尾用独立 `FRAGMENT` 帧型，运行不可穿插故接收端免块序号，重组总量以 `MaxMessageSize`（默认 64 MiB）为硬帽（[protocol §3.4](docs/protocol.md)；规范已定稿，Go 参考实现落地中）。
- 不做客户端掩码：MASK 防的是浏览器时代的代理缓存投毒，专用客户端/服务端直连没有这个威胁模型。
- 不做 7/16/64 位变长长度：变长编码多数帧省 2~6 字节，换来解码端的分支状态机；固定 4B 长度的上限同时是内存闸门（先校验后分配，OOM 开关不在对端手里）。

### 交互模型

四种交互原语照 RSocket 的划分：`REQUEST→RESPONSE`（一问一答）、`REQUEST+Flags.Stream→N×RESPONSE+COMPLETE`（流式）、`ONEWAY`（单向，连错误都不回）、`REQUEST+Flags.Channel`（双向多帧）。每帧必带 Stream ID（控制帧除外），按发起方分奇偶（客户端奇数、服务端偶数，HTTP/2 同思路），不用协商就知道帧的归属，两端计数器跨重连单调递增。

两个值得知道的细节：请求/响应不追加 COMPLETE 帧——响应帧自带终结语义，一次交互少一个往返；CANCEL 是流的一部分——背压管「生产快于消费」，CANCEL 管「消费者根本不要了」。与 RSocket 的分歧在一个点：它把三种请求拆成三个帧类型，这里用一个 `REQUEST` 加 Flags.Stream/Flags.Channel 位表达，解析分支更克制；代价是中间件只看帧头无法预判交互模式，v1 以「Flags 声明与 handler 不一致回 ERROR(PROTOCOL)」兜底（[design-notes §2](docs/design-notes.md)）。

### 其余机制速览

- Metadata 与 Payload 分离：`route`/`topic` 在帧头 Metadata 段、恒为明文，网关不解码 payload 即可鉴权、限流、路由；代价是路由可被链路旁路统计（[design-notes §4](docs/design-notes.md)）。
- 心跳：应用层 PING/PONG 双向独立发送，读空闲超 1.5× 间隔判死；1.5× 是容忍一次丢帧抖动与判死速度的折中（[protocol §7.2](docs/protocol.md)）。
- 断线恢复：CONNECT 携带 `session_id`，服务端在保留期内（默认 30s、每会话 4 MiB 上限）重放订阅与下行帧（at-least-once），同会话新连接 takeover 顶替旧连接；边界写进规范——TCP 在途帧不保证重放（非幂等操作靠业务 ID 兜底）、上行不缓存（[protocol §8](docs/protocol.md)）。
- 压缩与加密：每帧 Flags 自描述是否压缩/加密，心跳帧恒明文、大帧才压缩，单连接内混合存在；顺序冻结为先压后加（密文不可压）；AES-256-GCM + 32B PSK，PSK 不解决密钥分发，生产上外层套 TLS（[protocol §6](docs/protocol.md)、[design-notes §3](docs/design-notes.md)）。
- 背压：credit 按条数授权、交付后归还，默认关闭——额度归零时 `Emit()` 阻塞，是全协议唯一反向影响应用并发模型的机制；请求/响应与 ONEWAY 不纳入 credit（[design-notes §5](docs/design-notes.md)）。
- pub/sub 与 req/res 同连接混用：同一 PUBLISH 帧类型两个方向语义对偶；订阅建立失败回 ERROR、投递失败不回帧的不对称是刻意的，已写进规范（[design-notes §6](docs/design-notes.md)）。

### 测试里抓到的真 bug：不这么设计的后果实证

实测后自己踩到又修掉的 9 个真缺陷——每一个都是一条设计教训：

1. **解压炸弹**（fuzz 抓到）：解压结果不设上限，单帧 16 MiB 的 flate 数据能膨胀三个数量级——不防等于把 OOM 开关交给对端。
2. **编码不自洽**（fuzz 抓到）：Flags 声明带 Metadata 但内容为空时跳过 metaLen 段，解析-编码不再是互逆——序列化必须满足 round-trip 恒等。
3. **credit 破坏 at-least-once**：断连后取额度立即失败，handler 误判退出、保留队列变空，重放丢了——连接级流控泄漏进了会话级恢复语义，修复为失败改道保留队列。
4. **死连接上的 select 双就绪**：发送通道有空位时 `select` 随机选中已死分支，帧静默丢失——Go 的 select 随机性在错误路径上是真陷阱。
5. **Stream ID 撞号**：重连后 ID 计数器归零，新流与迁移流同 ID，旧流迟到的 COMPLETE 误杀新流——多路复用加恢复，ID 必须跨重连单调。
6. **陈旧连接引用**：订阅句柄持有创建时的 endpoint，重连后退订帧发给死连接被静默吞——出站一律取当前 endpoint，让编译器消灭陈旧引用。
7. **responder 流不迁移**：服务端被动流的 handler 在重连后悬空白产帧，能把保留队列撑爆——会话迁移必须连 handler 一起搬。
8. **断连后悬挂**：服务端主动发起的交互在连接死亡后永久挂起等待一个必然不会来的响应——响应是上行、不缓存，等待无意义，断连即败。
9. **元数据上限 off-by-one**（gosec 抓到）：上限写成 64 KiB 整，但长度字段是 uint16——恰 64 KiB 会静默截断成 0 编出错乱帧。上界必须等于字段可表达的值，不是顺手的整数。

## 配置参考

`Config` 是值类型：传入 `Dial`/`NewServer` 之后再修改原变量不影响已建立的端。生效值以 CONNACK 下发的为准（服务端是权威）；`DefaultConfig()` 返回推荐的完整默认值。逐字段 API 细节见 [docs/api.md](docs/api.md)。

| 字段 | 类型 | 默认 | 语义 |
| --- | --- | --- | --- |
| `Heartbeat` | `time.Duration` | `DefaultHeartbeat`（10s） | PING 间隔；读空闲超 1.5× 判死。零值取默认，低于 1s 钳到 1s |
| `Compress` | `bool` | `false` | flate 压缩（载荷 ≥64B 才实际压缩） |
| `Encrypt` | `bool` | `false` | AES-256-GCM；启用时 `Key` 必须是 32 字节 |
| `Key` | `[]byte` | `nil` | 32 字节预共享密钥，不上线传输 |
| `Credit` | `int` | `0`（关闭） | 连接级信用窗口（条数）；生效值 = min(双方配置)，任一方 ≤0 关闭 |
| `Auth` | `string` | `""` | 透传到 CONNECT 的令牌，服务端在 `OnAuth` 中校验 |
| `Retention` | `time.Duration` | `DefaultRetention`（30s） | 服务端会话保留期；负值禁用会话恢复 |
| `RetentionBytes` | `int` | `DefaultRetentionBytes`（4 MiB） | 每会话下行保留队列字节上限，超限失去恢复资格 |
| `DialTimeout` | `time.Duration` | `DefaultDialTimeout`（5s） | 建立 TCP 连接的超时 |
| `Reconnect` | `*bool` | `nil`（= true） | false 时客户端不自动重连 |
| `BackoffInitial` | `time.Duration` | `DefaultBackoffInitial`（100ms） | 重连退避起点（指数 + 抖动） |
| `BackoffMax` | `time.Duration` | `DefaultBackoffMax`（5s） | 重连退避上限 |
| `Logger` | `Logger` | `nil`（静默） | 适配 `*log.Logger` 等常见实现 |

```go
type Logger interface{ Printf(format string, v ...any) }
```

## 生态位对比

一句话定位：后端服务间通信拿 WebSocket 的分帧、RSocket 的交互模型、MQTT 的会话语义，各取一截。与 WebSocket（RFC 6455）的能力边界：

- WS 有、本协议没有或显式不做：浏览器原生可达、TLS 一等承载与 443 复用、子协议/扩展协商、文本/二进制 opcode 区分（客户端 Masking 明确不需要，raw TCP 直连没有那个威胁模型）。
- WS 标准没有、本协议内建：请求/响应、发布/订阅、流式、credit 背压、断线恢复、Stream ID 多路复用——这些在 WS 应用里都要自造。
- 逐条依据（协议章节与代码位置）见 [docs/websocket-comparison.md](docs/websocket-comparison.md)；更宽的协议横评（MQTT / RSocket / HTTP/1.1→HTTP/2→HTTP/3 与汇总对比表）见 [docs/tcp-and-landscape.md](docs/tcp-and-landscape.md)。

流控选型的对照（为什么是 credit 而不是滑动窗口或租约）：

| 模型 | 代表 | 语义 | 复杂度 |
| --- | --- | --- | --- |
| 滑动窗口 | TCP / HTTP/2 | 按字节授权 | 高（字节级记账） |
| credit 按条数 | 本协议、Reactive Streams `request(n)` | 授权 N 条，交付后归还 | 中 |
| 租约按时间 | RSocket `LEASE` | 约束速率不约束在途量 | 低 |

滑动窗口按字节记账对 JSON 消息过度工程；LEASE 防的是滥用不是背压（RSocket 自己也靠订阅方 request(n) 做真流控）；按条计数恰好是业务方心智单位。

## 仓库布局与多语言规划

本仓库规划未来实现其他语言版本（Python/Rust/Java 等），布局按此定位：

- **Go 实现保持仓库根**（`go.mod` 在根目录，Go 生态标准布局）。不会把 Go 代码挪进子目录——module path 变更会破坏所有现有 import。
- **未来其他语言实现以平级子目录进入**（`python/`、`rust/`…），互不干扰。
- **协调中枢是语言无关的协议规范**：[docs/protocol.md](docs/protocol.md) 是所有实现的单一事实源，任何语言的实现互通性以它为准；实现细节文档（DESIGN/NOTES 等）描述的是 Go 参考实现，不构成跨语言契约。
