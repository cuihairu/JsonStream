# JsonStream

一道面试题

设计一个自定义的协议：

## 传输可靠性与容错性

- 基于TCP  
- 支持 断线重连恢复
- 支持 心跳机制

## 便捷性

- 以Json为交互数据

## 高性能

- 二进制帧
- 支持 参数化是否启用压缩

## 安全性

- 支持 可以选是否启用加密

## 灵活性

- 支持 请求/响应模式
- 支持 发布/订阅模式
- 支持 流式传输
- 支持 双工
- 支持 单向发送，无需返回
- 支持 返回错误
- 支持 可选背压

## 工程要求

- 对应的测试用例
- 性能测试
- 完整的使用例子
- 相关说明文档

# 设计

设计参考了Websocket和Rsocket

## 协议帧

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

- 定长 14B 头 + 大端序，长度前缀分帧（对比 WebSocket 的 2/8 字节变长长度：不做分片，一条流的多帧语义由 Stream ID 与帧类型承担）。
- `Type` 区分 CONNECT/CONNACK/PING/PONG/REQUEST/RESPONSE/COMPLETE/CANCEL/ONEWAY/ERROR/SUBSCRIBE/SUBACK/UNSUBSCRIBE/PUBLISH/CREDIT。
- `Flags` 位：压缩 / 加密 / HasMeta / 流式 / 双工；保留位必须为 0。
- 单帧上限：Payload 16MiB、Metadata 64KiB。

## 心跳

双方各自按协商间隔发送 PING（对端回 PONG）；读空闲超过 **1.5×间隔** 判定对端失联并断开。

## 断线重连恢复

- 客户端自动重连（指数退避+抖动），CONNECT 携带 `session_id`；服务端在保留期内（默认 30s）保留订阅关系与断开期间产生的下行帧，重连时以 `CONNACK{resumed=true}` 确认并重放（at-least-once）。
- 同会话的新连接顶替旧连接（takeover）；保留超期/禁用/队列超限则 `resumed=false`，挂起流以 `SESSION_EXPIRED` 失败、订阅自动重订、`OnResumeFailed` 回调。

## 压缩与加密（参数化）

- 握手协商生效（双方都开启才启用）：Payload ≥64B 走 flate；启用加密时 AES-256-GCM（32B PSK），**先压缩后加密**。握手帧恒为明文。

## 背压（可选）

连接级信用窗口（默认关闭；开启时取双方协商的最小值）：发送方发数据帧前取令牌，额度耗尽阻塞；接收方交付应用后归还，攒到半窗批量回授 `CREDIT` 帧。

# 快速开始

```bash
# 运行测试与基准（无第三方依赖，Go ≥ 1.24；语句覆盖率 100%，
# go test -cover 实测，测试策略见 docs/design-notes.md §9）
go test ./...
go vet ./...
go test -cover ./...
go test -bench . -benchtime 2s

# fuzz 信任边界（帧解析 / 变换层 / 握手状态机），已实锤并修复过 2 个真 bug
# 注意 -fuzz 只接受单个包，./... 会报错
go test -run '^$' -fuzz FuzzReadFrame -fuzztime 30s .

# 示例：终端 1 启动服务端
go run ./examples/server

# 终端 2 运行客户端（请求/响应 + 流式取消 + 单向 + 发布/订阅 + 服务端主动发起）
go run ./examples/client
```

客户端 API 一览：

```go
c, _ := jsonstream.Dial(ctx, addr, jsonstream.DefaultConfig())
m, _ := c.Request(ctx, "math.add", map[string]int{"a": 2, "b": 40}) // 请求/响应
s, _ := c.Stream(ctx, "range", map[string]int{"n": 100})           // 流式
defer s.Cancel()
ch, _ := c.Channel(ctx, "chat", nil)                               // 双工
_ = ch.Send(v); msg, _ := ch.Receive(ctx); _ = ch.Close()          //   Close=半关闭
_ = c.SendOneWay("notify", v)                                      // 单向
sub, _ := c.Subscribe(ctx, "ticks", func(m *jsonstream.Message) error { ... })
_ = c.Publish("metrics", v)                                        // client → server
```

服务端（`Server.Handle/HandleStream/HandleChannel/HandleOneWay/HandlePublish` + `Serve`）见 `examples/server/main.go`；服务端也可主动向指定会话发起交互（`Server.Sessions/Request/Stream/Channel/SendOneWay`，客户端以 `Handle*` 应答，Stream ID 按偶数分配）。

# 性能

`go test -bench . -benchtime 2s -count=3`，i9-10880H / Go 1.24（量级参考，绝对值受机器影响）：

| 基准 | 结果 |
|---|---|
| 帧编解码往返 64B / 1KiB / 64KiB | ~5.5µs / ~9.6µs（~110MB/s）/ ~170µs（~400MB/s） |
| 变换管线（1.4KiB JSON，含往返） | 明文 ~20ns；AES-GCM ~25µs；flate ~370µs；flate+GCM ~440µs |
| 请求/响应 RTT（本机回环） | ~0.2–0.4ms |

压缩路径按帧复用 flate 编解码器（`sync.Pool` + `Reset`）：按帧新建 writer 的实测代价是 ~1.3ms / ~800KB 垃圾每帧，池化后压缩往返 4.1 倍提速、分配降 162 倍。加密吞吐偏低是本机无 AES-NI 所致，支持 AES-NI 的硬件上 GCM 接近线速。

# 文档

- [docs/protocol.md](docs/protocol.md) — 协议规范 v1：帧格式、握手、心跳、流状态机、各交互模式的帧语义、会话恢复、错误码、扩展点
- [docs/design-notes.md](docs/design-notes.md) — 设计权衡：为什么这么设计、WebSocket/RSocket 惯例对比、优缺点（帧头开销、压缩加密取舍、背压复杂度、pub/sub 与 req/res 混用的边界）

