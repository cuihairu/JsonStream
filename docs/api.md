# API 参考

Go 参考实现的全部公开 API，签名逐一对照源码（module `github.com/cuihairu/jsonstream`，Go ≥ 1.24）。本页是人类可读的导览，规范以 [pkg.go.dev](https://pkg.go.dev/github.com/cuihairu/jsonstream) 为准；线上字节的语义见[协议规范](/protocol)。

## 入口

```go
func Dial(ctx context.Context, addr string, cfg Config) (*Client, error)
func NewServer(ln net.Listener, cfg Config) (*Server, error)
```

- `Dial` 建立连接并完成握手（阻塞到 CONNACK）。**首连失败（网络不可达或握手被拒）立即返回错误**；连接成功后的断连才由内部循环自动重连（指数退避 + 抖动，受 `Config.Reconnect` 控制）。
- `NewServer` 创建服务端但不开始接受连接，调用 `Serve` 启动。

## Client

`Client` 是客户端：自动重连、会话恢复（resumed 时保留挂起流等待重放）、断连期间发起调用挂起而非报错。公开方法可被多个 goroutine 并发调用。

### 发起交互

```go
func (c *Client) Request(ctx context.Context, route string, payload any) (*Message, error)
func (c *Client) Stream(ctx context.Context, route string, payload any) (*ReadStream, error)
func (c *Client) Channel(ctx context.Context, route string, payload any) (*Channel, error)
func (c *Client) SendOneWay(ctx context.Context, route string, payload any) error
func (c *Client) Publish(ctx context.Context, topic string, payload any) error
func (c *Client) Subscribe(ctx context.Context, topic string, h func(*Message) error) (*Subscription, error)
```

`ctx` 约束两段等待：「等可用连接」（断连重连期间调用挂起）与「等发送入队」（对端停滞时队列满）。

### 作为响应端注册（客户端也可当服务端的对等面）

```go
func (c *Client) Handle(route string, h func(*Request) (any, error))
func (c *Client) HandleStream(route string, h func(*Request, Emitter) error)
func (c *Client) HandleChannel(route string, h func(*Channel) error)
func (c *Client) HandleOneWay(route string, h func(*Message) error)
```

### 钩子与状态

```go
func (c *Client) OnReconnect(fn func())
func (c *Client) OnResumeFailed(fn func(error))
func (c *Client) SessionID() string
func (c *Client) Close() error
```

- `OnReconnect`：每次重连成功后回调（可用于重建业务状态）。
- `OnResumeFailed`：会话恢复失败（保留超期 / 保留队列溢出）时回调；此时挂起流已以 `SESSION_EXPIRED` 失败、订阅会自动重订。
- `Close` 幂等，终结全部挂起流。

## Server

```go
func (s *Server) Handle(route string, h func(*Request) (any, error))
func (s *Server) HandleStream(route string, h func(*Request, Emitter) error)
func (s *Server) HandleChannel(route string, h func(*Channel) error)
func (s *Server) HandleOneWay(route string, h func(*Message) error)
func (s *Server) HandlePublish(topic string, h func(*Message) error)
func (s *Server) OnAuth(h func(*ConnectJSON) error)
```

- 路由注册可并发调用，运行中注册对新请求立即生效（建议仍在 `Serve` 前完成）。
- `OnAuth` 在握手期校验 CONNECT 的 `Auth` 令牌，返回非 nil 错误即拒绝连接（错误码原样回给对端）。

### 发布与主动发起

```go
func (s *Server) Publish(ctx context.Context, topic string, payload any) error
func (s *Server) Sessions() []string
func (s *Server) Request(ctx context.Context, sessionID, route string, payload any) (*Message, error)
func (s *Server) Stream(ctx context.Context, sessionID, route string, payload any) (*ReadStream, error)
func (s *Server) Channel(ctx context.Context, sessionID, route string, payload any) (*Channel, error)
func (s *Server) SendOneWay(ctx context.Context, sessionID, route string, payload any) error
```

服务端发起类方法以 `sessionID` 定位目标会话（`Sessions` 列出当前全部会话）；会话不存在立即失败。

### 生命周期

```go
func (s *Server) Serve() error
func (s *Server) Close() error
```

`Serve` 阻塞接受连接直到 `Close`；`Close` 幂等，终结全部会话与挂起流。

## 数据类型

### Message

```go
type Message struct {
    StreamID uint32
    Route    string          // REQUEST/ONEWAY 的路由名（来自 Metadata）
    Topic    string          // PUBLISH/SUBSCRIBE 的主题名（来自 Metadata）
    Payload  json.RawMessage
}

func (m *Message) Decode(v any) error // 空 payload 什么都不做
```

### Request

```go
type Request struct { *Message /* 内嵌 */ }
func (r *Request) Context() context.Context // 所在流被取消/终结后 Done
```

### Emitter

```go
type Emitter interface{ Emit(v any) error }
```

流式 handler 的下发端：逐帧发送（受背压约束，额度耗尽时阻塞），handler 正常返回时自动 COMPLETE。仅在 handler 的 goroutine 内使用。

### ReadStream

```go
func (s *ReadStream) Next(ctx context.Context) (*Message, bool) // ok=false 表示流终结
func (s *ReadStream) Err() error                                // 非正常终结时的错误
func (s *ReadStream) Cancel() error                             // 幂等；终结后为空操作
```

`Next` 是单消费接口：多 goroutine 并发调用会互相抢帧。

### Channel

```go
func (c *Channel) Send(v any) error                        // 受背压约束；可并发调用
func (c *Channel) Receive(ctx context.Context) (*Message, error) // 对端 COMPLETE 后返回 io.EOF；单消费
func (c *Channel) Close() error                            // 优雅关闭（对端收到 COMPLETE）
func (c *Channel) Cancel() error                           // 立即终结整条流
```

### Subscription

```go
func (s *Subscription) Close() error // 退订
```

主题回调在内部独占 goroutine 中逐帧串行执行，回调内可安全调用 Client 的其他方法；回调返回的错误只记日志，不终止订阅。

## Config

`Config` 是值类型：传入 `Dial`/`NewServer` 之后再修改原变量不影响已建立的端。生效值以 CONNACK 下发的为准（服务端是权威）；`DefaultConfig()` 返回推荐的完整默认值。

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

## 帧层（低层）

多数应用不需要直接接触帧层；自研其他语言实现或做网关时才有用。

```go
const (
    ProtocolVersion uint8 = 1
    MaxPayloadSize  = 16 << 20 // 单帧载荷（变换后）上限：16 MiB
    MaxMetadataSize = math.MaxUint16 // Metadata 上限：64 KiB - 1
)

type FrameType uint8 // 0x01–0x0F，见下表
type Header struct {
    Version  uint8
    Flags    uint8
    Type     FrameType
    StreamID uint32
}
type Frame struct {
    Header /* 内嵌 */
    Metadata []byte // 恒为明文 JSON，可为 nil
    Payload  []byte // 协议栈边界内是明文 JSON
}

func ReadFrame(r io.Reader) (*Frame, error) // 读到的是变换后字节，由调用方做逆向变换
```

| Type | 值 | Type | 值 |
| --- | --- | --- | --- |
| CONNECT | 0x01 | ERROR | 0x0A |
| CONNACK | 0x02 | SUBSCRIBE | 0x0B |
| PING | 0x03 | SUBACK | 0x0C |
| PONG | 0x04 | UNSUBSCRIBE | 0x0D |
| REQUEST | 0x05 | PUBLISH | 0x0E |
| RESPONSE | 0x06 | CREDIT | 0x0F |
| COMPLETE | 0x07 | | |
| CANCEL | 0x08 | | |
| ONEWAY | 0x09 | | |

| Flag | 值 | 语义 |
| --- | --- | --- |
| `FlagCompressed` | `1 << 0` | payload 经 flate |
| `FlagEncrypted` | `1 << 1` | payload 经 AES-256-GCM |
| `FlagHasMeta` | `1 << 2` | 携带 Metadata 段 |
| `FlagStream` | `1 << 3` | 流式请求 |
| `FlagChannel` | `1 << 4` | 双工通道请求 |

未知帧类型一律立即断开；Flags 保留位（第 5–7 位）必须为 0，解码端见到非零即按 Malformed 断开。

## 错误

```go
type Error struct {
    Code    ErrorCode `json:"code"`
    Message string    `json:"message,omitempty"`
}
func (e *Error) Error() string
func (e *Error) IsProtocol() bool // Code == PROTOCOL：握手与帧层违规随后断开
```

| 错误码 | 值 | 语义 |
| --- | --- | --- |
| `CodeInternal` | 1 | handler 内部错误（含 panic） |
| `CodeNotFound` | 2 | 路由不存在 |
| `CodeInvalid` | 3 | 载荷或参数非法 |
| `CodeProtocol` | 4 | 协议违规（必须断开连接） |
| `CodeCancelled` | 5 | 对端已取消 |
| `CodeTimeout` | 6 | 处理超时（v1 预留，无产生路径） |
| `CodeAuthDenied` | 7 | 鉴权失败 / 拒绝连接 |
| `CodeSessionExpired` | 8 | 会话过期，恢复失败 |
| `CodeBusy` | 9 | 过载拒绝（含保留队列溢出） |
| `CodeUnsupported` | 10 | 对端未启用的能力被错误使用 |

包级哨兵错误：

```go
var ErrMalformed = errors.New("jsonstream: malformed frame")  // 帧结构损坏
var ErrClosed    = errors.New("jsonstream: connection closed") // 连接已关闭
```

## 并发模型速览

- `Client`/`Server` 的公开方法均可多 goroutine 并发调用；`Close` 幂等。
- 流式/双工 handler 每条流一个 goroutine 顺序执行，handler 内的 `Emitter.Emit`、`Channel.Send/Receive` 无需额外同步。
- `ReadStream.Next` 与 `Channel.Receive` 是单消费接口；`Subscription` 回调独占 goroutine 串行执行。
- `HandlePublish`/`HandleOneWay` 注册的回调每帧一个 goroutine 并发执行，顺序与互斥由回调自行保证。

完整论述见包文档（[pkg.go.dev「并发模型」节](https://pkg.go.dev/github.com/cuihairu/jsonstream#section-documentation)）与 [DESIGN.md](/DESIGN)。
