# 上手指南

从安装到五种交互模式的完整走一遍。所有示例都对照 Go 参考实现的真实 API 写成，可直接运行；逐方法的参考见 [API 参考](/api)，线上字节格式见[协议规范](/protocol)。

## 环境与安装

Go ≥ 1.24，纯标准库、零第三方依赖：

```bash
go get github.com/cuihairu/jsonstream
```

## 第一个请求/响应

服务端：监听、注册路由、`Serve`（`Serve` 阻塞，通常 `go srv.Serve()`；`Close` 会停掉它）：

```go
ln, _ := net.Listen("tcp", "127.0.0.1:9000")
srv, _ := jsonstream.NewServer(ln, jsonstream.DefaultConfig())
srv.Handle("math.add", func(r *jsonstream.Request) (any, error) {
    var in map[string]int
    if err := r.Decode(&in); err != nil {
        return nil, err
    }
    return in["a"] + in["b"], nil
})
go srv.Serve()
defer srv.Close()
```

客户端：`Dial` 阻塞到握手完成（CONNACK），之后发请求：

```go
c, err := jsonstream.Dial(ctx, "127.0.0.1:9000", jsonstream.DefaultConfig())
if err != nil {
    log.Fatal(err) // 首连失败立即报错；连上之后的断连才自动重连
}
defer c.Close()

m, err := c.Request(ctx, "math.add", map[string]int{"a": 2, "b": 40})
if err != nil {
    log.Fatal(err)
}
var sum int
m.Decode(&sum) // sum == 42
```

要点：

- 发起类方法的第一个参数都是 `ctx`，它约束「等可用连接」与「等发送入队」两段等待；
- handler 返回的值会被 JSON 序列化为响应载荷；返回 `error` 则对端收到错误帧（`*jsonstream.Error` 原样保留错误码，其余归一为 INTERNAL）。

## 五种交互模式

一条 TCP 连接多路复用，每条消息属于一条流（Stream ID）：

| 模式 | 发起 | 服务端 handler | 语义 |
| --- | --- | --- | --- |
| 请求/响应 | `Client.Request` | `Handle(route, func(*Request) (any, error))` | 一问一答，响应帧自带终结语义 |
| 流式 | `Client.Stream` | `HandleStream(route, func(*Request, Emitter) error)` | handler 逐帧 `Emit`，返回即 COMPLETE |
| 双工通道 | `Client.Channel` | `HandleChannel(route, func(*Channel) error)` | 双方随时互发，任意方关闭/取消 |
| 单向 | `Client.SendOneWay` | `HandleOneWay(route, func(*Message) error)` | 只管发出，连错误都不回 |
| 发布/订阅 | `Client.Subscribe` / `Client.Publish` | `HandlePublish(topic, func(*Message) error)` | 主题双向：C→S 打 handler，S→C 投订阅 |

### 流式（服务端 → 客户端多帧）

```go
srv.HandleStream("range", func(r *jsonstream.Request, em jsonstream.Emitter) error {
    var in map[string]int
    _ = r.Decode(&in)
    for i := 0; i < in["n"]; i++ {
        if err := em.Emit(map[string]int{"i": i}); err != nil {
            return err // 流被取消等场景，错误会传给对端
        }
    }
    return nil // 正常返回自动发 COMPLETE
})
```

```go
st, _ := c.Stream(ctx, "range", map[string]int{"n": 3})
defer st.Cancel() // 幂等；流正常结束后是空操作
for {
    m, ok := st.Next(ctx) // ok == false 表示流终结
    if !ok {
        break
    }
    var it map[string]int
    _ = m.Decode(&it)
}
if err := st.Err(); err != nil {
    // 非正常终结（对端错误 / 本端取消）时非 nil
}
```

### 双工通道

```go
srv.HandleChannel("chat", func(ch *jsonstream.Channel) error {
    for {
        m, err := ch.Receive(context.Background()) // 阻塞收对端消息
        if err != nil {
            return err // 对端 COMPLETE 后是 io.EOF；取消/出错则是对应错误
        }
        var reply map[string]any
        _ = m.Decode(&reply)
        if err := ch.Send(map[string]string{"echo": "ok"}); err != nil {
            return err
        }
    }
})
```

```go
ch, _ := c.Channel(ctx, "chat", nil)
defer ch.Close()
_ = ch.Send(map[string]string{"hello": "world"})
m, err := ch.Receive(ctx)
```

`Channel.Send` 可并发调用；`Channel.Receive` 是单消费接口。

### 发布/订阅

```go
srv.HandlePublish("metrics", func(m *jsonstream.Message) error {
    log.Printf("C→S publish on %s", m.Topic)
    return nil
})
```

```go
sub, _ := c.Subscribe(ctx, "ticks", func(m *jsonstream.Message) error {
    return nil // S→C 投递；返回错误只记日志，不断订阅
})
defer sub.Close() // 退订

_ = c.Publish(ctx, "metrics", map[string]float64{"cpu": 0.42}) // C→S
```

订阅回调在内部独占 goroutine 中逐帧串行执行，回调内可安全调用 Client 的其他方法。

## 参数化开关

压缩、加密、背压都是 `Config` 开关，握手协商后生效（双方都开才启用）：

```go
cfg := jsonstream.DefaultConfig()
cfg.Compress = true // ≥64B 载荷走 flate
cfg.Encrypt = true  // AES-256-GCM（先压后加），Key 必须是 32 字节
cfg.Key = key32     // 预共享密钥，不上线传输
cfg.Credit = 64     // 连接级信用窗口（条数），生效值取双方最小
```

断线恢复默认开启：客户端自动重连（指数退避 + 抖动），服务端默认保留会话 30s / 每会话下行 4 MiB。应用可挂两个钩子：

```go
c.OnReconnect(func() { log.Println("reconnected") })
c.OnResumeFailed(func(err error) { log.Println("session lost:", err) })
```

服务端也能主动发起（按 `sessionID` 定位会话）：

```go
for _, id := range srv.Sessions() {
    _, _ = srv.Request(ctx, id, "ping", nil)
}
```

鉴权钩子在握手期校验 CONNECT 的令牌，拒绝即断开：

```go
srv.OnAuth(func(cj *jsonstream.ConnectJSON) error {
    if cj.Auth != "secret" {
        return &jsonstream.Error{Code: jsonstream.CodeAuthDenied, Message: "bad token"}
    }
    return nil
})
```

## 下一步

- [API 参考](/api)：全部公开类型与方法签名；
- [协议术语速查](/glossary)：帧类型、Flags、错误码、配置参数四张对照表；
- [协议规范](/protocol)：语言无关的线上格式单一事实源；
- [架构与取舍](/DESIGN)：每条设计决策的备选方案与放弃理由；
- 可运行的完整示例：[`examples/server`](https://github.com/cuihairu/jsonstream/tree/main/examples/server) 与 [`examples/client`](https://github.com/cuihairu/jsonstream/tree/main/examples/client)（请求/响应、流式取消、双工、单向、发布/订阅、服务端主动发起全覆盖）。
