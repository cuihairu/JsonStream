// Package jsonstream 实现一套基于 TCP 的 JSON 消息帧协议：定长二进制
// 帧头承载路由与元信息，载荷为 JSON，单条连接多路复用五种交互模式
// （请求/响应、流式、双工通道、单向、发布/订阅），并在传输层参数化
// 心跳保活、断线重连与会话恢复、flate 压缩、AES-256-GCM 加密与连接级
// 信用窗口背压。
//
// 客户端入口是 [Dial]，服务端入口是 [NewServer]；协议规范见
// docs/protocol.md，设计权衡见 docs/design-notes.md。
//
// # 并发模型
//
// [Client] 与 [Server] 的公开方法（注册回调、发起交互、Close 等）均可
// 被多个 goroutine 并发调用：出站帧经带缓冲通道串行写出，会话与端点
// 引用有锁保护，Close 幂等且与在途请求竞争时以错误终结全部挂起流。
//
// 回调与消费端的边界：
//
//   - 流式与双工 handler（HandleStream/HandleChannel 及客户端对称面）
//     每条流一个 goroutine 顺序执行，handler 内的 Emitter.Emit、
//     Channel.Send/Receive 无需额外同步；
//   - [ReadStream.Next] 与 [Channel.Receive] 是单消费接口：多个
//     goroutine 并发调用会互相抢帧（不崩溃，但帧的归属不确定）；
//   - [Subscription] 的主题回调在内部独占 goroutine 中逐帧串行执行，
//     回调内可安全调用 Client 的其他方法；
//   - HandlePublish 与 HandleOneWay 注册的回调每帧一个 goroutine 并发
//     执行——同一主题高频发布时回调可能同时运行且不保证顺序，需要
//     顺序或互斥时由回调自行保证。
//
// Config 是值类型：传入 [Dial]/[NewServer] 之后再修改原变量，不影响已
// 建立的端。
package jsonstream
