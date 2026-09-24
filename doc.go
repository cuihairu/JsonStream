// Package jsonstream 实现一套基于 TCP 的 JSON 消息帧协议：定长二进制
// 帧头承载路由与元信息，载荷为 JSON，单条连接多路复用六种交互模式
// （请求/响应、单向、双向流、双工通道、发布/订阅），并在传输层参数化
// 心跳保活、断线重连与会话恢复、flate 压缩、AES-256-GCM 加密与连接级
// 信用窗口背压。
//
// 客户端入口是 [Dial]，服务端入口是 [NewServer]；协议规范见
// docs/protocol.md，设计权衡见 docs/design-notes.md。
package jsonstream
