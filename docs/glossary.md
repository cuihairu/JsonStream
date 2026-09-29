# 协议术语速查

帧类型、Flags、错误码、配置参数四张表，写实现和排障时快速对照。口径以[协议规范](/protocol)为单一事实源；方法签名与 Go 常量的完整出处见 [API 参考](/api)。本页只收数值与一句话语义，取舍理由、帧序规则与恢复流程不在这里展开。

## 帧类型

线上 `Type` 字段的全部合法值（[协议 §4](/protocol#_4-帧类型)）。收到未定义类型：立即断开且不回帧——对照 WebSocket 对未知 opcode 直接 fail the connection。

| 值 | 名称 | 方向 | Stream ID | 用途 |
| --- | --- | --- | --- | --- |
| 0x01 | `CONNECT` | C→S | 0 | 握手，携带能力声明与会话恢复凭证 |
| 0x02 | `CONNACK` | S→C | 0 | 握手应答，下发会话 ID 与生效参数 |
| 0x03 | `PING` | 双向 | 0 | 心跳探测 |
| 0x04 | `PONG` | 双向 | 0 | 心跳应答 |
| 0x05 | `REQUEST` | 双向 | ≠0 | 发起一次请求（单帧/流式/双工，看 Flags） |
| 0x06 | `RESPONSE` | 双向 | ≠0 | 流上的一帧数据 |
| 0x07 | `COMPLETE` | 双向 | ≠0 | 流正常终止 |
| 0x08 | `CANCEL` | 双向 | ≠0 | 取消流：立即终结 |
| 0x09 | `ONEWAY` | 双向 | ≠0 | 单向发送，永不产生任何响应帧 |
| 0x0A | `ERROR` | 双向 | ≠0/0 | 终结流；Stream ID=0 是连接级致命错误（随后必须断开） |
| 0x0B | `SUBSCRIBE` | C→S | ≠0 | 订阅主题 |
| 0x0C | `SUBACK` | S→C | ≠0 | 订阅确认 |
| 0x0D | `UNSUBSCRIBE` | C→S | ≠0 | 退订；成功不回帧，失败回 `ERROR` |
| 0x0E | `PUBLISH` | 双向 | ≠0* | 发布/投递（由订阅/发布动作关联，本身不开启流） |
| 0x0F | `CREDIT` | 双向 | ≠0 | 背压授权：补 n 条发送额度 |
| 0x10 | `FRAGMENT` | 双向 | ≠0 | 分片消息的续段/收尾帧，永不单独出现 |

`FRAGMENT`（0x10）与 bit5 `Fragmented` 为协议定稿能力，Go 参考实现的支持在落地中——合入前对端不应发送这类帧，当前参考实现按未知类型/保留位违规断开（[§3.4 实现状态](/protocol#_3-4-分片传输-大消息)）。Go 常量表当前到 0x0F，见 [API 参考 · 帧层](/api#帧层-低层)。

帧形惯例：`PING`/`PONG`/`COMPLETE`/`CANCEL`/`SUBACK` 的 payload 为空且不带 Metadata；`UNSUBSCRIBE` 携带 topic Metadata；`CREDIT` 的 payload 为 `{"n": N}`；`FRAGMENT` 不带 Metadata 且除 `Fragmented` 外不得置其他位。完整清单见[协议 §4](/protocol#_4-帧类型)。

## opcode 与 Flags

本协议没有 WebSocket 式的 opcode：连接复用由 `Type`（上一节）承担，两种分帧方案的对照见[协议 §3.3](/protocol#_3-3-为什么是-长度前缀-类型字段)。每帧的第二个控制字节是 `Flags`，标注载荷变换与流语义（[§3.2 字段说明](/protocol#_3-2-字段说明)）：

| 位 | 名称 | 语义 |
| --- | --- | --- |
| bit0 | `Compressed` | payload 经 flate（raw DEFLATE 流，无 zlib/gzip 外壳） |
| bit1 | `Encrypted` | payload 经 AES-256-GCM（12B nonce 前置 + 16B tag，共 28B 开销） |
| bit2 | `HasMeta` | 帧头后携带 Metadata 段（明文 JSON，上限 64 KiB−1） |
| bit3 | `Stream` | 流式请求 |
| bit4 | `Channel` | 双工通道请求（隐含 Stream） |
| bit5 | `Fragmented` | 分片：开帧与续段置位，收尾段不带（见 [§3.4](/protocol#_3-4-分片传输-大消息)） |
| bit6–7 | 保留 | 必须为 0 |

一帧的语义由 Type + Flags 共同表达：同一个 `RESPONSE` 帧型按位组合落在请求/响应、流式、双工、订阅投递等不同模式里。解码端按帧内 Flags 走变换管线、不依赖握手协商结果，所以单连接内可以混合发送（心跳与握手恒明文，大载荷才压缩）。

## 错误码

`ERROR` 帧 payload 为 `{"code": N, "message": "…"}`（[协议 §7.10](/protocol#_7-10-错误)）。携带非零 Stream ID 时终结该流；Stream ID = 0 表示连接级致命错误，双方随后必须断开 TCP。

| code | 名称 | 含义 |
| --- | --- | --- |
| 1 | `INTERNAL` | 处理器内部错误（handler panic 归并为此） |
| 2 | `NOT_FOUND` | 路由不存在 |
| 3 | `INVALID` | 载荷/参数非法（JSON 解码失败等） |
| 4 | `PROTOCOL` | 协议违规（坏魔数、未知类型等帧层错误）；Flags 声明与 handler 模式不符也用它，但只终结所在流 |
| 5 | `CANCELLED` | 对端已取消 |
| 6 | `TIMEOUT` | 服务端处理超时（v1 预留，当前无产生路径） |
| 7 | `AUTH_DENIED` | 鉴权失败 / 拒绝连接 |
| 8 | `SESSION_EXPIRED` | 会话过期，恢复失败 |
| 9 | `BUSY` | 过载拒绝（含保留队列溢出） |
| 10 | `UNSUPPORTED` | 对端使用了本端未启用的能力（如未启用加密却收到加密帧）；发生在变换层，接收方直接断连 |

断连与否看错误发生在哪一层：握手与帧/变换层错误（版本不匹配、坏魔数、未知类型、非零保留位、解密/解压失败、`UNSUPPORTED`）必须断开；应用层语义错误（1/2/3/6/9）以 `ERROR` 回执后连接继续服务其他流。完整惯例见[协议 §7.10](/protocol#_7-10-错误)。

Go 侧另有包级哨兵错误 `ErrMalformed`（帧结构损坏）与 `ErrClosed`（连接已关闭，[API 参考 · 错误](/api#错误)）：它们是本地 API 返回值，不是线上错误码。

## 配置参数

`Config` 全部 13 个字段的默认值与一句话语义。逐字段完整说明见 [API 参考 · Config](/api#config)；协商生效规则（压缩/加密双方都开才启用、credit 取双方大于 0 者的最小值、心跳与保留期由服务端权威下发）见[协议 §6.1](/protocol#_6-1-协商)，可运行的开关注册法见上手指南的[参数化开关](/getting-started#参数化开关)。

| 字段 | 类型 | 默认 | 语义 |
| --- | --- | --- | --- |
| `Heartbeat` | `time.Duration` | 10s | PING 间隔；读空闲超 1.5× 判死。零值取默认，低于 1s 钳到 1s |
| `Compress` | `bool` | `false` | flate 压缩（载荷 ≥64B 才实际压缩） |
| `Encrypt` | `bool` | `false` | AES-256-GCM；启用时 `Key` 必须是 32 字节 |
| `Key` | `[]byte` | `nil` | 32 字节预共享密钥，不上线传输 |
| `Credit` | `int` | 0（关闭） | 连接级信用窗口（条数）；生效值 = min(双方配置)，任一方 ≤0 关闭 |
| `Auth` | `string` | `""` | 透传到 CONNECT 的令牌，服务端在 `OnAuth` 中校验 |
| `Retention` | `time.Duration` | 30s | 服务端会话保留期；负值禁用会话恢复 |
| `RetentionBytes` | `int` | 4 MiB | 每会话下行保留队列字节上限，超限失去恢复资格 |
| `DialTimeout` | `time.Duration` | 5s | 建立 TCP 连接的超时 |
| `Reconnect` | `*bool` | nil（= true） | false 时客户端不自动重连 |
| `BackoffInitial` | `time.Duration` | 100ms | 重连退避起点（指数 + 抖动） |
| `BackoffMax` | `time.Duration` | 5s | 重连退避上限 |
| `Logger` | `Logger` | nil（静默） | 适配 `*log.Logger` 等常见实现 |

排障时先分清层次：帧/Flags/错误码的问题查本页前两节与 [FAQ](/faq)，行为不符合预期（重连、恢复、背压）查[协议 §7/§8](/protocol#_7-连接与交互语义)。
