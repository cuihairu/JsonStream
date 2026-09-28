---
layout: home

hero:
  name: JsonStream
  text: 一道面试题衍生的 TCP JSON 帧协议库
  tagline: 基于自定义二进制帧协议承载 JSON 业务数据——一条 TCP 连接多路复用请求/响应、流式、双工、单向与发布/订阅，内建心跳、断线恢复、可选压缩/加密与 credit 背压。纯标准库 Go 参考实现，零第三方依赖；协议规范语言无关，其他语言实现规划中。
  image:
    src: /logo.svg
    alt: JsonStream
  actions:
    - theme: brand
      text: 快速开始
      link: /getting-started
    - theme: alt
      text: 协议规范（单一事实源）
      link: /protocol
    - theme: alt
      text: API 参考
      link: /api

features:
  - icon: 🚀
    title: 上手指南
    details: 从 go get 到五种交互模式跑通一遍：请求/响应、流式、双工、单向、发布/订阅，全部对照真实 API，可直接运行。
    link: /getting-started
    linkText: 开始使用
  - icon: 📋
    title: 面试题要求
    details: 题面原文原样分组归档，逐条对照实现现状——完成即标注依据，取舍即说明理由，不夸大。
    link: /interview-requirements
    linkText: 题面与现状
  - icon: 📐
    title: 协议规范
    details: 语言无关的单一事实源：帧格式逐字段、握手协商、交互原语、错误码表、变换管线、背压与恢复——只拿到这一份文档就应能写出可互通的实现。
    link: /protocol
    linkText: 阅读规范
  - icon: 📚
    title: API 参考
    details: Go 参考实现的全部公开 API：入口、五种交互的发起与响应端、Config 逐字段默认值、帧层与错误码表。
    link: /api
    linkText: 查阅 API
  - icon: 🔄
    title: 与 WebSocket 对照
    details: WS 有我们没有的（浏览器可达、TLS 承载），分片已定稿为独立 FRAGMENT 帧型（实现落地中），WS 没有我们内建的（req/res、pub/sub、背压、恢复）——每条附文档依据。
    link: /websocket-comparison
    linkText: 逐条对照
  - icon: 🧵
    title: TCP 流特性与协议横评
    details: 粘包/半包的一般处理、自定义协议的决策框架，以及 MQTT、WebSocket、RSocket、HTTP/2、HTTP/3 的机制与取舍横评。
    link: /tcp-and-landscape
    linkText: 看横评
  - icon: 🏗
    title: 架构与取舍
    details: Go 参考实现的架构级讲解：整体架构、一帧的旅程、并发模型，以及每条决策的备选方案与放弃理由。
    link: /DESIGN
    linkText: 架构讲解
  - icon: 🧠
    title: 知识点梳理
    details: 按面试考点组织：JSON 解析文法、TCP 分帧、Go 并发与陷阱，每条先讲原理再对应到仓库实现。
    link: /NOTES
    linkText: 复习考点
  - icon: 🔧
    title: 故障排查 / FAQ
    details: Pages 大小写 404、base 与部署链、pnpm 12 构建脚本放行、fuzz/集成测试命令、codecov 门禁逐项解读——全部对到仓库实际配置。
    link: /faq
    linkText: 查 FAQ
---

## 快速开始

```bash
go get github.com/cuihairu/jsonstream
```

```go
c, _ := jsonstream.Dial(ctx, addr, jsonstream.DefaultConfig())
m, _ := c.Request(ctx, "math.add", map[string]int{"a": 2, "b": 40}) // 请求/响应
s, _ := c.Stream(ctx, "range", map[string]int{"n": 100})           // 流式
defer s.Cancel()
sub, _ := c.Subscribe(ctx, "ticks", func(m *jsonstream.Message) error { return nil })
_ = c.Publish(ctx, "metrics", v) // client → server
```

压缩/加密/背压都是参数化开关，握手协商后生效（双方都开才启用）：

```go
cfg := jsonstream.DefaultConfig()
cfg.Compress = true // ≥64B 载荷走 flate
cfg.Encrypt = true  // AES-256-GCM，先压后加
cfg.Key = key32     // 32 字节预共享密钥
cfg.Credit = 64     // 连接级信用窗口，生效值取双方最小
```

库与协议的全貌：上手与五种模式见 [getting-started.md](/getting-started)，全部公开 API 见 [api.md](/api)，帧/变换/端到端的基准实测见 [benchmarks.md](/benchmarks)，架构与取舍见 [DESIGN.md](/DESIGN)，协议逐字段定义见 [protocol.md](/protocol)，与 WebSocket 的能力边界见 [websocket-comparison.md](/websocket-comparison)，构建/测试/部署的常见问题见 [faq.md](/faq)。
