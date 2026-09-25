// 命令 client 是 JsonStream 的示例客户端：依次演示请求/响应、流式响应
// （含中途取消）、双工通道、单向发送、发布/订阅与服务端主动发起的应答。运行：
//
//	go run ./examples/client            # 连接默认地址
//	go run ./examples/client -addr 127.0.0.1:9000
//
// 断网重连：会话恢复（服务端默认保留 30s），挂起流在重连后继续。
// 详见仓库根 README.md。
package main

import (
	"context"
	"flag"
	"log"
	"time"

	jsonstream "github.com/cuihairu/jsonstream"
)

func main() {
	addr, demo := parseOptions()
	// 错误经 run 返回后统一 Fatal：run 内的 defer（连接/订阅清理）
	// 先执行，不用 log.Fatal 直接中断而跳过它们。
	if err := runWith(defaultRunOptions(demo), addr); err != nil {
		log.Fatal(err)
	}
}

// parseOptions 把命令行翻译成 (地址, 演示时长)。与运行逻辑分开是为了让
// runWith 能被测试直接驱动，不必经过 flag。
func parseOptions() (string, time.Duration) {
	addr := flag.String("addr", "127.0.0.1:9000", "服务端地址")
	demoSec := flag.Int("demo", 10, "演示运行时长（秒）")
	flag.Parse()
	return *addr, time.Duration(*demoSec) * time.Second
}

// runOptions 是演示脚本的可调项。
type runOptions struct {
	cfg jsonstream.Config
	// callTTL 是每次跨端调用的超时上限。默认 5s 对 demo 合适（够慢让人看清
	// 日志，又不至于永远挂着）；测试注入毫秒级——run 里的每个错误出口都要
	// 等满一次超时，没有短上限的话整个测试套件会慢到不可用。
	callTTL time.Duration
	// demo 是演示收尾前的观察时长。
	demo time.Duration
}

func defaultRunOptions(demo time.Duration) runOptions {
	return runOptions{cfg: jsonstream.DefaultConfig(), callTTL: 5 * time.Second, demo: demo}
}

func run(addr string, demo time.Duration) error {
	return runWith(defaultRunOptions(demo), addr)
}

func runWith(o runOptions, addr string) error {
	ctx := context.Background()
	c, err := jsonstream.Dial(ctx, addr, o.cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	c.OnReconnect(func() { log.Printf("reconnected (session resumed)") })
	c.OnResumeFailed(func(err error) { log.Printf("session lost: %v; state rebuilt", err) })

	// ---- 服务端主动发起：客户端作为 responder 注册 handler ----
	c.Handle("client.time", func(_ *jsonstream.Request) (any, error) {
		return map[string]string{"now": time.Now().Format(time.Kitchen)}, nil
	})
	c.HandleOneWay("client.notice", func(msg *jsonstream.Message) error {
		log.Printf("server notice: %s", msg.Payload)
		return nil
	})

	// ---- 发布/订阅：订阅 ticks 主题，后台收广播 ----
	// 带 ctx 的跨端调用都设了上限：对端不实现该路由或卡死时，无超时的
	// 调用会永久挂起（demo 是 API 范例，应示范带上限的用法）。
	subCtx, cancel := context.WithTimeout(ctx, o.callTTL)
	sub, err := c.Subscribe(subCtx, "ticks", func(msg *jsonstream.Message) error {
		log.Printf("broadcast: %s", msg.Payload)
		return nil
	})
	cancel()
	if err != nil {
		return err
	}
	defer sub.Close()

	// ---- 请求/响应 ----
	var sum struct {
		Sum int `json:"sum"`
	}
	reqCtx, cancel := context.WithTimeout(ctx, o.callTTL)
	m, err := c.Request(reqCtx, "math.add", map[string]int{"a": 2, "b": 40})
	cancel()
	if err != nil {
		return err
	}
	if err := m.Decode(&sum); err != nil {
		return err
	}
	log.Printf("math.add(2, 40) = %d", sum.Sum)

	// ---- 流式响应：读两条后取消（服务端 Emitter 收到错误随之收尾） ----
	streamCtx, cancel := context.WithTimeout(ctx, o.callTTL)
	stream, err := c.Stream(streamCtx, "range", map[string]int{"n": 100})
	cancel()
	if err != nil {
		return err
	}
	nextCtx, nextCancel := context.WithTimeout(ctx, o.callTTL)
	defer nextCancel()
	for i := 0; i < 2; i++ {
		msg, ok := stream.Next(nextCtx)
		if !ok {
			break
		}
		log.Printf("range item: %s", msg.Payload)
	}
	// Cancel 的契约是幂等且不返回错误（对端已经消失时它只是静默失败），
	// 所以这里按例丢弃返回值：检查一个恒为 nil 的 error 是噪音，真正的
	// 失败已经在上面的 Next 里体现。
	_ = stream.Cancel()
	log.Printf("range cancelled after 2 items")

	// ---- 双工：双方都可发多帧；Close 是半关闭（"我发完了"） ----
	chCtx, cancel := context.WithTimeout(ctx, o.callTTL)
	ch, err := c.Channel(chCtx, "chat", nil)
	cancel()
	if err != nil {
		return err
	}
	if err := ch.Send(map[string]string{"text": "ping"}); err != nil {
		return err
	}
	recvCtx, cancel := context.WithTimeout(ctx, o.callTTL)
	ack, err := ch.Receive(recvCtx)
	cancel()
	if err != nil {
		return err
	}
	log.Printf("chat ack: %s", ack.Payload)
	// Close 同样是幂等的半关闭，不返回错误（见上面 Cancel 的说明）。
	_ = ch.Close()

	// ---- 单向发送 + 客户端 → 服务端主题发布 ----
	// 注意 API 的不对称：SendOneWay/Publish 没有 ctx 参数（v1 沿用了
	// "不需要返回的交互不带上下文"的直觉），它们在连接断开时等的是重连
	// 而不是调用方超时。带 ctx 的调用（Subscribe/Request/Stream/Channel/
	// Next/Receive）上面都设了上限。
	if err := c.SendOneWay("notify", map[string]string{"text": "fire and forget"}); err != nil {
		return err
	}
	if err := c.Publish("metrics", map[string]int{"cpu": 42}); err != nil {
		return err
	}
	log.Printf("sent oneway + published metrics")

	// 挂在订阅上观察广播；演示时长结束后退出。
	time.Sleep(o.demo)
	log.Printf("done")
	return nil
}
