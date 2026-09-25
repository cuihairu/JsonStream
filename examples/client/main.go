// 命令 client 是 JsonStream 的示例客户端：依次演示请求/响应、流式响应
// （含中途取消）、单向发送、发布/订阅与服务端主动发起的应答。运行：
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
	addr := flag.String("addr", "127.0.0.1:9000", "服务端地址")
	demoSec := flag.Int("demo", 10, "演示运行时长（秒）")
	flag.Parse()

	// 错误经 run 返回后统一 Fatal：run 内的 defer（连接/订阅清理）
	// 先执行，不用 log.Fatal 直接中断而跳过它们。
	if err := run(*addr, time.Duration(*demoSec)*time.Second); err != nil {
		log.Fatal(err)
	}
}

func run(addr string, demo time.Duration) error {
	ctx := context.Background()
	c, err := jsonstream.Dial(ctx, addr, jsonstream.DefaultConfig())
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
	// 跨端调用都带上限：对端不实现该路由或卡死时，无超时的调用会永久
	// 挂起（demo 是 API 范例，应示范带上限的用法）
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	stream, err := c.Stream(ctx, "range", map[string]int{"n": 100})
	if err != nil {
		return err
	}
	nextCtx, nextCancel := context.WithTimeout(ctx, 5*time.Second)
	defer nextCancel()
	for i := 0; i < 2; i++ {
		msg, ok := stream.Next(nextCtx)
		if !ok {
			break
		}
		log.Printf("range item: %s", msg.Payload)
	}
	if err := stream.Cancel(); err != nil {
		return err
	}
	log.Printf("range cancelled after 2 items")

	// ---- 单向发送 + 客户端 → 服务端主题发布 ----
	if err := c.SendOneWay("notify", map[string]string{"text": "fire and forget"}); err != nil {
		return err
	}
	if err := c.Publish("metrics", map[string]int{"cpu": 42}); err != nil {
		return err
	}
	log.Printf("sent oneway + published metrics")

	// 挂在订阅上观察广播；演示时长结束后退出。
	time.Sleep(demo)
	log.Printf("done")
	return nil
}
