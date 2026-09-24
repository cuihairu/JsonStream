// 命令 server 是 JsonStream 的示例服务端：演示请求/响应、流式响应与
// 发布/订阅两条链路。运行：
//
//	go run ./examples/server
//
// 详见仓库根 README.md。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	jsonstream "github.com/cuihairu/jsonstream"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "监听地址")
	broadcastEvery := flag.Duration("broadcast", 2*time.Second, "ticks 主题广播间隔")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	cfg := jsonstream.DefaultConfig()

	srv, err := jsonstream.NewServer(ln, cfg)
	if err != nil {
		log.Fatal(err)
	}

	// ---- 请求/响应：单帧回包 ----
	srv.Handle("math.add", func(req *jsonstream.Request) (any, error) {
		var args struct {
			A int `json:"a"`
			B int `json:"b"`
		}
		if err := req.Decode(&args); err != nil {
			return nil, &jsonstream.Error{Code: jsonstream.CodeInvalid, Message: err.Error()}
		}
		log.Printf("math.add(%d, %d)", args.A, args.B)
		return map[string]int{"sum": args.A + args.B}, nil
	})

	// ---- 流式响应：Emitter 多帧下发，正常返回自动 COMPLETE ----
	srv.HandleStream("range", func(req *jsonstream.Request, em jsonstream.Emitter) error {
		var args struct {
			N int `json:"n"`
		}
		if err := req.Decode(&args); err != nil {
			return err
		}
		if args.N > 1000 {
			return &jsonstream.Error{Code: jsonstream.CodeInvalid, Message: "n too large"}
		}
		for i := 0; i < args.N; i++ {
			if err := em.Emit(map[string]int{"n": i}); err != nil {
				return err // 对端 Cancel 或连接断开
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	})

	// ---- 单向：永不回帧（ONEWAY 无响应、无错误帧） ----
	srv.HandleOneWay("notify", func(msg *jsonstream.Message) error {
		log.Printf("oneway notify: %s", msg.Payload)
		return nil
	})

	// ---- 客户端 → 服务端单向发布（如指标上报） ----
	srv.HandlePublish("metrics", func(msg *jsonstream.Message) error {
		log.Printf("client metrics: %s", msg.Payload)
		return nil
	})

	// ---- 服务端 → 订阅者周期广播 ----
	go func() {
		for tick := 0; ; tick++ {
			time.Sleep(*broadcastEvery)
			if err := srv.Publish("ticks", map[string]any{"tick": tick, "at": time.Now().Format(time.Kitchen)}); err != nil {
				log.Printf("publish: %v", err)
			}
		}
	}()

	// ---- 服务端主动发起：向每个在线会话单向推送通知并请求 client.time ----
	go func() {
		for {
			time.Sleep(3 * time.Second)
			for _, sid := range srv.Sessions() {
				if err := srv.SendOneWay(sid, "client.notice", map[string]string{"text": "server says hi"}); err != nil {
					log.Printf("oneway to %s: %v", sid[:8], err)
					continue
				}
				// 对端请求必须带超时：客户端不实现 client.time 或卡死时，
				// 无超时的 Request 会永久挂起并停摆整个通知循环
				reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				m, err := srv.Request(reqCtx, sid, "client.time", nil)
				cancel()
				if err != nil {
					log.Printf("request to %s: %v", sid[:8], err)
					continue
				}
				log.Printf("client %s… time: %s", sid[:8], m.Payload)
			}
		}
	}()

	log.Printf("JsonStream server listening on %s", *addr)
	log.Fatal(srv.Serve())
}
