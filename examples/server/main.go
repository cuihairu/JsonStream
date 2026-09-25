// 命令 server 是 JsonStream 的示例服务端：演示请求/响应、流式响应、双工
// 通道、单向发送、发布/订阅与服务端主动发起。运行：
//
//	go run ./examples/server
//	go run ./examples/server -addr 127.0.0.1:9000 -broadcast 500ms
//
// 详见仓库根 README.md。
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"time"

	jsonstream "github.com/cuihairu/jsonstream"
)

// options 是示例服务端的可调项。flag 解析与运行逻辑分开（parseOptions /
// serve），是为了让 serve 能被测试直接驱动：测试把两个循环的间隔压到毫秒
// 级、用 ctx 取消来停机，而不用真的等 2~3 秒——demo 的默认节奏对测试太慢。
type options struct {
	addr string
	// ln 非 nil 时 serve 改用它而不再自行监听。留这个口子只有一个理由：
	// 测试需要先拿到确定的端口再启动 serve（自行监听时端口由内核随机
	// 分配，调用方无从得知）。正常使用留 nil。
	ln             net.Listener
	cfg            jsonstream.Config
	broadcastEvery time.Duration
	notifyEvery    time.Duration
	notifyTimeout  time.Duration
}

func main() {
	if err := serve(context.Background(), parseOptions()); err != nil {
		log.Fatal(err)
	}
}

// parseOptions 把命令行翻译成 options。
func parseOptions() options {
	addr := flag.String("addr", "127.0.0.1:9000", "监听地址")
	broadcastEvery := flag.Duration("broadcast", 2*time.Second, "ticks 主题广播间隔")
	notifyEvery := flag.Duration("notify", 3*time.Second, "服务端主动发起的间隔")
	flag.Parse()

	return options{
		addr:           *addr,
		cfg:            jsonstream.DefaultConfig(),
		broadcastEvery: *broadcastEvery,
		notifyEvery:    *notifyEvery,
		// 跨端调用必须带上限：对端不实现该路由或卡死时，无超时的调用会
		// 永久挂起（demo 是 API 范例，应示范带上限的用法）。
		notifyTimeout: 5 * time.Second,
	}
}

// serve 装配路由、启动后台循环并阻塞服务，直到 ctx 取消或监听失败。
// 停机只走一条路径：ctx 取消 → srv.Close() → Serve 返回 nil。
func serve(ctx context.Context, o options) error {
	ln := o.ln
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", o.addr); err != nil {
			return err
		}
	}
	srv, err := jsonstream.NewServer(ln, o.cfg)
	if err != nil {
		return err
	}
	registerHandlers(srv)

	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			_ = srv.Close()
		case <-stopped:
		}
	}()

	go broadcastLoop(ctx, srv, o.broadcastEvery)
	go notifyLoop(ctx, srv, o.notifyEvery, o.notifyTimeout)

	log.Printf("JsonStream server listening on %s", ln.Addr())
	return srv.Serve()
}

func registerHandlers(srv *jsonstream.Server) {
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

	// ---- 双工：双方都可发多帧，"我说完了"是半关闭（COMPLETE）----
	srv.HandleChannel("chat", func(ch *jsonstream.Channel) error {
		for {
			// 阻塞等对端是双工的正常态；对端 Close/Cancel 或断连让
			// Receive 返回错误，循环随之收尾
			m, err := ch.Receive(context.Background())
			if errors.Is(err, io.EOF) {
				// 对端正常半关闭（COMPLETE）：这不是错误，返回 nil 让框架
				// 走 Close 分支。若原样把 io.EOF 抛上去，框架会当成 handler
				// 出错、向一条已经正常结束的流补一个 ERROR(INTERNAL) 帧。
				return nil
			}
			if err != nil {
				return err
			}
			var msg struct {
				Text string `json:"text"`
			}
			if err := m.Decode(&msg); err != nil {
				return err
			}
			if err := ch.Send(map[string]string{"ack": "got: " + msg.Text}); err != nil {
				return err
			}
		}
	})

	// ---- 单向：永不回帧（ONEWAY 无响应、无错误帧）----
	srv.HandleOneWay("notify", func(msg *jsonstream.Message) error {
		log.Printf("oneway notify: %s", msg.Payload)
		return nil
	})

	// ---- 客户端 → 服务端单向发布（如指标上报）----
	srv.HandlePublish("metrics", func(msg *jsonstream.Message) error {
		log.Printf("client metrics: %s", msg.Payload)
		return nil
	})
}

// broadcastLoop 周期向 ticks 主题广播。Publish 的错误只有"载荷无法 JSON
// 编码"一种来源，而这里的载荷是固定结构的 map，恒可编码——所以返回值按
// 例忽略。真实代码里遇到来源不确定的错误则必须处理。
func broadcastLoop(ctx context.Context, srv *jsonstream.Server, every time.Duration) {
	for tick := 0; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		_ = srv.Publish("ticks", map[string]any{"tick": tick, "at": time.Now().Format(time.Kitchen)})
	}
}

// notifyLoop 周期向每个在线会话单向推送通知并回读一次时间。
func notifyLoop(ctx context.Context, srv *jsonstream.Server, every, timeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		for _, sid := range srv.Sessions() {
			if err := notify(srv, sid, timeout); err != nil {
				log.Printf("notify %s: %v", sid, err)
			}
		}
	}
}

// notify 向一个会话发通知并回读一次时间。两步的失败对 demo 是同一件事
// （这个会话此刻不配合），合并成一个错误出口，省掉调用点一处重复分支。
func notify(srv *jsonstream.Server, sessionID string, timeout time.Duration) error {
	if err := srv.SendOneWay(sessionID, "client.notice", map[string]string{"text": "server says hi"}); err != nil {
		return err
	}
	// 对端请求必须带超时：客户端不实现 client.time 或卡死时，无超时的
	// Request 会永久挂起并停摆整个通知循环。
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	m, err := srv.Request(ctx, sessionID, "client.time", nil)
	if err != nil {
		return err
	}
	log.Printf("client %s time: %s", sessionID, m.Payload)
	return nil
}
