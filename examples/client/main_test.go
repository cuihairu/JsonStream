package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	jsonstream "github.com/cuihairu/jsonstream"
)

// 测试脚手架：示例客户端的 runWith 会用到全部五种交互原语，这里按需注册
// 路由、注入失败，并在指定路由被调用后掐断连接——run 是顺序执行的，连接
// 在哪一步断掉，就决定了哪个错误出口被走到。

// trackingListener 记录接入的连接，供测试主动掐断（触发"连接已死"），并
// 统计接入总数（判断客户端是否真的重连过：Sessions() 只反映"当前在线"，
// 旧连接被摘除后计数会回落，用它判断重连会看漏）。
type trackingListener struct {
	net.Listener
	mu      sync.Mutex
	conns   []net.Conn
	accepts int
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.accepts++
	l.mu.Unlock()
	return c, nil
}

func (l *trackingListener) dropAll() {
	l.mu.Lock()
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (l *trackingListener) accepted() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.accepts
}

type serverOpts struct {
	omit         map[string]bool // 不注册的路由（制造 NOT_FOUND）
	mathAddReply string          // math.add 的响应 JSON
	chatErr      bool            // chat handler 直接返回错误
	stallRange   bool            // range 只发一帧就停住，稍后掐断连接
	retention    time.Duration   // 会话保留期；-1 表示禁用恢复
}

// startDemoServer 起一个覆盖示例客户端全流程的服务端。
func startDemoServer(t *testing.T, o serverOpts) *demoServer {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &trackingListener{Listener: raw}
	cfg := jsonstream.DefaultConfig()
	if o.retention != 0 {
		cfg.Retention = o.retention
	}
	srv, err := jsonstream.NewServer(ln, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// stall 用于把流式 handler 挂住（见 range 分支），测试收尾时放行，避免
	// handler goroutine 泄漏到后续测试。
	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })

	if !o.omit["math.add"] {
		reply := o.mathAddReply
		if reply == "" {
			reply = `{"sum":42}`
		}
		srv.Handle("math.add", func(*jsonstream.Request) (any, error) {
			return json.RawMessage(reply), nil
		})
	}
	if !o.omit["range"] {
		srv.HandleStream("range", func(_ *jsonstream.Request, em jsonstream.Emitter) error {
			if o.stallRange {
				// 只发一帧就停住：示例的读循环会卡在第二次 Next 上（等它
				// 自己的超时上限），这就给测试留出一个稳定的"客户端阻塞"
				// 窗口——连接可以在窗口正中掐断，而不必和客户端抢微秒级的
				// 竞态窗口。之后示例会走到 Channel 那一步并撞上死连接。
				if err := em.Emit(map[string]int{"n": 0}); err != nil {
					return err
				}
				time.AfterFunc(50*time.Millisecond, ln.dropAll)
				<-stall // 挂住 handler，测试收尾时统一放行
			}
			// 产出 5 帧：示例只读 2 帧就取消，多几帧保证读循环走满。
			for i := 0; i < 5; i++ {
				if err := em.Emit(map[string]int{"n": i}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if !o.omit["chat"] {
		srv.HandleChannel("chat", func(ch *jsonstream.Channel) error {
			// 先收一条再报错：若 handler 一上来就报错，ERROR 帧可能在
			// 客户端的 ch.Send 之前抵达，于是 run 停在 Send 而不是
			// Receive 上——那是个竞态，不是被测行为。先收一条可以把
			// 客户端的 Send 稳定地放在前面。
			m, err := ch.Receive(context.Background())
			if o.chatErr {
				return &jsonstream.Error{Code: jsonstream.CodeInvalid, Message: "chat refused"}
			}
			if err != nil {
				return err
			}
			return ch.Send(map[string]string{"ack": "got " + string(m.Payload)})
		})
	}
	if !o.omit["notify"] {
		srv.HandleOneWay("notify", func(*jsonstream.Message) error { return nil })
	}
	if !o.omit["metrics"] {
		srv.HandlePublish("metrics", func(*jsonstream.Message) error { return nil })
	}

	go srv.Serve()
	t.Cleanup(func() {
		_ = srv.Close()
		ln.dropAll()
	})
	return &demoServer{Server: srv, ln: ln}
}

type demoServer struct {
	*jsonstream.Server
	ln *trackingListener
}

func (d *demoServer) addr() string { return d.ln.Addr().String() }

func waitSessions(t *testing.T, srv *jsonstream.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(srv.Sessions()) >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("sessions did not reach %d", want)
}

// noReconnect 关掉自动重连：连接一死，后续调用就立刻走到"等不到端点"的
// 错误出口，而不是被重连循环悄悄救回来（那样测不到错误分支）。
func noReconnect(demo time.Duration) runOptions {
	o := defaultRunOptions(demo)
	o.callTTL = 300 * time.Millisecond
	no := false
	o.cfg.Reconnect = &no
	return o
}

// run 是不带选项的薄封装：走一遍它的默认路径。
func TestRunDefaultOptions(t *testing.T) {
	d := startDemoServer(t, serverOpts{})
	if err := run(d.addr(), 0); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// ---- 正常路径 ----

// runWith 走完全部交互原语：订阅 / 请求响应 / 流式 / 双工 / 单向 / 发布，
// 并让服务端反向触发客户端注册的 client.time、client.notice 与 ticks
// 广播回调。
func TestRunFullDemo(t *testing.T) {
	d := startDemoServer(t, serverOpts{})
	addr := d.addr()

	// 服务端侧动作延后到客户端 handler 注册完成之后。
	go func() {
		waitSessions(t, d.Server, 1)
		time.Sleep(60 * time.Millisecond)
		for _, sid := range d.Sessions() {
			_ = d.SendOneWay(sid, "client.notice", map[string]string{"text": "hi"})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = d.Request(ctx, sid, "client.time", nil)
			cancel()
		}
		_ = d.Publish("metrics", map[string]int{"cpu": 1})
		_ = d.Publish("ticks", map[string]int{"tick": 1})
	}()

	if err := runWith(defaultRunOptions(400*time.Millisecond), addr); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// range 路由缺失不会让 Stream 立即失败：Stream 只负责发出 REQUEST，返回的
// ReadStream 要等第一帧才知道结果。示例的读循环因此走 break 分支并继续。
func TestRunStreamNotFound(t *testing.T) {
	d := startDemoServer(t, serverOpts{omit: map[string]bool{"range": true}})
	if err := runWith(defaultRunOptions(0), d.addr()); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// ---- 错误出口 ----

func TestRunDialFailure(t *testing.T) {
	// 取一个端口随即关闭：连接必被拒。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	if err := runWith(defaultRunOptions(0), addr); err == nil {
		t.Fatal("run against a closed port must fail")
	}
}

// run 是顺序执行的，连接死在哪一步之后，就决定了哪个错误出口被走到。
// 这里只用确定性的触发方式：要么服务端直接回错误，要么让客户端卡在一个
// 有已知超时的阻塞点上再掐断连接（不去抢"上一步刚返回、下一步还没发起"
// 那个微秒级窗口——那是概率而非测试）。
func TestRunChannelAfterStreamStall(t *testing.T) {
	d := startDemoServer(t, serverOpts{stallRange: true})
	err := runWith(noReconnect(0), d.addr())
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("run error = %v, want the Channel call to time out on a dead connection", err)
	}
}

// 握手成功、随即发一个连接级 ERROR 帧（Stream ID=0）：协议规定此后双方必须
// 断开，于是 run 的第一个跨端调用（Subscribe）必然失败。这比"掐断 TCP"更
// 确定——不依赖客户端在哪一步察觉死连接。
//
// 失败有两种同样正确的形态：连接已被读循环判死，Subscribe 等不到端点、等到
// 自己的超时上限；或者判死还没发生，Subscribe 的 SUBSCRIBE 帧直接撞上关闭
// 的传输。竞态在"读循环处理 ERROR"与"Subscribe 发帧"之间，所以两种都接受。
func TestRunSubscribeAfterFatalHandshakeError(t *testing.T) {
	addr := startFatalHandshakeListener(t)
	err := runWith(noReconnect(0), addr)
	if err == nil {
		t.Fatal("run must fail after a fatal connection-level error")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("run error = %v, want a dead-connection error", err)
	}
}

// rawFrame 手工编码一个 JsonStream 帧（示例测试里没有库内部的 appendTo 可用，
// 而这里需要的正是"发一个库自己绝不会发的帧"）。
func rawFrame(typ byte, sid uint32, payload []byte) []byte {
	b := make([]byte, 14+len(payload))
	b[0], b[1] = 0x4A, 0x53 // Magic "JS"
	b[2] = 1                // Version
	b[4] = typ
	binary.BigEndian.PutUint32(b[6:10], sid)
	binary.BigEndian.PutUint32(b[10:14], uint32(len(payload)))
	copy(b[14:], payload)
	return b
}

func startFatalHandshakeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// 吃掉 CONNECT，回一个合法 CONNACK 让 Dial 成功……
		if _, err := jsonstream.ReadFrame(bufio.NewReader(c)); err != nil {
			return
		}
		ack, _ := json.Marshal(map[string]any{"session_id": "deadbeef", "heartbeat_ms": 10000})
		if _, err := c.Write(rawFrame(0x02, 0, ack)); err != nil {
			return
		}
		// ……紧接着宣告连接级致命错误（TypeError=0x0A, Stream ID=0）。
		_, _ = c.Write(rawFrame(0x0A, 0, []byte(`{"code":1,"message":"fatal"}`)))
		time.Sleep(2 * time.Second)
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func TestRunRouteNotFound(t *testing.T) {
	d := startDemoServer(t, serverOpts{omit: map[string]bool{"math.add": true}})
	err := runWith(defaultRunOptions(0), d.addr())
	if err == nil || !strings.Contains(err.Error(), "NOT_FOUND") {
		t.Fatalf("run error = %v, want NOT_FOUND", err)
	}
}

func TestRunResponseDecodeError(t *testing.T) {
	d := startDemoServer(t, serverOpts{mathAddReply: `{"sum":"not-a-number"}`})
	if err := runWith(defaultRunOptions(0), d.addr()); err == nil {
		t.Fatal("run must fail when the response cannot be decoded")
	}
}

// 路由缺失时服务端在收到 REQUEST(Channel) 的同一拍就回 ERROR(NOT_FOUND)，
// 而客户端在 c.Channel 返回后立刻 ch.Send——两者谁先到取决于一次回环的
// 调度。所以"run 失败"是确定的，失败落在 Receive（NOT_FOUND）还是 Send
// （流已终结 → closed）不是。两种都接受，这正是流终结语义该有的表现。
func TestRunChannelNotFound(t *testing.T) {
	d := startDemoServer(t, serverOpts{omit: map[string]bool{"chat": true}})
	err := runWith(defaultRunOptions(0), d.addr())
	if err == nil {
		t.Fatal("run against a server without chat must fail")
	}
	if !strings.Contains(err.Error(), "NOT_FOUND") && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("run error = %v, want NOT_FOUND or a closed-stream error", err)
	}
}

func TestRunChannelHandlerError(t *testing.T) {
	d := startDemoServer(t, serverOpts{chatErr: true})
	err := runWith(defaultRunOptions(0), d.addr())
	if err == nil || !strings.Contains(err.Error(), "chat refused") {
		t.Fatalf("run error = %v, want the handler's error", err)
	}
}

// ---- 断线重连回调 ----

// demo 等待期内掐断连接：保留期禁用 → 会话无法恢复，OnReconnect 与
// OnResumeFailed 都应触发（两个回调体在示例里是 log.Printf，需要真的执行）。
func TestRunReconnectCallbacks(t *testing.T) {
	d := startDemoServer(t, serverOpts{retention: -1})
	addr := d.addr()

	// 第二个连接只在重连期间存在（run 返回时客户端已关闭），所以在演示
	// 进行当中观察接入总数，而不是事后查 Sessions()。
	resumed := make(chan struct{}, 1)
	go func() {
		waitSessions(t, d.Server, 1)
		time.Sleep(300 * time.Millisecond)
		d.ln.dropAll()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if d.ln.accepted() >= 2 {
				resumed <- struct{}{}
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	if err := runWith(defaultRunOptions(900*time.Millisecond), addr); err != nil {
		t.Fatalf("run: %v", err)
	}
	select {
	case <-resumed:
	default:
		t.Fatal("client did not reconnect")
	}
}

// ---- main 与命令行 ----

// withArgs 替换 os.Args 与 flag.CommandLine，让 main/parseOptions 可以在
// 测试里被调用（flag 在全局 CommandLine 上重复注册同名 flag 会 panic）。
func withArgs(t *testing.T, args ...string) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	flag.CommandLine = flag.NewFlagSet("jsonstream-client-example", flag.ContinueOnError)
	os.Args = append([]string{"client"}, args...)
}

func TestMainRunsDemo(t *testing.T) {
	d := startDemoServer(t, serverOpts{})
	withArgs(t, "-addr", d.addr(), "-demo", "0")
	main()
}

// main 的失败出口走 log.Fatal（os.Exit），只能在子进程里观察。
func TestMainExitsOnDialFailure(t *testing.T) {
	if os.Getenv("JSONSTREAM_EXAMPLE_MAIN") == "1" {
		withArgs(t, "-addr", "127.0.0.1:1", "-demo", "0")
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsOnDialFailure$")
	cmd.Env = append(os.Environ(), "JSONSTREAM_EXAMPLE_MAIN=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("example client should exit non-zero, output:\n%s", out)
	}
}
