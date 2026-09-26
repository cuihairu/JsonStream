package main

import (
	"context"
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

// 测试脚手架：serve 会在随机端口上起一个真实服务端，测试用注入了的监听器
// 拿到确定地址，再拿真实客户端把五种交互原语逐个走一遍——示例的 handler
// 就是被这样验证的。

// runningServer 是一次运行中的 serve。
type runningServer struct {
	addr string
	err  error
	done chan struct{}
}

// startServe 起一个后台 serve，返回控制句柄；stop 即 ctx 取消。
func startServe(t *testing.T, tune func(*options)) *runningServer {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	o := options{
		ln:             raw,
		cfg:            jsonstream.DefaultConfig(),
		broadcastEvery: 20 * time.Millisecond,
		notifyEvery:    20 * time.Millisecond,
		notifyTimeout:  2 * time.Second,
	}
	if tune != nil {
		tune(&o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rs := &runningServer{addr: raw.Addr().String(), done: make(chan struct{})}
	go func() {
		defer close(rs.done)
		rs.err = serve(ctx, o)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-rs.done:
		case <-time.After(3 * time.Second):
			t.Error("serve did not return after ctx cancel")
		}
	})
	return rs
}

// dial 连到运行中的示例服务端。
func (rs *runningServer) dial(t *testing.T) *jsonstream.Client {
	t.Helper()
	cfg := jsonstream.DefaultConfig()
	cfg.Heartbeat = time.Second
	c, err := jsonstream.Dial(context.Background(), rs.addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// defaultOptions 是 main 会构造的那份 options（把循环节奏调快，供测试用）。
func defaultOptions() options {
	return options{
		cfg:            jsonstream.DefaultConfig(),
		broadcastEvery: 20 * time.Millisecond,
		notifyEvery:    20 * time.Millisecond,
		notifyTimeout:  time.Second,
	}
}

// ---- serve 生命周期 ----

func TestServeStopsOnContextCancel(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Request(ctx, "math.add", map[string]int{"a": 1, "b": 2}); err != nil {
		t.Fatalf("math.add: %v", err)
	}
}

func TestServeListenError(t *testing.T) {
	o := defaultOptions()
	o.addr = "127.0.0.1:not-a-port"
	if err := serve(context.Background(), o); err == nil {
		t.Fatal("serve must fail on an unusable listen address")
	}
}

func TestServeConfigError(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	o := defaultOptions()
	o.ln = raw
	o.cfg.Encrypt = true
	o.cfg.Key = []byte("too-short")
	if err := serve(context.Background(), o); err == nil {
		t.Fatal("serve must fail when the config is rejected")
	}
}

// 监听器被外部关掉时 Accept 直接报错（不走 srv.Close 的正常停机路径），
// serve 因此返回错误，后台停机 goroutine 走 stopped 分支而不是 ctx 分支。
func TestServeReturnsWhenListenerDies(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	o := defaultOptions()
	o.ln = raw
	done := make(chan error, 1)
	go func() { done <- serve(context.Background(), o) }()

	// 关监听器的时机与 serve 的进度无关紧要：无论 Close 落在 Serve 的
	// Accept 之前还是之后，Accept 都会立即报错返回——测试对两种顺序
	// 一视同仁，所以不做任何同步。
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("serve should report the accept failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("serve did not return after the listener died")
	}
}

// ---- 路由：请求/响应 ----

func TestMathAddAndDecodeError(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var sum struct {
		Sum int `json:"sum"`
	}
	m, err := c.Request(ctx, "math.add", map[string]int{"a": 40, "b": 2})
	if err != nil {
		t.Fatalf("math.add: %v", err)
	}
	if err := m.Decode(&sum); err != nil || sum.Sum != 42 {
		t.Fatalf("sum = %d (err %v), want 42", sum.Sum, err)
	}

	// 载荷不是 JSON 对象 → handler 的 Decode 失败，回 INVALID。
	if _, err := c.Request(ctx, "math.add", "not-an-object"); err == nil ||
		!strings.Contains(err.Error(), "INVALID") {
		t.Fatalf("bad payload error = %v, want INVALID", err)
	}
}

func TestRouteNotFound(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Request(ctx, "no.such.route", nil); err == nil {
		t.Fatal("unknown route must fail")
	}
}

// ---- 路由：流式 ----

func TestRangeStream(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s, err := c.Stream(ctx, "range", map[string]int{"n": 3})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	for i := 0; i < 3; i++ {
		msg, ok := s.Next(ctx)
		if !ok {
			t.Fatalf("item %d: %v", i, s.Err())
		}
		var item struct {
			N int `json:"n"`
		}
		if err := msg.Decode(&item); err != nil || item.N != i {
			t.Fatalf("item %d = %+v (err %v)", i, item, err)
		}
	}
	if _, ok := s.Next(ctx); ok {
		t.Fatal("stream should be complete after 3 items")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
}

func TestRangeErrors(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 载荷不是对象 → handler 的 Decode 失败。
	s, err := c.Stream(ctx, "range", "not-an-object")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Next(ctx); ok || s.Err() == nil {
		t.Fatal("stream with a bad payload must fail")
	}

	// n 越界 → handler 主动回 INVALID。
	s2, err := c.Stream(ctx, "range", map[string]int{"n": 100000})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Next(ctx); ok || !strings.Contains(errString(s2.Err()), "INVALID") {
		t.Fatalf("oversized n error = %v, want INVALID", s2.Err())
	}
}

func TestRangeCancelStopsHandler(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// n=1000 会被 50ms/帧的节奏拖很久，中途取消后 handler 的 Emit 必须
	// 立刻拿到错误（这条断言同时验证了示例没有把错误吞掉）。
	s, err := c.Stream(ctx, "range", map[string]int{"n": 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Next(ctx); !ok {
		t.Fatalf("first item: %v", s.Err())
	}
	_ = s.Cancel()
	if _, ok := s.Next(ctx); ok {
		t.Fatal("cancelled stream must not deliver more items")
	}
	// handler 每帧之间 sleep 50ms，它要到下一次 Emit 才会发现流已取消。
	// 那个错误分支发生在服务端 goroutine 里，只能靠等它发生来覆盖。
	time.Sleep(200 * time.Millisecond)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ---- 路由：双工 / 单向 / 发布 ----

func TestChatChannel(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := c.Channel(ctx, "chat", nil)
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := ch.Send(map[string]string{"text": "hi"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	ack, err := ch.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	var got struct {
		Ack string `json:"ack"`
	}
	if err := ack.Decode(&got); err != nil || got.Ack != "got: hi" {
		t.Fatalf("ack = %+v (err %v)", got, err)
	}
	_ = ch.Close()
	// Close 是半关闭：服务端阻塞中的 Receive 会拿到 io.EOF 并正常收尾。
	// 那条分支在服务端 handler 的 goroutine 里跑，测试结束得比它早，
	// 所以显式等一会儿（协议不回执，无从观测）。
	time.Sleep(200 * time.Millisecond)
}

func TestChatHandlerErrors(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 载荷不是对象 → handler 的 Decode 失败。
	bad, err := c.Channel(ctx, "chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Send("not-an-object"); err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Receive(ctx); err == nil {
		t.Fatal("chat handler must fail on a non-object payload")
	}

	// 直接退订（Cancel）→ handler 阻塞中的 Receive 拿到错误。
	cancelled, err := c.Channel(ctx, "chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = cancelled.Cancel()
	// 与上面同理：服务端的 Receive 返回发生在它自己的 goroutine 里。
	time.Sleep(200 * time.Millisecond)
}

func TestOneWayAndPublish(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	if err := c.SendOneWay("notify", map[string]string{"text": "ping"}); err != nil {
		t.Fatalf("oneway: %v", err)
	}
	if err := c.Publish("metrics", map[string]int{"cpu": 7}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// ONEWAY/PUBLISH 的 handler 各自跑在独立 goroutine 里（协议不回执），
	// 测试结束得比它们晚，所以显式等一会儿。
	time.Sleep(200 * time.Millisecond)
}

// ---- 后台循环 ----

// ticks 广播与客户端侧订阅回调：serve 起来后 broadcastLoop 会周期性发布。
func TestBroadcastReachesSubscriber(t *testing.T) {
	rs := startServe(t, nil)
	c := rs.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var mu sync.Mutex
	var seen []string
	sub, err := c.Subscribe(ctx, "ticks", func(m *jsonstream.Message) error {
		mu.Lock()
		seen = append(seen, string(m.Payload))
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	waitFor(t, "a ticks broadcast", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) > 0
	})
}

// notifyLoop：服务端主动发通知并回读时间。客户端实现了 client.time 时成功，
// 不实现时 notify 返回错误（覆盖 Request 的失败分支）。
func TestNotifyLoopSuccessAndRequestError(t *testing.T) {
	rs := startServe(t, nil)

	good := rs.dial(t)
	good.Handle("client.time", func(*jsonstream.Request) (any, error) {
		return map[string]string{"now": "noon"}, nil
	})
	var mu sync.Mutex
	var notices int
	good.HandleOneWay("client.notice", func(*jsonstream.Message) error {
		mu.Lock()
		notices++
		mu.Unlock()
		return nil
	})
	waitFor(t, "a client.notice oneway", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return notices > 0
	})

	// 第二个客户端什么都不注册：notify 周期（20ms）运行期间它在线，
	// Request 必然 NOT_FOUND——错误分支发生在 serve 的 goroutine 里，
	// 这里只需保证它的握手已完成。
	bare := rs.dial(t)
	waitFor(t, "the bare client's handshake", func() bool {
		return len(bare.SessionID()) > 0
	})
}

// notify 直接调用：未知会话 → SendOneWay 失败。
func TestNotifyUnknownSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := jsonstream.NewServer(ln, jsonstream.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if err := notify(srv, "no-such-session", time.Second); err == nil {
		t.Fatal("notify to an unknown session must fail")
	}
}

// ---- main 与命令行 ----

// withArgs 替换 os.Args 与 flag.CommandLine，让 main/parseOptions 可以在
// 测试里被调用（flag 在全局 CommandLine 上重复注册同名 flag 会 panic）。
func withArgs(t *testing.T, args ...string) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	flag.CommandLine = flag.NewFlagSet("jsonstream-server-example", flag.ContinueOnError)
	os.Args = append([]string{"server"}, args...)
}

func TestParseOptionsDefaults(t *testing.T) {
	withArgs(t)
	o := parseOptions()
	if o.addr != "127.0.0.1:9000" || o.broadcastEvery != 2*time.Second || o.notifyEvery != 3*time.Second {
		t.Fatalf("parseOptions defaults = %+v", o)
	}
	if o.ln != nil {
		t.Fatal("parseOptions must not inject a listener")
	}
	if o.notifyTimeout != 5*time.Second {
		t.Fatalf("notifyTimeout = %v, want 5s", o.notifyTimeout)
	}
	if o.cfg.Heartbeat != jsonstream.DefaultHeartbeat {
		t.Fatalf("cfg heartbeat = %v, want the library default", o.cfg.Heartbeat)
	}
}

func TestParseOptionsFlags(t *testing.T) {
	withArgs(t, "-addr", "127.0.0.1:1234", "-broadcast", "10ms", "-notify", "20ms")
	o := parseOptions()
	if o.addr != "127.0.0.1:1234" || o.broadcastEvery != 10*time.Millisecond || o.notifyEvery != 20*time.Millisecond {
		t.Fatalf("parseOptions = %+v", o)
	}
}

// main 阻塞在 serve 上（正常的长跑行为），所以在后台起、验证它真的服务，
// 进程退出时随之结束。
func TestMainServes(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := raw.Addr().String()
	raw.Close()
	withArgs(t, "-addr", addr, "-broadcast", "20ms", "-notify", "1h")
	go main()

	cfg := jsonstream.DefaultConfig()
	cfg.Heartbeat = time.Second
	var c *jsonstream.Client
	waitFor(t, "the example server to accept", func() bool {
		var derr error
		c, derr = jsonstream.Dial(context.Background(), addr, cfg)
		return derr == nil
	})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Request(ctx, "math.add", map[string]int{"a": 20, "b": 22}); err != nil {
		t.Fatalf("math.add against the example server: %v", err)
	}
}

// main 的失败出口走 log.Fatal（os.Exit），只能在子进程里观察。
func TestMainExitsOnListenFailure(t *testing.T) {
	if os.Getenv("JSONSTREAM_EXAMPLE_MAIN") == "1" {
		withArgs(t, "-addr", "127.0.0.1:not-a-port")
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsOnListenFailure$")
	cmd.Env = append(os.Environ(), "JSONSTREAM_EXAMPLE_MAIN=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("example server should exit non-zero, output:\n%s", out)
	}
}
