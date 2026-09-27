package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"log"
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
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for _, sid := range d.Sessions() {
			_ = d.SendOneWay(ctx, sid, "client.notice", map[string]string{"text": "hi"})
			_, _ = d.Request(ctx, sid, "client.time", nil)
		}
		_ = d.Publish(ctx, "metrics", map[string]int{"cpu": 1})
		_ = d.Publish(ctx, "ticks", map[string]int{"tick": 1})
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

// ---- 窄竞态错误出口：脚本化线上对端 + 日志点停泊 ----
//
// runWith 的四个错误出口（Stream / ch.Send / SendOneWay / Publish 的
// `return err`）要求连接/流恰好在「上一步成功之后、本步发起之前」的死
// 窗口内终结——真实服务端的错误都要走一整个网络来回（≥微秒级），客户端
// 下一步只需几十纳秒，直接构造是偏斜的硬币。这里用两种测试侧手段，不改
// 示例一行代码：
//
//  1. parkOnLog：示例的 log.Printf 是顺序执行的，在两条业务语句之间把
//     main goroutine 泊在日志写入里，等断连完全发生后再放行——泊点两端
//     是确定的相邻语句，错误出口随之确定（Stream、SendOneWay 两个出口
//     因此完全无竞态）。
//  2. 线上脚本对端（startRawScript）：手工帧序列决定每一拍回什么、何时
//     掐线、掐线后继续排读客户端迟到的帧（作正面证据用）。
//     ch.Send 与 Publish 两个出口仍剩一次微秒级竞态（错误往返 vs 下一
//     步发起），按 design-notes §9 的纪律用「重跑到出现目标签名」处理：
//     每轮几毫秒，预算内全漏检概率压到 2^-20 以下；且两条判据都要求
//     正面证据（见各测试内注释），不会把没走到的分支认成走到了。

// wireFrame 手工编码一帧线上字节（可带元数据）。布局与 appendTo 一致：
// 14B 定长头（payload 长度不含 meta），有 meta 时再跟 2B metaLen + meta。
func wireFrame(typ jsonstream.FrameType, flags uint8, sid uint32, meta, payload string) []byte {
	var mb []byte
	if meta != "" {
		mb = []byte(meta)
		flags |= uint8(jsonstream.FlagHasMeta)
	}
	pb := []byte(payload)
	n := 14 + len(pb)
	if meta != "" {
		n += 2 + len(mb)
	}
	b := make([]byte, n)
	b[0], b[1] = 0x4A, 0x53
	b[2] = 1 // ProtocolVersion
	b[3] = flags
	b[4] = byte(typ)
	binary.BigEndian.PutUint32(b[6:10], sid)
	binary.BigEndian.PutUint32(b[10:14], uint32(len(pb)))
	if meta != "" {
		binary.BigEndian.PutUint16(b[14:16], uint16(len(mb)))
		copy(b[16:], mb)
		copy(b[16+len(mb):], pb)
	} else {
		copy(b[14:], pb)
	}
	return b
}

// rawScript 是脚本化对端的单连接会话：Read 按序读一帧，WriteB 写原始字节，
// Kill 半关闭（发 FIN、之后仍可排读对端迟到的帧），WaitClosed 等 FIN。
type rawScript struct {
	c      net.Conn
	br     *bufio.Reader
	closed chan struct{}
}

func (r *rawScript) Read(tb testing.TB) *jsonstream.Frame {
	tb.Helper()
	_ = r.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := jsonstream.ReadFrame(r.br)
	if err != nil {
		tb.Fatalf("raw script: read frame: %v", err)
	}
	return f
}

func (r *rawScript) WriteB(b []byte) {
	_, _ = r.c.Write(b)
}

func (r *rawScript) Kill() { _ = r.c.(*net.TCPConn).CloseWrite() }

func (r *rawScript) WaitClosed(tb testing.TB) {
	tb.Helper()
	select {
	case <-r.closed:
	case <-time.After(5 * time.Second):
		tb.Fatal("raw script: client never closed the connection")
	}
}

// startRawScript 起一个只服务一次接入的脚本化服务端。脚本返回即关连接；
// t.Cleanup 兜底（测试失败中途离开时强制放行 Kill/WaitClosed 的等待）。
func startRawScript(t *testing.T, script func(*rawScript)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	addr := ln.Addr().String()
	connCh := make(chan net.Conn, 1)
	failCh := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(failCh)
			return
		}
		connCh <- c
	}()
	t.Cleanup(func() {
		select {
		case <-failCh:
			t.Fatal("raw script: no connection was accepted")
		default:
		}
	})
	go func() {
		select {
		case c := <-connCh:
			s := &rawScript{c: c, br: bufio.NewReader(c), closed: make(chan struct{})}
			script(s)
			_ = c.Close()
			close(s.closed)
		case <-failCh:
		}
	}()
	return addr
}

// runIn 在独立 goroutine 里跑 runWith，返回结果通道（配合 parkOnLog 使用：
// 测试主 goroutine 需要观察 parked 信号，不能被 runWith 占住）。
func runIn(o runOptions, addr string) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- runWith(o, addr) }()
	return ch
}

// parkedRun 把「runWith 起线程 + 等泊点命中」两步合并：返回 runWith 的
// 结果通道。日志匹配走字符串包含，多解一条无害、少解一条致命，所以
// 模式一律取保守的子串。
func parkedRun(t *testing.T, o runOptions, addr, mark string) (<-chan error, func()) {
	t.Helper()
	parked, release := parkOnLog(t, mark)
	res := runIn(o, addr)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		release()
		t.Fatalf("example client never reached log point %q", mark)
	}
	return res, release
}

// killSignal 是「脚本何时掐线」的测试侧门闩：脚本在回完当前这一步该回的
// 帧后阻塞等信号，测试观察到泊点命中才放行掐线。没有这道门闩，掐线与客
// 户端到达下一步之间又剩下微秒级竞态（FIN 传播可能输给 waitEp 的存活检
// 查）；有了它，泊点→掐线→放行是严格串行的。
func killSignal(t *testing.T) (wait <-chan struct{}, fire func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	fire = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(fire) // 测试中途失败时也不吊住脚本 goroutine
	return ch, fire
}

// parkedKill 组合完整的确定性剧本：起 runWith → 泊在 mark → 掐线 →
// 留足判死传播时间（环回 FIN ≪ 毫秒级，50ms 已是千倍余量）→ 放行 → 返回
// runWith 的错误。
func parkedKill(t *testing.T, o runOptions, addr, mark string, fireKill func()) error {
	t.Helper()
	res, release := parkedRun(t, o, addr, mark)
	fireKill()
	time.Sleep(50 * time.Millisecond)
	release()
	return <-res
}

// parkOnLog 把 std log 的输出换成一个条件拦停器：写到包含 mark 的那一行时
// 阻塞调用方 goroutine（log.Output 单行单 Write，示例的 main 就停在两条
// 业务语句之间），release 放行。其余写入一律吞掉。库自身的日志走
// Config.Logger（默认 discard），不经过这里，不会被误拦。
// 纪律：每个测试只泊一次，release 必须在断连/回包之后、再次触碰 runWith
// 之前调用（或留给 Cleanup 兜底）。
func parkOnLog(t *testing.T, mark string) (parked <-chan struct{}, release func()) {
	t.Helper()
	old := log.Writer()
	parkedCh := make(chan struct{})
	var closeOnce sync.Once
	resume := make(chan struct{})
	var resumeOnce sync.Once
	log.SetOutput(parkWriter{f: func(p []byte) {
		if bytes.Contains(p, []byte(mark)) {
			closeOnce.Do(func() { close(parkedCh) })
			<-resume
		}
	}})
	t.Cleanup(func() {
		log.SetOutput(old)
		resumeOnce.Do(func() { close(resume) })
	})
	return parkedCh, func() { resumeOnce.Do(func() { close(resume) }) }
}

type parkWriter struct{ f func([]byte) }

func (w parkWriter) Write(p []byte) (int, error) { w.f(p); return len(p), nil }

// handshakeOK 完成 Dial 所需的握手（CONNECT→CONNACK）。
func handshakeOK(s *rawScript) {
	s.WriteB(wireFrame(jsonstream.TypeConnAck, 0, 0, "", `{"session_id":"deadbeef","heartbeat_ms":10000}`))
}

// demoUpToMath 走通「订阅 + 请求/响应」两段（脚本侧的固定前奏），返回
// math 请求的 StreamID（客户端按 1,3,5,… 奇数递增分配，但脚本按实际帧走）。
func demoUpToMath(t *testing.T, s *rawScript) {
	t.Helper()
	s.Read(t) // CONNECT
	handshakeOK(s)
	f := s.Read(t) // SUBSCRIBE(ticks)
	s.WriteB(wireFrame(jsonstream.TypeSubAck, 0, f.StreamID, "", ""))
	f = s.Read(t) // REQUEST(math.add)
	s.WriteB(wireFrame(jsonstream.TypeResponse, 0, f.StreamID, "", `{"sum":42}`))
}

// ---- 出口一：Stream 的错误返回（runWith 第 117 行） ----

// 泊在 math.add 结果日志处（紧接 Stream 之前）掐线：放行后 waitEp 面对
// 已判死的连接，等满 300ms 上限，错误只能从 Stream 这一步返回。泊点两侧
// 是相邻语句，没有竞态可言。
func TestRunStreamStepDiesAfterMathSucceeded(t *testing.T) {
	kill, fire := killSignal(t)
	addr := startRawScript(t, func(s *rawScript) {
		demoUpToMath(t, s)
		<-kill
		s.Kill()
		s.WaitClosed(t)
	})
	err := parkedKill(t, noReconnect(0), addr, "math.add(2, 40)", fire)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("run error = %v, want the Stream step to fail on the dead connection", err)
	}
}

// ---- 出口二：ch.Send 的错误返回（runWith 第 142 行） ----

// chat 路由不存在：服务端对 REQUEST(chat) 回流级 ERROR(NOT_FOUND)。客户端
// Channel() 只负责发出 REQUEST 就返回，紧接着 ch.Send 先查流是否终结——
// 这条几十纳秒的本地检查与 ERROR 的网络来回赛跑，是偏斜的硬币；单次探测
// 实测命中率只有约 0.2%（示例内部从发出 CANCEL 到完成检查的链条快过一个
// 回程），不足以支撑重跑纪律。把硬币做"厚"的办法是盲发连发：客户端按
// 1,3,5,7… 递增分配奇数号，chat 恒为 sid=7；脚本在回完 range 的两条数据后
// 立刻连发 2048 帧 ERROR(sid=7)——chat 流注册前到达的帧在 lookupFlow 处被
// 静默忽略，注册后到达的第一帧就终结该流；连发的尾段恰好横贯注册时刻，
// 而 readLoop 逐帧处理的间隔（微秒级）小于注册到 isDone 检查之间的窗口，
// 于是几乎必有一帧落进窗口。实测命中率 ≥80%（-race 下同样成立），取 5%
// 的保守下界，700 轮漏检概率 0.95^700 = e^-36 ≪ 2^-20（design-notes §9
// 的重跑纪律）。判据保持排除法唯一：连接全程活着，runWith 里能产出裸
// "connection closed" 的出口只有 ch.Send 的流终结检查一处——NOT_FOUND 落在
// Receive（文本含 "NOT_FOUND(2)"），别的步骤都要求连接判死而本场景从不
// 掐线；chat 的兜底 NOT_FOUND 回复保证每轮 run 必失败，漏检轮只可能以
// NOT_FOUND 面貌出现，绝不会出现成功轮。
func TestRunChannelSendWinsStreamErrorRace(t *testing.T) {
	probe := bytes.Repeat(wireFrame(jsonstream.TypeError, 0, 7, "", `{"code":2,"message":"chat race probe"}`), 2048)
	// 脚本：走通前奏后盲发连发 ERROR(sid=7)。此刻 range 还没终结、
	// chat 尚未注册，probe 帧会被 lookupFlow 逐个忽略；一次 WriteB 即
	// 全部入队，尾段横贯之后的注册时刻。剩下的是对 REQUEST(chat) 的
	// 兜底 NOT_FOUND 回复（漏检轮靠它保证 run 必失败、且以 NOT_FOUND
	// 面貌失败）。命中轮里客户端在 ch.Send 报错后随即退出、关闭连接，
	// 脚本的读会以 EOF/复位告终——脚本 goroutine 常活过测试函数，那时
	// 调用 t.Fatalf 会变成 panic，所以尾部读失败必须静默（不走 Read）。
	script := func(s *rawScript) {
		demoUpToMath(t, s)
		f := s.Read(t) // REQUEST(range)
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":0}`))
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":1}`))
		s.WriteB(probe)
		_ = s.c.SetReadDeadline(time.Now().Add(2 * time.Second))
		for i := 0; i < 2; i++ { // 依序：CANCEL(range)、REQUEST(chat)
			f2, err := jsonstream.ReadFrame(s.br)
			if err != nil {
				return
			}
			if f2.Type == jsonstream.TypeRequest {
				s.WriteB(wireFrame(jsonstream.TypeError, 0, f2.StreamID, "", `{"code":2,"message":"no handler for channel chat"}`))
			}
		}
	}
	const maxRounds = 700
	for round := 1; round <= maxRounds; round++ {
		addr := startRawScript(t, script)
		err := runWith(defaultRunOptions(0), addr)
		if err == nil {
			t.Fatalf("round %d: run must fail (chat route never served)", round)
		}
		msg := err.Error()
		if strings.Contains(msg, "closed") && !strings.Contains(msg, "NOT_FOUND") {
			return // 目标出口命中
		}
		if !strings.Contains(msg, "NOT_FOUND") {
			t.Fatalf("round %d: unexpected error %v (want NOT_FOUND or the Send-arm's closed)", round, err)
		}
	}
	t.Fatalf("after %d rounds the ch.Send error arm never won the race", maxRounds)
}

// ---- 出口三：SendOneWay 的错误返回（runWith 第 162 行） ----

// 泊在 "chat ack"（紧接 Close/SendOneWay 之前）掐线：放行后 ch.Close 的
// 错误按例被丢弃（幂等半关闭），SendOneWay 的 waitEp 面对死连接等满上限。
// 泊点与出口之间只有无阻塞语句，错误出口确定。
func TestRunSendOneWayDiesAfterChatAck(t *testing.T) {
	kill, fire := killSignal(t)
	addr := startRawScript(t, func(s *rawScript) {
		demoUpToMath(t, s)
		f := s.Read(t) // REQUEST(range)
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":0}`))
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":1}`))
		s.WriteB(wireFrame(jsonstream.TypeComplete, 0, f.StreamID, "", ""))
		f = s.Read(t) // REQUEST(chat)：通道建立不等待回执
		s.Read(t)     // 客户端 ch.Send 的 ping（RESPONSE 帧，双工数据）
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream|jsonstream.FlagChannel), f.StreamID, "", `{"ack":"got: ping"}`))
		<-kill
		s.Kill()
		s.WaitClosed(t)
	})
	err := parkedKill(t, noReconnect(0), addr, "chat ack:", fire)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("run error = %v, want SendOneWay to fail on the dead connection", err)
	}
}

// ---- 出口四：Publish 的错误返回（runWith 第 171 行） ----

// 该出口要求「SendOneWay 已返回 nil，Publish 随后撞上死连接」。这个时刻
// 用纯时序竞态抓不住：掐线锚在 chat ack 回包后的 δ 空转上扫过
// δ∈{0…480µs} 共 90 轮，没有一轮出现「判死报错且 ONEWAY 在线上」的形态
// ——判死链路（FIN→readLoop→kill）与示例主 goroutine 的剩余链条并发推
// 进，要么输给两次入队（全程成功），要么赢过两次入队（死在第 162 行）。
// 退一步用 beforePublish 钩子在两点之间掐线后，还剩一条隐蔽竞态：写循环
// 把迟到的 ONEWAY 推上线与 kill 关闭 socket 也在赛跑（-race 下实测约
// 1/8 轮次输掉它，帧就此消失）。所以掐线时刻不猜、改锚定在正面证据上：
// 脚本收到 ONEWAY 帧（此刻它必然已在线上——收到即证明）才 CloseWrite，
// 并向钩子确认已掐；钩子（在示例主 goroutine 里、Publish 之前）等这个
// 确认再加 20ms 判死传播余量才返回。于是 Publish 的 waitEp 面对死连接
// 等满 callTTL（确定性报错），而「上一出口已通过」由对端数到 ONEWAY 帧
// 本身支撑——全链条没有未锚定的赛跑。
func TestRunPublishDiesBetweenSendOneWayAndPublish(t *testing.T) {
	killWait, killDone := killSignal(t)
	var mu sync.Mutex
	var sawOneway, sawPublish int
	addr := startRawScript(t, func(s *rawScript) {
		demoUpToMath(t, s)
		f := s.Read(t) // REQUEST(range)
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":0}`))
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream), f.StreamID, "", `{"n":1}`))
		s.WriteB(wireFrame(jsonstream.TypeComplete, 0, f.StreamID, "", ""))
		f = s.Read(t) // REQUEST(chat)
		s.Read(t)     // 客户端 ch.Send 的 ping
		s.WriteB(wireFrame(jsonstream.TypeResponse, uint8(jsonstream.FlagStream|jsonstream.FlagChannel), f.StreamID, "", `{"ack":"got: ping"}`))
		for {
			_ = s.c.SetReadDeadline(time.Now().Add(2 * time.Second))
			f2, err := jsonstream.ReadFrame(s.br)
			if err != nil {
				return // 对端关闭或读超时：计数定局
			}
			mu.Lock()
			switch f2.Type {
			case jsonstream.TypeOneWay:
				sawOneway++
			case jsonstream.TypePublish:
				sawPublish++
			}
			mu.Unlock()
			if f2.Type == jsonstream.TypeOneWay {
				s.Kill() // 正面证据到手才掐线
				killDone()
			}
		}
	})
	o := noReconnect(0)
	o.beforePublish = func() {
		select {
		case <-killWait:
		case <-time.After(2 * time.Second): // 脚本始终没数到 ONEWAY：不挂死，
		} // 让 Publish 照常走，下面的断言以清晰方式失败
		time.Sleep(20 * time.Millisecond) // FIN→判死传播余量（与 parkedKill 同纪律）
	}
	err := runWith(o, addr)
	mu.Lock()
	oneway, publish := sawOneway, sawPublish
	mu.Unlock()
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("run error = %v, want Publish to fail on the dead connection", err)
	}
	if oneway != 1 {
		t.Fatalf("SendOneWay frame on wire = %d, want 1（上一出口须有正面证据）", oneway)
	}
	if publish != 0 {
		t.Fatal("Publish 帧到达了对端——Publish 出口未被走到")
	}
}
