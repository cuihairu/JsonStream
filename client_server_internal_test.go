package jsonstream

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// ---- 通用辅助 ----

// waitFor 轮询直到条件成立，超时 fatal。
func waitFor(t *testing.T, desc string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met in time: %s", desc)
}

// clientEp 在锁下读取客户端当前 endpoint。
func clientEp(c *Client) *endpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ep
}

// startRawListener 脚本化验收：每个接入连接交给 handle 处理（raw 握手矩阵）。
func startRawListener(t *testing.T, handle func(c net.Conn)) net.Addr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr()
}

// rawConn 是 raw 握手测试用的对端连接。
type rawConn struct {
	c  net.Conn
	br *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawConn{c: c, br: bufio.NewReader(c)}
}

func (r *rawConn) write(f *Frame) error {
	buf, err := f.appendTo(nil)
	if err != nil {
		return err
	}
	_ = r.c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err = r.c.Write(buf)
	return err
}

func (r *rawConn) read(t *testing.T) *Frame {
	t.Helper()
	_ = r.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := ReadFrame(r.br)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

// stubGCM 构造 gcmNew 接缝的桩：第 failCall 次调用注入错误，其余走真实现
// （真实现需要真实 cipher.Block，不能拿 nil 去调）。
func stubGCM(t *testing.T, key []byte, failCall int) {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	real, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	orig := gcmNew
	calls := 0
	gcmNew = func(cipher.Block) (cipher.AEAD, error) {
		calls++
		if calls == failCall {
			return nil, errors.New("gcm injected")
		}
		return real, nil
	}
	t.Cleanup(func() { gcmNew = orig })
}

// CONNECT 编码失败（接缝注入）→ establish 首步报错。
//
// 顺序约束：jsonMarshal 桩必须先于一切可能泄漏「尚未读桩的握手
// goroutine」的 Dial 测试执行（本文件是字母序最早的测试文件，桩测试
// 置于文件首即满足；先写后读经 goroutine spawn 边天然有序）。桩写入
// 与泄漏读侧之间不存在 happens-before，race detector 按向量时钟判定，
// 与墙钟无关——不要把会泄漏握手 goroutine 的测试挪到本文件之前。
func TestClientHandshakeConnectMarshalError(t *testing.T) {
	addr := startRawListener(t, func(net.Conn) {})
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal injected") }
	defer func() { jsonMarshal = orig }()
	mustFailDial(t, addr, shortConfig(), "marshal error must abort handshake")
}

// mustConnack 对编码失败只能 panic（connackJSON 编码不会失败的不变量）。
func TestMustConnackPanicsOnMarshalError(t *testing.T) {
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal injected") }
	defer func() { jsonMarshal = orig }()

	defer func() {
		if recover() == nil {
			t.Fatal("mustConnack must panic on marshal failure")
		}
	}()
	mustConnack(&connackJSON{})
}

// ---- client：构造与 Dial ----

// Dial 的前置变换器构造失败（接缝注入）。
func TestDialTransformerConstructionError(t *testing.T) {
	stubGCM(t, testKey(), 1)
	cfg := shortConfig()
	cfg.Encrypt = true
	cfg.Key = testKey()
	if _, err := Dial(context.Background(), "127.0.0.1:1", cfg); err == nil {
		t.Fatal("expected Dial to fail on transformer construction")
	}
}

// 握手等待被 ctx 打断：Dial 返回 ctx.Err() 并关闭客户端。
func TestDialContextCancel(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		time.Sleep(5 * time.Second) // 装聋作哑：握手停在等 CONNACK
	})
	cfg := shortConfig()
	cfg.DialTimeout = 300 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := Dial(ctx, addr.String(), cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial = %v, want context.Canceled", err)
	}
	// 等 connectLoop 随握手 deadline 自行退出，避免其残留的 jsonMarshal
	// 读取与后续测试的全局接缝桩竞争
	time.Sleep(400 * time.Millisecond)
}

// 首连即失败（连接拒绝）必须直接抛错，不进入重连循环。
func TestDialConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := ln.Addr().String()
	ln.Close()
	if _, err := Dial(context.Background(), refused, shortConfig()); err == nil {
		t.Fatal("expected dial failure against closed listener")
	}
}

// connectLoop 在循环顶端发现已关闭：直接终结握手并退出。
func TestConnectLoopStopsWhenClosed(t *testing.T) {
	c := &Client{
		cfg:       shortConfig(),
		addr:      "127.0.0.1:1",
		table:     newRouteTable(),
		subs:      make(map[uint32]*Subscription),
		notify:    make(chan struct{}, 1),
		handshake: make(chan error, 1),
		closed:    make(chan struct{}),
	}
	close(c.closed)
	go c.connectLoop()
	if err := <-c.handshake; !errors.Is(err, ErrClosed) {
		t.Fatalf("handshake = %v, want ErrClosed", err)
	}
}

// ---- client：握手矩阵（raw 服务端脚本） ----

func mustFailDial(t *testing.T, addr net.Addr, cfg Config, why string) {
	t.Helper()
	if _, err := Dial(context.Background(), addr.String(), cfg); err == nil {
		t.Fatalf("expected Dial to fail: %s", why)
	}
}

// CONNECT 写不进 socket（对端装聋 + 写超时）→ writeOnce 失败。
func TestClientHandshakeWriteTimeout(t *testing.T) {
	addr := startRawListener(t, func(net.Conn) { time.Sleep(5 * time.Second) })
	cfg := shortConfig()
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.Auth = strings.Repeat("x", 1<<20) // 1MiB 载荷必然塞满回环缓冲
	mustFailDial(t, addr, cfg, "write timeout must abort handshake")
}

// 对端收下 CONNECT 后直接断开 → 读到 EOF。
func TestClientHandshakeEOF(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		_, _ = ReadFrame(br)
	})
	mustFailDial(t, addr, shortConfig(), "closed handshake must fail")
}

// CONNACK 载荷不是 JSON → INVALID。
func TestClientHandshakeBadConnackPayload(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if f, err := ReadFrame(br); err != nil || f.Type != TypeConnect {
			return
		}
		_ = writeOnce(c, &Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnAck}, Payload: []byte("notjson")})
		time.Sleep(100 * time.Millisecond)
	})
	mustFailDial(t, addr, shortConfig(), "bad CONNACK payload must fail")
}

// 握手期收到非握手帧 → 协议违规。
func TestClientHandshakeUnexpectedFrame(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if f, err := ReadFrame(br); err != nil || f.Type != TypeConnect {
			return
		}
		_ = writeOnce(c, &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}})
		time.Sleep(100 * time.Millisecond)
	})
	mustFailDial(t, addr, shortConfig(), "unexpected frame must fail handshake")
}

// bindSession 的变换器构造失败（CONNACK 声明加密后的装配路径）。
func TestClientBindTransformerError(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if f, err := ReadFrame(br); err != nil || f.Type != TypeConnect {
			return
		}
		aj, _ := connackFrame(&connackJSON{SessionID: "s", Encrypt: true, HeartbeatMS: 1000})
		_ = writeOnce(c, aj)
		time.Sleep(100 * time.Millisecond)
	})
	cfg := shortConfig()
	cfg.Encrypt = true
	cfg.Key = testKey()
	stubGCM(t, cfg.Key, 2) // 第 1 次：Dial 前置检查；第 2 次：bindSession 装配
	_, err := Dial(context.Background(), addr.String(), cfg)
	if err == nil || !strings.Contains(err.Error(), "gcm injected") {
		t.Fatalf("bindSession transformer error = %v", err)
	}
}

// ---- client：重连路径 ----

// 重连矩阵：连接得而复失 → establish 失败重试 → 拨号失败重试 → 关闭退出。
func TestClientReconnectRetryPaths(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				f, err := ReadFrame(br)
				if err != nil || f.Type != TypeConnect {
					return
				}
				aj, _ := connackFrame(&connackJSON{SessionID: "s1", HeartbeatMS: 1000})
				if err := writeOnce(c, aj); err != nil {
					return
				}
				time.Sleep(200 * time.Millisecond) // 持有连接，随后关闭触发重连
			}()
		}
	}()

	c, err := Dial(context.Background(), ln.Addr().String(), shortConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	waitFor(t, "首连会话建立", func() bool { return c.SessionID() == "s1" })

	// 服务端持有到期自动断开：客户端应自动重连（每轮握手后即被断开，
	// 触发 establish 失败退避重试路径），随后关闭监听制造拨号失败。
	time.Sleep(700 * time.Millisecond)
	ln.Close()
	time.Sleep(400 * time.Millisecond)

	// 关闭后一切发起 API 立即以 ErrClosed 终结
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Request(ctx, "r", item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("request after close = %v, want ErrClosed", err)
	}
}

// 已建立会话后握手失败：connectLoop 走「日志 + 退避 + 续期重拨」的重试
// 路径（区别于首连失败直接上抛）；Close 打断退避使循环收敛退出。
func TestClientHandshakeRetryAfterEstablished(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		n := 0
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			n++
			go func(cn net.Conn, idx int) {
				defer cn.Close()
				br := bufio.NewReader(cn)
				f, err := ReadFrame(br)
				if err != nil || f.Type != TypeConnect {
					return
				}
				if idx != 1 {
					return // 不回 CONNACK 直接断：establish 以 EOF 失败
				}
				aj, _ := connackFrame(&connackJSON{SessionID: "s1"})
				if err := writeOnce(cn, aj); err != nil {
					return
				}
				time.Sleep(50 * time.Millisecond) // 持有首连，随后断开触发重连
			}(cn, n)
		}
	}()

	c, err := Dial(context.Background(), ln.Addr().String(), shortConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if got := c.SessionID(); got != "s1" {
		t.Fatalf("session = %q, want s1", got)
	}

	// 首连 50ms 后断开；此后每轮重拨都 EOF 失败。shortConfig 退避
	// 5ms→50ms，300ms 足以跑数十轮重试，随后 Close 在退避中收敛。
	time.Sleep(300 * time.Millisecond)
	c.Close()
}

// 断线后的重拨退避窗口内 Close：connectLoop 以静默 return 收敛，
// 不再发起重拨（BackoffInitial 拉长以让 Close 稳定落在窗口内）。
func TestClientCloseDuringReconnectBackoff(t *testing.T) {
	addr := startRawListener(t, func(cn net.Conn) {
		defer cn.Close()
		br := bufio.NewReader(cn)
		f, err := ReadFrame(br)
		if err != nil || f.Type != TypeConnect {
			return
		}
		aj, _ := connackFrame(&connackJSON{SessionID: "b1"})
		if err := writeOnce(cn, aj); err != nil {
			return
		}
		time.Sleep(100 * time.Millisecond) // 持有后断开
	})

	cfg := shortConfig()
	cfg.BackoffInitial = 2 * time.Second
	cfg.BackoffMax = 2 * time.Second
	c, err := Dial(context.Background(), addr.String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if got := c.SessionID(); got != "b1" {
		t.Fatalf("session = %q, want b1", got)
	}

	// 断线约发生在 100ms；此后 connectLoop 在 [1s,2s] 的退避抖动里，
	// 400ms 时 Close 必然命中 sleepBackoff 的 closed 分支。
	time.Sleep(400 * time.Millisecond)
	c.Close()
}

// 关闭自动重连后连接消亡：connectLoop 静默退出，不再重拨。
func TestClientReconnectDisabledExitsQuietly(t *testing.T) {
	addr := startRawListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		f, err := ReadFrame(br)
		if err != nil || f.Type != TypeConnect {
			return
		}
		aj, _ := connackFrame(&connackJSON{SessionID: "s2", HeartbeatMS: 1000})
		_ = writeOnce(c, aj)
		time.Sleep(100 * time.Millisecond) // 随后关闭：连接消亡
	})
	cfg := shortConfig()
	cfg.Reconnect = ptr(false)
	c, err := Dial(context.Background(), addr.String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	waitFor(t, "会话建立", func() bool { return c.SessionID() == "s2" })
	time.Sleep(250 * time.Millisecond) // 连接死亡 + 循环退出窗口
}

// ---- client：会话恢复失败时的流迁移 ----

// 恢复失败（会话已过期）：奇数（发起方）流被置为 SESSION_EXPIRED，
// 偶数（服务端发起）流随旧连接消亡（跳过不迁移）。
func TestClientResumeFailedSkipsServerInitiatedFlows(t *testing.T) {
	releaseClient := make(chan struct{})
	releaseServer := make(chan struct{})
	t.Cleanup(func() { close(releaseClient); close(releaseServer) })

	srvCfg := shortConfig()
	srvCfg.Retention = -1 // 断开即弃会话：重连必为全新会话（Resumed=false）
	srv, addr := startTestServer(t, srvCfg, func(s *Server) {
		s.Handle("slow", func(req *Request) (any, error) {
			<-releaseServer
			return item{N: 1}, nil
		})
	})

	c, err := Dial(context.Background(), addr, shortConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.Handle("clientSlow", func(req *Request) (any, error) {
		<-releaseClient
		return item{N: 2}, nil
	})
	resumeFailed := make(chan error, 4)
	c.OnResumeFailed(func(err error) { resumeFailed <- err })

	waitFor(t, "会话登记", func() bool { return len(srv.Sessions()) == 1 })
	sess := srv.Sessions()[0]

	// 发起方流（客户端奇数 id）与被动流（客户端偶数 id）各挂一条
	reqErr := make(chan error, 1)
	go func() {
		_, err := c.Request(context.Background(), "slow", item{N: 1})
		reqErr <- err
	}()
	srvErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := srv.Request(ctx, sess, "clientSlow", item{N: 1})
		srvErr <- err
	}()
	waitFor(t, "客户端两条流登记", func() bool {
		odd, even := false, false
		for id := range clientEp(c).snapshot() {
			if id%2 == 1 {
				odd = true
			} else {
				even = true
			}
		}
		return odd && even
	})

	// 杀服务端连接（保留监听）：客户端自动重连进全新会话
	srv.mu.Lock()
	var sc *serverConn
	for cand := range srv.conns {
		sc = cand
	}
	srv.mu.Unlock()
	sc.ep.tr.kill(errors.New("simulated crash"))

	select {
	case err := <-resumeFailed:
		var se *Error
		if !errors.As(err, &se) || se.Code != CodeSessionExpired {
			t.Fatalf("OnResumeFailed = %v, want SESSION_EXPIRED", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnResumeFailed not fired after reconnect")
	}
	if err := <-reqErr; err == nil {
		t.Fatal("pending client request must fail on resume failure")
	}
	<-srvErr // 服务端发起的请求随连接死亡终结（超时或错误皆可）
}

// ---- client：重订与断连回调 ----

// resubscribeLocked 的三条非 happy-path 出口：发送失败、流被终结、等待超时。
func TestClientResubscribeFailurePaths(t *testing.T) {
	// 发送失败：连接已死（sendCh 填满使 select 只剩 dead 分支）→
	// 流立刻失败并摘除，不重启消费
	ep, tr := newTestEndpoint(t, true, nil)
	for len(tr.sendCh) < cap(tr.sendCh) {
		tr.sendCh <- &Frame{}
	}
	tr.kill(ErrClosed)
	c := &Client{cfg: shortConfig(), table: newRouteTable()}
	c.resubscribeLocked(ep, &Subscription{ep: ep, topic: "t"})
	if n := len(ep.snapshot()); n != 0 {
		t.Fatalf("failed resubscribe left %d flows", n)
	}

	// 流被对端终结（ERROR 先于 SUBACK）→ 放弃重订
	ep2, _ := newTestEndpoint(t, true, nil)
	sub2 := &Subscription{ep: ep2, topic: "t"}
	done := make(chan struct{})
	go func() {
		c.resubscribeLocked(ep2, sub2)
		close(done)
	}()
	id := waitFlow(t, ep2, 1)
	_ = ep2.handleFrame(errorFrame(id, &Error{Code: CodeBusy, Message: "no"}))
	<-done
	sub2.mu.Lock()
	f := sub2.f
	sub2.mu.Unlock()
	if f != nil {
		t.Fatal("failed resubscribe must not rebind the subscription")
	}

	// SUBACK 迟迟不来 → 超时放弃并留日志
	var log syncLogger
	ep3, _ := newTestEndpoint(t, true, func(cfg *Config) { cfg.Logger = &log })
	orig := resubAckTimeout
	resubAckTimeout = 30 * time.Millisecond
	defer func() { resubAckTimeout = orig }()
	c.resubscribeLocked(ep3, &Subscription{ep: ep3, topic: "slow"})
	log.waitFor(t, 1)
}

// onDead 带错误时记日志；isClosed 反映关闭状态。
func TestClientOnDeadLogAndIsClosed(t *testing.T) {
	var log syncLogger
	c := &Client{cfg: Config{Logger: &log}}
	tr, _ := newTestTransport(t, shortConfig(), 0)
	c.onDead(tr)(errors.New("boom"))
	c.onDead(tr)(nil) // 无错误不打日志
	log.waitFor(t, 1)
	if n := log.n(); n != 1 {
		t.Fatalf("log lines = %d, want 1", n)
	}

	if c.isClosed() {
		t.Fatal("fresh client must be open")
	}
	c.closed = make(chan struct{})
	close(c.closed)
	if !c.isClosed() {
		t.Fatal("closed client must report closed")
	}
}

// ---- client：关闭后的 API 与死连接等待 ----

// Close 之后所有发起 API 立即以 ErrClosed 终结。
func TestClientAPIAfterClose(t *testing.T) {
	_, addr := startTestServer(t, shortConfig(), nil)
	c := dialTest(t, addr, shortConfig())
	c.Close()

	ctx := context.Background()
	if _, err := c.Request(ctx, "r", item{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Request = %v", err)
	}
	if _, err := c.Stream(ctx, "r", item{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Stream = %v", err)
	}
	if _, err := c.Channel(ctx, "r", item{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Channel = %v", err)
	}
	if _, err := c.Subscribe(ctx, "t", func(*Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe = %v", err)
	}
	if err := c.SendOneWay("r", item{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendOneWay = %v", err)
	}
	if err := c.Publish("t", item{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish = %v", err)
	}
}

// 未启用重连且连接已死：waitEp 经 ctx.Done 出口返回。
func TestClientWaitEpContextCanceled(t *testing.T) {
	cfg := shortConfig()
	cfg.Reconnect = ptr(false)
	_, addr := startTestServer(t, cfg, nil)
	c := dialTest(t, addr, cfg)
	clientEp(c).tr.kill(errors.New("dead"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Request(ctx, "r", item{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Request on dead endpoint = %v, want context.Canceled", err)
	}
}

// waitEp 放行后发送阻塞（sendCh 填满、写循环未启动不会排空）、随后连接
// 死亡：Subscribe 以 ErrClosed 终结并摘除流。
func TestClientSubscribeSendError(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, nil)
	c := &Client{
		cfg: shortConfig(), addr: "pipe", table: newRouteTable(),
		subs:   make(map[uint32]*Subscription),
		notify: make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
	c.mu.Lock()
	c.ep = ep
	c.mu.Unlock()

	for len(tr.sendCh) < cap(tr.sendCh) {
		tr.sendCh <- &Frame{}
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "t", func(*Message) error { return nil })
		errCh <- err
	}()
	waitFlow(t, ep, 1) // 卡在 send 之前流已登记
	tr.kill(ErrClosed)
	if err := <-errCh; !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe on dead connection = %v, want ErrClosed", err)
	}
}

// ---- server ----

// NewServer 的前置变换器构造失败。
func TestNewServerTransformerError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	stubGCM(t, testKey(), 1)

	cfg := shortConfig()
	cfg.Encrypt = true
	cfg.Key = testKey()
	if _, err := NewServer(ln, cfg); err == nil {
		t.Fatal("expected NewServer to fail on transformer construction")
	}
}

// Publish 的载荷编码失败（store 访问之前即返回）。
func TestServerPublishMarshalError(t *testing.T) {
	srv := &Server{}
	if err := srv.Publish("t", make(chan int)); err == nil {
		t.Fatal("expected marshal error")
	}
}

// sessionEp 的两种失联：会话仍在但连接已解绑；连接未及摘除但已死。
func TestServerSessionEpStates(t *testing.T) {
	// 断开保留中的会话（连接已解绑）
	srv, addr := startTestServer(t, shortConfig(), nil)
	c := dialTest(t, addr, shortConfig())
	waitFor(t, "会话登记", func() bool { return len(srv.Sessions()) == 1 })
	sess := srv.Sessions()[0]
	c.Close()
	waitFor(t, "连接解绑", func() bool {
		ss := srv.store.take(sess)
		if ss == nil {
			return false
		}
		ss.mu.Lock()
		defer ss.mu.Unlock()
		return ss.conn == nil
	})
	if _, err := srv.sessionEp(sess); err == nil {
		t.Fatal("disconnected session must not be addressable")
	}

	// 连接未及摘除但 transport 已死（诚实报错的窗口期）
	tr, _ := newTestTransport(t, shortConfig(), 0)
	tr.kill(errors.New("dead"))
	zombie := &Server{
		store: &sessionStore{sessions: map[string]*serverSession{
			"zombie": {id: "zombie", conn: &serverConn{ep: &endpoint{tr: tr}}},
		}},
	}
	if _, err := zombie.sessionEp("zombie"); err == nil {
		t.Fatal("dead transport must surface as disconnected")
	}
}

// 未知会话：服务端各发起 API 都必须报错。
func TestServerInitiatorsUnknownSession(t *testing.T) {
	srv, _ := startTestServer(t, shortConfig(), nil)
	ctx := context.Background()
	if _, err := srv.Request(ctx, "ghost", "r", item{}); err == nil {
		t.Fatal("Request")
	}
	if _, err := srv.Stream(ctx, "ghost", "r", item{}); err == nil {
		t.Fatal("Stream")
	}
	if _, err := srv.Channel(ctx, "ghost", "r", item{}); err == nil {
		t.Fatal("Channel")
	}
	if err := srv.SendOneWay("ghost", "r", item{}); err == nil {
		t.Fatal("SendOneWay")
	}
}

// stubListener：Accept 由测试投递结果（Serve 的非关闭错误路径）。
type stubListener struct {
	ch chan error
}

func (l *stubListener) Accept() (net.Conn, error) { return nil, <-l.ch }
func (l *stubListener) Close() error              { return nil }
func (l *stubListener) Addr() net.Addr            { return stubAddr{} }

type stubAddr struct{}

func (stubAddr) Network() string { return "stub" }
func (stubAddr) String() string  { return "stub" }

// Accept 的非关闭错误必须从 Serve 原样返回。
func TestServeAcceptError(t *testing.T) {
	ln := &stubListener{ch: make(chan error, 1)}
	srv, err := NewServer(ln, shortConfig())
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	ln.ch <- errors.New("accept boom")
	select {
	case err := <-serveErr:
		if err == nil || err.Error() != "accept boom" {
			t.Fatalf("Serve = %v, want accept boom", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return on accept error")
	}
}

// ---- server：握手拒绝矩阵（raw 客户端） ----

// 非 CONNECT 首帧、坏载荷、拒绝降级加密 → 握手期 ERROR 后断开。
func TestServerHandshakeRejectMatrix(t *testing.T) {
	t.Run("非CONNECT首帧", func(t *testing.T) {
		_, addr := startTestServer(t, shortConfig(), nil)
		rc := dialRaw(t, addr)
		_ = rc.write(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeRequest, StreamID: 1}})
		f := rc.read(t)
		if f.Type != TypeError || errDecode(f.Payload).Message != "expected CONNECT" {
			t.Fatalf("got %v %q", f.Type, errDecode(f.Payload).Message)
		}
	})

	t.Run("坏CONNECT载荷", func(t *testing.T) {
		_, addr := startTestServer(t, shortConfig(), nil)
		rc := dialRaw(t, addr)
		_ = rc.write(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnect}, Payload: []byte("notjson")})
		f := rc.read(t)
		if f.Type != TypeError || errDecode(f.Payload).Code != CodeInvalid {
			t.Fatalf("got %v %+v", f.Type, errDecode(f.Payload))
		}
	})

	t.Run("服务端强制加密", func(t *testing.T) {
		cfg := shortConfig()
		cfg.Encrypt = true
		cfg.Key = testKey()
		_, addr := startTestServer(t, cfg, nil)
		rc := dialRaw(t, addr)
		_ = rc.write(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnect},
			Payload: []byte(`{"version":1,"compress":false,"encrypt":false}`)})
		f := rc.read(t)
		if f.Type != TypeError || errDecode(f.Payload).Code != CodeAuthDenied {
			t.Fatalf("got %v %+v", f.Type, errDecode(f.Payload))
		}
	})
}

// 连接期变换器装配失败（CONNACK 之前）→ INTERNAL 错误帧后断开。
func TestServerConnTransformerError(t *testing.T) {
	cfg := shortConfig()
	cfg.Encrypt = true
	cfg.Key = testKey()
	_, addr := startTestServer(t, cfg, nil)

	// startTestServer 内的 NewServer 已在装桩前消耗了一次 gcmNew，不计数；
	// 装桩后：第 1 次 = 客户端 Dial 前置，第 2 次 = 服务端连接装配。
	stubGCM(t, cfg.Key, 2)
	if _, err := Dial(context.Background(), addr, cfg); err == nil {
		t.Fatal("Dial must fail when the server cannot assemble its transformer")
	}
}

// CONNACK 落网失败（写前连接已被 RST）→ 连接登记被回滚。
func TestServerConnackWriteFailure(t *testing.T) {
	srv, addr := startTestServer(t, shortConfig(), func(s *Server) {
		// 拖住握手期，给测试留出 RST 连接的窗口
		s.OnAuth(func(*connectJSON) error {
			time.Sleep(300 * time.Millisecond)
			return nil
		})
	})
	rc := dialRaw(t, addr)
	_ = rc.write(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnect},
		Payload: []byte(`{"version":1}`)})
	time.Sleep(80 * time.Millisecond) // 服务端已读走 CONNECT，正卡在鉴权钩子
	if tc, ok := rc.c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0) // RST 而非 FIN：CONNACK 的写入必然失败
	}
	rc.c.Close()

	waitFor(t, "连接登记回滚", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.conns) == 0
	})
}

// writeOnce 透传帧编码错误（超上限元数据）。
func TestWriteOnceMetadataTooLarge(t *testing.T) {
	tr, _ := newTestTransport(t, shortConfig(), 0)
	f := &Frame{Header: Header{Version: ProtocolVersion}, Metadata: make([]byte, MaxMetadataSize+1)}
	if err := writeOnce(tr.conn, f); err == nil {
		t.Fatal("writeOnce must surface encode errors")
	}
}

// bind 重放保留队列：帧进入新连接的 sendCh，队列清零，接管语义不误伤。
func TestSessionBindReplaysRetained(t *testing.T) {
	tr, _ := newTestTransport(t, shortConfig(), 0)
	cfg := shortConfig()
	sc := &serverConn{ep: newEndpoint(false, cfg.normalized(), tr, newRouteTable())}
	ss := &serverSession{
		id:   "s",
		subs: make(map[uint32]string),
		retained: map[uint32][]*Frame{
			7: {testFrame(TypeResponse, 7, 0, "", "", []byte(`{"n":1}`))},
		},
		retainedBytes: 100,
	}
	ss.bind(sc)

	sent := sentFrames(tr)
	if len(sent) != 1 || sent[0].StreamID != 7 || sent[0].Type != TypeResponse {
		t.Fatalf("replayed %d frames, want the retained RESPONSE on stream 7", len(sent))
	}
	if len(ss.retained) != 0 || ss.retainedBytes != 0 {
		t.Fatalf("retained queue not cleared: %v %d", ss.retained, ss.retainedBytes)
	}
	if ss.conn != sc || ss.timer != nil {
		t.Fatalf("bind state wrong: conn bound=%v, timer=%v", ss.conn != nil, ss.timer)
	}
}
