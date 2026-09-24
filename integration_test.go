package jsonstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type item struct {
	N int `json:"n"`
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// ---- 心跳 ----

// TestHeartbeatKeepsIdleConnectionAlive：连接上无任何业务流量，仅靠
// PING/PONG 保活，持续多个心跳周期后依然可用。
func TestHeartbeatKeepsIdleConnectionAlive(t *testing.T) {
	cfg := shortConfig()
	cfg.Heartbeat = 100 * time.Millisecond
	_, addr := startTestServer(t, cfg, func(s *Server) {
		s.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	})
	c := dialTest(t, addr, cfg)
	time.Sleep(500 * time.Millisecond) // ≥ 4 个心跳周期
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m, err := c.Request(ctx, "ping", nil)
	if err != nil {
		t.Fatalf("connection should survive idle period via heartbeat: %v", err)
	}
	var it item
	if err := m.Decode(&it); err != nil || it.N != 1 {
		t.Fatalf("unexpected response: %v %v", it, err)
	}
}

// TestTransportReadIdleTimeout：读空闲超过 1.5×心跳，transport 必须自杀。
func TestTransportReadIdleTimeout(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	cfg := shortConfig()
	cfg.Heartbeat = 50 * time.Millisecond
	tr := newTransport(c1, c1, &transformer{}, &cfg, 0, func(_ *Frame) error { return nil }, nil)
	tr.start()
	// c2 保持沉默：既不发 PING 也不发业务帧。
	select {
	case <-tr.dead:
		// 预期：约 75ms 后读超时断开
	case <-time.After(2 * time.Second):
		t.Fatal("transport was not killed after read idle timeout")
	}
}

// TestSilentPeerDisconnectedByHeartbeat：完成握手后不再发送任何帧的对端
// （连 PING 都不发）会被心跳超时清理。
func TestSilentPeerDisconnectedByHeartbeat(t *testing.T) {
	cfg := shortConfig()
	cfg.Heartbeat = 100 * time.Millisecond
	_, addr := startTestServer(t, cfg, nil)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cj := &ConnectJSON{Version: int(ProtocolVersion)}
	f, _ := connectFrame(cj)
	if err := writeOnce(conn, f); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(conn); err != nil { // CONNACK
		t.Fatal(err)
	}
	// 之后保持沉默：server 应在 ~150ms 读超时后断开。期间 server 的写循环
	// 仍会发 PING（写入本端接收缓冲），读到控制帧应忽略，最终必然 EOF。
	deadline := time.Now().Add(3 * time.Second)
	conn.SetReadDeadline(deadline)
	for {
		if _, err := ReadFrame(conn); err != nil {
			return // 服务端心跳超时断开 ✔
		}
		if time.Now().After(deadline) {
			t.Fatal("silent connection should be closed by server heartbeat timeout")
		}
	}
}

// ---- 断线重连恢复 ----

// TestResumeReplaysPendingFrames：流式响应读到一半断线，重连成功（resumed）
// 后服务端重放断开期间产生的帧，流从调用方视角无缝继续。
func TestResumeReplaysPendingFrames(t *testing.T) {
	continueCh := make(chan struct{})
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandleStream("slow", func(_ *Request, em Emitter) error {
			for i := 1; i <= 2; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
			}
			<-continueCh // 测试在这里掐断连接，之后再放行
			for i := 3; i <= 4; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
			}
			return nil
		})
	})

	cfg := shortConfig() // Reconnect 默认开启，BackoffInitial=5ms
	c := dialTest(t, addr, cfg)
	reconnected := make(chan struct{}, 4)
	c.OnReconnect(func() { reconnected <- struct{}{} })

	ctx := context.Background()
	stream, err := c.Stream(ctx, "slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	sidBefore := c.SessionID()

	m, ok := stream.Next(ctx)
	if !ok || m.Decode(&item{}) != nil {
		t.Fatalf("expected frame 1, ok=%v", ok)
	}
	var i1 item
	if err := m.Decode(&i1); err != nil || i1.N != 1 {
		t.Fatalf("want n=1, got %+v err=%v", i1, err)
	}
	m, _ = stream.Next(ctx) // 失败会让下面的 Decode 空指针崩掉测试，无需查 ok
	var i2 item
	if err := m.Decode(&i2); err != nil || i2.N != 2 {
		t.Fatalf("want n=2, got %+v err=%v", i2, err)
	}

	// 掐断底层 TCP，模拟网络故障。
	c.mu.Lock()
	tr := c.ep.tr
	c.mu.Unlock()
	tr.kill(errors.New("simulated network failure"))
	time.Sleep(100 * time.Millisecond) // 让服务端先走完 unbind（否则在途帧语义不保）
	close(continueCh)                  // 服务端 handler 继续 emit → 进入会话保留队列

	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect")
	}
	if c.SessionID() != sidBefore {
		t.Fatalf("session id changed across resume: %s -> %s", sidBefore, c.SessionID())
	}

	// 同一条流应继续吐出重放的帧。
	for want := 3; want <= 4; want++ {
		m, ok := stream.Next(ctx)
		if !ok {
			t.Fatalf("expected replayed frame %d, stream ended (err=%v)", want, stream.Err())
		}
		var it item
		if err := m.Decode(&it); err != nil || it.N != want {
			t.Fatalf("want n=%d, got %+v err=%v", want, it, err)
		}
	}
	m, ok = stream.Next(ctx)
	if ok {
		t.Fatalf("unexpected extra frame: %v", m)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream should end cleanly after replay, got %v", err)
	}
}

// blobItem 载荷 100B 过压缩阈值：保证重放帧在压缩+加密全开下走完整变换管线。
type blobItem struct {
	N    int    `json:"n"`
	Blob string `json:"blob"`
}

// TestResumeFullStackReplay：断线重连恢复 × 压缩+加密+背压 全开组合——
// 重放帧经新连接的 flate/AES-GCM 管线还原无损，断开期间产生的帧数超过
// 信用窗口（4 帧 > Credit=2）时重放仍须正确驱动信用额度，重放完成后
// 同连接继续正常收发。
func TestResumeFullStackReplay(t *testing.T) {
	continueCh := make(chan struct{})
	key := bytes.Repeat([]byte{0x3C}, 32)
	scfg := shortConfig()
	scfg.Compress = true
	scfg.Encrypt = true
	scfg.Key = key
	scfg.Credit = 2
	_, addr := startTestServer(t, scfg, func(s *Server) {
		s.Handle("echo", func(req *Request) (any, error) {
			var v map[string]int
			return v, req.Decode(&v)
		})
		s.HandleStream("slow", func(_ *Request, em Emitter) error {
			for i := 1; i <= 2; i++ {
				if err := em.Emit(blobItem{N: i, Blob: string(bytes.Repeat([]byte("x"), 100))}); err != nil {
					return err
				}
			}
			<-continueCh // 测试在这里掐断连接，之后再放行
			for i := 3; i <= 6; i++ {
				if err := em.Emit(blobItem{N: i, Blob: string(bytes.Repeat([]byte("y"), 100))}); err != nil {
					return err
				}
			}
			return nil
		})
	})

	ccfg := shortConfig()
	ccfg.Compress = true
	ccfg.Encrypt = true
	ccfg.Key = key
	c := dialTest(t, addr, ccfg)
	reconnected := make(chan struct{}, 4)
	c.OnReconnect(func() { reconnected <- struct{}{} })

	ctx := context.Background()
	stream, err := c.Stream(ctx, "slow", nil)
	if err != nil {
		t.Fatal(err)
	}

	for want := 1; want <= 2; want++ {
		m, ok := stream.Next(ctx)
		var it blobItem
		if !ok || m.Decode(&it) != nil || it.N != want || len(it.Blob) != 100 {
			t.Fatalf("live frame %d: got %+v ok=%v", want, it, ok)
		}
	}

	// 掐断底层 TCP，模拟网络故障；服务端随后 emit 的 4 帧进入保留队列。
	c.mu.Lock()
	tr := c.ep.tr
	c.mu.Unlock()
	tr.kill(errors.New("simulated network failure"))
	time.Sleep(100 * time.Millisecond) // 等服务端走完 unbind
	close(continueCh)

	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect")
	}

	// 重放的 4 帧应经解密+解压无损还原（超过 Credit=2 的窗口额度）。
	for want := 3; want <= 6; want++ {
		m, ok := stream.Next(ctx)
		if !ok {
			t.Fatalf("replayed frame %d missing (err=%v)", want, stream.Err())
		}
		var it blobItem
		if err := m.Decode(&it); err != nil || it.N != want || len(it.Blob) != 100 {
			t.Fatalf("replayed frame %d: got %+v err=%v", want, it, err)
		}
	}
	if _, ok := stream.Next(ctx); ok {
		t.Fatal("unexpected extra frame after replay")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream should end cleanly after replay, got %v", err)
	}

	// 信用额度未被重放耗散的证明：同连接继续正常往返。
	m, err := c.Request(ctx, "echo", map[string]int{"a": 7})
	if err != nil {
		t.Fatalf("post-replay request failed (credit drift?): %v", err)
	}
	var v map[string]int
	if err := m.Decode(&v); err != nil || v["a"] != 7 {
		t.Fatalf("post-replay echo: %+v err=%v", v, err)
	}
}

// TestResumeExpiredFailsPendingAndResubscribes：会话恢复失败（服务端禁用
// 保留）时：挂起请求以 SESSION_EXPIRED 失败、OnResumeFailed 触发、订阅自动
// 重订后广播继续可达。
func TestResumeExpiredFailsPendingAndResubscribes(t *testing.T) {
	blockCh := make(chan struct{})
	var srv *Server
	_, addr := startTestServer(t, func() Config {
		cfg := shortConfig()
		cfg.Retention = -1 // 禁用会话保留 → 重连必然恢复失败
		return cfg
	}(), func(s *Server) {
		srv = s
		s.Handle("hang", func(_ *Request) (any, error) {
			<-blockCh // 挂起请求，制造断线时的 in-flight 流
			return item{N: 1}, nil
		})
	})

	cfg := shortConfig()
	c := dialTest(t, addr, cfg)

	got := make(chan int, 8)
	sub, err := c.Subscribe(context.Background(), "news", func(msg *Message) error {
		var it item
		if err := msg.Decode(&it); err != nil {
			return err
		}
		got <- it.N
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Publish("news", item{N: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("want 1, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no first broadcast")
	}

	resumeFailed := make(chan error, 1)
	c.OnResumeFailed(func(err error) { resumeFailed <- err })

	type reqResult struct {
		m   *Message
		err error
	}
	reqCh := make(chan reqResult, 1)
	go func() {
		m, err := c.Request(context.Background(), "hang", nil)
		reqCh <- reqResult{m, err}
	}()
	time.Sleep(100 * time.Millisecond) // 等请求到达服务端并挂起

	c.mu.Lock()
	tr := c.ep.tr
	c.mu.Unlock()
	tr.kill(errors.New("simulated network failure"))
	// 重连很快（backoff 只有一拍），必须等服务端先感知断连并在
	// Retention=-1 下 drop 旧会话；否则重连会被 takeover 判为恢复成功。
	// 按「旧会话 ID 消失」等待，而非 sessions 为空——重连成功后 store
	// 里出现的是新会话。
	oldID := c.SessionID()
	waitDeadline := time.Now().Add(2 * time.Second)
	for {
		srv.store.mu.Lock()
		_, present := srv.store.sessions[oldID]
		srv.store.mu.Unlock()
		if !present {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("server did not drop expired session in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(blockCh)

	select {
	case err := <-resumeFailed:
		var je *Error
		if !errors.As(err, &je) || je.Code != CodeSessionExpired {
			t.Fatalf("want SESSION_EXPIRED, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnResumeFailed not fired")
	}
	select {
	case r := <-reqCh:
		var je *Error
		if !errors.As(r.err, &je) || je.Code != CodeSessionExpired {
			t.Fatalf("pending request should fail with SESSION_EXPIRED, got %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending request neither failed nor returned")
	}

	// 订阅应已自动重订：断线后的广播仍能送达。
	if err := srv.Publish("news", item{N: 2}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 2 {
			t.Fatalf("want 2, got %d", n)
		}
	case <-time.After(2 * time.Second):
		c.mu.Lock()
		subs := len(c.subs)
		epStreams := len(c.ep.streams)
		c.mu.Unlock()
		srv.mu.Lock()
		srvSubs := map[uint32]string{}
		for sc := range srv.conns {
			for id, tp := range sc.sess.subs {
				srvSubs[id] = tp
			}
		}
		srvConns := len(srv.conns)
		srv.mu.Unlock()
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("broadcast after resubscribe not delivered (client subs=%d epStreams=%d; srv conns=%d subs=%v)\n%s",
			subs, epStreams, srvConns, srvSubs, buf[:n])
	}
	_ = sub
}

// ---- 六种消息模式 ----

func setupModeServer(t *testing.T) string {
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		// 1. 请求/响应
		s.Handle("echo", func(req *Request) (any, error) {
			var it item
			if err := req.Decode(&it); err != nil {
				return nil, &Error{Code: CodeInvalid, Message: err.Error()}
			}
			return item{N: it.N * 2}, nil
		})
		// 2. 流式
		s.HandleStream("range", func(req *Request, em Emitter) error {
			var it item
			if err := req.Decode(&it); err != nil {
				return err
			}
			for i := 0; i < it.N; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
			}
			return nil // 自动 COMPLETE
		})
		// 3. 双工
		s.HandleChannel("chat", func(ch *Channel) error {
			for {
				m, err := ch.Receive(context.Background())
				if err != nil {
					return nil // EOF（对端 Close）或取消
				}
				var it item
				if err := m.Decode(&it); err != nil {
					return err
				}
				if err := ch.Send(item{N: it.N + 100}); err != nil {
					return err
				}
			}
		})
		// 4. 单向
		s.HandleOneWay("notify", func(_ *Message) error {
			return nil // 送达断言由 TestModeOneWay 独立完成
		})
		// 6. 错误
		s.Handle("boom", func(_ *Request) (any, error) {
			return nil, &Error{Code: CodeBusy, Message: "overloaded on purpose"}
		})
		s.Handle("panic", func(_ *Request) (any, error) {
			panic("handler exploded")
		})
		s.Handle("plain-err", func(_ *Request) (any, error) {
			return nil, errors.New("plain failure")
		})
	})
	return addr
}

func TestModeRequestResponse(t *testing.T) {
	addr := setupModeServer(t)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	m, err := c.Request(ctx, "echo", item{N: 21})
	if err != nil {
		t.Fatal(err)
	}
	var it item
	if err := m.Decode(&it); err != nil || it.N != 42 {
		t.Fatalf("want 42, got %+v err=%v", it, err)
	}

	// 未知路由 → NOT_FOUND
	_, err = c.Request(ctx, "nope", nil)
	var je *Error
	if !errors.As(err, &je) || je.Code != CodeNotFound {
		t.Fatalf("want NOT_FOUND, got %v", err)
	}
}

func TestModeStreaming(t *testing.T) {
	addr := setupModeServer(t)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	st, err := c.Stream(ctx, "range", item{N: 5})
	if err != nil {
		t.Fatal(err)
	}
	for want := 0; want < 5; want++ {
		m, ok := st.Next(ctx)
		if !ok {
			t.Fatalf("stream ended early at %d (err=%v)", want, st.Err())
		}
		var it item
		if err := m.Decode(&it); err != nil || it.N != want {
			t.Fatalf("want n=%d, got %+v err=%v", want, it, err)
		}
	}
	if m, ok := st.Next(ctx); ok {
		t.Fatalf("unexpected extra frame %v", m)
	}
	if err := st.Err(); err != nil {
		t.Fatalf("clean stream should have nil Err, got %v", err)
	}
}

func TestModeStreamingCancel(t *testing.T) {
	addr := setupModeServer(t)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()
	st, err := c.Stream(ctx, "range", item{N: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Next(ctx); !ok {
		t.Fatal("no first frame")
	}
	if err := st.Cancel(); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Next(ctx); ok {
		t.Fatal("stream should be over after cancel")
	}
	var je *Error
	if !errors.As(st.Err(), &je) || je.Code != CodeCancelled {
		t.Fatalf("want CANCELLED, got %v", st.Err())
	}
}

func TestModeDuplex(t *testing.T) {
	addr := setupModeServer(t)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	ch, err := c.Channel(ctx, "chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := ch.Send(item{N: i}); err != nil {
			t.Fatal(err)
		}
		m, err := ch.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var it item
		if err := m.Decode(&it); err != nil || it.N != i+100 {
			t.Fatalf("want %d, got %+v err=%v", i+100, it, err)
		}
	}
	if err := ch.Close(); err != nil { // 半关闭：我说完了
		t.Fatal(err)
	}
	if _, err := ch.Receive(ctx); err != nil && !errors.Is(err, ErrClosed) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, io.EOF) {
		t.Fatalf("receive after terminal = %v, want ErrClosed/deadline/EOF", err)
	}
}

func TestModeOneWay(t *testing.T) {
	got := make(chan int, 8)
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandleOneWay("notify", func(msg *Message) error {
			var it item
			_ = msg.Decode(&it)
			got <- it.N
			return nil
		})
	})
	c := dialTest(t, addr, shortConfig())
	if err := c.SendOneWay("notify", item{N: 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 7 {
			t.Fatalf("want 7, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oneway message not delivered")
	}
	// 路由不存在也必须静默（协议规定 ONEWAY 永不回帧）。
	if err := c.SendOneWay("missing", item{N: 1}); err != nil {
		t.Fatalf("oneway to missing route must not error: %v", err)
	}
}

func TestModePubSub(t *testing.T) {
	// 服务端 → 客户端广播
	var srv *Server
	_, addr := startTestServer(t, shortConfig(), func(s *Server) { srv = s })
	c := dialTest(t, addr, shortConfig())

	got := make(chan int, 8)
	sub, err := c.Subscribe(context.Background(), "ticks", func(msg *Message) error {
		var it item
		if err := msg.Decode(&it); err != nil {
			return err
		}
		got <- it.N
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := srv.Publish("ticks", item{N: i}); err != nil {
			t.Fatal(err)
		}
		select {
		case n := <-got:
			if n != i {
				t.Fatalf("want %d, got %d", i, n)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("broadcast %d not delivered", i)
		}
	}
	// 退订后不再投递
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := srv.Publish("ticks", item{N: 99}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		t.Fatalf("should not receive after unsubscribe, got %d", n)
	case <-time.After(200 * time.Millisecond):
	}

	// 客户端 → 服务端发布，服务端处理后再广播回来（双向主题）
	_, addr2 := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandlePublish("events", func(msg *Message) error {
			var it item
			if err := msg.Decode(&it); err != nil {
				return err
			}
			return s.Publish("mirror", item{N: it.N * 10})
		})
	})
	c2 := dialTest(t, addr2, shortConfig())
	mirror := make(chan int, 4)
	if _, err := c2.Subscribe(context.Background(), "mirror", func(msg *Message) error {
		var it item
		if err := msg.Decode(&it); err != nil {
			return err
		}
		mirror <- it.N
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c2.Publish("events", item{N: 5}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-mirror:
		if n != 50 {
			t.Fatalf("want 50, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client→server publish roundtrip failed")
	}
}

// TestStreamCancelUnblocksEmit：额度耗尽把 handler 阻塞在 Emit 的取令牌
// 上之后，客户端取消流应解除阻塞（take 的 ctx 失败上抛，handler 收尾），
// 而不是永远卡死。
func TestStreamCancelUnblocksEmit(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	scfg := shortConfig()
	scfg.Credit = 1
	_, addr := startTestServer(t, scfg, func(s *Server) {
		s.HandleStream("firehose", func(_ *Request, em Emitter) error {
			close(started)
			for i := 0; ; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					done <- err
					return err
				}
			}
		})
	})

	ccfg := shortConfig()
	ccfg.Credit = 1 // 双方都启用：生效值取 min，闸门才存在
	c := dialTest(t, addr, ccfg)
	stream, err := c.Stream(context.Background(), "firehose", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// 不消费任何帧：Credit=1 在第一帧后耗尽，handler 阻塞在下一次取令牌。
	time.Sleep(100 * time.Millisecond)

	stream.Cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Emit should surface the unblock error to the handler")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Cancel did not unblock Emit stuck on credit take")
	}
}

func TestModeErrorPropagation(t *testing.T) {
	addr := setupModeServer(t)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	// 带 code 的流错误原样传播
	_, err := c.Request(ctx, "boom", nil)
	var je *Error
	if !errors.As(err, &je) || je.Code != CodeBusy || je.Message != "overloaded on purpose" {
		t.Fatalf("want BUSY with message, got %v", err)
	}
	// 普通 error → INTERNAL
	_, err = c.Request(ctx, "plain-err", nil)
	if !errors.As(err, &je) || je.Code != CodeInternal {
		t.Fatalf("want INTERNAL, got %v", err)
	}
	// panic → INTERNAL，且连接仍然可用（错误只终结流）
	_, err = c.Request(ctx, "panic", nil)
	if !errors.As(err, &je) || je.Code != CodeInternal {
		t.Fatalf("want INTERNAL for panic, got %v", err)
	}
	m, err := c.Request(ctx, "echo", item{N: 2})
	if err != nil {
		t.Fatalf("connection should survive handler errors: %v", err)
	}
	var it item
	_ = m.Decode(&it)
	if it.N != 4 {
		t.Fatalf("want 4, got %d", it.N)
	}
}

// ---- 背压 ----

// TestBackpressureBlocksSender：credit 窗口为 4 时，接收方不消费，发送方
// 必须恰好发出 4 条后阻塞；恢复消费后应全部送达并以 COMPLETE 收尾。
func TestBackpressureBlocksSender(t *testing.T) {
	const window = 4
	const total = 10
	var emitted int32
	done := make(chan struct{})
	scfg := shortConfig()
	scfg.Credit = window
	_, addr := startTestServer(t, scfg, func(s *Server) {
		s.HandleStream("firehose", func(_ *Request, em Emitter) error {
			for i := 0; i < total; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
				atomic.AddInt32(&emitted, 1)
			}
			close(done)
			return nil
		})
	})

	ccfg := shortConfig()
	ccfg.Credit = window
	c := dialTest(t, addr, ccfg)
	ctx := context.Background()
	st, err := c.Stream(ctx, "firehose", nil)
	if err != nil {
		t.Fatal(err)
	}

	// 不消费：给发送方充分时间冲进 credit 墙。
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&emitted); got != window {
		t.Fatalf("sender should be blocked at window=%d, emitted=%d", window, got)
	}
	select {
	case <-done:
		t.Fatal("handler finished without consumer (backpressure failed)")
	default:
	}

	// 恢复消费：全部送达。
	for want := 0; want < total; want++ {
		m, ok := st.Next(ctx)
		if !ok {
			t.Fatalf("stream ended at %d (err=%v)", want, st.Err())
		}
		var it item
		if err := m.Decode(&it); err != nil || it.N != want {
			t.Fatalf("want n=%d, got %+v err=%v", want, it, err)
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish after consumer drained")
	}
	if _, ok := st.Next(ctx); ok || st.Err() != nil {
		t.Fatalf("stream should end cleanly: ok=%v err=%v", ok, st.Err())
	}
}

// TestNoBackpressureByDefault：未启用 credit 时发送方不被阻塞。
func TestNoBackpressureByDefault(t *testing.T) {
	const total = 64
	var emitted int32
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandleStream("bulk", func(_ *Request, em Emitter) error {
			for i := 0; i < total; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
				atomic.AddInt32(&emitted, 1)
			}
			return nil
		})
	})
	c := dialTest(t, addr, shortConfig()) // Credit=0
	ctx := context.Background()
	st, err := c.Stream(ctx, "bulk", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&emitted) < total {
		select {
		case <-deadline:
			t.Fatalf("sender blocked without credit: emitted=%d", atomic.LoadInt32(&emitted))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if _, ok := st.Next(ctx); !ok {
		t.Fatal("expected at least one frame")
	}
}

// ---- 连接级错误 ----

// TestConnectionLevelErrorCloses：Stream ID=0 的 ERROR 表示连接级致命错误，
// 客户端必须断开（protocol.md §7.10）。
func TestConnectionLevelErrorCloses(t *testing.T) {
	_, addr := startTestServer(t, shortConfig(), nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f, _ := connectFrame(&ConnectJSON{Version: int(ProtocolVersion)})
	if err := writeOnce(conn, f); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(conn); err != nil { // CONNACK
		t.Fatal(err)
	}
	ef := errorFrame(0, &Error{Code: CodeProtocol, Message: "fatal"})
	if err := writeOnce(conn, ef); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	for {
		if _, err := conn.Read(buf); err != nil {
			return // 服务端断开 ✔
		}
	}
}

func TestCreditFrameRoundTrip(t *testing.T) {
	cp := creditPayload{N: 3}
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypeCredit, StreamID: 5}, Payload: encodeCredit(cp.N)}
	buf, err := f.appendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(bytesReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	var back creditPayload
	if err := jsonUnmarshal(got.Payload, &back); err != nil {
		t.Fatal(err)
	}
	if back.N != 3 {
		t.Fatalf("want 3, got %d", back.N)
	}
}

// ---- 服务端主动发起（odd/even 双向对等的另一半） ----

// TestServerInitiatedModes：服务端向客户端会话主动发起四种交互，
// 客户端以 Handle/HandleStream/HandleChannel/HandleOneWay 注册的 handler 应答。
func TestServerInitiatedModes(t *testing.T) {
	srv, addr := startTestServer(t, shortConfig(), nil)
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	c.Handle("client.echo", func(req *Request) (any, error) {
		var it item
		if err := req.Decode(&it); err != nil {
			return nil, &Error{Code: CodeInvalid, Message: err.Error()}
		}
		return item{N: it.N * 2}, nil
	})
	c.HandleStream("client.range", func(req *Request, em Emitter) error {
		var it item
		if err := req.Decode(&it); err != nil {
			return err
		}
		for i := 0; i < it.N; i++ {
			if err := em.Emit(item{N: i}); err != nil {
				return err
			}
		}
		return nil
	})
	c.HandleChannel("client.chat", func(ch *Channel) error {
		m, err := ch.Receive(context.Background())
		if err != nil {
			return err
		}
		var it item
		if err := m.Decode(&it); err != nil {
			return err
		}
		return ch.Send(item{N: it.N + 200})
	})
	gotOneWay := make(chan item, 1)
	c.HandleOneWay("client.log", func(msg *Message) error {
		var it item
		if err := msg.Decode(&it); err != nil {
			return err
		}
		gotOneWay <- it
		return nil
	})

	sid := c.SessionID()
	if got := srv.Sessions(); len(got) != 1 || got[0] != sid {
		t.Fatalf("Sessions() = %v, want [%s]", got, sid)
	}

	// 请求/响应
	m, err := srv.Request(ctx, sid, "client.echo", item{N: 21})
	if err != nil {
		t.Fatal(err)
	}
	var it item
	if err := m.Decode(&it); err != nil || it.N != 42 {
		t.Fatalf("echo: want 42, got %+v err=%v", it, err)
	}

	// 流式
	rs, err := srv.Stream(ctx, sid, "client.range", item{N: 3})
	if err != nil {
		t.Fatal(err)
	}
	for want := 0; want < 3; want++ {
		m, ok := rs.Next(ctx)
		if !ok {
			t.Fatalf("stream ended early at %d (err=%v)", want, rs.Err())
		}
		if err := m.Decode(&it); err != nil || it.N != want {
			t.Fatalf("want n=%d, got %+v err=%v", want, it, err)
		}
	}
	if _, ok := rs.Next(ctx); ok {
		t.Fatal("unexpected extra frame")
	}

	// 双工
	ch, err := srv.Channel(ctx, sid, "client.chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Send(item{N: 5}); err != nil {
		t.Fatal(err)
	}
	m, err = ch.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Decode(&it); err != nil || it.N != 205 {
		t.Fatalf("chat: want 205, got %+v err=%v", it, err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}

	// 单向
	if err := srv.SendOneWay(sid, "client.log", item{N: 9}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gotOneWay:
		if got.N != 9 {
			t.Fatalf("oneway: want 9, got %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oneway handler not called")
	}

	// 未知会话/未连接：立即报错，不挂起
	if _, err := srv.Request(ctx, "no-such-session", "client.echo", item{N: 1}); err == nil {
		t.Fatal("want error for unknown session")
	}
}

// TestReconnectAfterServerRestart：服务端整个下线（监听关闭 + 连接被杀），
// 客户端对拒连端口按退避持续重试；服务端在原地址复活后自动重连成功。
func TestReconnectAfterServerRestart(t *testing.T) {
	cfg := shortConfig()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	srv, err := NewServer(ln, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	go srv.Serve()

	c, err := Dial(context.Background(), addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reconnected := make(chan struct{}, 4)
	c.OnReconnect(func() { reconnected <- struct{}{} })

	// 服务端下线：连接死亡 → 退避重拨 → 拒连 → backoff 增长到上限。
	// 端口故意空窗 200ms：若立刻复活，客户端首次重拨总能赶上新服务端，
	// 拒连分支（nextBackoff 的增长与 BackoffMax 钳制）永远不会被执行。
	_ = srv.Close()
	time.Sleep(200 * time.Millisecond)

	// 同一地址复活（全新会话存储：旧会话必然 resume 失败）
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv2, err := NewServer(ln2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv2.Handle("ping", func(_ *Request) (any, error) { return item{N: 2}, nil })
	go srv2.Serve()
	t.Cleanup(func() { _ = srv2.Close() })

	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		c.mu.Lock()
		epAlive := false
		if c.ep != nil {
			select {
			case <-c.ep.tr.dead:
			default:
				epAlive = true
			}
		}
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		c.mu.Unlock()
		t.Fatalf("client did not reconnect after server restart: session=%q epAlive=%v\n%s",
			c.SessionID(), epAlive, buf[:n])
	}
	m, err := c.Request(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("request after reconnect: %v", err)
	}
	var it item
	if err := m.Decode(&it); err != nil || it.N != 2 {
		t.Fatalf("want 2 from restarted server, got %+v err=%v", it, err)
	}
}

// TestChannelHandlerError：双工通道的 handler 返回错误 → 错误帧回传，
// 对端 Receive 以该错误收场；错误只终结流，连接仍可用。
func TestChannelHandlerError(t *testing.T) {
	srv, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandleChannel("bad-chat", func(ch *Channel) error {
			if _, err := ch.Receive(context.Background()); err != nil {
				return err
			}
			return &Error{Code: CodeInvalid, Message: "bad chat on purpose"}
		})
		s.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	})
	_ = srv
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	ch, err := c.Channel(ctx, "bad-chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Send(item{N: 1}); err != nil {
		t.Fatal(err)
	}
	_, err = ch.Receive(ctx)
	var je *Error
	if !errors.As(err, &je) || je.Code != CodeInvalid || je.Message != "bad chat on purpose" {
		t.Fatalf("want INVALID 'bad chat on purpose', got %v", err)
	}
	if _, err := c.Request(ctx, "ping", nil); err != nil {
		t.Fatalf("connection should survive channel handler errors: %v", err)
	}
}

// TestChannelCancel：Cancel 立即终结整条双工流——对端阻塞中的 Receive 以
// CANCELLED 失败，本端后续 Send/Receive 同样立即失败而非挂起（§7.4 半关闭
// 由 Close 承担，Cancel 是无差别终结）。
func TestChannelCancel(t *testing.T) {
	srvFailed := make(chan error, 1)
	srv, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.HandleChannel("cancel-chat", func(ch *Channel) error {
			_, err := ch.Receive(context.Background()) // 阻塞，直到对端 Cancel
			srvFailed <- err
			return nil
		})
		s.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	})
	_ = srv
	c := dialTest(t, addr, shortConfig())
	ctx := context.Background()

	ch, err := c.Channel(ctx, "cancel-chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	// 留出时间让服务端 handler 停进 Receive 的阻塞读，再立即终结
	time.Sleep(50 * time.Millisecond)
	if err := ch.Cancel(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-srvFailed:
		var je *Error
		if !errors.As(err, &je) || je.Code != CodeCancelled {
			t.Fatalf("server Receive after client Cancel: want CANCELLED, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server Receive not failed after client Cancel")
	}
	// 本端流已终结：后续收发立即失败
	if err := ch.Send(item{N: 2}); !errors.Is(err, ErrClosed) {
		t.Fatalf("client Send after Cancel: want ErrClosed, got %v", err)
	}
	if _, err := ch.Receive(ctx); err == nil {
		t.Fatal("client Receive after Cancel should fail")
	}
	// Cancel 只终结这条流，连接仍存活
	if _, err := c.Request(ctx, "ping", nil); err != nil {
		t.Fatalf("connection should survive channel cancel: %v", err)
	}
}

// TestCreditNegotiationTakesMin：credit 生效值 = 双方声明中大于 0 者的最小值
// （protocol.md §6.1）。服务端 2、客户端 8 → 生效窗口 2：发送方须在 2 处撞墙。
func TestCreditNegotiationTakesMin(t *testing.T) {
	const total = 8
	var emitted int32
	done := make(chan struct{})
	scfg := shortConfig()
	scfg.Credit = 2
	_, addr := startTestServer(t, scfg, func(s *Server) {
		s.HandleStream("firehose", func(_ *Request, em Emitter) error {
			for i := 0; i < total; i++ {
				if err := em.Emit(item{N: i}); err != nil {
					return err
				}
				atomic.AddInt32(&emitted, 1)
			}
			close(done)
			return nil
		})
	})

	ccfg := shortConfig()
	ccfg.Credit = 8 // 比服务端大：生效值应取服务端的 2
	c := dialTest(t, addr, ccfg)
	ctx := context.Background()
	st, err := c.Stream(ctx, "firehose", nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&emitted); got != 2 {
		t.Fatalf("effective window should be min(2,8)=2, emitted=%d", got)
	}
	for want := 0; want < total; want++ {
		if _, ok := st.Next(ctx); !ok {
			t.Fatalf("stream ended at %d (err=%v)", want, st.Err())
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish after consumer drained")
	}
}

// TestEncryptedCompressedEndToEnd：压缩+加密全链路——握手协商生效、
// 每帧按 Flags 走 flate→AES-GCM 管线，四种交互模式（含 >64B 压缩阈值
// 的大 payload）在真实连接上往返无损。
func TestEncryptedCompressedEndToEnd(t *testing.T) {
	key := bytes.Repeat([]byte{0x5A}, 32)
	scfg := shortConfig()
	scfg.Compress = true
	scfg.Encrypt = true
	scfg.Key = key
	big := map[string]any{"blob": string(bytes.Repeat([]byte("JsonStream"), 400))} // 4KB 高冗余，必压缩
	_, addr := startTestServer(t, scfg, func(s *Server) {
		s.Handle("echo", func(req *Request) (any, error) {
			var v map[string]any
			return v, req.Decode(&v)
		})
		s.HandleStream("bulk", func(_ *Request, em Emitter) error {
			for i := 0; i < 3; i++ {
				if err := em.Emit(big); err != nil {
					return err
				}
			}
			return nil
		})
	})

	ccfg := shortConfig()
	ccfg.Compress = true
	ccfg.Encrypt = true
	ccfg.Key = key
	c := dialTest(t, addr, ccfg)
	ctx := context.Background()

	// 请求/响应（小 payload，明文直发路径）
	m, err := c.Request(ctx, "echo", map[string]int{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	var small map[string]int
	if err := m.Decode(&small); err != nil || small["a"] != 1 {
		t.Fatalf("small echo: %+v err=%v", small, err)
	}

	// 流式（大 payload，压缩+加密路径），内容逐字节比对
	st, err := c.Stream(ctx, "bulk", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantBlob := big["blob"].(string)
	for i := 0; i < 3; i++ {
		got, ok := st.Next(ctx)
		if !ok {
			t.Fatalf("stream ended at %d (err=%v)", i, st.Err())
		}
		var frame map[string]any
		if err := got.Decode(&frame); err != nil {
			t.Fatal(err)
		}
		if frame["blob"] != wantBlob {
			t.Fatal("large payload corrupted through compress+encrypt pipeline")
		}
	}
}

// TestResumeOverflowedSessionStartsFresh：断开期间保留队列超限 → 会话标记
// overflowed、队列清空，重连时 resumed=false（诚实降级为全新会话）；
// 订阅自动重订后广播继续可达（§8.1/§8.2 的溢出语义）。
func TestResumeOverflowedSessionStartsFresh(t *testing.T) {
	var srv *Server
	scfg := shortConfig()
	scfg.RetentionBytes = 512 // 故意调小：几帧广播即溢出
	_, addr := startTestServer(t, scfg, func(s *Server) {
		srv = s
	})
	ccfg := shortConfig()
	ccfg.BackoffInitial = 300 * time.Millisecond // 拉长重连退避，给灌帧留出窗口
	ccfg.BackoffMax = 300 * time.Millisecond
	c := dialTest(t, addr, ccfg)
	ctx := context.Background()

	resumeFailed := make(chan error, 1)
	c.OnResumeFailed(func(err error) { resumeFailed <- err })

	got := make(chan int, 256)
	sub, err := c.Subscribe(ctx, "news", func(msg *Message) error {
		var it item
		if err := msg.Decode(&it); err != nil {
			return err
		}
		got <- it.N
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := srv.Publish("news", item{N: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("want 1, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no first broadcast")
	}

	// 掐断底层连接，等会话进入保留态（sess.conn == nil）
	oldID := c.SessionID()
	c.mu.Lock()
	tr := c.ep.tr
	c.mu.Unlock()
	tr.kill(errors.New("simulated network failure"))
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.store.mu.Lock()
		ss := srv.store.sessions[oldID]
		srv.store.mu.Unlock()
		disconnected := false
		if ss != nil {
			ss.mu.Lock()
			disconnected = ss.conn == nil
			ss.mu.Unlock()
		}
		if disconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session did not enter retention")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 断开期间灌入超限广播：溢出 → 清空队列并标记不可恢复
	for i := 2; i < 100; i++ {
		if err := srv.Publish("news", item{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	srv.store.mu.Lock()
	ss := srv.store.sessions[oldID]
	srv.store.mu.Unlock()
	ss.mu.Lock()
	overflowed := ss.overflowed
	ss.mu.Unlock()
	if !overflowed {
		t.Fatal("session should be marked overflowed after flood")
	}

	// 重连 → resumed=false → OnResumeFailed；订阅自动重订，广播继续可达
	select {
	case <-resumeFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnResumeFailed not fired for overflowed session")
	}
	if c.SessionID() == oldID {
		t.Fatal("client should have a fresh session after overflow")
	}
	if err := srv.Publish("news", item{N: 999}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 999 {
			t.Fatalf("want 999 after resubscribe, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no broadcast after resubscribe on fresh session")
	}
}

// TestTakeoverKicksOldConnection：同会话新连接顶替旧连接——raw 连接携带
// 客户端 session_id 接管：CONNACK resumed=true、旧连接立即死亡；客户端
// 自动重连后反过来顶替 raw，业务恢复可用。
func TestTakeoverKicksOldConnection(t *testing.T) {
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	})
	c := dialTest(t, addr, shortConfig())
	sid := c.SessionID()
	c.mu.Lock()
	oldDead := c.ep.tr.dead
	c.mu.Unlock()

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	cj := &ConnectJSON{Version: int(ProtocolVersion), SessionID: sid, HeartbeatMS: 1000}
	f, _ := connectFrame(cj)
	if err := writeOnce(raw, f); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != TypeConnAck {
		t.Fatalf("want CONNACK, got %s", ack.Type)
	}
	var aj connackJSON
	if err := jsonUnmarshal(ack.Payload, &aj); err != nil {
		t.Fatal(err)
	}
	if !aj.Resumed || aj.SessionID != sid {
		t.Fatalf("want resumed session %s, got resumed=%v id=%q", sid, aj.Resumed, aj.SessionID)
	}

	// 旧连接被顶替后必须立刻死亡
	select {
	case <-oldDead:
	case <-time.After(2 * time.Second):
		t.Fatal("old connection not killed by takeover")
	}

	// 客户端自动重连 → 反过来顶替 raw → 业务恢复
	recoverDeadline := time.Now().Add(5 * time.Second)
	for {
		reqCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.Request(reqCtx, "ping", nil)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(recoverDeadline) {
			t.Fatalf("client did not recover after takeover: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRetentionExpiryStartsFresh：保留期自然到期（非禁用）——定时器触发
// drop，重连时旧会话已不在存储中 → resumed=false、全新会话。
func TestRetentionExpiryStartsFresh(t *testing.T) {
	var srv *Server
	scfg := shortConfig()
	scfg.Retention = 100 * time.Millisecond // 短保留期：走定时器 drop 路径
	_, addr := startTestServer(t, scfg, func(s *Server) {
		srv = s
	})
	ccfg := shortConfig()
	ccfg.BackoffInitial = 300 * time.Millisecond // 重连晚于过期，必然恢复失败
	ccfg.BackoffMax = 300 * time.Millisecond
	c := dialTest(t, addr, ccfg)

	resumeFailed := make(chan error, 1)
	c.OnResumeFailed(func(err error) { resumeFailed <- err })

	sid := c.SessionID()
	c.mu.Lock()
	tr := c.ep.tr
	c.mu.Unlock()
	tr.kill(errors.New("simulated network failure"))

	// 等保留定时器触发：旧会话从存储消失
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.store.mu.Lock()
		_, present := srv.store.sessions[sid]
		srv.store.mu.Unlock()
		if !present {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired session was not dropped by retention timer")
		}
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-resumeFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnResumeFailed not fired after retention expiry")
	}
	if c.SessionID() == sid {
		t.Fatal("client should have a fresh session after retention expiry")
	}
}

// TestRequestContextTimeout：请求超时由客户端 ctx 决定——Request 以
// DeadlineExceeded 失败并终结该流，连接本身不受影响、后续请求正常。
func TestRequestContextTimeout(t *testing.T) {
	block := make(chan struct{})
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.Handle("hang", func(_ *Request) (any, error) {
			<-block
			return item{N: 1}, nil
		})
		s.Handle("ping", func(_ *Request) (any, error) { return item{N: 2}, nil })
	})
	c := dialTest(t, addr, shortConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := c.Request(ctx, "hang", nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	close(block) // 放行服务端 handler：迟到的响应应被静默丢弃而非搞挂连接

	m, err := c.Request(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("connection should survive a timed-out request: %v", err)
	}
	var it item
	if err := m.Decode(&it); err != nil || it.N != 2 {
		t.Fatalf("want 2, got %+v err=%v", it, err)
	}
}

// 高并发 + 泄漏守卫：N 客户端并发请求/流式交互全部完成后，goroutine
// 数必须回归基线——读写循环、connectLoop、流消费 goroutine 都要随
// 连接关闭退出，长跑服务不得积累。基线取自拨号之前，守卫只看增量。
func TestConcurrentClientsGoroutineBaseline(t *testing.T) {
	addr := setupModeServer(t)
	before := runtime.NumGoroutine()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Dial(context.Background(), addr, shortConfig())
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for k := 0; k < 3; k++ {
				if _, err := c.Request(ctx, "echo", item{N: k}); err != nil {
					t.Errorf("request: %v", err)
					return
				}
			}
			st, err := c.Stream(ctx, "range", item{N: 3})
			if err != nil {
				t.Errorf("stream: %v", err)
				return
			}
			for want := 0; want < 3; want++ {
				if _, ok := st.Next(ctx); !ok {
					t.Errorf("stream ended early at %d (err=%v)", want, st.Err())
					return
				}
			}
			_ = st.Cancel()
		}()
	}
	wg.Wait()

	// 守卫：GC + 轮询等待 goroutine 收敛；基线 ±2 容差吸收 runtime 噪声
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines did not return to baseline: %d > %d\n%s",
				runtime.NumGoroutine(), before+2, buf[:n])
		}
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
}
