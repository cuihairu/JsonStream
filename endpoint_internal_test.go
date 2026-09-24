package jsonstream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncLogger 是并发安全的日志采集器：断言异步路径（goroutine 里的
// recover、订阅回调）的日志副作用时使用。
type syncLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *syncLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *syncLogger) n() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// waitFor 轮询直到日志条数 ≥ n，超时 fatal 并吐出已有日志。
func (l *syncLogger) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for l.n() < n {
		if time.Now().After(deadline) {
			l.mu.Lock()
			defer l.mu.Unlock()
			t.Fatalf("timed out waiting for %d log lines, have %d: %v", n, len(l.lines), l.lines)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForLine 轮询直到出现含 substr 的日志行。作为异步 goroutine 执行
// 进度的锚点使用：日志行与紧随其后的代码是连续语句，看到日志即可断定
// 后续路径已到达——比固定时窗可靠（race 模式下调度慢化会漂移）。
func (l *syncLogger) waitForLine(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		var found bool
		for _, line := range l.lines {
			if strings.Contains(line, substr) {
				found = true
				break
			}
		}
		snapshot := append([]string(nil), l.lines...)
		l.mu.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for line %q, have %v", substr, snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

// newTestEndpoint 构造未启动读写循环的 endpoint + transport：出站帧滞留
// sendCh 供断言，入站帧经 handleFrame 直接驱动（协议违规矩阵不必走网络）。
func newTestEndpoint(t *testing.T, clientSide bool, mutate func(*Config)) (*endpoint, *transport) {
	t.Helper()
	cfg := shortConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	cfg = cfg.normalized()
	tr, _ := newTestTransport(t, cfg, cfg.Credit)
	ep := newEndpoint(clientSide, cfg, tr, newRouteTable())
	return ep, tr
}

// sentFrames 取走滞留在 sendCh 的出站帧。
func sentFrames(tr *transport) []*Frame {
	var out []*Frame
	for {
		select {
		case f := <-tr.sendCh:
			out = append(out, f)
		default:
			return out
		}
	}
}

// testFrame 组装一帧供 handleFrame 直接分发。
func testFrame(typ FrameType, sid uint32, flags uint8, route, topic string, payload []byte) *Frame {
	return &Frame{
		Header:   Header{Version: ProtocolVersion, Flags: flags, Type: typ, StreamID: sid},
		Metadata: encodeMeta(route, topic),
		Payload:  payload,
	}
}

// waitFlow 等待 endpoint 上恰好出现 want 条活跃流并返回其中一个 id
// （doXxx 在 goroutine 里发起，注册时机不可知，只能轮询）。
func waitFlow(t *testing.T, ep *endpoint, want int) uint32 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := ep.snapshot()
		if len(snap) == want {
			for id := range snap {
				return id
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("flow not registered, have %d want %d", len(ep.snapshot()), want)
	return 0
}

// ---- endpoint 分发矩阵 ----

// credit 窗口的批量回授阈值：窗口过小时钳到 1。
func TestEndpointCreditFlushClamp(t *testing.T) {
	ep, _ := newTestEndpoint(t, false, func(c *Config) { c.Credit = 1 })
	if ep.creditFlushAt != 1 {
		t.Fatalf("creditFlushAt = %d, want 1", ep.creditFlushAt)
	}
}

// 主题 handler panic 必须被 recover 并留日志（panic 不得打穿读循环）。
func TestEndpointPublishHandlerPanicRecovered(t *testing.T) {
	var log syncLogger
	ep, _ := newTestEndpoint(t, false, func(c *Config) { c.Logger = &log })
	ep.table.HandlePublish("boom", func(*Message) error { panic("topic exploded") })
	if err := ep.handleFrame(testFrame(TypePublish, 99, 0, "", "boom", []byte(`{}`))); err != nil {
		t.Fatal(err)
	}
	log.waitFor(t, 1)
}

// 主题 handler 返回错误 → 回错误帧（对端可感知投递失败）。
func TestEndpointPublishHandlerErrorEmitsErrorFrame(t *testing.T) {
	ep, tr := newTestEndpoint(t, false, nil)
	ep.table.HandlePublish("bad", func(*Message) error { return errors.New("handler failed") })
	if err := ep.handleFrame(testFrame(TypePublish, 99, 0, "", "bad", []byte(`{}`))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		frames := sentFrames(tr)
		if len(frames) == 1 {
			if frames[0].Type != TypeError || frames[0].StreamID != 99 {
				t.Fatalf("emitted %+v, want ERROR on stream 99", frames[0])
			}
			if got := errDecode(frames[0].Payload); got.Message != "handler failed" {
				t.Fatalf("error payload = %+v", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("error frame not emitted for failing topic handler")
		}
		time.Sleep(time.Millisecond)
	}
}

// CREDIT 帧载荷损坏属协议违规。
func TestEndpointCreditBadPayload(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	err := ep.handleFrame(testFrame(TypeCredit, 0, 0, "", "", []byte(`{bad`)))
	var e *Error
	if !errors.As(err, &e) || !e.IsProtocol() {
		t.Fatalf("bad credit payload err = %v, want protocol error", err)
	}
}

// client 侧没有 onSubscribe 钩子：SUBSCRIBE 被静默忽略、不回帧。
func TestEndpointSubscribeWithoutHookIgnored(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, nil)
	if err := ep.handleFrame(testFrame(TypeSubscribe, 5, 0, "", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if n := len(sentFrames(tr)); n != 0 {
		t.Fatalf("client answered SUBSCRIBE with %d frames", n)
	}
}

// server 侧订阅登记：钩子拒绝 → ERROR；接受 → SUBACK。
func TestEndpointSubscribeHook(t *testing.T) {
	ep, tr := newTestEndpoint(t, false, nil)
	ep.onSubscribe = func(_ uint32, topic string) error {
		if topic == "deny" {
			return errors.New("denied")
		}
		return nil
	}
	if err := ep.handleFrame(testFrame(TypeSubscribe, 5, 0, "", "deny", nil)); err != nil {
		t.Fatal(err)
	}
	if err := ep.handleFrame(testFrame(TypeSubscribe, 7, 0, "", "ok", nil)); err != nil {
		t.Fatal(err)
	}
	frames := sentFrames(tr)
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want 2", len(frames))
	}
	if frames[0].Type != TypeError || errDecode(frames[0].Payload).Code != CodeInternal {
		t.Fatalf("frames[0] = %v %+v, want internal ERROR", frames[0].Type, errDecode(frames[0].Payload))
	}
	if frames[1].Type != TypeSubAck || frames[1].StreamID != 7 {
		t.Fatalf("frames[1] = %v sid=%d, want SUBACK sid=7", frames[1].Type, frames[1].StreamID)
	}
}

// 未知的帧类型是协议违规（握手帧不该出现在已建立的连接上）。
func TestEndpointUnknownFrameType(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	err := ep.handleFrame(testFrame(TypeConnect, 0, 0, "", "", nil))
	var e *Error
	if !errors.As(err, &e) || !e.IsProtocol() {
		t.Fatalf("unknown type err = %v, want protocol error", err)
	}
}

// prepareRequest 的拒绝矩阵：通道路由不存在 → NOT_FOUND；请求/流模式
// 与注册类型不符 → PROTOCOL。均回错误帧且不注册流。
func TestEndpointPrepareRequestRejects(t *testing.T) {
	ep, tr := newTestEndpoint(t, false, nil)
	ep.table.Handle("req", func(*Request) (any, error) { return nil, nil })
	ep.table.HandleStream("str", func(*Request, Emitter) error { return nil })

	cases := []struct {
		name  string
		frame *Frame
		code  ErrorCode
	}{
		{"通道路由不存在", testFrame(TypeRequest, 4, FlagChannel, "ghost", "", nil), CodeNotFound},
		{"请求路由收到流标志", testFrame(TypeRequest, 6, FlagStream, "req", "", nil), CodeProtocol},
		{"流路由缺少流标志", testFrame(TypeRequest, 8, 0, "str", "", nil), CodeProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ep.handleFrame(tc.frame); err != nil {
				t.Fatal(err)
			}
			frames := sentFrames(tr)
			if len(frames) != 1 || frames[0].Type != TypeError {
				t.Fatalf("emitted %d frames, want 1 ERROR", len(frames))
			}
			if got := errDecode(frames[0].Payload).Code; got != tc.code {
				t.Fatalf("code = %v, want %v", got, tc.code)
			}
			if n := len(ep.snapshot()); n != 0 {
				t.Fatalf("rejected request leaked %d flows", n)
			}
		})
	}
}

// 请求 handler 返回无法编码的值 → INVALID 错误帧。
func TestEndpointRunRequestEncodeError(t *testing.T) {
	ep, tr := newTestEndpoint(t, false, nil)
	ep.table.Handle("enc", func(*Request) (any, error) { return make(chan int), nil })
	f := testFrame(TypeRequest, 10, 0, "enc", "", []byte(`{}`))
	flw := ep.prepareRequest(f)
	if flw == nil {
		t.Fatal("flow not registered")
	}
	ep.runRequest(flw, f) // 同步驱动，免 goroutine 时序
	frames := sentFrames(tr)
	if len(frames) != 1 || frames[0].Type != TypeError {
		t.Fatalf("emitted %d frames, want 1 ERROR", len(frames))
	}
	if got := errDecode(frames[0].Payload).Code; got != CodeInvalid {
		t.Fatalf("code = %v, want INVALID", got)
	}
}

// 流式 handler 返回错误 → 错误帧透传业务错误码。
func TestEndpointRunStreamHandlerError(t *testing.T) {
	ep, tr := newTestEndpoint(t, false, nil)
	ep.table.HandleStream("serr", func(*Request, Emitter) error {
		return &Error{Code: CodeBusy, Message: "overloaded"}
	})
	f := testFrame(TypeRequest, 12, FlagStream, "serr", "", []byte(`{}`))
	flw := ep.prepareRequest(f)
	if flw == nil {
		t.Fatal("flow not registered")
	}
	ep.runRequest(flw, f)
	frames := sentFrames(tr)
	if len(frames) != 1 || frames[0].Type != TypeError {
		t.Fatalf("emitted %d frames, want 1 ERROR", len(frames))
	}
	if got := errDecode(frames[0].Payload); got.Code != CodeBusy || got.Message != "overloaded" {
		t.Fatalf("error payload = %+v, want BUSY/overloaded", got)
	}
}

// ONEWAY 永不回帧：handler panic 只留日志；路由不存在静默。
func TestEndpointServeOneWayPanicAndMissing(t *testing.T) {
	var log syncLogger
	ep, tr := newTestEndpoint(t, false, func(c *Config) { c.Logger = &log })
	ep.table.HandleOneWay("boom", func(*Message) error { panic("oneway exploded") })
	ep.serveOneWay(testFrame(TypeOneWay, 14, 0, "boom", "", []byte(`{}`)))
	log.waitFor(t, 1)

	ep.serveOneWay(testFrame(TypeOneWay, 16, 0, "ghost", "", []byte(`{}`)))
	if n := log.n(); n != 1 {
		t.Fatalf("log lines = %d, want 1 (missing route must stay silent)", n)
	}
	if n := len(sentFrames(tr)); n != 0 {
		t.Fatalf("ONEWAY answered with %d frames, want none", n)
	}
}

// ---- 发起侧终态与错误 ----

// 发起侧编码失败：五个发起 API 都不得登记流。
func TestEndpointDoXxxMarshalError(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	ctx := context.Background()
	if _, err := ep.doRequest(ctx, "r", make(chan int)); err == nil {
		t.Fatal("doRequest: expected marshal error")
	}
	if _, err := ep.doStream(ctx, "r", make(chan int)); err == nil {
		t.Fatal("doStream: expected marshal error")
	}
	if _, err := ep.doChannel(ctx, "r", make(chan int)); err == nil {
		t.Fatal("doChannel: expected marshal error")
	}
	if err := ep.doOneWay("r", make(chan int)); err == nil {
		t.Fatal("doOneWay: expected marshal error")
	}
	if err := ep.doPublish("t", make(chan int)); err == nil {
		t.Fatal("doPublish: expected marshal error")
	}
	if n := len(ep.snapshot()); n != 0 {
		t.Fatalf("marshal failures registered %d flows", n)
	}
}

// 发送失败（sendCh 填满 + 连接死亡 → select 只剩 dead 分支）：四个需要
// 应答的发起 API 必须摘除刚登记的流并返回错误。
func TestEndpointDoXxxSendError(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, nil)
	for i := 0; i < cap(tr.sendCh); i++ {
		tr.sendCh <- &Frame{}
	}
	tr.kill(ErrClosed)

	ctx := context.Background()
	if _, err := ep.doRequest(ctx, "r", item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("doRequest = %v", err)
	}
	if _, err := ep.doStream(ctx, "r", item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("doStream = %v", err)
	}
	if _, err := ep.doChannel(ctx, "r", item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("doChannel = %v", err)
	}
	if _, err := ep.doSubscribe(ctx, "t", func(*Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("doSubscribe = %v", err)
	}
	if n := len(ep.snapshot()); n != 0 {
		t.Fatalf("failed sends leaked %d flows", n)
	}
}

// doRequest 的终结竞态：COMPLETE 先到且无响应 → ErrClosed；响应已入队
// 后 COMPLETE 再到 → select 双就绪随机，两种取序都必须交出响应帧。
func TestEndpointDoRequestTerminalRaces(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, nil)
	ctx := context.Background()

	// 场景 A：无响应的 COMPLETE → 流关闭语义
	errCh := make(chan error, 1)
	go func() {
		_, err := ep.doRequest(ctx, "r", item{N: 0})
		errCh <- err
	}()
	id := waitFlow(t, ep, 1)
	if err := ep.handleFrame(completeFrame(id)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; !errors.Is(err, ErrClosed) {
		t.Fatalf("request closed without response = %v, want ErrClosed", err)
	}

	// 场景 B：doRequest 卡在 sendRequestFrame（sendCh 填满），预先布置
	// 「响应已入队 + 流已终结」的双就绪状态，放行后 select 两种取序都
	// 必须交出响应帧（doneCh 分支的排空防丢帧）。
	for i := 0; i < 60; i++ {
		for len(tr.sendCh) < cap(tr.sendCh) {
			tr.sendCh <- &Frame{}
		}
		errCh := make(chan error, 1)
		msgCh := make(chan *Message, 1)
		go func() {
			m, err := ep.doRequest(ctx, "r", item{N: i})
			msgCh <- m
			errCh <- err
		}()
		id := waitFlow(t, ep, 1)
		_ = ep.handleFrame(testFrame(TypeResponse, id, 0, "", "", []byte(`{"n":1}`)))
		_ = ep.handleFrame(completeFrame(id))
		<-tr.sendCh // 放出一个空位：此刻 frames 与 doneCh 对 select 双就绪
		if err := <-errCh; err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if m := <-msgCh; m == nil {
			t.Fatalf("iteration %d: nil response", i)
		}
	}
}

// doSubscribe 的三种等待出口：对端 ERROR → 流错误；对端 COMPLETE →
// ErrClosed；ctx 取消 → ctx.Err() 并向对端发 CANCEL。
func TestEndpointDoSubscribeTerminals(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, nil)
	h := func(*Message) error { return nil }

	start := func(ctx context.Context) chan error {
		errCh := make(chan error, 1)
		go func() {
			_, err := ep.doSubscribe(ctx, "t", h)
			errCh <- err
		}()
		return errCh
	}

	// ERROR 终结
	errCh := start(context.Background())
	id := waitFlow(t, ep, 1)
	_ = ep.handleFrame(errorFrame(id, &Error{Code: CodeBusy, Message: "no"}))
	err := <-errCh
	var se *Error
	if !errors.As(err, &se) || se.Code != CodeBusy {
		t.Fatalf("subscribe killed by peer = %v, want BUSY", err)
	}

	// 无错误的 COMPLETE → 连接关闭语义
	errCh = start(context.Background())
	id = waitFlow(t, ep, 1)
	_ = ep.handleFrame(completeFrame(id))
	if err := <-errCh; !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe completed by peer = %v, want ErrClosed", err)
	}

	// ctx 取消
	ctx, cancel := context.WithCancel(context.Background())
	errCh = start(ctx)
	waitFlow(t, ep, 1)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe ctx cancel = %v, want context.Canceled", err)
	}
	frames := sentFrames(tr)
	if len(frames) == 0 || frames[len(frames)-1].Type != TypeCancel {
		t.Fatalf("ctx cancel must send CANCEL, got %d frames", len(frames))
	}
}

// ---- flow 终态语义 ----

// finish 幂等：终结一旦落定，迟到的 fail 不得覆盖错误状态。
func TestFlowFinishIdempotent(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	flw := newFlow(1, flowRequest, ep)
	ep.registerFlow(flw)
	flw.complete()
	flw.fail(&Error{Code: CodeInternal, Message: "late"})
	flw.mu.Lock()
	defer flw.mu.Unlock()
	if flw.err != nil {
		t.Fatalf("late fail overwrote terminal state: %v", flw.err)
	}
}

func TestReadStreamNilReceiverSafe(t *testing.T) {
	var s *ReadStream
	if m, ok := s.Next(context.Background()); m != nil || ok {
		t.Fatal("nil ReadStream Next must be inert")
	}
	if err := s.Cancel(); err != nil {
		t.Fatalf("nil ReadStream Cancel = %v", err)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("nil ReadStream Err = %v", err)
	}
}

// 取帧与终结的竞态：帧后带错终结 → 立即终结语义优先（两种 select 取序
// 都不得把帧当作流仍在继续的信号）；ctx 取消 → 本地 CANCEL 终结。
func TestReadStreamTerminalRaces(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)

	for i := 0; i < 20; i++ { // 双就绪随机：多轮迭代保证两入口都被走到
		flw := newFlow(ep.allocID(), flowStream, ep)
		ep.registerFlow(flw)
		s := &ReadStream{f: flw}
		flw.frames <- testFrame(TypeResponse, flw.id, 0, "", "", []byte(`{}`))
		flw.fail(&Error{Code: CodeCancelled, Message: "cancelled by peer"})
		if m, ok := s.Next(context.Background()); m != nil || ok {
			t.Fatalf("iteration %d: Next = %v,%v; want immediate termination", i, m, ok)
		}
	}
	var e *Error
	flw := newFlow(1, flowStream, ep)
	ep.registerFlow(flw)
	s := &ReadStream{f: flw}
	flw.fail(&Error{Code: CodeCancelled, Message: "cancelled by peer"})
	if !errors.As(s.Err(), &e) || e.Code != CodeCancelled {
		t.Fatalf("Err = %v, want CANCELLED", s.Err())
	}

	// ctx 取消 → 本地发 CANCEL 并终结流
	flw2 := newFlow(3, flowStream, ep)
	ep.registerFlow(flw2)
	s2 := &ReadStream{f: flw2}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if m, ok := s2.Next(ctx); m != nil || ok {
		t.Fatal("canceled ctx must end Next")
	}
	if !flw2.isDone() {
		t.Fatal("ctx cancel must terminate the flow")
	}
}

// 已终结流上的 Cancel 是无操作。
func TestReadStreamCancelAfterDone(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	flw := newFlow(1, flowStream, ep)
	ep.registerFlow(flw)
	s := &ReadStream{f: flw}
	flw.complete()
	if err := s.Cancel(); err != nil {
		t.Fatalf("Cancel on finished stream = %v, want nil", err)
	}
}

// Channel.Send 的两个错误出口：编码失败；信用闸门随连接关闭。
func TestChannelSendErrors(t *testing.T) {
	ep, tr := newTestEndpoint(t, true, func(c *Config) { c.Credit = 4 })
	ch := &Channel{f: newFlow(1, flowChannel, ep), ctx: context.Background()}

	if err := ch.Send(make(chan int)); err == nil {
		t.Fatal("expected marshal error")
	}
	tr.kill(ErrClosed) // 闸门随 kill 关闭
	if err := ch.Send(item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("send on closed gate = %v, want ErrClosed", err)
	}
}

// Channel.Receive 的终态：帧后带错终结（立即终结优先）、对端 COMPLETE
// 后排空余帧、ctx 取消。
func TestChannelReceiveTerminal(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)

	// 帧与错误双就绪：两种 select 取序都必须报错而非假装流还在
	for i := 0; i < 20; i++ {
		flw := newFlow(ep.allocID(), flowChannel, ep)
		ep.registerFlow(flw)
		ch := &Channel{f: flw, ctx: context.Background()}
		flw.frames <- testFrame(TypeResponse, flw.id, 0, "", "", []byte(`{}`))
		flw.fail(&Error{Code: CodeCancelled, Message: "x"})
		if m, err := ch.Receive(context.Background()); m != nil || err == nil {
			t.Fatalf("iteration %d: receive = %v,%v; want error", i, m, err)
		}
	}

	// ctx 取消
	flw2 := newFlow(3, flowChannel, ep)
	ep.registerFlow(flw2)
	ch2 := &Channel{f: flw2}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ch2.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive on canceled ctx = %v", err)
	}

	// COMPLETE 后的余帧排空：双就绪随机，两种取序都必须交付余帧
	for i := 0; i < 100; i++ {
		flw3 := newFlow(ep.allocID(), flowChannel, ep)
		ep.registerFlow(flw3)
		ch3 := &Channel{f: flw3}
		flw3.frames <- testFrame(TypeResponse, flw3.id, 0, "", "", []byte(`{"n":1}`))
		flw3.complete()
		m, err := ch3.Receive(context.Background())
		if err != nil {
			t.Fatalf("iteration %d: buffered frame lost after COMPLETE: %v", i, err)
		}
		if m == nil {
			t.Fatalf("iteration %d: nil message", i)
		}
	}
}

// Cancel 幂等：第二次调用是 no-op。
func TestChannelCancelIdempotent(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	ch := &Channel{f: newFlow(1, flowChannel, ep), ctx: context.Background()}
	if err := ch.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := ch.Cancel(); err != nil {
		t.Fatalf("second Cancel = %v, want nil", err)
	}
}

// ---- 订阅消费 ----

// f == nil 的订阅（构造期）直接退出消费循环。
func TestSubscriptionConsumeNilFlow(_ *testing.T) {
	sub := &Subscription{topic: "t"}
	sub.consumeWg.Add(1)
	sub.consume()
	sub.consumeWg.Wait()
}

// 回调错误只记日志不回帧；主循环与终结后排空两个入口都要覆盖
// （预载帧后终结流，select 双就绪随机选择入口，迭代保证两处都走到）。
func TestSubscriptionCallbackErrorsLogged(t *testing.T) {
	var log syncLogger
	ep, tr := newTestEndpoint(t, true, func(c *Config) { c.Logger = &log })
	calls := make(chan struct{}, 8)
	h := func(*Message) error {
		calls <- struct{}{}
		return errors.New("callback failed")
	}

	const iterations = 20
	for i := 0; i < iterations; i++ {
		flw := newFlow(ep.allocID(), flowSubscribe, ep)
		ep.registerFlow(flw)
		sub := &Subscription{topic: "t", h: h}
		sub.mu.Lock()
		sub.f = flw
		sub.mu.Unlock()
		flw.frames <- testFrame(TypePublish, flw.id, 0, "", "t", []byte(`{}`))
		flw.frames <- testFrame(TypePublish, flw.id, 0, "", "t", []byte(`{}`))
		flw.complete()
		sub.consumeWg.Add(1)
		go sub.consume()
		for k := 0; k < 2; k++ {
			select {
			case <-calls:
			case <-time.After(2 * time.Second):
				t.Fatalf("iteration %d: callback %d not invoked", i, k)
			}
		}
		sub.consumeWg.Wait()
	}
	log.waitFor(t, 2*iterations)
	if n := len(sentFrames(tr)); n != 0 {
		t.Fatalf("callback errors answered with %d frames, want none", n)
	}
}

// 流终结后的投递必须被丢弃并返回 false：填满 frames 缓冲让 doneCh 成为
// select 唯一就绪分支（否则双就绪随机会让覆盖抖动）。
func TestFlowDeliverAfterDone(t *testing.T) {
	ep, _ := newTestEndpoint(t, true, nil)
	flw := newFlow(7, flowStream, ep)
	for i := 0; i < cap(flw.frames); i++ {
		flw.frames <- &Frame{}
	}
	flw.finish(nil)
	if flw.deliver(&Frame{}) {
		t.Fatal("deliver after done must be dropped")
	}
}

// TestRouteTableDuplicateRegister：重复注册覆盖旧值（table.go 注释
// 承诺的语义）——同一路由注册两次，查找必须命中后者。
func TestRouteTableDuplicateRegister(t *testing.T) {
	tbl := newRouteTable()
	tbl.Handle("svc", func(*Request) (any, error) { return "first", nil })
	tbl.Handle("svc", func(*Request) (any, error) { return "second", nil })
	e, ok := tbl.lookupRoute("svc")
	if !ok || e.handle == nil {
		t.Fatal("route missing after duplicate register")
	}
	got, err := e.handle(&Request{})
	if err != nil || got != "second" {
		t.Fatalf("duplicate register = %q,%v, want second", got, err)
	}
}
