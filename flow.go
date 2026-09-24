package jsonstream

import (
	"context"
	"io"
	"sync"
)

// flowKind 描述一条流的交互模式。
type flowKind uint8

const (
	flowRequest   flowKind = iota // 单帧响应
	flowStream                    // 多帧响应 + COMPLETE
	flowChannel                   // 双向
	flowSubscribe                 // 订阅投递
)

// flowChanBuffer 是单条流入站帧的缓冲深度。启用背压时在途条数受
// credit 窗口约束（窗口通常远小于此值），缓冲不会积压；未启用背压时
// 缓冲写满会反压读循环，是"关闭背压 = 信任 TCP 兜底"的兜底行为。
const flowChanBuffer = 32

// flow 是一条活跃流的本端视图：既表示本端发起的流，也表示本端正在
// 响应的流。生命周期终结以 doneCh 关闭为准。
type flow struct {
	id     uint32
	kind   flowKind
	ep     *endpoint
	frames chan *Frame

	mu     sync.Mutex
	done   bool
	err    *Error
	doneCh chan struct{}

	ackCh chan struct{} // SUBACK 信号（仅订阅流使用）

	inflight      int // 已收到未交付的数据帧数（背压记账）
	pendingCredit int // 已交付未回授的额度

	// 被动流的 handler 载体：prepareRequest 在 readLoop 同步路径注册 flow
	// 时挂上，runRequest 在异步阶段执行。二者由 dispatch 与 goroutine 分阶段
	// 使用，注册先行于任何后续帧的 dispatch，无并发访问。
	reqEntry routeEntry
	chEntry  func(*Channel) error
}

func newFlow(id uint32, kind flowKind, ep *endpoint) *flow {
	return &flow{
		id:     id,
		kind:   kind,
		ep:     ep,
		frames: make(chan *Frame, flowChanBuffer),
		doneCh: make(chan struct{}),
		ackCh:  make(chan struct{}),
	}
}

func (flw *flow) ctx() context.Context { return streamContext{flw} }

func (flw *flow) isDone() bool {
	select {
	case <-flw.doneCh:
		return true
	default:
		return false
	}
}

// deliver 投递一帧入站数据；流已终结时丢弃并返回 false。
func (flw *flow) deliver(f *Frame) bool {
	select {
	case flw.frames <- f:
		return true
	case <-flw.doneCh:
		return false
	}
}

// creditIn 在收到受背压约束的数据帧时记账。
func (flw *flow) creditIn() {
	if flw.currentEP().creditWindow <= 0 || flw.kind == flowRequest {
		return
	}
	flw.mu.Lock()
	flw.inflight++
	flw.mu.Unlock()
}

// release 在消息交付应用后归还额度；累计到半窗时批量回授 CREDIT 帧
// （HTTP/2 WINDOW_UPDATE 的惯例位置：太早背压失效，太晚吞吐骤降）。
func (flw *flow) release(n int) {
	ep := flw.currentEP()
	if ep.creditWindow <= 0 || flw.kind == flowRequest {
		return
	}
	flw.mu.Lock()
	if flw.inflight > n {
		flw.inflight -= n
	} else {
		flw.inflight = 0
	}
	flw.pendingCredit += n
	var grant int
	if flw.pendingCredit >= ep.creditFlushAt {
		grant = flw.pendingCredit
		flw.pendingCredit = 0
	}
	flw.mu.Unlock()
	if grant > 0 {
		ep.tr.grantCredit(flw.id, grant)
	}
}

func (flw *flow) complete() { flw.finish(nil) }

func (flw *flow) fail(e *Error) { flw.finish(e) }

// setEndpoint 会话恢复迁移时换绑当前连接的 endpoint。读写都以 mu 保护：
// 迁移与用户 goroutine 的出站（Send/Cancel/Close）及消费侧记账
// （creditIn/release）和终结（finish）之间没有任何别的同步关系。
func (flw *flow) setEndpoint(ep *endpoint) {
	flw.mu.Lock()
	flw.ep = ep
	flw.mu.Unlock()
}

func (flw *flow) currentEP() *endpoint {
	flw.mu.Lock()
	defer flw.mu.Unlock()
	return flw.ep
}

func (flw *flow) finish(e *Error) {
	flw.mu.Lock()
	if flw.done {
		flw.mu.Unlock()
		return
	}
	flw.done = true
	flw.err = e
	close(flw.doneCh)
	ep := flw.ep
	flw.mu.Unlock()
	ep.unregisterFlow(flw)
}

func (flw *flow) ack() {
	select {
	case <-flw.ackCh:
	default:
		close(flw.ackCh)
	}
}

// ---- 发起方视角的流读取器 ----

// ReadStream 是流式响应（Flags.Stream）的读取端。
type ReadStream struct {
	f *flow
}

// Next 返回下一帧；正常结束（COMPLETE/CANCEL）返回 false 且 Err() 为 nil，
// 出错返回 false 且 Err() 非 nil。
//
// 终结帧与数据帧的交付是异步的：doneCh 关闭时 frames 里可能还有余帧，
// 因此终结分支必须先排空 chan 再返回 false——select 双就绪是随机选择，
// 直接返回会随机丢帧。
func (s *ReadStream) Next(ctx context.Context) (*Message, bool) {
	if s == nil {
		return nil, false
	}
	select {
	case f := <-s.f.frames:
		s.f.release(1)
		// 取帧后流可能已带着错误终结（CANCEL/ERROR 与取帧竞态）：
		// 立即终结语义优先，返回剩余帧会让调用方误以为流仍在继续。
		if s.f.isDone() && s.f.err != nil {
			return nil, false
		}
		return toMessage(f), true
	case <-ctx.Done():
		_ = s.Cancel() // 超时即主动取消；连接已死时流随之终结，错误无处可报
		return nil, false
	case <-s.f.doneCh:
		// 正常 COMPLETE 终结后先排空余帧（终结与交付异步，select 双就绪
		// 随机选择，直接返回会随机丢帧）；CANCEL/ERROR 属立即终结，
		// 在途帧语义上作废，不排空。
		if s.f.err != nil {
			return nil, false
		}
		select {
		case f := <-s.f.frames:
			s.f.release(1)
			return toMessage(f), true
		default:
			return nil, false
		}
	}
}

// Err 返回终结错误；正常终结返回 nil。
func (s *ReadStream) Err() error {
	if s == nil || s.f.err == nil {
		return nil
	}
	return s.f.err
}

// Cancel 取消流：发 CANCEL 并本地终结。帧经当前 endpoint 发送——会话恢复
// 迁移后是重连的新连接，构造时的 endpoint 已死（陈旧 ep 引用只会让 CANCEL
// 静默丢失，对端 handler 永远收不到取消）。
func (s *ReadStream) Cancel() error {
	if s == nil || s.f.isDone() {
		return nil
	}
	ep := s.f.currentEP()
	_ = ep.tr.send(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeCancel, StreamID: s.f.id}})
	s.f.fail(&Error{Code: CodeCancelled, Message: "cancelled by caller"})
	return nil
}

// ---- 双工通道 ----

// Channel 是双工（Flags.Channel）流的两端通用视图：双方都可
// Send/Receive，Close 发送 COMPLETE（半关闭："我说完了"），
// Cancel 立即终结整条流。
type Channel struct {
	f   *flow
	ctx context.Context
}

// Send 发送一帧（受背压约束；额度耗尽时阻塞直到补充或 ctx/流终结）。
// 出站一律经当前 endpoint：会话恢复迁移后是重连的新连接，构造时的
// endpoint 已死。
func (c *Channel) Send(v any) error {
	data, err := jsonMarshal(v)
	if err != nil {
		return err
	}
	if c.f.isDone() {
		return ErrClosed
	}
	ep := c.f.currentEP()
	if err := ep.tr.takeCredit(c.ctx); err != nil {
		return err
	}
	return ep.emit(&Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: c.f.id},
		Payload: data,
	})
}

// Receive 接收对端一帧。对端 COMPLETE 后返回 io.EOF；流出错返回错误。
func (c *Channel) Receive(ctx context.Context) (*Message, error) {
	select {
	case f := <-c.f.frames:
		c.f.release(1)
		if c.f.isDone() && c.f.err != nil {
			return nil, c.f.err
		}
		return toMessage(f), nil
	case <-c.f.doneCh:
		if c.f.err != nil {
			return nil, c.f.err
		}
		// 对端正常 COMPLETE：先排空余帧（终结与交付异步），再报 EOF。
		select {
		case f := <-c.f.frames:
			c.f.release(1)
			return toMessage(f), nil
		default:
			return nil, io.EOF
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close 半关闭：发送 COMPLETE 表示本端不再发送；仍可 Receive 直到对端关闭。
func (c *Channel) Close() error {
	if c.f.isDone() {
		return nil
	}
	ep := c.f.currentEP()
	_ = ep.emit(completeFrame(c.f.id))
	c.f.complete()
	return nil
}

// Cancel 立即终结整条流。
func (c *Channel) Cancel() error {
	if c.f.isDone() {
		return nil
	}
	ep := c.f.currentEP()
	_ = ep.tr.send(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeCancel, StreamID: c.f.id}})
	c.f.fail(&Error{Code: CodeCancelled, Message: "cancelled"})
	return nil
}

// ---- 订阅 ----

// Subscription 是一条主题订阅。回调错误只记日志不回帧（投递方向上
// 回应会形成"响应的响应"，见 docs/protocol.md §7.8）。
type Subscription struct {
	topic string
	h     func(*Message) error

	mu sync.Mutex
	f  *flow

	closeOnce sync.Once
	consumeWg sync.WaitGroup
}

func (s *Subscription) consume() {
	defer s.consumeWg.Done()
	for {
		s.mu.Lock()
		f := s.f
		s.mu.Unlock()
		if f == nil {
			return
		}
		select {
		case frm := <-f.frames:
			f.release(1)
			if err := s.h(toMessage(frm)); err != nil {
				f.currentEP().log.Printf("jsonstream: subscription %q callback error: %v", s.topic, err)
			}
		case <-f.doneCh:
			// 排空终结前已入队的余帧后再退出。
			for {
				select {
				case frm := <-f.frames:
					f.release(1)
					if err := s.h(toMessage(frm)); err != nil {
						f.currentEP().log.Printf("jsonstream: subscription %q callback error: %v", s.topic, err)
					}
				default:
					return
				}
			}
		}
	}
}

// Close 退订：发 UNSUBSCRIBE 并终结本地流。帧必须经当前 endpoint 发送
// ——会话恢复后订阅流已迁移到新连接，构造时的旧连接上发送只会得到
// ErrClosed，退订帧静默丢失（服务端永远摘不掉该订阅）。
func (s *Subscription) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		f := s.f
		s.mu.Unlock()
		if f != nil {
			ep := f.currentEP()
			_ = ep.tr.send(&Frame{
				Header:   Header{Version: ProtocolVersion, Type: TypeUnsubscribe, StreamID: f.id},
				Metadata: encodeMeta("", s.topic),
			})
			f.finish(nil)
		}
	})
	return nil
}
