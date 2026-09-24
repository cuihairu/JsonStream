package jsonstream

import (
	"context"
	"sync"
)

// creditGate 是发送侧的信用令牌闸门：接收方每授权 n 条，这里增加 n 个
// 令牌；发送方发出一条受背压约束的数据帧前取走一个，取不到就阻塞
// （不丢弃、不无限排队——背压的意义就是把"消费不动"传导回来）。
type creditGate struct {
	mu     sync.Mutex
	n      int
	notify chan struct{} // cap 1 的信号量，避免每令牌一个 channel 元素
	closed bool
}

func newCreditGate(initial int) *creditGate {
	return &creditGate{n: initial, notify: make(chan struct{}, 1)}
}

func (g *creditGate) add(n int) {
	if g == nil || n <= 0 {
		return
	}
	g.mu.Lock()
	g.n += n
	closed := g.closed
	g.mu.Unlock()
	if !closed {
		select {
		case g.notify <- struct{}{}:
		default:
		}
	}
}

// take 取走一个令牌；额度耗尽时阻塞直到补充、ctx 取消或连接关闭。
func (g *creditGate) take(ctx context.Context) error {
	if g == nil { // 与 add/close 一致：nil gate 视为已关闭
		return ErrClosed
	}
	for {
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return ErrClosed
		}
		if g.n > 0 {
			g.n--
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()
		select {
		case <-g.notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *creditGate) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	select {
	case g.notify <- struct{}{}:
	default:
	}
}
