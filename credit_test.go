package jsonstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// creditGate 的边界契约：nil 安全、关闭语义、阻塞与取消。
func TestCreditGateNilSafe(t *testing.T) {
	var g *creditGate
	g.add(1)  // 不 panic
	g.close() // 不 panic
	if err := g.take(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil gate take = %v, want ErrClosed", err)
	}
}

func TestCreditGateAddIgnoresNonPositive(t *testing.T) {
	g := newCreditGate(1)
	g.add(0)
	g.add(-3)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n != 1 {
		t.Fatalf("credit after non-positive add = %d, want 1", g.n)
	}
}

func TestCreditGateTakeBlocksThenClosed(t *testing.T) {
	g := newCreditGate(0)
	// 耗尽 + ctx 取消 → ctx.Err()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := g.take(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("take with exhausted gate = %v, want DeadlineExceeded", err)
	}
	// close 唤醒阻塞者 → ErrClosed
	done := make(chan error, 1)
	go func() { done <- g.take(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	g.close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("take after close = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked take not woken by close")
	}
	// 关闭后再取立即失败；close 幂等
	if err := g.take(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("take on closed gate = %v, want ErrClosed", err)
	}
	g.close()
	// 已关闭的 gate 补充令牌不唤醒任何人（已无人等待）
	g.add(5)
	g.mu.Lock()
	n := g.n
	g.mu.Unlock()
	if n != 5 {
		t.Fatalf("credit on closed gate = %d, want 5 (add still books)", n)
	}
}

func TestCreditGateAddWakesTaker(t *testing.T) {
	g := newCreditGate(0)
	done := make(chan error, 1)
	go func() { done <- g.take(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	g.add(1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("take after add = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked take not woken by add")
	}
}
