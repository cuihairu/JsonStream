package jsonstream

import (
	"errors"
	"testing"
)

// 空 payload 的 Decode 是无操作（如 ONEWAY/CANCEL 等无载荷帧）。
func TestMessageDecodeEmptyPayload(t *testing.T) {
	m := &Message{}
	var v item
	if err := m.Decode(&v); err != nil || v.N != 0 {
		t.Fatalf("Decode(empty) = %v, %+v", err, v)
	}
}

// Emitter 编码失败立即返回，不触碰 transport。
func TestEmitterMarshalError(t *testing.T) {
	e := &emitter{}
	if err := e.Emit(make(chan int)); err == nil {
		t.Fatal("expected marshal error")
	}
}

// 背压额度随连接关闭而耗尽：Emit 以 ErrClosed 终止（挂起而非漏发）。
func TestEmitterTakeCreditError(t *testing.T) {
	cfg := shortConfig()
	cfg.Credit = 4
	tr, _ := newTestTransport(t, cfg.normalized(), cfg.Credit)
	tr.kill(ErrClosed) // 闸门随连接关闭
	ep := &endpoint{tr: tr}
	e := &emitter{ep: ep, flw: newFlow(1, flowStream, ep)}
	if err := e.Emit(item{N: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("emit on closed gate = %v, want ErrClosed", err)
	}
}

// 流正常终结（complete，无错误）后再 Emit：立即以 CANCELLED 失败，不往
// 已终结的流上发帧（handler complete 后继续 emit 的收尾路径）。
func TestEmitterEmitAfterComplete(t *testing.T) {
	cfg := shortConfig()
	tr, _ := newTestTransport(t, cfg.normalized(), 0)
	ep := &endpoint{tr: tr, cfg: cfg.normalized()}
	e := &emitter{ep: ep, flw: newFlow(1, flowStream, ep)}
	e.flw.complete()
	err := e.Emit(item{N: 1})
	var je *Error
	if !errors.As(err, &je) || je.Code != CodeCancelled {
		t.Fatalf("emit after complete = %v, want CANCELLED", err)
	}
}
