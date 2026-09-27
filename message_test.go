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

// TestTypedNilErrorPayloadRoundTrip 钉住 DESIGN §7 末条与 §8.3 记录的
// typed-nil 降级契约（此前只有文档、无测试守着）：应用写
// `var e *Error; return nil, e` 时 err != nil 为真走错误分支，但
// asStreamError 的类型断言对 typed-nil 成功并原样返回 nil；errorFrame
// 把 nil *Error 编码成字面量 "null"；对端 errDecode 因解码成功但
// Code==0 归一为 INTERNAL。结论「降级而非崩溃、错误信息丢失」逐环成立。
// （改形方案已变异反证：守卫若写成 `ok && e != nil`，归一臂会去调
// typed-nil 的 Error()，nil 解引用当场 panic——单元层直接炸，端到端层
// 靠 runRequest 的 recover 兜底才没有崩进程。）
func TestTypedNilErrorPayloadRoundTrip(t *testing.T) {
	var typedNil *Error
	// 环节一：断言命中（ok==true）且返回的正是 nil 指针——盲区所在。
	if got := asStreamError(typedNil); got != nil {
		t.Fatalf("asStreamError(typed-nil *Error) = %+v, want nil", got)
	}
	// 环节二：nil *Error 上线即 "null"（json.Marshal 对 nil 指针产出
	// null，errEncode 丢弃的 error 恒为 nil）。
	f := errorFrame(7, asStreamError(typedNil))
	if f.Type != TypeError || f.StreamID != 7 {
		t.Fatalf("errorFrame header = %v/sid %d, want ERROR/sid 7", f.Type, f.StreamID)
	}
	if string(f.Payload) != "null" {
		t.Fatalf("error payload = %q, want \"null\"", f.Payload)
	}
	// 环节三：对端归一。"null" 是合法 JSON，触发的是 Code==0 分支而非
	// 解码失败分支，但出口同为 INTERNAL 兜底——"信息丢失"指线上根本没有
	// 信息可丢（typed-nil 无 code 无 message）。
	e := errDecode(f.Payload)
	if e.Code != CodeInternal || e.Message != "undecodable error payload" {
		t.Fatalf("errDecode(\"null\") = %+v, want INTERNAL 归一形态", e)
	}
	// 对照组：非 nil 的 *Error 原样透传，code/message 俱在——上段的 nil
	// 返回是 typed-nil 断言命中的专属盲区，不是归一逻辑的普遍行为。
	keep := &Error{Code: CodeBusy, Message: "explicit"}
	if got := asStreamError(keep); got != keep {
		t.Fatalf("asStreamError(non-nil *Error) = %+v, want 同一指针透传", got)
	}
}
