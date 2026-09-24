package jsonstream

import (
	"context"
	"encoding/json"
	"time"
)

// Message 是交付给应用的数据单元：一帧的业务语义视图。
type Message struct {
	StreamID uint32
	// Route 是 REQUEST/ONEWAY 的路由名（来自 Metadata）。
	Route string
	// Topic 是 PUBLISH/SUBSCRIBE 的主题名（来自 Metadata）。
	Topic   string
	Payload json.RawMessage
}

// Decode 把 payload 反序列化到 v；空 payload 什么都不做。
func (m *Message) Decode(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}

func toMessage(f *Frame) *Message {
	meta := decodeMeta(f.Metadata)
	return &Message{
		StreamID: f.StreamID,
		Route:    meta.Route,
		Topic:    meta.Topic,
		Payload:  json.RawMessage(f.Payload),
	}
}

// Request 是路由 handler 收到的请求。Context 在所在流被取消/终结后 Done，
// 长处理可据此提前退出。
type Request struct {
	*Message
	ctx context.Context
}

// Context 返回与所在流绑定的 context。
func (r *Request) Context() context.Context { return r.ctx }

// streamContext 把流的终结事件包装成 context.Context。
type streamContext struct{ f *flow }

func (c streamContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c streamContext) Done() <-chan struct{}       { return c.f.doneCh }
func (c streamContext) Value(any) any               { return nil }
func (c streamContext) Err() error {
	select {
	case <-c.f.doneCh:
		if c.f.err != nil {
			return c.f.err
		}
		return &Error{Code: CodeCancelled, Message: "stream closed"}
	default:
		return nil
	}
}

// Emitter 由流式 handler 用于逐帧下发（受背压约束，额度耗尽时阻塞）。
type Emitter interface {
	Emit(v any) error
}

type emitter struct {
	ep  *endpoint
	flw *flow
}

func (e *emitter) Emit(v any) error {
	data, err := jsonMarshal(v)
	if err != nil {
		return err
	}
	if err := e.ep.tr.takeCredit(e.flw.ctx()); err != nil {
		return err
	}
	return e.ep.emit(&Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: e.flw.id},
		Payload: data,
	})
}

// asStreamError 把 handler 返回的任意 error 归一为错误帧载荷：
// *Error 原样保留错误码，其余按 INTERNAL 处理。
func asStreamError(err error) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

func errorFrame(streamID uint32, e *Error) *Frame {
	return &Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeError, StreamID: streamID},
		Payload: errEncode(e),
	}
}

func completeFrame(streamID uint32) *Frame {
	return &Frame{Header: Header{Version: ProtocolVersion, Type: TypeComplete, StreamID: streamID}}
}
