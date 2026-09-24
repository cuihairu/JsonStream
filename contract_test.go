package jsonstream

import (
	"context"
	"errors"
	"testing"
)

// 公共 API 的对外契约：名字映射、日志文案、context 适配器。这些零逻辑
// 方法被日志、断言与下游代码依赖，改动应显式可见。

func TestFrameTypeStringNames(t *testing.T) {
	names := map[FrameType]string{
		TypeConnect:     "CONNECT",
		TypeConnAck:     "CONNACK",
		TypePing:        "PING",
		TypePong:        "PONG",
		TypeRequest:     "REQUEST",
		TypeResponse:    "RESPONSE",
		TypeComplete:    "COMPLETE",
		TypeCancel:      "CANCEL",
		TypeOneWay:      "ONEWAY",
		TypeError:       "ERROR",
		TypeSubscribe:   "SUBSCRIBE",
		TypeSubAck:      "SUBACK",
		TypeUnsubscribe: "UNSUBSCRIBE",
		TypePublish:     "PUBLISH",
		TypeCredit:      "CREDIT",
	}
	for ft, want := range names {
		if got := ft.String(); got != want {
			t.Errorf("FrameType(%d).String() = %q, want %q", uint8(ft), got, want)
		}
	}
	if got := FrameType(99).String(); got != "TYPE(99)" {
		t.Fatalf("unknown type renders %q, want TYPE(99)", got)
	}
}

func TestFrameStringFormat(t *testing.T) {
	f := &Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeRequest, Flags: FlagHasMeta, StreamID: 7},
		Payload: []byte("abc"),
	}
	want := "<REQUEST sid=7 flags=04 len=3>"
	if got := f.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestRequestContext(t *testing.T) {
	type keyT struct{}
	ctx := context.WithValue(context.Background(), keyT{}, "v")
	r := &Request{Message: &Message{}, ctx: ctx}
	if r.Context().Value(keyT{}) != "v" {
		t.Fatal("Request.Context must return the bound context")
	}
}

func TestStreamContextAdapter(t *testing.T) {
	live := &flow{doneCh: make(chan struct{})}
	sc := streamContext{f: live}
	if _, ok := sc.Deadline(); ok {
		t.Fatal("stream context has no deadline")
	}
	if sc.Value("k") != nil {
		t.Fatal("stream context carries no values")
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("live stream Err = %v, want nil", err)
	}

	// 以错误终结：Done 关闭，Err 透出终结原因
	closed := &flow{doneCh: make(chan struct{}), err: &Error{Code: CodeCancelled, Message: "cancelled"}}
	close(closed.doneCh)
	sc2 := streamContext{f: closed}
	select {
	case <-sc2.Done():
	default:
		t.Fatal("Done must be closed once the stream has ended")
	}
	var je *Error
	if err := sc2.Err(); !errors.As(err, &je) || je.Code != CodeCancelled {
		t.Fatalf("Err after failed stream = %v, want CANCELLED", err)
	}

	// 无错误终结（COMPLETE）：Err 给出兜底解释而非 nil
	ended := &flow{doneCh: make(chan struct{})}
	close(ended.doneCh)
	err := (streamContext{f: ended}).Err()
	var je2 *Error
	if !errors.As(err, &je2) || je2.Code != CodeCancelled || je2.Message != "stream closed" {
		t.Fatalf("Err after completed stream = %v, want CANCELLED 'stream closed'", err)
	}
}
