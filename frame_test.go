package jsonstream

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// ---- 测试辅助 ----

func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i * 7)
	}
	return key
}

func shortConfig() Config {
	cfg := DefaultConfig()
	cfg.Heartbeat = time.Second
	b := 5 * time.Millisecond
	cfg.BackoffInitial = b
	cfg.BackoffMax = 50 * time.Millisecond
	return cfg
}

func startTestServer(t *testing.T, cfg Config, setup func(*Server)) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(ln, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(srv)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, ln.Addr().String()
}

func dialTest(t *testing.T, addr string, cfg Config) *Client {
	t.Helper()
	c, err := Dial(context.Background(), addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// ---- 帧编解码 ----

func TestFrameRoundTrip(t *testing.T) {
	cases := []*Frame{
		{Header: Header{Version: ProtocolVersion, Type: TypePing}},
		{Header: Header{Version: ProtocolVersion, Type: TypePong}},
		{Header: Header{Version: ProtocolVersion, Type: TypeRequest, StreamID: 7}, Metadata: []byte(`{"route":"echo"}`), Payload: []byte(`{"hello":"world"}`)},
		{Header: Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: 9}, Payload: []byte(`[1,2,3]`)},
		{Header: Header{Version: ProtocolVersion, Type: TypeError, StreamID: 3}, Payload: []byte(`{"code":2,"message":"nope"}`)},
		{Header: Header{Version: ProtocolVersion, Type: TypePublish, StreamID: 1}, Metadata: []byte(`{"topic":"ticks"}`)},
		{Header: Header{Version: ProtocolVersion, Type: TypeRequest, StreamID: 0xFFFFFFFF, Flags: FlagStream | FlagChannel}, Payload: bytes.Repeat([]byte("x"), 1<<16)},
	}
	for i, f := range cases {
		buf, err := f.appendTo(nil)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		got, err := ReadFrame(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		// 编码端对带 Metadata 的帧自动置位 FlagHasMeta，对比时排除该位。
		wantFlags := f.Flags &^ FlagHasMeta
		gotFlags := got.Flags &^ FlagHasMeta
		if got.Version != f.Version || got.Type != f.Type || got.StreamID != f.StreamID || gotFlags != wantFlags {
			t.Fatalf("case %d header mismatch: got %+v want %+v", i, got.Header, f.Header)
		}
		if !bytes.Equal(got.Metadata, f.Metadata) {
			t.Fatalf("case %d metadata mismatch", i)
		}
		if !bytes.Equal(got.Payload, f.Payload) {
			t.Fatalf("case %d payload mismatch", i)
		}
	}
}

func TestFrameRejectsBadMagic(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}}
	buf, _ := f.appendTo(nil)
	buf[0] = 'X'
	if _, err := ReadFrame(bytes.NewReader(buf)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestFrameRejectsBadVersion(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}}
	buf, _ := f.appendTo(nil)
	buf[2] = 9
	if _, err := ReadFrame(bytes.NewReader(buf)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestFrameRejectsReservedFlags(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing, Flags: 0x80}}
	buf, _ := f.appendTo(nil)
	if _, err := ReadFrame(bytes.NewReader(buf)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestFrameRejectsOversizePayload(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypeRequest, StreamID: 1}}
	buf, _ := f.appendTo(nil)
	// 改写长度字段为 MaxPayloadSize+1，不实际提供数据。
	binary := []byte(buf)
	n := MaxPayloadSize + 1
	binary[10] = byte(n >> 24)
	binary[11] = byte(n >> 16)
	binary[12] = byte(n >> 8)
	binary[13] = byte(n)
	if _, err := ReadFrame(bytes.NewReader(binary)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestFrameRejectsTruncated(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: 2}, Payload: []byte("0123456789")}
	buf, _ := f.appendTo(nil)
	for _, cut := range []int{4, 10, 14, len(buf) - 1} {
		if _, err := ReadFrame(bytes.NewReader(buf[:cut])); !errors.Is(err, ErrMalformed) {
			t.Fatalf("cut=%d: want ErrMalformed, got %v", cut, err)
		}
	}
}

func TestFrameEncodeRejectsOversize(t *testing.T) {
	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypeResponse}, Payload: make([]byte, MaxPayloadSize+1)}
	if _, err := f.appendTo(nil); err == nil {
		t.Fatal("want error for oversize payload")
	}
}

// ---- 变换管线（压缩/加密） ----

func transformRoundTrip(t *testing.T, cfg Config, payload []byte) {
	t.Helper()
	tx, err := newTransformer(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	out, flags, err := tx.outbound(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tx.inbound(flags, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d bytes want %d", len(got), len(payload))
	}
}

func TestTransformPlain(t *testing.T) {
	tx, _ := newTransformer(&Config{})
	out, flags, err := tx.outbound([]byte(`{"a":1}`))
	if err != nil || flags != 0 {
		t.Fatalf("plain transform should be identity, flags=%d err=%v", flags, err)
	}
	if !bytes.Equal(out, []byte(`{"a":1}`)) {
		t.Fatal("plain payload changed")
	}
}

func TestTransformCompressRoundTrip(t *testing.T) {
	cfg := Config{Compress: true}
	transformRoundTrip(t, cfg, bytes.Repeat([]byte(`{"message":"hello json stream"},`), 200))
}

func TestTransformEncryptRoundTrip(t *testing.T) {
	cfg := Config{Encrypt: true, Key: testKey()}
	transformRoundTrip(t, cfg, []byte(`{"secret":[1,2,3]}`))
}

func TestTransformBothRoundTrip(t *testing.T) {
	cfg := Config{Compress: true, Encrypt: true, Key: testKey()}
	payload := bytes.Repeat([]byte(`{"k":"v","n":42},`), 100)
	tx, _ := newTransformer(&cfg)
	out, flags, err := tx.outbound(payload)
	if err != nil {
		t.Fatal(err)
	}
	if flags&(FlagCompressed|FlagEncrypted) != FlagCompressed|FlagEncrypted {
		t.Fatalf("want both flags set, got %02x", flags)
	}
	// 加密开销 = 12B nonce + 16B GCM tag。
	if len(out) >= len(payload)+28 {
		t.Fatalf("compressed+encrypted frame suspiciously large: %d > %d", len(out), len(payload)+28)
	}
	got, err := tx.inbound(flags, out)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("round-trip failed: %v", err)
	}
}

func TestTransformSmallPayloadNotCompressed(t *testing.T) {
	tx, _ := newTransformer(&Config{Compress: true})
	_, flags, err := tx.outbound([]byte(`tiny`))
	if err != nil {
		t.Fatal(err)
	}
	if flags&FlagCompressed != 0 {
		t.Fatal("small payload should skip compression")
	}
}

func TestTransformWrongKey(t *testing.T) {
	tx, _ := newTransformer(&Config{Encrypt: true, Key: testKey()})
	out, flags, err := tx.outbound([]byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	other, _ := newTransformer(&Config{Encrypt: true, Key: bytes.Repeat([]byte{9}, 32)})
	if _, err := other.inbound(flags, out); err == nil {
		t.Fatal("decrypt with wrong key should fail")
	}
}

func TestTransformRejectsUnexpectedFlags(t *testing.T) {
	plain, _ := newTransformer(&Config{})
	if _, err := plain.inbound(FlagEncrypted, make([]byte, 32)); err == nil {
		t.Fatal("encrypted frame against disabled config should fail")
	}
	if _, err := plain.inbound(FlagCompressed, []byte{0x00}); err == nil {
		t.Fatal("compressed frame against disabled config should fail")
	}
}

// ---- 握手 ----

func TestHandshakeAuthDenied(t *testing.T) {
	_, addr := startTestServer(t, shortConfig(), func(s *Server) {
		s.OnAuth(func(cj *connectJSON) error {
			if cj.Auth != "secret" {
				return &Error{Code: CodeAuthDenied, Message: "bad token"}
			}
			return nil
		})
	})
	cfg := shortConfig()
	cfg.Auth = "wrong"
	if _, err := Dial(context.Background(), addr, cfg); err == nil {
		t.Fatal("want auth failure")
	}
	cfg.Auth = "secret"
	c := dialTest(t, addr, cfg)
	if _, err := c.Request(context.Background(), "ping", nil); err == nil {
		t.Fatal("request on unauthorized route should fail with NOT_FOUND, not silently")
	}
}

func TestHandshakeVersionMismatch(t *testing.T) {
	_, addr := startTestServer(t, shortConfig(), nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	bad := &connectJSON{Version: int(ProtocolVersion) + 1}
	f, _ := connectFrame(bad)
	if err := writeOnce(conn, f); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != TypeError || errDecode(got.Payload).Code != CodeProtocol {
		t.Fatalf("want ERROR(PROTOCOL), got %s", got.Type)
	}
}
