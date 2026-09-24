package jsonstream

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
)

// ---- 帧层 ----

func benchmarkFrameRoundTrip(b *testing.B, payloadSize int) {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	f := &Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: 7},
		Metadata: encodeMeta("svc/user.get", ""),
		Payload:  payload,
	}
	buf, err := f.appendTo(nil)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := ReadFrame(bytes.NewReader(buf))
		if err != nil {
			b.Fatal(err)
		}
		if got.StreamID != 7 {
			b.Fatal("bad stream id")
		}
	}
}

func BenchmarkFrameRoundTrip64B(b *testing.B)   { benchmarkFrameRoundTrip(b, 64) }
func BenchmarkFrameRoundTrip1KiB(b *testing.B)  { benchmarkFrameRoundTrip(b, 1<<10) }
func BenchmarkFrameRoundTrip64KiB(b *testing.B) { benchmarkFrameRoundTrip(b, 1<<16) }

// ---- 变换管线 ----

func benchmarkTransform(b *testing.B, compress, encrypt bool) {
	cfg := shortConfig()
	cfg.Compress = compress
	cfg.Encrypt = encrypt
	cfg.Key = testKey()
	tr, err := newTransformer(&cfg)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte(`{"n":123,"text":"hello jsonstream benchmark"}`), 32) // ~1.3KiB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, flags, err := tr.outbound(payload)
		if err != nil {
			b.Fatal(err)
		}
		got, err := tr.inbound(flags, out)
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			b.Fatal("roundtrip mismatch")
		}
	}
}

func BenchmarkTransformPlain(b *testing.B)           { benchmarkTransform(b, false, false) }
func BenchmarkTransformCompress(b *testing.B)        { benchmarkTransform(b, true, false) }
func BenchmarkTransformEncrypt(b *testing.B)         { benchmarkTransform(b, false, true) }
func BenchmarkTransformCompressEncrypt(b *testing.B) { benchmarkTransform(b, true, true) }

// ---- 端到端 ----

// benchmarkRequestResponse 走完整链路：编码 → TCP → 解码 → 分发 → handler →
// 响应帧 → 回程解码。测量的是库整体的往返延迟，不是吞吐上限。
func benchmarkRequestResponse(b *testing.B, cfg Config) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	srv, err := NewServer(ln, cfg)
	if err != nil {
		b.Fatal(err)
	}
	srv.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
	go func() { _ = srv.Serve() }()
	defer srv.Close()

	c, err := Dial(context.Background(), ln.Addr().String(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	// 预热一条连接与路径，避免把首次握手计入。
	if _, err := c.Request(ctx, "ping", nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Request(ctx, "ping", nil); err != nil {
			b.Fatal(fmt.Errorf("round %d: %w", i, err))
		}
	}
}

func BenchmarkRequestResponsePlain(b *testing.B) {
	benchmarkRequestResponse(b, shortConfig())
}

func BenchmarkRequestResponseEncrypted(b *testing.B) {
	cfg := shortConfig()
	cfg.Encrypt = true
	cfg.Key = testKey()
	benchmarkRequestResponse(b, cfg)
}
