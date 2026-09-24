package jsonstream

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"net"
	"testing"
)

// ReadFrame 与 transformer.inbound 是不可信字节的信任边界：
// 对端发来的任何序列都不得 panic、不得绕过长度上限。

// FuzzReadFrame：任意字节流解析不得 panic，且所有失败必须归一为
// ErrMalformed；成功解析的帧必须可原样重编码并重放出同一帧——
// 该不变量被打破即说明解析器接受了不自洽的帧。
func FuzzReadFrame(f *testing.F) {
	seed, err := (&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeRequest, Flags: FlagHasMeta, StreamID: 7},
		Metadata: []byte(`{"route":"ping"}`),
		Payload:  []byte(`{"n":1}`),
	}).appendTo(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0x4a, 0x53})                                                    // 仅魔数（截断头）
	f.Add([]byte{0x4a, 0x53, 1, 0x1f, 3, 0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff}) // 保留位+payload 超限
	f.Add([]byte{0x00, 0x00, 1, 0, 3, 0, 0, 0, 0, 1, 0, 0, 0, 0})                // 坏魔数
	f.Add([]byte{0x4a, 0x53, 9, 0, 99, 0, 0, 0, 0, 1, 0, 0, 0, 0})               // 坏版本+坏类型
	f.Fuzz(func(t *testing.T, data []byte) {
		f1, err := ReadFrame(bytes.NewReader(data))
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("parse failure must normalize to ErrMalformed, got %v", err)
			}
			return
		}
		re, err := f1.appendTo(nil)
		if err != nil {
			t.Fatalf("accepted frame must re-encode: %v", err)
		}
		f2, err := ReadFrame(bytes.NewReader(re))
		if err != nil {
			t.Fatalf("re-encoded frame must parse: %v", err)
		}
		if f1.Header != f2.Header || !bytes.Equal(f1.Metadata, f2.Metadata) || !bytes.Equal(f1.Payload, f2.Payload) {
			t.Fatal("frame not stable across re-encode")
		}
	})
}

// FuzzTransformInbound：解密/解压入口对任意 flag 与字节不得 panic，
// 成功时的输出不得超过解压上限（readAll 的 OOM 防线）。
func FuzzTransformInbound(f *testing.F) {
	plain, err := newTransformer(&Config{})
	if err != nil {
		f.Fatal(err)
	}
	full, err := newTransformer(&Config{Compress: true, Encrypt: true, Key: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint8(0), []byte("hello"))
	f.Add(uint8(FlagCompressed), []byte{0x01, 0x05, 0x00, 0xfa, 0xff}) // 坏 flate 流
	f.Add(uint8(FlagEncrypted), bytes.Repeat([]byte{0}, 64))           // 均匀垃圾密文
	f.Add(uint8(FlagEncrypted|FlagCompressed), []byte("x"))
	// 合法压缩流与合法密文：让 fuzz 直接站在解压/解密成功路径上探索，
	// 而不是先花预算重新发现合法流
	compOnly, err := newTransformer(&Config{Compress: true})
	if err != nil {
		f.Fatal(err)
	}
	encOnly, err := newTransformer(&Config{Encrypt: true, Key: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		f.Fatal(err)
	}
	if payload, flags, err := compOnly.outbound(bytes.Repeat([]byte(`{"n":`), 40)); err != nil {
		f.Fatal(err)
	} else {
		f.Add(flags, payload)
	}
	if payload, flags, err := encOnly.outbound(bytes.Repeat([]byte("x"), 200)); err != nil {
		f.Fatal(err)
	} else {
		f.Add(flags, payload)
	}
	f.Fuzz(func(t *testing.T, flags uint8, data []byte) {
		var nilTr *transformer
		for _, tr := range []*transformer{plain, full, nilTr} {
			out, err := tr.inbound(flags, data)
			if err != nil {
				if len(out) != 0 {
					t.Fatalf("error result must carry no payload, got %d bytes", len(out))
				}
				continue
			}
			if len(out) > MaxPayloadSize {
				t.Fatalf("inbound returned %d bytes, exceeds cap %d", len(out), MaxPayloadSize)
			}
		}
	})
}

// TestDecompressionBombCapped：wire 侧 16MiB 的 flate 帧可膨胀三个数量级，
// 解压结果超上限必须以 ErrMalformed 拒绝，而不是分配 GiB 级内存。
func TestDecompressionBombCapped(t *testing.T) {
	tr, err := newTransformer(&Config{Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	var bomb bytes.Buffer
	// BestSpeed：零串的膨胀率与压缩档位无关，-race 下省掉无谓的压缩耗时
	w, _ := flate.NewWriter(&bomb, flate.BestSpeed)
	if _, err := w.Write(bytes.Repeat([]byte{0}, MaxPayloadSize+(1<<20))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := tr.inbound(FlagCompressed, bomb.Bytes())
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("decompression bomb must be rejected with ErrMalformed, got %v", err)
	}
	if out != nil {
		t.Fatal("no payload should accompany the error")
	}

	// 上限之内（1MiB）照常解压，往返一致
	var ok bytes.Buffer
	w2, _ := flate.NewWriter(&ok, flate.BestCompression)
	src := bytes.Repeat([]byte("JsonStream"), 100<<10/10)
	if _, err := w2.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	out, err = tr.inbound(FlagCompressed, ok.Bytes())
	if err != nil {
		t.Fatalf("legitimate payload within cap must pass: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatal("roundtrip mismatch for legitimate compressed payload")
	}
}

// FuzzRawPeer：把任意字节流打进真实监听器的握手状态机（handleConn），
// 服务端必须优雅拒绝——不得 panic、不得被单条恶意连接拖垮，且随后
// 合法客户端仍能完成一次完整请求（跨连接状态未受污染）。
func FuzzRawPeer(f *testing.F) {
	cfg := shortConfig()
	cfg.Logger = nil   // 静默：fuzz 的每次拒绝都会打 bad handshake 日志
	cfg.Retention = -1 // 禁用会话保留：探测客户端用完即弃，不积累
	scfg := cfg
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.Skip(err)
	}
	srv, err := NewServer(ln, scfg)
	if err != nil {
		f.Fatal(err)
	}
	srv.Handle("ping", func(req *Request) (any, error) { return item{N: 1}, nil })
	go srv.Serve()
	f.Cleanup(func() { _ = srv.Close() })

	connectSeed, err := (&Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeConnect},
		Payload: []byte(`{"version":1,"credit":8}`),
	}).appendTo(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(connectSeed)
	f.Add([]byte{}) // 空连接（立即断开）
	f.Add([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	f.Add(bytes.Repeat([]byte{0x4a, 0x53, 1, 0x0f, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0}, 4))
	f.Add([]byte{0x4a, 0x53, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0x7b, 0x22, 0x7d}) // 半截 JSON
	ping, err := (&Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}}).appendTo(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(append(append([]byte{}, connectSeed...), ping...)) // 握手后心跳：PING→PONG 路径

	f.Fuzz(func(t *testing.T, data []byte) {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Skip(err)
		}
		_, _ = nc.Write(data) // 写失败合法：服务端可能已判死并关闭
		_ = nc.Close()

		// 恶意流之后，合法客户端必须仍能完成完整请求
		c, err := Dial(context.Background(), ln.Addr().String(), scfg)
		if err != nil {
			t.Fatalf("server not serving after malicious peer: %v", err)
		}
		if _, err := c.Request(context.Background(), "ping", nil); err != nil {
			t.Fatalf("request after malicious peer: %v", err)
		}
		_ = c.Close()
	})
}
