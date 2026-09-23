package jsonstream

import (
	"bytes"
	"compress/flate"
	"errors"
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
