package jsonstream

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"net"
	"testing"
	"time"
	"unicode/utf8"
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
	// metaLen 恰为 MaxMetadataSize：off-by-one 真 bug（uint16 截断）的
	// 触发边界，fuzz 从「恰好合法」出发突变最有价值
	big, err := (&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeRequest, Flags: FlagHasMeta, StreamID: 7},
		Metadata: bytes.Repeat([]byte{'a'}, MaxMetadataSize),
		Payload:  []byte(`{}`),
	}).appendTo(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(big)
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
	srv.Handle("ping", func(_ *Request) (any, error) { return item{N: 1}, nil })
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

// FuzzDecodeMeta：元数据编解码层的两条性质——
//  1. decodeMeta 对任意字节不 panic（截断 JSON、类型错值、非法 UTF-8、
//     超长、空）。它吞掉 Unmarshal 错误是设计（protocol.md：坏 meta 退化
//     为空路由，由分发层按 NOT_FOUND 处理），所以只钉 panic，不钉"出错
//     时解出哪些部分"——那会把当时的实现细节错锁成契约。
//  2. decodeMeta∘encodeMeta 对任意 (route, topic) 往返恒等。恒等域声明
//     为合法 UTF-8：encoding/json 对非法字节按语言规范改写为 U+FFFD，
//     那是本函数对边界之外的第一个行为，性质测试显式让出该域而不是悄悄
//     放宽判据（design-notes §9 的同一条纪律）；让出域内的 panic 性质照测。
func FuzzDecodeMeta(f *testing.F) {
	f.Add([]byte(`{"route":"ping"}`), "ping", "")
	f.Add([]byte(`{"topic":"ticks"}`), "", "ticks")
	f.Add([]byte(`{"route":"r","topic":"t"}`), "r", "t")
	f.Add([]byte(`{`), "", "")                                      // 截断 JSON
	f.Add([]byte("not-json"), "x", "y")                             // 垃圾字节 + 合法字符串：恒等臂仍执行
	f.Add([]byte(`{"route":123}`), "", "")                          // 类型错值：静默吞错是设计
	f.Add([]byte(`{"topic":"`+"\xff\xfe"+`"}`), "\xff\xfe", "ok")   // 非法 UTF-8 双侧
	f.Add([]byte(nil), "", "")                                      // 双空短路：encodeMeta 返回 nil，decodeMeta(nil) 归零
	f.Add(bytes.Repeat([]byte("z"), 2*MaxMetadataSize), "long", "") // 超长非法 JSON + 合法往返
	f.Fuzz(func(t *testing.T, raw []byte, route, topic string) {
		_ = decodeMeta(raw)
		got := decodeMeta(encodeMeta(route, topic))
		if !utf8.ValidString(route) || !utf8.ValidString(topic) {
			return
		}
		if got.Route != route || got.Topic != topic {
			t.Fatalf("meta roundtrip: encodeMeta(%q, %q) decoded back = (%q, %q)", route, topic, got.Route, got.Topic)
		}
	})
}

// FuzzHandshakeJSON：握手 JSON 与协商层的三条性质——
//  1. 任意字节 Unmarshal 进 ConnectJSON/connackJSON 不 panic（服务端解
//     CONNECT 载荷、客户端解 CONNACK 载荷，两侧都是对端可控输入）。
//  2. connectFrame/connackFrame 的 marshal∘unmarshal 对任意字段组合恒等
//     （omitempty 的 credit/session_id/auth 零值省略后解回零值，语义等价；
//     恒等域同样声明为合法 UTF-8，理由见 FuzzDecodeMeta）。
//  3. effectiveConnack 对任意（服务端配置 × 客户端声明）不 panic，且协商
//     必须是 wire 形态的纯函数：对同一 cfg，effectiveConnack(cj) 与
//     effectiveConnack(cj 上线往返后的副本) 逐字段相等（编码层不得吞掉
//     协商输入——negotiation 只消费 bool/int，非法 UTF-8 域也必须成立）；
//     credit 用独立重算式核对 §6.1 规则（双方 >0 才启用、取较小；任一
//     ≤0 关闭），不复读实现表达式。
func FuzzHandshakeJSON(f *testing.F) {
	f.Add([]byte(`{"version":1,"compress":true,"encrypt":false,"heartbeat_ms":10000,"credit":5,"session_id":"s","auth":"tok"}`),
		1, 10000, 5, "s", "tok", true, false, 64, 15000, true, true)
	f.Add([]byte(`{"version":"x"}`), // 类型错值：部分解码后被调用方吞错，只要求不 panic
		0, 0, 0, "", "", false, false, 0, 0, false, false)
	f.Add([]byte(`[`), // 截断 + 负 credit：背压"任一 ≤0 关闭"臂
		0, 0, -7, "", "", false, false, 3, -5, false, false)
	f.Add([]byte("null"), // 合法 JSON 的 null 文档 + 超大值（Duration 换算溢出不得 panic）
		99, -1, 1<<40, "s", "a", true, true, 1<<40, 1<<40, true, true)
	f.Add([]byte(nil), // 空字节：双零 credit 关闭臂
		2, 15000, 0, "", "", false, false, 64, 1000, false, false)
	f.Add(bytes.Repeat([]byte("y"), 4096), // 长垃圾 + 双方恰好相等的小 credit：min 边界
		1, 1000, 2, "", "", false, false, 2, 1000, false, false)
	f.Fuzz(func(t *testing.T, raw []byte, version, heartbeatMS, credit int, sessionID, auth string, compress, encrypt bool, srvCredit, srvHeartbeatMS int, srvCompress, srvEncrypt bool) {
		// 性质 1：对端可控字节 → 握手结构体。
		_ = jsonUnmarshal(raw, &ConnectJSON{})
		_ = jsonUnmarshal(raw, &connackJSON{})

		// 性质 2：CONNECT/CONNACK 帧的 marshal∘unmarshal 恒等。
		cj := ConnectJSON{Version: version, Compress: compress, Encrypt: encrypt,
			HeartbeatMS: heartbeatMS, Credit: credit, SessionID: sessionID, Auth: auth}
		fr, err := connectFrame(&cj)
		if err != nil {
			t.Fatalf("connectFrame(%+v): %v", cj, err)
		}
		var back ConnectJSON
		if err := jsonUnmarshal(fr.Payload, &back); err != nil {
			t.Fatalf("CONNECT payload must re-decode: %v", err)
		}
		if utf8.ValidString(sessionID) && utf8.ValidString(auth) && back != cj {
			t.Fatalf("CONNECT wire roundtrip: %+v -> %+v", cj, back)
		}
		aj := connackJSON{SessionID: sessionID, Resumed: compress, Compress: encrypt,
			Encrypt: compress, HeartbeatMS: heartbeatMS, Credit: credit, RetentionMS: srvHeartbeatMS}
		fr2, err := connackFrame(&aj)
		if err != nil {
			t.Fatalf("connackFrame(%+v): %v", aj, err)
		}
		var backAJ connackJSON
		if err := jsonUnmarshal(fr2.Payload, &backAJ); err != nil {
			t.Fatalf("CONNACK payload must re-decode: %v", err)
		}
		if utf8.ValidString(sessionID) && backAJ != aj {
			t.Fatalf("CONNACK wire roundtrip: %+v -> %+v", aj, backAJ)
		}

		// 性质 3：协商纯函数性 + §6.1 规则独立重算。
		cfg := Config{Credit: srvCredit, Compress: srvCompress, Encrypt: srvEncrypt,
			Heartbeat: time.Duration(srvHeartbeatMS) * time.Millisecond,
			Retention: time.Duration(heartbeatMS) * time.Millisecond}
		got := effectiveConnack(cfg, &cj)
		if viaWire := effectiveConnack(cfg, &back); got != viaWire {
			t.Fatalf("negotiation not pure over wire form: %+v vs %+v", got, viaWire)
		}
		wantCredit := 0
		if cfg.Credit > 0 && cj.Credit > 0 {
			wantCredit = cfg.Credit
			if cj.Credit < wantCredit {
				wantCredit = cj.Credit
			}
		}
		if got.Credit != wantCredit {
			t.Fatalf("credit negotiation: cfg=%d cj=%d -> %d, want %d", cfg.Credit, cj.Credit, got.Credit, wantCredit)
		}
		if got.Compress != (cfg.Compress && cj.Compress) || got.Encrypt != (cfg.Encrypt && cj.Encrypt) {
			t.Fatalf("feature flags must be AND of both sides: got %+v", got)
		}
	})
}
