package jsonstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// newTestTransport 构造未启动读写循环的裸 transport（net.Pipe 两端均由
// 测试持有）：出站帧滞留 sendCh 供断言，入站字节由测试向对端写入。
func newTestTransport(t *testing.T, cfg Config, creditWindow int) (*transport, net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })
	tx, err := newTransformer(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTransport(c1, c1, tx, &cfg, creditWindow, func(*Frame) error { return nil }, nil)
	return tr, c2
}

// send 在连接死亡后必须立即失败——dead 预检直接命中（kill 完成后的
// send 不依赖 select 双就绪的随机选择）。
func TestTransportSendAfterDead(t *testing.T) {
	tr, _ := newTestTransport(t, shortConfig(), 0)
	tr.kill(ErrClosed)
	if err := tr.send(&Frame{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after dead = %v, want ErrClosed", err)
	}
}

// send 阻塞在满载 sendCh 上时连接死亡：预检通过（当时未死）后入队
// 分支阻塞，kill 解除阻塞并确定性走内层 dead 分支返回 ErrClosed。
func TestTransportSendUnblockedByKill(t *testing.T) {
	tr, _ := newTestTransport(t, shortConfig(), 0)
	for i := 0; i < cap(tr.sendCh); i++ {
		tr.sendCh <- &Frame{}
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		tr.kill(ErrClosed)
	}()
	done := make(chan error, 1)
	go func() { done <- tr.send(&Frame{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("send unblocked by kill = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("kill did not unblock send stuck on full sendCh")
	}
}

// grantCredit 的守卫：未启用背压（credit == nil）与 n<=0 都是静默无操作。
func TestTransportGrantCreditGuards(t *testing.T) {
	tr, _ := newTestTransport(t, shortConfig(), 0)
	tr.grantCredit(1, 5)
	if n := len(tr.sendCh); n != 0 {
		t.Fatalf("grantCredit without gate sent %d frames", n)
	}
	tr2, _ := newTestTransport(t, shortConfig(), 4)
	tr2.grantCredit(1, 0)
	tr2.grantCredit(1, -2)
	if n := len(tr2.sendCh); n != 0 {
		t.Fatalf("grantCredit n<=0 sent %d frames", n)
	}
}

// encodeFrame 透传 outbound 的变换错误（经 randRead 接缝注入）。
func TestTransportEncodeFrameOutboundError(t *testing.T) {
	tr, _ := newTestTransport(t, Config{Encrypt: true, Key: bytes.Repeat([]byte{9}, 32)}, 0)
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errors.New("rand injected") }
	defer func() { randRead = orig }()

	f := &Frame{Header: Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: 1}, Payload: []byte(`{}`)}
	if _, err := tr.encodeFrame(nil, f); err == nil {
		t.Fatal("expected outbound error to surface from encodeFrame")
	}
}

// readWireFrame 从对端连接读取一帧线上字节并解码（带读超时）。
func readWireFrame(t *testing.T, c net.Conn) *Frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var head [headerSize]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	buf := append([]byte{}, head[:]...)
	if head[3]&FlagHasMeta != 0 {
		var ml [metaLenSize]byte
		if _, err := io.ReadFull(c, ml[:]); err != nil {
			t.Fatalf("read meta length: %v", err)
		}
		buf = append(buf, ml[:]...)
		meta := make([]byte, binary.BigEndian.Uint16(ml[:]))
		if _, err := io.ReadFull(c, meta); err != nil {
			t.Fatalf("read metadata: %v", err)
		}
		buf = append(buf, meta...)
	}
	payload := make([]byte, binary.BigEndian.Uint32(head[10:14]))
	if _, err := io.ReadFull(c, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	f, err := ReadFrame(bytes.NewReader(append(buf, payload...)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return f
}

// 心跳往来：读循环收到 PING 必须回 PONG；PONG 无动作（用下一次 PING 的
// PONG 应答证明 PONG 帧已被消费而非堆积）。
func TestTransportPingPong(t *testing.T) {
	tr, peer := newTestTransport(t, Config{Heartbeat: 5 * time.Second}, 0)
	tr.start()
	defer func() { tr.kill(nil); peer.Close() }()

	ping := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}}
	pong := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePong}}
	if err := writeOnce(peer, ping); err != nil {
		t.Fatal(err)
	}
	if f := readWireFrame(t, peer); f.Type != TypePong {
		t.Fatalf("reply to PING = %v, want PONG", f.Type)
	}
	// 先 PONG（应无任何反应）再 PING：仍能收到 PONG 即证明 PONG 分支执行过
	if err := writeOnce(peer, pong); err != nil {
		t.Fatal(err)
	}
	if err := writeOnce(peer, ping); err != nil {
		t.Fatal(err)
	}
	if f := readWireFrame(t, peer); f.Type != TypePong {
		t.Fatalf("reply after PONG = %v, want PONG", f.Type)
	}
}

// 入站 payload 逆向变换失败（坏 flate 流）必须杀死连接并记录原因。
func TestTransportInboundTransformErrorKills(t *testing.T) {
	tr, peer := newTestTransport(t, Config{Compress: true, Heartbeat: 5 * time.Second}, 0)
	tr.start()
	defer peer.Close()

	// 0x07 的 BTYPE=11 是 flate 保留值：解压立即报错
	bad := &Frame{
		Header:  Header{Version: ProtocolVersion, Flags: FlagCompressed, Type: TypeResponse, StreamID: 1},
		Payload: []byte{0x07, 0x00, 0x01, 0x02},
	}
	if err := writeOnce(peer, bad); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tr.dead:
		if tr.deadErr == nil {
			t.Fatal("kill reason must carry the transform error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("transport not killed on undecodable compressed payload")
	}
}
