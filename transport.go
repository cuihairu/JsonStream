package jsonstream

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// transport 是一条 TCP 连接上的帧收发与心跳：读循环负责定长头分帧、
// payload 逆向变换与读空闲超时；写循环串行化所有出站帧并按间隔注入
// PING。上层（endpoint）只看到 Frame 与 send/close。
type transport struct {
	conn   net.Conn
	reader io.Reader // 与握手阶段共享的 bufio，避免缓冲数据丢失
	tr     *transformer
	cfg    *Config
	log    Logger
	credit *creditGate // 发送侧闸门；nil 表示未启用背压

	sendCh  chan *Frame
	handler func(*Frame) error // 读循环分发回调；返回 error 视为协议违规
	onDead  func(error)        // 连接断开回调（由 close 触发一次）

	deadOnce sync.Once
	dead     chan struct{}
	deadErr  error
}

func newTransport(conn net.Conn, reader io.Reader, tr *transformer, cfg *Config, creditWindow int) *transport {
	t := &transport{
		conn:   conn,
		reader: reader,
		tr:     tr,
		cfg:    cfg,
		log:    cfg.logger(),
		sendCh: make(chan *Frame, 256),
		dead:   make(chan struct{}),
	}
	if creditWindow > 0 {
		t.credit = newCreditGate(creditWindow)
	}
	return t
}

func (t *transport) readIdleTimeout() time.Duration {
	return t.cfg.Heartbeat + t.cfg.Heartbeat/2 // 1.5 × 间隔，见 protocol.md §7.2
}

// start 启动读写循环。握手完成（CONNECT/CONNACK 交换完毕）后调用。
func (t *transport) start(handler func(*Frame) error, onDead func(error)) {
	t.handler = handler
	t.onDead = onDead
	go t.writeLoop()
	go t.readLoop()
}

// rawWrite 在启动写循环之前同步写一帧（仅握手阶段使用，单线程）。
func (t *transport) rawWrite(f *Frame) error {
	buf, err := f.appendTo(nil)
	if err == nil {
		_ = t.conn.SetWriteDeadline(time.Now().Add(t.cfg.DialTimeout))
		_, err = t.conn.Write(buf)
	}
	return err
}

// send 把帧交给写循环；连接已死时返回错误。
// 受背压约束的调用方必须先 takeCredit。
func (t *transport) send(f *Frame) error {
	select {
	case t.sendCh <- f:
		return nil
	case <-t.dead:
		return ErrClosed
	}
}

// takeCredit 在发送受背压约束的数据帧前取走一个令牌。
func (t *transport) takeCredit(ctx context.Context) error {
	if t.credit == nil {
		return nil
	}
	return t.credit.take(ctx)
}

// grantCredit 由接收侧在交付应用后归还额度：既补本地闸门，也回发 CREDIT 帧。
func (t *transport) grantCredit(streamID uint32, n int) {
	if t.credit == nil || n <= 0 {
		return
	}
	_ = t.send(&Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeCredit, StreamID: streamID},
		Payload: encodeCredit(n),
	})
}

func (t *transport) writeLoop() {
	ticker := time.NewTicker(t.cfg.Heartbeat)
	defer ticker.Stop()
	var buf []byte
	for {
		select {
		case f := <-t.sendCh:
			var err error
			buf, err = t.encodeFrame(buf[:0], f)
			if err == nil {
				_ = t.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				_, err = t.conn.Write(buf)
			}
			if err != nil {
				t.kill(err)
				return
			}
		case <-ticker.C:
			pf := &Frame{Header: Header{Version: ProtocolVersion, Type: TypePing}}
			buf, err := pf.appendTo(buf[:0])
			if err == nil {
				_ = t.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				_, err = t.conn.Write(buf)
			}
			if err != nil {
				t.kill(err)
				return
			}
		case <-t.dead:
			return
		}
	}
}

// encodeFrame 变换 payload 并整帧编码到 dst；变换产生的标志位与调用方
// 设置的模式标志位（FlagStream/FlagChannel 等）合并。
func (t *transport) encodeFrame(dst []byte, f *Frame) ([]byte, error) {
	payload, flagBits, err := t.tr.outbound(f.Payload)
	if err != nil {
		return dst, err
	}
	enc := &Frame{Header: Header{Version: f.Version, Flags: f.Flags | flagBits, Type: f.Type, StreamID: f.StreamID}, Metadata: f.Metadata, Payload: payload}
	return enc.appendTo(dst)
}

func (t *transport) readLoop() {
	reader := bufio.NewReaderSize(t.reader, 16<<10) // 再包一层 bufio 无害且兜底
	for {
		_ = t.conn.SetReadDeadline(time.Now().Add(t.readIdleTimeout()))
		wire, err := ReadFrame(reader)
		if err != nil {
			t.kill(err)
			return
		}
		if wire.Type == TypePing {
			// 收到任何帧都意味着对端活跃（读 deadline 已被重置）；PING 额外回 PONG。
			_ = t.send(&Frame{Header: Header{Version: ProtocolVersion, Type: TypePong}})
			continue
		}
		if wire.Type == TypePong {
			continue
		}
		plain, err := t.tr.inbound(wire.Flags, wire.Payload)
		if err != nil {
			t.kill(err)
			return
		}
		wire.Payload = plain
		if err := t.handler(wire); err != nil {
			t.kill(err)
			return
		}
	}
}

func (t *transport) kill(err error) {
	t.deadOnce.Do(func() {
		t.deadErr = err
		close(t.dead)
		if t.credit != nil {
			t.credit.close()
		}
		_ = t.conn.Close()
		if t.onDead != nil {
			go t.onDead(err)
		}
	})
}

func (t *transport) waitDead(ctx context.Context) error {
	select {
	case <-t.dead:
		return t.deadErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
