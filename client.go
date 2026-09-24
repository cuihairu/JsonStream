package jsonstream

import (
	"bufio"
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Client 是 JsonStream 客户端：自动重连（指数退避 + 抖动）、会话恢复
// （resumed 时保留挂起流等待重放）、断连期间 API 调用挂起而非报错。
type Client struct {
	cfg   Config
	addr  string
	table *routeTable

	mu        sync.Mutex
	ep        *endpoint
	sessionID string
	subs      map[uint32]*Subscription // 跨重连的订阅（重连失败后自动重订）
	notify    chan struct{}            // endpoint 变更信号

	onReconnect    []func()
	onResumeFailed []func(error)

	handshakeOnce sync.Once
	handshake     chan error

	closeOnce sync.Once
	closed    chan struct{}
}

// Dial 建立连接并完成握手（阻塞到 CONNACK）。
// 首连失败（网络不可达或握手被拒）立即返回错误，与常规客户端库直觉一致；
// 连接成功后的断连才由 connectLoop 自动重连（指数退避 + 抖动，受 Reconnect 控制）。
func Dial(ctx context.Context, addr string, cfg Config) (*Client, error) {
	cfg = cfg.normalized()
	if _, err := newTransformer(&cfg); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:       cfg,
		addr:      addr,
		table:     newRouteTable(),
		subs:      make(map[uint32]*Subscription),
		notify:    make(chan struct{}, 1),
		handshake: make(chan error, 1),
		closed:    make(chan struct{}),
	}
	go c.connectLoop()
	select {
	case err := <-c.handshake:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		c.Close()
		return nil, ctx.Err()
	}
	return c, nil
}

// ---- 路由注册（本端作为 responder：服务端主动发起交互时生效） ----

func (c *Client) Handle(route string, h func(*Request) (any, error)) { c.table.Handle(route, h) }
func (c *Client) HandleStream(route string, h func(*Request, Emitter) error) {
	c.table.HandleStream(route, h)
}
func (c *Client) HandleChannel(route string, h func(*Channel) error) { c.table.HandleChannel(route, h) }
func (c *Client) HandleOneWay(route string, h func(*Message) error)  { c.table.HandleOneWay(route, h) }

// ---- 回调 ----

// OnReconnect 在每次（非首次）重连成功后回调。
func (c *Client) OnReconnect(fn func()) {
	c.mu.Lock()
	c.onReconnect = append(c.onReconnect, fn)
	c.mu.Unlock()
}

// OnResumeFailed 在会话恢复失败（过期/未知）后回调，应用可据此重建状态。
func (c *Client) OnResumeFailed(fn func(error)) {
	c.mu.Lock()
	c.onResumeFailed = append(c.onResumeFailed, fn)
	c.mu.Unlock()
}

// SessionID 返回服务端分配的当前会话 ID。
func (c *Client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Close 关闭客户端并停止重连循环；所有挂起的流以连接关闭错误终结。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		ep := c.ep
		c.mu.Unlock()
		if ep != nil {
			ep.tr.kill(ErrClosed)
			closed := &Error{Code: CodeInternal, Message: ErrClosed.Error()}
			for _, flw := range ep.snapshot() {
				flw.fail(closed)
			}
		}
	})
	return nil
}

// ---- 发起侧 API（断连期间挂起等待重连，受 ctx 约束） ----

func (c *Client) waitEp(ctx context.Context) (*endpoint, error) {
	for {
		c.mu.Lock()
		ep := c.ep
		alive := false
		if ep != nil {
			select {
			case <-ep.tr.dead:
			default:
				alive = true
			}
		}
		c.mu.Unlock()
		if alive {
			return ep, nil
		}
		select {
		case <-c.notify:
		case <-c.closed:
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Request 请求/响应：等待单帧响应（响应帧自带终结语义）。
func (c *Client) Request(ctx context.Context, route string, payload any) (*Message, error) {
	ep, err := c.waitEp(ctx)
	if err != nil {
		return nil, err
	}
	return ep.doRequest(ctx, route, payload)
}

// Stream 发起流式请求，逐帧读取直到 COMPLETE/ERROR。
func (c *Client) Stream(ctx context.Context, route string, payload any) (*ReadStream, error) {
	ep, err := c.waitEp(ctx)
	if err != nil {
		return nil, err
	}
	return ep.doStream(ctx, route, payload)
}

// Channel 发起双工通道。
func (c *Client) Channel(ctx context.Context, route string, payload any) (*Channel, error) {
	ep, err := c.waitEp(ctx)
	if err != nil {
		return nil, err
	}
	return ep.doChannel(ctx, route, payload)
}

// SendOneWay 单向发送，协议保证不产生任何响应帧。
func (c *Client) SendOneWay(route string, payload any) error {
	ep, err := c.waitEp(context.Background())
	if err != nil {
		return err
	}
	return ep.doOneWay(route, payload)
}

// Publish 把消息发布到服务端主题（由 Server.HandlePublish 处理）。
func (c *Client) Publish(topic string, payload any) error {
	ep, err := c.waitEp(context.Background())
	if err != nil {
		return err
	}
	return ep.doPublish(topic, payload)
}

// Subscribe 订阅服务端广播的主题，投递经回调交付。
func (c *Client) Subscribe(ctx context.Context, topic string, h func(*Message) error) (*Subscription, error) {
	ep, err := c.waitEp(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := ep.doSubscribe(ctx, topic, h)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.subs[sub.f.id] = sub
	c.mu.Unlock()
	return sub, nil
}

// ---- 连接循环 ----

func (c *Client) connectLoop() {
	backoff := c.cfg.BackoffInitial
	established := false // 首连成功前失败应快速抛给 Dial，之后才走自动重连
	for {
		select {
		case <-c.closed:
			c.finishHandshake(ErrClosed)
			return
		default:
		}
		conn, err := c.dialTCP()
		if err != nil {
			if !established || !c.cfg.reconnectEnabled() || c.isClosed() {
				c.finishHandshake(err)
				return
			}
			c.logf("jsonstream: client: dial %s: %v (retry in %v)", c.addr, err, backoff)
			if !c.sleepBackoff(backoff) {
				return
			}
			backoff = c.nextBackoff(backoff)
			continue
		}
		tr, err := c.establish(conn)
		if err != nil {
			_ = conn.Close()
			if !established || !c.cfg.reconnectEnabled() || c.isClosed() {
				c.finishHandshake(err)
				return
			}
			c.logf("jsonstream: client: handshake with %s: %v (retry in %v)", c.addr, err, backoff)
			if !c.sleepBackoff(backoff) {
				return
			}
			backoff = c.nextBackoff(backoff)
			continue
		}
		backoff = c.cfg.BackoffInitial
		if established {
			c.fireReconnect()
		}
		established = true
		c.finishHandshake(nil)

		<-tr.dead
		select {
		case <-c.closed:
			return
		default:
		}
		if !c.cfg.reconnectEnabled() {
			return
		}
		// 断开后先退避一拍再重拨：立即重连常会抢在服务端感知断连之前
		// 到达，把本该判为「会话过期」的重连误判成 takeover 恢复。
		if !c.sleepBackoff(c.cfg.BackoffInitial) {
			return
		}
		c.logf("jsonstream: client: connection lost, reconnecting to %s", c.addr)
	}
}

func (c *Client) dialTCP() (net.Conn, error) {
	d := net.Dialer{Timeout: c.cfg.DialTimeout}
	return d.Dial("tcp", c.addr)
}

// establish 发送 CONNECT、等待 CONNACK，并用生效参数构建 transport 与 endpoint。
func (c *Client) establish(conn net.Conn) (*transport, error) {
	_ = conn.SetDeadline(time.Now().Add(c.cfg.DialTimeout))
	cj := &ConnectJSON{
		Version:     int(ProtocolVersion),
		Compress:    c.cfg.Compress,
		Encrypt:     c.cfg.Encrypt,
		HeartbeatMS: int(c.cfg.Heartbeat / time.Millisecond),
		Credit:      c.cfg.Credit,
		SessionID:   c.SessionID(),
		Auth:        c.cfg.Auth,
	}
	cf, err := connectFrame(cj)
	if err != nil {
		return nil, err
	}
	if err := writeOnce(conn, cf); err != nil {
		return nil, err
	}

	br := bufio.NewReader(conn)
	for {
		f, err := ReadFrame(br)
		if err != nil {
			return nil, err
		}
		switch f.Type {
		case TypeConnAck:
			var aj connackJSON
			if err := json.Unmarshal(f.Payload, &aj); err != nil {
				return nil, &Error{Code: CodeInvalid, Message: "bad CONNACK payload"}
			}
			return c.bindSession(conn, br, &aj)
		case TypeError:
			return nil, errDecode(f.Payload)
		default:
			return nil, &Error{Code: CodeProtocol, Message: "unexpected " + f.Type.String() + " during handshake"}
		}
	}
}

// bindSession 用 CONNACK 的生效参数装配新 endpoint，并处理会话恢复的
// 两 种结果：resumed（迁移挂起流等待服务端重放）与恢复失败（挂起流
// 全部失败、订阅自动重订、触发 OnResumeFailed）。
func (c *Client) bindSession(conn net.Conn, br *bufio.Reader, aj *connackJSON) (*transport, error) {
	cfg := c.cfg
	cfg.Compress = aj.Compress
	cfg.Encrypt = aj.Encrypt
	if aj.HeartbeatMS > 0 {
		cfg.Heartbeat = time.Duration(aj.HeartbeatMS) * time.Millisecond
	}
	cfg.Credit = aj.Credit
	tx, err := newTransformer(&cfg)
	if err != nil {
		return nil, err
	}

	// endpoint 与 transport 互引用（tr 挂 ep 的分发回调、ep 持 tr），
	// 用闭包晚绑定：tr.start 之前两者都已赋值，回调解引用安全。
	var ep *endpoint
	var tr *transport
	tr = newTransport(conn, br, tx, &cfg, cfg.Credit,
		func(f *Frame) error { return ep.handleFrame(f) },
		func(err error) { c.onDead(tr)(err) })
	ep = newEndpoint(true, cfg, tr, c.table)
	_ = conn.SetDeadline(time.Time{}) // 清除握手 deadline，交由心跳超时接管

	c.mu.Lock()
	prev := c.sessionID
	c.sessionID = aj.SessionID
	oldEp := c.ep
	c.ep = ep
	resumeFailed := prev != "" && !aj.Resumed

	if resumeFailed && oldEp != nil {
		expired := &Error{Code: CodeSessionExpired, Message: "session expired"}
		for id, flw := range oldEp.snapshot() {
			if id%2 == 0 {
				continue // 偶数 = 服务端发起的被动流，随旧连接消亡
			}
			flw.fail(expired)
		}
	}
	// 迁移仍然存活的发起方流到新 endpoint：resumed 时它们等待重放，
	// 恢复失败时已在上一步 fail，不会出现在这里。
	if oldEp != nil {
		for id, flw := range oldEp.snapshot() {
			if id%2 == 1 && !flw.isDone() {
				flw.ep = ep
				ep.registerFlow(flw)
			}
		}
	}
	var toResub []*Subscription
	if resumeFailed {
		for _, sub := range c.subs {
			toResub = append(toResub, sub)
		}
	}
	callbacks := append([]func(error){}, c.onResumeFailed...)
	c.mu.Unlock()

	// 启动读写循环：此后入站帧由 ep.handleFrame 分发，出站帧由写循环串行化。
	// 重订与回调必须在循环启动之后——它们要收发帧（SUBACK）或触发应用动作。
	tr.start()

	for _, sub := range toResub {
		c.resubscribeLocked(ep, sub)
	}
	if resumeFailed {
		for _, fn := range callbacks {
			fn(&Error{Code: CodeSessionExpired, Message: "session expired"})
		}
	}

	select {
	case c.notify <- struct{}{}:
	default:
	}
	return tr, nil
}

// resubAckTimeout 是重订等待 SUBACK 的上限；包级接缝便于测试缩短。
var resubAckTimeout = 5 * time.Second

// resubscribeLocked 在新 endpoint 上重发 SUBSCRIBE（订阅关系未被服务端
// 保留时的重建路径），等 SUBACK 确认后重启投递消费 goroutine。等待是
// 必要的：回调（OnResumeFailed）先于应用恢复广播发布，若不等确认，
// 广播会赶在服务端登记订阅之前发出而丢失。
func (c *Client) resubscribeLocked(ep *endpoint, sub *Subscription) {
	flw := newFlow(ep.allocID(), flowSubscribe, ep)
	ep.registerFlow(flw)
	err := ep.tr.send(&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeSubscribe, StreamID: flw.id},
		Metadata: encodeMeta("", sub.topic),
	})
	if err != nil {
		flw.fail(&Error{Code: CodeInternal, Message: err.Error()})
		return
	}
	select {
	case <-flw.ackCh:
	case <-flw.doneCh:
		return // 流已被终结（错误），不重启消费
	case <-time.After(resubAckTimeout):
		ep.log.Printf("jsonstream: client: resubscribe %q timed out waiting SUBACK", sub.topic)
		return
	}
	sub.mu.Lock()
	sub.f = flw
	sub.mu.Unlock()
	sub.consumeWg.Add(1)
	go sub.consume()
}

func (c *Client) onDead(tr *transport) func(error) {
	return func(err error) {
		if err != nil {
			c.logf("jsonstream: client: connection error: %v", err)
		}
		_ = tr // endpoint 的替换在 bindSession；此处无需清理流（跨重连保留）
	}
}

func (c *Client) sleepBackoff(d time.Duration) bool {
	// 指数退避 + 抖动：避免服务端重启后的重连风暴同时打到同一时刻。
	jitter := time.Duration(float64(d) * (0.5 + 0.5*randFloat()))
	timer := time.NewTimer(jitter)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-c.closed:
		return false
	}
}

func (c *Client) nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > c.cfg.BackoffMax {
		d = c.cfg.BackoffMax
	}
	return d
}

func (c *Client) finishHandshake(err error) {
	c.handshakeOnce.Do(func() { c.handshake <- err })
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Client) fireReconnect() {
	c.mu.Lock()
	fns := append([]func(){}, c.onReconnect...)
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (c *Client) logf(format string, v ...any) {
	c.cfg.logger().Printf(format, v...)
}

// randFloat 供退避抖动使用。
func randFloat() float64 { return rand.Float64() }
