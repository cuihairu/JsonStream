package jsonstream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

// Server 是 JsonStream 服务端。注册路由须在 Serve 之前完成。
type Server struct {
	cfg   Config
	ln    net.Listener
	table *routeTable
	store *sessionStore

	mu    sync.Mutex
	conns map[*serverConn]struct{}

	closeOnce sync.Once
	closed    chan struct{}
}

// NewServer 创建服务端（不开始接受连接，调用 Serve 启动）。
func NewServer(ln net.Listener, cfg Config) (*Server, error) {
	cfg = cfg.normalized()
	if _, err := newTransformer(&cfg); err != nil {
		return nil, err
	}
	s := &Server{
		cfg:    cfg,
		ln:     ln,
		table:  newRouteTable(),
		store:  &sessionStore{sessions: make(map[string]*serverSession)},
		conns:  make(map[*serverConn]struct{}),
		closed: make(chan struct{}),
	}
	return s, nil
}

// ---- 路由注册（转发到共享表） ----

// Handle 注册请求/响应路由。
func (s *Server) Handle(route string, h func(*Request) (any, error)) { s.table.Handle(route, h) }

// HandleStream 注册流式路由。
func (s *Server) HandleStream(route string, h func(*Request, Emitter) error) {
	s.table.HandleStream(route, h)
}

// HandleChannel 注册双工通道路由。
func (s *Server) HandleChannel(route string, h func(*Channel) error) { s.table.HandleChannel(route, h) }

// HandleOneWay 注册单向路由（永不回帧，路由不存在时静默）。
func (s *Server) HandleOneWay(route string, h func(*Message) error) { s.table.HandleOneWay(route, h) }

// HandlePublish 注册客户端发布主题的处理。
func (s *Server) HandlePublish(topic string, h func(*Message) error) { s.table.HandlePublish(topic, h) }

// OnAuth 注册握手鉴权钩子；返回非 nil error 时以 AUTH_DENIED 拒绝连接。
func (s *Server) OnAuth(h func(*ConnectJSON) error) { s.table.OnAuth(h) }

// Publish 向所有已订阅该主题的连接广播一帧 PUBLISH。
func (s *Server) Publish(topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	// 遍历会话存储而非活跃连接：断开保留中的会话仍订阅着主题，
	// 其投递经 sendDown 进入保留队列，重连后重放（§8.1「如广播消息」）。
	// 锁序注意：store.mu 不与 ss.mu 嵌套持有（store.drop 先释放 store.mu
	// 再由 terminate 取 ss.mu，此处先快照后逐会话取锁，无交叉）。
	s.store.mu.Lock()
	sessions := make([]*serverSession, 0, len(s.store.sessions))
	for _, ss := range s.store.sessions {
		sessions = append(sessions, ss)
	}
	s.store.mu.Unlock()
	type target struct {
		ss *serverSession
		id uint32
	}
	var targets []target
	for _, ss := range sessions {
		ss.mu.Lock()
		for id, t := range ss.subs {
			if t == topic {
				targets = append(targets, target{ss, id})
			}
		}
		ss.mu.Unlock()
	}
	for _, tg := range targets {
		_ = tg.ss.sendDown(&Frame{
			Header:   Header{Version: ProtocolVersion, Type: TypePublish, StreamID: tg.id},
			Metadata: encodeMeta("", topic),
			Payload:  data,
		})
	}
	return nil
}

// ---- 发起侧 API（服务端作为 initiator）：向指定客户端会话主动发起交互，
// 客户端以 Handle/HandleStream/HandleChannel/HandleOneWay 注册的 handler 应答。
// Stream ID 由本端按偶数分配（protocol.md §7.3）——端点层方向无关，这里只是门面。

// Sessions 返回当前有活跃连接的会话 ID（升序），可作为下列方法的目标。
func (s *Server) Sessions() []string {
	s.mu.Lock()
	ids := make([]string, 0, len(s.conns))
	for sc := range s.conns {
		ids = append(ids, sc.sess.id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	return ids
}

// sessionEp 解析目标会话当前活跃连接的端点。
func (s *Server) sessionEp(sessionID string) (*endpoint, error) {
	ss := s.store.take(sessionID)
	if ss == nil {
		return nil, fmt.Errorf("jsonstream: unknown session %q", sessionID)
	}
	ss.mu.Lock()
	sc := ss.conn
	ss.mu.Unlock()
	if sc == nil {
		return nil, fmt.Errorf("jsonstream: session %q disconnected", sessionID)
	}
	select {
	case <-sc.ep.tr.dead: // 连接已死、尚未从会话摘除的窗口期，诚实报错
		return nil, fmt.Errorf("jsonstream: session %q disconnected", sessionID)
	default:
	}
	return sc.ep, nil
}

// Request 向指定会话发起请求/响应。
func (s *Server) Request(ctx context.Context, sessionID, route string, payload any) (*Message, error) {
	ep, err := s.sessionEp(sessionID)
	if err != nil {
		return nil, err
	}
	return ep.doRequest(ctx, route, payload)
}

// Stream 向指定会话发起流式响应。
func (s *Server) Stream(ctx context.Context, sessionID, route string, payload any) (*ReadStream, error) {
	ep, err := s.sessionEp(sessionID)
	if err != nil {
		return nil, err
	}
	return ep.doStream(ctx, route, payload)
}

// Channel 向指定会话发起双工通道。
func (s *Server) Channel(ctx context.Context, sessionID, route string, payload any) (*Channel, error) {
	ep, err := s.sessionEp(sessionID)
	if err != nil {
		return nil, err
	}
	return ep.doChannel(ctx, route, payload)
}

// SendOneWay 向指定会话单向发送。
func (s *Server) SendOneWay(sessionID, route string, payload any) error {
	ep, err := s.sessionEp(sessionID)
	if err != nil {
		return err
	}
	return ep.doOneWay(route, payload)
}

// Serve 开始接受连接，阻塞直到 Close。
func (s *Server) Serve() error {
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return nil
			default:
				return err
			}
		}
		go s.handleConn(nc)
	}
}

// Close 停止监听并断开所有活跃连接。
func (s *Server) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	_ = s.ln.Close()
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.conns))
	for sc := range s.conns {
		conns = append(conns, sc)
	}
	s.mu.Unlock()
	for _, sc := range conns {
		sc.ep.tr.kill(ErrClosed)
	}
	return nil
}

// ---- 连接处理 ----

func (s *Server) handleConn(nc net.Conn) {
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(s.cfg.DialTimeout))

	br := bufio.NewReader(nc)
	hf, err := ReadFrame(br)
	if err != nil {
		s.cfg.logger().Printf("jsonstream: server: bad handshake: %v", err)
		return
	}
	if hf.Type != TypeConnect {
		_ = writeOnce(nc, errorFrame(0, &Error{Code: CodeProtocol, Message: "expected CONNECT"}))
		return
	}
	var cj ConnectJSON
	if err := json.Unmarshal(hf.Payload, &cj); err != nil {
		_ = writeOnce(nc, errorFrame(0, &Error{Code: CodeInvalid, Message: "bad CONNECT payload"}))
		return
	}
	if cj.Version != int(ProtocolVersion) {
		_ = writeOnce(nc, errorFrame(0, &Error{Code: CodeProtocol, Message: "version mismatch"}))
		return
	}
	if s.table.auth != nil {
		if err := s.table.auth(&cj); err != nil {
			_ = writeOnce(nc, errorFrame(0, asStreamError(err)))
			return
		}
	}
	if s.cfg.Encrypt && !cj.Encrypt {
		_ = writeOnce(nc, errorFrame(0, &Error{Code: CodeAuthDenied, Message: "encryption required by server"}))
		return
	}

	// 会话恢复：客户端携带 session_id 且保留未过期、未溢出 → resumed。
	resumed := false
	var sess *serverSession
	if cj.SessionID != "" {
		if ss := s.store.take(cj.SessionID); ss != nil && !ss.overflowed {
			sess = ss
			resumed = true
		}
	}
	if sess == nil {
		sess = s.newSession()
	}

	aj := effectiveConnack(s.cfg, &cj)
	aj.SessionID = sess.id
	aj.Resumed = resumed

	connCfg := s.cfg
	connCfg.Compress = aj.Compress
	connCfg.Encrypt = aj.Encrypt
	connCfg.Credit = aj.Credit
	tx, err := newTransformer(&connCfg)
	if err != nil {
		_ = writeOnce(nc, errorFrame(0, &Error{Code: CodeInternal, Message: err.Error()}))
		return
	}

	// endpoint 与 transport 互引用（tr 挂 ep 的分发回调、ep 持 tr），
	// 用闭包晚绑定：tr.start 之前两者都已赋值，回调解引用安全。
	sc := &serverConn{srv: s, sess: sess}
	var ep *endpoint
	tr := newTransport(nc, br, tx, &connCfg, aj.Credit,
		func(f *Frame) error { return ep.handleFrame(f) },
		sc.onDead)
	sc.ep = newEndpoint(false, connCfg, tr, s.table)
	ep = sc.ep
	sc.ep.onSubscribe = sc.onSubscribe
	sc.ep.onUnsubscribe = sc.onUnsubscribe
	sc.ep.downSink = sess.sendDown

	_ = nc.SetDeadline(time.Time{})
	// 先注册、后写 CONNACK：客户端收到 CONNACK 即视为已建立（Dial 返回、
	// 会话可被 Sessions/发起侧 API 寻址、Close 能 kill 到它），连接登记
	// （s.conns）与 sess.conn 绑定都必须 happens-before CONNACK 落网，
	// 否则存在窗口——Close 漏杀该连接（客户端永远收不到断连、handleConn
	// 悬在 <-tr.dead 上泄漏），或发起侧 API 拿到 sess.conn == nil。
	s.mu.Lock()
	s.conns[sc] = struct{}{}
	s.mu.Unlock()
	// bind 同时把 resumed 会话的保留帧排入 sendCh：CONNACK 走 rawWrite
	// 直写 socket，写循环 tr.start 后才排空 sendCh，线上顺序仍为
	// CONNACK → 重放帧 → 实时帧。
	sess.bind(sc)
	if err := tr.rawWrite(mustConnack(&aj)); err != nil {
		s.mu.Lock()
		delete(s.conns, sc)
		s.mu.Unlock()
		return
	}
	tr.start()

	<-tr.dead
	s.mu.Lock()
	delete(s.conns, sc)
	s.mu.Unlock()
	sess.unbind(sc)
}

func (s *Server) newSession() *serverSession {
	ss := &serverSession{
		id:       newSessionID(),
		srv:      s,
		subs:     make(map[uint32]string),
		retained: make(map[uint32][]*Frame),
	}
	s.store.put(ss)
	return ss
}

func mustConnack(aj *connackJSON) *Frame {
	f, err := connackFrame(aj)
	if err != nil {
		panic(err) // connackJSON 编码不会失败
	}
	return f
}

// writeOnce 在启动写循环之前同步写一帧（仅握手期）。
func writeOnce(nc net.Conn, f *Frame) error {
	buf, err := f.appendTo(nil)
	if err != nil {
		return err
	}
	_ = nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = nc.Write(buf)
	return err
}

type serverConn struct {
	srv  *Server
	sess *serverSession
	ep   *endpoint
}

func (sc *serverConn) onSubscribe(id uint32, topic string) error {
	sc.sess.mu.Lock()
	sc.sess.subs[id] = topic
	sc.sess.mu.Unlock()
	return nil
}

func (sc *serverConn) onUnsubscribe(id uint32) {
	sc.sess.mu.Lock()
	delete(sc.sess.subs, id)
	sc.sess.mu.Unlock()
}

func (sc *serverConn) onDead(err error) {
	sc.sess.unbind(sc)
	if err != nil && !errors.Is(err, ErrClosed) {
		sc.srv.cfg.logger().Printf("jsonstream: server: connection lost (session %s): %v", sc.sess.id, err)
	}
}

// ---- 会话与恢复 ----

// serverSession 是跨 TCP 断连存活的逻辑会话：保留订阅关系与断开期间
// 产生的下行帧，供 RESUME 重放（语义见 docs/protocol.md §8）。
type serverSession struct {
	id  string
	srv *Server

	mu            sync.Mutex
	conn          *serverConn
	lastConn      *serverConn // 最近一次绑定的连接（断开后供流迁移/终结）
	subs          map[uint32]string
	retained      map[uint32][]*Frame
	retainedBytes int
	overflowed    bool
	timer         *time.Timer
}

// sendDown 是下行帧落点：连接活着直达 transport；断开期间按流保留进
// 恢复队列（RESPONSE/PUBLISH/SUBACK/COMPLETE/ERROR；CREDIT 不保留）。
func (ss *serverSession) sendDown(f *Frame) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.conn != nil {
		return ss.conn.ep.tr.send(f)
	}
	if f.Type == TypeCredit || ss.overflowed {
		return nil
	}
	switch f.Type {
	case TypeResponse, TypePublish, TypeSubAck, TypeComplete, TypeError:
		ss.retained[f.StreamID] = append(ss.retained[f.StreamID], f)
		ss.retainedBytes += headerSize + metaLenSize + len(f.Metadata) + len(f.Payload)
		if ss.retainedBytes > ss.srv.cfg.RetentionBytes {
			// 超限：会话失去恢复资格（protocol.md §8.1），清空队列。
			ss.overflowed = true
			ss.retained = make(map[uint32][]*Frame)
			ss.retainedBytes = 0
		}
	}
	return nil
}

// bind 把新连接绑到会话，重放保留帧并停掉过期计时。
func (ss *serverSession) bind(sc *serverConn) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.conn != nil && ss.conn != sc {
		// 同会话新连接顶替：废弃旧连接（重连风暴下的 takeover 惯例）。
		ss.conn.ep.tr.kill(errors.New("session taken over by newer connection"))
	}
	// 迁移仍然活跃的 responder 流（客户端发起，奇数 ID）到新连接的
	// endpoint（protocol.md §8.1「未终结流」跨恢复存活）：迁移后客户端
	// 的上行帧（通道数据/CANCEL/额度回授）在新流表命中，handler 不再
	// 悬空。服务端发起的流（偶数 ID）随旧连接消亡，不迁移。迁移必须在
	// tr.start 之前完成，新连接上任何入站帧都晚于流表就绪。
	src := ss.conn
	if src == nil {
		src = ss.lastConn
	}
	if src != nil && src != sc {
		for id, flw := range src.ep.snapshot() {
			if id%2 == 1 && !flw.isDone() {
				flw.setEndpoint(sc.ep)
				sc.ep.registerFlow(flw)
			}
		}
	}
	ss.conn = sc
	ss.lastConn = sc
	if ss.timer != nil {
		ss.timer.Stop()
		ss.timer = nil
	}
	for sid, frames := range ss.retained {
		for _, f := range frames {
			_ = sc.ep.tr.send(f)
		}
		delete(ss.retained, sid)
	}
	ss.retainedBytes = 0
}

// unbind 断开：保留会话等待重连，超过保留期后由 store 清理。
func (ss *serverSession) unbind(sc *serverConn) {
	ss.mu.Lock()
	if ss.conn != sc {
		ss.mu.Unlock()
		return
	}
	ss.conn = nil
	retention := ss.srv.cfg.Retention
	if retention < 0 {
		ss.mu.Unlock()
		ss.srv.store.drop(ss.id)
		return
	}
	ss.timer = time.AfterFunc(retention, func() { ss.srv.store.drop(ss.id) })
	ss.mu.Unlock()
}

type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]*serverSession
}

func (st *sessionStore) put(ss *serverSession) {
	st.mu.Lock()
	st.sessions[ss.id] = ss
	st.mu.Unlock()
}

// take 查找可恢复的会话；不存在返回 nil。会话仍挂着旧连接（TCP 半开、
// 服务端尚未判死）时同样放行——bind 会对旧连接做 takeover（踢旧迎新），
// 否则客户端重连快于服务端感知断开时会被错误地判为会话不可恢复。
func (st *sessionStore) take(id string) *serverSession {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.sessions[id]
}

func (st *sessionStore) drop(id string) {
	st.mu.Lock()
	ss, ok := st.sessions[id]
	if ok {
		delete(st.sessions, id)
	}
	st.mu.Unlock()
	if ok {
		ss.terminate()
	}
}

// terminate 会话终结（保留期到/禁用保留/溢出后由重连走新会话）：fail
// 会话上仍然活跃的 responder 流——handler 的 ctx() 随之取消，应用 handler
// 得以退出；否则会话离开 store 后无人再能取消它们，handler 连同它们的
// 下行产出一起悬挂。锁序：store.mu 与 ss.mu 顺序获取不嵌套，ss.mu 与
// ep.streamsMu 单向（ss.mu → streamsMu）。
func (ss *serverSession) terminate() {
	ss.mu.Lock()
	if ss.timer != nil {
		ss.timer.Stop()
		ss.timer = nil
	}
	src := ss.conn
	if src == nil {
		src = ss.lastConn
	}
	var flows []*flow
	if src != nil {
		for id, flw := range src.ep.snapshot() {
			if id%2 == 1 && !flw.isDone() {
				flows = append(flows, flw)
			}
		}
	}
	ss.mu.Unlock()
	expired := &Error{Code: CodeSessionExpired, Message: "session terminated"}
	for _, flw := range flows {
		flw.fail(expired)
	}
}
