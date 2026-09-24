package jsonstream

import (
	"context"
	"encoding/json"
	"sync"
)

// endpoint 是连接一端的流调度核心：帧分发、路由响应、发起各类交互。
// Client 与 Server 的连接各自持有一个；差异逻辑（客户端重连、服务端
// 会话保留）以钩子注入（onSubscribe / onUnsubscribe / downSink）。
type endpoint struct {
	clientSide bool
	cfg        Config
	tr         *transport
	table      *routeTable
	log        Logger

	// creditWindow > 0 表示启用了连接级信用背压（生效值）。
	creditWindow  int
	creditFlushAt int

	idMu   sync.Mutex
	nextID uint32 // client 从 1 起奇数，server 从 2 起偶数（protocol.md §7.3）

	streamsMu sync.RWMutex
	streams   map[uint32]*flow

	// server 侧注入的钩子
	onSubscribe   func(id uint32, topic string) error
	onUnsubscribe func(id uint32)
	// downSink 非 nil 时，下行帧（对本端被动流的响应）经它发出——
	// 服务端借此在断连期间把帧保留进会话恢复队列。
	downSink func(f *Frame) error
}

func newEndpoint(clientSide bool, cfg Config, tr *transport, table *routeTable) *endpoint {
	ep := &endpoint{
		clientSide: clientSide,
		cfg:        cfg,
		tr:         tr,
		table:      table,
		log:        cfg.logger(),
		streams:    make(map[uint32]*flow),
	}
	ep.creditWindow = cfg.Credit
	if ep.creditWindow > 0 {
		half := ep.creditWindow / 2
		if half < 1 {
			half = 1
		}
		ep.creditFlushAt = half
	}
	return ep
}

// allocID 按奇偶划分分配 Stream ID：双方同时主动发起也不会撞号。
func (ep *endpoint) allocID() uint32 {
	ep.idMu.Lock()
	defer ep.idMu.Unlock()
	if ep.nextID == 0 {
		if ep.clientSide {
			ep.nextID = 1
		} else {
			ep.nextID = 2
		}
	}
	id := ep.nextID
	ep.nextID += 2
	return id
}

func (ep *endpoint) registerFlow(flw *flow) {
	ep.streamsMu.Lock()
	ep.streams[flw.id] = flw
	ep.streamsMu.Unlock()
}

func (ep *endpoint) unregisterFlow(flw *flow) {
	ep.streamsMu.Lock()
	if ep.streams[flw.id] == flw {
		delete(ep.streams, flw.id)
	}
	ep.streamsMu.Unlock()
}

func (ep *endpoint) lookupFlow(id uint32) *flow {
	ep.streamsMu.RLock()
	defer ep.streamsMu.RUnlock()
	return ep.streams[id]
}

// snapshot 返回当前活跃流的浅拷贝（重连恢复时迁移用）。
func (ep *endpoint) snapshot() map[uint32]*flow {
	ep.streamsMu.RLock()
	defer ep.streamsMu.RUnlock()
	out := make(map[uint32]*flow, len(ep.streams))
	for id, flw := range ep.streams {
		out[id] = flw
	}
	return out
}

// emit 发送一帧下行数据；server 侧经会话保留落点，client 侧直达 transport。
func (ep *endpoint) emit(f *Frame) error {
	if ep.downSink != nil {
		return ep.downSink(f)
	}
	return ep.tr.send(f)
}

// ---- 帧分发（transport 读循环回调） ----

// handleFrame 返回非 nil error 表示协议违规，transport 会断开连接。
func (ep *endpoint) handleFrame(f *Frame) error {
	switch f.Type {
	case TypeResponse:
		flw := ep.lookupFlow(f.StreamID)
		if flw == nil {
			// 未知/已终结流上的迟到帧：忽略（不做协议错误，CANCEL 竞态下属正常）。
			return nil
		}
		flw.creditIn()
		flw.deliver(f)
		return nil

	case TypePublish:
		// 先按"订阅投递"处理（本端有对应订阅流）；否则视为对端主动发布，
		// 路由到本端注册的主题 handler；都没有则静默丢弃（§7.8）。
		if flw := ep.lookupFlow(f.StreamID); flw != nil {
			flw.creditIn()
			flw.deliver(f)
			return nil
		}
		meta := decodeMeta(f.Metadata)
		if h, ok := ep.table.lookupTopic(meta.Topic); ok {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						ep.log.Printf("jsonstream: topic handler panic on %q: %v", meta.Topic, r)
					}
				}()
				if err := h(toMessage(f)); err != nil {
					_ = ep.emit(errorFrame(f.StreamID, asStreamError(err)))
				}
			}()
		}
		return nil

	case TypeSubAck:
		if flw := ep.lookupFlow(f.StreamID); flw != nil {
			flw.ack()
		}
		return nil

	case TypeComplete:
		if flw := ep.lookupFlow(f.StreamID); flw != nil {
			flw.complete()
		}
		return nil

	case TypeError:
		e := errDecode(f.Payload)
		if f.StreamID == 0 {
			// 连接级错误：协议状态已不可信，必须断开（protocol.md §7.10）。
			return &Error{Code: e.Code, Message: "peer fatal: " + e.Message}
		}
		if flw := ep.lookupFlow(f.StreamID); flw != nil {
			flw.fail(e)
		}
		return nil

	case TypeCancel:
		if flw := ep.lookupFlow(f.StreamID); flw != nil {
			flw.fail(&Error{Code: CodeCancelled, Message: "cancelled by peer"})
		}
		return nil

	case TypeUnsubscribe:
		if ep.onUnsubscribe != nil {
			ep.onUnsubscribe(f.StreamID)
		}
		return nil

	case TypeCredit:
		var cp creditPayload
		if err := json.Unmarshal(f.Payload, &cp); err != nil {
			return &Error{Code: CodeProtocol, Message: "bad credit payload"}
		}
		ep.tr.credit.add(cp.N)
		return nil

	case TypeSubscribe:
		if ep.onSubscribe == nil {
			return nil // client 不会收到 SUBSCRIBE；忽略
		}
		meta := decodeMeta(f.Metadata)
		if err := ep.onSubscribe(f.StreamID, meta.Topic); err != nil {
			_ = ep.emit(errorFrame(f.StreamID, asStreamError(err)))
			return nil
		}
		_ = ep.emit(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeSubAck, StreamID: f.StreamID}})
		return nil

	case TypeRequest:
		// flow 必须在 readLoop 的同步路径注册：对端发起 Channel 后会立即
		// 发数据帧，若注册在异步 goroutine 里，背靠背到达的数据帧会因
		// lookupFlow miss 而被静默丢弃（死等互锁）。handler 的执行保持异步。
		if flw := ep.prepareRequest(f); flw != nil {
			go ep.runRequest(flw, f)
		}
		return nil

	case TypeOneWay:
		go ep.serveOneWay(f)
		return nil

	default:
		return &Error{Code: CodeProtocol, Message: "unknown frame type " + f.Type.String()}
	}
}

// ---- 响应路径（本端作为 responder） ----

// prepareRequest 在 readLoop 同步路径上完成被动流的准备：路由查找、
// 模式校验、flow 注册；失败时回 ERROR 帧并返回 nil。注册必须同步完成，
// 否则对端紧随 REQUEST 之后的通道数据帧会因 flow 未注册而被丢弃。
func (ep *endpoint) prepareRequest(f *Frame) *flow {
	meta := decodeMeta(f.Metadata)

	if f.Flags&FlagChannel != 0 {
		entry, ok := ep.table.lookupChannel(meta.Route)
		if !ok {
			_ = ep.emit(errorFrame(f.StreamID, &Error{Code: CodeNotFound, Message: "no handler for channel " + meta.Route}))
			return nil
		}
		flw := newFlow(f.StreamID, flowChannel, ep)
		flw.chEntry = entry
		ep.registerFlow(flw)
		return flw
	}

	entry, ok := ep.table.lookupRoute(meta.Route)
	if !ok {
		_ = ep.emit(errorFrame(f.StreamID, &Error{Code: CodeNotFound, Message: "no handler for route " + meta.Route}))
		return nil
	}

	var kind flowKind
	if entry.kind == kindStream {
		kind = flowStream
	} else {
		kind = flowRequest
	}
	// 模式校验：请求声明的 Flags 与路由注册的类型必须一致（protocol.md §2.2 的代价说明）。
	if entry.kind == kindRequest && f.Flags&FlagStream != 0 {
		_ = ep.emit(errorFrame(f.StreamID, &Error{Code: CodeProtocol, Message: "route " + meta.Route + " is request/response, not a stream"}))
		return nil
	}
	if entry.kind == kindStream && f.Flags&FlagStream == 0 {
		_ = ep.emit(errorFrame(f.StreamID, &Error{Code: CodeProtocol, Message: "route " + meta.Route + " is a stream, not request/response"}))
		return nil
	}

	flw := newFlow(f.StreamID, kind, ep)
	flw.reqEntry = entry
	ep.registerFlow(flw)
	return flw
}

// runRequest 执行已注册流的 handler 并收尾（发起侧视角见 doRequest/doStream）。
func (ep *endpoint) runRequest(flw *flow, f *Frame) {
	defer func() {
		if r := recover(); r != nil {
			ep.log.Printf("jsonstream: handler panic on stream %d: %v", f.StreamID, r)
			_ = ep.emit(errorFrame(f.StreamID, &Error{Code: CodeInternal, Message: "internal error"}))
		}
	}()

	switch flw.kind {
	case flowChannel:
		ep.runChannel(flw)
	case flowRequest:
		req := &Request{Message: toMessage(f), ctx: flw.ctx()}
		v, err := flw.reqEntry.handle(req)
		if err != nil {
			_ = ep.emit(errorFrame(flw.id, asStreamError(err)))
		} else {
			// handler 返回 nil 时响应 "null"；单帧自带终结语义（对齐 RSocket REQUEST_RESPONSE）。
			data, merr := json.Marshal(v)
			if merr != nil {
				_ = ep.emit(errorFrame(flw.id, &Error{Code: CodeInvalid, Message: "response encode: " + merr.Error()}))
			} else {
				_ = ep.emit(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeResponse, StreamID: flw.id}, Payload: data})
			}
		}
		flw.finish(nil)

	case flowStream:
		req := &Request{Message: toMessage(f), ctx: flw.ctx()}
		err := flw.reqEntry.handleStream(req, &emitter{ep: ep, flw: flw})
		if err != nil {
			_ = ep.emit(errorFrame(flw.id, asStreamError(err)))
		} else {
			_ = ep.emit(completeFrame(flw.id))
		}
		flw.finish(nil)
	}
}

// runChannel 运行双工通道 handler；handler 返回视为本端发完，补发
// COMPLETE（若 handler 已 Close 则幂等）。
func (ep *endpoint) runChannel(flw *flow) {
	ch := &Channel{f: flw, ctx: flw.ctx()}
	if err := flw.chEntry(ch); err != nil {
		_ = ep.emit(errorFrame(flw.id, asStreamError(err)))
		flw.finish(nil)
		return
	}
	_ = ch.Close()
}

func (ep *endpoint) serveOneWay(f *Frame) {
	defer func() {
		if r := recover(); r != nil {
			// ONEWAY 永不回帧（protocol.md §7.7），panic 只留日志。
			ep.log.Printf("jsonstream: oneway handler panic on stream %d: %v", f.StreamID, r)
		}
	}()
	meta := decodeMeta(f.Metadata)
	h, ok := ep.table.lookupOneWay(meta.Route)
	if !ok {
		return // 协议规定：路由不存在也静默
	}
	_ = h(toMessage(f))
}

// ---- 发起路径（本端作为 initiator） ----

func (ep *endpoint) sendRequestFrame(id uint32, route string, flags uint8, payload []byte) error {
	return ep.tr.send(&Frame{
		Header:   Header{Version: ProtocolVersion, Flags: flags, Type: TypeRequest, StreamID: id},
		Metadata: encodeMeta(route, ""),
		Payload:  payload,
	})
}

// doRequest 请求/响应：等单帧 RESPONSE（其本身即终结）。
func (ep *endpoint) doRequest(ctx context.Context, route string, payload any) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	flw := newFlow(ep.allocID(), flowRequest, ep)
	ep.registerFlow(flw)
	if err := ep.sendRequestFrame(flw.id, route, 0, data); err != nil {
		ep.unregisterFlow(flw)
		return nil, err
	}
	select {
	case rf := <-flw.frames:
		flw.finish(nil)
		return toMessage(rf), nil
	case <-flw.doneCh:
		// 先排空可能已入队的响应帧（select 双就绪随机选择）。
		select {
		case rf := <-flw.frames:
			flw.finish(nil)
			return toMessage(rf), nil
		default:
		}
		if flw.err != nil {
			return nil, flw.err
		}
		return nil, ErrClosed
	case <-ctx.Done():
		_ = ep.tr.send(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeCancel, StreamID: flw.id}})
		flw.fail(&Error{Code: CodeCancelled, Message: ctx.Err().Error()})
		return nil, ctx.Err()
	}
}

// doStream 发起流式请求。
func (ep *endpoint) doStream(_ context.Context, route string, payload any) (*ReadStream, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	flw := newFlow(ep.allocID(), flowStream, ep)
	ep.registerFlow(flw)
	if err := ep.sendRequestFrame(flw.id, route, FlagStream, data); err != nil {
		ep.unregisterFlow(flw)
		return nil, err
	}
	return &ReadStream{f: flw}, nil
}

// doChannel 发起双工通道。
func (ep *endpoint) doChannel(ctx context.Context, route string, payload any) (*Channel, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	flw := newFlow(ep.allocID(), flowChannel, ep)
	ep.registerFlow(flw)
	if err := ep.sendRequestFrame(flw.id, route, FlagStream|FlagChannel, data); err != nil {
		ep.unregisterFlow(flw)
		return nil, err
	}
	return &Channel{f: flw, ctx: ctx}, nil
}

// doOneWay 单向发送；协议保证不产生任何响应帧。
func (ep *endpoint) doOneWay(route string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ep.tr.send(&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeOneWay, StreamID: ep.allocID()},
		Metadata: encodeMeta(route, ""),
		Payload:  data,
	})
}

// doSubscribe 订阅主题，成功后启动投递消费 goroutine。
func (ep *endpoint) doSubscribe(ctx context.Context, topic string, h func(*Message) error) (*Subscription, error) {
	flw := newFlow(ep.allocID(), flowSubscribe, ep)
	ep.registerFlow(flw)
	if err := ep.tr.send(&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypeSubscribe, StreamID: flw.id},
		Metadata: encodeMeta("", topic),
	}); err != nil {
		ep.unregisterFlow(flw)
		return nil, err
	}
	select {
	case <-flw.ackCh:
	case <-flw.doneCh:
		ep.unregisterFlow(flw)
		if flw.err != nil {
			return nil, flw.err
		}
		return nil, ErrClosed
	case <-ctx.Done():
		_ = ep.tr.send(&Frame{Header: Header{Version: ProtocolVersion, Type: TypeCancel, StreamID: flw.id}})
		flw.fail(&Error{Code: CodeCancelled, Message: ctx.Err().Error()})
		return nil, ctx.Err()
	}
	sub := &Subscription{topic: topic, h: h}
	sub.mu.Lock()
	sub.f = flw
	sub.mu.Unlock()
	sub.consumeWg.Add(1)
	go sub.consume()
	return sub, nil
}

// doPublish 把消息发布到对端主题（client→server 方向；server 广播见 Server.Publish）。
func (ep *endpoint) doPublish(topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ep.tr.send(&Frame{
		Header:   Header{Version: ProtocolVersion, Type: TypePublish, StreamID: ep.allocID()},
		Metadata: encodeMeta("", topic),
		Payload:  data,
	})
}
