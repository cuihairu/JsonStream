package jsonstream

import "sync"

type handlerKind uint8

const (
	kindRequest handlerKind = iota // 返回值即单帧响应
	kindStream                     // Emitter 多帧下发 + 自动 COMPLETE
)

// routeTable 是一端的路由注册表。注册与查找全程走 RWMutex：注册可
// 并发调用，运行中注册对新请求立即生效（既有惯例仍是 Serve/Dial 前
// 完成全部注册，语义最简单）。
type routeTable struct {
	mu       sync.RWMutex
	routes   map[string]routeEntry
	channels map[string]func(*Channel) error
	oneways  map[string]func(*Message) error
	topics   map[string]func(*Message) error
	auth     func(*ConnectJSON) error
}

type routeEntry struct {
	kind         handlerKind
	handle       func(*Request) (any, error)
	handleStream func(*Request, Emitter) error
}

func newRouteTable() *routeTable {
	return &routeTable{
		routes:   make(map[string]routeEntry),
		channels: make(map[string]func(*Channel) error),
		oneways:  make(map[string]func(*Message) error),
		topics:   make(map[string]func(*Message) error),
	}
}

// Handle 注册请求/响应路由；重复注册会覆盖旧值。
func (t *routeTable) Handle(route string, h func(*Request) (any, error)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.routes[route] = routeEntry{kind: kindRequest, handle: h}
}

// HandleStream 注册流式路由（handler 正常返回自动 COMPLETE，返回 error 转 ERROR 帧）。
func (t *routeTable) HandleStream(route string, h func(*Request, Emitter) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.routes[route] = routeEntry{kind: kindStream, handleStream: h}
}

// HandleChannel 注册双工通道路由。
func (t *routeTable) HandleChannel(route string, h func(*Channel) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.channels[route] = h
}

// HandleOneWay 注册单向路由。ONEWAY 永不回帧：路由不存在时也静默丢弃。
func (t *routeTable) HandleOneWay(route string, h func(*Message) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.oneways[route] = h
}

// HandlePublish 注册对端发布（PUBLISH）主题的处理。
func (t *routeTable) HandlePublish(topic string, h func(*Message) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.topics[topic] = h
}

// OnAuth 注册握手鉴权钩子；返回 error 时以该错误拒绝连接。
func (t *routeTable) OnAuth(h func(*ConnectJSON) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.auth = h
}

func (t *routeTable) lookupRoute(route string) (routeEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.routes[route]
	return e, ok
}

func (t *routeTable) lookupChannel(route string) (func(*Channel) error, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.channels[route]
	return h, ok
}

func (t *routeTable) lookupOneWay(route string) (func(*Message) error, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.oneways[route]
	return h, ok
}

func (t *routeTable) lookupTopic(topic string) (func(*Message) error, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.topics[topic]
	return h, ok
}
