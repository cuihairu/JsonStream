package jsonstream

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func readAll(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
}

// Logger 是协议栈的日志接口，适配 *log.Logger 等常见实现。
type Logger interface {
	Printf(format string, v ...any)
}

// 协议参数默认值。
const (
	DefaultHeartbeat      = 10 * time.Second
	DefaultRetention      = 30 * time.Second
	DefaultRetentionBytes = 4 << 20
	DefaultBackoffInitial = 100 * time.Millisecond
	DefaultBackoffMax     = 5 * time.Second
	DefaultDialTimeout    = 5 * time.Second
)

// Config 是连接两侧共享的协议参数。生效值以 CONNACK 下发的为准
// （服务端是权威），字段含义见 docs/protocol.md。
type Config struct {
	// Heartbeat 是 PING 间隔；读空闲超过 1.5 倍即判定断连。零值取默认。
	Heartbeat time.Duration
	// Compress 启用 flate 压缩（载荷 ≥64B 才实际压缩）。
	Compress bool
	// Encrypt 启用 AES-256-GCM；启用时 Key 必须是 32 字节。
	Encrypt bool
	// Key 是预共享密钥（AES-256），不在线上传输。
	Key []byte
	// Credit >0 时启用连接级信用背压窗口（条数）；生效值 = min(双方配置)。
	Credit int
	// Auth 透传到 CONNECT 的令牌，服务端在握手钩子中校验。
	Auth string
	// Retention 是服务端断连后的会话保留期；负值禁用会话恢复。
	Retention time.Duration
	// RetentionBytes 是每会话下行保留队列的字节上限，超限则失去恢复资格。
	RetentionBytes int
	// DialTimeout 是建立 TCP 连接的超时。
	DialTimeout time.Duration
	// Reconnect 为 false 时客户端不自动重连（默认 true）。
	Reconnect *bool
	// BackoffInitial / BackoffMax 是重连退避的起点与上限（指数 + 抖动）。
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	// Logger 为 nil 时静默。
	Logger Logger
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Heartbeat:      DefaultHeartbeat,
		Retention:      DefaultRetention,
		RetentionBytes: DefaultRetentionBytes,
		DialTimeout:    DefaultDialTimeout,
		Reconnect:      ptr(true),
		BackoffInitial: DefaultBackoffInitial,
		BackoffMax:     DefaultBackoffMax,
	}
}

func ptr[T any](v T) *T { return &v }

// normalized 返回补全默认值后的副本。
func (c *Config) normalized() Config {
	n := *c
	if n.Heartbeat == 0 {
		n.Heartbeat = DefaultHeartbeat
	}
	if n.Heartbeat < time.Second {
		n.Heartbeat = time.Second // 防止过激配置把连接打挂
	}
	if n.Retention == 0 {
		n.Retention = DefaultRetention
	}
	if n.RetentionBytes == 0 {
		n.RetentionBytes = DefaultRetentionBytes
	}
	if n.DialTimeout == 0 {
		n.DialTimeout = DefaultDialTimeout
	}
	if n.BackoffInitial == 0 {
		n.BackoffInitial = DefaultBackoffInitial
	}
	if n.BackoffMax == 0 {
		n.BackoffMax = DefaultBackoffMax
	}
	return n
}

func (c *Config) reconnectEnabled() bool { return c.Reconnect == nil || *c.Reconnect }

// discardLogger 是 Logger 缺省实现：静默。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

func (c *Config) logger() Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return discardLogger{}
}
