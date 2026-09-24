package jsonstream

import (
	"testing"
	"time"
)

// normalized 的默认值补全：零值与欠佳配置都落到安全默认。
func TestConfigNormalizedDefaults(t *testing.T) {
	got := (&Config{}).normalized()
	if got.Heartbeat != DefaultHeartbeat || got.Retention != DefaultRetention ||
		got.RetentionBytes != DefaultRetentionBytes || got.DialTimeout != DefaultDialTimeout ||
		got.BackoffInitial != DefaultBackoffInitial || got.BackoffMax != DefaultBackoffMax {
		t.Fatalf("zero config not fully defaulted: %+v", got)
	}

	// 心跳下限钳制：过激配置不允许把连接打挂
	clamped := (&Config{Heartbeat: time.Millisecond}).normalized()
	if clamped.Heartbeat != time.Second {
		t.Fatalf("aggressive heartbeat = %v, want clamped to 1s", clamped.Heartbeat)
	}

	// 原配置不被修改；已设置的字段原样保留
	src := Config{Heartbeat: 2 * time.Second, DialTimeout: time.Second}
	_ = (&src).normalized()
	if src.DialTimeout != time.Second {
		t.Fatal("normalized must not mutate the receiver")
	}
}

func TestConfigLogger(t *testing.T) {
	if _, ok := (&Config{}).logger().(discardLogger); !ok {
		t.Fatal("nil Logger must default to discardLogger")
	}
	custom := &capturedLogger{}
	if (&Config{Logger: custom}).logger() != Logger(custom) {
		t.Fatal("configured Logger must be returned as-is")
	}
	// 静默日志器可调用（无副作用）
	(&Config{}).logger().Printf("nothing %d", 1)
}

// capturedLogger 供 logger 路由断言使用。
type capturedLogger struct{ msgs []string }

func (c *capturedLogger) Printf(format string, _ ...any) { c.msgs = append(c.msgs, format) }
