package jsonstream

import (
	"errors"
	"testing"
	"time"
)

// effectiveConnack 的协商语义（§6.1）：以服务端为上限、客户端为请求，
// credit 取双方 >0 声明的较小值；压缩/加密双方都开才生效。
func TestEffectiveConnack(t *testing.T) {
	srv := Config{
		Compress: true, Encrypt: true,
		Heartbeat: 3 * time.Second, Retention: 40 * time.Second,
		Credit: 8,
	}
	tests := []struct {
		name string
		cj   ConnectJSON
		want connackJSON
	}{
		{
			name: "双方全开+客户端 credit 更小",
			cj:   ConnectJSON{Version: 1, Compress: true, Encrypt: true, Credit: 2},
			want: connackJSON{Compress: true, Encrypt: true, Credit: 2, HeartbeatMS: 3000, RetentionMS: 40000},
		},
		{
			name: "客户端拒绝压缩但同意加密",
			cj:   ConnectJSON{Version: 1, Compress: false, Encrypt: true, Credit: 16},
			want: connackJSON{Compress: false, Encrypt: true, Credit: 8, HeartbeatMS: 3000, RetentionMS: 40000},
		},
		{
			name: "客户端 credit 为 0（关闭背压请求）",
			cj:   ConnectJSON{Version: 1, Credit: 0},
			want: connackJSON{HeartbeatMS: 3000, RetentionMS: 40000},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveConnack(srv, &tc.cj)
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
	// 双方都未启用 credit：生效值为 0
	if aj := effectiveConnack(Config{}, &ConnectJSON{Version: 1, Credit: 4}); aj.Credit != 0 {
		t.Fatalf("credit with server disabled = %d, want 0", aj.Credit)
	}
	// 客户端声明 0（拒绝背压）：服务端即使启用也不得强加窗口
	if aj := effectiveConnack(Config{Credit: 8}, &ConnectJSON{Version: 1}); aj.Credit != 0 {
		t.Fatalf("credit with client declined = %d, want 0", aj.Credit)
	}
}

// 握手帧编码的 marshal 错误分支（经 jsonMarshal 接缝注入）。
func TestHandshakeFrameMarshalError(t *testing.T) {
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal failed") }
	defer func() { jsonMarshal = orig }()

	if _, err := connectFrame(&ConnectJSON{}); err == nil {
		t.Fatal("connectFrame must surface marshal errors")
	}
	if _, err := connackFrame(&connackJSON{}); err == nil {
		t.Fatal("connackFrame must surface marshal errors")
	}
}
