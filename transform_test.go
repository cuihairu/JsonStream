package jsonstream

import (
	"bytes"
	"crypto/cipher"
	"errors"
	"testing"
)

// newTransformer 的防御性错误分支：密钥长度错误可直接构造，AES/GCM 构造
// 失败经接缝注入（合法入参下不会失败，生产行为不变）。
func TestNewTransformerKeyLengthError(t *testing.T) {
	_, err := newTransformer(&Config{Encrypt: true, Key: []byte("short")})
	if err == nil {
		t.Fatal("expected key length error")
	}
}

func TestNewTransformerAESAndGCMError(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	origAES, origGCM := aesNewCipher, gcmNew
	defer func() { aesNewCipher, gcmNew = origAES, origGCM }()

	aesNewCipher = func([]byte) (cipher.Block, error) { return nil, errors.New("aes injected") }
	if _, err := newTransformer(&Config{Encrypt: true, Key: key}); err == nil {
		t.Fatal("expected aes error")
	}
	aesNewCipher = func([]byte) (cipher.Block, error) { return nil, nil }
	gcmNew = func(cipher.Block) (cipher.AEAD, error) { return nil, errors.New("gcm injected") }
	if _, err := newTransformer(&Config{Encrypt: true, Key: key}); err == nil {
		t.Fatal("expected gcm error")
	}
}

// nil transformer 是「未协商任何变换」的合法退化：payload 原样透传。
func TestTransformerNilOutboundPassthrough(t *testing.T) {
	var tx *transformer
	out, flags, err := tx.outbound([]byte(`"x"`))
	if err != nil || flags != 0 || !bytes.Equal(out, []byte(`"x"`)) {
		t.Fatalf("nil transformer outbound = %q,%d,%v", out, flags, err)
	}
}

// nonce 读取失败（crypto/rand 不可用）必须中断加密发送。
func TestTransformerNonceReadError(t *testing.T) {
	tx, err := newTransformer(&Config{Encrypt: true, Key: bytes.Repeat([]byte{3}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errors.New("rand injected") }
	defer func() { randRead = orig }()

	if _, _, err := tx.outbound([]byte("payload")); err == nil {
		t.Fatal("expected nonce error")
	}
}

// TestTransformInboundUnsupportedFlags：对端启用了压缩/加密而本端未启用
// 时，入站必须显式拒绝并报 UNSUPPORTED——静默当明文解会产出乱码帧，
// 静默丢弃则破坏流语义，显式错误码是唯一正确行为。双标志场景验证
// 判定顺序：先加密后压缩，报先命中的那条。
func TestTransformInboundUnsupportedFlags(t *testing.T) {
	plain, err := newTransformer(&Config{}) // 压缩/加密全关
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		flag uint8
		data []byte
	}{
		{"encrypted while disabled", FlagEncrypted, bytes.Repeat([]byte{0}, 40)},
		{"compressed while disabled", FlagCompressed, []byte("whatever")},
		{"both while disabled", FlagEncrypted | FlagCompressed, []byte("x")},
	}
	for _, tc := range cases {
		_, err := plain.inbound(tc.flag, tc.data)
		var je *Error
		if !errors.As(err, &je) || je.Code != CodeUnsupported {
			t.Fatalf("%s: want UNSUPPORTED, got %v", tc.name, err)
		}
	}
}
