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
