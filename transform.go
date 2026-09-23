package jsonstream

import (
	"bytes"
	"compress/flate"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

// minCompressSize 之下的载荷不值得压缩（flate 的帧内冷启动开销大于收益）。
const minCompressSize = 64

// transformer 是单连接的 payload 变换管线：JSON → flate（可选）→ AES-256-GCM（可选）。
// 先压后加密：加密输出近似随机字节，先加密后压缩得不到任何压缩率。
type transformer struct {
	compress bool
	encrypt  bool
	gcm      cipher.AEAD
}

func newTransformer(cfg *Config) (*transformer, error) {
	t := &transformer{compress: cfg.Compress, encrypt: cfg.Encrypt}
	if t.encrypt {
		if len(cfg.Key) != 32 {
			return nil, fmt.Errorf("jsonstream: encryption requires a 32-byte AES-256 key, got %d bytes", len(cfg.Key))
		}
		block, err := aes.NewCipher(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: aes: %v", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: gcm: %v", err)
		}
		t.gcm = gcm
	}
	return t, nil
}

// outbound 返回变换后的 payload 与应置位的 Flags。
func (t *transformer) outbound(data []byte) ([]byte, uint8, error) {
	out := data
	var flagBits uint8
	if t == nil {
		return out, 0, nil
	}
	if t.compress && len(out) >= minCompressSize {
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, flate.DefaultCompression)
		if err != nil {
			return nil, 0, fmt.Errorf("jsonstream: flate: %v", err)
		}
		if _, err := w.Write(out); err != nil {
			return nil, 0, fmt.Errorf("jsonstream: compress: %v", err)
		}
		if err := w.Close(); err != nil {
			return nil, 0, fmt.Errorf("jsonstream: compress: %v", err)
		}
		out = buf.Bytes()
		flagBits |= FlagCompressed
	}
	if t.encrypt {
		nonce := make([]byte, t.gcm.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, 0, fmt.Errorf("jsonstream: nonce: %v", err)
		}
		out = t.gcm.Seal(nonce, nonce, out, nil)
		flagBits |= FlagEncrypted
	}
	return out, flagBits, nil
}

// inbound 按帧内 Flags 逆向变换；配置未启用却收到对应标志视为协议违规。
func (t *transformer) inbound(flagBits uint8, data []byte) ([]byte, error) {
	if t == nil {
		t = &transformer{}
	}
	if flagBits&FlagEncrypted != 0 {
		if !t.encrypt {
			return nil, &Error{Code: CodeUnsupported, Message: "received encrypted frame but encryption is disabled"}
		}
		ns := t.gcm.NonceSize()
		if len(data) < ns+16 {
			return nil, fmt.Errorf("%w: encrypted payload too short", ErrMalformed)
		}
		plain, err := t.gcm.Open(nil, data[:ns], data[ns:], nil)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: decrypt: %w", err)
		}
		data = plain
	}
	if flagBits&FlagCompressed != 0 {
		if !t.compress {
			return nil, &Error{Code: CodeUnsupported, Message: "received compressed frame but compression is disabled"}
		}
		r := flate.NewReader(bytes.NewReader(data))
		out, err := readAll(r, MaxPayloadSize)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: decompress: %w", err)
		}
		if err := r.Close(); err != nil {
			return nil, fmt.Errorf("jsonstream: decompress: %w", err)
		}
		data = out
	}
	return data, nil
}
