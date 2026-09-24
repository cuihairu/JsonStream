package jsonstream

import (
	"bytes"
	"compress/flate"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
)

// minCompressSize 之下的载荷不值得压缩：短 JSON 的压缩率常不足 1，
// flate 流自身的头开销就足以吃掉全部收益（编解码器已池化，冷启动不是
// 阈值的依据）。
const minCompressSize = 64

// flate 编解码器按帧新建的代价极高（实测仅 NewWriter 即 ~1.3ms / 800KB
// 分配，远超压缩本身），用池复用；每次取出先 Reset 重置状态，用坏也
// 无碍——Reset 会重新初始化。
var (
	flateWriterPool = sync.Pool{New: func() any {
		w, _ := flate.NewWriter(nil, flate.DefaultCompression)
		return w
	}}
	flateReaderPool = sync.Pool{New: func() any {
		return flate.NewReader(nil)
	}}
)

// transformer 是单连接的 payload 变换管线：JSON → flate（可选）→ AES-256-GCM（可选）。
// 先压后加密：加密输出近似随机字节，先加密后压缩得不到任何压缩率。
type transformer struct {
	compress bool
	encrypt  bool
	gcm      cipher.AEAD
}

// crypto/flate 原语的包级接缝：这些构造在合法入参下不会失败，测试注入
// 错误以覆盖防御性分支（生产行为不变）。
var (
	aesNewCipher = aes.NewCipher
	gcmNew       = cipher.NewGCM
	randRead     = rand.Read
)

func newTransformer(cfg *Config) (*transformer, error) {
	t := &transformer{compress: cfg.Compress, encrypt: cfg.Encrypt}
	if t.encrypt {
		if len(cfg.Key) != 32 {
			return nil, fmt.Errorf("jsonstream: encryption requires a 32-byte AES-256 key, got %d bytes", len(cfg.Key))
		}
		block, err := aesNewCipher(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: aes: %w", err)
		}
		gcm, err := gcmNew(block)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: gcm: %w", err)
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
		// Write/Close 的错误只可能来自底层写入器；此处恒为 bytes.Buffer
		// （Reset 后写入），16MiB 上界的压缩输出远不可及 ErrTooLarge，
		// 两者的错误分支是构造性死码，不设检查。
		w := flateWriterPool.Get().(*flate.Writer)
		w.Reset(&buf)
		_, _ = w.Write(out)
		_ = w.Close()
		flateWriterPool.Put(w)
		out = buf.Bytes()
		flagBits |= FlagCompressed
	}
	if t.encrypt {
		nonce := make([]byte, t.gcm.NonceSize())
		if _, err := randRead(nonce); err != nil {
			return nil, 0, fmt.Errorf("jsonstream: nonce: %w", err)
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
		// 取出即 Reset：上一使用者遗留的流状态（含越限中断的半流）被
		// 重新初始化，池化不会跨帧泄漏状态
		rc := flateReaderPool.Get().(io.ReadCloser)
		// flate 的 Resetter.Reset 恒返回 nil（接口兼容保留返回值）
		_ = rc.(flate.Resetter).Reset(bytes.NewReader(data), nil)
		out, err := readAll(rc, MaxPayloadSize)
		if cerr := rc.Close(); err == nil {
			err = cerr
		}
		flateReaderPool.Put(rc)
		if err != nil {
			return nil, fmt.Errorf("jsonstream: decompress: %w", err)
		}
		data = out
	}
	return data, nil
}
