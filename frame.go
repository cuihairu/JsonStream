package jsonstream

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// 线上帧布局（大端序），见 docs/protocol.md §3：
//
//	 0  1   Magic "JS"
//	 2      Version
//	 3      Flags
//	 4      Type
//	 5      Reserved
//	 6..9   Stream ID
//	10..13  Payload Length（变换后字节数）
//	14..15  Meta Length（仅 Flags.HasMeta）+ Metadata（明文 JSON）
//	...     Payload
const (
	magic0 = 0x4A // 'J'
	magic1 = 0x53 // 'S'

	// ProtocolVersion 是当前协议版本。
	ProtocolVersion uint8 = 1

	// MaxPayloadSize 是单帧载荷（变换后）的上限，超限直接断开以防 OOM。
	MaxPayloadSize = 16 << 20 // 16 MiB
	// MaxMetadataSize 是 Metadata 的上限。线上 metaLen 字段是 uint16，
	// 恰 64 KiB 的元数据无法表达（编码会截断成 0），故上界为 64 KiB-1。
	MaxMetadataSize = math.MaxUint16 // 64 KiB - 1

	headerSize  = 14
	metaLenSize = 2
)

// FrameType 帧类型，见 docs/protocol.md §4。
type FrameType uint8

// 帧类型（§4）：未知类型一律 ERROR(PROTOCOL) 断开，为新能力演进留位。
const (
	TypeConnect     FrameType = 0x01
	TypeConnAck     FrameType = 0x02
	TypePing        FrameType = 0x03
	TypePong        FrameType = 0x04
	TypeRequest     FrameType = 0x05
	TypeResponse    FrameType = 0x06
	TypeComplete    FrameType = 0x07
	TypeCancel      FrameType = 0x08
	TypeOneWay      FrameType = 0x09
	TypeError       FrameType = 0x0A
	TypeSubscribe   FrameType = 0x0B
	TypeSubAck      FrameType = 0x0C
	TypeUnsubscribe FrameType = 0x0D
	TypePublish     FrameType = 0x0E
	TypeCredit      FrameType = 0x0F
)

func (t FrameType) String() string {
	switch t {
	case TypeConnect:
		return "CONNECT"
	case TypeConnAck:
		return "CONNACK"
	case TypePing:
		return "PING"
	case TypePong:
		return "PONG"
	case TypeRequest:
		return "REQUEST"
	case TypeResponse:
		return "RESPONSE"
	case TypeComplete:
		return "COMPLETE"
	case TypeCancel:
		return "CANCEL"
	case TypeOneWay:
		return "ONEWAY"
	case TypeError:
		return "ERROR"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeSubAck:
		return "SUBACK"
	case TypeUnsubscribe:
		return "UNSUBSCRIBE"
	case TypePublish:
		return "PUBLISH"
	case TypeCredit:
		return "CREDIT"
	}
	return fmt.Sprintf("TYPE(%d)", uint8(t))
}

// Flags 位定义。Stream/Channel 由请求发起方声明，服务端据此校验注册的
// handler 类型；Compressed/Encrypted 描述本帧 payload 是否经过对应变换。
const (
	FlagCompressed uint8 = 1 << 0
	FlagEncrypted  uint8 = 1 << 1
	FlagHasMeta    uint8 = 1 << 2
	FlagStream     uint8 = 1 << 3
	FlagChannel    uint8 = 1 << 4
)

// Header 是帧的定长头部。
type Header struct {
	Version  uint8
	Flags    uint8
	Type     FrameType
	StreamID uint32
}

// Frame 是内存中的帧。Metadata 恒为明文 JSON（可为 nil）；
// Payload 在协议栈边界内是明文 JSON，读写线上字节时才做压缩/加密变换。
type Frame struct {
	Header
	Metadata []byte
	Payload  []byte
}

func (f *Frame) String() string {
	return fmt.Sprintf("<%s sid=%d flags=%02x len=%d>", f.Type, f.StreamID, f.Flags, len(f.Payload))
}

// appendTo 把帧编码后追加到 dst（append 风格，便于复用缓冲）。
// len(Metadata)>0 时自动置位 FlagHasMeta；flag 已置位时即使元数据为空
// 也写 metaLen 段（长度 0）——否则重编码会产生「声称有 meta 却不带
// metaLen」的自相矛盾帧，解析-编码不再是互逆（fuzz 实锤）。
func (f *Frame) appendTo(dst []byte) ([]byte, error) {
	if len(f.Metadata) > MaxMetadataSize {
		return dst, fmt.Errorf("%w: metadata %d > %d", ErrMalformed, len(f.Metadata), MaxMetadataSize)
	}
	if len(f.Payload) > MaxPayloadSize {
		return dst, fmt.Errorf("%w: payload %d > %d", ErrMalformed, len(f.Payload), MaxPayloadSize)
	}
	flags := f.Flags
	if len(f.Metadata) > 0 {
		flags |= FlagHasMeta
	}
	dst = binary.BigEndian.AppendUint16(dst, magic0<<8|magic1)
	dst = append(dst, f.Version, flags, byte(f.Type), 0)
	dst = binary.BigEndian.AppendUint32(dst, f.StreamID)
	// 上方已校验 len ≤ MaxPayloadSize（uint32 远不可及），min 把边界
	// 显式化，转换不会溢出。
	dst = binary.BigEndian.AppendUint32(dst, uint32(min(len(f.Payload), MaxPayloadSize)))
	if flags&FlagHasMeta != 0 {
		// 上方已校验 len ≤ MaxMetadataSize（uint16 可表达上界），min
		// 把边界显式化，转换不会截断。
		dst = binary.BigEndian.AppendUint16(dst, uint16(min(len(f.Metadata), MaxMetadataSize)))
		dst = append(dst, f.Metadata...)
	}
	dst = append(dst, f.Payload...)
	return dst, nil
}

// ErrMalformed 表示帧结构损坏（坏魔数、长度越界、截断等）。
var ErrMalformed = errors.New("jsonstream: malformed frame")

// ErrClosed 表示连接已关闭。
var ErrClosed = errors.New("jsonstream: connection closed")

// ReadFrame 从 r 流式读取一帧。读入的 Payload 仍是变换后的原始字节，
// 由调用方（transport）负责解压解密。
func ReadFrame(r io.Reader) (*Frame, error) {
	var head [headerSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, fmt.Errorf("%w: header: %w", ErrMalformed, err)
	}
	if head[0] != magic0 || head[1] != magic1 {
		return nil, fmt.Errorf("%w: bad magic %02x%02x", ErrMalformed, head[0], head[1])
	}
	f := &Frame{
		Header: Header{
			Version:  head[2],
			Flags:    head[3],
			Type:     FrameType(head[4]),
			StreamID: binary.BigEndian.Uint32(head[6:10]),
		},
	}
	if f.Version != ProtocolVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrMalformed, f.Version)
	}
	if f.Flags&^(FlagCompressed|FlagEncrypted|FlagHasMeta|FlagStream|FlagChannel) != 0 {
		return nil, fmt.Errorf("%w: reserved flag bits set: %02x", ErrMalformed, f.Flags)
	}
	payloadLen := binary.BigEndian.Uint32(head[10:14])
	if payloadLen > MaxPayloadSize {
		return nil, fmt.Errorf("%w: payload length %d exceeds %d", ErrMalformed, payloadLen, MaxPayloadSize)
	}
	if f.Flags&FlagHasMeta != 0 {
		var ml [metaLenSize]byte
		if _, err := io.ReadFull(r, ml[:]); err != nil {
			return nil, fmt.Errorf("%w: meta length: %w", ErrMalformed, err)
		}
		n := int(binary.BigEndian.Uint16(ml[:]))
		// n ≤ 65535 = MaxMetadataSize（uint16 上界保证，§2）；编码侧
		// appendTo 对称校验。
		f.Metadata = make([]byte, n)
		if _, err := io.ReadFull(r, f.Metadata); err != nil {
			return nil, fmt.Errorf("%w: metadata: %w", ErrMalformed, err)
		}
	}
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, fmt.Errorf("%w: payload: %w", ErrMalformed, err)
		}
	}
	return f, nil
}

// ---- Metadata ----

type metaJSON struct {
	Route string `json:"route,omitempty"`
	Topic string `json:"topic,omitempty"`
}

func encodeMeta(route, topic string) []byte {
	if route == "" && topic == "" {
		return nil
	}
	b, _ := json.Marshal(metaJSON{Route: route, Topic: topic})
	return b
}

func decodeMeta(b []byte) metaJSON {
	var m metaJSON
	if len(b) > 0 {
		_ = json.Unmarshal(b, &m)
	}
	return m
}

// creditPayload 是 CREDIT 帧的 payload：{"n": 补充条数}。
type creditPayload struct {
	N int `json:"n"`
}

func encodeCredit(n int) []byte {
	b, _ := json.Marshal(creditPayload{N: n})
	return b
}
