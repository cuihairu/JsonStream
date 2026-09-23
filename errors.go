package jsonstream

import "fmt"

// ErrorCode 是错误帧 payload 中机器可读的错误分类，取值见 docs/protocol.md §7.10。
type ErrorCode uint8

const (
	CodeInternal       ErrorCode = 1  // INTERNAL: 处理器内部错误（含 handler panic）
	CodeNotFound       ErrorCode = 2  // NOT_FOUND: 路由不存在
	CodeInvalid        ErrorCode = 3  // INVALID: 载荷或参数非法
	CodeProtocol       ErrorCode = 4  // PROTOCOL: 协议违规（必须断开连接）
	CodeCancelled      ErrorCode = 5  // CANCELLED: 对端已取消
	CodeTimeout        ErrorCode = 6  // TIMEOUT: 处理超时
	CodeAuthDenied     ErrorCode = 7  // AUTH_DENIED: 鉴权失败 / 拒绝连接
	CodeSessionExpired ErrorCode = 8  // SESSION_EXPIRED: 会话过期，恢复失败
	CodeBusy           ErrorCode = 9  // BUSY: 过载拒绝（含保留队列溢出）
	CodeUnsupported    ErrorCode = 10 // UNSUPPORTED: 对端未启用的能力被错误使用
)

func (c ErrorCode) String() string {
	switch c {
	case CodeInternal:
		return "INTERNAL"
	case CodeNotFound:
		return "NOT_FOUND"
	case CodeInvalid:
		return "INVALID"
	case CodeProtocol:
		return "PROTOCOL"
	case CodeCancelled:
		return "CANCELLED"
	case CodeTimeout:
		return "TIMEOUT"
	case CodeAuthDenied:
		return "AUTH_DENIED"
	case CodeSessionExpired:
		return "SESSION_EXPIRED"
	case CodeBusy:
		return "BUSY"
	case CodeUnsupported:
		return "UNSUPPORTED"
	}
	return "UNKNOWN"
}

// Error 同时实现标准 error 与错误帧 payload 的编解码载体。
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("jsonstream: %s(%d): %s", e.Code, e.Code, e.Message)
}

// IsProtocol 报告该错误是否为协议级错误（对端随后必然断开连接）。
func (e *Error) IsProtocol() bool { return e.Code == CodeProtocol }

func errEncode(e *Error) []byte {
	b, _ := jsonMarshal(e)
	return b
}

func errDecode(b []byte) *Error {
	var e Error
	if err := jsonUnmarshal(b, &e); err != nil || e.Code == 0 {
		return &Error{Code: CodeInternal, Message: "undecodable error payload"}
	}
	return &e
}
