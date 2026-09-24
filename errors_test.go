package jsonstream

import "testing"

// 错误载体的对外契约：Error() 文案、IsProtocol 判定、错误码名字映射
// （errors.go，§7.10）。线上格式被日志与断言依赖，改动应显式可见。
func TestErrorStringAndClassification(t *testing.T) {
	e := &Error{Code: CodeInvalid, Message: "boom"}
	if got, want := e.Error(), "jsonstream: INVALID(3): boom"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if e.IsProtocol() {
		t.Fatal("INVALID must not be classified as a protocol error")
	}
	pe := &Error{Code: CodeProtocol, Message: "bad frame"}
	if !pe.IsProtocol() {
		t.Fatal("PROTOCOL must be classified as a protocol error")
	}
	if CodeSessionExpired.String() != "SESSION_EXPIRED" || CodeBusy.String() != "BUSY" {
		t.Fatal("ErrorCode.String name mapping broken")
	}
	// 全表：错误码 → 线上/日志名字的完整映射（§7.10）
	all := map[ErrorCode]string{
		CodeInternal:       "INTERNAL",
		CodeNotFound:       "NOT_FOUND",
		CodeInvalid:        "INVALID",
		CodeProtocol:       "PROTOCOL",
		CodeCancelled:      "CANCELLED",
		CodeTimeout:        "TIMEOUT",
		CodeAuthDenied:     "AUTH_DENIED",
		CodeSessionExpired: "SESSION_EXPIRED",
		CodeBusy:           "BUSY",
		CodeUnsupported:    "UNSUPPORTED",
	}
	for code, want := range all {
		if got := code.String(); got != want {
			t.Errorf("ErrorCode(%d).String() = %q, want %q", uint8(code), got, want)
		}
	}
	if got := ErrorCode(99).String(); got != "UNKNOWN" {
		t.Fatalf("unknown code should render UNKNOWN, got %q", got)
	}
}
