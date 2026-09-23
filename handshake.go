package jsonstream

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// connectJSON 是 CONNECT 帧的 payload：客户端能力声明与恢复凭证。
type connectJSON struct {
	Version     int    `json:"version"`
	Compress    bool   `json:"compress"`
	Encrypt     bool   `json:"encrypt"`
	HeartbeatMS int    `json:"heartbeat_ms"`
	Credit      int    `json:"credit,omitempty"`
	SessionID   string `json:"session_id,omitempty"` // 断线重连时携带
	Auth        string `json:"auth,omitempty"`
}

// connackJSON 是 CONNACK 帧的 payload：服务端是权威，下发实际生效的参数。
type connackJSON struct {
	SessionID   string `json:"session_id"`
	Resumed     bool   `json:"resumed"`
	Compress    bool   `json:"compress"`
	Encrypt     bool   `json:"encrypt"`
	HeartbeatMS int    `json:"heartbeat_ms"`
	Credit      int    `json:"credit"`
	RetentionMS int    `json:"retention_ms"`
}

// 握手帧（CONNECT/CONNACK/握手期 ERROR）恒为明文：协商完成之前
// 双方无从得知对方是否启用了压缩/加密。
func connectFrame(cj *connectJSON) (*Frame, error) {
	data, err := jsonMarshal(cj)
	if err != nil {
		return nil, err
	}
	return &Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnect}, Payload: data}, nil
}

func connackFrame(aj *connackJSON) (*Frame, error) {
	data, err := jsonMarshal(aj)
	if err != nil {
		return nil, err
	}
	return &Frame{Header: Header{Version: ProtocolVersion, Type: TypeConnAck}, Payload: data}, nil
}

// effectiveConnack 计算生效参数：以服务端配置为上限、客户端声明为请求
// （protocol.md §6.1）。credit 双方都 > 0 才启用，取较小值。
func effectiveConnack(srvCfg Config, cj *connectJSON) connackJSON {
	aj := connackJSON{
		Resumed:     false,
		Compress:    srvCfg.Compress && cj.Compress,
		Encrypt:     srvCfg.Encrypt && cj.Encrypt,
		HeartbeatMS: int(srvCfg.Heartbeat / time.Millisecond),
		RetentionMS: int(srvCfg.Retention / time.Millisecond),
	}
	if srvCfg.Credit > 0 && cj.Credit > 0 && cj.Credit < srvCfg.Credit {
		aj.Credit = cj.Credit
	} else if srvCfg.Credit > 0 {
		aj.Credit = srvCfg.Credit
	}
	return aj
}

func newSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
