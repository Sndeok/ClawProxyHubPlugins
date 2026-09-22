// Package qodersign —— Qoder 系（Qoder / QoderWork）上游签名与编码。
//
// 三部分：
//  1. QoderEncoding：上游要求的 body 编码（自定义字母表 base64 + 三段重排）
//  2. COSY 签名：RSA 包裹 AES 会话密钥 + AES-CBC 身份 + MD5 请求签名
//  3. 设备指纹：由账号种子确定性派生 machineId / machineToken / machineType
package qodersign

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// 上游自定义字母表（与桌面客户端一致；勿改）
const (
	customAlphabet = "_doRTgHZBKcGVjlvpC,@aFSx#DPuNJme&i*MzLOEn)sUrthbf%Y^w.(kIQyXqWA!"
	stdAlphabet    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	customPad      = '$'
)

var (
	stdToCustom [128]byte
	customToStd [128]int
)

func init() {
	for i := range stdToCustom {
		stdToCustom[i] = 0xFF
	}
	for i := range customToStd {
		customToStd[i] = -1
	}
	for i := 0; i < len(stdAlphabet); i++ {
		stdToCustom[stdAlphabet[i]] = customAlphabet[i]
		customToStd[customAlphabet[i]] = i
	}
	customToStd[customPad] = 64 // 视作 '='
}

// Encode 编码：标准 base64 → 三段重排（尾/中/头）→ 字符映射（'=' → '$'）。
func Encode(plain []byte) (string, error) {
	std := base64.StdEncoding.EncodeToString(plain)
	n := len(std)
	if n == 0 {
		return "", nil
	}
	a := n / 3
	rearranged := std[n-a:] + std[a:n-a] + std[:a]
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		c := rearranged[i]
		if c >= 128 {
			return "", fmt.Errorf("qodersign: char out of range: %d", c)
		}
		if c == '=' {
			sb.WriteByte(customPad)
			continue
		}
		mapped := stdToCustom[c]
		if mapped == 0xFF {
			return "", fmt.Errorf("qodersign: char out of std alphabet: %q", c)
		}
		sb.WriteByte(mapped)
	}
	return sb.String(), nil
}

// Decode Encode 的逆运算（测试/排查用）。
func Decode(encoded string) ([]byte, error) {
	n := len(encoded)
	if n == 0 {
		return nil, nil
	}
	mapped := make([]byte, n)
	for i := 0; i < n; i++ {
		c := encoded[i]
		if c >= 128 || customToStd[c] < 0 {
			return nil, fmt.Errorf("qodersign: char not in custom alphabet: %d", c)
		}
		v := customToStd[c]
		if v == 64 {
			mapped[i] = '='
			continue
		}
		mapped[i] = stdAlphabet[v]
	}
	a := n / 3
	std := string(mapped[n-a:]) + string(mapped[a:n-a]) + string(mapped[:a])
	return base64.StdEncoding.DecodeString(std)
}
