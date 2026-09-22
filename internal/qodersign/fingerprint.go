package qodersign

import (
	"crypto/sha512"
	"encoding/base64"
)

// 设备指纹：由账号种子（uid 优先，无 uid 时用凭证）确定性派生。
//
// 同一账号跨进程 / 跨重启 / 多实例始终得到同一组机器码——指纹漂移本身就是上游的风控信号。
// 派生算法与上游 CLI 及两个参考实现（qoder2api / qoderwork2api）逐字节一致：
//
//	machineId    md5("machine:" + seed)
//	machineType  md5("machinetype:" + seed) 截 18 位
//	machineToken base64url(sha512("machinetoken:" + seed)) 截 43 位
//
// 本机盐（可选）由插件设置提供：加了盐后每个部署拥有独立指纹空间（换盐 = 换设备）。
type Fingerprint struct {
	MachineID    string
	MachineType  string
	MachineToken string
}

// DeriveFingerprint salt 为空时与参考实现完全一致；非空时混入本机盐。
func DeriveFingerprint(seed, salt string) Fingerprint {
	s := seeded(seed, salt)
	mid := md5Hex("machine:" + s)
	mtype := md5Hex("machinetype:" + s)[:18]
	sum := sha512.Sum512([]byte("machinetoken:" + s))
	mtoken := base64.RawURLEncoding.EncodeToString(sum[:])[:43]
	return Fingerprint{MachineID: mid, MachineType: mtype, MachineToken: mtoken}
}

// seeded 混入本机盐（空盐 = 原样，保持与参考实现兼容）。
func seeded(seed, salt string) string {
	if salt == "" {
		return seed
	}
	return seed + "|salt:" + salt
}

// SeedFor 选择指纹种子：优先账号 uid；uid 未知时退回凭证（稳定性优先）。
func SeedFor(uid, credential string) string {
	if uid != "" {
		return uid
	}
	return "cred:" + credential
}

// SessionID 派生稳定会话标识（与 hub 的 derive_id(uid, "session") 同构）。
func SessionID(seed, salt string) string {
	return md5Hex("session:" + seeded(seed, salt))
}
