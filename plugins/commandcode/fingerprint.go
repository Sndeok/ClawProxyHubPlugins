package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// 设备档案：指纹 / config.environment / config.workingDir / x-project-slug /
// lifecycle 的 os 共用同一份，避免"指纹说 win32、环境说 linux"这种自相矛盾。
//
// 信号值全部由 API Key 确定性派生（同一个 Key 永远报同一台设备）：
// 重启、内存回收、多实例、停用数周后恢复，上游都应看到同一台设备——
// 换指纹本身就是可疑信号。
type deviceProfile struct {
	Platform    string // win32
	Arch        string // x64
	OSRelease   string
	ProjectDir  string
	CPUModel    string
	CPUCores    int
	MemGiB      int
	Timezone    string
	IsContainer bool
	MACs        []string
	Hostname    string
	OSUser      string
	GitEmail    string
	MachineID   string // Windows MachineGuid 形状 8-4-4-4-12
	Thumbmark   string // 设备指纹摘要
}

var (
	fpCPUs = []struct {
		Model string
		Cores int
	}{
		{"AMD Ryzen 7 5800X 8-Core Processor", 16},
		{"Intel(R) Core(TM) i7-12700K", 20},
		{"AMD Ryzen 9 5900X 12-Core Processor", 24},
		{"Intel(R) Core(TM) i5-13600K", 20},
	}
	fpMems     = []int{16, 32, 64}
	fpZones    = []string{"Asia/Shanghai", "Asia/Singapore", "America/Los_Angeles", "Europe/Berlin"}
	fpMACCount = []int{2, 3, 4}
	fpUsers    = []string{"dev", "developer", "builder", "coder"}
	fpDomains  = []string{"gmail.com", "outlook.com", "proton.me"}
)

// deriveProfile 由 API Key（+ 可选本机盐）派生设备档案。
func deriveProfile(apiKey, salt string) deviceProfile {
	seed := apiKey + "\x00" + salt
	pick := func(field string, n int) int {
		if n <= 1 {
			return 0
		}
		sum := hmacSum(seed, field)
		return int(sum[0]) % n
	}
	hexN := func(field string, n int) string {
		sum := hmacSum(seed, field)
		return hex.EncodeToString(sum)[:n]
	}
	cpu := fpCPUs[pick("cpu", len(fpCPUs))]
	macCount := fpMACCount[pick("macCount", len(fpMACCount))]
	macs := make([]string, 0, macCount)
	for i := 0; i < macCount; i++ {
		h := hmacSum(seed, fmt.Sprintf("mac%d", i))
		macs = append(macs, fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
			h[0], h[1], h[2], h[3], h[4], h[5]))
	}
	osUser := fpUsers[pick("osUser", len(fpUsers))]
	mid := hexN("machineId", 32)
	machineID := fmt.Sprintf("%s-%s-%s-%s-%s", mid[0:8], mid[8:12], mid[12:16], mid[16:20], mid[20:32])
	prof := deviceProfile{
		Platform:   "win32",
		Arch:       "x64",
		OSRelease:  "10.0.22631",
		ProjectDir: `C:\Users\` + osUser + `\projects\app`,
		CPUModel:   cpu.Model,
		CPUCores:   cpu.Cores,
		MemGiB:     fpMems[pick("mem", len(fpMems))],
		Timezone:   fpZones[pick("timezone", len(fpZones))],
		MACs:       macs,
		Hostname:   "DESKTOP-" + strings.ToUpper(hexN("hostname", 6)),
		OSUser:     osUser,
		GitEmail:   osUser + "." + hexN("gitEmail", 6) + "@" + fpDomains[pick("mailDomain", len(fpDomains))],
		MachineID:  machineID,
	}
	// thumbmark：与官方客户端同构的「主盐 + machine + 机器码/MAC」摘要
	th := sha256.Sum256([]byte("machine\x00" + machineID + "|" + strings.Join(macs, ",")))
	prof.Thumbmark = hex.EncodeToString(th[:])
	return prof
}

func hmacSum(seed, field string) []byte {
	m := hmac.New(sha256.New, []byte("commandcode-fingerprint"))
	m.Write([]byte(seed + "\x00" + field))
	return m.Sum(nil)
}

// payload 设备指纹上报体（/alpha/fingerprint/record）。
func (d deviceProfile) payload() map[string]interface{} {
	macHashes := make([]string, 0, len(d.MACs))
	for _, mac := range d.MACs {
		macHashes = append(macHashes, sha256Hex(mac))
	}
	return map[string]interface{}{
		"thumbmark": d.Thumbmark,
		"components": map[string]interface{}{
			"machineIdHash":    sha256Hex(d.MachineID),
			"macHashes":        macHashes,
			"osUserHash":       sha256Hex(d.OSUser),
			"hostnameHash":     sha256Hex(d.Hostname),
			"gitEmailHash":     sha256Hex(d.GitEmail),
			"platform":         d.Platform,
			"arch":             d.Arch,
			"osRelease":        d.OSRelease,
			"cpuModel":         d.CPUModel,
			"cpuCount":         d.CPUCores,
			"memGiB":           d.MemGiB,
			"isContainer":      d.IsContainer,
			"timezone":         d.Timezone,
			"runtime":          "cli",
			"collectorVersion": 1,
		},
	}
}

// slugifyProjectPath CLI 的 x-project-slug：工作目录 slug 化（与 config.workingDir 同源）。
func (d deviceProfile) slugifyProjectPath() string {
	s := strings.ToLower(d.ProjectDir)
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "root"
	}
	return out
}

// lifecycleBody lifecycle-events 上报体（cli_session_exists）。
func (d deviceProfile) lifecycleBody(cliVersion string) map[string]interface{} {
	return map[string]interface{}{
		"eventType": "cli_session_exists",
		"metadata": map[string]interface{}{
			"sessionId":  "sess_" + sha256Hex(d.Thumbmark)[:16],
			"cliVersion": cliVersion,
			"mode":       "interactive",
			"os":         d.Platform + "-" + d.Arch,
		},
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// threadIDFor 由会话种派生稳定 UUID（官方要求 threadId 是合法 UUID，否则整键省略）。
func threadIDFor(seed string) string {
	sum := sha256.Sum256([]byte("thread\x00" + seed))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// initSchedule 指纹/lifecycle 的下次刷新时间：8h + 最多 2h 抖动。
func initSchedule(randByte byte) int64 {
	jitter := int64(randByte) * 47 * 1000 // 0–约 2h
	return time.Now().UnixMilli() + 8*3600*1000 + jitter
}
