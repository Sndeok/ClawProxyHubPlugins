// commandcode 插件 —— Command Code 反代。
//
// 上游：https://api.commandcode.ai（自家信封 + AI-SDK 事件流），
// 四个端点：/alpha/generate（对话）、/alpha/fingerprint/record（设备指纹）、
// /alpha/lifecycle-events（生命周期）、/provider/v1/models（公开模型目录）。
//
// 客户端伪装（逐条对齐官方 CLI，抄错任何一条都会被上游识破）：
//   - 设备档案单一真源：指纹 / config.environment / config.workingDir /
//     x-project-slug / lifecycle 的 os 全部来自同一份档案，不自相矛盾
//   - 信号值由 API Key 确定性派生：同一 Key 永远报同一台设备，重启/多实例都不漂移
//   - User-Agent 固定 "cli" + x-command-code-version 报实际实现的协议版本
//   - 缺 system 时发空格占位，绕过上游注入的约 7.5K token 默认提示词
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Sndeok/ClawProxyHub-Next/sdk"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// version 打包时经 -ldflags "-X main.version=..." 注入。
var version = "dev"

func main() { sdk.Serve(&plugin{}) }

type plugin struct {
	pb.UnimplementedClawPluginServer
	host *sdk.Host

	mu sync.Mutex
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// ccCred 账号凭据：只需一个 user_ 开头的 API Key；
// 设备档案由 Key 确定性派生（不落库，换 salt 即换设备）。
type ccCred struct {
	APIKey     string `json:"api_key"`
	NextInitAt int64  `json:"next_init_at,omitempty"` // 下次指纹/lifecycle 刷新（毫秒）
	proxyURL   string
}

func (c *ccCred) profile(salt string) deviceProfile { return deriveProfile(c.APIKey, salt) }

func credFrom(blob *pb.CredentialBlob) (*ccCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c ccCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, fmt.Errorf("凭据缺少 API Key")
	}
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func marshalCred(c *ccCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

// ---------- 握手 ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "commandcode", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "Command Code", "en": "Command Code"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		SettingsSchema: `{
			"type": "object",
			"properties": {
				"protocol_version": {
					"type": "string",
					"title": "上报的协议版本",
					"description": "x-command-code-version。参考实现刻意报「实际实现的协议版本」而非 npm 最新版；上游更新只告警不改号。留空 = 1.53.1",
					"default": "1.53.1"
				},
				"cli_mode": {
					"type": "string",
					"title": "信封 mode",
					"description": "官方枚举：agent / learning / custom-agent / title-gen / tool-desc / compact / vision",
					"enum": ["agent", "learning", "custom-agent", "title-gen", "tool-desc", "compact", "vision"],
					"default": "agent"
				},
				"zdr": {
					"type": "string",
					"title": "ZDR-only 路由",
					"description": "true = 请求附加 x-cmd-zdr: 1（要求上游走零数据留存路由）",
					"enum": ["false", "true"],
					"default": "false"
				},
				"fingerprint_salt": {
					"type": "string",
					"title": "指纹本机盐",
					"description": "混入设备指纹派生。留空 = 与参考实现同构；成批换设备身份就改这里（同一个 Key 仍永远报同一台设备）",
					"default": ""
				},
				"project_dir": {
					"type": "string",
					"title": "伪装的项目目录",
					"description": "x-project-slug 与 config.workingDir 同源；留空 = 内置 C:\\Users\\<用户>\\projects\\app",
					"default": ""
				}
			}
		}`,
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "api_key", Label: map[string]string{"zh": "API Key（user_ 开头）", "en": "API Key (user_)"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{{
					Name: "content", Label: map[string]string{"zh": "Command Code API Key", "en": "Command Code API Key"},
					Type: "password", Required: true, Placeholder: "user_...",
				}},
			},
		},
	}}, nil
}

// ---------- 登录 ----------

// Login 粘贴 user_ Key：用「指纹上报」端点校验（该端点需要鉴权，但不产生计费请求）。
func (p *plugin) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	if req.MethodId != "api_key" && req.MethodId != "" {
		return nil, status.Error(codes.InvalidArgument, "未知授权方式: "+req.MethodId)
	}
	key := strings.TrimSpace(req.Form["content"])
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "请填写 API Key")
	}
	if !strings.HasPrefix(key, "user_") {
		return nil, status.Error(codes.InvalidArgument, "Key 必须以 user_ 开头（官方只认这个前缀）")
	}
	cred := &ccCred{APIKey: key}
	if err := p.ensureInitialized(ctx, cred); err != nil {
		return nil, status.Error(codes.Unauthenticated, "Key 校验失败: "+err.Error())
	}
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

// ---------- 刷新 / 资料 ----------

// Refresh 重新上报指纹（超期时）并刷新资料；Key 本身不过期。
func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	_ = p.ensureInitialized(ctx, cred)
	return &pb.RefreshResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

func (p *plugin) GetProfile(ctx context.Context, blob *pb.CredentialBlob) (*pb.AccountProfile, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return p.profileFor(ctx, cred), nil
}

// profileFor Command Code 没有余额接口，展示设备档案 + 模型目录。
func (p *plugin) profileFor(ctx context.Context, cred *ccCred) *pb.AccountProfile {
	prof := &pb.AccountProfile{
		DisplayName: "Command Code " + keyTail(cred.APIKey),
		Healthy:     true,
		Quota:       map[string]string{"machine_id": cred.profile(p.fingerprintSalt()).MachineID},
	}
	models, err := fetchModels(ctx, p.httpClient(cred))
	if err != nil {
		prof.Healthy = false
		prof.Sections = append(prof.Sections, sectionNote("models_error", "模型目录不可达", err.Error()))
		return prof
	}
	prof.Quota["models"] = fmt.Sprintf("%d", len(models))
	dev := cred.profile(p.fingerprintSalt())
	prof.Sections = append(prof.Sections, &pb.ProfileSection{
		Id:    "device",
		Title: map[string]string{"zh": "伪装设备档案", "en": "Spoofed device profile"},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "平台", "en": "Platform"}, Value: dev.Platform + "-" + dev.Arch},
			{Label: map[string]string{"zh": "机器码", "en": "Machine ID"}, Value: dev.MachineID},
			{Label: map[string]string{"zh": "主机名", "en": "Hostname"}, Value: dev.Hostname},
			{Label: map[string]string{"zh": "项目目录", "en": "Project dir"}, Value: dev.ProjectDir},
			{Label: map[string]string{"zh": "CPU / 内存", "en": "CPU / RAM"}, Value: fmt.Sprintf("%s / %dGB", dev.CPUModel, dev.MemGiB)},
			{Label: map[string]string{"zh": "时区", "en": "Timezone"}, Value: dev.Timezone},
		},
	})
	prof.Sections = append(prof.Sections, modelSection(models))
	return prof
}

// keyTail 只显示 Key 末尾用于区分账号（不泄漏完整 Key）。
func keyTail(key string) string {
	if len(key) <= 6 {
		return "···"
	}
	return "···" + key[len(key)-6:]
}

// ---------- 工具 ----------

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
