// opencode 插件 —— OpenCode Zen / Zen Go 反代。
//
// 上游：https://opencode.ai/zen（Zen 付费 Key）/ https://opencode.ai/zen/go（Go 订阅），
// 外加官方「匿名免费通道」：Authorization: Bearer public，仅服务免费模型。
//
// 客户端伪装要点（全部实测过，缺一项就被拒）：
//   - 必须带 x-opencode-client: cli + session/request/project 四个关联头
//   - 匿名通道的 session 必须是官方格式 ses_<12hex><14base62>，其它形状直接 403 FreeTierError
//   - 匿名通道只服务「agent 形状」的流式请求：必须 stream:true 且带
//     bash/edit/glob/grep/read 这 5 个核心工具名，否则 403
//   - 每个模型的原生协议不同（chat completions / messages / responses），按官方文档表选路
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

	mu      sync.Mutex
	modelMu sync.Mutex
	models  []zenModel // 目录缓存（含元数据与协议）
	at      int64      // 缓存时间
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// ---------- 凭据 ----------

const (
	tierZen  = "zen"
	tierGo   = "go"
	tierAnon = "anon"
	anonKey  = "public"
)

// openCred 账号凭据。
type openCred struct {
	Tier     string `json:"tier"`              // zen / go / anon
	APIKey   string `json:"api_key,omitempty"` // sk- 开头的 Zen Key（anon 固定 public）
	UID      string `json:"uid,omitempty"`     // 展示名 / 会话种子
	proxyURL string
}

func credFrom(blob *pb.CredentialBlob) (*openCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c openCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	c.Tier = normalizeTier(c.Tier)
	if c.Tier == tierAnon {
		c.APIKey = anonKey
	}
	if c.APIKey == "" {
		return nil, fmt.Errorf("凭据缺少 API Key")
	}
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func marshalCred(c *openCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

func normalizeTier(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case tierGo:
		return tierGo
	case tierAnon:
		return tierAnon
	default:
		return tierZen
	}
}

func tierLabel(t string) string {
	switch t {
	case tierGo:
		return "Zen Go（订阅）"
	case tierAnon:
		return "匿名免费通道"
	default:
		return "Zen（付费 Key）"
	}
}

// ---------- 握手 ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "opencode", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "OpenCode Zen", "en": "OpenCode Zen"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		SettingsSchema: `{
			"type": "object",
			"properties": {
				"cli_version": {
					"type": "string",
					"title": "OpenCode 版本（伪装）",
					"description": "出站 User-Agent 的 opencode/<版本>；留空 = 1.18.31",
					"default": "1.18.31"
				},
				"reasoning_effort": {
					"type": "string",
					"title": "推理强度（默认值）",
					"description": "客户端未指定时使用的 reasoning_effort；留空 = 不发送",
					"enum": ["", "low", "medium", "high", "max"],
					"default": ""
				},
				"anon_inject_tools": {
					"type": "string",
					"title": "匿名通道补齐核心工具",
					"description": "匿名免费通道要求请求带 bash/edit/glob/grep/read 五个工具名，缺了会 403；true = 自动补齐（推荐）",
					"enum": ["true", "false"],
					"default": "true"
				}
			}
		}`,
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "zen_key", Label: map[string]string{"zh": "Zen API Key（付费）", "en": "Zen API Key"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{{
					Name: "content", Label: map[string]string{"zh": "Zen API Key（sk- 开头）", "en": "Zen API Key"},
					Type: "password", Required: true, Placeholder: "sk-...",
				}},
			},
			{
				Id: "go_key", Label: map[string]string{"zh": "Zen Go Key（订阅）", "en": "Zen Go Key"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{{
					Name: "content", Label: map[string]string{"zh": "Go 订阅的 API Key", "en": "Zen Go API Key"},
					Type: "password", Required: true, Placeholder: "订阅页里的 Key",
				}},
			},
			{
				Id: "anon", Label: map[string]string{"zh": "匿名免费通道（无需账号）", "en": "Anonymous free lane"},
				Capabilities: []string{"profile"},
			},
		},
	}}, nil
}

// ---------- 登录 ----------

func (p *plugin) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	switch req.MethodId {
	case "anon", "":
		cred := &openCred{Tier: tierAnon, APIKey: anonKey, UID: "anonymous"}
		return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
	case "zen_key", "go_key":
		key := strings.TrimSpace(req.Form["content"])
		if key == "" {
			return nil, status.Error(codes.InvalidArgument, "请填写 API Key")
		}
		tier := tierZen
		if req.MethodId == "go_key" {
			tier = tierGo
		}
		cred := &openCred{Tier: tier, APIKey: key}
		if _, err := p.fetchModelIDs(ctx, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "Key 校验失败: "+err.Error())
		}
		return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "未知授权方式: "+req.MethodId)
	}
}

// ---------- 刷新 / 资料 ----------

// Refresh OpenCode 的 Key 不过期，这里只做一次轻量可用性检查并刷新资料。
func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := p.fetchModelIDs(ctx, cred); err != nil {
		return nil, status.Error(codes.Unavailable, "上游不可达: "+err.Error())
	}
	return &pb.RefreshResult{Profile: p.profileFor(ctx, cred)}, nil
}

func (p *plugin) GetProfile(ctx context.Context, blob *pb.CredentialBlob) (*pb.AccountProfile, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return p.profileFor(ctx, cred), nil
}

// profileFor OpenCode 没有余额接口，展示通道类型 + 可用模型统计。
func (p *plugin) profileFor(ctx context.Context, cred *openCred) *pb.AccountProfile {
	prof := &pb.AccountProfile{
		DisplayName: orDefault(cred.UID, tierLabel(cred.Tier)),
		Healthy:     true,
		Quota:       map[string]string{"tier": cred.Tier},
	}
	models, err := p.listModels(ctx, cred)
	if err != nil {
		prof.Healthy = false
		prof.Sections = append(prof.Sections, sectionNote("models_error", "模型目录不可达", err.Error()))
		return prof
	}
	prof.Quota["models"] = fmt.Sprintf("%d", len(models))
	free := 0
	for _, m := range models {
		if m.Free {
			free++
		}
	}
	prof.Quota["free_models"] = fmt.Sprintf("%d", free)
	prof.Sections = append(prof.Sections, modelSection(cred, models))
	return prof
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
