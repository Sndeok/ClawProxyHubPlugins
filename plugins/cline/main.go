// cline 插件 —— Cline（cline.bot）反代。
//
// 上游：https://api.cline.bot/api/v1（OpenAI 兼容 /chat/completions +
// 公开模型目录）；鉴权：WorkOS 设备授权 → Cline refreshToken → accessToken
// （出站 Authorization 形如 Bearer workos:<accessToken>）。
//
// 三个上游硬约束（照抄参考实现，抄错会直接 403/500/空响应）：
//  1. 必须带 Cline 客户端指纹头，否则 403「only available via Cline product surfaces」
//  2. 免费通道请求体不能带 max_tokens，否则 500 empty response content
//  3. 免费通道并发 > 1 会返回空响应 → 插件侧串行排队（可配置）
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
	logins  map[string]*loginSession // WorkOS 设备授权会话
	gate    chan struct{}            // 免费通道串行闸门（长度 1）
	lastRun int64                    // 上次上游请求时间（毫秒），用于最小间隔
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// clineCred 账号凭据（核心只当 opaque blob 存储）。
type clineCred struct {
	RefreshToken string `json:"refresh_token"`          // Cline refreshToken（长期）
	AccessToken  string `json:"access_token,omitempty"` // WorkOS accessToken（短期）
	ExpiresAt    int64  `json:"expires_at,omitempty"`   // accessToken 过期（毫秒）
	Email        string `json:"email,omitempty"`        // 展示用
	UserID       string `json:"user_id,omitempty"`      // 展示用

	proxyURL string
}

func credFrom(blob *pb.CredentialBlob) (*clineCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c clineCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	if c.RefreshToken == "" {
		return nil, fmt.Errorf("凭据缺少 refreshToken")
	}
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func marshalCred(c *clineCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

// ---------- 握手 ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "cline", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "Cline", "en": "Cline"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		SettingsSchema: `{
			"type": "object",
			"properties": {
				"reasoning_effort": {
					"type": "string",
					"title": "推理强度",
					"description": "上游 reasoning_effort（low / medium / high / max）；留空 = high",
					"enum": ["low", "medium", "high", "max"],
					"default": "high"
				},
				"serialize_scope": {
					"type": "string",
					"title": "串行保护范围",
					"description": "上游免费通道并发 >1 会返回空响应。free_only = 只串行免费模型（推荐）；all = 全部串行；none = 关闭",
					"enum": ["free_only", "all", "none"],
					"default": "free_only"
				},
				"min_gap_ms": {
					"type": "string",
					"title": "请求最小间隔（毫秒）",
					"description": "串行保护命中时的两次上游请求间隔；留空 = 800",
					"default": "800"
				},
				"client_version": {
					"type": "string",
					"title": "客户端版本（伪装）",
					"description": "X-CLIENT-VERSION / X-PLATFORM-VERSION / User-Agent 里的版本号；留空 = 3.0.47",
					"default": "3.0.47"
				},
				"extra_headers": {
					"type": "string",
					"title": "额外请求头",
					"description": "形如 Key: Value; Key2: Value2，用于上游指纹变化时临时覆盖",
					"default": ""
				}
			}
		}`,
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "oauth", Label: map[string]string{"zh": "浏览器授权（Cline 账号）", "en": "Browser auth (Cline)"},
				Capabilities: []string{"refreshable", "auto_relogin", "profile"},
				// auto：插件自己在服务端轮询 WorkOS，域名部署也能自动完成，无需粘贴回调
				Callback: "auto",
			},
			{
				Id: "token", Label: map[string]string{"zh": "粘贴 refreshToken", "en": "Paste refreshToken"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{{
					Name: "content", Label: map[string]string{"zh": "Cline refreshToken", "en": "Cline refreshToken"},
					Type: "textarea", Required: true,
					Placeholder: "从 Cline 客户端 / cline_oauth.py 拿到的 refreshToken",
				}},
			},
		},
	}}, nil
}

// ---------- 登录 ----------

func (p *plugin) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	switch req.MethodId {
	case "oauth", "":
		return p.loginOAuth(ctx, req)
	case "token":
		return p.loginToken(ctx, req)
	default:
		return nil, status.Error(codes.InvalidArgument, "未知授权方式: "+req.MethodId)
	}
}

// loginToken 粘贴 refreshToken（也兼容 JSON 形态 {"refreshToken":"..."}）。
func (p *plugin) loginToken(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	raw := strings.TrimSpace(req.Form["content"])
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "请粘贴 refreshToken")
	}
	rt := parseRefreshToken(raw)
	if rt == "" {
		return nil, status.Error(codes.InvalidArgument, "未能从内容里识别出 refreshToken")
	}
	cred := &clineCred{RefreshToken: rt}
	client := p.httpClient(cred)
	// 立刻换一次 accessToken 校验凭据（失败即报错，不留坏账号）
	tok, err := refreshClineToken(ctx, client, cred)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "refreshToken 校验失败: "+err.Error())
	}
	_ = tok
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

// parseRefreshToken 兼容：纯 token、{"refreshToken":...}、"refreshToken=..."。
func parseRefreshToken(raw string) string {
	var m map[string]interface{}
	if json.Unmarshal([]byte(raw), &m) == nil {
		for _, k := range []string{"refreshToken", "refresh_token"} {
			if v, ok := m[k].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
		if d, ok := m["data"].(map[string]interface{}); ok {
			for _, k := range []string{"refreshToken", "refresh_token"} {
				if v, ok := d[k].(string); ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	if i := strings.Index(raw, "="); i > 0 && !strings.ContainsAny(raw[:i], " \n") {
		raw = raw[i+1:]
	}
	return strings.TrimSpace(strings.Trim(raw, `"`))
}

// ---------- 刷新 / 资料 ----------

func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	client := p.httpClient(cred)
	changed := false
	if accessTokenExpiring(cred) {
		if _, err := refreshClineToken(ctx, client, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "accessToken 刷新失败（需重新授权）: "+err.Error())
		}
		changed = true
	}
	res := &pb.RefreshResult{Profile: p.profileFor(ctx, cred)}
	if changed {
		res.Blob = marshalCred(cred)
	}
	return res, nil
}

func (p *plugin) GetProfile(ctx context.Context, blob *pb.CredentialBlob) (*pb.AccountProfile, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return p.profileFor(ctx, cred), nil
}

// profileFor 组装资料：Cline 没有余额接口，这里展示账号标识 + 当前免费模型清单。
func (p *plugin) profileFor(ctx context.Context, cred *clineCred) *pb.AccountProfile {
	prof := &pb.AccountProfile{
		DisplayName: orDefault(cred.Email, orDefault(cred.UserID, "Cline")),
		Healthy:     true,
		Quota:       map[string]string{},
	}
	free, pass, err := fetchRecommended(ctx, p.httpClient(cred))
	if err != nil {
		prof.Sections = append(prof.Sections, sectionNote("catalog_error", "模型目录不可达", err.Error()))
		return prof
	}
	prof.Quota["free_models"] = fmt.Sprintf("%d", len(free))
	prof.Sections = append(prof.Sections, modelSection(free, pass))
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
