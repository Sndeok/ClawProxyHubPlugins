// qoder 插件 —— Qoder（qoder.sh / qoder.com.cn）反代。
//
// 上游：Qoder 桌面端 agent API（COSY 签名 + QoderEncoding 请求体，
// 嵌套 SSE 返回 OpenAI chunk）；业务接口走 openapi.{qoder.sh|qoder.com.cn}。
// 凭据：设备授权（PKCE）获取 dt-/drt-，或粘贴 PAT / dt-。
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
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// version 打包时经 -ldflags "-X main.version=..." 注入。
var version = "dev"

func main() { sdk.Serve(&plugin{}) }

type plugin struct {
	pb.UnimplementedClawPluginServer
	host *sdk.Host

	mu     sync.Mutex
	logins map[string]*loginSession
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// qoderCred 账号凭据（核心只当 opaque blob 存储）。
type qoderCred struct {
	Region   string `json:"region,omitempty"`    // global | cn
	AuthMode string `json:"auth_mode,omitempty"` // oauth | pat
	DT       string `json:"dt,omitempty"`        // 当前 Bearer（dt- 或 PAT）
	DRT      string `json:"drt,omitempty"`       // 刷新令牌（OAuth）
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty"`
	UserType string `json:"user_type,omitempty"`
	OrgID    string `json:"org_id,omitempty"`
	OrgName  string `json:"org_name,omitempty"`
	// COSY 身份里的两项（OAuth: dt/drt；PAT: jobToken 换来的那两个）
	SecToken  string `json:"sec_token,omitempty"`
	RefToken  string `json:"ref_token,omitempty"`
	ExpiresAt int64  `json:"expires_at,omitempty"`
	// COSY 设备指纹（首次登录派生并持久化，跨重启不变）
	MachineID    string `json:"machine_id,omitempty"`
	MachineType  string `json:"machine_type,omitempty"`
	MachineToken string `json:"machine_token,omitempty"`

	proxyURL string
}

func credFrom(blob *pb.CredentialBlob) (*qoderCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c qoderCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	if c.DT == "" {
		return nil, fmt.Errorf("凭据缺少访问令牌")
	}
	c.Region = normalizeRegion(c.Region)
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func marshalCred(c *qoderCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

func normalizeRegion(r string) string {
	if strings.EqualFold(strings.TrimSpace(r), regionCN) {
		return regionCN
	}
	return regionGlobal
}

// ---------- 握手 ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "qoder", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "Qoder", "en": "Qoder"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat", "tasks"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "oauth_cn", Label: map[string]string{"zh": "浏览器授权（国内站 qoder.com.cn）", "en": "Browser auth (CN)"},
				Capabilities: []string{"refreshable", "auto_relogin", "profile"},
				Callback:     "auto",
			},
			{
				Id: "oauth_global", Label: map[string]string{"zh": "浏览器授权（国际站 qoder.sh）", "en": "Browser auth (Global)"},
				Capabilities: []string{"refreshable", "auto_relogin", "profile"},
				Callback:     "auto",
			},
			{
				Id: "token", Label: map[string]string{"zh": "粘贴令牌 / PAT", "en": "Paste token or PAT"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{
					{
						Name: "content", Label: map[string]string{"zh": "dt- / PAT 令牌（或授权 JSON）", "en": "dt- / PAT token (or auth JSON)"},
						Type: "textarea", Required: true, Placeholder: `{"token":"dt-...","refresh_token":"drt-..."} 或直接粘贴 PAT`,
					},
					{
						Name: "region", Label: map[string]string{"zh": "区域（global / cn）", "en": "Region (global / cn)"},
						Type: "text", Placeholder: "global", Required: false,
					},
				},
			},
		},
	}}, nil
}

// ---------- 登录 ----------

func (p *plugin) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	switch req.MethodId {
	case "oauth_cn":
		return p.loginOAuth(ctx, req, regionCN)
	case "oauth_global", "":
		return p.loginOAuth(ctx, req, regionGlobal)
	case "token":
		return p.loginToken(ctx, req)
	default:
		return nil, status.Error(codes.InvalidArgument, "未知授权方式: "+req.MethodId)
	}
}

// loginToken 粘贴 dt- / PAT（也兼容参考实现的 auth JSON 形态）。
func (p *plugin) loginToken(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	raw := strings.TrimSpace(req.Form["content"])
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "请粘贴令牌或 PAT")
	}
	region := normalizeRegion(req.Form["region"])
	ep := endpointsFor(region)
	token, refresh := parseTokenInput(raw)
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "未能从内容里识别出令牌")
	}

	cred := &qoderCred{Region: region, DT: token, DRT: refresh}
	client := p.httpClient(cred)

	if strings.HasPrefix(token, "dt-") {
		cred.AuthMode = "oauth"
		cred.SecToken, cred.RefToken = token, refresh
		info, err := fetchUserInfo(ctx, client, ep, token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "令牌校验失败: "+err.Error())
		}
		cred.UID, cred.Name = info.UID, info.Name
		cred.UserType, cred.OrgID, cred.OrgName = info.UserType, info.OrganizationID, info.OrganizationName
	} else {
		// PAT：换 jobToken，拿 COSY 身份与 uid
		cred.AuthMode = "pat"
		if err := p.fillFingerprint(cred); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		jt, err := jobTokenExchange(ctx, client, ep, token, cred.MachineID, cred.MachineToken, cred.MachineType)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "PAT 校验失败: "+err.Error())
		}
		cred.UID, cred.Name, cred.UserType = jt.ID, jt.Name, jt.UserType
		cred.SecToken, cred.RefToken = jt.SecurityOauthToken, jt.RefreshToken
	}
	if err := p.fillFingerprint(cred); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if cred.UID == "" {
		return nil, status.Error(codes.Unauthenticated, "上游未返回用户 id，令牌可能无效")
	}
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

// parseTokenInput 兼容：纯 dt-/PAT 令牌、{"token","refresh_token"}、参考实现 auth 嵌套结构。
func parseTokenInput(raw string) (token, refresh string) {
	var flat struct {
		Token        string `json:"token"`
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refresh_token"`
		RefreshTok2  string `json:"refreshToken"`
	}
	if json.Unmarshal([]byte(raw), &flat) == nil {
		token = firstNonEmpty(flat.Token, flat.AccessToken)
		refresh = firstNonEmpty(flat.RefreshToken, flat.RefreshTok2)
		if token != "" || refresh != "" {
			return
		}
		var nested struct {
			Auth struct {
				AccessToken  string `json:"accessToken"`
				RefreshToken string `json:"refreshToken"`
			} `json:"auth"`
		}
		if json.Unmarshal([]byte(raw), &nested) == nil {
			return nested.Auth.AccessToken, nested.Auth.RefreshToken
		}
	}
	for _, f := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == ' ' || r == ',' }) {
		f = strings.TrimSpace(f)
		switch {
		case token == "" && !strings.HasPrefix(f, "drt-"):
			token = f
		case refresh == "" && strings.HasPrefix(f, "drt-"):
			refresh = f
		}
	}
	return
}

// ---------- 刷新 / 资料 ----------

func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ep := endpointsFor(cred.Region)
	client := p.httpClient(cred)
	changed := false

	switch {
	case cred.AuthMode == "pat" || (!strings.HasPrefix(cred.DT, "dt-") && cred.DT != ""):
		// PAT：重新换一次 jobToken（无过期概念，但 securityOauthToken 可能轮换）
		if jt, err := jobTokenExchange(ctx, client, ep, cred.DT, cred.MachineID, cred.MachineToken, cred.MachineType); err == nil {
			if jt.SecurityOauthToken != cred.SecToken || jt.RefreshToken != cred.RefToken {
				cred.SecToken, cred.RefToken = jt.SecurityOauthToken, jt.RefreshToken
				changed = true
			}
			if cred.UID == "" {
				cred.UID, cred.Name = jt.ID, jt.Name
				changed = true
			}
		}
	case cred.DRT != "" && cred.ExpiresAt > 0 && nowUnix() > cred.ExpiresAt-7200:
		// OAuth：dt- 临近过期 → 用 drt- 换新
		if err := refreshDeviceToken(ctx, client, ep, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "设备令牌刷新失败（需重新授权）: "+err.Error())
		}
		changed = true
	}
	if cred.UID == "" || cred.Name == "" {
		if info, err := fetchUserInfo(ctx, client, ep, cred.DT); err == nil {
			if cred.UID == "" {
				cred.UID = info.UID
				changed = true
			}
			if cred.Name == "" {
				cred.Name = info.Name
				changed = true
			}
		}
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

// profileFor 拉取额度 / 套餐（失败只影响展示，不影响账号可用性）。
func (p *plugin) profileFor(ctx context.Context, cred *qoderCred) *pb.AccountProfile {
	prof := &pb.AccountProfile{
		DisplayName: orDefault(cred.Name, orDefault(cred.UID, "Qoder")),
		Healthy:     true,
		Quota:       map[string]string{},
	}
	ep := endpointsFor(cred.Region)
	q, err := fetchQuota(ctx, p.httpClient(cred), ep, cred.DT)
	if err != nil {
		prof.Healthy = false
		prof.Sections = append(prof.Sections, sectionNote("quota_error", "额度查询失败", err.Error()))
		return prof
	}
	prof.Quota["credits"] = fmt.Sprintf("%d", q.Remaining())
	prof.Quota["total_credits"] = fmt.Sprintf("%d", q.Total())
	prof.Quota["used_credits"] = fmt.Sprintf("%d", q.Used())
	if q.Exceeded {
		prof.Quota["quota_exceeded"] = "true"
		prof.Healthy = false
	}
	prof.CreditsJson = q.CreditsJSON()
	if sec := planSection(q); sec != nil {
		prof.Sections = append(prof.Sections, sec)
	}
	return prof
}

// ---------- 设备指纹 / 会话 ----------

// fillFingerprint 派生并写入稳定设备指纹（已有则保留）。
func (p *plugin) fillFingerprint(c *qoderCred) error {
	if c.MachineID != "" && c.MachineToken != "" && c.MachineType != "" {
		return nil
	}
	seed := qodersign.SeedFor(c.UID, orDefault(c.DT, c.SecToken))
	fp := qodersign.DeriveFingerprint(seed, p.machineSalt())
	c.MachineID, c.MachineType, c.MachineToken = fp.MachineID, fp.MachineType, fp.MachineToken
	return nil
}

// sessionFor 构建 COSY 会话（身份字段来自 userinfo 或 jobToken）。
func (p *plugin) sessionFor(c *qoderCred) (*qodersign.Session, error) {
	if err := p.fillFingerprint(c); err != nil {
		return nil, err
	}
	return qodersign.NewSession(qodersign.Identity{
		Name:               c.Name,
		Aid:                c.UID,
		Uid:                c.UID,
		OrganizationID:     c.OrgID,
		OrganizationName:   c.OrgName,
		UserType:           orDefault(c.UserType, "personal_standard"),
		SecurityOauthToken: orDefault(c.SecToken, c.DT),
		RefreshToken:       orDefault(c.RefToken, c.DRT),
	}, c.MachineID, c.MachineToken, c.MachineType)
}

func (p *plugin) machineSalt() string {
	return strings.TrimSpace(str(p.settings()["machine_salt"]))
}

func (p *plugin) settings() map[string]interface{} {
	out := map[string]interface{}{}
	if p.host == nil {
		return out
	}
	raw := p.host.Settings("qoder")
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
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
