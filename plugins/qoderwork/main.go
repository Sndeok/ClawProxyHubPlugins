// qoderwork 插件 —— QoderWork（CN）反代。
//
// 上游：gateway.openapi.qoder.com.cn（COSY 签名 + QoderEncoding 请求体），
// 业务接口：openapi.qoder.com.cn（Bearer dt- 设备令牌）。
// 凭据：OAuth 设备授权（PKCE）或直接粘贴 dt-/drt-。
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

	mu     sync.Mutex
	logins map[string]*loginSession // loginID → 进行中的设备授权
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// accountCred 账号凭据（核心只当 opaque blob 存储）。
type accountCred struct {
	DT        string `json:"dt,omitempty"`  // dt- 设备令牌（Bearer）
	DRT       string `json:"drt,omitempty"` // drt- 刷新令牌（每次刷新轮换）
	UID       string `json:"uid,omitempty"`
	Nickname  string `json:"nickname,omitempty"`
	Region    string `json:"region,omitempty"`
	ExpiresAt int64  `json:"expires_at,omitempty"` // dt 过期时间（Unix 秒，0 = 未知）

	// COSY 设备指纹：首次登录派生并持久化，之后跨重启保持不变
	MachineID    string `json:"machine_id,omitempty"`
	MachineType  string `json:"machine_type,omitempty"`
	MachineToken string `json:"machine_token,omitempty"`

	// 出站代理（核心注入，不序列化）
	proxyURL string
}

// credFrom 解析凭据 + 代理配置。
func credFrom(blob *pb.CredentialBlob) (*accountCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c accountCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	if c.DT == "" {
		return nil, fmt.Errorf("凭据缺少 dt- 设备令牌")
	}
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func marshalCred(c *accountCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

// ---------- 握手：Manifest ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "qoderwork", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "QoderWork", "en": "QoderWork"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat", "tasks"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "oauth", Label: map[string]string{"zh": "浏览器授权（设备码）", "en": "Browser (device flow)"},
				Capabilities: []string{"refreshable", "auto_relogin", "profile"},
				// auto：插件自己在服务端轮询上游，域名部署（非本机访问）也能自动完成，
				// 不会退化成「粘贴回调地址」；auto_wait 只在本机访问时才轮询。
				Callback: "auto",
			},
			{
				Id: "token", Label: map[string]string{"zh": "粘贴令牌", "en": "Paste tokens"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{{
					Name: "content", Label: map[string]string{"zh": "dt- / drt- 令牌或授权 JSON", "en": "dt- / drt- tokens or auth JSON"},
					Type: "textarea", Required: true, Placeholder: `{"token":"dt-...","refresh_token":"drt-...","user_id":"..."}`,
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

// loginToken 粘贴 dt-/drt-（兼容参考实现的 auth JSON 形态）。
func (p *plugin) loginToken(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	raw := strings.TrimSpace(req.Form["content"])
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "请粘贴 dt-/drt- 令牌或授权 JSON")
	}
	dt, drt, uid := parseTokenInput(raw)
	if dt == "" {
		return nil, status.Error(codes.InvalidArgument, "未能从内容里识别出 dt- 令牌")
	}
	cred := &accountCred{DT: dt, DRT: drt, UID: uid, Region: regionCN}
	if err := p.fillFingerprint(cred); err != nil {
		return nil, err
	}
	// 用 userinfo 校正 uid/昵称（也顺带校验令牌是否可用）
	name, realUID, err := p.fetchUserInfo(ctx, p.httpClient(cred), cred.DT)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "令牌校验失败: "+err.Error())
	}
	cred.UID, cred.Nickname = realUID, name
	if cred.UID == "" {
		return nil, status.Error(codes.Unauthenticated, "上游未返回 user_id，令牌可能无效")
	}
	profile := p.profileFor(ctx, cred)
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: profile}, nil
}

// parseTokenInput 兼容三种输入：纯 dt-、JSON {token,refresh_token}、参考实现的 auth 嵌套结构。
func parseTokenInput(raw string) (dt, drt, uid string) {
	var probe map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &probe) == nil {
		var flat struct {
			Token        string `json:"token"`
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refresh_token"`
			RefreshTok2  string `json:"refreshToken"`
			UserID       string `json:"user_id"`
		}
		_ = json.Unmarshal([]byte(raw), &flat)
		var nested struct {
			Auth struct {
				AccessToken  string `json:"accessToken"`
				RefreshToken string `json:"refreshToken"`
			} `json:"auth"`
			Account struct {
				UID string `json:"uid"`
			} `json:"account"`
		}
		_ = json.Unmarshal([]byte(raw), &nested)
		dt = firstNonEmpty(flat.Token, flat.AccessToken, nested.Auth.AccessToken)
		drt = firstNonEmpty(flat.RefreshToken, flat.RefreshTok2, nested.Auth.RefreshToken)
		uid = firstNonEmpty(flat.UserID, nested.Account.UID)
		return
	}
	// 纯令牌，可能是 "dt-xxx" 或两行 "dt-xxx / drt-xxx"
	for _, f := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == ' ' || r == ',' }) {
		f = strings.TrimSpace(f)
		switch {
		case strings.HasPrefix(f, "dt-") && dt == "":
			dt = f
		case strings.HasPrefix(f, "drt-") && drt == "":
			drt = f
		}
	}
	if dt == "" {
		dt = raw // 上游若改用别的前缀，仍然尝试
	}
	return
}

// ---------- 刷新 / 资料 ----------

func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	client := p.httpClient(cred)
	changed := false
	if needRefresh(cred) {
		if err := p.refreshDeviceToken(ctx, client, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "设备令牌刷新失败（需重新授权）: "+err.Error())
		}
		changed = true
	}
	// 昵称/uid 缺失时补齐（粘贴令牌的场景）
	if cred.UID == "" || cred.Nickname == "" {
		if name, uid, err := p.fetchUserInfo(ctx, client, cred.DT); err == nil {
			if cred.UID == "" && uid != "" {
				cred.UID = uid
				changed = true
			}
			if cred.Nickname == "" && name != "" {
				cred.Nickname = name
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

// profileFor 拉取额度 / 套餐 / 签到状态，组装为 CPH 资料块。
// 任何一项失败都不影响返回（额度缺失只影响展示）。
func (p *plugin) profileFor(ctx context.Context, cred *accountCred) *pb.AccountProfile {
	prof := &pb.AccountProfile{
		DisplayName: orDefault(cred.Nickname, orDefault(cred.UID, "千问办公")),
		Healthy:     true,
		Quota:       map[string]string{},
	}
	client := p.httpClient(cred)
	q, err := p.fetchQuota(ctx, client, cred.DT)
	if err != nil {
		prof.Healthy = false
		prof.Sections = append(prof.Sections, sectionNote("quota_error", "额度查询失败", err.Error()))
		return prof
	}
	// 客户端同源的「积分余额」（日/月/长期钱包）优先；失败不影响主流程
	if w, werr := p.fetchWallets(ctx, client, cred.DT); werr == nil {
		q.Wallets = w
	} else {
		// 退回 quota/usage，并把失败原因挂到资料块（/admin/accounts/{id}/detail 可见）
		if p.host != nil {
			p.host.Log("warn", "qoderwork wallets 拉取失败，退回 quota/usage: "+werr.Error())
		}
		prof.Sections = append(prof.Sections, sectionNote("wallets_error", "积分余额（钱包）拉取失败", werr.Error()))
	}
	prof.Quota["credits"] = fmt.Sprintf("%d", q.Remaining())
	prof.Quota["total_credits"] = fmt.Sprintf("%d", q.Total())
	prof.Quota["used_credits"] = fmt.Sprintf("%d", q.Used())
	if q.Exceeded {
		prof.Quota["quota_exceeded"] = "true"
		prof.Healthy = false
	}
	prof.CreditsJson = q.CreditsJSON()
	if st, err := p.fetchCheckinStatus(ctx, client, cred.DT); err == nil {
		prof.Sections = append(prof.Sections, checkinSection(st, q))
	}
	if plan, err := p.fetchPlan(ctx, client, cred.DT); err == nil {
		prof.Sections = append(prof.Sections, planSection(plan))
	}
	return prof
}

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
