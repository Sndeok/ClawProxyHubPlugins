package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// QoderWork（CN）上游端点（逐步与参考实现核对）。
const (
	regionCN       = "cn"
	modelsPath     = "/algo/api/v2/model/list?Encode=1"
	chatPath       = "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1"
	cosyVersion    = "0.1.43"
	cosyClientIP   = "169.254.198.161"
	defaultTimeout = 180 * time.Second

	// 客户端 ClientMetadata 默认值（qoder-auth-wasm 常量，可用插件设置覆盖）
	defaultClientType   = "5"          // 实测：6 会被上游拒（403 code=112），5 + qoder_work 才通
	defaultProduct      = "qoder_work" // Yqe
	defaultBusinessType = "agent"      // b1t
	defaultScene        = "assistant"  // P1t

	// 客户端（千问办公）请求头默认值：适配器 API 需要这组 X-QwenWork-* 头，
	// 值取自客户端 getQwenWorkClientHeaders()；可用插件设置覆盖。
	defaultQwenWorkVersion  = "1.2.1"
	defaultQwenWorkRelease  = "1.2.1-26092107"
	defaultQwenWorkBuild    = "26092107"
	defaultQwenWorkPlatform = "win32"
	defaultQwenWorkArch     = "x64"
	defaultQwenWorkChannel  = "stable"

	// 请求体默认值（对齐客户端 QwenWork 构建的 body）
	defaultSessionType    = "qoder_work" // session_type
	defaultAliyunUserType = ""           // 客户端发空串
	defaultTaskID         = "common"     // task_id
)

// 千问办公（qwenworkcn）网关。官方客户端把三件事都放在同一台主机上：
//
//	授权页       https://<website>/device/selectAccounts
//	OpenAPI      https://<openapi>/api/v1/**（设备令牌、adapter 钱包、quota）
//	模型 + 对话   https://<gateway>/algo/api/**（COSY 签名）
//
// 在 qwenworkcn 产品下 resolveWebsiteDomain() / resolveOpenApiDomain() /
// resolveGatewayDomain() 三者取值相同，均为 gateway.qwenwork.cn。实测（2026-09-23）：
//
//	GET /device/selectAccounts?<真 PKCE + UUID nonce/machine_id> -> 302 到 qwenwork.cn/oauth2/auth
//	GET /api/v1/adapter/user/wallets                            -> 401 INVALID_TOKEN（路由存在）
//	GET /api/v2/quota/usage                                     -> 401 INVALID_TOKEN（路由存在）
//	GET /algo/api/v2/model/list?Encode=1                        -> 403 Signature invalid（路由存在）
//	GET /algo/api/v2/service/pro/sse/agent_chat_generation      -> 405（路由存在，需 POST）
//
// Qoder（qoder.com.cn / openapi.qoder.com.cn）是另一套账号体系，别混用。
//
// 这三个变量是默认值（测试里可整体替换成 httptest 地址）；插件设置
// gateway_base / openapi_base / adapter_base 优先。
var (
	gatewayBase = "https://gateway.qwenwork.cn"
	openapiBase = "https://gateway.qwenwork.cn"
	adapterBase = "https://gateway.qwenwork.cn"
)

// apiBase 插件设置优先、否则用默认端点（统一去掉尾部 /）。
func (p *plugin) apiBase(key, fallback string) string {
	return strings.TrimRight(p.settingStr(key, fallback), "/")
}

func (p *plugin) gatewayBaseURL() string { return p.apiBase("gateway_base", gatewayBase) }
func (p *plugin) openapiBaseURL() string { return p.apiBase("openapi_base", openapiBase) }
func (p *plugin) adapterBaseURL() string { return p.apiBase("adapter_base", adapterBase) }

// headerCfg QoderWork（千问办公）的 COSY 头配置。
//
// 该产品的客户端把 ClientMetadata 一并发给上游，取值来自 qoder-auth-wasm 里的常量：
//
//	nxe="5" / S1t="6"（client_type）、D1t="cli" / Yqe="qoder_work"（business_product）、
//	b1t="agent"（business_type）、P1t="assistant"（scene）
//
// 之前只发 CLI 默认值（不带 product），上游按 CLI 处理，结果就是 x-model-key 被忽略、
// 无论选哪个模型都回默认的 Qwen3.5。
func (p *plugin) headerCfg() qodersign.HeaderConfig {
	return qodersign.HeaderConfig{
		CosyVersion:  p.settingStr("cosy_version", cosyVersion),
		ClientIP:     p.settingStr("cosy_client_ip", cosyClientIP),
		DataPolicy:   "AGREE",
		ClientType:   p.settingStr("cosy_client_type", defaultClientType),
		Product:      p.settingStr("cosy_business_product", defaultProduct),
		BusinessType: p.settingStr("cosy_business_type", defaultBusinessType),
		Scene:        p.settingStr("cosy_scene", defaultScene),
	}
}

// ---------- HTTP 客户端（支持账号级出站代理） ----------

var clientCache sync.Map // proxyURL → *http.Client

func proxyURLOf(p *pb.ProxyConfig) string {
	if p == nil || p.GetHost() == "" {
		return ""
	}
	u := &url.URL{
		Scheme: orDefault(p.GetScheme(), "http"),
		Host:   fmt.Sprintf("%s:%d", p.GetHost(), p.GetPort()),
	}
	if p.GetUsername() != "" {
		u.User = url.UserPassword(p.GetUsername(), p.GetPassword())
	}
	return u.String()
}

// httpClient 出站客户端：QoderWork 网关对 HTTP/2 不友好（流式会 INTERNAL_ERROR），强制 HTTP/1.1。
func (p *plugin) httpClient(cred *accountCred) *http.Client {
	key := ""
	if cred != nil {
		key = cred.proxyURL
	}
	if c, ok := clientCache.Load(key); ok {
		return c.(*http.Client)
	}
	tr := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{}, // 关 HTTP/2
	}
	if key != "" {
		if u, err := url.Parse(key); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr.Proxy = http.ProxyFromEnvironment
	}
	c := &http.Client{Transport: tr, Timeout: defaultTimeout}
	clientCache.Store(key, c)
	return c
}

// ---------- 业务接口（Bearer dt-） ----------

// walletBalances 客户端（千问办公）「积分余额」的三个钱包：日 / 月 / 长期。
// 客户端走 GET /api/v1/adapter/user/wallets 取每个钱包的 total_balance——
// 只读 /api/v2/quota/usage 的 userQuota/addOnQuota 会少算（实测 399 vs 客户端 2100）。
type walletBalances struct {
	Daily       float64
	Monthly     float64
	Longterm    float64
	HasDaily    bool
	HasMonthly  bool
	HasLongterm bool
}

// Total 三个钱包余额之和 = 客户端侧「积分余额」。
func (w *walletBalances) Total() float64 { return w.Daily + w.Monthly + w.Longterm }

// Any 至少解析出一个钱包。
func (w *walletBalances) Any() bool { return w.HasDaily || w.HasMonthly || w.HasLongterm }

type quotaInfo struct {
	Wallets        *walletBalances
	UserTotal      float64
	UserUsed       float64
	UserRemaining  float64
	AddonTotal     float64
	AddonUsed      float64
	AddonRemaining float64
	Exceeded       bool
}

// Total / Remaining 优先用客户端同源钱包余额；拿不到才退回 quota/usage。
func (q *quotaInfo) Total() int64 {
	if q.Wallets != nil && q.Wallets.Any() {
		return int64(q.Wallets.Total())
	}
	return int64(q.UserTotal + q.AddonTotal)
}
func (q *quotaInfo) Used() int64 { return int64(q.UserUsed + q.AddonUsed) }
func (q *quotaInfo) Remaining() int64 {
	if q.Wallets != nil && q.Wallets.Any() {
		return int64(q.Wallets.Total())
	}
	return int64(q.UserRemaining + q.AddonRemaining)
}

// CreditsJSON 核心侧积分快照（十进制字符串，避免精度丢失）。
func (q *quotaInfo) CreditsJSON() string {
	// 有客户端同源钱包就用它（余额口径），否则退回 quota/usage 的订阅 + 赠送额度
	var pkgs []map[string]string
	if q.Wallets != nil && q.Wallets.Any() {
		if q.Wallets.HasDaily {
			pkgs = append(pkgs, map[string]string{"name": "日额度", "total": ftoa(q.Wallets.Daily), "remaining": ftoa(q.Wallets.Daily)})
		}
		if q.Wallets.HasMonthly {
			pkgs = append(pkgs, map[string]string{"name": "月度额度", "total": ftoa(q.Wallets.Monthly), "used": ftoa(q.UserUsed), "remaining": ftoa(q.Wallets.Monthly)})
		}
		if q.Wallets.HasLongterm {
			pkgs = append(pkgs, map[string]string{"name": "长期额度", "total": ftoa(q.Wallets.Longterm), "remaining": ftoa(q.Wallets.Longterm)})
		}
	} else {
		pkgs = append(pkgs, map[string]string{"name": "订阅额度", "total": ftoa(q.UserTotal), "used": ftoa(q.UserUsed), "remaining": ftoa(q.UserRemaining)})
		if q.AddonTotal > 0 {
			pkgs = append(pkgs, map[string]string{"name": "赠送额度", "total": ftoa(q.AddonTotal), "used": ftoa(q.AddonUsed), "remaining": ftoa(q.AddonRemaining)})
		}
	}
	b, _ := json.Marshal(map[string]interface{}{
		"total":     ftoa(float64(q.Total())),
		"used":      ftoa(float64(q.Used())),
		"remaining": ftoa(float64(q.Remaining())),
		"packages":  pkgs,
		"source":    "qoderwork.quota",
	})
	return string(b)
}

func ftoa(f float64) string {
	s := strings.TrimSuffix(fmt.Sprintf("%.2f", f), "00")
	return strings.TrimSuffix(s, ".")
}

func (p *plugin) fetchQuota(ctx context.Context, client *http.Client, dt string) (*quotaInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", p.openapiBaseURL()+"/api/v2/quota/usage", nil)
	if err != nil {
		return nil, err
	}
	authedJSON(req, dt)
	for k, v := range p.qwenWorkClientHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("quota HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var q struct {
		UserQuota struct {
			Total     float64 `json:"total"`
			Used      float64 `json:"used"`
			Remaining float64 `json:"remaining"`
		} `json:"userQuota"`
		AddOnQuota struct {
			Total     float64 `json:"total"`
			Used      float64 `json:"used"`
			Remaining float64 `json:"remaining"`
		} `json:"addOnQuota"`
		IsQuotaExceeded bool `json:"isQuotaExceeded"`
	}
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil, fmt.Errorf("配额解析失败: %w", err)
	}
	return &quotaInfo{
		UserTotal: q.UserQuota.Total, UserUsed: q.UserQuota.Used, UserRemaining: q.UserQuota.Remaining,
		AddonTotal: q.AddOnQuota.Total, AddonUsed: q.AddOnQuota.Used, AddonRemaining: q.AddOnQuota.Remaining,
		Exceeded: q.IsQuotaExceeded,
	}, nil
}

// fetchWallets 拉客户端同源的钱包余额（GET /api/v1/adapter/user/wallets）。
// 响应等价于客户端 unwrapAdapterResponse：有 data 对象就取 data；三个钱包各取 total_balance。
// qwenWorkClientHeaders 客户端（千问办公）请求头。
// 适配器 API（/api/v1/adapter/**）要求这组 X-QwenWork-* 头，缺了会被拒；
// 取值对应客户端 getQwenWorkClientHeaders()，都可用插件设置覆盖。
func (p *plugin) qwenWorkClientHeaders() map[string]string {
	return map[string]string{
		"Accept":                     "application/json",
		"User-Agent":                 "qoderwork/" + p.settingStr("qwenwork_version", defaultQwenWorkVersion),
		"X-Request-Id":               randomUUID(),
		"X-QwenWork-Version":         p.settingStr("qwenwork_version", defaultQwenWorkVersion),
		"X-QwenWork-Release-Version": p.settingStr("qwenwork_release_version", defaultQwenWorkRelease),
		"X-QwenWork-Build":           p.settingStr("qwenwork_build", defaultQwenWorkBuild),
		"X-QwenWork-Platform":        p.settingStr("qwenwork_platform", defaultQwenWorkPlatform),
		"X-QwenWork-Arch":            p.settingStr("qwenwork_arch", defaultQwenWorkArch),
		"X-QwenWork-Channel":         p.settingStr("qwenwork_channel", defaultQwenWorkChannel),
	}
}

func (p *plugin) fetchWallets(ctx context.Context, client *http.Client, dt string) (*walletBalances, error) {
	base := p.adapterBaseURL()
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+"/api/v1/adapter/user/wallets", nil)
	if err != nil {
		return nil, err
	}
	authedJSON(req, dt)
	for k, v := range p.qwenWorkClientHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("wallets HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("wallets 解析失败: %w", err)
	}
	body := raw
	if d, ok := top["data"]; ok && len(d) > 0 && d[0] == '{' {
		body = d
	}
	var wallets map[string]json.RawMessage
	if err := json.Unmarshal(body, &wallets); err != nil {
		return nil, fmt.Errorf("wallets 结构解析失败: %w", err)
	}
	out := &walletBalances{}
	for _, spec := range []struct {
		key   string
		field *float64
		seen  *bool
	}{
		{"daily_credits", &out.Daily, &out.HasDaily},
		{"monthly_credits", &out.Monthly, &out.HasMonthly},
		{"longterm_credits", &out.Longterm, &out.HasLongterm},
	} {
		rawWallet, ok := wallets[spec.key]
		if !ok {
			continue
		}
		var m map[string]interface{}
		if json.Unmarshal(rawWallet, &m) != nil {
			continue
		}
		if v, ok := numField(m, "total_balance", "totalBalance"); ok {
			*spec.field = v
			*spec.seen = true
		}
	}
	if !out.Any() {
		return nil, fmt.Errorf("wallets 未返回余额字段: %s", clip(string(body), 200))
	}
	return out, nil
}

// numField 从 map 里按候选键取数字（兼容 snake_case / camelCase 与字符串数字）。
func numField(m map[string]interface{}, keys ...string) (float64, bool) {
	for _, k := range keys {
		switch v := m[k].(type) {
		case float64:
			return v, true
		case string:
			if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}

// accountContext 取客户端同源的账号上下文：
// GET /api/v1/adapter/user/account-context?include=user,plan,quota,page,data_sharing
//
// 千问办公没有 Qoder 的 /sash/api/v1/me（实测 404），昵称 / uid / 套餐都从这里取。
// 响应形如 {code:0,data:{user:{...},plan:{...},quota:{...}}}，与客户端
// unwrapAdapterResponse()（有 data 对象就取 data）保持一致。
func (p *plugin) accountContext(ctx context.Context, client *http.Client, dt string) (map[string]interface{}, error) {
	rawURL := p.openapiBaseURL() + "/api/v1/adapter/user/account-context?include=user,plan,quota,page,data_sharing"
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	authedJSON(req, dt)
	for k, v := range p.qwenWorkClientHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("account-context HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	body := raw
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err == nil {
		if d, ok := top["data"]; ok && len(d) > 0 && d[0] == '{' {
			body = d
		}
	}
	var env map[string]interface{}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("account-context 解析失败: %w", err)
	}
	return env, nil
}

// fetchUserInfo 取昵称与 uid（兼作令牌校验）。
func (p *plugin) fetchUserInfo(ctx context.Context, client *http.Client, dt string) (name, uid string, err error) {
	env, err := p.accountContext(ctx, client, dt)
	if err != nil {
		return "", "", err
	}
	user, _ := env["user"].(map[string]interface{})
	if user == nil { // 兼容没有 user 包裹的返回
		user = env
	}
	name = firstNonEmpty(str(user["name"]), str(user["nickname"]), str(user["display_name"]),
		str(user["displayName"]), str(user["username"]), str(user["email"]))
	uid = firstNonEmpty(str(user["user_id"]), str(user["userId"]), str(user["uid"]), str(user["id"]))
	return name, uid, nil
}

// fetchPlan 套餐信息，取自 account-context 的 user / plan 段。
func (p *plugin) fetchPlan(ctx context.Context, client *http.Client, dt string) (map[string]string, error) {
	env, err := p.accountContext(ctx, client, dt)
	if err != nil {
		return nil, err
	}
	user, _ := env["user"].(map[string]interface{})
	plan, _ := env["plan"].(map[string]interface{})
	out := map[string]string{}
	put := func(key string, vals ...interface{}) {
		for _, v := range vals {
			if sv := str(v); sv != "" {
				out[key] = sv
				return
			}
		}
	}
	if plan != nil {
		put("plan_tier_name", plan["plan_name"], plan["display_name"], plan["tier_name"])
		put("plan_name", plan["plan_name"], plan["name"])
		put("expire_time", plan["expire_time"], plan["expires_at"], plan["next_due_date"])
		put("user_type", plan["user_type"], plan["type"])
	}
	if user != nil {
		put("plan_tier_name", user["plan_name"], user["plan_tier_name"])
		put("user_type", user["user_type"], user["account_type"])
		put("expire_time", user["expire_time"], user["plan_expire_time"])
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("账号上下文里没有套餐信息")
	}
	return out, nil
}

type checkinStatus struct {
	Status             string `json:"status"`
	RewardCredits      int64  `json:"rewardCredits"`
	NextClaimAt        int64  `json:"nextClaimAt"`
	CurrentStreakDays  int64  `json:"currentStreakDays"`
	TotalClaimDays     int64  `json:"totalClaimDays"`
	TotalRewardCredits int64  `json:"totalRewardCredits"`
	LastClaimedAt      int64  `json:"lastClaimedAt"`
}

// errNoCheckinAPI 千问办公没有 Qoder 的 /sash 签到接口（实测 404）。
var errNoCheckinAPI = errors.New("该产品没有每日签到接口（/sash 仅 Qoder 账号体系提供）")

func (p *plugin) fetchCheckinStatus(ctx context.Context, client *http.Client, dt string) (*checkinStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", p.openapiBaseURL()+"/sash/api/v1/me/daily-check-in/status", nil)
	if err != nil {
		return nil, err
	}
	authedJSON(req, dt)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == 404 {
		return nil, errNoCheckinAPI
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("checkin status HTTP %d", resp.StatusCode)
	}
	var st checkinStatus
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// claimCheckin 领取每日签到积分；已领取返回 claimed=true（幂等，不算失败）。
func (p *plugin) claimCheckin(ctx context.Context, client *http.Client, dt string) (ok bool, claimed bool, detail string, err error) {
	req, err := http.NewRequestWithContext(ctx, "POST", p.openapiBaseURL()+"/sash/api/v1/me/daily-check-in/claim", strings.NewReader("{}"))
	if err != nil {
		return false, false, "", err
	}
	authedJSON(req, dt)
	resp, err := client.Do(req)
	if err != nil {
		return false, false, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	body := string(raw)
	if resp.StatusCode == 404 {
		return false, false, "", errNoCheckinAPI
	}
	if resp.StatusCode >= 400 {
		low := strings.ToLower(body)
		if resp.StatusCode == 409 || strings.Contains(low, "not_eligible") || strings.Contains(low, "claimed") {
			return false, true, "今日已领取", nil
		}
		return false, false, "", fmt.Errorf("签到 HTTP %d: %s", resp.StatusCode, clip(body, 200))
	}
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if success, _ := m["success"].(bool); success {
		credits := firstNonEmpty(str(m["rewardCredits"]), str(m["credits"]))
		if credits == "" {
			return true, false, "签到成功", nil
		}
		return true, false, "签到成功，获得 " + credits + " 积分", nil
	}
	return false, false, "", fmt.Errorf("签到失败: %s", clip(body, 200))
}

func authedJSON(req *http.Request, dt string) {
	req.Header.Set("Authorization", "Bearer "+dt)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Go-http-client/2.0")
}

// ---------- 凭据刷新 ----------

// needRefresh dt 是否临近过期（<2h）；过期时间未知但已有刷新令牌时也刷新一次。
func needRefresh(c *accountCred) bool {
	if c.DRT == "" {
		return false
	}
	if c.ExpiresAt == 0 {
		return true
	}
	return time.Now().Unix() > c.ExpiresAt-2*3600
}

// refreshDeviceToken 用 drt- 换新 dt-（上游会轮换 refresh token）。
//
// 官方客户端 refreshDeviceToken() 的请求体是 {refresh_token, target:"c"}，打到
// openApiBase 的 /api/v1/deviceToken/refresh（实测该路由存在，缺 refresh_token 时 400）。
func (p *plugin) refreshDeviceToken(ctx context.Context, client *http.Client, c *accountCred) error {
	body, _ := json.Marshal(map[string]string{"refresh_token": c.DRT, "target": "c"})
	req, err := http.NewRequestWithContext(ctx, "POST", p.openapiBaseURL()+"/api/v1/deviceToken/refresh", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range p.qwenWorkClientHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var t struct {
		DeviceToken  string `json:"device_token"`
		Token        string `json:"token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		ExpiresAt    string `json:"expires_at"`
	}
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	dt := firstNonEmpty(t.DeviceToken, t.Token)
	if dt == "" {
		return fmt.Errorf("刷新响应里没有新令牌")
	}
	c.DT = dt
	if t.RefreshToken != "" {
		c.DRT = t.RefreshToken
	}
	if t.ExpiresAt != "" {
		if parsed, perr := time.Parse(time.RFC3339, strings.TrimSpace(t.ExpiresAt)); perr == nil {
			c.ExpiresAt = parsed.Unix()
			return nil
		}
	}
	if t.ExpiresIn > 0 {
		c.ExpiresAt = expiryFromNow(t.ExpiresIn)
	}
	return nil
}

// ---------- 设备指纹 ----------

// fillFingerprint 派生并写入稳定设备指纹（已有则保留）。
func (p *plugin) fillFingerprint(c *accountCred) error {
	if c.MachineID != "" && c.MachineToken != "" && c.MachineType != "" {
		return nil
	}
	seed := qodersign.SeedFor(c.UID, c.DT)
	fp := qodersign.DeriveFingerprint(seed, p.machineSalt())
	c.MachineID, c.MachineType, c.MachineToken = fp.MachineID, fp.MachineType, fp.MachineToken
	return nil
}

// machineSalt 插件设置里的本机盐（空 = 与参考实现同构）。
func (p *plugin) machineSalt() string {
	return strings.TrimSpace(str(p.settings()["machine_salt"]))
}

// settingStr 读插件设置里的字符串（空值回落默认）。
func (p *plugin) settingStr(key, def string) string {
	if v := strings.TrimSpace(str(p.settings()[key])); v != "" {
		return v
	}
	return def
}

// settings 读插件设置（核心侧保存后即时生效；失败返回空表）。
func (p *plugin) settings() map[string]interface{} {
	out := map[string]interface{}{}
	if p.host == nil {
		return out
	}
	raw := p.host.Settings("qoderwork")
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

// ---------- 工具 ----------

func str(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.2f", x), "00"), ".")
	case bool:
		if x {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
