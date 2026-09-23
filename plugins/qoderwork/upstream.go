package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// QoderWork（CN）上游端点（逐步与参考实现核对）。
const (
	regionCN       = "cn"
	openapiBase    = "https://openapi.qoder.com.cn"
	gatewayBase    = "https://gateway.qoder.com.cn"
	modelsPath     = "/algo/api/v2/model/list?Encode=1"
	chatPath       = "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1"
	cosyVersion    = "0.1.43"
	cosyClientIP   = "169.254.198.161"
	defaultTimeout = 180 * time.Second
)

// headerCfg QoderWork（千问办公）的 COSY 头配置。
//
// 该产品的客户端把 ClientMetadata 一并发给上游，取值来自 qoder-auth-wasm 里的常量：
//
//	nxe="5" / S1t="6"（client_type）、D1t="cli" / Yqe="qoder_work"（business_product）、
//	b1t="agent"（business_type）、P1t="assistant"（scene）
//
// 之前只发 CLI 默认值（不带 product），上游按 CLI 处理，结果就是 x-model-key 被忽略、
// 无论选哪个模型都回默认的 Qwen3.5。
func headerCfg() qodersign.HeaderConfig {
	return qodersign.HeaderConfig{
		CosyVersion:  cosyVersion,
		ClientIP:     cosyClientIP,
		DataPolicy:   "AGREE",
		ClientType:   "6",          // = S1t（qoder_work 产品）
		Product:      "qoder_work", // = Yqe
		BusinessType: "agent",      // = b1t
		Scene:        "assistant",  // = P1t
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

type quotaInfo struct {
	UserTotal      float64
	UserUsed       float64
	UserRemaining  float64
	AddonTotal     float64
	AddonUsed      float64
	AddonRemaining float64
	Exceeded       bool
}

func (q *quotaInfo) Total() int64     { return int64(q.UserTotal + q.AddonTotal) }
func (q *quotaInfo) Used() int64      { return int64(q.UserUsed + q.AddonUsed) }
func (q *quotaInfo) Remaining() int64 { return int64(q.UserRemaining + q.AddonRemaining) }

// CreditsJSON 核心侧积分快照（十进制字符串，避免精度丢失）。
func (q *quotaInfo) CreditsJSON() string {
	pkgs := []map[string]string{
		{"name": "订阅额度", "total": ftoa(q.UserTotal), "used": ftoa(q.UserUsed), "remaining": ftoa(q.UserRemaining)},
	}
	if q.AddonTotal > 0 {
		pkgs = append(pkgs, map[string]string{"name": "赠送额度", "total": ftoa(q.AddonTotal), "used": ftoa(q.AddonUsed), "remaining": ftoa(q.AddonRemaining)})
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

func fetchQuota(ctx context.Context, client *http.Client, dt string) (*quotaInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", openapiBase+"/api/v2/quota/usage", nil)
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

// fetchUserInfo 取昵称与 uid（兼作令牌校验）。
func fetchUserInfo(ctx context.Context, client *http.Client, dt string) (name, uid string, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", openapiBase+"/api/v1/userinfo", nil)
	if err != nil {
		return "", "", err
	}
	authedJSON(req, dt)
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return "", "", fmt.Errorf("userinfo HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", "", err
	}
	name = firstNonEmpty(str(m["name"]), str(m["nickname"]), str(m["username"]))
	uid = firstNonEmpty(str(m["uid"]), str(m["user_id"]), str(m["userId"]), str(m["id"]))
	return name, uid, nil
}

func fetchPlan(ctx context.Context, client *http.Client, dt string) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", openapiBase+"/api/v2/user/plan", nil)
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
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("plan HTTP %d", resp.StatusCode)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, k := range []string{"user_type", "plan_tier_name", "plan_name", "display_name", "expire_time", "expires_at"} {
		if v := str(m[k]); v != "" {
			out[k] = v
		}
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

func fetchCheckinStatus(ctx context.Context, client *http.Client, dt string) (*checkinStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", openapiBase+"/sash/api/v1/me/daily-check-in/status", nil)
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
func claimCheckin(ctx context.Context, client *http.Client, dt string) (ok bool, claimed bool, detail string, err error) {
	req, err := http.NewRequestWithContext(ctx, "POST", openapiBase+"/sash/api/v1/me/daily-check-in/claim", strings.NewReader("{}"))
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
func refreshDeviceToken(ctx context.Context, client *http.Client, c *accountCred) error {
	body, _ := json.Marshal(map[string]string{"refresh_token": c.DRT})
	req, err := http.NewRequestWithContext(ctx, "POST", openapiBase+"/api/v1/deviceToken/refresh", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Go-http-client/2.0")
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
	if t.ExpiresIn > 0 {
		c.ExpiresAt = time.Now().Unix() + t.ExpiresIn/1000
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
