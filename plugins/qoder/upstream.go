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

// Qoder 站点区域。
const (
	regionGlobal = "global"
	regionCN     = "cn"
)

// endpoints 一个区域的全部上游端点（与参考实现 qoder2api/account/region.go 一致）。
type endpoints struct {
	Region      string
	DeviceLogin string
	Poll        string
	Userinfo    string
	Plan        string
	Quota       string
	ChatStream  string
	ModelList   string
	JobToken    string
	CheckinHost string // 签到（活动领取）：CN 用 openapi.qoder.com.cn，Global 用 openapi.qoder.sh
}

func endpointsFor(region string) endpoints {
	if region == regionCN {
		return endpoints{
			Region:      regionCN,
			DeviceLogin: "https://qoder.com.cn/device/selectAccounts",
			Poll:        "https://openapi.qoder.com.cn/api/v1/deviceToken/poll",
			Userinfo:    "https://openapi.qoder.com.cn/api/v1/userinfo",
			Plan:        "https://openapi.qoder.com.cn/api/v2/user/plan",
			Quota:       "https://openapi.qoder.com.cn/api/v2/quota/usage",
			ChatStream:  "https://gateway.qoder.com.cn/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1",
			ModelList:   "https://gateway.qoder.com.cn/algo/api/v2/model/list?Encode=1",
			JobToken:    "https://gateway.qoder.com.cn/algo/api/v3/user/jobToken?Encode=1",
			CheckinHost: "openapi.qoder.com.cn",
		}
	}
	return endpoints{
		Region:      regionGlobal,
		DeviceLogin: "https://qoder.com/device/selectAccounts",
		Poll:        "https://openapi.qoder.sh/api/v1/deviceToken/poll",
		Userinfo:    "https://openapi.qoder.sh/api/v1/userinfo",
		Plan:        "https://openapi.qoder.sh/api/v2/user/plan",
		Quota:       "https://openapi.qoder.sh/api/v2/quota/usage",
		ChatStream:  "https://api1.qoder.sh/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1",
		ModelList:   "https://api2.qoder.sh/algo/api/v2/model/list?Encode=1",
		JobToken:    "https://center.qoder.sh/algo/api/v3/user/jobToken?Encode=1",
		CheckinHost: "openapi.qoder.sh",
	}
}

// cosyVersion Qoder 桌面端的 COSY 版本。
const cosyVersion = "1.0.10"

// cosySecretB64 COSY legacy 签名固定盐（与客户端一致）。
const cosySecretB64 = "d2FyLCB3YXIgbmV2ZXIgY2hhbmdlcw=="

func headerCfg() qodersign.HeaderConfig {
	return qodersign.HeaderConfig{
		CosyVersion:  cosyVersion,
		Scene:        "assistant",
		Product:      "ide",
		BusinessType: "agent",
		DataPolicy:   "agree",
	}
}

// ---------- HTTP ----------

var clientCache sync.Map

func proxyURLOf(p *pb.ProxyConfig) string {
	if p == nil || p.GetHost() == "" {
		return ""
	}
	u := &url.URL{Scheme: orDefault(p.GetScheme(), "http"), Host: fmt.Sprintf("%s:%d", p.GetHost(), p.GetPort())}
	if p.GetUsername() != "" {
		u.User = url.UserPassword(p.GetUsername(), p.GetPassword())
	}
	return u.String()
}

func (p *plugin) httpClient(cred *qoderCred) *http.Client {
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
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	if key != "" {
		if u, err := url.Parse(key); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr.Proxy = http.ProxyFromEnvironment
	}
	c := &http.Client{Transport: tr, Timeout: 180 * time.Second}
	clientCache.Store(key, c)
	return c
}

// ---------- 用户信息 ----------

type userInfo struct {
	Name             string
	UID              string
	OrganizationID   string
	OrganizationName string
	UserType         string
}

func fetchUserInfo(ctx context.Context, client *http.Client, ep endpoints, token string) (*userInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", ep.Userinfo, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("userinfo HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	info := &userInfo{
		Name:             firstNonEmpty(str(m["name"]), str(m["nickname"]), str(m["username"])),
		UID:              firstNonEmpty(str(m["id"]), str(m["uid"]), str(m["user_id"])),
		OrganizationID:   firstNonEmpty(str(m["organization_id"]), str(m["organizationId"])),
		OrganizationName: firstNonEmpty(str(m["organization_name"]), str(m["organizationName"])),
		UserType:         firstNonEmpty(str(m["userType"]), str(m["user_type"]), "personal_standard"),
	}
	if info.UID == "" {
		return nil, fmt.Errorf("userinfo 未返回用户 id")
	}
	return info, nil
}

// jobToken 交换结果：securityOauthToken / refreshToken 才是 COSY 身份里的那两项。
type jobToken struct {
	Name               string
	ID                 string
	UserType           string
	SecurityOauthToken string
	RefreshToken       string
}

// jobTokenExchange 把 PAT / refresh token 换成 jobToken（COSY legacy 签名 + Encode 体）。
func jobTokenExchange(ctx context.Context, client *http.Client, ep endpoints, token, machineID, machineToken, machineType string) (*jobToken, error) {
	date := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	sig := qodersign.MD5Hex("cosy&" + cosySecretB64 + "&" + date)

	personalToken, refreshToken := "", ""
	if strings.HasPrefix(token, "drt-") {
		refreshToken = token
	} else {
		personalToken = token
	}
	inner, _ := json.Marshal(map[string]interface{}{
		"personalToken":      personalToken,
		"securityOauthToken": "",
		"refreshToken":       refreshToken,
		"needRefresh":        refreshToken != "",
		"authInfo":           map[string]interface{}{},
	})
	outer, _ := json.Marshal(map[string]interface{}{"payload": string(inner), "encodeVersion": "1"})
	body, err := qodersign.Encode(outer)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ep.JobToken, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	h := req.Header
	h.Set("cosy-machinetoken", machineToken)
	h.Set("cosy-machinetype", machineType)
	h.Set("cosy-machineid", machineID)
	h.Set("login-version", "v2")
	h.Set("appcode", "cosy")
	h.Set("accept", "application/json")
	h.Set("accept-encoding", "identity")
	h.Set("cosy-version", cosyVersion)
	h.Set("cosy-clienttype", "5")
	h.Set("date", date)
	h.Set("signature", sig)
	h.Set("content-type", "application/json")
	h.Set("user-agent", "Go-http-client/2.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("jobToken HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	jt := &jobToken{
		Name:               str(m["name"]),
		ID:                 firstNonEmpty(str(m["id"]), str(m["uid"])),
		UserType:           firstNonEmpty(str(m["userType"]), str(m["user_type"]), "personal_standard"),
		SecurityOauthToken: firstNonEmpty(str(m["securityOauthToken"]), str(m["security_oauth_token"])),
		RefreshToken:       firstNonEmpty(str(m["refreshToken"]), str(m["refresh_token"])),
	}
	if jt.SecurityOauthToken == "" {
		return nil, fmt.Errorf("jobToken 响应里没有 securityOauthToken")
	}
	return jt, nil
}

// ---------- 额度 / 套餐 ----------

type quotaInfo struct {
	Plan        string
	UserTotal   float64
	UserUsed    float64
	UserRemain  float64
	AddonTotal  float64
	AddonUsed   float64
	AddonRemain float64
	ResetTime   string
	Exceeded    bool
	ExpiresAt   int64
}

func (q *quotaInfo) Total() int64     { return int64(q.UserTotal + q.AddonTotal) }
func (q *quotaInfo) Used() int64      { return int64(q.UserUsed + q.AddonUsed) }
func (q *quotaInfo) Remaining() int64 { return int64(q.UserRemain + q.AddonRemain) }

// CreditsJSON 核心侧积分快照（十进制字符串，避免精度丢失）。
func (q *quotaInfo) CreditsJSON() string {
	pkgs := []map[string]string{
		{"name": "订阅额度", "total": ftoa(q.UserTotal), "used": ftoa(q.UserUsed), "remaining": ftoa(q.UserRemain)},
	}
	if q.AddonTotal > 0 {
		pkgs = append(pkgs, map[string]string{"name": "赠送额度", "total": ftoa(q.AddonTotal), "used": ftoa(q.AddonUsed), "remaining": ftoa(q.AddonRemain)})
	}
	snap := map[string]interface{}{
		"total":     ftoa(float64(q.Total())),
		"used":      ftoa(float64(q.Used())),
		"remaining": ftoa(float64(q.Remaining())),
		"packages":  pkgs,
		"source":    "qoder.quota",
	}
	if q.ResetTime != "" {
		snap["reset_time"] = q.ResetTime
	}
	if q.ExpiresAt > 0 {
		snap["expires_at"] = q.ExpiresAt
	}
	b, _ := json.Marshal(snap)
	return string(b)
}

func fetchQuota(ctx context.Context, client *http.Client, ep endpoints, token string) (*quotaInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", ep.Quota, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("quota HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	q := &quotaInfo{Exceeded: m["isQuotaExceeded"] == true, ExpiresAt: int64(toFloat(m, "expiresAt"))}
	if b := bucketOf(m, "userQuota"); b != nil {
		q.UserTotal, q.UserUsed, q.UserRemain, q.ResetTime = b.total, b.used, b.remain, b.reset
	}
	if b := bucketOf(m, "addOnQuota"); b != nil {
		q.AddonTotal, q.AddonUsed, q.AddonRemain = b.total, b.used, b.remain
		if q.ResetTime == "" {
			q.ResetTime = b.reset
		}
	}
	q.Plan = fetchPlanName(ctx, client, ep, token)
	return q, nil
}

type bucketInfo struct {
	total, used, remain float64
	reset               string
}

// bucketOf 解析额度桶；三项全 0 视为未提供。
func bucketOf(m map[string]interface{}, key string) *bucketInfo {
	obj, ok := m[key].(map[string]interface{})
	if !ok {
		return nil
	}
	b := &bucketInfo{
		total:  toFloat(obj, "total"),
		used:   toFloat(obj, "used"),
		remain: toFloat(obj, "remaining"),
		reset:  firstNonEmpty(str(obj["resetTime"]), str(obj["reset_time"])),
	}
	if b.total == 0 && b.used == 0 && b.remain == 0 {
		return nil
	}
	return b
}

func fetchPlanName(ctx context.Context, client *http.Client, ep endpoints, token string) string {
	req, err := http.NewRequestWithContext(ctx, "GET", ep.Plan, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return ""
	}
	var m map[string]interface{}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&m) != nil {
		return ""
	}
	return firstNonEmpty(str(m["plan_tier_name"]), str(m["plan_name"]), str(m["plan"]), str(m["userType"]))
}

func toFloat(m map[string]interface{}, key string) float64 {
	switch v := m[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	}
	return 0
}

// ---------- 每日签到（活动领取） ----------

type campaign struct {
	CampaignID  string `json:"campaignId"`
	CampaignKey string `json:"campaignKey"`
	ActionType  string `json:"actionType"`
	ClaimStatus string `json:"claimStatus"`
	Benefit     *struct {
		Kind   string `json:"kind"`
		Amount int    `json:"amount"`
	} `json:"benefit"`
}

type checkinResult struct {
	Claimed bool
	Detail  string
	Amount  int
}

// claimDailyCheckin 走桌面端活动流程：查列表 → 找可领取的 CLAIM_BENEFIT → 领取（空 body）。
func claimDailyCheckin(ctx context.Context, client *http.Client, ep endpoints, dt string) (*checkinResult, error) {
	if ep.CheckinHost == "" {
		return nil, fmt.Errorf("该区域不支持每日签到")
	}
	base := "https://" + ep.CheckinHost
	req, err := http.NewRequestWithContext(ctx, "GET", base+"/sash/api/v1/me/campaigns", nil)
	if err != nil {
		return nil, err
	}
	checkinHeaders(req, dt, ep.CheckinHost, false)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("活动列表 HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var list struct {
		Campaigns []campaign `json:"campaigns"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	var target *campaign
	claimed := false
	for i := range list.Campaigns {
		c := &list.Campaigns[i]
		if c.ActionType != "CLAIM_BENEFIT" {
			continue
		}
		switch c.ClaimStatus {
		case "CLAIMABLE":
			target = c
		case "CLAIMED":
			claimed = true
		}
	}
	if target == nil {
		if claimed {
			return &checkinResult{Claimed: true, Detail: "今日已领取"}, nil
		}
		return &checkinResult{Detail: "当前没有可领取的签到活动"}, nil
	}

	claimURL := fmt.Sprintf("%s/sash/api/v1/me/campaigns/%s/claim", base, url.PathEscape(target.CampaignID))
	claimReq, err := http.NewRequestWithContext(ctx, "POST", claimURL, nil)
	if err != nil {
		return nil, err
	}
	checkinHeaders(claimReq, dt, ep.CheckinHost, true)
	claimResp, err := client.Do(claimReq)
	if err != nil {
		return nil, err
	}
	claimRaw, _ := io.ReadAll(io.LimitReader(claimResp.Body, 1<<20))
	claimResp.Body.Close()
	if claimResp.StatusCode >= 400 {
		low := strings.ToLower(string(claimRaw))
		if claimResp.StatusCode == 409 || strings.Contains(low, "claimed") || strings.Contains(low, "not_eligible") {
			return &checkinResult{Claimed: true, Detail: "今日已领取"}, nil
		}
		return nil, fmt.Errorf("领取 HTTP %d: %s", claimResp.StatusCode, clip(string(claimRaw), 200))
	}
	amount := 0
	if target.Benefit != nil {
		amount = target.Benefit.Amount
	}
	detail := "签到成功"
	if amount > 0 {
		detail = fmt.Sprintf("签到成功，获得 %d 积分", amount)
	}
	return &checkinResult{Claimed: false, Detail: detail, Amount: amount}, nil
}

// checkinHeaders 桌面端签到请求头（抓包确认的必需头）。
func checkinHeaders(req *http.Request, dt, host string, post bool) {
	req.Header.Set("Authorization", "Bearer "+dt)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "zh-CN")
	req.Header.Set("User-Agent", "Qoder")
	req.Header.Set("cosy-clienttype", "10")
	if post {
		req.Header.Set("Origin", "https://"+host)
		req.Header.Set("Content-Type", "application/json")
	}
}

// ---------- 模型目录 ----------

type qoderModel struct {
	Key             string  `json:"key"`
	DisplayName     string  `json:"display_name"`
	Enable          bool    `json:"enable"`
	IsDefault       bool    `json:"is_default"`
	IsReasoning     bool    `json:"is_reasoning"`
	ContextWindow   int     `json:"context_window"`
	MaxOutputTokens int     `json:"max_output_tokens"`
	MaxInputTokens  int     `json:"max_input_tokens"`
	PriceFactor     float64 `json:"price_factor"`
}

func (m qoderModel) Display() string {
	if strings.TrimSpace(m.DisplayName) != "" {
		return m.DisplayName
	}
	return m.Key
}

func (m qoderModel) Context() int {
	if m.ContextWindow > 0 {
		return m.ContextWindow
	}
	return m.MaxInputTokens
}

// fetchModels COSY 签名 GET 模型目录：依次尝试 assistant / developer / chat 分类。
func (p *plugin) fetchModels(ctx context.Context, cred *qoderCred) ([]qoderModel, error) {
	sess, err := p.sessionFor(cred)
	if err != nil {
		return nil, err
	}
	rawURL := endpointsFor(cred.Region).ModelList
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	if err := sess.ApplyHeaders(req, headerCfg(), "", cred.UID, ""); err != nil {
		return nil, err
	}
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("模型目录 HTTP %d: %s", resp.StatusCode, clip(string(raw), 300))
	}
	var top map[string]interface{}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("模型目录解析失败: %w", err)
	}
	for _, cat := range []string{"assistant", "developer", "chat"} {
		if out := parseModels(top[cat]); len(out) > 0 {
			return out, nil
		}
	}
	return nil, fmt.Errorf("上游模型目录为空")
}

func parseModels(raw interface{}) []qoderModel {
	list, _ := raw.([]interface{})
	out := make([]qoderModel, 0, len(list))
	for _, it := range list {
		b, _ := json.Marshal(it)
		var m qoderModel
		if json.Unmarshal(b, &m) == nil && m.Enable && m.Key != "" {
			out = append(out, m)
		}
	}
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

func ftoa(f float64) string {
	return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.2f", f), "00"), ".")
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
