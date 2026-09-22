package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// Cline 的 WorkOS 设备授权常量（与官方客户端一致）。
// clineAPIBase / clineRegisterURL / clineRefreshURL 走 var 而非 const：
// 单测用 httptest 覆盖它们做模型目录与凭据链路的回归。
var (
	clineAPIBase     = "https://api.cline.bot/api/v1"
	clineRegisterURL = clineAPIBase + "/auth/register"
	clineRefreshURL  = clineAPIBase + "/auth/refresh"
)

const (
	workosClientID   = "client_01K3A541FN8TA3EPPHTD2325AR"
	workosDeviceURL  = "https://api.workos.com/user_management/authorize/device"
	workosAuthURL    = "https://api.workos.com/user_management/authenticate"
	deviceGrantType  = "urn:ietf:params:oauth:grant-type:device_code"
	defaultDeviceTTL = 300 * time.Second
)

// loginSession 一次进行中的 WorkOS 设备授权。
type loginSession struct {
	DeviceCode string    `json:"device_code"`
	Interval   int       `json:"interval"`
	ExpiresAt  time.Time `json:"expires_at"`
	AuthURL    string    `json:"auth_url"`
	UserCode   string    `json:"user_code"`
	CreatedAt  time.Time `json:"created_at"`
}

// loginOAuth 设备授权：首次返回授权链接（前端轮询），后续步轮询 WorkOS 换令牌。
func (p *plugin) loginOAuth(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	state := map[string]string{}
	if len(req.State) > 0 {
		_ = json.Unmarshal(req.State, &state)
	}
	loginID := state["login_id"]

	// 首次：向 WorkOS 申请设备码
	if loginID == "" {
		sess, err := startDeviceAuth(ctx, p.httpClient(nil))
		if err != nil {
			return nil, status.Error(codes.Unavailable, "申请设备授权失败: "+err.Error())
		}
		loginID = randomHexID(16)
		p.putLogin(loginID, sess)
		return &pb.LoginResult{Next: p.waitStep(loginID, sess)}, nil
	}

	sess := p.getLogin(loginID)
	if sess == nil || time.Now().After(sess.ExpiresAt) {
		// 设备码过期：重新申请（前端会拿到新链接继续轮询）
		fresh, err := startDeviceAuth(ctx, p.httpClient(nil))
		if err != nil {
			return nil, status.Error(codes.Unavailable, "设备码已过期，请重新授权: "+err.Error())
		}
		loginID = randomHexID(16)
		p.putLogin(loginID, fresh)
		return &pb.LoginResult{Next: p.waitStep(loginID, fresh)}, nil
	}

	workos, pending, err := pollWorkOS(ctx, p.httpClient(nil), sess)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "轮询授权结果失败: "+err.Error())
	}
	if pending {
		return &pb.LoginResult{Next: p.waitStep(loginID, sess)}, nil
	}

	// 用 WorkOS token 在 Cline 注册，换 Cline refreshToken
	cred, err := registerCline(ctx, p.httpClient(nil), workos)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "Cline 注册失败: "+err.Error())
	}
	p.deleteLogin(loginID)
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

func (p *plugin) waitStep(loginID string, sess *loginSession) *pb.LoginNextStep {
	st, _ := json.Marshal(map[string]string{"login_id": loginID})
	return &pb.LoginNextStep{
		Action: "open_url", Url: sess.AuthURL,
		Prompt: map[string]string{
			"zh": fmt.Sprintf("已生成 Cline 授权链接（设备码 %s）：浏览器打开并登录授权后，本页面会自动完成，无需粘贴回调", sess.UserCode),
			"en": fmt.Sprintf("Cline auth link ready (code %s). Approve in browser; this page completes automatically.", sess.UserCode),
		},
		State: st,
		Wait:  true,
	}
}

func (p *plugin) putLogin(id string, s *loginSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logins == nil {
		p.logins = map[string]*loginSession{}
	}
	for k, v := range p.logins {
		if time.Since(v.CreatedAt) > 20*time.Minute {
			delete(p.logins, k)
		}
	}
	p.logins[id] = s
}

func (p *plugin) getLogin(id string) *loginSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logins[id]
}

func (p *plugin) deleteLogin(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.logins, id)
}

// startDeviceAuth 申请设备码（form 编码，非 JSON）。
func startDeviceAuth(ctx context.Context, client *http.Client) (*loginSession, error) {
	form := url.Values{}
	form.Set("client_id", workosClientID)
	raw, err := postForm(ctx, client, workosDeviceURL, form)
	if err != nil {
		return nil, err
	}
	var resp struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURI         string `json:"verification_uri"`
		VerificationURIComplete string `json:"verification_uri_complete"`
		Interval                int    `json:"interval"`
		ExpiresIn               int    `json:"expires_in"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("设备授权响应解析失败: %w", err)
	}
	if resp.DeviceCode == "" {
		return nil, fmt.Errorf("设备授权响应缺少 device_code: %s", clip(string(raw), 200))
	}
	authURL := orDefault(resp.VerificationURIComplete, resp.VerificationURI)
	if authURL == "" {
		return nil, fmt.Errorf("设备授权响应缺少授权链接")
	}
	ttl := defaultDeviceTTL
	if resp.ExpiresIn > 0 {
		ttl = time.Duration(resp.ExpiresIn) * time.Second
	}
	interval := resp.Interval
	if interval < 5 {
		interval = 5
	}
	return &loginSession{
		DeviceCode: resp.DeviceCode,
		Interval:   interval,
		ExpiresAt:  time.Now().Add(ttl),
		AuthURL:    authURL,
		UserCode:   resp.UserCode,
		CreatedAt:  time.Now(),
	}, nil
}

type workosTokens struct {
	AccessToken  string
	RefreshToken string
}

// pollWorkOS 轮询一次；pending=true 表示用户还没授权。
func pollWorkOS(ctx context.Context, client *http.Client, sess *loginSession) (workosTokens, bool, error) {
	form := url.Values{}
	form.Set("grant_type", deviceGrantType)
	form.Set("device_code", sess.DeviceCode)
	form.Set("client_id", workosClientID)
	raw, err := postForm(ctx, client, workosAuthURL, form)
	if err != nil {
		return workosTokens{}, false, err
	}
	var resp struct {
		AccessToken      string `json:"access_token"`
		RefreshToken     string `json:"refresh_token"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return workosTokens{}, false, fmt.Errorf("授权轮询响应解析失败: %w", err)
	}
	if resp.AccessToken != "" {
		return workosTokens{AccessToken: resp.AccessToken, RefreshToken: resp.RefreshToken}, false, nil
	}
	switch resp.Error {
	case "authorization_pending":
		return workosTokens{}, true, nil
	case "slow_down":
		// 上游要求放慢轮询：抬高本会话间隔
		p := sess
		p.Interval += 5
		return workosTokens{}, true, nil
	case "expired_token", "access_denied":
		return workosTokens{}, false, fmt.Errorf("%s: %s", resp.Error, resp.ErrorDescription)
	default:
		if resp.Error == "" {
			return workosTokens{}, true, nil
		}
		return workosTokens{}, false, fmt.Errorf("%s: %s", resp.Error, resp.ErrorDescription)
	}
}

// registerCline 用 WorkOS token 换 Cline refreshToken（accessToken 也一并带回）。
func registerCline(ctx context.Context, client *http.Client, w workosTokens) (*clineCred, error) {
	body, _ := json.Marshal(map[string]string{
		"accessToken":  w.AccessToken,
		"refreshToken": w.RefreshToken,
	})
	raw, status, err := postJSON(ctx, client, clineRegisterURL, body, nil)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", status, clip(string(raw), 200))
	}
	var resp struct {
		Data struct {
			AccessToken  string          `json:"accessToken"`
			RefreshToken string          `json:"refreshToken"`
			ExpiresAt    json.RawMessage `json:"expiresAt"`
			UserInfo     *struct {
				Email string `json:"email"`
				ID    string `json:"id"`
			} `json:"userInfo"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("注册响应解析失败: %w", err)
	}
	if resp.Data.RefreshToken == "" {
		return nil, fmt.Errorf("注册响应缺少 refreshToken: %s", clip(string(raw), 200))
	}
	cred := &clineCred{
		RefreshToken: resp.Data.RefreshToken,
		AccessToken:  resp.Data.AccessToken,
		ExpiresAt:    parseExpiry(resp.Data.ExpiresAt),
	}
	if resp.Data.UserInfo != nil {
		cred.Email, cred.UserID = resp.Data.UserInfo.Email, resp.Data.UserInfo.ID
	}
	return cred, nil
}

// refreshClineToken 用 refreshToken 换新的 accessToken（上游会轮换 refreshToken）。
func refreshClineToken(ctx context.Context, client *http.Client, cred *clineCred) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"refreshToken": cred.RefreshToken,
		"grantType":    "refresh_token",
	})
	raw, status, err := postJSON(ctx, client, clineRefreshURL, body, nil)
	if err != nil {
		return "", err
	}
	if status >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", status, clip(string(raw), 200))
	}
	var resp struct {
		Data struct {
			AccessToken  string          `json:"accessToken"`
			RefreshToken string          `json:"refreshToken"`
			ExpiresAt    json.RawMessage `json:"expiresAt"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("刷新响应解析失败: %w", err)
	}
	if resp.Data.AccessToken == "" {
		return "", fmt.Errorf("刷新响应缺少 accessToken: %s", clip(string(raw), 200))
	}
	cred.AccessToken = resp.Data.AccessToken
	if resp.Data.RefreshToken != "" {
		cred.RefreshToken = resp.Data.RefreshToken
	}
	cred.ExpiresAt = parseExpiry(resp.Data.ExpiresAt)
	return cred.AccessToken, nil
}

// accessTokenExpiring accessToken 是否缺失或临近过期（提前 2 分钟刷新）。
func accessTokenExpiring(cred *clineCred) bool {
	if cred.AccessToken == "" {
		return true
	}
	if cred.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixMilli() > cred.ExpiresAt-120000
}

// parseExpiry expiresAt 可能是毫秒数、秒数或 RFC3339 字符串。
func parseExpiry(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return 0
	}
	var num float64
	if json.Unmarshal(raw, &num) == nil && num > 0 {
		v := int64(num)
		if v < 1e12 { // 秒 → 毫秒
			v *= 1000
		}
		return v
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UnixMilli()
			}
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			if n < 1e12 {
				n *= 1000
			}
			return n
		}
	}
	return 0
}

// postForm 发送 form 编码请求（WorkOS 设备授权用）。
func postForm(ctx context.Context, client *http.Client, endpoint string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return raw, nil
}

// postJSON 发送 JSON 请求；headers 为空时只设 Content-Type。
func postJSON(ctx context.Context, client *http.Client, endpoint string, body []byte, headers map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(string(body)))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return raw, resp.StatusCode, nil
}

// randRead 读随机字节（登录会话 id 用）。
func randRead(b []byte) (int, error) { return rand.Read(b) }

// randomHexID 随机十六进制串（登录会话 id）。
func randomHexID(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = randRead(b)
	return fmt.Sprintf("%x", b)[:n]
}
