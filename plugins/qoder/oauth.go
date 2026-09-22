package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// Qoder 设备授权 client_id（桌面端硬编码，两区一致）。
const oauthClientID = "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb"

// loginSession 一次进行中的设备授权。
type loginSession struct {
	Verifier  string    `json:"verifier"`
	Nonce     string    `json:"nonce"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
}

// loginOAuth 设备授权：首次返回授权链接（前端轮询），后续步轮询上游换令牌。
// callback 声明为 auto：插件自己在服务端轮询，域名部署也无需粘贴任何回调。
func (p *plugin) loginOAuth(ctx context.Context, req *pb.LoginRequest, region string) (*pb.LoginResult, error) {
	state := map[string]string{}
	if len(req.State) > 0 {
		_ = json.Unmarshal(req.State, &state)
	}
	loginID := state["login_id"]
	// 区域由授权方式决定：换区就重开会话，避免用户拿错区的链接
	if loginID == "" || state["region"] != region {
		sess := newLoginSession(region)
		loginID = randomHexID(16)
		p.putLogin(loginID, sess)
		return &pb.LoginResult{Next: p.waitStep(loginID, region, authURL(sess))}, nil
	}

	sess := p.getLogin(loginID)
	if sess == nil || time.Since(sess.CreatedAt) > 15*time.Minute {
		fresh := newLoginSession(region)
		loginID = randomHexID(16)
		p.putLogin(loginID, fresh)
		return &pb.LoginResult{Next: p.waitStep(loginID, region, authURL(fresh))}, nil
	}

	tok, pending, err := pollDeviceToken(ctx, p.httpClient(nil), endpointsFor(region), sess)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "轮询授权结果失败: "+err.Error())
	}
	if pending {
		return &pb.LoginResult{Next: p.waitStep(loginID, region, authURL(sess))}, nil
	}

	ep := endpointsFor(region)
	cred := &qoderCred{Region: region, AuthMode: "oauth", DT: tok.DeviceToken, DRT: tok.RefreshToken,
		SecToken: tok.DeviceToken, RefToken: tok.RefreshToken}
	if err := p.fillFingerprint(cred); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	info, err := fetchUserInfo(ctx, p.httpClient(cred), ep, cred.DT)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "授权成功但读取用户信息失败: "+err.Error())
	}
	cred.UID, cred.Name = info.UID, info.Name
	cred.UserType, cred.OrgID, cred.OrgName = info.UserType, info.OrganizationID, info.OrganizationName
	p.deleteLogin(loginID)
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

func (p *plugin) waitStep(loginID, region, url string) *pb.LoginNextStep {
	st, _ := json.Marshal(map[string]string{"login_id": loginID, "region": region})
	return &pb.LoginNextStep{
		Action: "open_url", Url: url,
		Prompt: map[string]string{
			"zh": "已在浏览器打开 Qoder 授权页：登录并确认授权后，本页面会自动完成（无需粘贴回调）",
			"en": "Qoder auth page opened. After you approve, this step completes automatically.",
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
		if time.Since(v.CreatedAt) > 15*time.Minute {
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

// newLoginSession 生成 PKCE 参数（verifier = base64url(32 字节随机)）。
func newLoginSession(region string) *loginSession {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	return &loginSession{
		Verifier:  base64.RawURLEncoding.EncodeToString(buf),
		Nonce:     randomHexID(32),
		Region:    region,
		CreatedAt: time.Now(),
	}
}

// authURL 授权页链接（含 S256 challenge）。
func authURL(s *loginSession) string {
	sum := sha256.Sum256([]byte(s.Verifier))
	q := url.Values{}
	q.Set("nonce", s.Nonce)
	q.Set("challenge", base64.RawURLEncoding.EncodeToString(sum[:]))
	q.Set("challenge_method", "S256")
	q.Set("client_id", oauthClientID)
	return endpointsFor(s.Region).DeviceLogin + "?" + q.Encode()
}

type deviceTokenResp struct {
	DeviceToken  string
	RefreshToken string
}

// pollDeviceToken 轮询一次：404/401 = 用户尚未完成授权（pending）。
func pollDeviceToken(ctx context.Context, client *http.Client, ep endpoints, s *loginSession) (deviceTokenResp, bool, error) {
	q := url.Values{}
	q.Set("nonce", s.Nonce)
	q.Set("verifier", s.Verifier)
	q.Set("challenge_method", "S256")
	req, err := http.NewRequestWithContext(ctx, "GET", ep.Poll+"?"+q.Encode(), nil)
	if err != nil {
		return deviceTokenResp{}, false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return deviceTokenResp{}, false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == 404 || resp.StatusCode == 401 {
		return deviceTokenResp{}, true, nil // 还没授权 / 会话尚未生效
	}
	if resp.StatusCode >= 400 {
		return deviceTokenResp{}, false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return deviceTokenResp{}, false, err
	}
	out := deviceTokenResp{
		DeviceToken:  firstNonEmpty(str(m["token"]), str(m["device_token"])),
		RefreshToken: firstNonEmpty(str(m["refresh_token"]), str(m["refreshToken"])),
	}
	if out.DeviceToken == "" {
		return deviceTokenResp{}, true, nil
	}
	return out, false, nil
}

// refreshDeviceToken OAuth 路径：用 drt- 换新 dt-（两区都有该端点）。
func refreshDeviceToken(ctx context.Context, client *http.Client, ep endpoints, c *qoderCred) error {
	body, _ := json.Marshal(map[string]string{"refresh_token": c.DRT})
	base := "https://openapi.qoder.sh"
	if ep.Region == regionCN {
		base = "https://openapi.qoder.com.cn"
	}
	req, err := http.NewRequestWithContext(ctx, "POST", base+"/api/v1/deviceToken/refresh", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
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
	c.DT, c.SecToken = dt, dt
	if t.RefreshToken != "" {
		c.DRT, c.RefToken = t.RefreshToken, t.RefreshToken
	}
	if t.ExpiresIn > 0 {
		c.ExpiresAt = nowUnix() + t.ExpiresIn/1000
	}
	return nil
}

// randomHexID 随机十六进制串（nonce / 登录会话 id）。
func randomHexID(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)[:n]
}

// nowUnix 当前时间戳（秒）。
func nowUnix() int64 { return time.Now().Unix() }

// randomUUID 随机 UUID v4（请求 id / 会话 id）。
func randomUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
