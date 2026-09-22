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

// QoderWork（CN）设备授权（PKCE）常量——与官方桌面端一致。
const (
	oauthWebsiteCN = "https://qoder.com.cn"
	oauthOpenapiCN = "https://openapi.qoder.com.cn"
	oauthClientID  = "1c5e33e1-364d-4ce6-b02c-acaa81274a5c"
	oauthRedirect  = "qoder-work-cn://"
)

// loginSession 一次进行中的设备授权。
type loginSession struct {
	Verifier  string    `json:"verifier"`
	Nonce     string    `json:"nonce"`
	MachineID string    `json:"machine_id"`
	CreatedAt time.Time `json:"created_at"`
}

// loginOAuth 设备授权：首次返回授权链接（前端轮询），后续步轮询上游取令牌。
//
// callback 声明为 auto：插件自己轮询上游，不需要用户回粘贴任何东西——
// 服务器部署（域名访问）也能完成，不会退化成手动粘贴回调。
func (p *plugin) loginOAuth(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	state := map[string]string{}
	if len(req.State) > 0 {
		_ = json.Unmarshal(req.State, &state)
	}
	loginID := state["login_id"]

	// 首次：建会话 + 返回授权链接
	if loginID == "" {
		sess := newLoginSession()
		loginID = randomID(16)
		p.putLogin(loginID, sess)
		return &pb.LoginResult{Next: p.waitStep(loginID, authURL(sess))}, nil
	}

	sess := p.getLogin(loginID)
	if sess == nil || time.Since(sess.CreatedAt) > 15*time.Minute {
		// 会话过期：重新开始（前端会拿到新链接继续轮询）
		fresh := newLoginSession()
		loginID = randomID(16)
		p.putLogin(loginID, fresh)
		return &pb.LoginResult{Next: p.waitStep(loginID, authURL(fresh))}, nil
	}

	token, pending, err := pollDeviceToken(ctx, p.httpClient(nil), sess)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "轮询授权结果失败: "+err.Error())
	}
	if pending {
		// 用户还没在浏览器里完成授权：保持轮询
		return &pb.LoginResult{Next: p.waitStep(loginID, authURL(sess))}, nil
	}

	cred := &accountCred{
		DT:        token.DeviceToken,
		DRT:       token.RefreshToken,
		UID:       token.UserID,
		Region:    regionCN,
		ExpiresAt: expiryFromNow(token.ExpiresIn),
	}
	if cred.DT == "" {
		return nil, status.Error(codes.Internal, "上游授权成功但没有返回设备令牌")
	}
	if err := p.fillFingerprint(cred); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// 昵称：userinfo 顺带校验令牌
	if name, uid, err := fetchUserInfo(ctx, p.httpClient(cred), cred.DT); err == nil {
		if name != "" {
			cred.Nickname = name
		}
		if cred.UID == "" {
			cred.UID = uid
		}
	}
	p.deleteLogin(loginID)
	return &pb.LoginResult{Blob: marshalCred(cred), Profile: p.profileFor(ctx, cred)}, nil
}

func (p *plugin) waitStep(loginID, url string) *pb.LoginNextStep {
	st, _ := json.Marshal(map[string]string{"login_id": loginID})
	return &pb.LoginNextStep{
		Action: "open_url", Url: url,
		Prompt: map[string]string{
			"zh": "已在浏览器打开 QoderWork 授权页：登录并确认授权后，本页会自动完成（无需粘贴任何回调）",
			"en": "QoderWork auth page opened in your browser. After you approve, this step completes automatically.",
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
	// 顺手清理 15 分钟前的僵尸会话
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

// newLoginSession 生成 PKCE 参数与稳定机器码（授权链接里要带 machine_id）。
func newLoginSession() *loginSession {
	verifier, _ := makePKCE()
	return &loginSession{
		Verifier:  verifier,
		Nonce:     randomID(32),
		MachineID: randomUUID(),
		CreatedAt: time.Now(),
	}
}

// authURL 授权页（challenge=S256(verifier)，redirect_uri 与官方客户端一致）。
func authURL(s *loginSession) string {
	sum := sha256.Sum256([]byte(s.Verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	q := url.Values{}
	q.Set("challenge", challenge)
	q.Set("challenge_method", "S256")
	q.Set("nonce", s.Nonce)
	q.Set("machine_id", s.MachineID)
	q.Set("client_id", oauthClientID)
	q.Set("redirect_uri", oauthRedirect)
	return oauthWebsiteCN + "/device/selectAccounts?" + q.Encode()
}

type deviceToken struct {
	DeviceToken  string
	RefreshToken string
	UserID       string
	ExpiresIn    int64 // 毫秒
}

// pollDeviceToken 轮询一次；pending=true 表示用户尚未完成授权（上游 404）。
func pollDeviceToken(ctx context.Context, client *http.Client, s *loginSession) (deviceToken, bool, error) {
	q := url.Values{}
	q.Set("nonce", s.Nonce)
	q.Set("verifier", s.Verifier)
	q.Set("challenge_method", "S256")
	req, err := http.NewRequestWithContext(ctx, "GET", oauthOpenapiCN+"/api/v1/deviceToken/poll?"+q.Encode(), nil)
	if err != nil {
		return deviceToken{}, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Go-http-client/2.0")
	resp, err := client.Do(req)
	if err != nil {
		return deviceToken{}, false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == 404 {
		return deviceToken{}, true, nil // 还没授权
	}
	if resp.StatusCode >= 400 {
		return deviceToken{}, false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return deviceToken{}, false, err
	}
	tok := deviceToken{
		DeviceToken:  firstNonEmpty(str(m["token"]), str(m["device_token"])),
		RefreshToken: str(m["refresh_token"]),
		UserID:       firstNonEmpty(str(m["user_id"]), str(m["userId"])),
	}
	if f, ok := m["expires_in"].(float64); ok {
		tok.ExpiresIn = int64(f)
	}
	if tok.DeviceToken == "" {
		return deviceToken{}, true, nil // 授权中了但令牌还没生成
	}
	return tok, false, nil
}

// expiryFromNow 把 expires_in（毫秒）换算成绝对过期时间；0 表示未知。
func expiryFromNow(ms int64) int64 {
	if ms <= 0 {
		return 0
	}
	return time.Now().Unix() + ms/1000
}

// makePKCE 生成 64 字符 verifier（与官方客户端同一取模采样，勿"优化"）。
func makePKCE() (string, string) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
	buf := make([]byte, 64)
	_, _ = rand.Read(buf)
	var sb strings.Builder
	sb.Grow(64)
	for _, b := range buf {
		sb.WriteByte(alphabet[int(b)%len(alphabet)])
	}
	verifier := sb.String()
	sum := sha256.Sum256([]byte(verifier))
	return verifier, base64.RawURLEncoding.EncodeToString(sum[:])
}

// randomID 随机十六进制串。
func randomID(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)[:n]
}

// randomUUID 随机 UUID v4。
func randomUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
