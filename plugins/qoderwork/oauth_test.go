package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

// TestAuthURLTargetsQwenWork 授权链接必须指向千问办公（qwenworkcn）的网关，
// 而不是 Qoder：域名、client_id、redirect_uri 三者错一个都拿不到千问办公的令牌。
func TestAuthURLTargetsQwenWork(t *testing.T) {
	p := &plugin{}
	sess := &loginSession{
		Verifier:  strings.Repeat("a", 64),
		Nonce:     "11111111-2222-4333-8444-555555555555",
		MachineID: "66666666-7777-4888-8999-aaaaaaaaaaaa",
	}
	raw := p.authURL(sess)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("authURL 不是合法 URL: %v", err)
	}
	if u.Scheme != "https" || u.Host != "gateway.qwenwork.cn" {
		t.Fatalf("授权链接应指向 https://gateway.qwenwork.cn，实际 %s://%s", u.Scheme, u.Host)
	}
	if u.Path != "/device/selectAccounts" {
		t.Fatalf("授权路径应为 /device/selectAccounts，实际 %s", u.Path)
	}
	q := u.Query()
	if got := q.Get("client_id"); got != "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb" {
		t.Fatalf("client_id 应为千问办公的 QWENWORK_CN_CLIENT_ID，实际 %s", got)
	}
	if got := q.Get("redirect_uri"); got != "qwenwork-cn://" {
		t.Fatalf("redirect_uri 应为 qwenwork-cn://，实际 %s", got)
	}
	if got := q.Get("challenge_method"); got != "S256" {
		t.Fatalf("challenge_method 应为 S256，实际 %s", got)
	}
	if got := q.Get("nonce"); got != sess.Nonce {
		t.Fatalf("nonce 未原样透传，实际 %s", got)
	}
	if got := q.Get("machine_id"); got != sess.MachineID {
		t.Fatalf("machine_id 未原样透传，实际 %s", got)
	}
	if l := len(q.Get("challenge")); l != 43 {
		t.Fatalf("challenge 应为 43 字符 base64url（sha256），实际长度 %d", l)
	}
}

// TestLoginSessionNonceIsUUID 回归：服务端要求 nonce / machine_id 是标准 UUID。
// 早期用 32 位十六进制串，会被 /device/selectAccounts 判为 400
// INVALID_DEVICE_FLOW（details.field=query, reason=not_allowed）。
func TestLoginSessionNonceIsUUID(t *testing.T) {
	sess := newLoginSession()
	isUUID := func(s string) bool {
		if len(s) != 36 {
			return false
		}
		for i, r := range s {
			switch i {
			case 8, 13, 18, 23:
				if r != '-' {
					return false
				}
			default:
				if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
					return false
				}
			}
		}
		return true
	}
	if !isUUID(sess.Nonce) {
		t.Fatalf("nonce 必须是 UUID（36 位带横线），实际 %q", sess.Nonce)
	}
	if !isUUID(sess.MachineID) {
		t.Fatalf("machine_id 必须是 UUID（36 位带横线），实际 %q", sess.MachineID)
	}
	if len(sess.Verifier) < 43 {
		t.Fatalf("PKCE verifier 太短: %d", len(sess.Verifier))
	}
}

// TestLiveDeviceFlowAgainstQwenWork 真连千问办公网关：
// 校验通过时 /device/selectAccounts 会 302 到 qwenwork.cn 的 IAM 授权页。
// 默认跳过（CI 不保证出网），本地用 CPH_LIVE_TEST=1 跑。
func TestLiveDeviceFlowAgainstQwenWork(t *testing.T) {
	if os.Getenv("CPH_LIVE_TEST") != "1" {
		t.Skip("设置 CPH_LIVE_TEST=1 才跑真实网关校验")
	}
	p := &plugin{}
	sess := newLoginSession()
	req, err := http.NewRequestWithContext(context.Background(), "GET", p.authURL(sess), nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("请求千问办公授权网关失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("期望 302（跳转千问办公 IAM），实际 HTTP %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasPrefix(loc, "https://qwenwork.cn/oauth2/auth") {
		t.Fatalf("302 目标应是千问办公 IAM，实际 %s", loc)
	}
	if !strings.Contains(loc, "client_id=qwenwork-desktop-app") {
		t.Fatalf("IAM 回调里应带 client_id=qwenwork-desktop-app，实际 %s", loc)
	}
	t.Logf("授权跳转正常: %s", loc)
}