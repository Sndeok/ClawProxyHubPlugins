package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

// 线上联调：默认跳过，CPH_JOYCODE_LIVE=1 开启。
// 不依赖真实账号——用一段假 ptKey 打真实网关，验证三件事：
//  1. 网关/直连地址可达（不是 DNS/网络问题）
//  2. HMAC 签名与 query 构造被上游接受（若签名错，返回签名类错误而非鉴权类错误）
//  3. 响应体是预期 JSON 信封（code/msg 字段存在）
func requireJoyLive(t *testing.T) {
	t.Helper()
	if os.Getenv("CPH_JOYCODE_LIVE") != "1" {
		t.Skip("设置 CPH_JOYCODE_LIVE=1 才跑线上联调")
	}
}

func liveProbe(t *testing.T, p *plugin, cred *joyCred, endpoint, label string) map[string]interface{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := p.joyPost(ctx, cred, endpoint, map[string]interface{}{})
	if err != nil {
		t.Fatalf("%s 请求失败: %v", label, err)
	}
	raw, _ := json.Marshal(resp)
	t.Logf("%s 原始响应: %s", label, clipForLog(string(raw), 600))
	return resp
}

func clipForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func TestLiveJoyGatewayUserInfo(t *testing.T) {
	requireJoyLive(t)
	p := &plugin{}
	cred := &joyCred{PtKey: "AAHtbGciOiJIUzI1NiJ9fake-token-for-probe", UserID: "0", ColorBaseURL: "https://api-ai.jd.com"}
	resp := liveProbe(t, p, cred, joyEpUserInfo, "color gateway userInfo（假凭据）")
	if _, ok := resp["code"]; !ok {
		t.Errorf("响应缺少 code 字段，可能不是 JoyCode 业务响应: %+v", resp)
	}
}

func TestLiveJoyDirectV2UserInfo(t *testing.T) {
	requireJoyLive(t)
	p := &plugin{}
	cred := &joyCred{PtKey: "AAHtbGciOiJIUzI1NiJ9fake-token-for-probe", UserID: "0", ColorBaseURL: joyColorDisabled, MasterBaseURL: joyBaseURL}
	resp := liveProbe(t, p, cred, joyEpUserInfo, "直连 v2 userInfo（假凭据）")
	if _, ok := resp["code"]; !ok {
		t.Errorf("响应缺少 code 字段: %+v", resp)
	}
}

// 京东扫码登录二维码接口（无需凭据）：验证二维码能申请到 + token cookie 能取到。
func TestLiveJoyQRCodeEndpoint(t *testing.T) {
	requireJoyLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET",
		"https://qr.m.jd.com/show?appid=133&size=147&t="+strconv.FormatInt(time.Now().UnixMilli(), 10), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://passport.jd.com/new/login.aspx")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("二维码接口请求失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("二维码接口 HTTP %d", resp.StatusCode)
	}
	var hasToken bool
	for _, c := range resp.Cookies() {
		if c.Name == "wlfstk_smdl" && c.Value != "" {
			hasToken = true
		}
	}
	if !hasToken {
		t.Errorf("未拿到 wlfstk_smdl cookie（扫码链路前提）: %v", resp.Cookies())
	}
	t.Logf("二维码接口可达，wlfstk_smdl=%v", hasToken)
}
