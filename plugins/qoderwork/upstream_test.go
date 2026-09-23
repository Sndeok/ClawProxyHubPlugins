package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestBuildAgentBody 校验 QoderWork 请求体骨架：必填字段、客户端内容透传、
// 模板 system/tools 不注入（省 token 的关键行为）。
func TestBuildAgentBody(t *testing.T) {
	chatBody := map[string]interface{}{
		"model": "qmodel_preview",
		"messages": []map[string]interface{}{
			{"role": "system", "content": "你是助手"},
			{"role": "user", "content": "帮我看看这段代码"},
		},
		"tools": []interface{}{map[string]interface{}{"type": "function"}},
	}
	raw := (&plugin{}).buildAgentBody(chatBody, "qmodel_preview", &accountCred{UID: "u1"})
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v", err)
	}
	// 必填骨架
	for _, k := range []string{"request_id", "chat_record_id", "request_set_id", "session_id", "chat_task", "agent_id", "session_type", "model_config", "chat_context", "business"} {
		if _, ok := body[k]; !ok {
			t.Errorf("请求体缺少 %s", k)
		}
	}
	if body["stream"] != true {
		t.Errorf("stream 必须为 true（上游只支持流式）")
	}
	// 对齐客户端：千问办公用 qoder_work，空值回落默认（插件设置可覆盖）
	if body["session_type"] != defaultSessionType {
		t.Errorf("session_type = %v, want %v", body["session_type"], defaultSessionType)
	}
	if mc, ok := body["model_config"].(map[string]interface{}); !ok || mc["key"] != "qmodel_preview" {
		t.Errorf("model_config.key 未按模型写入: %v", body["model_config"])
	}
	msgs, _ := body["messages"].([]interface{})
	if len(msgs) != 2 {
		t.Errorf("客户端消息未完整透传: %d", len(msgs))
	}
	if _, ok := body["tools"]; !ok {
		t.Errorf("客户端 tools 未透传")
	}
	// chat_context.text 取最后一条 user 文本
	ctx, _ := body["chat_context"].(map[string]interface{})
	txt, _ := ctx["text"].(map[string]interface{})
	if txt["text"] != "帮我看看这段代码" {
		t.Errorf("chat_context.text 取错: %v", txt["text"])
	}
	biz, _ := body["business"].(map[string]interface{})
	if biz["name"] != "帮我看看这段代码" {
		t.Errorf("business.name 取错: %v", biz["name"])
	}
}

// TestBuildAgentBodyNoTools 客户端没给 tools 时不应注入（否则会带上模板的 74 个工具定义）。
func TestBuildAgentBodyNoTools(t *testing.T) {
	raw := (&plugin{}).buildAgentBody(map[string]interface{}{
		"messages": []map[string]interface{}{{"role": "user", "content": "hi"}},
	}, "m1", &accountCred{UID: "u1"})
	var body map[string]interface{}
	_ = json.Unmarshal(raw, &body)
	if _, ok := body["tools"]; ok {
		t.Error("客户端未传 tools 时不应出现 tools 字段")
	}
}

// TestParseTokenInput 凭据解析兼容三种输入形态。
func TestParseTokenInput(t *testing.T) {
	dt, drt, uid := parseTokenInput(`{"token":"dt-abc","refresh_token":"drt-xyz","user_id":"u-1"}`)
	if dt != "dt-abc" || drt != "drt-xyz" || uid != "u-1" {
		t.Errorf("扁平 JSON 解析错误: %q %q %q", dt, drt, uid)
	}
	dt, drt, uid = parseTokenInput(`{"auth":{"accessToken":"dt-a","refreshToken":"drt-b"},"account":{"uid":"u-2"}}`)
	if dt != "dt-a" || drt != "drt-b" || uid != "u-2" {
		t.Errorf("嵌套 JSON 解析错误: %q %q %q", dt, drt, uid)
	}
	dt, drt, _ = parseTokenInput("dt-plain\ndrt-plain")
	if dt != "dt-plain" || drt != "drt-plain" {
		t.Errorf("纯令牌解析错误: %q %q", dt, drt)
	}
}

// TestCheckinStatusText 展示文案映射。
func TestCheckinStatusText(t *testing.T) {
	cases := map[string]string{
		"CLAIMABLE":      "今日可领取",
		"CLAIMED":        "今日已领取",
		"CLAIMED_TODAY":  "今日已领取",
		"":               "未查询到",
		"SOMETHING_ELSE": "SOMETHING_ELSE",
	}
	for in, want := range cases {
		if got := checkinStatusText(in); got != want {
			t.Errorf("checkinStatusText(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestQuotaCreditsJSON 额度快照形态（核心按此渲染积分列）。
func TestQuotaCreditsJSON(t *testing.T) {
	q := &quotaInfo{UserTotal: 100, UserUsed: 40, UserRemaining: 60, AddonTotal: 20, AddonRemaining: 20}
	raw := q.CreditsJSON()
	var snap map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatalf("积分快照不是合法 JSON: %v", err)
	}
	if snap["remaining"] != "80" || snap["total"] != "120" || snap["used"] != "40" {
		t.Errorf("积分快照聚合错误: %v", snap)
	}
	if pkgs, _ := snap["packages"].([]interface{}); len(pkgs) != 2 {
		t.Errorf("积分包数量应为 2（订阅 + 赠送）: %v", snap["packages"])
	}
}

// TestLastUserPrompt 多模态 content 数组取文本。
func TestLastUserPrompt(t *testing.T) {
	body := map[string]interface{}{
		"messages": []interface{}{
			map[string]interface{}{"role": "user", "content": "第一问"},
			map[string]interface{}{"role": "assistant", "content": "答"},
			map[string]interface{}{"role": "user", "content": []interface{}{
				map[string]interface{}{"type": "text", "text": "第二问"},
			}},
		},
	}
	if got := lastUserPrompt(body); got != "第二问" {
		t.Errorf("lastUserPrompt = %q", got)
	}
}

// TestSettingsWithoutHost 无宿主注入时不应 panic（插件被单测/离线调用）。
func TestSettingsWithoutHost(t *testing.T) {
	p := &plugin{}
	if got := p.machineSalt(); got != "" {
		t.Errorf("无 host 时会话盐应为空，实际 %q", got)
	}
	if s := p.settings(); len(s) != 0 {
		t.Errorf("无 host 时设置应为空表，实际 %v", s)
	}
}

// TestStripCacheControl 内容块上的 cache_control 必须被剥离（本协议没有该字段），
// 同时文本/图片等内容本体保持不动。
func TestStripCacheControl(t *testing.T) {
	in := []interface{}{
		map[string]interface{}{
			"type": "text", "text": "看图",
			"cache_control": map[string]interface{}{"type": "ephemeral"},
		},
		map[string]interface{}{
			"type": "image_url",
			"image_url": map[string]interface{}{
				"url":           "data:image/png;base64,AA==",
				"cache_control": map[string]interface{}{"type": "ephemeral"},
			},
		},
	}
	out := stripCacheControl(in)
	parts, _ := out.([]interface{})
	if len(parts) != 2 {
		t.Fatalf("块数量被改动: %v", out)
	}
	first, _ := parts[0].(map[string]interface{})
	if _, ok := first["cache_control"]; ok {
		t.Error("顶层 cache_control 未剥离")
	}
	if first["text"] != "看图" || first["type"] != "text" {
		t.Errorf("内容本体被改动: %v", first)
	}
	second, _ := parts[1].(map[string]interface{})
	img, _ := second["image_url"].(map[string]interface{})
	if _, ok := img["cache_control"]; ok {
		t.Error("嵌套 cache_control 未剥离")
	}
	if img["url"] != "data:image/png;base64,AA==" {
		t.Errorf("图片地址被改动: %v", img)
	}
}

// TestFetchWalletsUsesClientEndpoint 钱包余额：客户端同源接口 /api/v1/adapter/user/wallets，
// 三个钱包取 total_balance 求和（与客户端「积分余额」一致），并覆盖 {data:{...}} 包装形态。
func TestFetchWalletsUsesClientEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/adapter/user/wallets" {
			t.Errorf("请求路径 = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer dt-test" {
			t.Errorf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{
		  "daily_credits":{"total_balance":100.5},
		  "monthly_credits":{"totalBalance":1000},
		  "longterm_credits":{"total_balance":"999.5"}
		}}`)
	}))
	defer srv.Close()
	old := openapiBase
	openapiBase = srv.URL
	defer func() { openapiBase = old }()

	w, err := fetchWallets(context.Background(), srv.Client(), "dt-test")
	if err != nil {
		t.Fatalf("fetchWallets: %v", err)
	}
	if got := w.Total(); got != 2100 {
		t.Errorf("钱包合计 = %v，期望 2100", got)
	}
	if !w.HasDaily || !w.HasMonthly || !w.HasLongterm {
		t.Errorf("三个钱包都应解析成功：%+v", w)
	}

	// 快照：有钱包时按钱包出包，remaining/total 都用钱包合计
	q := &quotaInfo{Wallets: w, UserTotal: 300, UserUsed: 1, UserRemaining: 299}
	if q.Remaining() != 2100 || q.Total() != 2100 {
		t.Errorf("quota 聚合未优先用钱包：remaining=%d total=%d", q.Remaining(), q.Total())
	}
	var snap struct {
		Remaining string              `json:"remaining"`
		Packages  []map[string]string `json:"packages"`
	}
	if err := json.Unmarshal([]byte(q.CreditsJSON()), &snap); err != nil {
		t.Fatalf("快照不是合法 JSON: %v", err)
	}
	if snap.Remaining != "2100" {
		t.Errorf("快照 remaining = %s，期望 2100", snap.Remaining)
	}
	if len(snap.Packages) != 3 {
		t.Errorf("应输出 日/月度/长期 三个额度包，实际 %v", snap.Packages)
	}
}
