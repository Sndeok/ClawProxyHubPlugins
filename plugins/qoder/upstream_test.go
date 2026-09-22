package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// TestEndpointsForRegions 两区端点表完整性（手抄易错，这里钉住）。
func TestEndpointsForRegions(t *testing.T) {
	g := endpointsFor(regionGlobal)
	c := endpointsFor(regionCN)
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"global userinfo", g.Userinfo, "https://openapi.qoder.sh/api/v1/userinfo"},
		{"global quota", g.Quota, "https://openapi.qoder.sh/api/v2/quota/usage"},
		{"global chat", g.ChatStream, "https://api1.qoder.sh/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1"},
		{"global models", g.ModelList, "https://api2.qoder.sh/algo/api/v2/model/list?Encode=1"},
		{"global jobToken", g.JobToken, "https://center.qoder.sh/algo/api/v3/user/jobToken?Encode=1"},
		{"cn userinfo", c.Userinfo, "https://openapi.qoder.com.cn/api/v1/userinfo"},
		{"cn chat", c.ChatStream, "https://gateway.qoder.com.cn/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1"},
		{"cn models", c.ModelList, "https://gateway.qoder.com.cn/algo/api/v2/model/list?Encode=1"},
		{"cn jobToken", c.JobToken, "https://gateway.qoder.com.cn/algo/api/v3/user/jobToken?Encode=1"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q，want %q", tc.name, tc.got, tc.want)
		}
	}
	// 每个端点都必须能被签名逻辑解析出 path（否则请求头签名会错）
	for _, ep := range []endpoints{g, c} {
		for _, raw := range []string{ep.ChatStream, ep.ModelList, ep.JobToken} {
			path, err := qodersign.PathForSignature(raw)
			if err != nil || path == "" {
				t.Errorf("端点 %q 无法取签名路径: %v", raw, err)
			}
			if strings.HasPrefix(path, "/algo") {
				t.Errorf("签名路径不应带 /algo 前缀: %q", path)
			}
		}
	}
}

// TestBuildQoderBody 请求体骨架与省 token 行为（不注入模板 system/tools）。
func TestBuildQoderBody(t *testing.T) {
	chatBody := map[string]interface{}{
		"model": "qmodel_38max",
		"messages": []map[string]interface{}{
			{"role": "system", "content": "你是助手"},
			{"role": "user", "content": "写一个二分查找"},
		},
	}
	raw := buildQoderBody(chatBody, "qmodel_38max", &qoderCred{UserType: "personal_pro"}, 8192)
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v", err)
	}
	required := []string{
		"request_id", "request_set_id", "chat_record_id", "stream", "chat_task", "chat_context",
		"image_urls", "is_reply", "is_retry", "session_id", "code_language", "source", "version",
		"chat_prompt", "parameters", "aliyun_user_type", "session_type", "agent_id", "task_id",
		"model_config", "messages", "business",
	}
	for _, k := range required {
		if _, ok := body[k]; !ok {
			t.Errorf("请求体缺少 %s", k)
		}
	}
	if body["session_type"] != "qoder" {
		t.Errorf("session_type = %v，want qoder", body["session_type"])
	}
	if body["aliyun_user_type"] != "personal_pro" {
		t.Errorf("aliyun_user_type 未用账号类型: %v", body["aliyun_user_type"])
	}
	if params, _ := body["parameters"].(map[string]interface{}); params["max_tokens"] != float64(8192) {
		t.Errorf("max_tokens 未透传: %v", body["parameters"])
	}
	mc, _ := body["model_config"].(map[string]interface{})
	if mc["key"] != "qmodel_38max" || mc["format"] != "openai" || mc["source"] != "system" {
		t.Errorf("model_config 字段错误: %v", mc)
	}
	if _, ok := body["tools"]; ok {
		t.Error("客户端未传 tools 时不应注入模板工具定义（否则每次多烧 ~10K token）")
	}
	ctx, _ := body["chat_context"].(map[string]interface{})
	txt, _ := ctx["text"].(map[string]interface{})
	if txt["text"] != "写一个二分查找" {
		t.Errorf("chat_context.text 取错: %v", txt["text"])
	}
	biz, _ := body["business"].(map[string]interface{})
	if biz["product"] != "ide" || biz["stage"] != "start" {
		t.Errorf("business 静态字段错误: %v", biz)
	}
}

// TestBuildQoderBodyDefaultMaxTokens 未指定 max_tokens 时用模板默认 32768。
func TestBuildQoderBodyDefaultMaxTokens(t *testing.T) {
	raw := buildQoderBody(map[string]interface{}{"messages": []map[string]interface{}{{"role": "user", "content": "hi"}}}, "auto", &qoderCred{}, 0)
	var body map[string]interface{}
	_ = json.Unmarshal(raw, &body)
	params, _ := body["parameters"].(map[string]interface{})
	if params["max_tokens"] != float64(32768) {
		t.Errorf("默认 max_tokens = %v，want 32768", params["max_tokens"])
	}
}

// TestParseTokenInputQoder PAT / dt / JSON 三种形态。
func TestParseTokenInputQoder(t *testing.T) {
	if tok, ref := parseTokenInput("pt-personal-token"); tok != "pt-personal-token" || ref != "" {
		t.Errorf("PAT 解析错误: %q %q", tok, ref)
	}
	if tok, ref := parseTokenInput("dt-abc\ndrt-xyz"); tok != "dt-abc" || ref != "drt-xyz" {
		t.Errorf("dt/drt 解析错误: %q %q", tok, ref)
	}
	tok, ref := parseTokenInput(`{"token":"dt-a","refresh_token":"drt-b"}`)
	if tok != "dt-a" || ref != "drt-b" {
		t.Errorf("JSON 解析错误: %q %q", tok, ref)
	}
	tok, ref = parseTokenInput(`{"auth":{"accessToken":"dt-n","refreshToken":"drt-n"}}`)
	if tok != "dt-n" || ref != "drt-n" {
		t.Errorf("嵌套 JSON 解析错误: %q %q", tok, ref)
	}
}

// TestParseModels 只保留启用且带 key 的模型。
func TestParseModels(t *testing.T) {
	raw := []interface{}{
		map[string]interface{}{"key": "auto", "display_name": "Auto", "enable": true, "is_default": true, "context_window": 180000, "price_factor": 1},
		map[string]interface{}{"key": "disabled", "enable": false},
		map[string]interface{}{"key": "", "enable": true},
		map[string]interface{}{"key": "qmodel_38max", "display_name": "Performance", "enable": true, "is_reasoning": true, "max_input_tokens": 200000, "price_factor": 2.5},
	}
	out := parseModels(raw)
	if len(out) != 2 {
		t.Fatalf("解析出 %d 个模型，want 2: %+v", len(out), out)
	}
	if out[0].Key != "auto" || !out[0].IsDefault || out[0].Context() != 180000 {
		t.Errorf("auto 模型字段错误: %+v", out[0])
	}
	if out[1].Context() != 200000 || out[1].PriceFactor != 2.5 || !out[1].IsReasoning {
		t.Errorf("付费模型字段错误: %+v", out[1])
	}
}

// TestQuotaCreditsJSON 额度快照（核心按此渲染积分与到期）。
func TestQuotaCreditsJSON(t *testing.T) {
	q := &quotaInfo{UserTotal: 1000, UserUsed: 250, UserRemain: 750, AddonTotal: 500, AddonUsed: 0, AddonRemain: 500,
		ResetTime: "2026-10-01T00:00:00Z", ExpiresAt: 1790000000}
	var snap map[string]interface{}
	if err := json.Unmarshal([]byte(q.CreditsJSON()), &snap); err != nil {
		t.Fatalf("积分快照不合法: %v", err)
	}
	if snap["remaining"] != "1250" || snap["total"] != "1500" || snap["used"] != "250" {
		t.Errorf("聚合错误: %v", snap)
	}
	if snap["reset_time"] != "2026-10-01T00:00:00Z" || snap["expires_at"] != float64(1790000000) {
		t.Errorf("重置/到期字段缺失: %v", snap)
	}
}

// TestNormalizeRegion 区域归一化（未知值一律 global）。
func TestNormalizeRegion(t *testing.T) {
	cases := map[string]string{"cn": regionCN, "CN": regionCN, " global ": regionGlobal, "": regionGlobal, "us": regionGlobal}
	for in, want := range cases {
		if got := normalizeRegion(in); got != want {
			t.Errorf("normalizeRegion(%q) = %q, want %q", in, got, want)
		}
	}
}
