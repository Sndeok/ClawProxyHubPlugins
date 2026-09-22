package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// TestBuildClineBody 请求体：必须剥掉 max_tokens、强制 stream、带上会话与推理强度。
func TestBuildClineBody(t *testing.T) {
	chatBody := map[string]interface{}{
		"model":      "cline-free/deepseek-v4.1-flash",
		"max_tokens": 64000, // 上游免费通道带这个字段会 500
		"messages": []map[string]interface{}{
			{"role": "system", "content": "你是助手"},
			{"role": "user", "content": "写个快排"},
		},
		"tools":       []interface{}{map[string]interface{}{"type": "function"}},
		"temperature": 0.3,
		"top_p":       0.9,
	}
	raw := buildClineBody(chatBody, "cline-free/deepseek-v4.1-flash", "sess_abc", "high")
	body := map[string]interface{}{}
	if err := json.Unmarshal(mustJSON(t, raw), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["max_tokens"]; ok {
		t.Error("max_tokens 必须被剥掉（上游免费通道会 500 empty response content）")
	}
	if body["stream"] != true {
		t.Error("免费通道必须 stream=true")
	}
	if body["session_id"] != "sess_abc" {
		t.Errorf("session_id = %v", body["session_id"])
	}
	if body["reasoning_effort"] != "high" {
		t.Errorf("reasoning_effort = %v", body["reasoning_effort"])
	}
	if _, ok := body["tools"]; !ok {
		t.Error("tools 应透传")
	}
	if body["temperature"] != 0.3 || body["top_p"] != 0.9 {
		t.Errorf("采样参数未透传: %v / %v", body["temperature"], body["top_p"])
	}
	if msgs, _ := body["messages"].([]interface{}); len(msgs) != 2 {
		t.Errorf("messages 未透传: %v", body["messages"])
	}
}

// TestSessionIDStablePerConversation 同一会话多轮 → 同一 session_id；不同会话 → 不同。
func TestSessionIDStablePerConversation(t *testing.T) {
	first := chatReq("user", "第一问")
	second := chatReq("user", "第一问", "assistant", "第一答", "user", "第二问")
	if sessionIDFor(first) != sessionIDFor(second) {
		t.Errorf("同一会话应派生相同 session_id: %s vs %s", sessionIDFor(first), sessionIDFor(second))
	}
	other := chatReq("user", "完全不同的开场")
	if sessionIDFor(first) == sessionIDFor(other) {
		t.Error("不同会话不应共用 session_id")
	}
	if !strings.HasPrefix(sessionIDFor(first), "sess_") {
		t.Errorf("session_id 前缀错误: %s", sessionIDFor(first))
	}
}

// TestClineHeaders F 指纹头必须齐全（缺任一个上游 403）。
func TestClineHeaders(t *testing.T) {
	p := &plugin{}
	h := p.clineHeaders(&clineCred{AccessToken: "tok"}, "sess_1")
	required := []string{"Authorization", "User-Agent", "HTTP-Referer", "X-Title",
		"X-IS-MULTIROOT", "X-CLIENT-TYPE", "X-CLIENT-VERSION", "X-PLATFORM", "X-PLATFORM-VERSION", "X-Task-ID"}
	for _, k := range required {
		if h[k] == "" {
			t.Errorf("缺少指纹头 %s", k)
		}
	}
	if h["Authorization"] != "Bearer workos:tok" {
		t.Errorf("Authorization 必须带 workos: 前缀，实际 %q", h["Authorization"])
	}
	if h["X-CLIENT-VERSION"] != defaultClientVer || h["User-Agent"] != "Cline/"+defaultClientVer {
		t.Errorf("版本头默认值不对: %v / %v", h["X-CLIENT-VERSION"], h["User-Agent"])
	}
	if h["X-CLIENT-TYPE"] != "cline-sdk" {
		t.Errorf("X-CLIENT-TYPE = %v", h["X-CLIENT-TYPE"])
	}
}

// TestIsFreeLane 免费通道判定（决定是否串行保护）。
func TestIsFreeLane(t *testing.T) {
	free := []string{"deepseek/deepseek-v4-flash", "cline-free/deepseek-v4.1-flash",
		"cline-pass/glm-5.3", "cline-cloud/kimi-k3", "poolside/laguna-s-2.1:free", "z-ai/glm-5.2:free"}
	for _, m := range free {
		if !isFreeLane(m) {
			t.Errorf("%s 应判定为免费通道", m)
		}
	}
	if isFreeLane("anthropic/claude-opus-5") {
		t.Error("付费模型不应判定为免费通道")
	}
}

// TestUnwrapReader 上游 {"data":{...}} 包装必须被剥掉，标准 chunk 原样通过。
func TestUnwrapReader(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"data":{"id":"x","choices":[{"delta":{"content":"你"}}]}}`,
		"",
		`data: {"choices":[{"delta":{"content":"好"}}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	out, err := io.ReadAll(unwrapReader(strings.NewReader(stream)))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if strings.Contains(s, `"data":`) {
		t.Errorf("包装未被剥掉: %s", s)
	}
	if !strings.Contains(s, `"content":"你"`) || !strings.Contains(s, `"content":"好"`) {
		t.Errorf("内容丢失: %s", s)
	}
	if !strings.Contains(s, "data: [DONE]") {
		t.Errorf("结束帧丢失: %s", s)
	}
}

// TestParseRefreshToken 三种输入形态。
func TestParseRefreshToken(t *testing.T) {
	cases := map[string]string{
		"rt-abc":                              "rt-abc",
		`{"refreshToken":"rt-json"}`:          "rt-json",
		`{"data":{"refreshToken":"rt-nest"}}`: "rt-nest",
		`refreshToken=rt-eq`:                  "rt-eq",
	}
	for in, want := range cases {
		if got := parseRefreshToken(in); got != want {
			t.Errorf("parseRefreshToken(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestParseExpiry 毫秒 / 秒 / RFC3339。
func TestParseExpiry(t *testing.T) {
	if got := parseExpiry(json.RawMessage("1790000000000")); got != 1790000000000 {
		t.Errorf("毫秒解析: %d", got)
	}
	if got := parseExpiry(json.RawMessage("1790000000")); got != 1790000000000 {
		t.Errorf("秒应换算成毫秒: %d", got)
	}
	if got := parseExpiry(json.RawMessage(`"2026-09-22T06:00:00Z"`)); got == 0 {
		t.Error("RFC3339 解析失败")
	}
}

// TestMapUpstreamStatus 402/403/429 语义映射（决定核心换号还是暂停）。
func TestMapUpstreamStatus(t *testing.T) {
	cases := []struct {
		code int
		body string
		want int32
	}{
		{401, "", 401},
		{402, "", 402},
		{429, "Daily free limit reached", 429},
		{403, "insufficient credits", 402},
		{403, "only available via Cline product surfaces", 401},
		{500, "", 502},
	}
	for _, c := range cases {
		if got := mapUpstreamStatus(c.code, c.body); got != c.want {
			t.Errorf("mapUpstreamStatus(%d, %q) = %d, want %d", c.code, c.body, got, c.want)
		}
	}
}

// TestSerializeGate 串行保护：免费通道两个并发请求必须被串行化且间隔 ≥ min_gap_ms。
func TestSerializeGate(t *testing.T) {
	p := &plugin{}
	ctx := context.Background()
	const gap = 120
	var mu sync.Mutex
	var stamps []int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := p.acquire(ctx, "cline-free/deepseek-v4.1-flash")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			stamps = append(stamps, time.Since(start).Milliseconds())
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			release()
		}()
	}
	wg.Wait()
	if len(stamps) != 2 {
		t.Fatalf("stamps = %v", stamps)
	}
	diff := stamps[1] - stamps[0]
	if diff < gap {
		t.Errorf("两次上游请求间隔 %dms < %dms（未串行保护）", diff, gap)
	}
}

// TestSerializeGateSkipsPaid 付费模型默认不排队（scope=free_only）。
func TestSerializeGateSkipsPaid(t *testing.T) {
	p := &plugin{}
	release, err := p.acquire(context.Background(), "anthropic/claude-opus-5")
	if err != nil {
		t.Fatal(err)
	}
	release() // 直接返回即可，不应阻塞
}

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func chatReq(pairs ...string) *pb.ChatRequest {
	req := &pb.ChatRequest{}
	for i := 0; i+1 < len(pairs); i += 2 {
		req.Messages = append(req.Messages, &pb.EnvelopeMessage{Role: pairs[i], Text: pairs[i+1]})
	}
	return req
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
