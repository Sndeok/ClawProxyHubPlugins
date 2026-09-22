package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// TestCanonicalSessionShape 匿名通道只接受官方 session 形状（否则 403 FreeTierError）。
func TestCanonicalSessionShape(t *testing.T) {
	pattern := regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$`)
	seed := "写一个二分查找"
	s1 := canonicalSession(seed)
	if !pattern.MatchString(s1) {
		t.Fatalf("session 形状不合官方格式: %q", s1)
	}
	if s1 != canonicalSession(seed) {
		t.Error("同一会话种子必须派生相同 session（缓存亲和）")
	}
	if s1 == canonicalSession("另一段对话") {
		t.Error("不同会话不应共用 session")
	}
}

// TestHeadersPerProtocol Anthropic 方言用 x-api-key，其余用 Bearer；伪装头一个都不能少。
func TestHeadersPerProtocol(t *testing.T) {
	p := &plugin{}
	cred := &openCred{Tier: tierAnon, APIKey: anonKey, UID: "anonymous"}
	required := []string{"x-opencode-client", "x-opencode-session", "x-session-affinity",
		"X-Session-Id", "x-opencode-request", "x-opencode-project", "User-Agent"}

	chat := p.headers(cred, protocolChat, "ses_x", "req_x")
	if chat["Authorization"] != "Bearer "+anonKey {
		t.Errorf("chat 方言应带 Bearer：%v", chat["Authorization"])
	}
	resp := p.headers(cred, protocolResp, "ses_x", "req_x")
	if resp["Authorization"] != "Bearer "+anonKey {
		t.Errorf("responses 方言应带 Bearer：%v", resp["Authorization"])
	}
	msgs := p.headers(cred, protocolMsgs, "ses_x", "req_x")
	if msgs["x-api-key"] != anonKey || msgs["anthropic-version"] == "" {
		t.Errorf("messages 方言应带 x-api-key + anthropic-version：%v", msgs)
	}
	if _, hasBearer := msgs["Authorization"]; hasBearer {
		t.Error("messages 方言不应带 Authorization（会被上游忽略/拒绝）")
	}
	for _, h := range required {
		if chat[h] == "" || msgs[h] == "" || resp[h] == "" {
			t.Errorf("缺少伪装头 %s", h)
		}
	}
	if !strings.HasPrefix(chat["User-Agent"], "opencode/") {
		t.Errorf("User-Agent 应为 opencode/<版本>：%q", chat["User-Agent"])
	}
}

// TestEnsureAnonTools 每个协议都要补齐 5 个核心工具名，且不重复、不动客户端已有工具。
func TestEnsureAnonTools(t *testing.T) {
	cases := []struct {
		protocol string
		existing string
	}{
		{protocolChat, `[{"type":"function","function":{"name":"bash","description":"客户端自带的 bash"}}]`},
		{protocolMsgs, `[{"name":"bash","description":"客户端自带的 bash","input_schema":{"type":"object"}}]`},
		{protocolResp, `[{"type":"function","name":"bash","description":"客户端自带的 bash"}]`},
	}
	for _, c := range cases {
		var tools []interface{}
		if err := json.Unmarshal([]byte(c.existing), &tools); err != nil {
			t.Fatal(err)
		}
		body := map[string]interface{}{"tools": tools}
		ensureAnonTools(body, c.protocol)
		got, _ := body["tools"].([]interface{})
		names := map[string]int{}
		for _, it := range got {
			m, _ := it.(map[string]interface{})
			name := str(m["name"])
			if nested, ok := m["function"].(map[string]interface{}); ok {
				name = str(nested["name"])
			}
			names[name]++
		}
		for _, want := range anonCoreTools {
			if names[want] == 0 {
				t.Errorf("[%s] 缺少核心工具 %s", c.protocol, want)
			}
			if names[want] > 1 {
				t.Errorf("[%s] 工具 %s 重复注入（客户端已带时不应再补）", c.protocol, want)
			}
		}
		if len(names) != len(anonCoreTools) {
			t.Errorf("[%s] 工具数量 = %d, want %d: %v", c.protocol, len(names), len(anonCoreTools), names)
		}
		// 客户端已有工具的描述不能被覆盖
		first, _ := got[0].(map[string]interface{})
		desc := str(first["description"])
		if nested, ok := first["function"].(map[string]interface{}); ok {
			desc = str(nested["description"])
		}
		if !strings.Contains(desc, "客户端自带的 bash") {
			t.Errorf("[%s] 覆盖了客户端已有工具定义: %v", c.protocol, first)
		}
	}
}

// TestDocProtocolParsing 官方文档表的「模型 → 原生协议」解析（走错协议会被上游 500）。
func TestDocProtocolParsing(t *testing.T) {
	md := strings.Join([]string{
		"| Model | ID | Endpoint | SDK |",
		"| --- | --- | --- | --- |",
		"| GPT 5.6 Sol | `gpt-5.6-sol` | `https://opencode.ai/zen/v1/responses` | `@ai-sdk/openai` |",
		"| Claude Sonnet 4.6 | `claude-sonnet-4-6` | `https://opencode.ai/zen/v1/messages` | `@ai-sdk/anthropic` |",
		"| MiniMax M3 | `minimax-m3` | `https://opencode.ai/zen/v1/chat/completions` | `@ai-sdk/openai-compatible` |",
	}, "\n")
	got := map[string]string{}
	for _, line := range strings.Split(md, "\n") {
		if m := docRowPattern.FindStringSubmatch(line); len(m) >= 3 {
			got[m[1]] = m[2]
		}
	}
	if got["gpt-5.6-sol"] != "responses" || got["claude-sonnet-4-6"] != "messages" || got["minimax-m3"] != "chat/completions" {
		t.Fatalf("协议表解析错误: %v", got)
	}
}

// TestStreamGuardDelaysStart 空响应的 MessageStart/Finish 必须被吞掉，
// 只有拿到有效内容后才补发 MessageStart（否则核心不会换号重试）。
func TestStreamGuardDelaysStart(t *testing.T) {
	var sent []*pb.StreamEvent
	g := &streamGuard{send: func(ev *pb.StreamEvent) error { sent = append(sent, ev); return nil }}
	g.emit(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{}})
	if g.started {
		t.Error("没有内容时不应置 started")
	}
	g.emit(&pb.StreamEvent{Event: &pb.StreamEvent_ContentDelta{ContentDelta: &pb.ContentDelta{Text: "hi"}}})
	if !g.started {
		t.Error("首段内容到达后应 started")
	}
	// 首帧必须是被延迟的 MessageStart，然后才是内容
	if len(sent) != 2 {
		t.Fatalf("首发事件数 = %d, want 2（MessageStart + ContentDelta）", len(sent))
	}
	if _, ok := sent[0].Event.(*pb.StreamEvent_MessageStart); !ok {
		t.Errorf("首发事件应为 MessageStart，实际 %T", sent[0].Event)
	}

	// 空响应：只有 start + finish
	g2 := &streamGuard{send: func(ev *pb.StreamEvent) error { return nil }}
	g2.emit(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{}})
	g2.emit(&pb.StreamEvent{Event: &pb.StreamEvent_MessageFinish{MessageFinish: &pb.MessageFinish{}}})
	if g2.started {
		t.Error("空响应不应 started（调用方据此按 429 首事件上报）")
	}
}

// TestEffortPrefersClient 客户端指定的推理强度优先于插件默认。
func TestEffortPrefersClient(t *testing.T) {
	p := &plugin{}
	req := &pb.ChatRequest{Extra: map[string]string{"reasoning_effort": "low"}}
	if got := p.effortFor(req); got != "low" {
		t.Errorf("客户端值未优先: %q", got)
	}
	if got := p.effortFor(&pb.ChatRequest{Extra: map[string]string{}}); got != "" {
		t.Errorf("无配置时不应发 reasoning_effort: %q", got)
	}
}

// 线上联调：默认跳过，CPH_OPENCODE_LIVE=1 开启。
// 用官方「匿名免费通道」实打实跑一次免费模型，验证伪装头 + session 形状 + 协议选路都对。
func TestLiveAnonFreeLane(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过线上联调")
	}
	if strings.TrimSpace(getenvOrEmpty("CPH_OPENCODE_LIVE")) != "1" {
		t.Skip("设置 CPH_OPENCODE_LIVE=1 才跑线上联调")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	p := &plugin{}
	cred := &openCred{Tier: tierAnon, APIKey: anonKey, UID: "anonymous"}

	models, err := p.listModels(ctx, cred)
	if err != nil {
		t.Fatalf("匿名模型目录失败: %v", err)
	}
	if len(models) == 0 {
		t.Fatal("匿名通道没有可见模型")
	}
	var target zenModel
	for _, m := range models {
		if m.Free && m.Protocol == protocolChat {
			target = m
			break
		}
	}
	if target.ID == "" {
		t.Skip("本次匿名清单里没有 chat 方言的免费模型")
	}
	t.Logf("匿名可见 %d 个模型，测试目标 %s（协议 %s）", len(models), target.ID, target.Protocol)

	body := openaiup.ChatBody(&pb.ChatRequest{
		Model:    target.ID,
		Messages: []*pb.EnvelopeMessage{{Role: "user", Text: "Reply with exactly: OK"}},
	})
	body["model"] = target.ID
	body["stream"] = true
	ensureAnonTools(body, protocolChat)
	payload, _ := json.Marshal(body)

	url := baseFor(cred.Tier) + pathFor(protocolChat)
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range p.headers(cred, protocolChat, canonicalSession("live-probe"), requestID()) {
		req.Header.Set(k, v)
	}
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		t.Fatalf("上游连接失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf := make([]byte, 400)
		n, _ := resp.Body.Read(buf)
		t.Fatalf("匿名通道返回 %d: %s", resp.StatusCode, string(buf[:n]))
	}
	// 读前几帧确认真的有内容增量
	sc := newBufScanner(resp.Body)
	parser := openaiup.NewParser(func(ev *pb.StreamEvent) {
		if d, ok := ev.Event.(*pb.StreamEvent_ContentDelta); ok && d.ContentDelta.Text != "" {
			t.Logf("收到内容增量: %q", d.ContentDelta.Text)
		}
	})
	lines := 0
	for sc.Scan() && lines < 400 {
		parser.Feed(sc.Text())
		lines++
	}
	if lines == 0 {
		t.Fatal("匿名通道没有返回任何帧")
	}
}

// getenvOrEmpty 读环境变量（联调开关）。
func getenvOrEmpty(key string) string { return os.Getenv(key) }

// newBufScanner 大行缓冲扫描器（SSE 单帧可能很大）。
func newBufScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	return sc
}
