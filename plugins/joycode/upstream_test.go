package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// ---------- 签名 / 地址 ----------

// 期望值由外部工具（Node crypto）独立算出，避免自证自洽。
func TestJoyColorSignMatchesReference(t *testing.T) {
	cases := []struct {
		fn   string
		want string
	}{
		{"chat_completions", "31a01ea18362dbe10998cf5f61bf4b8ae5f883a133117e40b54871086997052f"},
		{"joycode_userInfo", "cdd840600507505a3c17a681aa012558c662f1b5d3a49178457efb5b714167e3"},
		{"joycode_modelList", "2ee8228c1a646f90beb5a25d6673b154aae5eb86c49937a1fdc818ca6d36d94f"},
		{"anthropic_completions", "129068d67c42ad4d7d6379cbd90b23462bfaf98a28783cc89823aa3cc383f27f"},
		{"web_search", "87501d429aa5888b21536eab2e18f14a5ac82285a71e6bd55be9b41834894da0"},
	}
	for _, c := range cases {
		q, sign := joyColorSign(c.fn, 1758529000000)
		if sign != c.want {
			t.Errorf("%s 签名不符\n got %s\nwant %s", c.fn, sign, c.want)
		}
		wantQuery := "appid=joycode_ide&functionId=" + c.fn + "&t=1758529000000"
		if q != wantQuery {
			t.Errorf("%s query 不符: %s", c.fn, q)
		}
	}
}

func TestJoyRequestURL(t *testing.T) {
	p := &plugin{}

	gateway := &joyCred{PtKey: "k", UserID: "u", ColorBaseURL: "https://api-ai.jd.com"}
	u := p.requestURL(gateway, joyEpChat)
	if !strings.HasPrefix(u, "https://api-ai.jd.com/api?") {
		t.Fatalf("网关地址不符: %s", u)
	}
	for _, frag := range []string{"appid=joycode_ide", "functionId=chat_completions", "&t=", "&sign="} {
		if !strings.Contains(u, frag) {
			t.Errorf("网关地址缺少 %s: %s", frag, u)
		}
	}

	direct := &joyCred{PtKey: "k", UserID: "u", ColorBaseURL: joyColorDisabled, MasterBaseURL: "https://joycode-api.jd.com"}
	if got, want := p.requestURL(direct, joyEpChat), "https://joycode-api.jd.com/api/saas/openai/v2/chat/completions"; got != want {
		t.Errorf("直连地址\n got %s\nwant %s", got, want)
	}
	if got, want := p.requestURL(direct, joyEpUserInfo), "https://joycode-api.jd.com/api/saas/user/v2/userInfo"; got != want {
		t.Errorf("直连 userInfo\n got %s\nwant %s", got, want)
	}

	// 未登记端点保持原文（如已下线的 rerank）
	if got := p.requestURL(direct, "/api/saas/openai/v1/rerank"); !strings.HasSuffix(got, "/api/saas/openai/v1/rerank") {
		t.Errorf("未登记端点应原样拼接: %s", got)
	}
}

// ---------- 信封 / 请求头 ----------

func TestJoyEnvelopeFields(t *testing.T) {
	p := &plugin{}
	cred := &joyCred{PtKey: "k", UserID: "u-123", OrgFullName: "org-a", Tenant: "JOYCODE"}
	body := p.joyEnvelope(cred, map[string]interface{}{"model": "GLM-5.1", "stream": true})

	for k, want := range map[string]interface{}{
		"tenant": "JOYCODE", "orgFullName": "org-a", "userId": "u-123",
		"client": "JoyCode", "clientVersion": joyDefaultVersion, "language": "UNKNOWN",
		"model": "GLM-5.1", "stream": true,
	} {
		if got := body[k]; got != want {
			t.Errorf("信封字段 %s = %v，期望 %v", k, got, want)
		}
	}

	// Anthropic 方言：tenant 缺省 JD
	anthropic := p.joyAnthropicEnvelope(&joyCred{PtKey: "k", UserID: "u"}, map[string]interface{}{})
	if anthropic["tenant"] != "JD" || anthropic["stream"] != true {
		t.Errorf("anthropic 信封不符: %#v", anthropic)
	}
}

func TestJoyHeaders(t *testing.T) {
	p := &plugin{}
	cred := &joyCred{PtKey: "pk-1", UserID: "u-1"}
	h := p.joyHeaders(cred, true)
	if h.Get("ptKey") != "pk-1" {
		t.Errorf("ptKey 头缺失: %v", h)
	}
	if h.Get("source-type") != "joycoder-ide" {
		t.Errorf("source-type 不符: %s", h.Get("source-type"))
	}
	if h.Get("loginType") != joyDefaultLoginType {
		t.Errorf("loginType 不符: %s", h.Get("loginType"))
	}
	if !strings.Contains(h.Get("User-Agent"), "JoyCode/"+joyDefaultVersion) {
		t.Errorf("UA 未含版本号: %s", h.Get("User-Agent"))
	}
	// 流式必须 identity，否则 gzip 会把整个响应缓冲成一次性下发
	if h.Get("Accept-Encoding") != "identity" {
		t.Errorf("流式 Accept-Encoding 必须是 identity，实际 %s", h.Get("Accept-Encoding"))
	}
	if nh := p.joyHeaders(cred, false).Get("Accept-Encoding"); nh != "gzip, deflate" {
		t.Errorf("非流式 Accept-Encoding 不符: %s", nh)
	}
	if ah := p.joyAnthropicHeaders(cred, true); ah.Get("loginType") != "PIN_JD_CLOUD" {
		t.Errorf("anthropic loginType 不符: %s", ah.Get("loginType"))
	}
}

// ---------- 凭据解析 ----------

// 出站 UA 默认值必须与官方分发包一致（指纹不对容易被上游风控）。
func TestJoyBuildUserAgent(t *testing.T) {
	got := joyBuildUserAgent("JoyCode", joyDefaultVersion)
	want := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) JoyCode/2.7.5 Chrome/133.0.0.0 Electron/35.2.0 Safari/537.36"
	if got != want {
		t.Errorf("UA 不符\n got %s\nwant %s", got, want)
	}
	if custom := joyBuildUserAgent("MyClient", "9.9.9"); !strings.Contains(custom, "MyClient/9.9.9") {
		t.Errorf("自定义名称/版本未生效: %s", custom)
	}
}

func TestJoyParseCredentialInput(t *testing.T) {
	t.Run("回调URL", func(t *testing.T) {
		c, err := parseJoyCredentialInput("http://127.0.0.1:19999/api/oauth-callback?pt_key=AA_hz&login_type=PIN&tenant=JOYCODE&user_id=99")
		if err != nil {
			t.Fatal(err)
		}
		if c.PtKey != "AA_hz" || c.UserID != "99" || c.LoginType != "PIN" || c.Tenant != "JOYCODE" {
			t.Errorf("解析结果不符: %#v", c)
		}
	})

	t.Run("扁平JSON", func(t *testing.T) {
		c, err := parseJoyCredentialInput(`{"ptKey":"AA1","userId":"1001","colorBaseUrl":"https://api-ai.jd.com"}`)
		if err != nil {
			t.Fatal(err)
		}
		if c.PtKey != "AA1" || c.UserID != "1001" || c.ColorBaseURL != "https://api-ai.jd.com" {
			t.Errorf("解析结果不符: %#v", c)
		}
	})

	t.Run("IDE包裹JSON", func(t *testing.T) {
		raw := `{"joyCoderUser":{"ptKey":"AA2","userId":"1002","masterBaseUrl":"https://joycode-api.jd.com","tenant":"JOYCODE","loginType":"N_PIN_PC","orgFullName":""}}`
		c, err := parseJoyCredentialInput(raw)
		if err != nil {
			t.Fatal(err)
		}
		if c.PtKey != "AA2" || c.UserID != "1002" || c.MasterBaseURL != "https://joycode-api.jd.com" || c.LoginType != "N_PIN_PC" {
			t.Errorf("解析结果不符: %#v", c)
		}
	})

	t.Run("键值文本", func(t *testing.T) {
		c, err := parseJoyCredentialInput("ptKey=AA3\nuserId=1003\nloginType=N_PIN_PC")
		if err != nil {
			t.Fatal(err)
		}
		if c.PtKey != "AA3" || c.UserID != "1003" || c.LoginType != "N_PIN_PC" {
			t.Errorf("解析结果不符: %#v", c)
		}
	})

	t.Run("纯ptKey", func(t *testing.T) {
		c, err := parseJoyCredentialInput("AAHtbGciOiJIUzI1NiJ9longtokenvalue")
		if err != nil {
			t.Fatal(err)
		}
		if c.PtKey == "" || c.UserID != "" {
			t.Errorf("纯 ptKey 应只填 PtKey: %#v", c)
		}
	})

	t.Run("空内容报错", func(t *testing.T) {
		if _, err := parseJoyCredentialInput("   "); err == nil {
			t.Error("空内容应当报错")
		}
	})

	t.Run("无ptKey报错", func(t *testing.T) {
		if _, err := parseJoyCredentialInput(`{"userId":"1"}`); err == nil {
			t.Error("缺少 ptKey 应当报错")
		}
	})
}

// ---------- 状态 / 错误映射 ----------

func TestJoyMapUpstreamStatus(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   int32
	}{
		{401, "", 401},
		{402, "", 402},
		{429, "", 429},
		{403, "credits exhausted", 402},
		{403, "额度不足", 402},
		{403, "forbidden", 401},
		{500, "", 502},
		{0, "pt_key expired", 401},
		{0, "rate limit exceeded", 429},
		{0, "余额不足", 402},
	}
	for _, c := range cases {
		if got := joyMapUpstreamStatus(c.status, c.body); got != c.want {
			t.Errorf("status=%d body=%q → %d，期望 %d", c.status, c.body, got, c.want)
		}
	}
}

func TestJoySSEErrorText(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{`data: {"error":{"message":"model not supported","code":400}}`, "model not supported"},
		{`data: {"code":4030,"msg":"额度不足"}`, "额度不足"},
		{`data: {"choices":[{"delta":{"content":"hi"}}]}`, ""},
		{`data: [DONE]`, ""},
		{`: keepalive`, ""},
	}
	for _, c := range cases {
		if got := joySSEErrorText(c.line); got != c.want {
			t.Errorf("line=%s\n got %q\nwant %q", c.line, got, c.want)
		}
	}
}

func TestJoyStripCacheControl(t *testing.T) {
	in := []interface{}{
		map[string]interface{}{
			"role": "user",
			"content": []interface{}{
				map[string]interface{}{"type": "text", "text": "hi", "cache_control": map[string]interface{}{"type": "ephemeral"}},
			},
		},
	}
	b, err := json.Marshal(stripCacheControl(in))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "cache_control") {
		t.Errorf("cache_control 未剥净: %s", b)
	}
	if !strings.Contains(string(b), `"text":"hi"`) {
		t.Errorf("正文被误删: %s", b)
	}
}

// ---------- 端到端（mock 上游） ----------

func mustJoyCredBlob(c *joyCred) *pb.CredentialBlob {
	raw, _ := json.Marshal(c)
	return &pb.CredentialBlob{Blob: raw}
}

// mockJoyUpstream 校验出站信封与请求头，并回一段标准 JoyCode SSE。
func mockJoyUpstream(t *testing.T, captured *map[string]interface{}, capturedHeader *http.Header, sse string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*capturedHeader = r.Header.Clone()
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("请求体不是 JSON: %v", err)
		}
		*captured = body
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sse)
	}))
}

func collectJoyEvents(t *testing.T, srv *httptest.Server, req *pb.ChatRequest) []*pb.StreamEvent {
	t.Helper()
	p := &plugin{}
	var events []*pb.StreamEvent
	if err := p.joyRunChat(context.Background(), req, func(ev *pb.StreamEvent) error {
		events = append(events, ev)
		return nil
	}); err != nil {
		t.Fatalf("joyRunChat 返回错误: %v", err)
	}
	return events
}

func TestJoyRunChatStreaming(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"id":"c1","choices":[{"delta":{"role":"assistant","content":""}}]}`,
		``,
		`data: {"id":"c1","choices":[{"delta":{"content":"你好"}}]}`,
		``,
		`data: {"id":"c1","choices":[{"delta":{"content":"，世界"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":7}}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")

	var gotBody map[string]interface{}
	var gotHeader http.Header
	srv := mockJoyUpstream(t, &gotBody, &gotHeader, sse)
	defer srv.Close()

	req := &pb.ChatRequest{
		Model:      "GLM-5.1",
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "打个招呼"}},
		Credential: mustJoyCredBlob(&joyCred{PtKey: "pk-e2e", UserID: "u-77", ColorBaseURL: joyColorDisabled, MasterBaseURL: srv.URL}),
	}
	events := collectJoyEvents(t, srv, req)

	// 出站侧断言
	if gotHeader.Get("ptKey") != "pk-e2e" {
		t.Errorf("ptKey 头未透传: %s", gotHeader.Get("ptKey"))
	}
	if gotHeader.Get("Accept-Encoding") != "identity" {
		t.Errorf("流式 Accept-Encoding 应为 identity，实际 %s", gotHeader.Get("Accept-Encoding"))
	}
	if gotBody["userId"] != "u-77" || gotBody["tenant"] != "JOYCODE" || gotBody["client"] != "JoyCode" {
		t.Errorf("信封字段不符: %#v", gotBody)
	}
	if gotBody["model"] != "GLM-5.1" {
		t.Errorf("model 未透传: %v", gotBody["model"])
	}
	if _, has := gotBody["stream_options"]; has {
		t.Error("stream_options 不应发给上游")
	}
	if gotBody["stream"] != true {
		t.Errorf("stream 必须为 true: %v", gotBody["stream"])
	}

	// 事件侧断言：首事件必须是 MessageStart（延迟首发哨兵）
	if len(events) == 0 {
		t.Fatal("没有任何事件")
	}
	if _, ok := events[0].Event.(*pb.StreamEvent_MessageStart); !ok {
		t.Fatalf("首事件应为 MessageStart，实际 %T", events[0].Event)
	}
	var text strings.Builder
	var finished bool
	var usage *pb.Usage
	for _, ev := range events {
		switch e := ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			text.WriteString(e.ContentDelta.Text)
		case *pb.StreamEvent_MessageFinish:
			finished = true
			usage = e.MessageFinish.Usage
		}
	}
	if text.String() != "你好，世界" {
		t.Errorf("正文拼接不符: %q", text.String())
	}
	if !finished {
		t.Error("缺少 MessageFinish")
	}
	if usage == nil {
		t.Fatal("缺少 Usage 事件")
	}
	if usage.InputTokens != 11 || usage.OutputTokens != 3 || usage.CachedTokens != 7 {
		t.Errorf("usage 映射不符: %+v", usage)
	}
}

// HTTP 200 + JSON 错误体：必须给出可诊断的错误，而不是空响应。
func TestJoyRunChatJSONErrorBody(t *testing.T) {
	var body map[string]interface{}
	var header http.Header
	srv := mockJoyUpstream(t, &body, &header, `{"code":4030,"msg":"额度不足，请充值"}`)
	defer srv.Close()

	req := &pb.ChatRequest{
		Model:      "GLM-5.1",
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "hi"}},
		Credential: mustJoyCredBlob(&joyCred{PtKey: "pk", UserID: "u", ColorBaseURL: joyColorDisabled, MasterBaseURL: srv.URL}),
	}
	events := collectJoyEvents(t, srv, req)
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d", len(events))
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("应为 TaskFailed，实际 %T", events[0].Event)
	}
	if fail.TaskFailed.Error.Code != 402 {
		t.Errorf("业务码 4030 应映射为 402，实际 %d", fail.TaskFailed.Error.Code)
	}
	if !strings.Contains(fail.TaskFailed.Detail, "额度不足") {
		t.Errorf("详情未带完整上游返回: %s", fail.TaskFailed.Detail)
	}
}

// SSE 里先给错误帧、没有正文：以首事件失败上报（核心据此换号）。
func TestJoyRunChatEmptyStreamWithErrorFrame(t *testing.T) {
	sse := "data: {\"error\":{\"message\":\"inference failed\",\"code\":500}}\n\ndata: [DONE]\n\n"
	var body map[string]interface{}
	var header http.Header
	srv := mockJoyUpstream(t, &body, &header, sse)
	defer srv.Close()

	req := &pb.ChatRequest{
		Model:      "GLM-5.1",
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "hi"}},
		Credential: mustJoyCredBlob(&joyCred{PtKey: "pk", UserID: "u", ColorBaseURL: joyColorDisabled, MasterBaseURL: srv.URL}),
	}
	events := collectJoyEvents(t, srv, req)
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d", len(events))
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("应为 TaskFailed，实际 %T", events[0].Event)
	}
	if !strings.Contains(fail.TaskFailed.Detail, "inference failed") {
		t.Errorf("详情未带上游错误帧: %s", fail.TaskFailed.Detail)
	}
}

// 工具调用增量：OpenAI 只在首块给 id/name，解析器需补齐。
func TestJoyRunChatToolCalls(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"read_file","arguments":"{\"pa"}}]}}]}`,
		``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"th\":\"a.go\"}"}}]},"finish_reason":"tool_calls"}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	var body map[string]interface{}
	var header http.Header
	srv := mockJoyUpstream(t, &body, &header, sse)
	defer srv.Close()

	req := &pb.ChatRequest{
		Model:      "GLM-5.1",
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "读文件"}},
		Tools:      []*pb.ToolDefinition{{Name: "read_file", Description: "读", ParametersSchema: `{"type":"object"}`}},
		Credential: mustJoyCredBlob(&joyCred{PtKey: "pk", UserID: "u", ColorBaseURL: joyColorDisabled, MasterBaseURL: srv.URL}),
	}
	events := collectJoyEvents(t, srv, req)

	var args strings.Builder
	for _, ev := range events {
		if e, ok := ev.Event.(*pb.StreamEvent_ToolCallDelta); ok {
			args.WriteString(e.ToolCallDelta.ArgumentsDelta)
		}
	}
	if args.String() != `{"path":"a.go"}` {
		t.Errorf("工具参数拼接不符: %q", args.String())
	}
	if _, has := body["tools"]; !has {
		t.Error("tools 未透传给上游")
	}
}

// ---------- 模型目录映射 ----------

func TestJoyModelMapping(t *testing.T) {
	if got := joyModelID(joyModelInfo{ModelID: "GLM-5.1"}); got != "GLM-5.1" {
		t.Errorf("modelId 优先: %s", got)
	}
	if got := joyModelID(joyModelInfo{ChatAPIModel: "Kimi-K2.6"}); got != "Kimi-K2.6" {
		t.Errorf("chatApiModel 兜底: %s", got)
	}
	if got := joyModelID(joyModelInfo{Label: "JoyAI"}); got != "JoyAI" {
		t.Errorf("label 兜底: %s", got)
	}
	cap, ok := joyModelCaps["GLM-5.1"]
	if !ok || cap.Context != 200000 || cap.Output != 16384 || !cap.Reason {
		t.Errorf("模型能力表不符: %+v", cap)
	}
	if !joyReasoningModel("GLM-5.1") || !joyReasoningModel("MiniMax-M2.7") {
		t.Error("推理模型判定失败")
	}
	if joyReasoningModel("Doubao-Seed-2.0-pro") {
		t.Error("非推理模型被误判")
	}
}

// 模型元数据映射：上游字段优先，能力表兜底。
func TestJoyModelToInfo(t *testing.T) {
	// 上游给了上下文/输出 → 用上游值
	got := joyModelToInfo("GLM-5.1", "GLM-5.1", 131072, 8192, []string{"reasoning", "chat"})
	if got.ContextWindow != 131072 || got.MaxOutputTokens != 8192 {
		t.Errorf("上游字段未优先: ctx=%d out=%d", got.ContextWindow, got.MaxOutputTokens)
	}
	if got.Series != "GLM" {
		t.Errorf("系列未从能力表补齐: %s", got.Series)
	}
	if got.DefaultReasoningEffort != "high" || len(got.ReasoningEfforts) == 0 {
		t.Errorf("推理档位缺失: %+v", got.ReasoningEfforts)
	}
	if !joyHasTag(got.Tags, "支持推理") {
		t.Errorf("推理标签缺失: %v", got.Tags)
	}

	// 上游只给名称 → 能力表兜底上下文/输出
	got = joyModelToInfo("Claude-Opus-4.7", "Claude Opus 4.7", 0, 0, nil)
	if got.ContextWindow != 200000 || got.MaxOutputTokens != 32000 {
		t.Errorf("能力表兜底失败: ctx=%d out=%d", got.ContextWindow, got.MaxOutputTokens)
	}
	if got.SupportsTools != true || got.SupportsStream != true {
		t.Errorf("工具/流式能力应为 true: %+v", got)
	}

	// 未知模型：不崩、字段为 0、标签为空
	got = joyModelToInfo("Unknown-Model-X", "", 0, 0, nil)
	if got.Id != "Unknown-Model-X" || got.Label["zh"] != "Unknown-Model-X" {
		t.Errorf("未知模型 label 兜底不符: %+v", got.Label)
	}
	if got.DefaultReasoningEffort != "" {
		t.Errorf("未知模型不应带推理档位: %s", got.DefaultReasoningEffort)
	}

	// 视觉模型补「多模态」标签
	if got := joyModelToInfo("Kimi-K2.6", "Kimi K2.6", 0, 0, nil); !joyHasTag(got.Tags, "多模态") {
		t.Errorf("视觉模型缺多模态标签: %v", got.Tags)
	}
}

// 空凭据（核心刷新聚合目录）必须返回兜底清单而不是报错，否则 /v1/models 为空。
func TestJoyListModelsWithoutCredential(t *testing.T) {
	p := &plugin{}
	ml, err := p.ListModels(context.Background(), &pb.CredentialBlob{})
	if err != nil {
		t.Fatalf("空凭据应返回兜底清单，实际报错: %v", err)
	}
	if len(ml.Models) != len(joyFallbackModels) {
		t.Errorf("兜底模型数不符: got %d want %d", len(ml.Models), len(joyFallbackModels))
	}
	var hasDefault bool
	for _, mo := range ml.Models {
		if mo.Id == joyDefaultModel {
			hasDefault = true
		}
		if mo.Id == "" || mo.ContextWindow == 0 {
			t.Errorf("兜底模型元数据不完整: %+v", mo)
		}
	}
	if !hasDefault {
		t.Errorf("兜底清单缺少默认模型 %s", joyDefaultModel)
	}
}

func TestJoyMaskUserID(t *testing.T) {
	if got := maskUserID("15800006694"); got != "158***694" {
		t.Errorf("掩码不符: %s", got)
	}
	if got := maskUserID(""); got != "JoyCode" {
		t.Errorf("空值兜底不符: %s", got)
	}
	if got := maskUserID("123"); got != "123" {
		t.Errorf("短值应原样: %s", got)
	}
}
