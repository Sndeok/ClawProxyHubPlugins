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

// TestDeriveProfileDeterministic 设备指纹必须按 Key 确定性派生：
// 同一 Key（同一盐）任何时候都是同一台设备；换盐 = 换设备。
func TestDeriveProfileDeterministic(t *testing.T) {
	a := deriveProfile("user_abc", "")
	b := deriveProfile("user_abc", "")
	if !sameProfile(a, b) {
		t.Errorf("同一 Key 派生的档案必须完全一致\n%+v\n%+v", a, b)
	}
	c := deriveProfile("user_abc", "salt-1")
	if c.MachineID == a.MachineID || c.Hostname == a.Hostname {
		t.Error("换盐后设备身份必须改变")
	}
	d := deriveProfile("user_xyz", "")
	if d.MachineID == a.MachineID {
		t.Error("不同 Key 不应共用机器码")
	}
	// 形状检查（要像真机）
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(a.MachineID) {
		t.Errorf("machineId 形状不对: %s", a.MachineID)
	}
	if !regexp.MustCompile(`^DESKTOP-[0-9A-F]{6}$`).MatchString(a.Hostname) {
		t.Errorf("hostname 形状不对: %s", a.Hostname)
	}
	if len(a.MACs) < 2 || len(a.MACs) > 4 {
		t.Errorf("MAC 数量应为 2–4: %v", a.MACs)
	}
	for _, mac := range a.MACs {
		if !regexp.MustCompile(`^[0-9a-f]{2}(:[0-9a-f]{2}){5}$`).MatchString(mac) {
			t.Errorf("MAC 形状不对: %s", mac)
		}
	}
	if !strings.Contains(a.GitEmail, "@") || len(a.Thumbmark) != 64 {
		t.Errorf("gitEmail/thumbmark 异常: %q / %q", a.GitEmail, a.Thumbmark)
	}
	if a.OSRelease == "" || a.CPUModel == "" || a.CPUCores == 0 || a.MemGiB == 0 {
		t.Errorf("档案字段缺失: %+v", a)
	}
}

// TestProfileSingleSourceOfTruth 伪装要自洽：x-project-slug 与 config.workingDir 同源。
func TestProfileSingleSourceOfTruth(t *testing.T) {
	p := &plugin{}
	cred := &ccCred{APIKey: "user_abc"}
	prof := cred.profile("")
	h := p.ccHeaders(cred, "sess_1")
	if h["x-project-slug"] != prof.slugifyProjectPath() {
		t.Errorf("x-project-slug 与档案不同源: %q vs %q", h["x-project-slug"], prof.slugifyProjectPath())
	}
	if strings.ContainsAny(h["x-project-slug"], "\\:") {
		t.Errorf("slug 不应含路径分隔符: %q", h["x-project-slug"])
	}
}

// TestCCHeaders 官方 CLI 头一个都不能少。
func TestCCHeaders(t *testing.T) {
	p := &plugin{}
	cred := &ccCred{APIKey: "user_abc"}
	h := p.ccHeaders(cred, "sess_xyz")
	for _, k := range []string{"User-Agent", "x-command-code-version", "x-cli-environment",
		"x-project-slug", "x-taste-learning", "x-session-id", "Authorization", "traceparent", "Content-Type"} {
		if h[k] == "" {
			t.Errorf("缺少请求头 %s", k)
		}
	}
	if h["User-Agent"] != "cli" {
		t.Errorf("User-Agent 必须固定 cli，实际 %q", h["User-Agent"])
	}
	if h["x-session-id"] != "sess_xyz" {
		t.Errorf("x-session-id = %q", h["x-session-id"])
	}
	if h["Authorization"] != "Bearer user_abc" {
		t.Errorf("Authorization = %q", h["Authorization"])
	}
	if !strings.HasPrefix(h["traceparent"], "00-") {
		t.Errorf("traceparent 形状不对: %q", h["traceparent"])
	}
	if _, hasZDR := h["x-cmd-zdr"]; hasZDR {
		t.Error("默认不应带 x-cmd-zdr")
	}
}

// TestBuildCCBody 信封结构 + 关键默认值（含「缺 system 发空格占位」的省 token 行为）。
func TestBuildCCBody(t *testing.T) {
	p := &plugin{}
	cred := &ccCred{APIKey: "user_abc"}
	prof := cred.profile("")
	chatBody := map[string]interface{}{
		"messages": []map[string]interface{}{
			{"role": "user", "content": "写个快排"},
		},
		"tools": []interface{}{map[string]interface{}{
			"type": "function",
			"function": map[string]interface{}{"name": "bash_output", "description": "看输出",
				"parameters": map[string]interface{}{"type": "object"}},
		}},
	}
	req := &pb.ChatRequest{Model: "deepseek/deepseek-v4-flash", Extra: map[string]string{"reasoning_effort": "max"}}
	raw, _ := json.Marshal(p.buildCCBody(chatBody, req.Model, prof, "sess_1", req))
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"config", "memory", "taste", "skills", "permissionMode", "mode", "threadId", "params"} {
		if _, ok := body[k]; !ok {
			t.Errorf("信封缺少 %s", k)
		}
	}
	cfg, _ := body["config"].(map[string]interface{})
	if cfg["workingDir"] != prof.ProjectDir || cfg["environment"] != prof.Platform {
		t.Errorf("config 与设备档案不一致: %v", cfg)
	}
	if body["skills"] != nil {
		t.Error("skills 必须是 null（CLI 就这么发）")
	}
	if body["threadId"] == "" || !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(str(body["threadId"])) {
		t.Errorf("threadId 必须是合法 UUID v4: %v", body["threadId"])
	}
	params, _ := body["params"].(map[string]interface{})
	if params["stream"] != true {
		t.Error("CC 上游恒为流式，stream 必须 true")
	}
	if asInt(params["max_tokens"]) != int64(defaultMaxTokens) {
		t.Errorf("默认 max_tokens = %v, want %d", params["max_tokens"], defaultMaxTokens)
	}
	if params["reasoning_effort"] != "max" {
		t.Errorf("reasoning_effort 未透传: %v", params["reasoning_effort"])
	}
	// 缺 system → 空格占位（否则上游注入约 7.5K token 默认提示词）
	sys, _ := params["system"].([]interface{})
	if len(sys) != 1 {
		t.Fatalf("system 占位异常: %v", params["system"])
	}
	block, _ := sys[0].(map[string]interface{})
	if block["text"] != " " {
		t.Errorf("system 占位应为单个空格: %v", block)
	}
	// tools 是 CC wire 形态：{name,description,input_schema}，无 type；并做官方名重写
	tools, _ := params["tools"].([]interface{})
	if len(tools) != 1 {
		t.Fatalf("tools 丢失: %v", params["tools"])
	}
	tool, _ := tools[0].(map[string]interface{})
	if tool["name"] != "shell_output" {
		t.Errorf("工具名未按官方别名重写: %v", tool["name"])
	}
	if _, hasType := tool["type"]; hasType {
		t.Error("CC wire 的工具定义不应带 type 字段")
	}
	if _, ok := tool["input_schema"]; !ok {
		t.Errorf("工具定义缺少 input_schema: %v", tool)
	}
}

// TestMaxTokensCap 超过上限被截到 200000。
func TestMaxTokensCap(t *testing.T) {
	p := &plugin{}
	prof := (&ccCred{APIKey: "user_a"}).profile("")
	body := p.buildCCBody(map[string]interface{}{"messages": []map[string]interface{}{}}, "m", prof, "s", &pb.ChatRequest{MaxTokens: 999999})
	params, _ := body["params"].(map[string]interface{})
	if asInt(params["max_tokens"]) != int64(maxAllowedTokens) {
		t.Errorf("max_tokens 未封顶: %v", params["max_tokens"])
	}
}

// TestCCMessages 消息形态转换（含缓存断点、图片、工具调用/结果、非末块补换行）。
func TestCCMessages(t *testing.T) {
	messages := []map[string]interface{}{
		{"role": "system", "content": []interface{}{
			map[string]interface{}{"type": "text", "text": "系统提示 A", "cache_control": map[string]interface{}{"type": "ephemeral"}},
			map[string]interface{}{"type": "text", "text": "系统提示 B"},
		}},
		{"role": "user", "content": []interface{}{
			map[string]interface{}{"type": "text", "text": "看图"},
			map[string]interface{}{"type": "image_url", "image_url": map[string]interface{}{"url": "data:image/png;base64,AA=="}},
		}},
		{"role": "assistant", "content": "我来读文件", "tool_calls": []interface{}{
			map[string]interface{}{"id": "call_1", "type": "function",
				"function": map[string]interface{}{"name": "Read", "arguments": `{"path":"a.go"}`}},
		}},
		{"role": "tool", "content": "文件内容", "tool_call_id": "call_1"},
	}
	systemBlocks, out := ccMessages(messages)
	if len(systemBlocks) != 2 {
		t.Fatalf("system 块数 = %d: %v", len(systemBlocks), systemBlocks)
	}
	b0, _ := systemBlocks[0].(map[string]interface{})
	if _, ok := b0["cache_control"]; !ok {
		t.Error("system 块上的 cache_control 断点丢失")
	}
	if !strings.HasSuffix(str(b0["text"]), "\n") {
		t.Errorf("非末块应补换行（CLI 行为）: %q", b0["text"])
	}
	// user 图片块
	user, _ := out[0].(map[string]interface{})
	parts, _ := user["content"].([]interface{})
	if len(parts) != 2 {
		t.Fatalf("user 块数 = %d: %v", len(parts), parts)
	}
	img, _ := parts[1].(map[string]interface{})
	if img["type"] != "image" || img["mimeType"] != "image/png" {
		t.Errorf("图片块应转成 CC image + mimeType: %v", img)
	}
	// assistant：text + tool-call
	asst, _ := out[1].(map[string]interface{})
	aparts, _ := asst["content"].([]interface{})
	if len(aparts) != 2 {
		t.Fatalf("assistant 块数 = %d: %v", len(aparts), aparts)
	}
	tc, _ := aparts[1].(map[string]interface{})
	if tc["type"] != "tool-call" || tc["toolCallId"] != "call_1" || tc["toolName"] != "Read" {
		t.Errorf("tool-call 块错误: %v", tc)
	}
	if _, isStr := tc["input"].(string); isStr {
		t.Errorf("tool-call 的 input 应是解析后的对象: %v", tc["input"])
	}
	// tool → tool-result（toolName 由前文 assistant 的 tool_calls 反查）
	toolMsg, _ := out[2].(map[string]interface{})
	tparts, _ := toolMsg["content"].([]interface{})
	tr, _ := tparts[0].(map[string]interface{})
	if tr["type"] != "tool-result" || tr["toolCallId"] != "call_1" || tr["toolName"] != "Read" {
		t.Errorf("tool-result 块错误: %v", tr)
	}
	outp, _ := tr["output"].(map[string]interface{})
	if outp["value"] != "文件内容" {
		t.Errorf("tool-result 输出错误: %v", outp)
	}
}

// TestNDJSONReader AI-SDK NDJSON → 标准 OpenAI chunk（文本 / 工具 / 用量 / 错误）。
func TestNDJSONReader(t *testing.T) {
	stream := strings.Join([]string{
		`{"type":"start"}`,
		`{"type":"text-start"}`,
		`{"type":"reasoning-delta","text":"思考中"}`,
		`{"type":"text-delta","text":"你"}`,
		`{"type":"text-delta","text":"好"}`,
		`{"type":"tool-call","toolCallId":"call_1","toolName":"read_file","input":{"path":"a.go"}}`,
		`{"type":"finish","finishReason":"tool-calls","totalUsage":{"inputTokens":120,"outputTokens":9,"cachedInputTokens":100}}`,
	}, "\n")
	out, err := io.ReadAll(ndjsonReader(strings.NewReader(stream), "m"))
	if err != nil {
		t.Fatal(err)
	}
	var content, args, finish string
	var cached, prompt int64
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	parser := openaiup.NewParser(func(ev *pb.StreamEvent) {
		switch e := ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			content += e.ContentDelta.Text
		case *pb.StreamEvent_ToolCallDelta:
			args += e.ToolCallDelta.ArgumentsDelta
		case *pb.StreamEvent_MessageFinish:
			finish = e.MessageFinish.FinishReason
			if e.MessageFinish.Usage != nil {
				cached = e.MessageFinish.Usage.CachedTokens
				prompt = e.MessageFinish.Usage.InputTokens
			}
		}
	})
	for sc.Scan() {
		parser.Feed(sc.Text())
	}
	if content != "你好" {
		t.Errorf("正文 = %q", content)
	}
	if !strings.Contains(args, `"path":"a.go"`) {
		t.Errorf("工具参数丢失: %q", args)
	}
	if finish != "tool_calls" {
		t.Errorf("finish_reason = %q", finish)
	}
	if cached != 100 || prompt != 120 {
		t.Errorf("用量映射错误: cached=%d prompt=%d", cached, prompt)
	}
	if strings.Contains(string(out), "思考中") {
		t.Error("reasoning 内容不应混进正文（CPH 信封没有推理通道）")
	}
}

// TestNDJSONReaderError 流内错误事件要带状态码冒泡（供核心换号/暂停）。
func TestNDJSONReaderError(t *testing.T) {
	stream := `{"type":"error","error":{"message":"<429> daily free limit reached","statusCode":429}}` + "\n"
	_, err := io.ReadAll(ndjsonReader(strings.NewReader(stream), "m"))
	se, ok := err.(*StreamError)
	if !ok {
		t.Fatalf("want *StreamError, got %v", err)
	}
	if se.Status != 429 || !strings.Contains(se.Message, "daily free limit") {
		t.Errorf("错误映射错误: %+v", se)
	}
}

// TestMapUpstreamStatus 额度用尽按 402（核心暂停账号），限流按 429。
func TestMapUpstreamStatus(t *testing.T) {
	cases := []struct {
		code int
		body string
		want int32
	}{
		{402, "", 402},
		{400, `{"error":{"code":"USAGE_EXCEEDED"}}`, 402},
		{429, "", 429},
		{401, "", 401},
		{403, "", 401},
		{500, "", 502},
	}
	for _, c := range cases {
		if got := mapUpstreamStatus(c.code, c.body); got != c.want {
			t.Errorf("mapUpstreamStatus(%d, %q) = %d, want %d", c.code, c.body, got, c.want)
		}
	}
}

// TestStreamGuardDelaysStart 空响应必须作为首事件失败上报（核心才会换号）。
func TestStreamGuardDelaysStart(t *testing.T) {
	var sent []*pb.StreamEvent
	g := &ccStreamGuard{send: func(ev *pb.StreamEvent) error { sent = append(sent, ev); return nil }}
	g.emit(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{}})
	g.emit(&pb.StreamEvent{Event: &pb.StreamEvent_MessageFinish{MessageFinish: &pb.MessageFinish{}}})
	if g.started || len(sent) != 0 {
		t.Fatalf("空响应不应发出任何事件: started=%v sent=%d", g.started, len(sent))
	}
	g.emit(&pb.StreamEvent{Event: &pb.StreamEvent_ContentDelta{ContentDelta: &pb.ContentDelta{Text: "hi"}}})
	if !g.started || len(sent) != 2 {
		t.Fatalf("首段内容后应补发 MessageStart: started=%v sent=%d", g.started, len(sent))
	}
	if _, ok := sent[0].Event.(*pb.StreamEvent_MessageStart); !ok {
		t.Errorf("首发事件应为 MessageStart: %T", sent[0].Event)
	}
}

// ---------- 线上联调（CPH_COMMANDCODE_LIVE=1） ----------

// TestLivePublicModelCatalog 公开模型目录（无需 Key）。
func TestLivePublicModelCatalog(t *testing.T) {
	if os.Getenv("CPH_COMMANDCODE_LIVE") != "1" {
		t.Skip("设置 CPH_COMMANDCODE_LIVE=1 才跑线上联调")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	models, err := fetchModels(ctx, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("模型目录失败: %v", err)
	}
	if len(models) < 10 {
		t.Fatalf("模型数量异常: %d", len(models))
	}
	withCtx := 0
	for _, m := range models {
		if m.ContextLength > 0 {
			withCtx++
		}
	}
	t.Logf("模型 %d 个，其中 %d 个带 context_length，示例 %s ctx=%d 端点=%v",
		len(models), withCtx, models[0].ID, models[0].ContextLength, models[0].SupportedEndpoints)
}

// TestLiveKeyValidation 假 Key 必须在「指纹上报」这一步被拒（不产生计费请求）。
func TestLiveKeyValidation(t *testing.T) {
	if os.Getenv("CPH_COMMANDCODE_LIVE") != "1" {
		t.Skip("设置 CPH_COMMANDCODE_LIVE=1 才跑线上联调")
	}
	p := &plugin{}
	cred := &ccCred{APIKey: "user_bogus_probe_key_0001"}
	err := p.ensureInitialized(context.Background(), cred)
	if err == nil {
		t.Fatal("假 Key 竟然通过了校验")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("应以 401 拒绝，实际: %v", err)
	}
	t.Logf("校验失败信息: %v", err)
}

// sameProfile 档案逐字段比较（含切片）。
func sameProfile(a, b deviceProfile) bool {
	if a.Platform != b.Platform || a.Arch != b.Arch || a.OSRelease != b.OSRelease ||
		a.ProjectDir != b.ProjectDir || a.CPUModel != b.CPUModel || a.CPUCores != b.CPUCores ||
		a.MemGiB != b.MemGiB || a.Timezone != b.Timezone || a.IsContainer != b.IsContainer ||
		a.Hostname != b.Hostname || a.OSUser != b.OSUser || a.GitEmail != b.GitEmail ||
		a.MachineID != b.MachineID || a.Thumbmark != b.Thumbmark || len(a.MACs) != len(b.MACs) {
		return false
	}
	for i := range a.MACs {
		if a.MACs[i] != b.MACs[i] {
			return false
		}
	}
	return true
}

// asInt 统一 int / int64 / float64（直接构造与 JSON 反序列化两种路径）。
func asInt(v interface{}) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return -1
}
