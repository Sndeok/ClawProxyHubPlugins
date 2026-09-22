package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// ---------- 模型目录 ----------

func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	models, err := fetchModels(ctx, p.httpClient(cred))
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	sort.SliceStable(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	out := make([]*pb.ModelInfo, 0, len(models))
	for _, m := range models {
		tags := []string{}
		if m.OwnedBy != "" {
			tags = append(tags, m.OwnedBy)
		}
		for _, ep := range m.SupportedEndpoints {
			switch ep {
			case "/messages":
				tags = append(tags, "Anthropic 方言")
			case "/chat/completions":
				tags = append(tags, "Chat 方言")
			}
		}
		out = append(out, &pb.ModelInfo{
			Id:             m.ID,
			Label:          map[string]string{"zh": orDefault(m.Name, m.ID), "en": orDefault(m.Name, m.ID)},
			ContextWindow:  int32(m.ContextLength),
			Description:    m.Name,
			Tags:           tags,
			SupportsTools:  true,
			SupportsStream: true,
		})
	}
	return &pb.ModelList{Models: out}, nil
}

// ---------- 对话 ----------

// Chat 把 CPH 信封转成 Command Code /alpha/generate 请求。
//
// 官方客户端形态：9 键信封（config/memory/taste/skills/permissionMode/threadId/mode/promptCache/params）
// + 设备档案自洽（指纹 / config.environment / workingDir / x-project-slug 同源）。
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return stream.Send(failed(401, orHint(err)))
	}
	if req.Model == "" {
		return stream.Send(failed(400, "缺少模型名"))
	}
	prof := cred.profile(p.fingerprintSalt())
	sessionID := sessionIDFor(req)
	// 指纹 + lifecycle（首次 / 每 8h+抖动）：失败不阻塞正文
	_ = p.ensureInitialized(ctx, cred)

	chatBody := openaiup.ChatBody(req)
	body := p.buildCCBody(chatBody, req.Model, prof, sessionID, req)
	payload, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", ccAPIBase+ccGeneratePath, strings.NewReader(string(payload)))
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	for k, v := range p.ccHeaders(cred, sessionID) {
		httpReq.Header.Set(k, v)
	}
	resp, err := p.httpClient(cred).Do(httpReq)
	if err != nil {
		return stream.Send(failed(502, "上游连接失败: "+err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		detail := fmt.Sprintf("HTTP %d %s\n%s", resp.StatusCode, resp.Status, string(raw))
		return stream.Send(failedDetail(mapUpstreamStatus(resp.StatusCode, string(raw)),
			fmt.Sprintf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 300)), detail))
	}

	// 延迟首发 + 空响应识别（同其它插件：核心只对「首事件失败」换号重试）
	guard := &ccStreamGuard{send: func(ev *pb.StreamEvent) error { return stream.Send(ev) }}
	parser := openaiup.NewParser(guard.emit)
	sc := bufio.NewScanner(ndjsonReader(resp.Body, req.Model))
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		parser.Feed(sc.Text())
	}
	if err := sc.Err(); err != nil {
		if se, ok := err.(*StreamError); ok {
			return stream.Send(failed(mapUpstreamStatus(se.Status, se.Message), se.Message))
		}
		if !guard.started {
			return stream.Send(failed(502, "上游流中断（无有效内容）: "+err.Error()))
		}
		parser.FinishWithError(502, "上游流中断: "+err.Error())
		return nil
	}
	if !guard.started {
		return stream.Send(failed(429, "上游返回空内容（零输出风控），已暂停该账号并换号重试"))
	}
	parser.Finish()
	return nil
}

// buildCCBody 构造 /alpha/generate 信封。
func (p *plugin) buildCCBody(chatBody map[string]interface{}, model string, prof deviceProfile, sessionID string, req *pb.ChatRequest) map[string]interface{} {
	messages, _ := chatBody["messages"].([]map[string]interface{})
	systemBlocks, ccMessages := ccMessages(messages)

	maxTok := int(req.MaxTokens)
	if maxTok <= 0 {
		maxTok = defaultMaxTokens
	}
	if maxTok > maxAllowedTokens {
		maxTok = maxAllowedTokens
	}
	params := map[string]interface{}{
		"model":      model,
		"messages":   ccMessages,
		"max_tokens": maxTok,
		"stream":     true, // CC 上游恒为流式
		"tools":      ccTools(chatBody["tools"]),
	}
	if len(systemBlocks) > 0 {
		params["system"] = systemBlocks
	} else {
		// 缺 system 时上游会注入约 7.5K token 的默认提示词（既污染上下文又烧 token），
		// 官方客户端发空格占位绕过；这里对齐该行为。
		params["system"] = []interface{}{map[string]interface{}{"type": "text", "text": " "}}
	}
	if req.Temperature != 0 {
		params["temperature"] = req.Temperature
	}
	if v := strings.TrimSpace(req.Extra["reasoning_effort"]); v != "" {
		params["reasoning_effort"] = v
	}
	if v := strings.TrimSpace(req.Extra["parallel_tool_calls"]); v != "" {
		var b bool
		if json.Unmarshal([]byte(v), &b) == nil {
			params["parallel_tool_calls"] = b
		}
	}
	if tc := req.ToolChoice; tc != nil && tc.Type != "" {
		switch tc.Type {
		case "auto", "none":
			params["tool_choice"] = map[string]interface{}{"type": tc.Type}
		case "required", "any":
			params["tool_choice"] = map[string]interface{}{"type": "any"}
		case "tool", "function":
			params["tool_choice"] = map[string]interface{}{"type": "tool", "name": tc.ToolName}
		}
	}

	now := time.Now()
	body := map[string]interface{}{
		"config": map[string]interface{}{
			"workingDir":    prof.ProjectDir,
			"date":          now.Format("2006-01-02"),
			"environment":   prof.Platform,
			"structure":     []interface{}{},
			"isGitRepo":     false,
			"currentBranch": "",
			"mainBranch":    "",
			"gitStatus":     "",
			"recentCommits": []interface{}{},
		},
		"memory":         nil,
		"taste":          nil,
		"skills":         nil, // CLI 发 null，不是空串
		"permissionMode": defaultPermission,
		"mode":           p.cliMode(),
		"threadId":       threadIDFor(sessionID), // 官方只接受合法 UUID
		"params":         params,
	}
	return body
}

// ccMessages 把 CPH（OpenAI 形态）消息转成 CC wire 形态：
//   - system/developer → 抽到 params.system 块数组（保留 cache_control 断点，
//     非最后一块补 \n —— 与 CLI 的 composeSystemPrompt 一致）
//   - user → content 块数组（文本 + image_url 转 CC 的 image 块）
//   - assistant → [reasoning?, text, tool-call...] 块数组
//   - tool → tool-result 块（带 toolCallId/toolName/output）
func ccMessages(messages []map[string]interface{}) (systemBlocks []interface{}, out []interface{}) {
	toolNames := map[string]string{}
	for _, m := range messages {
		if str(m["role"]) != "assistant" {
			continue
		}
		if tcs, ok := m["tool_calls"].([]interface{}); ok {
			for _, it := range tcs {
				tc, _ := it.(map[string]interface{})
				fn, _ := tc["function"].(map[string]interface{})
				if id := str(tc["id"]); id != "" {
					toolNames[id] = str(fn["name"])
				}
			}
		}
	}

	for _, m := range messages {
		switch role := str(m["role"]); role {
		case "system", "developer":
			systemBlocks = append(systemBlocks, systemBlocksOf(m["content"])...)
		case "user":
			out = append(out, map[string]interface{}{"role": "user", "content": userBlocks(m["content"])})
		case "assistant":
			parts := []interface{}{}
			if text := contentText(m["content"]); text != "" {
				parts = append(parts, map[string]interface{}{"type": "text", "text": text})
			}
			if tcs, ok := m["tool_calls"].([]interface{}); ok {
				for _, it := range tcs {
					tc, _ := it.(map[string]interface{})
					fn, _ := tc["function"].(map[string]interface{})
					args := fn["arguments"]
					if s, ok := args.(string); ok {
						var parsed interface{}
						if json.Unmarshal([]byte(s), &parsed) == nil {
							args = parsed
						}
					}
					parts = append(parts, map[string]interface{}{
						"type": "tool-call", "toolCallId": str(tc["id"]),
						"toolName": str(fn["name"]), "input": args,
					})
				}
			}
			out = append(out, map[string]interface{}{"role": "assistant", "content": parts})
		case "tool":
			out = append(out, map[string]interface{}{
				"role": "tool",
				"content": []interface{}{map[string]interface{}{
					"type":       "tool-result",
					"toolCallId": m["tool_call_id"],
					"toolName":   firstNonEmpty(toolNames[str(m["tool_call_id"])], str(m["name"])),
					"output":     map[string]interface{}{"type": "text", "value": contentText(m["content"])},
				}},
			})
		default:
			out = append(out, map[string]interface{}{
				"role": "user", "content": []interface{}{map[string]interface{}{"type": "text", "text": str(m["content"])}},
			})
		}
	}
	// 非最后一块补换行（CLI 行为）
	for i := 0; i < len(systemBlocks)-1; i++ {
		if b, ok := systemBlocks[i].(map[string]interface{}); ok {
			b["text"] = str(b["text"]) + "\n"
		}
	}
	return systemBlocks, out
}

// systemBlocksOf system 内容 → 文本块数组（保留 cache_control）。
func systemBlocksOf(content interface{}) []interface{} {
	out := []interface{}{}
	switch v := content.(type) {
	case string:
		if v != "" {
			out = append(out, map[string]interface{}{"type": "text", "text": v})
		}
	case []interface{}:
		for _, it := range v {
			part, _ := it.(map[string]interface{})
			if part == nil {
				continue
			}
			text := firstNonEmpty(str(part["text"]), str(part["content"]))
			cc, hasCC := part["cache_control"]
			if text == "" && !hasCC {
				continue
			}
			block := map[string]interface{}{"type": "text", "text": text}
			if hasCC {
				block["cache_control"] = cc
			}
			out = append(out, block)
		}
	default:
		if v != nil {
			out = append(out, map[string]interface{}{"type": "text", "text": str(v)})
		}
	}
	return out
}

// userBlocks user 内容 → CC 块数组（image_url → image 块，含 mimeType）。
func userBlocks(content interface{}) []interface{} {
	switch v := content.(type) {
	case string:
		return []interface{}{map[string]interface{}{"type": "text", "text": v}}
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, it := range v {
			part, _ := it.(map[string]interface{})
			if part == nil {
				continue
			}
			if str(part["type"]) == "image_url" {
				url := ""
				if img, ok := part["image_url"].(map[string]interface{}); ok {
					url = str(img["url"])
				}
				if url == "" {
					continue
				}
				block := map[string]interface{}{"type": "image", "image": url}
				if i := strings.Index(url, ";"); strings.HasPrefix(url, "data:") && i > 5 {
					block["mimeType"] = url[5:i]
				}
				out = append(out, block)
				continue
			}
			out = append(out, part)
		}
		return out
	default:
		return []interface{}{map[string]interface{}{"type": "text", "text": str(v)}}
	}
}

// ccTools 工具定义 → CC wire 形态（{name, description, input_schema}，无 type 字段），
// 并做官方工具名重写（bash_output→shell_output 等）。
func ccTools(raw interface{}) []interface{} {
	list, _ := raw.([]interface{})
	out := []interface{}{}
	for _, it := range list {
		m, _ := it.(map[string]interface{})
		if m == nil {
			continue
		}
		fn, _ := m["function"].(map[string]interface{})
		name, desc, schema := str(m["name"]), str(m["description"]), m["parameters"]
		if fn != nil {
			name, desc, schema = str(fn["name"]), str(fn["description"]), fn["parameters"]
		}
		if name == "" {
			continue
		}
		if schema == nil {
			schema = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
		}
		out = append(out, map[string]interface{}{
			"name": wireToolName(name), "description": desc, "input_schema": schema,
		})
	}
	return out
}

// sessionIDFor 会话 id：同一会话稳定（X-Task-ID / threadId 都用它）。
func sessionIDFor(req *pb.ChatRequest) string {
	for _, m := range req.Messages {
		if m.Role == "user" && strings.TrimSpace(m.Text) != "" {
			return sha256Hex("sess\x00" + m.Text)[:24]
		}
	}
	return sha256Hex(fmt.Sprintf("sess\x00%d", time.Now().UnixNano()))[:24]
}

// contentText 取消息文本（字符串或块数组拼接）。
func contentText(content interface{}) string {
	switch v := content.(type) {
	case string:
		return v
	case []interface{}:
		var sb strings.Builder
		for _, it := range v {
			if part, ok := it.(map[string]interface{}); ok {
				sb.WriteString(str(part["text"]))
			}
		}
		return sb.String()
	default:
		if v == nil {
			return ""
		}
		return str(v)
	}
}

// mapUpstreamStatus 上游状态 → 核心语义状态。
// USAGE_EXCEEDED（额度用尽）按 402 处理 → 核心暂停账号并换号。
func mapUpstreamStatus(code int, body string) int32 {
	low := strings.ToLower(body)
	switch {
	case code == 402 || strings.Contains(low, "usage_exceeded") || strings.Contains(low, "insufficient_credit"):
		return 402
	case code == 429 || strings.Contains(low, "rate_limit"):
		return 429
	case code == 401 || code == 403:
		return 401
	default:
		return 502
	}
}

// ccStreamGuard 延迟首发（与其它插件同构，便于核心换号重试）。
type ccStreamGuard struct {
	send    func(*pb.StreamEvent) error
	started bool
}

func (g *ccStreamGuard) emit(ev *pb.StreamEvent) {
	switch ev.Event.(type) {
	case *pb.StreamEvent_MessageStart:
		if !g.started {
			return
		}
	case *pb.StreamEvent_ContentDelta, *pb.StreamEvent_ToolCallDelta:
		if !g.started {
			g.started = true
			_ = g.send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{}})
		}
	case *pb.StreamEvent_MessageFinish:
		if !g.started {
			return
		}
	}
	_ = g.send(ev)
}

// ---------- 资料块 ----------

func sectionNote(id, title, detail string) *pb.ProfileSection {
	return &pb.ProfileSection{
		Id:    id,
		Title: map[string]string{"zh": title, "en": title},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "说明", "en": "Note"}, Value: detail},
		},
	}
}

// modelSection 模型清单（详情页动态渲染，展示上下文与上行协议）。
func modelSection(models []ccModel) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "models",
		Title: map[string]string{"zh": "可用模型", "en": "Models"},
		Columns: []*pb.SectionColumn{
			{Key: "id", Title: map[string]string{"zh": "模型", "en": "Model"}},
			{Key: "ctx", Title: map[string]string{"zh": "上下文", "en": "Context"}},
			{Key: "ep", Title: map[string]string{"zh": "上游端点", "en": "Endpoint"}},
		},
	}
	limit := 20
	if len(models) < limit {
		limit = len(models)
	}
	for _, m := range models[:limit] {
		sec.Items = append(sec.Items, &pb.SectionRow{Cells: map[string]string{
			"id":  m.ID,
			"ctx": fmt.Sprintf("%d", m.ContextLength),
			"ep":  strings.Join(m.SupportedEndpoints, ","),
		}})
	}
	return sec
}

// ---------- 工具 ----------

func failed(code int32, msg string) *pb.StreamEvent {
	return &pb.StreamEvent{Event: &pb.StreamEvent_TaskFailed{
		TaskFailed: &pb.TaskFailed{Error: &pb.Error{Code: code, Message: msg}},
	}}
}

func failedDetail(code int32, msg, detail string) *pb.StreamEvent {
	return &pb.StreamEvent{Event: &pb.StreamEvent_TaskFailed{
		TaskFailed: &pb.TaskFailed{Error: &pb.Error{Code: code, Message: msg}, Detail: detail},
	}}
}

func orHint(err error) string {
	if err == nil {
		return "凭据不可用"
	}
	return err.Error()
}
