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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/anthropicup"
	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
	"github.com/Sndeok/ClawProxyHub-Next/sdk/responsesup"
)

// anonCoreTools 匿名免费通道要求的 5 个核心工具名（官方 CLI 的默认工具集）。
// 只认名字，不校验参数：缺任一个就 403 FreeTierError。
var anonCoreTools = []string{"bash", "edit", "glob", "grep", "read"}

// ---------- 模型目录 ----------

func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	models, err := p.listModels(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	out := make([]*pb.ModelInfo, 0, len(models))
	for _, m := range models {
		tags := []string{protocolLabel(m.Protocol)}
		if m.Free {
			tags = append(tags, "免费")
		}
		if m.CostMultiplier > 0 {
			tags = append(tags, "x"+trimFloat(m.CostMultiplier))
		}
		for _, in := range m.Tags {
			if in != "text" {
				tags = append(tags, in)
			}
		}
		def := ""
		if len(m.Reasoning) > 0 {
			def = pickEffort(m.Reasoning)
		}
		out = append(out, &pb.ModelInfo{
			Id:                     m.ID,
			Label:                  map[string]string{"zh": m.Name, "en": m.Name},
			ContextWindow:          int32(m.Context),
			Description:            m.Description,
			Tags:                   tags,
			SupportsTools:          true,
			SupportsStream:         true,
			CreditsMultiplier:      m.CostMultiplier,
			ReasoningEfforts:       m.Reasoning,
			DefaultReasoningEffort: def,
		})
	}
	return &pb.ModelList{Models: out}, nil
}

func protocolLabel(p string) string {
	switch p {
	case protocolMsgs:
		return "Anthropic 方言"
	case protocolResp:
		return "Responses 方言"
	default:
		return "Chat 方言"
	}
}

func pickEffort(values []string) string {
	for _, want := range []string{"high", "medium", "low", "max"} {
		for _, v := range values {
			if v == want {
				return v
			}
		}
	}
	if len(values) > 0 {
		return values[len(values)-1]
	}
	return ""
}

func trimFloat(f float64) string {
	return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.2f", f), "00"), ".")
}

// ---------- 对话 ----------

// Chat 把 CPH 信封转成 OpenCode Zen 请求：
// 按模型原生协议选路，套上官方 CLI 的关联头与会话形态，再按协议解析回统一事件。
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return stream.Send(failed(401, orHint(err)))
	}
	if req.Model == "" {
		return stream.Send(failed(400, "缺少模型名"))
	}
	protocol := p.protocolFor(ctx, cred, req.Model)
	sessionSeed := conversationSeed(req)
	sessionID := canonicalSession(sessionSeed)
	free := cred.Tier == tierAnon || p.isFreeModel(ctx, cred, req.Model)

	var body map[string]interface{}
	switch protocol {
	case protocolMsgs:
		body = anthropicup.ChatBody(req)
	case protocolResp:
		body = responsesup.ChatBody(req)
	default:
		body = openaiup.ChatBody(req)
	}
	body["model"] = req.Model
	// 上游只服务流式；免费/匿名通道尤其如此（非流式会被拒）。
	body["stream"] = true
	if free {
		// 匿名通道要求「agent 形状」：必须带核心工具名，缺了 403 FreeTierError
		if cred.Tier == tierAnon && p.injectAnonTools() {
			ensureAnonTools(body, protocol)
		}
	}
	if effort := p.effortFor(req); effort != "" && protocol != protocolResp {
		// Responses 方言没有顶层 reasoning_effort（Codex 会发上游不认的私有值）
		body["reasoning_effort"] = effort
	}
	payload, _ := json.Marshal(body)

	rawURL := baseFor(cred.Tier) + pathFor(protocol)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", rawURL, strings.NewReader(string(payload)))
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	for k, v := range p.headers(cred, protocol, sessionID, requestID()) {
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

	// 延迟首发 + 空响应识别：只在拿到第一段有效内容后才发 MessageStart，
	// 这样「HTTP 200 但空流」能作为首事件报 429，核心会暂停该账号并换号重试。
	guard := &streamGuard{send: func(ev *pb.StreamEvent) error { return stream.Send(ev) }}
	var parser interface {
		Feed(string)
		Finish()
		FinishWithError(int32, string)
	}
	switch protocol {
	case protocolMsgs:
		parser = anthropicup.NewParser(guard.emit)
	case protocolResp:
		parser = responsesup.NewParser(guard.emit)
	default:
		parser = openaiup.NewParser(guard.emit)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		parser.Feed(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return stream.Send(failed(502, "上游流中断: "+err.Error()))
	}
	if !guard.started {
		return stream.Send(failed(429, "上游返回空内容（免费通道限流），已暂停该账号并换号重试"))
	}
	parser.Finish()
	return nil
}

// streamGuard 延迟首发：吞掉「没有任何内容」的 MessageStart/MessageFinish，
// 让空响应能以首事件失败的形式上报（核心只对首事件失败做换号重试）。
type streamGuard struct {
	send    func(*pb.StreamEvent) error
	started bool
}

func (g *streamGuard) emit(ev *pb.StreamEvent) {
	switch ev.Event.(type) {
	case *pb.StreamEvent_MessageStart:
		if !g.started {
			return // 等第一段有效内容
		}
	case *pb.StreamEvent_ContentDelta, *pb.StreamEvent_ToolCallDelta:
		if !g.started {
			g.started = true
			_ = g.send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{}})
		}
	case *pb.StreamEvent_MessageFinish:
		if !g.started {
			return // 空响应的结束帧：交给调用方判定
		}
	}
	_ = g.send(ev)
}

// ensureAnonTools 补齐匿名通道要求的 5 个核心工具（已存在的不动）。
func ensureAnonTools(body map[string]interface{}, protocol string) {
	existing := map[string]bool{}
	if raw, ok := body["tools"].([]interface{}); ok {
		for _, it := range raw {
			m, _ := it.(map[string]interface{})
			if m == nil {
				continue
			}
			if nested, ok := m["function"].(map[string]interface{}); ok {
				existing[str(nested["name"])] = true
				continue
			}
			existing[str(m["name"])] = true
		}
	}
	tools, _ := body["tools"].([]interface{})
	for _, name := range anonCoreTools {
		if existing[name] {
			continue
		}
		schema := map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
		switch protocol {
		case protocolMsgs:
			tools = append(tools, map[string]interface{}{
				"name": name, "description": name + " tool", "input_schema": schema,
			})
		case protocolResp:
			tools = append(tools, map[string]interface{}{
				"type": "function", "name": name, "description": name + " tool", "parameters": schema,
			})
		default:
			tools = append(tools, map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name": name, "description": name + " tool", "parameters": schema,
				},
			})
		}
	}
	body["tools"] = tools
}

// effortFor 取推理强度：客户端 extra 优先，其次插件设置，最后不发。
func (p *plugin) effortFor(req *pb.ChatRequest) string {
	if v := strings.TrimSpace(req.Extra["reasoning_effort"]); v != "" {
		return v
	}
	return p.settingStr("reasoning_effort", "")
}

// conversationSeed 会话种子：首条 user 文本（多轮前缀不变 → 同会话稳定）。
func conversationSeed(req *pb.ChatRequest) string {
	for _, m := range req.Messages {
		if m.Role == "user" && strings.TrimSpace(m.Text) != "" {
			return m.Text
		}
	}
	return requestID()
}

// mapUpstreamStatus 上游状态 → 核心语义状态。
// 匿名/免费通道的 403（FreeTierError）按凭证问题处理，让核心换号重试。
func mapUpstreamStatus(code int, body string) int32 {
	switch code {
	case 401, 403:
		return 401
	case 402:
		return 402
	case 429:
		return 429
	default:
		return 502
	}
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

// modelSection 通道 + 免费模型清单（详情页动态渲染）。
func modelSection(cred *openCred, models []zenModel) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "models",
		Title: map[string]string{"zh": "通道与模型", "en": "Tier & models"},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "通道", "en": "Tier"}, Value: tierLabel(cred.Tier), Kind: "status"},
			{Label: map[string]string{"zh": "可见模型", "en": "Models"}, Value: fmt.Sprintf("%d 个", len(models))},
		},
		Columns: []*pb.SectionColumn{
			{Key: "id", Title: map[string]string{"zh": "模型", "en": "Model"}},
			{Key: "protocol", Title: map[string]string{"zh": "协议", "en": "Protocol"}},
			{Key: "kind", Title: map[string]string{"zh": "类型", "en": "Type"}},
		},
	}
	limit := 12
	if len(models) < limit {
		limit = len(models)
	}
	sorted := append([]zenModel(nil), models...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Free != sorted[j].Free {
			return sorted[i].Free
		}
		return sorted[i].ID < sorted[j].ID
	})
	for _, m := range sorted[:limit] {
		kind := "付费"
		if m.Free {
			kind = "免费"
		}
		sec.Items = append(sec.Items, &pb.SectionRow{Cells: map[string]string{
			"id": m.ID, "protocol": protocolLabel(m.Protocol), "kind": kind,
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
