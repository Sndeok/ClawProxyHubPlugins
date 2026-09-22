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
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// ---------- 模型目录 ----------

func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	models, err := p.fetchModels(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	sort.SliceStable(models, func(i, j int) bool { return models[i].Key < models[j].Key })
	out := make([]*pb.ModelInfo, 0, len(models))
	for _, m := range models {
		var tags []string
		if m.IsDefault {
			tags = append(tags, "默认")
		}
		if m.IsReasoning {
			tags = append(tags, "支持推理")
		}
		if m.PriceFactor > 0 {
			tags = append(tags, "x"+ftoa(m.PriceFactor))
		}
		dflt := ""
		if m.IsReasoning {
			dflt = "high"
		}
		out = append(out, &pb.ModelInfo{
			Id:                     m.Key,
			Label:                  map[string]string{"zh": m.Display(), "en": m.Display()},
			ContextWindow:          int32(minInt(m.Context(), 1<<31-1)),
			SupportsTools:          true,
			SupportsStream:         true,
			CreditsMultiplier:      m.PriceFactor,
			Tags:                   tags,
			DefaultReasoningEffort: dflt,
		})
	}
	return &pb.ModelList{Models: out}, nil
}

// ---------- 对话 ----------

// Chat 把 CPH 信封转成 Qoder agent 请求：构造 body → QoderEncoding → COSY 签名 → 嵌套 SSE → 标准 chunk。
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return stream.Send(failed(401, orHint(err)))
	}
	if req.Model == "" {
		return stream.Send(failed(400, "缺少模型名"))
	}
	chatBody := openaiup.ChatBody(req)
	envelope := buildQoderBody(chatBody, req.Model, cred, req.MaxTokens)
	encoded, err := qodersign.Encode(envelope)
	if err != nil {
		return stream.Send(failed(500, "请求编码失败: "+err.Error()))
	}

	sess, err := p.sessionFor(cred)
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	ep := endpointsFor(cred.Region)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", ep.ChatStream, strings.NewReader(encoded))
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	if err := sess.ApplyHeaders(httpReq, headerCfg(), encoded, cred.UID, req.Model); err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	resp, err := p.httpClient(cred).Do(httpReq)
	if err != nil {
		return stream.Send(failed(502, "上游连接失败: "+err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		detail := fmt.Sprintf("HTTP %d %s\n%s", resp.StatusCode, resp.Status, string(raw))
		return stream.Send(failedDetail(mapUpstreamStatus(resp.StatusCode), fmt.Sprintf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 300)), detail))
	}

	if err := stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{
		MessageStart: &pb.MessageStart{Model: req.Model},
	}}); err != nil {
		return err
	}

	parser := openaiup.NewParser(func(ev *pb.StreamEvent) { _ = stream.Send(ev) })
	scanner := bufio.NewScanner(qodersign.WrapNested(resp.Body))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	sawEvent := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") && !strings.Contains(line, "[DONE]") {
			sawEvent = true
		}
		parser.Feed(line)
	}
	if err := scanner.Err(); err != nil {
		var ee *qodersign.EnvelopeError
		if asEnvelopeError(err, &ee) {
			// HTTP 200 建流后 provider 报错（418/5xx）：交核心按瞬时故障换号
			return stream.Send(failed(mapUpstreamStatus(ee.StatusCode), ee.Error()))
		}
		parser.FinishWithError(502, "上游流中断: "+err.Error())
		return nil
	}
	if !sawEvent {
		parser.FinishWithError(502, "上游返回了空流")
		return nil
	}
	parser.Finish()
	return nil
}

// buildQoderBody 构造 agent_chat_generation 请求体。
//
// 骨架取自桌面端模板（baseprompt.json）的静态字段，但**不带**模板里的
// 3 条 system 消息与 14 个工具定义——客户端（Codex 等）自己会带；
// 不注入可让 baseline prompt 从 ~10K token 降到几十 token。
func buildQoderBody(chatBody map[string]interface{}, modelKey string, cred *qoderCred, maxTokens int32) []byte {
	prompt := lastUserPrompt(chatBody)
	now := time.Now()
	uuid := randomUUID()
	maxTok := int32(32768)
	if maxTokens > 0 {
		maxTok = maxTokens
	}
	modelCfg := map[string]interface{}{
		"key": modelKey, "display_name": modelKey, "model": "", "format": "openai",
		"is_vl": false, "is_reasoning": false, "api_key": "", "url": "",
		"source": "system", "max_input_tokens": 180000,
	}
	body := map[string]interface{}{
		"request_id":     uuid,
		"request_set_id": randomUUID(),
		"chat_record_id": uuid,
		"stream":         true,
		"chat_task":      "FREE_INPUT",
		"chat_context": map[string]interface{}{
			"chatPrompt": "",
			"extra": map[string]interface{}{
				"context":         []interface{}{},
				"modelConfig":     map[string]interface{}{"key": modelKey, "is_reasoning": false},
				"originalContent": map[string]interface{}{"type": "text", "text": prompt},
			},
			"features":  []interface{}{},
			"imageUrls": nil,
			"text":      map[string]interface{}{"type": "text", "text": prompt},
		},
		"image_urls":       nil,
		"is_reply":         true,
		"is_retry":         false,
		"session_id":       randomUUID(),
		"code_language":    "",
		"source":           1,
		"version":          "3",
		"chat_prompt":      "",
		"parameters":       map[string]interface{}{"max_tokens": maxTok},
		"aliyun_user_type": orDefault(cred.UserType, "personal_standard"),
		"session_type":     "qoder",
		"agent_id":         "agent_common",
		"task_id":          "common",
		"model_config":     modelCfg,
		"business": map[string]interface{}{
			"product": "ide", "version": "1.1.3", "type": "agent",
			"id": randomUUID(), "name": truncateRunes(prompt, 30),
			"begin_at": now.UnixMilli(), "stage": "start",
		},
	}
	if msgs, ok := chatBody["messages"]; ok {
		body["messages"] = msgs
	} else {
		body["messages"] = []interface{}{}
	}
	if tools, ok := chatBody["tools"]; ok {
		body["tools"] = tools // 客户端传了才带
	}
	b, _ := json.Marshal(body)
	return b
}

func lastUserPrompt(chatBody map[string]interface{}) string {
	if raw, ok := chatBody["messages"].([]map[string]interface{}); ok {
		for i := len(raw) - 1; i >= 0; i-- {
			if str(raw[i]["role"]) != "user" {
				continue
			}
			if s := contentText(raw[i]["content"]); s != "" {
				return s
			}
		}
		return ""
	}
	if raw, ok := chatBody["messages"].([]interface{}); ok {
		for i := len(raw) - 1; i >= 0; i-- {
			m, _ := raw[i].(map[string]interface{})
			if m == nil || str(m["role"]) != "user" {
				continue
			}
			if s := contentText(m["content"]); s != "" {
				return s
			}
		}
	}
	return ""
}

func contentText(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []interface{}:
		var sb strings.Builder
		for _, part := range x {
			if m, ok := part.(map[string]interface{}); ok {
				sb.WriteString(str(m["text"]))
			}
		}
		return sb.String()
	}
	return ""
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// mapUpstreamStatus 上游状态 → 核心语义状态（401 换号 / 402 暂停 / 429 暂停 10 分钟）。
func mapUpstreamStatus(code int) int32 {
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

// ---------- 任务：每日签到 ----------

func (p *plugin) ListTaskCapabilities(ctx context.Context, _ *pb.Empty) (*pb.TaskCapabilities, error) {
	return &pb.TaskCapabilities{Capabilities: []*pb.TaskCapability{
		{
			Id: "checkin", Label: map[string]string{"zh": "每日签到（活动领取）", "en": "Daily Check-in"},
			Kind: "recurring", PerAccount: true, DefaultSchedule: "daily 09:10",
		},
	}}, nil
}

func (p *plugin) RunTask(ctx context.Context, req *pb.RunTaskRequest) (*pb.RunTaskResponse, error) {
	if req.CapabilityId != "checkin" {
		return nil, status.Error(codes.NotFound, "未知任务能力: "+req.CapabilityId)
	}
	cred, err := credFrom(req.Credential)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ep := endpointsFor(cred.Region)
	client := p.httpClient(cred)
	res, err := claimDailyCheckin(ctx, client, ep, cred.DT)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	detail, _ := json.Marshal(map[string]interface{}{
		"claimed": res.Claimed, "amount": res.Amount, "detail": res.Detail, "region": ep.Region,
	})
	return &pb.RunTaskResponse{Summary: res.Detail, DetailJson: string(detail)}, nil
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

// planSection 套餐 / 额度块（含重置时间与用尽状态）。
func planSection(q *quotaInfo) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "quota",
		Title: map[string]string{"zh": "额度", "en": "Quota"},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "剩余", "en": "Remaining"}, Value: fmt.Sprintf("%d", q.Remaining())},
			{Label: map[string]string{"zh": "总量", "en": "Total"}, Value: fmt.Sprintf("%d", q.Total())},
			{Label: map[string]string{"zh": "已用", "en": "Used"}, Value: fmt.Sprintf("%d", q.Used())},
		},
	}
	if q.Plan != "" {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "套餐", "en": "Plan"}, Value: q.Plan,
		})
	}
	if q.ResetTime != "" {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "重置时间", "en": "Resets"}, Value: q.ResetTime,
		})
	}
	if q.Exceeded {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "额度状态", "en": "Status"}, Value: "额度已用尽", Kind: "status",
		})
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// asEnvelopeError 判断 scanner 错误链里是否含信封错误。
func asEnvelopeError(err error, target **qodersign.EnvelopeError) bool {
	for err != nil {
		if ee, ok := err.(*qodersign.EnvelopeError); ok {
			*target = ee
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
