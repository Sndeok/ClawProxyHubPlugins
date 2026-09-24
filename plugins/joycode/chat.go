package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// joyReasoningModels 支持 thinking/reasoning_effort 的模型（对齐参考实现白名单）。
var joyReasoningModels = map[string]bool{
	"GLM-5.1": true, "Kimi-K2.6": true, "MiniMax-M2.7": true,
}

func joyReasoningModel(model string) bool {
	if joyReasoningModels[model] {
		return true
	}
	// 上游目录可能带版本后缀（如 GLM-5.1-0715），按前缀再兜一层
	for name := range joyReasoningModels {
		if strings.HasPrefix(model, name) {
			return true
		}
	}
	return strings.Contains(strings.ToLower(model), "reason")
}

// Chat 把 CPH 信封转成 JoyCode /chat/completions 请求。
//
// 三个上游硬约束（照抄参考实现，抄错会空响应或丢流式）：
//  1. 流式必须 Accept-Encoding: identity，gzip 会整块缓冲（打字机效果消失）
//  2. 请求体在 OpenAI 字段外层再包一层 JoyCode 信封（tenant/userId/client/...）
//  3. 上游 TTFB 可达 10–30s（推理模型），不能按普通 SSE 的节奏判超时
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	return p.joyRunChat(stream.Context(), req, func(ev *pb.StreamEvent) error {
		return stream.Send(ev)
	})
}

// joyRunChat 是 Chat 的可测内核：所有出站逻辑与事件生成都在这里，send 负责投递。
func (p *plugin) joyRunChat(ctx context.Context, req *pb.ChatRequest, send func(*pb.StreamEvent) error) error {
	cred, err := joyCredFrom(req.GetCredential())
	if err != nil {
		return send(joyFailed(401, err.Error()))
	}
	if req.Model == "" {
		return send(joyFailed(400, "缺少模型名"))
	}

	body := openaiup.ChatBody(req)
	// 上游不认 stream_options（参考实现也只从末块 usage 取数）
	delete(body, "stream_options")
	body["stream"] = true
	if msgs, ok := body["messages"]; ok {
		body["messages"] = stripCacheControl(msgs)
	}
	if !joyReasoningModel(req.Model) {
		delete(body, "reasoning_effort")
	}

	start := time.Now()
	resp, rawErr, err := p.joyPostStream(ctx, cred, joyEpChat, body, joyConversationHints(req))
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		detail := fmt.Sprintf("HTTP %d（耗时 %dms）\n%s", statusCode, time.Since(start).Milliseconds(), string(rawErr))
		return send(joyFailedDetail(
			joyMapUpstreamStatus(statusCode, string(rawErr)),
			fmt.Sprintf("上游 HTTP %d: %s", statusCode, joyClip(string(rawErr), 300)),
			detail,
		))
	}
	defer resp.Body.Close()

	reader, closer, err := joyStreamReader(resp)
	if err != nil {
		return send(joyFailed(502, "响应解压失败: "+err.Error()))
	}
	if closer != nil {
		defer closer.Close()
	}
	br := bufio.NewReaderSize(reader, 64*1024)

	// HTTP 200 但整体是 JSON 错误体（上游额度/风控类错误常见形态）：直接上报，
	// 不让它退化成「空响应」而看不出原因。
	if head, perr := br.Peek(1); perr == nil && len(head) > 0 && head[0] == '{' {
		all, _ := io.ReadAll(io.LimitReader(br, 1<<20))
		msg, code := joyDecodeErrorBody(all)
		detail := fmt.Sprintf("HTTP 200（耗时 %dms）\n%s", time.Since(start).Milliseconds(), string(all))
		return send(joyFailedDetail(joyMapBusinessCode(code), msg, detail))
	}

	// 延迟首发：上游在额度/风控异常时可能全程只回错误帧或空流。失败必须作为
	// **首事件**上报，核心才会按 429/402 暂停该账号并换号重试。
	var contentDeltas, toolDeltas int
	started := false
	upstreamErr := ""
	ensureStart := func() {
		if started {
			return
		}
		started = true
		_ = send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{
			MessageStart: &pb.MessageStart{Model: req.Model},
		}})
	}
	emit := func(ev *pb.StreamEvent) {
		switch ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			// 思考增量同样算「上游有响应」：只有思考没有正文（例如 max_tokens 太小、预算被思考吃光）
			// 不是空响应，绝不能按 429 暂停账号。正文/思考的区分由核心按入口协议渲染。
			contentDeltas++
		case *pb.StreamEvent_ToolCallDelta:
			toolDeltas++
		case *pb.StreamEvent_MessageFinish:
			if contentDeltas == 0 && toolDeltas == 0 {
				return // 空响应的结束帧先吞掉，交给末尾判定
			}
		}
		ensureStart()
		_ = send(ev)
	}
	parser := openaiup.NewParser(emit)
	pumpErr := joyPumpSSE(br, func(line string) {
		if upstreamErr == "" {
			upstreamErr = joySSEErrorText(line)
		}
		parser.Feed(line)
	})

	if pumpErr != nil {
		if contentDeltas == 0 && toolDeltas == 0 {
			detail := fmt.Sprintf("流读取中断（耗时 %dms）", time.Since(start).Milliseconds())
			if upstreamErr != "" {
				detail += "\n上游错误帧: " + upstreamErr
			}
			return send(joyFailedDetail(502, "上游流中断（无有效内容）: "+pumpErr.Error(), detail))
		}
		return send(joyFailedDetail(502, "上游流中断: "+pumpErr.Error(), upstreamErr))
	}

	if contentDeltas == 0 && toolDeltas == 0 {
		detail := fmt.Sprintf("HTTP 200 空响应（耗时 %dms）", time.Since(start).Milliseconds())
		if upstreamErr != "" {
			detail += "\n上游错误帧: " + upstreamErr
		}
		msg := "上游返回空内容：已暂停该账号并换号重试"
		if upstreamErr != "" {
			msg = "上游报错：" + joyClip(upstreamErr, 200)
		}
		return send(joyFailedDetail(joyMapErrorText(upstreamErr), msg, detail))
	}
	parser.Finish()
	return nil
}

// joySSEErrorText 从 SSE 行里提取错误信息（data: {"error":...} / code!=0 / message）。
func joySSEErrorText(line string) string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return ""
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if payload == "" || payload == "[DONE]" || !strings.HasPrefix(payload, "{") {
		return ""
	}
	var probe struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
		Msg     string          `json:"msg"`
		Code    *float64        `json:"code"`
	}
	if json.Unmarshal([]byte(payload), &probe) != nil {
		return ""
	}
	if len(probe.Error) > 0 && string(probe.Error) != "null" {
		msg, _ := joyDecodeErrorBody(probe.Error)
		if msg == "" {
			msg = string(probe.Error)
		}
		return msg
	}
	if probe.Code != nil && *probe.Code != 0 {
		return joyClip(firstNonEmpty(probe.Msg, probe.Message, payload), 500)
	}
	if probe.Message != "" {
		return joyClip(probe.Message, 500)
	}
	return ""
}

// joyDecodeErrorBody 从错误体里取人类可读信息与业务码。
func joyDecodeErrorBody(raw []byte) (string, float64) {
	var probe struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
		Msg     string          `json:"msg"`
		Code    *float64        `json:"code"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return joyClip(strings.TrimSpace(string(raw)), 500), 0
	}
	msg := firstNonEmpty(probe.Msg, probe.Message)
	code := float64(0)
	if probe.Code != nil {
		code = *probe.Code
	}
	if len(probe.Error) > 0 && string(probe.Error) != "null" {
		var e struct {
			Message string   `json:"message"`
			Code    *float64 `json:"code"`
		}
		if json.Unmarshal(probe.Error, &e) == nil {
			msg = firstNonEmpty(msg, e.Message)
			if e.Code != nil {
				code = *e.Code
			}
		}
		if msg == "" {
			msg = string(probe.Error)
		}
	}
	if msg == "" {
		msg = joyClip(strings.TrimSpace(string(raw)), 500)
	}
	return msg, code
}

// joyMapUpstreamStatus 上游 HTTP 状态 → 核心语义状态。
// 402 = 额度不足（核心暂停账号）；429 = 限流（核心暂停 10 分钟后自动恢复）。
func joyMapUpstreamStatus(status int, body string) int32 {
	lower := strings.ToLower(body)
	switch status {
	case 401:
		return 401
	case 402:
		return 402
	case 429:
		return 429
	case 403:
		if strings.Contains(lower, "credit") || strings.Contains(lower, "quota") || strings.Contains(lower, "额度") {
			return 402
		}
		return 401
	}
	switch {
	case strings.Contains(lower, "pt_key") && strings.Contains(lower, "expire"):
		return 401
	case strings.Contains(lower, "unauthorized") || strings.Contains(lower, "未登录") || strings.Contains(lower, "登录已过期"):
		return 401
	case strings.Contains(lower, "quota") || strings.Contains(lower, "额度") || strings.Contains(lower, "余额"):
		return 402
	case strings.Contains(lower, "rate limit") || strings.Contains(lower, "too many") || strings.Contains(lower, "限流"):
		return 429
	}
	return 502
}

// joyMapBusinessCode 业务码 → 核心语义状态。
func joyMapBusinessCode(code float64) int32 {
	switch code {
	case 401, 4001:
		return 401
	case 402, 4030:
		return 402
	case 429:
		return 429
	}
	return 502
}

// joyMapErrorText 只能靠文案判断时的语义状态映射。
func joyMapErrorText(text string) int32 {
	return joyMapUpstreamStatus(0, text)
}

// ---------- 事件 / 工具 ----------

func joyFailed(code int32, msg string) *pb.StreamEvent {
	return &pb.StreamEvent{Event: &pb.StreamEvent_TaskFailed{
		TaskFailed: &pb.TaskFailed{Error: &pb.Error{Code: code, Message: msg}},
	}}
}

func joyFailedDetail(code int32, msg, detail string) *pb.StreamEvent {
	return &pb.StreamEvent{Event: &pb.StreamEvent_TaskFailed{
		TaskFailed: &pb.TaskFailed{Error: &pb.Error{Code: code, Message: msg}, Detail: detail},
	}}
}

// stripCacheControl 递归移除内容块上的 cache_control。
// JoyCode 走标准 OpenAI 形态，严格校验时多余字段会 400。
func stripCacheControl(v interface{}) interface{} {
	switch x := v.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(x))
		for _, it := range x {
			out = append(out, stripCacheControl(it))
		}
		return out
	case []map[string]interface{}:
		out := make([]interface{}, 0, len(x))
		for _, it := range x {
			out = append(out, stripCacheControl(it))
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, vv := range x {
			if k == "cache_control" {
				continue
			}
			out[k] = stripCacheControl(vv)
		}
		return out
	default:
		return v
	}
}
