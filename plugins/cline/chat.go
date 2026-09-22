package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// freeWhitelist 官方免费白名单（参考实现逐条核对；上游目录里不带 :free 后缀但也免费）。
var freeWhitelist = map[string]bool{
	"deepseek/deepseek-v4-flash":            true,
	"deepseek/deepseek-v4-flash-0731":       true,
	"z-ai/glm-5.3-flash":                    true,
	"z-ai/glm-5.2:free":                     true,
	"xiaomi/mimo-v2.5":                      true,
	"minimax/minimax-m3":                    true,
	"poolside/laguna-s-2.1":                 true,
	"cline-free/deepseek-v4.1-flash":        true,
	"cline-free/muse-spark-1.3-contributor": true,
	"cline-free/solar-pro4":                 true,
}

// ---------- 模型目录 ----------

// ListModels 汇总对外可用模型：官方推荐清单（免费 / 通行证）+ 公开目录里的免费白名单。
// 全部免费通道，不需要 credits —— 与参考实现一致，不把 445 条付费目录整段暴露。
func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	client := p.httpClient(cred)
	free, pass, err := fetchRecommended(ctx, client)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	type entry struct {
		id, name, desc string
		tags           []string
	}
	seen := map[string]bool{}
	var list []entry
	add := func(id, name, desc string, tags ...string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		list = append(list, entry{id: id, name: orDefault(name, id), desc: desc, tags: tags})
	}
	for _, m := range free {
		add(m.ID, m.Name, m.Description, "免费")
	}
	for _, m := range pass {
		add(m.ID, m.Name, m.Description, "通行证")
	}
	if all, err := fetchModels(ctx, client); err == nil {
		for _, m := range all {
			id := m.ID
			if strings.HasSuffix(id, ":free") || freeWhitelist[id] {
				add(id, id, "", "免费")
			}
		}
	}
	if len(list) == 0 {
		return nil, status.Error(codes.Unavailable, "上游没有可用模型")
	}
	sort.SliceStable(list, func(i, j int) bool { return list[i].id < list[j].id })

	out := make([]*pb.ModelInfo, 0, len(list))
	for _, e := range list {
		out = append(out, &pb.ModelInfo{
			Id:                     e.id,
			Label:                  map[string]string{"zh": e.name, "en": e.name},
			Description:            e.desc,
			Tags:                   e.tags,
			SupportsTools:          true,
			SupportsStream:         true,
			DefaultReasoningEffort: "high",
		})
	}
	return &pb.ModelList{Models: out}, nil
}

// ---------- 对话 ----------

// Chat 把 CPH 信封转成 Cline /chat/completions 请求。
//
// 关键差异（照抄参考实现的经验）：
//   - 请求体必须剥掉 max_tokens（免费通道带该字段一律 500 empty response content）
//   - 免费通道强制 stream=true（非流式会被上游限流）
//   - 必须带 Cline 指纹头；session_id / X-Task-ID 按会话稳定派生，保住上游缓存亲和
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return stream.Send(failed(401, orHint(err)))
	}
	if req.Model == "" {
		return stream.Send(failed(400, "缺少模型名"))
	}
	client := p.httpClient(cred)
	if accessTokenExpiring(cred) {
		if _, err := refreshClineToken(ctx, client, cred); err != nil {
			return stream.Send(failed(401, "accessToken 刷新失败，需重新授权: "+err.Error()))
		}
	}

	chatBody := openaiup.ChatBody(req)
	sessionID := sessionIDFor(req)
	body := buildClineBody(chatBody, req.Model, sessionID, p.reasoningEffort())
	payload, _ := json.Marshal(body)

	release, err := p.acquire(ctx, req.Model)
	if err != nil {
		return stream.Send(failed(499, "请求已取消: "+err.Error()))
	}
	defer release()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", clineAPIBase+clineChatPath, strings.NewReader(string(payload)))
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	for k, v := range p.clineHeaders(cred, sessionID) {
		httpReq.Header.Set(k, v)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return stream.Send(failed(502, "上游连接失败: "+err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		detail := fmt.Sprintf("HTTP %d %s\n%s", resp.StatusCode, resp.Status, string(raw))
		return stream.Send(failedDetail(mapUpstreamStatus(resp.StatusCode, string(raw)), fmt.Sprintf("HTTP %d: %s", resp.StatusCode, clip(string(raw), 300)), detail))
	}

	// 有效内容统计 + 延迟首发：上游免费通道存在「HTTP 200、全程只有 reasoning、
	// 没有 content」的坏响应（并发/参数风控）。这时把失败作为**首事件**上报，
	// 核心才会按 429 暂停该账号并换号重试；若提前发过 MessageStart 就只会报错给客户端。
	var contentDeltas, toolDeltas int
	started := false
	ensureStart := func() {
		if started {
			return
		}
		started = true
		_ = stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{
			MessageStart: &pb.MessageStart{Model: req.Model},
		}})
	}
	emit := func(ev *pb.StreamEvent) {
		switch ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			contentDeltas++
		case *pb.StreamEvent_ToolCallDelta:
			toolDeltas++
		case *pb.StreamEvent_MessageFinish:
			// 还没见过任何有效内容就收到结束帧 = 上游空响应：先吞掉，交给末尾判定
			if contentDeltas == 0 && toolDeltas == 0 {
				return
			}
		}
		ensureStart()
		_ = stream.Send(ev)
	}
	parser := openaiup.NewParser(emit)
	sc := bufio.NewScanner(unwrapReader(resp.Body))
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		parser.Feed(sc.Text())
	}
	if err := sc.Err(); err != nil {
		// 已被截断：若还没发过任何内容，可换号重试；否则只能如实上报中断
		return stream.Send(failed(502, "上游流中断（无有效内容）: "+err.Error()))
	}
	if contentDeltas == 0 && toolDeltas == 0 {
		return stream.Send(failed(429, "上游返回空内容（免费通道并发/参数风控）：已暂停该账号并换号重试"))
	}
	parser.Finish()
	return nil
}

// buildClineBody 构造上游请求体（含 Cline 独有字段）。
func buildClineBody(chatBody map[string]interface{}, model, sessionID, effort string) map[string]interface{} {
	body := map[string]interface{}{
		"model":            model,
		"session_id":       sessionID,
		"reasoning_effort": orDefault(effort, defaultEffort),
		"stream":           true, // 免费通道必须流式
	}
	if msgs, ok := chatBody["messages"]; ok {
		body["messages"] = msgs
	} else {
		body["messages"] = []interface{}{}
	}
	// 透传可选参数；max_tokens 一律丢弃（上游风控）
	for _, k := range []string{"temperature", "top_p", "tools", "tool_choice", "stop",
		"presence_penalty", "frequency_penalty", "response_format", "user", "n", "seed"} {
		if v, ok := chatBody[k]; ok {
			body[k] = v
		}
	}
	return body
}

// sessionIDFor 由对话内容派生稳定会话 id：同一会话多轮请求保持一致，
// 上游据此做 prompt 缓存亲和；不同会话互不影响。
func sessionIDFor(req *pb.ChatRequest) string {
	seed := ""
	for _, m := range req.Messages {
		if m.Role == "user" && strings.TrimSpace(m.Text) != "" {
			seed = m.Text
			break
		}
	}
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte("cline\x00" + seed))
	return "sess_" + hex.EncodeToString(sum[:12])
}

// mapUpstreamStatus 上游状态 → 核心语义状态。
// 402 = 余额不足（核心会暂停账号并换号）；429 = 限流（核心暂停 10 分钟后自动恢复）。
func mapUpstreamStatus(code int, body string) int32 {
	switch code {
	case 401:
		return 401
	case 402:
		return 402
	case 429:
		return 429
	case 403:
		if strings.Contains(strings.ToLower(body), "credits") {
			return 402
		}
		return 401
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

// modelSection 免费 / 通行证模型清单（详情页动态渲染）。
func modelSection(free, pass []recommendedModel) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "models",
		Title: map[string]string{"zh": "可用模型（免费额度）", "en": "Available models (free)"},
		Columns: []*pb.SectionColumn{
			{Key: "id", Title: map[string]string{"zh": "模型", "en": "Model"}},
			{Key: "kind", Title: map[string]string{"zh": "通道", "en": "Lane"}},
		},
	}
	for _, m := range free {
		sec.Items = append(sec.Items, &pb.SectionRow{Cells: map[string]string{"id": m.ID, "kind": "免费"}})
	}
	for _, m := range pass {
		sec.Items = append(sec.Items, &pb.SectionRow{Cells: map[string]string{"id": m.ID, "kind": "通行证"}})
	}
	if len(sec.Items) == 0 {
		return sectionNote("models", "可用模型", "上游未返回推荐模型清单")
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
