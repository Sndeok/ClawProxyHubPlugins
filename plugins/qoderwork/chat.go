package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// ---------- 模型目录 ----------

// dynamicModel 上游 chat scene 的模型条目。
type dynamicModel struct {
	Key            string                   `json:"key"`
	DisplayName    string                   `json:"display_name"`
	Enable         bool                     `json:"enable"`
	IsReasoning    bool                     `json:"is_reasoning"`
	IsVL           bool                     `json:"is_vl"`
	MaxInputTokens int64                    `json:"max_input_tokens"`
	PriceFactor    float64                  `json:"price_factor"`
	ContextConfig  map[string]contextOption `json:"context_config,omitempty"`
}

type contextOption struct {
	TokenCount int64 `json:"token_count"`
	IsDefault  bool  `json:"is_default"`
}

// MaxContext 取 context_config 里最大档，回退 max_input_tokens。
func (m dynamicModel) MaxContext() int64 {
	var max int64
	for _, opt := range m.ContextConfig {
		if opt.TokenCount > max {
			max = opt.TokenCount
		}
	}
	if max > 0 {
		return max
	}
	return m.MaxInputTokens
}

func (m dynamicModel) Display() string {
	if strings.TrimSpace(m.DisplayName) != "" {
		return m.DisplayName
	}
	return m.Key
}

func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	cred, err := credFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	models, err := p.fetchModels(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	out := make([]*pb.ModelInfo, 0, len(models))
	for _, m := range models {
		out = append(out, &pb.ModelInfo{
			Id:                     m.Key,
			Label:                  map[string]string{"zh": m.Display(), "en": m.Display()},
			ContextWindow:          int32(min64(m.MaxContext(), 1<<31-1)),
			SupportsTools:          true,
			SupportsStream:         true,
			CreditsMultiplier:      m.PriceFactor,
			Tags:                   modelTags(m),
			DefaultReasoningEffort: defaultEffort(m),
		})
	}
	if p.host != nil {
		// 模型 key 诊断：客户端里显示的模型名必须与这里的 key 完全一致，
		// 否则对话会因 key 无效被上游返回空流（docker logs 里搜「模型目录」）。
		keys := make([]string, 0, len(out))
		for _, m := range out {
			keys = append(keys, m.Id)
		}
		p.host.Log("info", fmt.Sprintf("模型目录 %d 个（上游原始 key）：%s", len(keys), strings.Join(keys, ", ")))
	}
	return &pb.ModelList{Models: out}, nil
}

// fetchModels COSY 签名 GET 模型目录（优先 qwork 场景，其次 chat 场景，仅启用项）。
//
// 路径来自千问办公自带的 Qoder CLI（resources\bin\qoderclicn.exe）里的真实实现：
//
//	/api/v2/model/list?Encode=1       ← 模型服务真实路径
//	/algo/api/v2/model/list?Encode=1  ← 旧前缀；两套签名一致（签名走的是去掉 /algo 的路径），
//	                                    但对 qwenworkcn 上游会返回
//	                                    503 {"code":"503","message":"Model catalog unavailable"}
//
// 所以按顺序各试一次：新路径优先，失败回退旧路径，Qoder / 千问办公两套部署都能用。
func (p *plugin) fetchModels(ctx context.Context, cred *accountCred) ([]dynamicModel, error) {
	if err := p.fillFingerprint(cred); err != nil {
		return nil, err
	}
	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return nil, err
	}
	// 依次尝试：插件设置的自定义路径 → 真实路径 → 旧前缀。
	// 自定义路径写错（例如误填 /algo 旧前缀）时仍能自动回退，不会把目录拉挂。
	paths := make([]string, 0, 3)
	addPath := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		for _, e := range paths {
			if e == v {
				return
			}
		}
		paths = append(paths, v)
	}
	addPath(p.settingStr("models_path", ""))
	addPath(modelsPath)
	addPath(modelsPathOld)
	var firstErr error
	for _, path := range paths {
		models, err := p.fetchModelsAt(ctx, cred, sess, path)
		if err == nil {
			if p.host != nil {
				p.host.Log("info", "模型目录来自 "+path)
			}
			return models, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if p.host != nil {
			p.host.Log("warn", "模型目录 "+path+" 失败："+err.Error())
		}
	}
	return nil, firstErr
}

// fetchModelsAt 单次取目录 + 解析（scene 优先 qwork，其次 chat，只取启用项）。
func (p *plugin) fetchModelsAt(ctx context.Context, cred *accountCred, sess *qodersign.Session, path string) ([]dynamicModel, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", p.gatewayBaseURL()+path, nil)
	if err != nil {
		return nil, err
	}
	if err := sess.ApplyHeaders(req, p.headerCfg(), "", cred.UID, ""); err != nil {
		return nil, err
	}
	// 千问办公客户端对网关 REST 请求都会带这组头；只补 X-QwenWork-*/X-Request-Id，
	// 不动 COSY 已设好的 Accept / User-Agent。
	p.applyQwenWorkHeaders(req)
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("模型目录 HTTP %d: %s", resp.StatusCode, clip(string(raw), 300))
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("模型目录解析失败: %w", err)
	}
	scene := ""
	var models []dynamicModel
	for _, name := range []string{"qwork", "chat"} {
		rawScene, ok := top[name]
		if !ok {
			continue
		}
		var parsed []dynamicModel
		if json.Unmarshal(rawScene, &parsed) != nil || len(parsed) == 0 {
			continue
		}
		scene, models = name, parsed
		break
	}
	if scene == "" {
		scenes := make([]string, 0, len(top))
		for k := range top {
			scenes = append(scenes, k)
		}
		sort.Strings(scenes)
		return nil, fmt.Errorf("模型目录缺少 qwork/chat 场景（上游返回场景: %s）", strings.Join(scenes, ","))
	}
	enabled := make([]dynamicModel, 0, len(models))
	for _, m := range models {
		if m.Enable && m.Key != "" {
			enabled = append(enabled, m)
		}
	}
	if len(enabled) == 0 {
		return nil, fmt.Errorf("上游没有启用的 %s 模型", scene)
	}
	sort.SliceStable(enabled, func(i, j int) bool { return enabled[i].Key < enabled[j].Key })
	return enabled, nil
}

func modelTags(m dynamicModel) []string {
	var tags []string
	if m.IsReasoning {
		tags = append(tags, "支持推理")
	}
	if m.IsVL {
		tags = append(tags, "多模态")
	}
	if m.PriceFactor > 0 {
		tags = append(tags, fmt.Sprintf("x%s", trimFloat(m.PriceFactor)))
	}
	return tags
}

func defaultEffort(m dynamicModel) string {
	if !m.IsReasoning {
		return ""
	}
	return "high"
}

// ---------- 对话 ----------

const defaultUserType = "personal_professional_trial"

// Chat 把 CPH 信封转成 QoderWork agent 请求：构造 body → QoderEncoding → COSY 签名 → 嵌套 SSE → 标准 chunk。
func (p *plugin) Chat(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) error {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return stream.Send(failed(401, orHint(err)))
	}
	if err := p.fillFingerprint(cred); err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	if needRefresh(cred) {
		if err := p.refreshDeviceToken(ctx, p.httpClient(cred), cred); err != nil {
			return stream.Send(failed(401, "设备令牌刷新失败，需重新授权: "+err.Error()))
		}
	}

	body := openaiup.ChatBody(req)
	modelKey := req.Model
	if modelKey == "" {
		return stream.Send(failed(400, "缺少模型名"))
	}
	envelope := p.buildAgentBody(body, modelKey, cred)
	encoded, err := qodersign.Encode(envelope)
	if err != nil {
		return stream.Send(failed(500, "请求编码失败: "+err.Error()))
	}

	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	rawURL := p.gatewayBaseURL() + p.settingStr("chat_path", chatPath)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", rawURL, strings.NewReader(encoded))
	if err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	if err := sess.ApplyHeaders(httpReq, p.headerCfg(), encoded, cred.UID, modelKey); err != nil {
		return stream.Send(failed(500, err.Error()))
	}
	// 流式必须 identity：gzip 会把 SSE 缓冲成一次性下发（打字机效果消失）
	httpReq.Header.Set("Accept-Encoding", "identity")
	// 千问办公客户端对网关的所有请求（含对话）都带 X-QwenWork-* + X-Request-Id。
	// 实测缺失时上游会回 503 {"code":"503","message":"Model catalog unavailable"}。
	p.applyQwenWorkHeaders(httpReq)
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

	// 延迟首发：拿到第一段有效内容才发 MessageStart。上游空流 / 建流后报错时必须把失败
	// 作为**首事件**上报，核心才会按 429 暂停该账号并换号重试；提前发过 MessageStart 只会报错。
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
	parser := openaiup.NewParser(func(ev *pb.StreamEvent) {
		switch ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			contentDeltas++
		case *pb.StreamEvent_ToolCallDelta:
			toolDeltas++
		case *pb.StreamEvent_MessageFinish:
			if contentDeltas == 0 && toolDeltas == 0 {
				return // 空响应的结束帧先吞掉，交给末尾判定
			}
		}
		ensureStart()
		_ = stream.Send(ev)
	})
	// 抓上游原始响应开头（最多 2KB）：空流时能看清上游到底回了什么格式。
	// TeeReader 包在 WrapNested 之前，拿到的是未解包的原始字节。
	tap := &limitedWriter{max: 2048}
	scanner := bufio.NewScanner(qodersign.WrapNested(io.TeeReader(resp.Body, tap)))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		parser.Feed(scanner.Text())
	}
	empty := contentDeltas == 0 && toolDeltas == 0
	if err := scanner.Err(); err != nil {
		// 信封错误（HTTP 200 建流后 provider 报错）单独映射，便于核心换号
		var ee *qodersign.EnvelopeError
		if asEnvelopeError(err, &ee) {
			if empty {
				return stream.Send(failed(mapUpstreamStatus(ee.StatusCode), ee.Error()))
			}
			return stream.Send(failedDetail(mapUpstreamStatus(ee.StatusCode), "上游建流后报错: "+ee.Error(), ee.Error()))
		}
		if empty {
			return stream.Send(failed(502, "上游流中断（无有效内容）: "+err.Error()))
		}
		parser.FinishWithError(502, "上游流中断: "+err.Error())
		return nil
	}
	if empty {
		// 空流诊断（内联，避免跨插件共享类型）：既写核心日志（ASCII 前缀便于 grep），
		// 也塞进错误详情（前端「日志 → 详情」直接可见）。最关键一条是 inCatalog：
		// 本次请求的模型 key 是否真在账号模型目录里 —— 不在时上游通常回 200 + 空流。
		keys := make([]string, 0, 16)
		inCatalog := false
		if models, mErr := p.fetchModels(ctx, cred); mErr == nil {
			for _, m := range models {
				keys = append(keys, m.Key)
				if m.Key == req.Model {
					inCatalog = true
				}
			}
		}
		diag := fmt.Sprintf("%s-empty-stream model=%q inCatalog=%v http=%d content-type=%q server=%q trace=%q bodyLen=%d",
			"qoderwork", req.Model, inCatalog, resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Server"),
			firstNonEmpty(resp.Header.Get("X-Trace-Id"), resp.Header.Get("Trace-Id"), resp.Header.Get("X-Request-Id"), "-"),
			len(encoded))
		diag += printf("\nx-model-key=%q login-version=%q cosy-version=%q",
			httpReq.Header.Get("x-model-key"), httpReq.Header.Get("login-version"), httpReq.Header.Get("cosy-version"))
		if len(keys) > 0 {
			diag += printf("\n账号模型目录（%d）: %s", len(keys), strings.Join(keys, ", "))
		} else {
			diag += "\n账号模型目录: 拉取失败（无法判定 key 是否有效）"
		}
		diag += printf("\n响应头: %v", resp.Header)
		if len(tap.buf) > 0 {
			diag += "\n上游原始响应（前 2KB）:\n" + string(tap.buf)
		} else {
			diag += "\n上游原始响应: 空（连接立即结束）"
		}
		if p.host != nil {
			p.host.Log("warn", strings.SplitN(diag, "\n", 2)[0])
		}
		// 诊断同时拼进错误消息（在线测试结果 / 日志列表都能直接看到，不再依赖 detail 通路）
		brief := fmt.Sprintf("qoderwork 上游返回空内容（已暂停该账号并换号重试）｜model=%s inCatalog=%v http=%d",
			req.Model, inCatalog, resp.StatusCode)
		if len(keys) > 0 {
			brief += "｜账号目录: " + strings.Join(keys, ",")
		}
		return stream.Send(failedDetail(429, brief, diag))
	}
	parser.Finish()
	return nil
}

// buildAgentBody 构造 QoderWork agent_chat_generation 请求体（最小可用骨架，
// 只带客户端自己的 messages/tools —— 参考实现实测模板 system/tools 非必需，
// 不注入可让 baseline prompt 从 ~10K token 降到 ~60）。
// buildAgentBody 组装 agent_chat_generation 的请求体。
//
// 形状逐字段对齐 qwenwork2api-makers（已验证可用的千问办公反代）：
// 上游对 body 做的是强校验，类型不对就直接 400 Invalid agent chat JSON body。踩过的坑：
//
//	chat_context.text / extra.originalContent 必须是**字符串**（不是 {type,text} 对象）
//	request_set_id / chat_record_id 必须等于 request_id（各随机一次会被判非法）
//	system 与 parameters 是必备字段；tools / messages 可空
func (p *plugin) buildAgentBody(chatBody map[string]interface{}, modelKey string, cred *accountCred) []byte {
	prompt := lastUserPrompt(chatBody)
	system, messages := splitSystemMessages(chatBody["messages"])
	if prompt == "" {
		prompt = "ping"
	}

	parameters := map[string]interface{}{}
	for _, k := range []string{"temperature", "top_p", "max_tokens", "presence_penalty", "frequency_penalty"} {
		if v, ok := chatBody[k]; ok && v != nil {
			parameters[k] = v
		}
	}
	if _, ok := parameters["max_tokens"]; !ok {
		parameters["max_tokens"] = 32000
	}

	tools := []interface{}{}
	if t, ok := chatBody["tools"]; ok && t != nil {
		tools, _ = t.([]interface{})
		if tools == nil {
			tools = []interface{}{}
		}
	}

	requestID := randomUUID()
	body := map[string]interface{}{
		"request_id":     requestID,
		"request_set_id": requestID,
		"chat_record_id": requestID,
		"session_id":     randomUUID(),
		"stream":         true,
		"chat_task":      "FREE_INPUT",
		"chat_context": map[string]interface{}{
			"text":       prompt,
			"features":   []interface{}{},
			"chatPrompt": "",
			"imageUrls":  nil,
			"extra": map[string]interface{}{
				"context":         []interface{}{},
				"modelConfig":     map[string]interface{}{"key": modelKey, "is_reasoning": false, "is_vl": true},
				"originalContent": prompt,
			},
		},
		"is_reply": true,
		"is_retry": false,
		"source":   1,
		"version":  "3",
		// 对齐千问办公客户端：session_type=qoder_work、aliyun_user_type 留空、
		// agent_id=agent_common、task_id=common（这几项 + model_config 不全时，上游会忽略所选模型）
		"aliyun_user_type": p.settingStr("aliyun_user_type", defaultAliyunUserType),
		"agent_id":         "agent_common",
		"session_type":     p.settingStr("session_type", defaultSessionType),
		"task_id":          p.settingStr("task_id", defaultTaskID),
		// model_config 必须是客户端 SDK 那种完整形状：实测只给 {key,is_reasoning} 时上游静默回落
		// 默认模型（所有模型都回同一个），补齐 source/format/is_vl/display_name 等后才真正按所选路由。
		"model_config": map[string]interface{}{
			"key":              modelKey,
			"display_name":     modelKey,
			"model":            "",
			"format":           p.settingStr("model_format", "openai"),
			"is_vl":            true,
			"is_reasoning":     false,
			"api_key":          "",
			"url":              "",
			"source":           p.settingStr("model_source", "system"),
			"max_input_tokens": 180000,
		},
		"system":     system,
		"messages":   stripCacheControl(messages),
		"tools":      tools,
		"parameters": parameters,
	}
	b, _ := json.Marshal(body)
	return b
}

// splitSystemMessages 把 system 消息抽成一段文本（上游要单独字段），其余按原样透传。
func splitSystemMessages(raw interface{}) (string, []interface{}) {
	list, _ := raw.([]interface{})
	if list == nil {
		if ms, ok := raw.([]map[string]interface{}); ok {
			list = make([]interface{}, 0, len(ms))
			for _, m := range ms {
				list = append(list, m)
			}
		}
	}
	sys := make([]string, 0, 2)
	out := make([]interface{}, 0, len(list))
	for _, item := range list {
		m, _ := item.(map[string]interface{})
		if m == nil {
			out = append(out, item)
			continue
		}
		if role, _ := m["role"].(string); role == "system" {
			if txt := contentText(m["content"]); strings.TrimSpace(txt) != "" {
				sys = append(sys, txt)
			}
			continue
		}
		out = append(out, m)
	}
	return strings.Join(sys, "\n\n"), out
}

func lastUserPrompt(chatBody map[string]interface{}) string {
	msgs, _ := chatBody["messages"].([]map[string]interface{})
	if msgs == nil {
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
	for i := len(msgs) - 1; i >= 0; i-- {
		if str(msgs[i]["role"]) != "user" {
			continue
		}
		if s := contentText(msgs[i]["content"]); s != "" {
			return s
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

// mapUpstreamStatus 上游状态 → 核心可识别的语义状态（401 触发换号 / 429 暂停 10 分钟）。
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
			Id: "checkin", Label: map[string]string{"zh": "每日签到", "en": "Daily Check-in"},
			Kind: "recurring", PerAccount: true, DefaultSchedule: "daily 09:05",
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
	client := p.httpClient(cred)
	if needRefresh(cred) {
		if err := p.refreshDeviceToken(ctx, client, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "设备令牌刷新失败: "+err.Error())
		}
	}
	ok, claimed, detail, err := p.claimCheckin(ctx, client, cred.DT)
	res := &pb.RunTaskResponse{Blob: marshalCred(cred), Changed: true}
	if errors.Is(err, errNoCheckinAPI) {
		return &pb.RunTaskResponse{
			Blob:    marshalCred(cred),
			Changed: false,
			Summary: "该账号没有每日签到：千问办公未提供 /sash 签到接口（这是 Qoder 账号体系的功能）",
		}, nil
	}
	switch {
	case err != nil:
		return nil, status.Error(codes.Internal, err.Error())
	case claimed:
		res.Summary = "今日已签到（" + detail + "）"
	case ok:
		res.Summary = detail
	default:
		res.Summary = "签到未生效"
	}
	if st, err := p.fetchCheckinStatus(ctx, client, cred.DT); err == nil {
		res.DetailJson = checkinDetailJSON(st)
	}
	return res, nil
}

func checkinDetailJSON(st *checkinStatus) string {
	b, _ := json.Marshal(map[string]interface{}{
		"status":              st.Status,
		"current_streak_days": st.CurrentStreakDays,
		"total_claim_days":    st.TotalClaimDays,
		"total_reward":        st.TotalRewardCredits,
		"reward_credits":      st.RewardCredits,
		"next_claim_at":       st.NextClaimAt,
	})
	return string(b)
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

// checkinSection 签到状态块（详情页动态渲染）。
func checkinSection(st *checkinStatus, q *quotaInfo) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "checkin",
		Title: map[string]string{"zh": "每日签到", "en": "Daily Check-in"},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "今日状态", "en": "Today"}, Value: checkinStatusText(st.Status), Kind: "status"},
			{Label: map[string]string{"zh": "连续签到", "en": "Streak"}, Value: fmt.Sprintf("%d 天", st.CurrentStreakDays)},
			{Label: map[string]string{"zh": "累计签到", "en": "Total days"}, Value: fmt.Sprintf("%d 天", st.TotalClaimDays)},
		},
	}
	if st.TotalRewardCredits > 0 {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "签到累计获得", "en": "Total from check-in"},
			Value: fmt.Sprintf("%d 积分", st.TotalRewardCredits),
		})
	}
	if st.RewardCredits > 0 {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "每日奖励", "en": "Daily reward"},
			Value: fmt.Sprintf("%d 积分", st.RewardCredits),
		})
	}
	if q != nil {
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": "当前余额", "en": "Balance"},
			Value: fmt.Sprintf("%d 积分", q.Remaining()),
		})
	}
	return sec
}

func checkinStatusText(status string) string {
	switch strings.ToUpper(status) {
	case "CLAIMED", "CLAIMED_TODAY":
		return "今日已领取"
	case "CLAIMABLE":
		return "今日可领取"
	case "":
		return "未查询到"
	default:
		return status
	}
}

func planSection(plan map[string]string) *pb.ProfileSection {
	sec := &pb.ProfileSection{Id: "plan", Title: map[string]string{"zh": "套餐", "en": "Plan"}}
	add := func(label, value string) {
		if strings.TrimSpace(value) == "" {
			return
		}
		sec.Entries = append(sec.Entries, &pb.SectionEntry{
			Label: map[string]string{"zh": label, "en": label}, Value: value,
		})
	}
	add("套餐", firstNonEmpty(plan["plan_tier_name"], plan["plan_name"], plan["display_name"]))
	add("账号类型", plan["user_type"])
	add("到期时间", firstNonEmpty(plan["expire_time"], plan["expires_at"]))
	if len(sec.Entries) == 0 {
		return nil
	}
	return sec
}

// ---------- 工具 ----------

func failed(code int32, msg string) *pb.StreamEvent {
	return &pb.StreamEvent{Event: &pb.StreamEvent_TaskFailed{
		TaskFailed: &pb.TaskFailed{Error: &pb.Error{Code: code, Message: msg}},
	}}
}

// failedDetail 失败事件带完整上游返回（核心落库到日志详情，排障用）。
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

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func trimFloat(f float64) string {
	return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.2f", f), "00"), ".")
}

// asEnvelopeError 判断 scanner 错误链里是否含信封错误。
func asEnvelopeError(err error, target **qodersign.EnvelopeError) bool {
	for err != nil {
		if ee, ok := err.(*qodersign.EnvelopeError); ok {
			*target = ee
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// stripCacheControl 递归移除内容块上的 cache_control。
//
// 本上游协议没有该字段（Cline / QoderWork 都是标准 OpenAI 形态），严格校验时可能 400；
// Qoder 官方形态支持它，因此只在不需要的上游剥离。
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

// printf fmt.Sprintf 简写（诊断拼装用，避免重复写返回值处理）。
func printf(format string, args ...interface{}) string { return fmt.Sprintf(format, args...) }

// limitedWriter 只保留前 max 字节（空流诊断用：抓上游原始响应开头）。
type limitedWriter struct {
	buf []byte
	max int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(w.buf) < w.max {
		n := w.max - len(w.buf)
		if n > len(p) {
			n = len(p)
		}
		w.buf = append(w.buf, p[:n]...)
	}
	return len(p), nil
}
