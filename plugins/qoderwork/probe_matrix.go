package main

// probe_matrix.go —— 线上诊断探针（只读，用账号真令牌打上游，结果挂到账号详情页）。
//
// 触发方式（插件设置，都是字符串）：
//
//	probe_json        : JSON 数组，描述用例矩阵（可覆盖 host/path/auth/body/encode/framed/model）
//	probe_dump_models : "1" 时把模型目录原始 JSON 前 3000 字挂出来（看 enable/source 等字段）
//
// 每个用例输出一行：name → HTTP / grpc-status / 关键响应头 / 请求侧关键头 / 正文摘要。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

type probeCase struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Path     string `json:"path"`
	Auth     string `json:"auth"`     // cosy（默认）/ none / bearer / cosybearer
	QwenWork *bool  `json:"qwenwork"` // 是否补 X-QwenWork-* 头（默认 true）
	Body     string `json:"body"`     // agent（默认）/ makers / minimal / empty
	Encode   bool   `json:"encode"`   // 是否 QoderEncoding 编码正文
	Framed   bool   `json:"framed"`   // 是否套 gRPC 5 字节帧（sign 与正文都按帧算）
	Model    string `json:"model"`
}

// probeMatrix 跑一组诊断用例，返回可直接贴账号详情页的多行文本。
func (p *plugin) probeMatrix(ctx context.Context, cred *accountCred) string {
	raw := strings.TrimSpace(p.settingStr("probe_json", ""))
	if raw == "" {
		return "未配置 probe_json"
	}
	var cases []probeCase
	if err := json.Unmarshal([]byte(raw), &cases); err != nil {
		return "probe_json 解析失败: " + err.Error()
	}
	if len(cases) > 24 {
		cases = cases[:24]
	}
	if err := p.fillFingerprint(cred); err != nil {
		return "探针失败(指纹): " + err.Error()
	}
	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return "探针失败(COSY): " + err.Error()
	}

	out := make([]string, 0, len(cases)+2)
	for i, c := range cases {
		if strings.TrimSpace(c.Name) == "" {
			c.Name = fmt.Sprintf("case%d", i+1)
		}
		if c.Model == "" {
			c.Model = "pro"
		}
		out = append(out, fmt.Sprintf("[%s] %s", c.Name, p.probeOnce(ctx, cred, sess, c)))
	}
	if strings.TrimSpace(p.settingStr("probe_dump_models", "")) == "1" {
		out = append(out, "[model-list] "+p.probeModelListRaw(ctx, cred))
	}
	return strings.Join(out, " | ")
}

// probeOnce 单个用例。
func (p *plugin) probeOnce(ctx context.Context, cred *accountCred, sess *qodersign.Session, c probeCase) string {
	host := strings.TrimRight(strings.TrimSpace(c.Host), "/")
	if host == "" {
		host = p.gatewayBaseURL()
	}
	path := strings.TrimSpace(c.Path)
	if path == "" {
		path = "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common"
	}
	body := p.probeBody(c, cred)
	wire := body
	if c.Encode {
		if enc, err := qodersign.Encode([]byte(body)); err == nil {
			wire = enc
		} else {
			return "编码失败: " + err.Error()
		}
	}
	signBody := wire
	if c.Framed {
		wire = string(grpcFrame([]byte(wire)))
		signBody = wire
	}
	req, err := http.NewRequestWithContext(ctx, "POST", host+path, strings.NewReader(wire))
	if err != nil {
		return "建请求失败: " + err.Error()
	}
	auth := strings.ToLower(strings.TrimSpace(c.Auth))
	if auth == "" {
		auth = "cosy"
	}
	if auth != "none" {
		if err := sess.ApplyHeaders(req, p.headerCfg(), signBody, cred.UID, c.Model); err != nil {
			return "签名失败: " + err.Error()
		}
	}
	if auth == "bearer" || auth == "cosybearer" {
		req.Header.Set("Authorization", "Bearer "+cred.DT)
	}
	qw := true
	if c.QwenWork != nil {
		qw = *c.QwenWork
	}
	if qw {
		p.applyQwenWorkHeaders(req)
	}
	client := p.httpClient(cred)
	if c.Framed {
		client = p.grpcHTTPClient(cred)
		req.Header.Set("content-type", "application/grpc+proto")
		req.Header.Set("accept", "application/grpc")
		req.Header.Set("te", "trailers")
		req.Header.Set("grpc-accept-encoding", "identity")
		req.Header.Set("accept-encoding", "identity")
	} else {
		req.Header.Set("Accept-Encoding", "identity")
	}

	resp, err := client.Do(req)
	if err != nil {
		return "请求失败: " + err.Error()
	}
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	keys := []string{"Content-Type", "Grpc-Status", "Grpc-Message", "Server", "X-Trace-Id", "Trace-Id", "X-Request-Id", "Eagleeye-Traceid"}
	parts := []string{fmt.Sprintf("HTTP %d proto=%s len=%d", resp.StatusCode, resp.Proto, len(rawResp))}
	for _, k := range keys {
		if v := resp.Header.Get(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	if gt := resp.Trailer.Get("grpc-status"); gt != "" {
		parts = append(parts, "trailer-grpc-status="+gt)
	}
	if gm := resp.Trailer.Get("grpc-message"); gm != "" {
		parts = append(parts, "trailer-grpc-message="+gm)
	}
	parts = append(parts, fmt.Sprintf("reqUA=%q reqCT=%q reqModel=%q reqClientType=%q reqScene=%q reqBP=%q reqCosyVer=%q reqMachine=%v/%v reqClientIP=%v reqLogin=%q reqAuthLen=%d", req.Header.Get("User-Agent"), req.Header.Get("Content-Type"), req.Header.Get("x-model-key"), req.Header.Get("cosy-clienttype"), req.Header.Get("cosy-scene"), req.Header.Get("cosy-business-product"), req.Header.Get("cosy-version"), req.Header.Get("cosy-machinetype") != "", req.Header.Get("cosy-machineid") != "", req.Header.Get("cosy-clientip") != "", req.Header.Get("login-version"), len(req.Header.Get("authorization"))))
	if len(rawResp) > 0 {
		parts = append(parts, "body="+clip(strings.ReplaceAll(string(rawResp), "\n", " | "), 300))
	}
	return strings.Join(parts, " ")
}

// probeBody 生成用例正文（全部明文 JSON）。
func (p *plugin) probeBody(c probeCase, cred *accountCred) string {
	switch strings.ToLower(strings.TrimSpace(c.Body)) {
	case "makers":
		return makersChatBody(c.Model, "只回答两个字符：OK")
	case "minimal":
		rid := randomUUID()
		b, _ := json.Marshal(map[string]interface{}{
			"request_id": rid, "request_set_id": rid, "chat_record_id": rid,
			"session_id": randomUUID(), "stream": true, "chat_task": "FREE_INPUT",
			"chat_context": map[string]interface{}{
				"text": "只回答两个字符：OK", "features": []interface{}{}, "chatPrompt": "", "imageUrls": nil,
				"extra": map[string]interface{}{"context": []interface{}{}, "modelConfig": map[string]interface{}{"key": c.Model, "is_reasoning": false, "is_vl": true}, "originalContent": "只回答两个字符：OK"},
			},
			"is_reply": true, "is_retry": false, "source": 1, "version": "3",
			"agent_id": "agent_common", "task_id": "common", "session_type": "qoder_work", "aliyun_user_type": "",
			"model_config": map[string]interface{}{"key": c.Model, "display_name": c.Model, "model": "", "format": "openai", "is_vl": true, "is_reasoning": false, "api_key": "", "url": "", "source": "system", "max_input_tokens": 180000},
			"system": "", "messages": []interface{}{map[string]interface{}{"role": "user", "content": "只回答两个字符：OK"}},
			"tools": []interface{}{}, "parameters": map[string]interface{}{"max_tokens": 32000},
		})
		return string(b)
	case "empty":
		return "{}"
	default:
		body := map[string]interface{}{
			"messages":   []interface{}{map[string]interface{}{"role": "user", "content": "只回答两个字符：OK"}},
			"max_tokens": 32000,
		}
		return string(p.buildAgentBody(body, c.Model, cred))
	}
}

// probeModelListRaw 原样打印模型目录（用于看 enable / source / price_factor 等字段）。
func (p *plugin) probeModelListRaw(ctx context.Context, cred *accountCred) string {
	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return "COSY 失败: " + err.Error()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", p.gatewayBaseURL()+"/api/v2/model/list?Encode=1", nil)
	if err != nil {
		return "建请求失败: " + err.Error()
	}
	if err := sess.ApplyHeaders(req, p.headerCfg(), "", cred.UID, ""); err != nil {
		return "签名失败: " + err.Error()
	}
	p.applyQwenWorkHeaders(req)
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return "请求失败: " + err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return fmt.Sprintf("HTTP %d ct=%s body=%s", resp.StatusCode, resp.Header.Get("content-type"), clip(strings.ReplaceAll(string(raw), "\n", " "), 3000))
}
