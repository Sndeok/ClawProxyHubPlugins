package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// Command Code 上游端点。
const (
	ccAPIBase         = "https://api.commandcode.ai"
	ccGeneratePath    = "/alpha/generate"
	ccFingerprintPath = "/alpha/fingerprint/record"
	ccLifecyclePath   = "/alpha/lifecycle-events"
	ccModelsPath      = "/provider/v1/models"
	ccProtocolVersion = "1.53.1"
	defaultPermission = "standard"
	defaultCLIMode    = "agent"
	defaultMaxTokens  = 64000
	maxAllowedTokens  = 200000
)

// ---------- HTTP ----------

var clientCache sync.Map

func proxyURLOf(p *pb.ProxyConfig) string {
	if p == nil || p.GetHost() == "" {
		return ""
	}
	u := &url.URL{Scheme: orDefault(p.GetScheme(), "http"), Host: fmt.Sprintf("%s:%d", p.GetHost(), p.GetPort())}
	if p.GetUsername() != "" {
		u.User = url.UserPassword(p.GetUsername(), p.GetPassword())
	}
	return u.String()
}

func (p *plugin) httpClient(cred *ccCred) *http.Client {
	key := ""
	if cred != nil {
		key = cred.proxyURL
	}
	if c, ok := clientCache.Load(key); ok {
		return c.(*http.Client)
	}
	tr := &http.Transport{
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	if key != "" {
		if u, err := url.Parse(key); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr.Proxy = http.ProxyFromEnvironment
	}
	c := &http.Client{Transport: tr, Timeout: 30 * time.Minute}
	clientCache.Store(key, c)
	return c
}

// ---------- 插件设置 ----------

func (p *plugin) settings() map[string]interface{} {
	out := map[string]interface{}{}
	if p.host == nil {
		return out
	}
	raw := p.host.Settings("commandcode")
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

func (p *plugin) settingStr(key, def string) string {
	if v, ok := p.settings()[key]; ok {
		if s := strings.TrimSpace(str(v)); s != "" {
			return s
		}
	}
	return def
}

func (p *plugin) fingerprintSalt() string { return p.settingStr("fingerprint_salt", "") }

func (p *plugin) cliMode() string { return p.settingStr("cli_mode", defaultCLIMode) }

func (p *plugin) zdr() bool { return p.settingStr("zdr", "false") == "true" }

// ---------- 出站请求头 ----------

// ccHeaders 官方 CLI 的请求头（User-Agent 固定 cli、带 traceparent）。
func (p *plugin) ccHeaders(cred *ccCred, sessionID string) map[string]string {
	prof := cred.profile(p.fingerprintSalt())
	h := map[string]string{
		"Content-Type":           "application/json",
		"User-Agent":             "cli",
		"x-command-code-version": ccProtocolVersion,
		"x-cli-environment":      "production",
		"x-project-slug":         prof.slugifyProjectPath(),
		"x-taste-learning":       "false",
		"x-session-id":           sessionID,
		"Authorization":          "Bearer " + cred.APIKey,
		"traceparent":            traceparent(),
		// 流式必须 identity：gzip 会把事件流缓冲成一次性下发
		"Accept-Encoding": "identity",
	}
	if p.zdr() {
		h["x-cmd-zdr"] = "1"
	}
	return h
}

func traceparent() string {
	b := make([]byte, 24)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (uint(i%8) * 8))
	}
	return fmt.Sprintf("00-%x-%x-01", b[:16], b[16:24])
}

// ---------- 指纹 / lifecycle 初始化 ----------

// ensureInitialized 首次（或超过 8h+ 抖动）上报指纹与 lifecycle。
// 失败不阻塞业务请求（官方客户端同样容忍），但会记录在返回值里。
func (p *plugin) ensureInitialized(ctx context.Context, cred *ccCred) error {
	now := time.Now().UnixMilli()
	if cred.NextInitAt > now {
		return nil
	}
	prof := cred.profile(p.fingerprintSalt())
	body, _ := json.Marshal(prof.payload())
	if _, status, err := p.postJSON(ctx, cred, ccFingerprintPath, body, ""); err != nil {
		return err
	} else if status >= 400 {
		return fmt.Errorf("指纹上报 HTTP %d", status)
	}
	lifecycle, _ := json.Marshal(prof.lifecycleBody(ccProtocolVersion))
	if _, status, err := p.postJSON(ctx, cred, ccLifecyclePath, lifecycle, ""); err != nil {
		return err
	} else if status >= 400 {
		return fmt.Errorf("lifecycle 上报 HTTP %d", status)
	}
	cred.NextInitAt = initSchedule(byte(time.Now().UnixNano() & 0xff))
	return nil
}

// postJSON 发 JSON 请求，返回 body / 状态码。
func (p *plugin) postJSON(ctx context.Context, cred *ccCred, path string, body []byte, sessionID string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", ccAPIBase+path, strings.NewReader(string(body)))
	if err != nil {
		return nil, 0, err
	}
	for k, v := range p.ccHeaders(cred, orDefault(sessionID, "sess_probe")) {
		req.Header.Set(k, v)
	}
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return raw, resp.StatusCode, nil
}

// ---------- 模型目录 ----------

// ccModel /provider/v1/models 条目（公开接口，无需鉴权）。
type ccModel struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	OwnedBy            string   `json:"owned_by"`
	ContextLength      int      `json:"context_length"`
	SupportedEndpoints []string `json:"supported_endpoints"`
}

func fetchModels(ctx context.Context, client *http.Client) ([]ccModel, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", ccAPIBase+ccModelsPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (cph-commandcode)")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("模型目录 HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var out struct {
		Data []ccModel `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if len(out.Data) == 0 {
		return nil, fmt.Errorf("模型目录为空")
	}
	return out.Data, nil
}

// ---------- 上游流：AI-SDK NDJSON → 标准 OpenAI chunk ----------

// StreamError 上游在流内投递的错误事件（HTTP 200 之后）。
type StreamError struct {
	Status  int
	Message string
}

func (e *StreamError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("upstream stream error (status %d)", e.Status)
	}
	return e.Message
}

// ndjsonReader 把上游 AI-SDK 事件流（每行一个 JSON，无 data: 前缀）
// 还原成标准 OpenAI SSE 行，交给 sdk/openaiup 复用增量/工具/用量逻辑。
//
// 事件覆盖：text-delta / tool-call / finish(-step) / error；
// start、text-start/end、reasoning-*、provider-metadata、tool-input-* 等无正文事件跳过
// （CPH 信封没有 reasoning 通道，思考过程不计入正文）。
func ndjsonReader(r io.Reader, model string) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		toolIndex := map[string]int{}
		write := func(payload map[string]interface{}) error {
			b, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			_, werr := pw.Write([]byte("data: " + string(b) + "\n\n"))
			return werr
		}
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || line == "[DONE]" {
				continue
			}
			if strings.HasPrefix(line, "data:") {
				line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
			var ev struct {
				Type       string          `json:"type"`
				Text       string          `json:"text"`
				Delta      string          `json:"delta"`
				ToolCallID string          `json:"toolCallId"`
				ToolName   string          `json:"toolName"`
				Input      json.RawMessage `json:"input"`
				Reason     string          `json:"finishReason"`
				TotalUsage *struct {
					InputTokens       int64 `json:"inputTokens"`
					OutputTokens      int64 `json:"outputTokens"`
					CachedInputTokens int64 `json:"cachedInputTokens"`
				} `json:"totalUsage"`
				Error *struct {
					Message    string `json:"message"`
					StatusCode int    `json:"statusCode"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				continue
			}
			switch ev.Type {
			case "text-delta":
				text := firstNonEmpty(ev.Text, ev.Delta)
				if text == "" {
					continue
				}
				if err := write(map[string]interface{}{
					"choices": []interface{}{map[string]interface{}{
						"index": 0, "delta": map[string]interface{}{"content": text},
					}},
				}); err != nil {
					pw.CloseWithError(err)
					return
				}
			case "tool-call":
				idx, seen := toolIndex[ev.ToolCallID]
				if !seen {
					idx = len(toolIndex)
					toolIndex[ev.ToolCallID] = idx
				}
				args := "{}"
				if len(ev.Input) > 0 {
					if ev.Input[0] == '"' {
						var s string
						if json.Unmarshal(ev.Input, &s) == nil {
							args = s
						}
					} else {
						args = string(ev.Input)
					}
				}
				call := map[string]interface{}{
					"index": idx, "id": ev.ToolCallID,
					"type":     "function",
					"function": map[string]interface{}{"name": ev.ToolName, "arguments": args},
				}
				if err := write(map[string]interface{}{
					"choices": []interface{}{map[string]interface{}{
						"index": 0, "delta": map[string]interface{}{"tool_calls": []interface{}{call}},
					}},
				}); err != nil {
					pw.CloseWithError(err)
					return
				}
			case "finish", "finish-step":
				if ev.Type == "finish-step" {
					continue // 只在最终 finish 收尾
				}
				chunk := map[string]interface{}{
					"model": model,
					"choices": []interface{}{map[string]interface{}{
						"index": 0, "delta": map[string]interface{}{},
						"finish_reason": mapFinish(ev.Reason),
					}},
				}
				if u := ev.TotalUsage; u != nil {
					chunk["usage"] = map[string]interface{}{
						"prompt_tokens":     u.InputTokens,
						"completion_tokens": u.OutputTokens,
						"total_tokens":      u.InputTokens + u.OutputTokens,
						"prompt_tokens_details": map[string]interface{}{
							"cached_tokens": u.CachedInputTokens,
						},
					}
				}
				if err := write(chunk); err != nil {
					pw.CloseWithError(err)
					return
				}
			case "error":
				status := 502
				msg := "上游返回错误事件"
				if ev.Error != nil {
					if ev.Error.StatusCode > 0 {
						status = ev.Error.StatusCode
					}
					if ev.Error.Message != "" {
						msg = ev.Error.Message
					}
				}
				pw.CloseWithError(&StreamError{Status: status, Message: msg})
				return
			default:
				continue
			}
		}
		if err := sc.Err(); err != nil {
			pw.CloseWithError(err)
			return
		}
		_, _ = pw.Write([]byte("data: [DONE]\n\n"))
		_ = pw.Close()
	}()
	return pr
}

// mapFinish AI-SDK 结束原因 → OpenAI finish_reason。
func mapFinish(reason string) string {
	switch reason {
	case "tool-calls", "tool_calls", "tool-call":
		return "tool_calls"
	case "length", "max-tokens", "max_tokens":
		return "length"
	case "content-filter", "content_filter":
		return "content_filter"
	case "":
		return "stop"
	default:
		return reason
	}
}

// ---------- 工具 ----------

// toolNameAliases CLI 发送前的工具名重写表（与官方客户端一致）。
var toolNameAliases = map[string]string{
	"bash_output":         "shell_output",
	"task_output":         "shell_output",
	"tool_search":         "search_tools",
	"read_multiple_files": "read_file",
}

func wireToolName(name string) string {
	if v, ok := toolNameAliases[name]; ok {
		return v
	}
	return name
}

func str(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}
