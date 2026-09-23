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

const (
	clineChatPath       = "/chat/completions"
	clineModelsPath     = "/models"
	clineFreeListPath   = "/ai/cline/recommended-models"
	clinePassModelsPath = "/cline-pass/models" // 账号可用的 ClinePass 模型（需 accessToken）
	defaultClientVer    = "3.0.47"
	defaultMinGapMs     = 800
	defaultEffort       = "high"
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

func (p *plugin) httpClient(cred *clineCred) *http.Client {
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
	c := &http.Client{Transport: tr, Timeout: 10 * time.Minute}
	clientCache.Store(key, c)
	return c
}

// ---------- 插件设置 ----------

func (p *plugin) settings() map[string]interface{} {
	out := map[string]interface{}{}
	if p.host == nil {
		return out
	}
	raw := p.host.Settings("cline")
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

func (p *plugin) settingStr(key, def string) string {
	if v := strings.TrimSpace(str(p.settings()[key])); v != "" {
		return v
	}
	return def
}

func (p *plugin) clientVersion() string {
	return p.settingStr("client_version", defaultClientVer)
}

func (p *plugin) reasoningEffort() string {
	return p.settingStr("reasoning_effort", defaultEffort)
}

func (p *plugin) minGapMs() int {
	v, err := strconv.Atoi(p.settingStr("min_gap_ms", strconv.Itoa(defaultMinGapMs)))
	if err != nil || v < 0 {
		return defaultMinGapMs
	}
	return v
}

func (p *plugin) serializeScope() string {
	return p.settingStr("serialize_scope", "free_only")
}

// extraHeaders 解析 "Key: Value; Key2: Value2"（上游指纹变化时的应急覆盖口）。
func (p *plugin) extraHeaders() map[string]string {
	raw := strings.TrimSpace(str(p.settings()["extra_headers"]))
	out := map[string]string{}
	if raw == "" {
		return out
	}
	for _, part := range strings.Split(raw, ";") {
		k, v, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		if k = strings.TrimSpace(k); k != "" {
			out[k] = strings.TrimSpace(v)
		}
	}
	return out
}

// ---------- 出站指纹头 ----------

// clineHeaders Cline 客户端指纹头：缺任何一个都会被上游 403
// （"only available via Cline product surfaces"）。
func (p *plugin) clineHeaders(cred *clineCred, sessionID string) map[string]string {
	ver := p.clientVersion()
	h := map[string]string{
		"Authorization":      "Bearer workos:" + cred.AccessToken,
		"Content-Type":       "application/json",
		"User-Agent":         "Cline/" + ver,
		"HTTP-Referer":       "https://cline.bot",
		"X-Title":            "Cline",
		"X-IS-MULTIROOT":     "false",
		"X-CLIENT-TYPE":      "cline-sdk",
		"X-CLIENT-VERSION":   ver,
		"X-PLATFORM":         "terminal",
		"X-PLATFORM-VERSION": ver,
		// 流式必须 identity：gzip 会把 SSE 缓冲成一次性下发（打字机效果消失）
		"Accept-Encoding": "identity",
	}
	if sessionID != "" {
		h["X-Task-ID"] = sessionID
	}
	for k, v := range p.extraHeaders() {
		h[k] = v
	}
	return h
}

// ---------- 并发闸门 ----------

// acquire 取得上游请求许可：命中串行范围时保证同一时刻只有一个在途请求，
// 且两次请求之间至少间隔 min_gap_ms（上游免费通道并发 >1 会返回空响应）。
// 返回的 release 必须 defer 调用。
func (p *plugin) acquire(ctx context.Context, model string) (func(), error) {
	scope := p.serializeScope()
	guarded := scope == "all" || (scope == "free_only" && isFreeLane(model))
	if !guarded {
		return func() {}, nil
	}
	p.mu.Lock()
	if p.gate == nil {
		p.gate = make(chan struct{}, 1)
	}
	gate := p.gate
	p.mu.Unlock()

	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	gap := time.Duration(p.minGapMs()) * time.Millisecond
	p.mu.Lock()
	wait := time.Duration(p.lastRun+int64(gap)) - time.Duration(time.Now().UnixMilli())
	p.mu.Unlock()
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			<-gate
			return nil, ctx.Err()
		}
	}
	return func() {
		p.mu.Lock()
		p.lastRun = time.Now().UnixMilli()
		p.mu.Unlock()
		<-gate
	}, nil
}

// isFreeLane 免费/通行证通道：上游对这类模型的并发与参数最敏感。
func isFreeLane(model string) bool {
	m := strings.ToLower(model)
	switch {
	case strings.HasPrefix(m, "deepseek/"),
		strings.HasPrefix(m, "cline-free/"),
		strings.HasPrefix(m, "cline-pass/"),
		strings.HasPrefix(m, "cline-cloud/"),
		strings.HasSuffix(m, ":free"):
		return true
	}
	return false
}

// ---------- 模型目录 ----------

type clineModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
	Created int64  `json:"created"`
}

type recommendedModel struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// fetchModels 拉公开模型目录（无需鉴权，445+ 条）。
func fetchModels(ctx context.Context, client *http.Client) ([]clineModel, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", clineAPIBase+clineModelsPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (cph-cline)")
	req.Header.Set("Accept", "application/json")
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
		Data []clineModel `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if len(out.Data) == 0 {
		return nil, fmt.Errorf("模型目录为空")
	}
	return out.Data, nil
}

// clineCatalog 官方推荐清单的四个通道。
// 上游 /ai/cline/recommended-models 返回 recommended / free / clinePass / clineCloud
// 四组 —— 早期实现只解析了 free 与 clinePass，导致 client 里看得到的模型（如
// moonshotai/kimi-k3、cline-cloud/kimi-k3）在插件侧不可见。
type clineCatalog struct {
	Recommended []recommendedModel // 官方精选（带 NEW 标签）
	Free        []recommendedModel // 免费额度
	ClinePass   []recommendedModel // 通行证
	ClineCloud  []recommendedModel // 云通道
}

// fetchRecommended 拉官方推荐清单（四个通道全解析）。
func fetchRecommended(ctx context.Context, client *http.Client) (*clineCatalog, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", clineAPIBase+clineFreeListPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (cph-cline)")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("推荐清单 HTTP %d", resp.StatusCode)
	}
	var out clineCatalog
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// fetchClinePassModels 拉「当前账号可用的 ClinePass 模型」——Cline 客户端第二个 provider
// （ClinePass）的清单来源，包含 Kimi K3 (free) 这类免费档。必须带账号 accessToken：
// 未登录 / 无资格时上游返回 401，此时调用方退回官方 recommended-models 的 clinePass 组。
func (p *plugin) fetchClinePassModels(ctx context.Context, client *http.Client, cred *clineCred) ([]recommendedModel, int, error) {
	// 路径与鉴权都做变体轮询：客户端用 Bearer workos:<accessToken>，不同网关对前缀 / 路径
	// 要求不一致（线上实测同 path 未鉴权 401、带 workos: 前缀却是 404）。第一个成功即返回。
	paths := []string{clinePassModelsPath, "/ai/cline/cline-pass/models"}
	auths := []string{"Bearer workos:" + cred.AccessToken, "Bearer " + cred.AccessToken}
	lastStatus, lastErr := 0, error(fmt.Errorf("ClinePass 模型目录未取到"))
	for _, path := range paths {
		for _, auth := range auths {
			req, err := http.NewRequestWithContext(ctx, "GET", clineAPIBase+path, nil)
			if err != nil {
				return nil, 0, err
			}
			for k, v := range p.clineHeaders(cred, "") {
				req.Header.Set(k, v)
			}
			req.Header.Set("Authorization", auth)
			resp, err := client.Do(req)
			if err != nil {
				return nil, 0, err
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				list, perr := parseModelList(body)
				if perr != nil {
					lastStatus, lastErr = resp.StatusCode, perr
					continue
				}
				return list, resp.StatusCode, nil
			}
			// 401/403/404 视为「这个路径或鉴权形态不对」，继续试下一个组合
			lastStatus = resp.StatusCode
			lastErr = fmt.Errorf("ClinePass 模型目录 HTTP %d（path=%s）: %s", resp.StatusCode, path, clip(string(body), 160))
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			default:
				return nil, lastStatus, lastErr
			}
		}
	}
	return nil, lastStatus, lastErr
}

// parseModelList 容错解析上游模型清单：
// {data:[...]} / {models:[...]} / {items:[...]} / [{...}] / ["id", ...] 都能吃。
func parseModelList(body []byte) ([]recommendedModel, error) {
	var wrapper struct {
		Data   []recommendedModel `json:"data"`
		Models []recommendedModel `json:"models"`
		Items  []recommendedModel `json:"items"`
	}
	if json.Unmarshal(body, &wrapper) == nil {
		for _, list := range [][]recommendedModel{wrapper.Data, wrapper.Models, wrapper.Items} {
			if len(list) > 0 {
				return list, nil
			}
		}
	}
	var direct []recommendedModel
	if json.Unmarshal(body, &direct) == nil && len(direct) > 0 {
		return direct, nil
	}
	var ids []string
	if json.Unmarshal(body, &ids) == nil && len(ids) > 0 {
		out := make([]recommendedModel, 0, len(ids))
		for _, id := range ids {
			if id = strings.TrimSpace(id); id != "" {
				out = append(out, recommendedModel{ID: id})
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	// 字段名不确定时兜底：从宽松 map 里挑第一个像 id 的键
	var loose []map[string]interface{}
	if json.Unmarshal(body, &loose) == nil {
		out := make([]recommendedModel, 0, len(loose))
		for _, m := range loose {
			for _, k := range []string{"id", "model_id", "modelId", "model", "slug", "key", "name"} {
				if s := strings.TrimSpace(str(m[k])); s != "" {
					out = append(out, recommendedModel{ID: s, Name: str(m["name"]), Description: str(m["description"])})
					break
				}
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, fmt.Errorf("ClinePass 模型目录结构未识别（前 200 字：%s）", clip(string(body), 200))
}

// shortModelName 取模型 id 的短名：`moonshotai/kimi-k3` → `kimi-k3`、
// `cline-pass/mimo-v2.6-flash` → `mimo-v2.6-flash`、`poolside/x:free` → `x`。
func shortModelName(id string) string {
	s := id
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	return strings.TrimSuffix(strings.TrimSpace(s), ":free")
}

// freeChannelAlias 免费通道别名：官方免费通道按 `cline-free/<短名>` 取模型
// （实测 cline-free/kimi-k3 可用，而它只出现在 recommended / clinePass 里）。
func freeChannelAlias(id string) string {
	if short := shortModelName(id); short != "" {
		return "cline-free/" + short
	}
	return ""
}

// ---------- SSE 解包 ----------

// unwrapReader 把上游可能包裹的 {"data":{...}} 去掉，输出标准 OpenAI SSE 行，
// 交给 sdk/openaiup 解析（工具调用 / 推理 / usage 全部复用现有逻辑）。
func unwrapReader(r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" || payload == "[DONE]" {
				if payload == "[DONE]" {
					_, _ = pw.Write([]byte("data: [DONE]\n\n"))
				}
				continue
			}
			out := payload
			var wrapper struct {
				Data json.RawMessage `json:"data"`
			}
			if json.Unmarshal([]byte(payload), &wrapper) == nil && len(wrapper.Data) > 0 {
				var probe struct {
					Choices json.RawMessage `json:"choices"`
					Usage   json.RawMessage `json:"usage"`
					ID      string          `json:"id"`
				}
				if json.Unmarshal(wrapper.Data, &probe) == nil && (len(probe.Choices) > 0 || len(probe.Usage) > 0 || probe.ID != "") {
					out = string(wrapper.Data)
				}
			}
			if _, err := pw.Write([]byte("data: " + out + "\n\n")); err != nil {
				break
			}
		}
		if err := sc.Err(); err != nil {
			pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	return pr
}

// ---------- 工具 ----------

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
