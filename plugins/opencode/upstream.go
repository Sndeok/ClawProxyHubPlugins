package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// OpenCode Zen 上游端点（Zen 付费 / Go 订阅各一套；匿名走 Zen + public 凭证）。
const (
	zenBase       = "https://opencode.ai/zen"
	goBase        = "https://opencode.ai/zen/go"
	catalogURL    = "https://models.opencode.ai/api.json"
	zenDocsURL    = "https://raw.githubusercontent.com/anomalyco/opencode/dev/packages/web/src/content/docs/zen.mdx"
	goDocsURL     = "https://raw.githubusercontent.com/anomalyco/opencode/dev/packages/web/src/content/docs/go.mdx"
	defaultCLIVer = "1.18.31"
	catalogTTL    = 10 * time.Minute
	protocolChat  = "chat"
	protocolMsgs  = "messages"
	protocolResp  = "responses"
)

func baseFor(tier string) string {
	if tier == tierGo {
		return goBase
	}
	return zenBase
}

// pathFor 各协议的上游路径。
func pathFor(protocol string) string {
	switch protocol {
	case protocolMsgs:
		return "/v1/messages"
	case protocolResp:
		return "/v1/responses"
	default:
		return "/v1/chat/completions"
	}
}

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

func (p *plugin) httpClient(cred *openCred) *http.Client {
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
	raw := p.host.Settings("opencode")
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

func (p *plugin) cliVersion() string { return p.settingStr("cli_version", defaultCLIVer) }

func (p *plugin) injectAnonTools() bool { return p.settingStr("anon_inject_tools", "true") != "false" }

// ---------- 客户端伪装头 ----------

// headers 组装 OpenCode CLI 的请求头。
//   - x-opencode-client / session / request / project：官方 CLI 都带，缺了会被判成非官方客户端
//   - 匿名通道（Bearer public）额外要求官方 session 形状，见 canonicalSession
func (p *plugin) headers(cred *openCred, protocol, sessionID, requestID string) map[string]string {
	h := map[string]string{
		"Content-Type":       "application/json",
		"Accept":             "text/event-stream, application/json",
		"User-Agent":         "opencode/" + p.cliVersion(),
		"x-opencode-client":  "cli",
		"x-opencode-session": sessionID,
		"x-session-affinity": sessionID,
		"X-Session-Id":       sessionID,
		"x-opencode-request": requestID,
		"x-opencode-project": "prj_" + hashHex("project\x00"+orDefault(cred.UID, "anonymous"), 12),
		// 流式必须 identity：gzip 会把 SSE 缓冲成一次性下发（打字机效果消失）
		"Accept-Encoding": "identity",
	}
	if protocol == protocolMsgs {
		// Anthropic 方言用 x-api-key，不是 Bearer
		h["x-api-key"] = cred.APIKey
		h["anthropic-version"] = "2023-06-01"
		h["anthropic-beta"] = "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14"
	} else {
		h["Authorization"] = "Bearer " + cred.APIKey
	}
	return h
}

// canonicalSession 官方会话 id 形状：ses_ + 12 位小写十六进制 + 14 位 Base62。
//
// 2026-09-16 起匿名免费通道（Bearer public）只接受这个形状，其它形状一律 403 FreeTierError。
// 同一个会话（首条 user 消息相同）派生结果稳定 → 上游缓存亲和。
func canonicalSession(seed string) string {
	sum := sha256.Sum256([]byte("ses\x00" + seed))
	timePart := hex.EncodeToString(sum[:6])
	randomPart := base62From(sum[6:16], 14)
	return "ses_" + timePart + randomPart
}

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// base62From 按 Base62 逐位取模（与参考实现同构，避免引入大整数依赖）。
func base62From(src []byte, width int) string {
	out := make([]byte, width)
	// 简单洗牌：用前若干字节做偏移，保证不同 seed 分布均匀
	for i := 0; i < width; i++ {
		out[i] = base62Alphabet[int(src[i%len(src)]+byte(i*7))%62]
	}
	return string(out)
}

func hashHex(s string, n int) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:n]
}

// requestID 每次请求唯一（官方 req_ 前缀 + 32 hex）。
func requestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "req_" + hex.EncodeToString(b)
}

// ---------- 模型目录 ----------

// zenModel 对外模型（含元数据与原生协议）。
type zenModel struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	Context          int      `json:"context"`
	MaxOutput        int      `json:"max_output"`
	Reasoning        []string `json:"reasoning"`
	DefaultReasoning string   `json:"default_reasoning"`
	CostMultiplier   float64  `json:"cost_multiplier"`
	Tags             []string `json:"tags"`
	Protocol         string   `json:"protocol"`
	Free             bool     `json:"free"`
}

// fetchModelIDs 拉档位可见模型 id（Zen 76 / Go 40；Key 不参与校验，仅作连通性检查）。
func (p *plugin) fetchModelIDs(ctx context.Context, cred *openCred) ([]string, error) {
	rawURL := baseFor(cred.Tier) + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range p.headers(cred, protocolChat, canonicalSession("probe"), requestID()) {
		req.Header.Set(k, v)
	}
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("模型目录 HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(out.Data))
	for _, m := range out.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("模型目录为空")
	}
	return ids, nil
}

// capabilityCatalog models.opencode.ai 的公开目录：上下文 / 输出上限 / 推理档位 / 倍率 / 成本。
type capabilityCatalog struct {
	Providers map[string]struct {
		ID     string `json:"id"`
		Models map[string]struct {
			ID      string `json:"id"`
			Name    string `json:"name"`
			Family  string `json:"family"`
			Desc    string `json:"description"`
			Reason  bool   `json:"reasoning"`
			ToolCal bool   `json:"tool_call"`
			Limit   struct {
				Context int `json:"context"`
				Output  int `json:"output"`
			} `json:"limit"`
			ReasoningOptions []struct {
				Type   string   `json:"type"`
				Values []string `json:"values"`
			} `json:"reasoning_options"`
			Modalities struct {
				Input  []string `json:"input"`
				Output []string `json:"output"`
			} `json:"modalities"`
			Cost struct {
				Input  float64 `json:"input"`
				Output float64 `json:"output"`
			} `json:"cost"`
		} `json:"models"`
	} `json:"providers"`
}

func fetchCatalog(ctx context.Context, client *http.Client) (*capabilityCatalog, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", catalogURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "opencode/"+defaultCLIVer)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("catalog HTTP %d", resp.StatusCode)
	}
	var out capabilityCatalog
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// 官方文档表：| 名称 | model-id | https://opencode.ai/zen/v1/<协议> | npm |
var docRowPattern = regexp.MustCompile("\\|[^|]+\\|\\s*`?([^|`\\s]+)`?\\s*\\|\\s*`?https://opencode\\.ai/zen(?:/go)?/v1/(chat/completions|messages|responses)`?")

// fetchProtocols 解析官方文档表得到「模型 → 原生协议」。
//
// 每个模型的原生协议不同（GPT/Codex 走 /v1/responses、Claude 走 /v1/messages、
// 其余走 /v1/chat/completions），走错会被上游 500。
func fetchProtocols(ctx context.Context, client *http.Client, docsURL string) map[string]string {
	out := map[string]string{}
	req, err := http.NewRequestWithContext(ctx, "GET", docsURL, nil)
	if err != nil {
		return out
	}
	resp, err := client.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return out
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	for _, line := range strings.Split(string(raw), "\n") {
		m := docRowPattern.FindStringSubmatch(line)
		if len(m) < 3 {
			continue
		}
		switch m[2] {
		case "messages":
			out[m[1]] = protocolMsgs
		case "responses":
			out[m[1]] = protocolResp
		default:
			out[m[1]] = protocolChat
		}
	}
	return out
}

// listModels 汇总「档位可见模型 + 公开元数据 + 原生协议」。
// 匿名通道只返回免费模型（上游也只服务这些）。
func (p *plugin) listModels(ctx context.Context, cred *openCred) ([]zenModel, error) {
	p.modelMu.Lock()
	if len(p.models) > 0 && time.Now().Unix()-p.at < int64(catalogTTL.Seconds()) {
		cached := append([]zenModel(nil), p.models...)
		p.modelMu.Unlock()
		return cached, nil
	}
	p.modelMu.Unlock()

	ids, err := p.fetchModelIDs(ctx, cred)
	if err != nil {
		return nil, err
	}
	visible := map[string]bool{}
	for _, id := range ids {
		visible[id] = true
	}
	client := p.httpClient(cred)
	cat, catErr := fetchCatalog(ctx, client)
	protocols := map[string]string{}
	docsURL := zenDocsURL
	if cred.Tier == tierGo {
		docsURL = goDocsURL
	}
	for k, v := range fetchProtocols(ctx, client, docsURL) {
		protocols[k] = v
	}

	models := make([]zenModel, 0, len(ids))
	for _, id := range ids {
		m := zenModel{ID: id, Name: id, Protocol: protocolChat}
		if catErr == nil {
			if provider, ok := cat.Providers["opencode"]; ok {
				if meta, ok := provider.Models[id]; ok {
					m.Name = firstNonEmpty(meta.Name, id)
					m.Description = meta.Desc
					m.Context = meta.Limit.Context
					m.MaxOutput = meta.Limit.Output
					m.CostMultiplier = meta.Cost.Input
					for _, opt := range meta.ReasoningOptions {
						if opt.Type == "effort" {
							m.Reasoning = opt.Values
						}
					}
					m.Tags = append(m.Tags, meta.Modalities.Input...)
					m.Free = meta.Cost.Input == 0 && meta.Cost.Output == 0
				}
			}
		}
		if strings.Contains(id, "free") || strings.Contains(id, "pickle") {
			m.Free = true
		}
		if p, ok := protocols[id]; ok {
			m.Protocol = p
		}
		if cred.Tier == tierAnon && !m.Free {
			continue // 匿名通道只列免费模型
		}
		models = append(models, m)
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("没有可用模型（tier=%s）", cred.Tier)
	}
	sort.SliceStable(models, func(i, j int) bool { return models[i].ID < models[j].ID })

	p.modelMu.Lock()
	p.models, p.at = models, time.Now().Unix()
	p.modelMu.Unlock()
	return models, nil
}

// protocolFor 查某模型的原生协议（缓存未命中时拉一次目录）。
func (p *plugin) protocolFor(ctx context.Context, cred *openCred, model string) string {
	models, err := p.listModels(ctx, cred)
	if err != nil {
		return protocolChat
	}
	for _, m := range models {
		if m.ID == model {
			return m.Protocol
		}
	}
	return protocolChat
}

// isFreeModel 该模型是否免费（匿名/免费额度通道）。
func (p *plugin) isFreeModel(ctx context.Context, cred *openCred, model string) bool {
	models, err := p.listModels(ctx, cred)
	if err != nil {
		return strings.Contains(model, "free")
	}
	for _, m := range models {
		if m.ID == model {
			return m.Free
		}
	}
	return false
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
