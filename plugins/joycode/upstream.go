// joycode 插件 —— 京东 JoyCode 反代（上游：joycode-api.jd.com）。
//
// 协议要点全部逆向自 JoyCode2Api（Apache-2.0）与 JoyCode IDE 2.7.5：
//   - 默认走 color gateway：{colorBase}/api?appid=joycode_ide&functionId=...&t=&sign=
//     sign = hex(HMAC-SHA256("0691a3f0b37b4a85aeb63ad0fc7db3ed", appid+"&"+functionId+"&"+ts))
//   - 没有 colorBase 时退化为直连 v2 路径（无签名）
//   - 鉴权头：ptKey / loginType / source-type: joycoder-ide
//   - 请求体外层信封：tenant / orgFullName / userId / client / clientVersion / language
//   - 流式必须 Accept-Encoding: identity：gzip 会缓冲整块，打字机效果消失
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

const (
	joyBaseURL          = "https://joycode-api.jd.com"
	joyDefaultColorBase = "https://api-ai.jd.com"
	joyColorPath        = "/api"
	joyColorAppID       = "joycode_ide"
	joyColorHMACKey     = "0691a3f0b37b4a85aeb63ad0fc7db3ed"
	joyDefaultVersion   = "2.7.5"
	joyDefaultModel     = "JoyAI-Code-1.5"
	joyDefaultTenant    = "JOYCODE"
	joyDefaultLoginType = "N_PIN_PC"
	joySourceType       = "joycoder-ide"
	joyClientName       = "JoyCode"
	// gateway 禁用哨兵：设置里填这些值就改走直连 v2 路径
	joyColorDisabled = "direct"
)

// JoyCode 上游端点（v1 路径 + color gateway functionId 映射）。
const (
	joyEpChat      = "/api/saas/openai/v1/chat/completions"
	joyEpAnthropic = "/api/saas/anthropic/v1/messages"
	joyEpModels    = "/api/saas/models/v1/modelList"
	joyEpUserInfo  = "/api/saas/user/v1/userInfo"
	joyEpWebSearch = "/api/saas/openai/v1/web-search"
)

type joyEndpoint struct {
	functionID string
	v2Path     string
}

var joyEndpoints = map[string]joyEndpoint{
	joyEpChat:      {"chat_completions", "/api/saas/openai/v2/chat/completions"},
	joyEpAnthropic: {"anthropic_completions", "/api/saas/anthropic/v1/messages"},
	joyEpModels:    {"joycode_modelList", "/api/saas/models/v2/modelList"},
	joyEpUserInfo:  {"joycode_userInfo", "/api/saas/user/v2/userInfo"},
	joyEpWebSearch: {"web_search", "/api/saas/openai/v2/web-search"},
}

// 内置模型兜底：上游目录不可达时仍能建号/建路由。
var joyFallbackModels = []string{
	"JoyAI-Code", "JoyAI-Code-1.5", "Claude-Opus-4.7", "MiniMax-M2.7",
	"Kimi-K2.6", "Kimi-K2.5", "GLM-5.1", "GLM-5", "GLM-4.7", "Doubao-Seed-2.0-pro",
}

// joyModelCap 模型能力（上下文 / 最大输出 / 推理），来源 JoyCode2Api 的能力表。
type joyModelCap struct {
	Context int32
	Output  int32
	Reason  bool
	Vision  bool
	Series  string
}

var joyModelCaps = map[string]joyModelCap{
	"JoyAI-Code":          {Context: 200000, Output: 64000, Series: "JoyAI"},
	"JoyAI-Code-1.5":      {Context: 200000, Output: 64000, Series: "JoyAI"},
	"Claude-Opus-4.7":     {Context: 200000, Output: 32000, Series: "Claude"},
	"MiniMax-M2.7":        {Context: 200000, Output: 16384, Reason: true, Series: "MiniMax"},
	"Kimi-K2.5":           {Context: 200000, Output: 16384, Vision: true, Series: "Kimi"},
	"Kimi-K2.6":           {Context: 200000, Output: 16384, Reason: true, Vision: true, Series: "Kimi"},
	"GLM-5.1":             {Context: 200000, Output: 16384, Reason: true, Series: "GLM"},
	"GLM-5":               {Context: 200000, Output: 8192, Series: "GLM"},
	"GLM-4.7":             {Context: 200000, Output: 8192, Series: "GLM"},
	"Doubao-Seed-2.0-pro": {Context: 200000, Output: 16384, Series: "Doubao"},
}

// ---------- 凭据 ----------

type joyCred struct {
	PtKey         string `json:"pt_key"`
	UserID        string `json:"user_id"`
	ColorBaseURL  string `json:"color_base_url,omitempty"`
	MasterBaseURL string `json:"master_base_url,omitempty"`
	Tenant        string `json:"tenant,omitempty"`
	LoginType     string `json:"login_type,omitempty"`
	OrgFullName   string `json:"org_full_name,omitempty"`
	DisplayName   string `json:"display_name,omitempty"`

	proxyURL string
}

func joyCredFrom(blob *pb.CredentialBlob) (*joyCred, error) {
	raw := blob.GetBlob()
	if len(raw) == 0 {
		return nil, fmt.Errorf("凭据为空")
	}
	var c joyCred
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("凭据解析失败：%w", err)
	}
	if strings.TrimSpace(c.PtKey) == "" {
		return nil, fmt.Errorf("凭据缺少 ptKey")
	}
	if strings.TrimSpace(c.UserID) == "" {
		return nil, fmt.Errorf("凭据缺少 userId")
	}
	c.proxyURL = proxyURLOf(blob.GetProxy())
	return &c, nil
}

func joyMarshalCred(c *joyCred) []byte {
	b, _ := json.Marshal(c)
	return b
}

// ---------- HTTP ----------

var joyClientCache sync.Map

func (p *plugin) httpClient(cred *joyCred) *http.Client {
	key := ""
	if cred != nil {
		key = cred.proxyURL
	}
	if c, ok := joyClientCache.Load(key); ok {
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
	joyClientCache.Store(key, c)
	return c
}

// ---------- 设置 ----------

func (p *plugin) settings() map[string]interface{} {
	out := map[string]interface{}{}
	if p.host == nil {
		return out
	}
	raw := p.host.Settings("joycode")
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

// clientVersion 出站版本号：核心「出站标识」优先，其次插件自有设置，最后内置默认。
// 两处都用于 UA 的 <客户端名称>/<版本> 与请求体 clientVersion。
func (p *plugin) clientVersion() string {
	if v := strings.TrimSpace(str(p.settings()["outbound_client_version"])); v != "" {
		return v
	}
	return p.settingStr("client_version", joyDefaultVersion)
}

// clientName 出站客户端名称（默认对齐官方分发包的 JoyCode）。
func (p *plugin) clientName() string {
	if v := strings.TrimSpace(str(p.settings()["outbound_client_name"])); v != "" {
		return v
	}
	return joyClientName
}

// colorBase 解析出站网关地址；返回空串表示走直连 v2。
func (p *plugin) colorBase(cred *joyCred) string {
	base := ""
	if cred != nil {
		base = strings.TrimSpace(cred.ColorBaseURL)
	}
	if base == "" {
		base = p.settingStr("color_base_url", joyDefaultColorBase)
	}
	if strings.EqualFold(base, joyColorDisabled) || base == "-" {
		return ""
	}
	return base
}

// baseURL 直连模式下的主站地址。
func (p *plugin) baseURL(cred *joyCred) string {
	if cred != nil && strings.TrimSpace(cred.MasterBaseURL) != "" {
		return strings.TrimRight(strings.TrimSpace(cred.MasterBaseURL), "/")
	}
	return p.settingStr("master_base_url", joyBaseURL)
}

// tenant 信封里的租户标识（Anthropic 方言缺省 JD）。
func joyTenantFor(cred *joyCred, anthropic bool) string {
	if cred != nil && strings.TrimSpace(cred.Tenant) != "" {
		return strings.TrimSpace(cred.Tenant)
	}
	if anthropic {
		return "JD"
	}
	return joyDefaultTenant
}

func joyLoginTypeFor(cred *joyCred, anthropic bool) string {
	if cred != nil && strings.TrimSpace(cred.LoginType) != "" {
		return strings.TrimSpace(cred.LoginType)
	}
	if anthropic {
		return "PIN_JD_CLOUD"
	}
	return joyDefaultLoginType
}

// ---------- 签名 ----------

// joyColorSign 构造 color gateway 的 query 与 HMAC 签名。
// 规范串 = appid + "&" + functionId + "&" + ts（Unix 毫秒）。
func joyColorSign(functionID string, ts int64) (string, string) {
	t := strconv.FormatInt(ts, 10)
	signStr := joyColorAppID + "&" + functionID + "&" + t
	mac := hmac.New(sha256.New, []byte(joyColorHMACKey))
	mac.Write([]byte(signStr))
	sign := hex.EncodeToString(mac.Sum(nil))
	query := "appid=" + joyColorAppID + "&functionId=" + functionID + "&t=" + t
	return query, sign
}

// requestURL 把端点解析为最终请求地址。
func (p *plugin) requestURL(cred *joyCred, endpoint string) string {
	ep, ok := joyEndpoints[endpoint]
	if !ok {
		return p.baseURL(cred) + endpoint
	}
	if cb := p.colorBase(cred); cb != "" {
		if u, err := url.Parse(cb); err == nil && u.Host != "" {
			query, sign := joyColorSign(ep.functionID, time.Now().UnixMilli())
			basePath := strings.TrimRight(u.Path, "/")
			return u.Scheme + "://" + u.Host + basePath + joyColorPath + "?" + query + "&sign=" + sign
		}
	}
	return p.baseURL(cred) + ep.v2Path
}

// ---------- 请求头 ----------

// joyBuildUserAgent 按官方指纹拼装 UA（纯函数，便于测试）。
func joyBuildUserAgent(name, ver string) string {
	return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) " +
		name + "/" + ver + " Chrome/133.0.0.0 Electron/35.2.0 Safari/537.36"
}

// joyUserAgent 出站 UA；outbound_user_agent 非空时整段覆盖。
func (p *plugin) joyUserAgent() string {
	if v := strings.TrimSpace(str(p.settings()["outbound_user_agent"])); v != "" {
		return v
	}
	return joyBuildUserAgent(p.clientName(), p.clientVersion())
}

// joyHeaders OpenAI 方言出站头（流式与非流式都用 identity，避免 gzip 缓冲）。
func (p *plugin) joyHeaders(cred *joyCred, stream bool) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=UTF-8")
	h.Set("source-type", joySourceType)
	h.Set("ptKey", cred.PtKey)
	h.Set("loginType", joyLoginTypeFor(cred, false))
	h.Set("User-Agent", p.joyUserAgent())
	h.Set("Accept", "*/*")
	h.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	if stream {
		h.Set("Accept-Encoding", "identity")
	} else {
		h.Set("Accept-Encoding", "gzip, deflate")
	}
	return h
}

// joyAnthropicHeaders Anthropic 方言出站头（loginType 缺省 PIN_JD_CLOUD）。
func (p *plugin) joyAnthropicHeaders(cred *joyCred, stream bool) http.Header {
	h := p.joyHeaders(cred, stream)
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("loginType", joyLoginTypeFor(cred, true))
	return h
}

// ---------- 请求体信封 ----------

func (p *plugin) joyEnvelope(cred *joyCred, extra map[string]interface{}) map[string]interface{} {
	body := map[string]interface{}{
		"tenant":        joyTenantFor(cred, false),
		"orgFullName":   cred.OrgFullName,
		"userId":        cred.UserID,
		"client":        joyClientName,
		"clientVersion": p.clientVersion(),
		"language":      "UNKNOWN",
	}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

func (p *plugin) joyAnthropicEnvelope(cred *joyCred, extra map[string]interface{}) map[string]interface{} {
	body := map[string]interface{}{
		"tenant":        joyTenantFor(cred, true),
		"orgFullName":   cred.OrgFullName,
		"userId":        cred.UserID,
		"client":        joyClientName,
		"clientVersion": p.clientVersion(),
		"language":      "UNKNOWN",
		"stream":        true,
	}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

// ---------- 请求执行 ----------

// joyPost 非流式请求：返回解析后的 JSON。
func (p *plugin) joyPost(ctx context.Context, cred *joyCred, endpoint string, extra map[string]interface{}) (map[string]interface{}, error) {
	payload, err := json.Marshal(p.joyEnvelope(cred, extra))
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.requestURL(cred, endpoint), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header = p.joyHeaders(cred, false)
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := joyReadBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, joyClip(string(raw), 500))
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("响应不是 JSON：%s", joyClip(string(raw), 300))
	}
	return out, nil
}

// joyPostStream 流式请求：返回原始响应体（已按需解 gzip）。
func (p *plugin) joyPostStream(ctx context.Context, cred *joyCred, endpoint string, extra map[string]interface{}) (*http.Response, []byte, error) {
	payload, err := json.Marshal(p.joyEnvelope(cred, extra))
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.requestURL(cred, endpoint), bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	req.Header = p.joyHeaders(cred, true)
	resp, err := p.httpClient(cred).Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return resp, raw, fmt.Errorf("HTTP %d: %s", resp.StatusCode, joyClip(string(raw), 500))
	}
	return resp, nil, nil
}

// joyReadBody 读取响应体（必要时解 gzip）。
func joyReadBody(resp *http.Response) ([]byte, error) {
	var r io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}
	return io.ReadAll(io.LimitReader(r, 8<<20))
}

// joyStreamReader 流式读取器：identity 优先，gzip 兜底。
func joyStreamReader(resp *http.Response) (io.Reader, io.Closer, error) {
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, nil, err
		}
		return gz, gz, nil
	}
	return resp.Body, nil, nil
}

// ---------- 上游业务接口 ----------

// joyUserInfo 拉取用户资料（同时作为凭据校验）。
func (p *plugin) joyUserInfo(ctx context.Context, cred *joyCred) (map[string]interface{}, error) {
	return p.joyPost(ctx, cred, joyEpUserInfo, map[string]interface{}{})
}

// joyModelInfo 上游模型目录条目。
type joyModelInfo struct {
	Label              string   `json:"label"`
	ChatAPIModel       string   `json:"chatApiModel"`
	MaxTotalTokens     int      `json:"maxTotalTokens"`
	RespMaxTokens      int      `json:"respMaxTokens"`
	Temperature        float64  `json:"temperature"`
	Features           []string `json:"features"`
	SupportStream      bool     `json:"supportStream"`
	VerificationStatus string   `json:"verificationStatus"`
	ModelID            string   `json:"modelId"`
	CreateTime         int64    `json:"createTime"`
}

// joyFetchModels 拉取上游模型目录。
func (p *plugin) joyFetchModels(ctx context.Context, cred *joyCred) ([]joyModelInfo, error) {
	resp, err := p.joyPost(ctx, cred, joyEpModels, map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	items, _ := resp["data"].([]interface{})
	if len(items) == 0 {
		return nil, fmt.Errorf("模型目录为空")
	}
	out := make([]joyModelInfo, 0, len(items))
	for _, it := range items {
		b, err := json.Marshal(it)
		if err != nil {
			continue
		}
		var m joyModelInfo
		if err := json.Unmarshal(b, &m); err != nil {
			continue
		}
		if m.ModelID == "" && m.ChatAPIModel == "" && m.Label == "" {
			continue
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("模型目录无可识别条目")
	}
	return out, nil
}

// joyModelID 上游条目对应的对外模型 id（优先 modelId，其次 chatApiModel，最后 label）。
func joyModelID(m joyModelInfo) string {
	if strings.TrimSpace(m.ModelID) != "" {
		return strings.TrimSpace(m.ModelID)
	}
	if strings.TrimSpace(m.ChatAPIModel) != "" {
		return strings.TrimSpace(m.ChatAPIModel)
	}
	return strings.TrimSpace(m.Label)
}

// ---------- 用户信息解析 ----------

// joyUserIDFrom 兼容两种响应形态：data.userId 与顶层 userId。
func joyUserIDFrom(resp map[string]interface{}) string {
	if d, ok := resp["data"].(map[string]interface{}); ok {
		if v, _ := d["userId"].(string); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if v, _ := resp["userId"].(string); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

// joyNameFrom 展示名：realName / nickName 依次兜底。
func joyNameFrom(resp map[string]interface{}) string {
	pick := func(m map[string]interface{}) string {
		for _, k := range []string{"realName", "nickName", "userName", "pin"} {
			if v, _ := m[k].(string); strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
		return ""
	}
	if d, ok := resp["data"].(map[string]interface{}); ok {
		if v := pick(d); v != "" {
			return v
		}
	}
	return pick(resp)
}

// joyRefreshedPtKey 上游可能在 userInfo 里回吐刷新后的 ptKey。
func joyRefreshedPtKey(resp map[string]interface{}) string {
	if d, ok := resp["data"].(map[string]interface{}); ok {
		if v, _ := d["ptKey"].(string); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if v, _ := resp["ptKey"].(string); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

// joyCode 上游统一业务码。
func joyCode(resp map[string]interface{}) (float64, string) {
	code, _ := resp["code"].(float64)
	msg, _ := resp["msg"].(string)
	if msg == "" {
		msg, _ = resp["message"].(string)
	}
	return code, msg
}

// ---------- 工具 ----------

func joyClip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// joySSEStream 把上游 SSE 响应体切成 data: 行交给 parser。
// 同时支持 [DONE] 终止符与事件边界。
func joyPumpSSE(r io.Reader, feed func(string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		feed(sc.Text())
	}
	return sc.Err()
}

// joySortedKeys 仅供测试与调试输出用的稳定键序。
func joySortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
