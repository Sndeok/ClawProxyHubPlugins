// joycode 插件入口：握手 / 登录 / 刷新 / 资料。
//
// 登录方式：
//  1. oauth  —— 打开 JoyCode 官方登录页（京东账号扫码），完成授权后浏览器会跳到
//     http://127.0.0.1:<authPort>/...?...pt_key=xxx；该地址打不开是正常的，
//     把地址栏完整 URL 粘贴回来即可（与 JoyCode2Api 的远程部署用法一致）。
//  2. manual —— 直接粘贴 ptKey + userId（或 state.vscdb 里的 joyCoderUser JSON）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Sndeok/ClawProxyHub-Next/sdk"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// version 打包时经 -ldflags "-X main.version=..." 注入。
var version = "dev"

func main() { sdk.Serve(&plugin{}) }

type plugin struct {
	pb.UnimplementedClawPluginServer
	host *sdk.Host
}

func (p *plugin) SetHost(host *sdk.Host) { p.host = host }

// OAuth 回调端口：插件不在用户浏览器所在机器上监听，这个端口只用于让
// JoyCode 登录页跳转出一个带 pt_key 的地址（用户复制回来）。
const joyOAuthAuthPort = "19999"

// joyLoginURL 构造 JoyCode 官方登录页地址（与 JoyCode IDE 同一套机制）。
func joyLoginURL(authKey string) string {
	q := url.Values{}
	q.Set("authPort", joyOAuthAuthPort)
	q.Set("fromIde", "true")
	q.Set("ideAppName", "JoyCode")
	q.Set("loginType", "PIN")
	q.Set("authKey", authKey)
	return "https://joycode.jd.com/login?" + q.Encode()
}

// ---------- 握手 ----------

func (p *plugin) Handshake(ctx context.Context, req *pb.HandshakeRequest) (*pb.HandshakeResponse, error) {
	return &pb.HandshakeResponse{Manifest: &pb.Manifest{
		Name: "joycode", Version: version, Author: "cph",
		Label:           map[string]string{"zh": "JoyCode", "en": "JoyCode"},
		Icon:            "icon.png",
		ProtocolVersion: sdk.ProtocolVersion,
		Capabilities:    []string{"account", "login", "refresh", "models", "chat"},
		Endpoints:       []string{"chat_completions", "messages", "responses"},
		SettingsSchema: `{
			"type": "object",
			"properties": {
				"client_version": {
					"type": "string",
					"title": "客户端版本（伪装）",
					"description": "出站 User-Agent 里 JoyCode/<版本> 与请求体 clientVersion，留空 = 2.7.5（对齐官方分发包）",
					"default": "2.7.5"
				},
				"color_base_url": {
					"type": "string",
					"title": "Color 网关地址",
					"description": "默认 https://api-ai.jd.com（带 HMAC 签名的快速通道）；填 direct 改为直连 joycode-api.jd.com 的 v2 路径",
					"default": ""
				},
				"master_base_url": {
					"type": "string",
					"title": "主站地址",
					"description": "直连模式使用，留空 = https://joycode-api.jd.com",
					"default": ""
				},
				"outbound_user_agent": {
					"type": "string",
					"title": "出站 User-Agent（整段）",
					"description": "留空 = 按下面的客户端名称/版本拼装（已对齐官方 JoyCode 分发包指纹）",
					"default": ""
				},
				"outbound_client_name": {
					"type": "string",
					"title": "客户端名称",
					"description": "UA 里 <名称>/<版本> 的名称段；留空 = JoyCode",
					"default": "JoyCode"
				},
				"outbound_client_version": {
					"type": "string",
					"title": "客户端版本",
					"description": "UA 的 JoyCode/<版本> 与请求体 clientVersion；留空 = 用插件设置里的客户端版本",
					"default": ""
				},
				"outbound_cli_version": {
					"type": "string",
					"title": "CLI 版本（JoyCode 不适用）",
					"description": "JoyCode 官方客户端 UA 没有 CLI 段，此项留空即可，填了也不会进 UA",
					"default": ""
				},
				"extra_headers": {
					"type": "string",
					"title": "额外请求头",
					"description": "形如 Key: Value; Key2: Value2，用于上游指纹变化时临时覆盖",
					"default": ""
				}
			}
		}`,
		AuthMethods: []*pb.AuthMethod{
			{
				Id: "oauth", Label: map[string]string{"zh": "浏览器授权（京东账号）", "en": "Browser auth (JD account)"},
				Capabilities: []string{"refreshable", "profile"},
				// wait：回调落在用户自己的 127.0.0.1，插件收不到 → 由用户粘贴回调地址
				Callback: "wait",
			},
			{
				Id: "manual", Label: map[string]string{"zh": "手动粘贴凭据", "en": "Paste credentials"},
				Capabilities: []string{"refreshable", "profile"},
				Fields: []*pb.AuthField{
					{
						Name: "content", Label: map[string]string{"zh": "凭据（ptKey + userId）", "en": "Credentials (ptKey + userId)"},
						Type: "textarea", Required: true,
						Placeholder: "{\"ptKey\":\"AA...\",\"userId\":\"123456\"} 或每行 key=value，也支持 state.vscdb 里 joyCoderUser 的完整 JSON",
					},
					{
						Name: "color_base_url", Label: map[string]string{"zh": "Color 网关地址（可选）", "en": "Color gateway (optional)"},
						Type: "text", Placeholder: "https://api-ai.jd.com",
					},
				},
			},
		},
	}}, nil
}

// ---------- 登录 ----------

func (p *plugin) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	switch req.MethodId {
	case "oauth", "":
		return p.loginOAuth(ctx, req)
	case "manual", "token":
		return p.loginManual(ctx, req)
	default:
		return nil, status.Error(codes.InvalidArgument, "未知授权方式: "+req.MethodId)
	}
}

// loginOAuth 两步：先给授权链接；用户粘贴回调地址后再换凭据入库。
func (p *plugin) loginOAuth(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	raw := strings.TrimSpace(req.Form["content"])
	state := strings.TrimSpace(string(req.State))
	if raw == "" && state == "" {
		authKey := "cph_" + strconv.FormatInt(nowUnixMilli(), 36)
		return &pb.LoginResult{Next: &pb.LoginNextStep{
			Action: "open_url",
			Url:    joyLoginURL(authKey),
			Prompt: map[string]string{
				"zh": "点「打开授权页」用京东账号登录 JoyCode。登录完成后浏览器会跳到一个打不开的 http://127.0.0.1:19999/... 地址——这是正常的，把地址栏里的完整 URL 复制粘贴到下面提交（其中含 pt_key）。",
				"en": "Open the auth page and sign in with your JD account. The browser then lands on an unreachable http://127.0.0.1:19999/... URL — that is expected. Copy the full URL from the address bar and paste it below (it contains pt_key).",
			},
			Wait:  true,
			State: []byte(authKey),
			Fields: []*pb.AuthField{{
				Name: "content", Label: map[string]string{"zh": "回调地址（含 pt_key）", "en": "Callback URL (contains pt_key)"},
				Type: "textarea", Required: true,
				Placeholder: "http://127.0.0.1:19999/?pt_key=AA...&login_type=PIN&tenant=JOYCODE",
			}},
		}}, nil
	}
	cred, err := parseJoyCredentialInput(raw)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return p.finishLogin(ctx, cred)
}

// loginManual 直接粘贴凭据（JSON / key=value / 回调 URL 均可）。
func (p *plugin) loginManual(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResult, error) {
	raw := strings.TrimSpace(req.Form["content"])
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "请粘贴凭据（至少包含 ptKey 与 userId）")
	}
	cred, err := parseJoyCredentialInput(raw)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// 表单里的可选覆盖项优先于粘贴内容
	for key, dst := range map[string]*string{
		"color_base_url": &cred.ColorBaseURL,
		"login_type":     &cred.LoginType,
		"tenant":         &cred.Tenant,
	} {
		if v := strings.TrimSpace(req.Form[key]); v != "" {
			*dst = v
		}
	}
	return p.finishLogin(ctx, cred)
}

// finishLogin 补齐 userId → 校验凭据 → 建档。
func (p *plugin) finishLogin(ctx context.Context, cred *joyCred) (*pb.LoginResult, error) {
	if strings.TrimSpace(cred.UserID) == "" {
		if err := p.fillUserID(ctx, cred); err != nil {
			return nil, status.Error(codes.Unauthenticated, "无法从上游获取 userId："+err.Error())
		}
	}
	profile, err := p.profileFor(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	// profileFor 可能刷新过 ptKey，必须重新序列化
	return &pb.LoginResult{Blob: joyMarshalCred(cred), Profile: profile}, nil
}

// fillUserID 拿 userInfo 补齐 userId（OAuth 回调只带 pt_key）。
func (p *plugin) fillUserID(ctx context.Context, cred *joyCred) error {
	resp, err := p.joyUserInfo(ctx, cred)
	if err != nil {
		return err
	}
	if code, msg := joyCode(resp); code != 0 {
		return fmt.Errorf("上游返回 code=%.0f %s", code, msg)
	}
	id := joyUserIDFrom(resp)
	if id == "" {
		return fmt.Errorf("上游 userInfo 未返回 userId")
	}
	cred.UserID = id
	if cred.DisplayName == "" {
		cred.DisplayName = joyNameFrom(resp)
	}
	return nil
}

// ---------- 刷新 / 资料 ----------

func (p *plugin) Refresh(ctx context.Context, blob *pb.CredentialBlob) (*pb.RefreshResult, error) {
	cred, err := joyCredFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	profile, err := p.profileFor(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return &pb.RefreshResult{Blob: joyMarshalCred(cred), Profile: profile}, nil
}

func (p *plugin) GetProfile(ctx context.Context, blob *pb.CredentialBlob) (*pb.AccountProfile, error) {
	cred, err := joyCredFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	prof, err := p.profileFor(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return prof, nil
}

// profileFor 拉 userInfo 校验凭据、顺手回写刷新后的 ptKey，并附模型清单。
func (p *plugin) profileFor(ctx context.Context, cred *joyCred) (*pb.AccountProfile, error) {
	resp, err := p.joyUserInfo(ctx, cred)
	if err != nil {
		return nil, fmt.Errorf("凭据校验失败：%w", err)
	}
	if code, msg := joyCode(resp); code != 0 {
		return nil, fmt.Errorf("凭据无效（code=%.0f）：%s", code, msg)
	}
	if fresh := joyRefreshedPtKey(resp); fresh != "" {
		cred.PtKey = fresh
	}
	if name := joyNameFrom(resp); name != "" {
		cred.DisplayName = name
	}
	if id := joyUserIDFrom(resp); id != "" {
		cred.UserID = id
	}

	prof := &pb.AccountProfile{
		DisplayName: joyOrDefault(cred.DisplayName, maskUserID(cred.UserID)),
		Healthy:     true,
		Quota:       map[string]string{},
	}
	models, err := p.joyFetchModels(ctx, cred)
	if err != nil {
		prof.Sections = append(prof.Sections, joyNoteSection("models", "模型目录不可达", err.Error()))
		return prof, nil
	}
	prof.Quota["models"] = strconv.Itoa(len(models))
	prof.Sections = append(prof.Sections, joyModelSection(models))
	return prof, nil
}

// joyModelSection 模型表格（详情页动态渲染）。
func joyModelSection(models []joyModelInfo) *pb.ProfileSection {
	sec := &pb.ProfileSection{
		Id:    "models",
		Title: map[string]string{"zh": "可用模型", "en": "Available models"},
		Columns: []*pb.SectionColumn{
			{Key: "id", Title: map[string]string{"zh": "模型", "en": "Model"}},
			{Key: "label", Title: map[string]string{"zh": "名称", "en": "Label"}},
			{Key: "ctx", Title: map[string]string{"zh": "上下文", "en": "Context"}},
		},
	}
	for _, m := range models {
		id := joyModelID(m)
		ctx := ""
		if m.MaxTotalTokens > 0 {
			ctx = strconv.Itoa(m.MaxTotalTokens)
		}
		sec.Items = append(sec.Items, &pb.SectionRow{Cells: map[string]string{
			"id": id, "label": m.Label, "ctx": ctx,
		}})
	}
	if len(sec.Items) == 0 {
		return joyNoteSection("models", "可用模型", "上游未返回模型清单")
	}
	return sec
}

func joyNoteSection(id, title, detail string) *pb.ProfileSection {
	return &pb.ProfileSection{
		Id:    id,
		Title: map[string]string{"zh": title, "en": title},
		Entries: []*pb.SectionEntry{
			{Label: map[string]string{"zh": "说明", "en": "Note"}, Value: detail},
		},
	}
}

// ---------- 模型目录 ----------

// ListModels 上游模型目录 → 信封 ModelInfo。
// 上游给 maxTotalTokens / respMaxTokens / features；能力表补齐系列、视觉与推理档位。
// 目录里缺的内置模型（如默认 JoyAI-Code-1.5）在此补回，避免路由同步漏掉默认模型。
func (p *plugin) ListModels(ctx context.Context, blob *pb.CredentialBlob) (*pb.ModelList, error) {
	// 空凭据 = 核心刷新聚合目录（RefreshCatalog 对每个插件传空 blob）：
	// 返回内置清单，保证未建路由时 /v1/models 也能透出可用模型名。
	if blob == nil || len(blob.GetBlob()) == 0 {
		return &pb.ModelList{Models: joyFallbackModelInfos()}, nil
	}
	cred, err := joyCredFrom(blob)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	models, err := p.joyFetchModels(ctx, cred)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "拉取上游模型目录失败："+err.Error())
	}
	seen := map[string]bool{}
	out := make([]*pb.ModelInfo, 0, len(models)+len(joyFallbackModels))
	for _, m := range models {
		id := joyModelID(m)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, joyModelToInfo(id, m.Label, m.MaxTotalTokens, m.RespMaxTokens, m.Features))
	}
	for _, id := range joyFallbackModels {
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, joyModelToInfo(id, id, 0, 0, nil))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return &pb.ModelList{Models: out}, nil
}

// joyFallbackModelInfos 内置清单 → 信封模型（无凭据时的兜底目录）。
func joyFallbackModelInfos() []*pb.ModelInfo {
	out := make([]*pb.ModelInfo, 0, len(joyFallbackModels))
	for _, id := range joyFallbackModels {
		out = append(out, joyModelToInfo(id, id, 0, 0, nil))
	}
	return out
}

// joyModelToInfo 组装单个模型元数据：上游字段优先，能力表兜底。
func joyModelToInfo(id, label string, ctxTokens, outTokens int, features []string) *pb.ModelInfo {
	cap, hasCap := joyModelCaps[id]
	ctxWindow := int32(ctxTokens)
	if ctxWindow <= 0 && hasCap {
		ctxWindow = int32(cap.Context)
	}
	maxOut := int32(outTokens)
	if maxOut <= 0 && hasCap {
		maxOut = int32(cap.Output)
	}
	series := ""
	if hasCap {
		series = cap.Series
	}
	tags := make([]string, 0, len(features)+2)
	for _, f := range features {
		if v := strings.TrimSpace(f); v != "" {
			tags = append(tags, v)
		}
	}
	if hasCap && cap.Vision && !joyHasTag(tags, "多模态") {
		tags = append(tags, "多模态")
	}
	reasoning := joyReasoningModel(id) || (hasCap && cap.Reason) || joyFeatureContains(features, "reason")
	if reasoning && !joyHasTag(tags, "支持推理") {
		tags = append(tags, "支持推理")
	}
	info := &pb.ModelInfo{
		Id:              id,
		Label:           map[string]string{"zh": joyOrDefault(label, id), "en": joyOrDefault(label, id)},
		ContextWindow:   ctxWindow,
		MaxOutputTokens: maxOut,
		SupportsTools:   true,
		SupportsStream:  true,
		Series:          series,
		Tags:            tags,
	}
	if reasoning {
		info.ReasoningEfforts = []string{"low", "medium", "high"}
		info.DefaultReasoningEffort = "high"
	}
	return info
}

// joyHasTag 标签去重判断。
func joyHasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

// joyFeatureContains 上游 features 里是否含某关键字（不区分大小写）。
func joyFeatureContains(features []string, kw string) bool {
	for _, f := range features {
		if strings.Contains(strings.ToLower(f), strings.ToLower(kw)) {
			return true
		}
	}
	return false
}

// ---------- 凭据解析 ----------

// parseJoyCredentialInput 兼容多种输入形态：
//  1. 回调 URL（http://127.0.0.1:19999/?pt_key=...&login_type=PIN&tenant=JOYCODE）
//  2. JSON（{"joyCoderUser":{...}} / {"ptKey":...,"userId":...} / data 包裹）
//  3. key=value 文本（每行一条，或 & 连接）
func parseJoyCredentialInput(raw string) (*joyCred, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, fmt.Errorf("凭据内容为空")
	}
	if u, err := url.Parse(s); err == nil && u.Scheme != "" && u.Host != "" {
		if pt := u.Query().Get("pt_key"); pt != "" {
			cred := &joyCred{
				PtKey:       strings.TrimSpace(pt),
				UserID:      strings.TrimSpace(firstNonEmpty(u.Query().Get("user_id"), u.Query().Get("userId"))),
				LoginType:   strings.TrimSpace(u.Query().Get("login_type")),
				Tenant:      strings.TrimSpace(u.Query().Get("tenant")),
				OrgFullName: strings.TrimSpace(u.Query().Get("orgFullName")),
			}
			return cred, nil
		}
	}

	if strings.HasPrefix(s, "{") {
		if cred, ok := joyCredFromJSON([]byte(s)); ok {
			return cred, nil
		}
	}

	// key=value / key: value 文本
	kv := map[string]string{}
	for _, line := range strings.FieldsFunc(s, func(r rune) bool { return r == '\n' || r == '\r' || r == '&' || r == ';' }) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var k, v string
		if i := strings.Index(line, "="); i > 0 {
			k, v = line[:i], line[i+1:]
		} else if i := strings.Index(line, ":"); i > 0 {
			k, v = line[:i], line[i+1:]
		} else {
			continue
		}
		k = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(k, "\"")))
		k = strings.TrimSuffix(k, "\"")
		if k == "" {
			continue
		}
		kv[k] = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(v, "\""), "\""))
	}
	if len(kv) > 0 {
		cred := &joyCred{
			PtKey:         firstNonEmpty(kv["ptkey"], kv["pt_key"], kv["pt-key"]),
			UserID:        firstNonEmpty(kv["userid"], kv["user_id"], kv["user-id"]),
			ColorBaseURL:  firstNonEmpty(kv["colorbaseurl"], kv["color_base_url"]),
			MasterBaseURL: firstNonEmpty(kv["masterbaseurl"], kv["master_base_url"]),
			Tenant:        kv["tenant"],
			LoginType:     kv["logintype"],
			OrgFullName:   kv["orgfullname"],
		}
		if cred.PtKey != "" {
			return cred, nil
		}
	}

	// 兜底：把整段当 ptKey（用户随后可补 userId）
	if !strings.ContainsAny(s, " \t\n") && len(s) >= 16 {
		return &joyCred{PtKey: s}, nil
	}
	return nil, fmt.Errorf("未能从内容中识别出 ptKey")
}

// joyCredFromJSON 解析 JSON 形态凭据。
func joyCredFromJSON(raw []byte) (*joyCred, bool) {
	var any map[string]interface{}
	if json.Unmarshal(raw, &any) != nil {
		return nil, false
	}
	node := any
	// 解开常见包裹：joyCoderUser / data / credentials
	for _, wrap := range []string{"joyCoderUser", "data", "credentials"} {
		if inner, ok := node[wrap].(map[string]interface{}); ok {
			node = inner
			break
		}
	}
	pick := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := node[k].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
		return ""
	}
	cred := &joyCred{
		PtKey:         pick("ptKey", "pt_key", "ptkey", "pt-key"),
		UserID:        pick("userId", "user_id", "userid", "user-id", "uid"),
		ColorBaseURL:  pick("colorBaseUrl", "color_base_url", "colorBaseURL"),
		MasterBaseURL: pick("masterBaseUrl", "master_base_url", "masterBaseURL"),
		Tenant:        pick("tenant"),
		LoginType:     pick("loginType", "login_type"),
		OrgFullName:   pick("orgFullName", "org_full_name"),
		DisplayName:   pick("realName", "nickName", "userName", "displayName"),
	}
	if cred.PtKey == "" {
		return nil, false
	}
	return cred, true
}

// ---------- 工具 ----------

// str 把设置项值转字符串（JSON 解析后可能是任意类型）。
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

// proxyURLOf 把核心下发的代理配置转成 URL 字符串。
func proxyURLOf(p *pb.ProxyConfig) string {
	if p == nil || p.GetHost() == "" {
		return ""
	}
	u := &url.URL{Scheme: joyOrDefault(p.GetScheme(), "http"), Host: fmt.Sprintf("%s:%d", p.GetHost(), p.GetPort())}
	if p.GetUsername() != "" {
		u.User = url.UserPassword(p.GetUsername(), p.GetPassword())
	}
	return u.String()
}

func joyOrDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// nowUnixMilli 当前毫秒时间戳（OAuth authKey / 签名用）。
func nowUnixMilli() int64 { return time.Now().UnixMilli() }

// maskUserID 展示名兜底：手机号/长 ID 打码，避免日志与 UI 暴露完整账号。
func maskUserID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "JoyCode"
	}
	if len(id) <= 6 {
		return id
	}
	return id[:3] + "***" + id[len(id)-3:]
}
