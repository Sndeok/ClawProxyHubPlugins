// catalog_models.go — 模型目录：直连腾讯模型接口，拿全「模型中心」要展示的字段。
//
// 为什么不走上游 /v1/models：那条路会丢掉显示名（name）与推理档位
// （reasoning.supportedEfforts），也拿不到积分倍率（credits）。模型中心要展示
// 系列 / 倍率 / 上下文 / 最大输出 / 推理档位，只能照官方客户端的取数方式直连：
//
//   1. 企业端点：/console/enterprises/personal/models（国内）或
//      /v2/enterprises/personal/models（国际，优先探测）
//   2. /v3/config：少数模型只在这里出现（实测国际版 deepseek-v4.1-flash 等）
//
// 两路合并：/v3/config 优先，企业端点补缺；disabled 与非对话模型剔除。
// 全部失败时调用方回退到 auto 单条，账号仍可正常使用。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

const (
	// 国际版企业端点在前：国内账号访问 /v2 会 404，国际版访问 /console 亦可能失败，
	// 两条都试一遍的代价只是一次 404。
	pathModelsV2     = "/v2/enterprises/personal/models"
	pathModelsV3     = "/v3/config"
	pathModelsConsole = "/console/enterprises/personal/models"
)

// upstreamModel 腾讯模型条目（字段名照官方客户端 / 参考实现实测结果）。
type upstreamModel struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	MaxInputTokens    int64    `json:"maxInputTokens"`
	MaxOutputTokens   int64    `json:"maxOutputTokens"`
	Disabled          bool     `json:"disabled"`
	SupportsImages    bool     `json:"supportsImages"`
	SupportsReasoning bool     `json:"supportsReasoning"`
	SupportsToolCall  bool     `json:"supportsToolCall"`
	OnlyReasoning     bool     `json:"onlyReasoning"`
	IsDefault         bool     `json:"isDefault"`
	DescriptionZh     string   `json:"descriptionZh"`
	Credits           string   `json:"credits"`
	Vendor            string   `json:"vendor"`
	Tags              []string `json:"tags"`
	Reasoning         struct {
		SupportedEfforts []string `json:"supportedEfforts"`
		DefaultEffort    string   `json:"defaultEffort"`
		Summary          string   `json:"summary"`
	} `json:"reasoning"`
}

// upstreamModelPayload 企业端点 / v3 的 models + agents 信封。
type upstreamModelPayload struct {
	Models []upstreamModel `json:"models"`
	Agents []struct {
		Name   string   `json:"name"`
		Models []string `json:"models"`
	} `json:"agents"`
}

// autoModel 目录不可用时的兜底：上游按 model=auto 自行路由。
func autoModel() *pb.ModelInfo {
	return &pb.ModelInfo{
		Id:             "auto",
		Label:          map[string]string{"zh": "自动（上游路由）", "en": "Auto (upstream routing)"},
		SupportsTools:  true,
		SupportsStream: true,
		Series:         "自动选择",
	}
}

// fetchModelCatalog 拉取并合并模型目录；两路都失败才算失败。
func (p *plugin) fetchModelCatalog(ctx context.Context, cred *credential) ([]*pb.ModelInfo, error) {
	sources := []struct {
		path    string
		cliOnly bool
	}{
		{pathModelsV3, false},
		{pathModelsV2, false},
		{pathModelsConsole, true},
	}
	var ok []catalogSource
	lastErr := fmt.Errorf("模型接口不可用")
	for _, src := range sources {
		payload, err := p.getModelPayload(ctx, cred, src.path)
		if err != nil {
			lastErr = err
			continue
		}
		ok = append(ok, catalogSource{payload: payload, cliOnly: src.cliOnly})
	}
	if len(ok) == 0 {
		return nil, lastErr
	}
	return mergeCatalog(ok)
}

// catalogSource 一路目录响应：cliOnly 表示要按 agents[cli].models 白名单过滤。
type catalogSource struct {
	payload *upstreamModelPayload
	cliOnly bool
}

// mergeCatalog 按来源优先级合并（调用方已按 /v3 → /v2 → /console 排序）：
// 同 id 先到先得，disabled 与非对话模型剔除。
func mergeCatalog(sources []catalogSource) ([]*pb.ModelInfo, error) {
	seen := map[string]bool{}
	out := make([]*pb.ModelInfo, 0, 32)
	for _, src := range sources {
		if src.payload == nil {
			continue
		}
		allow := map[string]bool{}
		if src.cliOnly {
			for _, ag := range src.payload.Agents {
				if ag.Name == "cli" {
					for _, id := range ag.Models {
						allow[id] = true
					}
				}
			}
		}
		for _, m := range src.payload.Models {
			if m.ID == "" || m.Disabled || seen[m.ID] {
				continue
			}
			if len(allow) > 0 && !allow[m.ID] {
				continue
			}
			if nonChatModel(m) {
				continue
			}
			seen[m.ID] = true
			out = append(out, toModelInfo(m))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("模型接口未返回可用模型")
	}
	return out, nil
}

// getModelPayload 取单路模型目录（容忍 models/agents 信封与「纯模型名数组」窄表）。
func (p *plugin) getModelPayload(ctx context.Context, cred *credential, path string) (*upstreamModelPayload, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", upstreamBase+path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range p.headers(cred, true) {
		req.Header.Set(k, v)
	}
	resp, err := p.hc(cred).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := envelope(resp)
	if err != nil {
		return nil, err
	}
	var payload upstreamModelPayload
	if json.Unmarshal(data, &payload) == nil && (len(payload.Models) > 0 || len(payload.Agents) > 0) {
		return &payload, nil
	}
	var names []string
	if json.Unmarshal(data, &names) == nil && len(names) > 0 {
		for _, n := range names {
			if n = strings.TrimSpace(n); n != "" {
				payload.Models = append(payload.Models, upstreamModel{ID: n})
			}
		}
		return &payload, nil
	}
	return nil, fmt.Errorf("模型目录解析失败: %s", path)
}

// toModelInfo 上游条目 → 信封模型信息（展示用字段全量带上）。
func toModelInfo(m upstreamModel) *pb.ModelInfo {
	name := strings.TrimSpace(m.Name)
	if name == "" {
		name = m.ID
	}
	info := &pb.ModelInfo{
		Id:                     m.ID,
		Label:                  map[string]string{"zh": name, "en": name},
		ContextWindow:          int32(m.MaxInputTokens),
		MaxOutputTokens:        int32(m.MaxOutputTokens),
		SupportsTools:          m.SupportsToolCall,
		SupportsStream:         true,
		Series:                 seriesOf(m.ID, m.Vendor),
		ReasoningEfforts:       m.Reasoning.SupportedEfforts,
		DefaultReasoningEffort: m.Reasoning.DefaultEffort,
		CreditsMultiplier:      parseCredits(m.Credits),
		Description:            strings.TrimSpace(m.DescriptionZh),
	}
	info.Tags = modelTags(m)
	return info
}

// seriesOf 系列归属：优先用上游 vendor，缺失时按 id 前缀推导，认不出归「其他」。
func seriesOf(id, vendor string) string {
	if v := strings.TrimSpace(vendor); v != "" {
		return v
	}
	low := strings.ToLower(id)
	for _, rule := range []struct {
		prefixes []string
		label    string
	}{
		{[]string{"glm"}, "智谱 GLM"},
		{[]string{"deepseek"}, "DeepSeek"},
		{[]string{"kimi", "moonshot"}, "Kimi"},
		{[]string{"minimax"}, "MiniMax"},
		{[]string{"hy", "hunyuan"}, "腾讯混元"},
		{[]string{"auto"}, "自动选择"},
	} {
		for _, p := range rule.prefixes {
			if strings.HasPrefix(low, p) {
				return rule.label
			}
		}
	}
	return "其他"
}

// parseCredits 积分倍率原文（如 "x0.05" / "0.1"）→ 数字；认不出返回 0（= 未提供）。
func parseCredits(s string) float64 {
	v := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(s), "x"), "X"))
	if v == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}

// modelTags 能力标签：上游 tags + 由布尔能力推导的中文标签（模型中心直接展示）。
func modelTags(m upstreamModel) []string {
	out := make([]string, 0, len(m.Tags)+4)
	for _, t := range m.Tags {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	if m.SupportsImages {
		out = append(out, "多模态")
	}
	if m.OnlyReasoning {
		out = append(out, "仅推理")
	} else if m.SupportsReasoning || len(m.Reasoning.SupportedEfforts) > 0 {
		out = append(out, "支持推理")
	}
	if m.SupportsToolCall {
		out = append(out, "工具调用")
	}
	if m.IsDefault {
		out = append(out, "默认")
	}
	return out
}

// nonChatModel 非对话模型（选了必然报错）：嵌入/补全/代码专用前缀、输出过小、文生图。
func nonChatModel(m upstreamModel) bool {
	low := strings.ToLower(m.ID)
	for _, p := range []string{"nes-", "completion-", "codewise-"} {
		if strings.HasPrefix(low, p) {
			return true
		}
	}
	if m.MaxOutputTokens > 0 && m.MaxOutputTokens <= 256 {
		return true
	}
	for _, t := range m.Tags {
		if t == "text-to-image" {
			return true
		}
	}
	return false
}
