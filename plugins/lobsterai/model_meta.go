// model_meta.go — 模型选型元数据：把上游 /api/models/available 的字段映射到信封。
//
// 字段名以官方客户端 src/main/main.ts 的 AvailableServerModel 类型为准：
//
//	modelId / modelName / provider / apiFormat
//	costMultiplier   —— 积分倍率（x0.73）
//	contextWindow    —— 上下文窗口
//	maxTokens        —— 单次最大输出
//	thinkingConfig   —— { options:[{level,openclawLevel}], defaultLevel }
//	supportsImage / supportsVideo / supportsThinking / supportsToolCalling
//	description / moreModel / accessible / restrictionHint
//
// 仍然做**容错**：按归一化 key 的别名表比对，并支持嵌套（thinkingConfig 在第二层），
// 取不到就留空（模型中心显示 -），绝不猜值。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// pathPricingCatalog 公开定价目录（无需鉴权）：倍率 / 上下文 / 推理档位的兜底来源。
const pathPricingCatalog = "/api/models/pricing-catalog"

// normKey 归一化 key：小写 + 去掉下划线/连字符/空格，便于别名比对。
func normKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch r {
		case '_', '-', ' ':
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// aliasGroups 每类元数据的候选字段名（归一化后比对，数组顺序即优先级）。
var aliasGroups = map[string][]string{
	"id":            {"modelid", "id", "model", "key"},
	"apiformat":     {"apiformat", "format", "protocol", "dialect"},
	"name":          {"modelname", "displayname", "name", "label", "title"},
	"series":        {"series", "family", "category", "vendor", "brand", "providerlabel", "provider"},
	"context":       {"contextwindow", "contextlength", "maxinputtokens", "maxcontexttokens", "contextsize", "inputtokenlimit", "context"},
	"maxoutput":     {"maxtokens", "maxoutputtokens", "outputtokenlimit", "maxcompletiontokens"},
	"multiplier":    {"costmultiplier", "creditsmultiplier", "creditratio", "creditsratio", "multiplier", "credits", "credit", "priceratio", "costratio", "rate", "factor", "weight", "points"},
	"efforts":       {"efforts", "supportedefforts", "reasoninglevels", "thinkinglevels", "thoughtlevels", "effortlevels", "levels", "options"},
	"defaulteffort": {"defaultlevel", "defaultreasoninglevel", "defaulteffort", "defaultthinking", "defaultthinkinglevel"},
	"tags":          {"tags", "capabilities", "features", "labels", "badges"},
	// 布尔能力（官方字段名 supportsImage / supportsVideo / supportsThinking / supportsToolCalling）
	"supportsimage":       {"supportsimage", "supportimage"},
	"supportsvideo":       {"supportsvideo", "supportvideo"},
	"supportsthinking":    {"supportsthinking", "supportreasoning"},
	"supportstoolcalling": {"supportstoolcalling", "supportstools", "supporttollcalling"},
	"description":         {"description", "descriptionzh", "desc", "intro", "remark"},
}

// findValue 递归查别名：本层按别名优先级命中即返回，未命中再按 key 排序下钻（深度上限）。
// 这样既能取顶层 costMultiplier，也能取 thinkingConfig.options[].level 这类嵌套值。
func findValue(node interface{}, group string, depth int) interface{} {
	if depth < 0 || node == nil {
		return nil
	}
	switch t := node.(type) {
	case map[string]interface{}:
		idx := make(map[string]interface{}, len(t))
		for k, v := range t {
			idx[normKey(k)] = v
		}
		for _, alias := range aliasGroups[group] {
			if v, ok := idx[normKey(alias)]; ok && v != nil {
				return v
			}
		}
		keys := make([]string, 0, len(idx))
		for k := range idx {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if got := findValue(idx[k], group, depth-1); got != nil {
				return got
			}
		}
	case []interface{}:
		for _, v := range t {
			if got := findValue(v, group, depth-1); got != nil {
				return got
			}
		}
	}
	return nil
}

// enrichModelInfo 用上游条目补全选型元数据（容错，缺字段就跳过）。
func enrichModelInfo(info *pb.ModelInfo, item map[string]json.RawMessage) {
	tree := map[string]interface{}{}
	if raw, err := json.Marshal(item); err == nil {
		_ = json.Unmarshal(raw, &tree)
	}
	if v := anyString(findValue(tree, "name", 2)); v != "" {
		info.Label = map[string]string{"zh": v, "en": v}
	}
	// 系列优先按 id 前缀推导（provider 对所有模型常常是同一个品牌名，无法分组）
	if info.Series == "" {
		info.Series = seriesOf(info.Id, anyString(findValue(tree, "series", 2)))
	}
	if n := anyCount(findValue(tree, "context", 2)); n > 0 {
		info.ContextWindow = int32(n)
	}
	if n := anyCount(findValue(tree, "maxoutput", 2)); n > 0 {
		info.MaxOutputTokens = int32(n)
	}
	if f := anyFloat(findValue(tree, "multiplier", 2)); f > 0 {
		info.CreditsMultiplier = f
	}
	if list := anyLevels(findValue(tree, "efforts", 3)); len(list) > 0 {
		info.ReasoningEfforts = list
	}
	if v := anyString(findValue(tree, "defaulteffort", 3)); v != "" {
		info.DefaultReasoningEffort = v
	}
	if v := anyString(findValue(tree, "description", 2)); v != "" {
		info.Description = v
	}
	info.Tags = modelTags(tree, info.ReasoningEfforts)
}

// anyLevels 把各种形态的「档位」归一成字符串数组：
//
//	["low","high"] / "低/中/高" / [{"level":"high"},{"level":"max"}]
func anyLevels(v interface{}) []string {
	switch t := v.(type) {
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, item := range t {
			switch e := item.(type) {
			case string:
				if s := strings.TrimSpace(e); s != "" {
					out = append(out, s)
				}
			case map[string]interface{}:
				// 官方形态：{level:"high", openclawLevel:"high"}
				if s := anyString(e["level"]); s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	case string:
		var out []string
		for _, part := range strings.FieldsFunc(t, func(r rune) bool {
			return r == ',' || r == '/' || r == '|' || r == '、' || r == ';'
		}) {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	}
	return nil
}

// modelTags 能力标签：上游 tags + 布尔能力推导（供模型中心筛选）。
func modelTags(tree map[string]interface{}, efforts []string) []string {
	out := []string{}
	if raw := findValue(tree, "tags", 2); raw != nil {
		if list := anyLevels(raw); len(list) > 0 {
			out = append(out, list...)
		} else if s := anyString(raw); s != "" {
			out = append(out, s)
		}
	}
	if anyBool(findValue(tree, "supportsimage", 2)) || anyBool(findValue(tree, "supportsvideo", 2)) {
		out = append(out, "多模态")
	}
	if anyBool(findValue(tree, "supportsthinking", 2)) || len(efforts) > 0 {
		out = append(out, "支持推理")
	}
	if anyBool(findValue(tree, "supportstoolcalling", 2)) {
		out = append(out, "工具调用")
	}
	return out
}

// enrichFromPricingCatalog 用公开定价目录补齐缺失的倍率/上下文/档位。
// 官方 /api/models/available 未必给 costMultiplier，定价目录一定有（且不需要凭据）。
// 失败静默：目录接口不可用时保持 available 的结果。
func (p *plugin) enrichFromPricingCatalog(ctx context.Context, cred *credential, models []*pb.ModelInfo) {
	if len(models) == 0 {
		return
	}
	req, err := http.NewRequestWithContext(ctx, "GET", serverBase+pathPricingCatalog, nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.hc(cred).Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	data, err := envelope(resp)
	if err != nil {
		return
	}
	var payload struct {
		TextModels []map[string]json.RawMessage `json:"textModels"`
	}
	if json.Unmarshal(data, &payload) != nil || len(payload.TextModels) == 0 {
		return
	}
	byID := make(map[string]map[string]json.RawMessage, len(payload.TextModels))
	for _, item := range payload.TextModels {
		if id := rawString(lookup(item, "id")); id != "" {
			byID[id] = item
		}
	}
	hit := 0
	for _, m := range models {
		if item, ok := byID[m.Id]; ok {
			enrichMissing(m, item)
			hit++
		}
	}
	if p.host != nil {
		p.host.Log("info", fmt.Sprintf("定价目录补齐 %d/%d 个模型（倍率/上下文/档位）", hit, len(models)))
	}
}

// seriesOf 系列归属：先按 id 前缀推导（参考 workbuddy-manager 的命名约定），
// 认不出再用上游 provider，最后归「其他」。
func seriesOf(id, provider string) string {
	low := strings.ToLower(strings.TrimSpace(id))
	for _, rule := range []struct {
		prefixes []string
		label    string
	}{
		{[]string{"glm"}, "智谱 GLM"},
		{[]string{"deepseek"}, "DeepSeek"},
		{[]string{"kimi", "moonshot"}, "Kimi"},
		{[]string{"minimax"}, "MiniMax"},
		{[]string{"doubao", "seed"}, "豆包"},
		{[]string{"qwen", "tongyi"}, "通义千问"},
		{[]string{"hy", "hunyuan"}, "腾讯混元"},
		{[]string{"claude"}, "Anthropic"},
		{[]string{"gpt"}, "OpenAI"},
		{[]string{"auto"}, "自动选择"},
	} {
		for _, pre := range rule.prefixes {
			if strings.HasPrefix(low, pre) {
				return rule.label
			}
		}
	}
	if provider != "" {
		return provider
	}
	return "其他"
}

// enrichMissing 只用条目里的非空字段补齐 target 的空缺（已有值不动）。
// 用于「公开定价目录」兜底：/api/models/available 缺 costMultiplier 时从这里补。
func enrichMissing(target *pb.ModelInfo, item map[string]json.RawMessage) {
	tmp := &pb.ModelInfo{Id: target.Id} // 带上 id：系列按前缀推导要用
	enrichModelInfo(tmp, item)
	if target.Label == nil && len(tmp.Label) > 0 {
		target.Label = tmp.Label
	}
	if target.Series == "" || target.Series == "其他" {
		if tmp.Series != "" {
			target.Series = tmp.Series
		}
	}
	if target.ContextWindow == 0 {
		target.ContextWindow = tmp.ContextWindow
	}
	if target.MaxOutputTokens == 0 {
		target.MaxOutputTokens = tmp.MaxOutputTokens
	}
	if len(target.ReasoningEfforts) == 0 {
		target.ReasoningEfforts = tmp.ReasoningEfforts
	}
	if target.DefaultReasoningEffort == "" {
		target.DefaultReasoningEffort = tmp.DefaultReasoningEffort
	}
	if target.CreditsMultiplier == 0 {
		target.CreditsMultiplier = tmp.CreditsMultiplier
	}
	if len(target.Tags) == 0 {
		target.Tags = tmp.Tags
	}
	if target.Description == "" {
		target.Description = tmp.Description
	}
}

// ---------- 基础取值 ----------

func anyString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	}
	return ""
}

func anyBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(strings.TrimSpace(t), "true")
	}
	return false
}

// anyFloat 数值：支持 0.73 / "x0.73" / "×0.73" / "0.73 倍"。
func anyFloat(v interface{}) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	s := anyString(v)
	if s == "" {
		return 0
	}
	s = strings.NewReplacer("x", "", "X", "", "×", "", "倍", "", " ", "").Replace(s)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// anyCount token 数：支持 128000 / "128K" / "1.2m" / "128,000"。
func anyCount(v interface{}) int64 {
	if f, ok := v.(float64); ok {
		return int64(f)
	}
	s := strings.NewReplacer(",", "", "_", "", " ", "").Replace(anyString(v))
	if s == "" {
		return 0
	}
	mult := 1.0
	switch strings.ToLower(s[len(s)-1:]) {
	case "k":
		mult, s = 1000, s[:len(s)-1]
	case "m":
		mult, s = 1000000, s[:len(s)-1]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(f * mult)
}

// ---------- 兼容入口（ListModels 里用） ----------

// lookup 按别名组取原始 JSON 值（顶层，保留给 id / apiFormat 这种必需字段）。
func lookup(item map[string]json.RawMessage, group string) json.RawMessage {
	flat := make(map[string]json.RawMessage, len(item))
	for k, v := range item {
		flat[normKey(k)] = v
	}
	for _, alias := range aliasGroups[group] {
		if v, ok := flat[normKey(alias)]; ok && len(v) > 0 && string(v) != "null" {
			return v
		}
	}
	return nil
}

// rawString 取字符串（数字转文本；非法 JSON 的裸文本原样用）。
func rawString(v json.RawMessage) string {
	if len(v) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(v, &s) == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if json.Unmarshal(v, &n) == nil {
		return n.String()
	}
	t := strings.TrimSpace(string(v))
	if t != "" && !strings.HasPrefix(t, "{") && !strings.HasPrefix(t, "[") {
		return t
	}
	return ""
}
