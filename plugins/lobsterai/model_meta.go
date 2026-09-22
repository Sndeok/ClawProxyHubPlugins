// model_meta.go — 模型选型元数据：把上游 /api/models/available 的富字段映射到信封。
//
// 桌面端模型选择器会展示「积分倍率 / 思考强度 / 上下文」，这些字段上游接口是带回来的，
// 但各家（以及同一家的不同版本）命名不统一：creditRatio、multiplier、credits… 都出现过。
// 所以这里做**容错映射**：按归一化后的 key 比对一组别名，能取到就填，取不到留空
// （模型中心显示为 -），绝不猜值、不写死数据。
package main

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

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

// aliasGroups 每类元数据的候选字段名（按归一化后比对，前者优先）。
var aliasGroups = map[string][]string{
	"id":            {"modelid", "id", "model", "name", "key"},
	"apiformat":     {"apiformat", "format", "protocol", "dialect"},
	"name":          {"modelname", "displayname", "name", "label", "title", "modelid"},
	"series":        {"series", "category", "family", "vendor", "provider", "brand", "seriesname"},
	"context":       {"contextwindow", "contextlength", "maxinputtokens", "maxcontexttokens", "contextsize", "inputtokenlimit", "context"},
	"maxoutput":     {"maxoutputtokens", "maxtokens", "outputtokenlimit", "maxcompletiontokens", "maxoutputtokenslimit"},
	"multiplier":    {"creditsmultiplier", "creditratio", "creditsratio", "multiplier", "credits", "credit", "priceratio", "costratio", "rate", "factor", "weight", "points"},
	"efforts":       {"reasoningefforts", "supportedefforts", "thinkinglevels", "reasoninglevels", "thoughtlevels", "effortlevels", "efforts", "levels", "thinkingeffort"},
	"defaulteffort": {"defaultreasoningeffort", "defaulteffort", "defaultlevel", "defaultthinking", "defaultthinkinglevel"},
	"tags":          {"tags", "capabilities", "features", "labels", "badges"},
	"description":   {"description", "descriptionZh", "desc", "intro", "remark"},
}

// lookup 按别名组取原始 JSON 值（大小写 / 下划线不敏感）。
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

// enrichModelInfo 用上游条目补全选型元数据（容错，缺字段就跳过）。
func enrichModelInfo(info *pb.ModelInfo, item map[string]json.RawMessage) {
	if v := rawString(lookup(item, "name")); v != "" {
		info.Label = map[string]string{"zh": v, "en": v}
	}
	if v := rawString(lookup(item, "series")); v != "" {
		info.Series = v
	}
	if n := rawCount(lookup(item, "context")); n > 0 {
		info.ContextWindow = int32(n)
	}
	if n := rawCount(lookup(item, "maxoutput")); n > 0 {
		info.MaxOutputTokens = int32(n)
	}
	if f := rawFloat(lookup(item, "multiplier")); f > 0 {
		info.CreditsMultiplier = f
	}
	if list := rawStrings(lookup(item, "efforts")); len(list) > 0 {
		info.ReasoningEfforts = list
	}
	if v := rawString(lookup(item, "defaulteffort")); v != "" {
		info.DefaultReasoningEffort = v
	}
	if list := rawStrings(lookup(item, "tags")); len(list) > 0 {
		info.Tags = list
	}
	if v := rawString(lookup(item, "description")); v != "" {
		info.Description = v
	}
}

// rawString 取字符串（数字也转成文本；对象/数组返回空）。
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
	// 非法 JSON（裸文本，如 128K）：原样当字符串用，上游偶尔不引号
	t := strings.TrimSpace(string(v))
	if t != "" && !strings.HasPrefix(t, "{") && !strings.HasPrefix(t, "[") {
		return t
	}
	return ""
}

// rawFloat 取数值：支持 0.73、"x0.73"、"×0.73"、"0.73x" 等写法；取不到返回 0。
func rawFloat(v json.RawMessage) float64 {
	if len(v) == 0 {
		return 0
	}
	var f float64
	if json.Unmarshal(v, &f) == nil {
		return f
	}
	s := rawString(v)
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

// rawCount 取 token 数：支持数字与 "128K" / "1.2m" / "128,000" 等写法。
func rawCount(v json.RawMessage) int64 {
	if len(v) == 0 {
		return 0
	}
	var n int64
	if json.Unmarshal(v, &n) == nil {
		return n
	}
	s := strings.NewReplacer(",", "", "_", "", " ", "").Replace(rawString(v))
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

// rawStrings 取字符串数组：数组取元素；字符串按 / , 、 拆分；数字转文本。
func rawStrings(v json.RawMessage) []string {
	if len(v) == 0 {
		return nil
	}
	var list []json.RawMessage
	if json.Unmarshal(v, &list) == nil {
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s := rawString(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	s := rawString(v)
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '/' || r == '|' || r == '、' || r == ';'
	}) {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	sort.Strings(out)
	return out
}
