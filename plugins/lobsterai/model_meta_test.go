package main

import (
	"encoding/json"
	"testing"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// 上游字段风格照桌面端模型选择器实测：倍率有 "x0.73" / 数字 / creditRatio 等写法，
// 思考强度有数组与 "低/中/高" 字符串两种形态，上下文可能是 128K 这类带单位文本。
const sampleModelsJSON = `[
  {"modelId":"kimi-k2.7-code","modelName":"Kimi-K2.7-Code","apiFormat":"openai",
   "creditRatio":"x0.73","contextWindow":"128K","maxOutputTokens":31000,
   "reasoningEfforts":["高","最大"],"defaultEffort":"高","series":"Kimi",
   "tags":["可读图"],"description":"代码专用"},
  {"modelId":"doubao-seed-2.1-pro","modelName":"Doubao-Seed-2.1-Pro","apiFormat":"anthropic",
   "multiplier":0.68,"context_length":256000,"max_tokens":32000,
   "thinking_levels":"低/中/高","capabilities":["多模态"]}
]`

func TestEnrichModelInfoFromUpstreamPayload(t *testing.T) {
	var items []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(sampleModelsJSON), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("样例应有 2 条，got %d", len(items))
	}

	first := &pb.ModelInfo{}
	enrichModelInfo(first, items[0])
	if first.Label["en"] != "Kimi-K2.7-Code" {
		t.Errorf("显示名未映射：%+v", first.Label)
	}
	if first.CreditsMultiplier != 0.73 {
		t.Errorf("倍率 x0.73 未映射：%v", first.CreditsMultiplier)
	}
	if first.ContextWindow != 128000 {
		t.Errorf("上下文 128K 未映射：%v", first.ContextWindow)
	}
	if first.MaxOutputTokens != 31000 {
		t.Errorf("最大输出未映射：%v", first.MaxOutputTokens)
	}
	if len(first.ReasoningEfforts) != 2 || first.ReasoningEfforts[1] != "最大" {
		t.Errorf("思考档位未映射：%v", first.ReasoningEfforts)
	}
	if first.DefaultReasoningEffort != "高" {
		t.Errorf("默认档位未映射：%q", first.DefaultReasoningEffort)
	}
	if first.Series != "Kimi" {
		t.Errorf("系列未映射：%q", first.Series)
	}
	if len(first.Tags) == 0 || first.Description == "" {
		t.Errorf("标签/说明未映射：tags=%v desc=%q", first.Tags, first.Description)
	}

	// 第二组：数字倍率 + 下划线 key + 字符串档位
	second := &pb.ModelInfo{}
	enrichModelInfo(second, items[1])
	if second.CreditsMultiplier != 0.68 {
		t.Errorf("数字倍率未映射：%v", second.CreditsMultiplier)
	}
	if second.ContextWindow != 256000 {
		t.Errorf("context_length 未映射：%v", second.ContextWindow)
	}
	if second.MaxOutputTokens != 32000 {
		t.Errorf("max_tokens 未映射：%v", second.MaxOutputTokens)
	}
	if len(second.ReasoningEfforts) != 3 {
		t.Errorf("字符串档位未拆分：%v", second.ReasoningEfforts)
	}
	if len(second.Tags) == 0 {
		t.Errorf("capabilities 未当标签：%v", second.Tags)
	}
}

func TestRawCountAndFloat(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"128000", 128000},
		{"128K", 128000},
		{"1.2m", 1200000},
		{"128,000", 128000},
		{"", 0},
	}
	for _, c := range cases {
		if got := rawCount(json.RawMessage(c.in)); got != c.want {
			t.Errorf("rawCount(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	if got := rawFloat(json.RawMessage(`"x0.73"`)); got != 0.73 {
		t.Errorf("rawFloat(x0.73) = %v", got)
	}
	if got := rawFloat(json.RawMessage(`0.1`)); got != 0.1 {
		t.Errorf("rawFloat(0.1) = %v", got)
	}
}
