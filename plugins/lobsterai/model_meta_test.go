package main

import (
	"encoding/json"
	"testing"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// 字段照官方客户端 AvailableServerModel / ServerModelMetadata 的真实形态。
const sampleModelsJSON = `[
  {"modelId":"kimi-k2.7-code","modelName":"Kimi-K2.7-Code","provider":"moonshot","apiFormat":"openai",
   "costMultiplier":0.73,"contextWindow":128000,"maxTokens":31000,
   "supportsImage":true,"supportsThinking":true,"supportsToolCalling":true,
   "thinkingConfig":{"options":[{"level":"high","openclawLevel":"high"},{"level":"max","openclawLevel":"xhigh"}],"defaultLevel":"high"},
   "description":"代码专用","accessible":true},
  {"modelId":"doubao-seed-2.1-pro","modelName":"Doubao-Seed-2.1-Pro","provider":"bytedance","apiFormat":"anthropic",
   "costMultiplier":"x0.68","contextWindow":"256K","maxTokens":"1.2m",
   "supportsThinking":true,"thinkingConfig":{"options":[{"level":"low"},{"level":"medium"},{"level":"high"}],"defaultLevel":"medium"}}
]`

func TestEnrichModelInfoFromOfficialPayload(t *testing.T) {
	var items []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(sampleModelsJSON), &items); err != nil {
		t.Fatal(err)
	}
	first := &pb.ModelInfo{}
	enrichModelInfo(first, items[0])
	if first.Label["en"] != "Kimi-K2.7-Code" {
		t.Errorf("显示名未映射：%+v", first.Label)
	}
	if first.CreditsMultiplier != 0.73 {
		t.Errorf("costMultiplier 未映射：%v", first.CreditsMultiplier)
	}
	if first.ContextWindow != 128000 {
		t.Errorf("contextWindow 未映射：%v", first.ContextWindow)
	}
	if first.MaxOutputTokens != 31000 {
		t.Errorf("maxTokens 未映射：%v", first.MaxOutputTokens)
	}
	if first.Series != "moonshot" {
		t.Errorf("provider 未映射为系列：%q", first.Series)
	}
	if len(first.ReasoningEfforts) != 2 || first.ReasoningEfforts[1] != "max" {
		t.Errorf("thinkingConfig.options 未映射：%v", first.ReasoningEfforts)
	}
	if first.DefaultReasoningEffort != "high" {
		t.Errorf("defaultLevel 未映射：%q", first.DefaultReasoningEffort)
	}
	if len(first.Tags) < 3 {
		t.Errorf("能力标签未推导：%v", first.Tags)
	}

	// 第二组：倍率带 x 前缀、上下文/输出带单位、档位在嵌套里
	second := &pb.ModelInfo{}
	enrichModelInfo(second, items[1])
	if second.CreditsMultiplier != 0.68 {
		t.Errorf("\"x0.68\" 未解析：%v", second.CreditsMultiplier)
	}
	if second.ContextWindow != 256000 {
		t.Errorf("\"256K\" 未解析：%v", second.ContextWindow)
	}
	if second.MaxOutputTokens != 1200000 {
		t.Errorf("\"1.2m\" 未解析：%v", second.MaxOutputTokens)
	}
	if len(second.ReasoningEfforts) != 3 || second.DefaultReasoningEffort != "medium" {
		t.Errorf("嵌套档位未映射：%v default=%q", second.ReasoningEfforts, second.DefaultReasoningEffort)
	}
}

func TestAnyCountAndFloat(t *testing.T) {
	cases := []struct {
		in   interface{}
		want int64
	}{
		{float64(128000), 128000},
		{"128K", 128000},
		{"1.2m", 1200000},
		{"128,000", 128000},
		{"", 0},
	}
	for _, c := range cases {
		if got := anyCount(c.in); got != c.want {
			t.Errorf("anyCount(%v) = %d, want %d", c.in, got, c.want)
		}
	}
	if got := anyFloat("x0.73"); got != 0.73 {
		t.Errorf("anyFloat(x0.73) = %v", got)
	}
	if got := anyFloat(0.1); got != 0.1 {
		t.Errorf("anyFloat(0.1) = %v", got)
	}
}
