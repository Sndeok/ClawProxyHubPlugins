package main

import "testing"

func cliPayload() *upstreamModelPayload {
	return &upstreamModelPayload{
		Models: []upstreamModel{
			{ID: "deepseek-v4.1-flash", Name: "DeepSeek-V4.1-Flash", MaxInputTokens: 977000, MaxOutputTokens: 31000,
				Credits: "x0.03", Vendor: "DeepSeek", DescriptionZh: "高性价比",
				SupportsToolCall: true, SupportsReasoning: true,
				Reasoning: struct {
					SupportedEfforts []string `json:"supportedEfforts"`
					DefaultEffort    string   `json:"defaultEffort"`
					Summary          string   `json:"summary"`
				}{SupportedEfforts: []string{"low", "high", "max"}, DefaultEffort: "high"}},
			{ID: "glm-5.3", Name: "GLM-5.3", MaxInputTokens: 200000, MaxOutputTokens: 32000, Credits: "1.33", SupportsToolCall: true},
			{ID: "nes-embed", Name: "Embedding", MaxInputTokens: 8000, MaxOutputTokens: 2000},
			{ID: "disabled-model", Name: "Disabled", MaxInputTokens: 1000, MaxOutputTokens: 1000, Disabled: true},
			{ID: "tiny-model", Name: "Tiny", MaxInputTokens: 1000, MaxOutputTokens: 128},
		},
	}
}

func TestParseCredits(t *testing.T) {
	cases := map[string]float64{"x0.05": 0.05, "X0.1": 0.1, "1.33": 1.33, "": 0, "abc": 0}
	for in, want := range cases {
		if got := parseCredits(in); got != want {
			t.Errorf("parseCredits(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSeriesOfUsesVendorThenPrefix(t *testing.T) {
	if got := seriesOf("glm-5.3", "智谱"); got != "智谱" {
		t.Errorf("上游 vendor 优先：got %q", got)
	}
	if got := seriesOf("deepseek-v4.1-flash", ""); got != "DeepSeek" {
		t.Errorf("按前缀推导 DeepSeek：got %q", got)
	}
	if got := seriesOf("mystery-model", ""); got != "其他" {
		t.Errorf("认不出应归「其他」：got %q", got)
	}
}

func TestNonChatModel(t *testing.T) {
	if !nonChatModel(upstreamModel{ID: "nes-embed", MaxOutputTokens: 4096}) {
		t.Error("nes- 前缀应判为非对话")
	}
	if !nonChatModel(upstreamModel{ID: "x", MaxOutputTokens: 128}) {
		t.Error("输出过小应判为非对话")
	}
	if !nonChatModel(upstreamModel{ID: "x", MaxOutputTokens: 4096, Tags: []string{"text-to-image"}}) {
		t.Error("文生图应判为非对话")
	}
	if nonChatModel(upstreamModel{ID: "glm-5.3", MaxOutputTokens: 32000}) {
		t.Error("正常对话模型不应被过滤")
	}
}

func TestMergeCatalogFiltersAndDedupes(t *testing.T) {
	v3 := &upstreamModelPayload{Models: []upstreamModel{
		{ID: "kimi-k2.8-preview", Name: "Kimi-K2.8-Preview", MaxInputTokens: 977000, MaxOutputTokens: 31000,
			Credits: "x0.77", Reasoning: struct {
				SupportedEfforts []string `json:"supportedEfforts"`
				DefaultEffort    string   `json:"defaultEffort"`
				Summary          string   `json:"summary"`
			}{SupportedEfforts: []string{"low", "high", "max"}, DefaultEffort: "high"}},
	}}
	// 企业端点与 v3 重叠 + 独有模型；cli 白名单只留 glm-5.3
	ent := &upstreamModelPayload{
		Models: []upstreamModel{
			{ID: "kimi-k2.8-preview", Name: "Kimi（企业口径）", MaxInputTokens: 1, MaxOutputTokens: 1},
			{ID: "glm-5.3", Name: "GLM-5.3", MaxInputTokens: 200000, MaxOutputTokens: 32000, Credits: "1.33"},
			{ID: "not-in-cli", Name: "NotInCli", MaxInputTokens: 1000, MaxOutputTokens: 4096},
		},
		Agents: []struct {
			Name   string   `json:"name"`
			Models []string `json:"models"`
		}{{Name: "cli", Models: []string{"glm-5.3"}}},
	}
	models, err := mergeCatalog([]catalogSource{{payload: v3}, {payload: ent, cliOnly: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 {
		t.Fatalf("应合并出 2 个模型（v3 独有 + cli 白名单），got %d: %+v", len(models), models)
	}
	if models[0].Id != "kimi-k2.8-preview" || models[0].ContextWindow != 977000 {
		t.Errorf("同 id 应以先到的 /v3 为准：%+v", models[0])
	}
	if len(models[0].ReasoningEfforts) != 3 || models[0].DefaultReasoningEffort != "high" {
		t.Errorf("推理档位未映射：%+v", models[0])
	}
	if models[0].CreditsMultiplier != 0.77 || models[0].Series != "Kimi" {
		t.Errorf("倍率 / 系列未映射：%+v", models[0])
	}
	if models[1].Id != "glm-5.3" || models[1].CreditsMultiplier != 1.33 {
		t.Errorf("cli 白名单模型缺失或字段错误：%+v", models[1])
	}
}
