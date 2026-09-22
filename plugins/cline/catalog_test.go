package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

func withClineUpstream(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)
	oldBase, oldReg, oldRef := clineAPIBase, clineRegisterURL, clineRefreshURL
	clineAPIBase, clineRegisterURL, clineRefreshURL = srv.URL, srv.URL+"/auth/register", srv.URL+"/auth/refresh"
	t.Cleanup(func() {
		clineAPIBase, clineRegisterURL, clineRefreshURL = oldBase, oldReg, oldRef
		srv.Close()
	})
}

// 短名与免费通道别名：官方免费通道按 cline-free/<短名> 取模型。
func TestShortModelNameAndFreeAlias(t *testing.T) {
	cases := map[string]string{
		"moonshotai/kimi-k3":         "kimi-k3",
		"cline-pass/mimo-v2.6-flash": "mimo-v2.6-flash",
		"z-ai/glm-5.3-flash":         "glm-5.3-flash",
		"poolside/laguna-s-2.1:free": "laguna-s-2.1",
		"bare-model":                 "bare-model",
		"":                           "",
	}
	for in, want := range cases {
		if got := shortModelName(in); got != want {
			t.Errorf("shortModelName(%q) = %q，期望 %q", in, got, want)
		}
	}
	if got := freeChannelAlias("moonshotai/kimi-k3"); got != "cline-free/kimi-k3" {
		t.Errorf("freeChannelAlias = %q，期望 cline-free/kimi-k3", got)
	}
}

// 目录汇总：四个通道 + 免费通道别名都要出现在对外模型列表里。
// 回归用户报告的问题：客户端里看得到 kimi-k3，插件侧却没有。
// 目录汇总：Cline 客户端的两个 provider 通道都要出现在对外模型列表里。
// 回归用户报告的问题：客户端里看得到 kimi-k3，插件侧却没有；
// ClinePass 下的清单以账号级 /cline-pass/models 为准。
func TestListModelsExposesAllChannels(t *testing.T) {
	withClineUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case clineFreeListPath:
			fmt.Fprint(w, `{
			  "recommended":[{"id":"moonshotai/kimi-k3","name":"kimi-k3","description":"flagship","tags":["NEW"]}],
			  "free":[{"id":"cline-free/deepseek-v4.1-flash","name":"Deepseek-v4.1-Flash","description":"fast","tags":[]}],
			  "clinePass":[{"id":"cline-pass/kimi-k3","name":"cline-pass/kimi-k3","description":"","tags":[]}],
			  "clineCloud":[{"id":"cline-cloud/kimi-k3","name":"cline-cloud/kimi-k3","description":"","tags":[]}]
			}`)
		case clinePassModelsPath:
			// 账号可用的 ClinePass 清单：客户端 ClinePass provider 里能选到的就这些
			fmt.Fprint(w, `{"data":[
			  {"id":"cline-free/kimi-k3","name":"Kimi K3 (free)","description":"free tier"},
			  {"id":"cline-pass/glm-5.3","name":"GLM-5.3 (ClinePass)","description":"subscription"}
			]}`)
		case clineModelsPath:
			fmt.Fprint(w, `{"data":[{"id":"poolside/laguna-s-2.1:free"}]}`)
		default:
			t.Logf("未预期请求 %s", r.URL.Path)
			fmt.Fprint(w, `{}`)
		}
	})

	p := &plugin{}
	blob := &pb.CredentialBlob{Blob: []byte(`{"refresh_token":"rt-test"}`)}
	ml, err := p.ListModels(context.Background(), blob)
	if err != nil {
		t.Fatalf("ListModels 报错: %v", err)
	}
	byID := map[string]*pb.ModelInfo{}
	for _, m := range ml.Models {
		byID[m.Id] = m
	}
	wants := []string{
		"moonshotai/kimi-k3",             // recommended 原样暴露（Cline Usage-Billing）
		"cline-free/deepseek-v4.1-flash", // 官方 free 组
		"cline-free/kimi-k3",             // ClinePass 账号清单里的免费档
		"cline-pass/glm-5.3",             // ClinePass 账号清单里的订阅款
		"cline-cloud/kimi-k3",            // 云通道（客户端已无该 provider，保留兼容）
		"poolside/laguna-s-2.1:free",     // 公开目录 :free
	}
	for _, id := range wants {
		if byID[id] == nil {
			t.Errorf("模型列表缺少 %s（共 %d 个）", id, len(ml.Models))
		}
	}
	if m := byID["cline-free/kimi-k3"]; m != nil {
		tags := strings.Join(m.Tags, ",")
		if !strings.Contains(tags, "ClinePass") || !strings.Contains(tags, "免费档") {
			t.Errorf("cline-free/kimi-k3 应带 ClinePass/免费档 标签，实际 %v", m.Tags)
		}
		if m.Label["zh"] != "Kimi K3 (free)" {
			t.Errorf("账号清单里的展示名应透传：%+v", m.Label)
		}
	}
	if m := byID["moonshotai/kimi-k3"]; m != nil {
		if m.Description != "flagship" {
			t.Errorf("recommended 描述未透传: %+v", m)
		}
		if !strings.Contains(strings.Join(m.Tags, ","), "Cline Usage-Billing") {
			t.Errorf("recommended 应标为 Cline Usage-Billing，实际 %v", m.Tags)
		}
	}
}

// extra_models 解析：逗号 / 分号 / 换行分隔，去空白与空项。
func TestParseExtraModels(t *testing.T) {
	got := parseExtraModels("cline-free/kimi-k3, cline-free/custom-x;\n cline-pass/glm-5.3 ")
	want := []string{"cline-free/kimi-k3", "cline-free/custom-x", "cline-pass/glm-5.3"}
	if len(got) != len(want) {
		t.Fatalf("解析结果 %v，期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 项 = %q，期望 %q", i, got[i], want[i])
		}
	}
	if parseExtraModels("   ") != nil {
		t.Error("空白输入应返回 nil")
	}
}

// 回归：付费 / 通行证模型不得凭空生成 cline-free/<短名> 别名。
// 实测 cline-free/glm-5.3、cline-free/gpt-6-astra、cline-free/qwen3.8-max、
// cline-free/grok-4.7 等 14 个别名全部返回 404 {"error":"model not found"}；
// 只有 freeLaneVerifiedShorts 里的短名（kimi-k3）实测可用。
func TestListModelsDoesNotInventFreeAliases(t *testing.T) {
	withClineUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case clineFreeListPath:
			fmt.Fprint(w, `{
			  "recommended":[{"id":"openai/gpt-6-astra","name":"gpt-6-astra"},{"id":"moonshotai/kimi-k3","name":"kimi-k3"}],
			  "free":[{"id":"cline-free/deepseek-v4.1-flash","name":"Deepseek-v4.1-Flash"}],
			  "clinePass":[{"id":"cline-pass/qwen3.8-max","name":"cline-pass/qwen3.8-max"}],
			  "clineCloud":[]
			}`)
		case clinePassModelsPath:
			// 账号级清单拿不到（未登录/无资格）→ 走官方 clinePass 组 + 实测可用别名的兜底
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"Unauthorized"}`)
		case clineModelsPath:
			fmt.Fprint(w, `{"data":[]}`)
		}
	})

	p := &plugin{}
	blob := &pb.CredentialBlob{Blob: []byte(`{"refresh_token":"rt-test"}`)}
	ml, err := p.ListModels(context.Background(), blob)
	if err != nil {
		t.Fatalf("ListModels 报错: %v", err)
	}
	byID := map[string]*pb.ModelInfo{}
	for _, m := range ml.Models {
		byID[m.Id] = m
	}
	for _, id := range []string{"cline-free/gpt-6-astra", "cline-free/qwen3.8-max", "cline-free/glm-5.3"} {
		if byID[id] != nil {
			t.Errorf("不该生成免费通道别名 %s（上游 404 model not found）", id)
		}
	}
	for _, id := range []string{"cline-free/kimi-k3", "cline-free/deepseek-v4.1-flash", "openai/gpt-6-astra", "cline-pass/qwen3.8-max"} {
		if byID[id] == nil {
			t.Errorf("模型列表缺少 %s", id)
		}
	}
}
