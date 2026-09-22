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
func TestListModelsExposesAllChannels(t *testing.T) {
	withClineUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case clineFreeListPath:
			// 与线上一致的形状：kimi-k3 只出现在 recommended / clinePass / clineCloud
			fmt.Fprint(w, `{
			  "recommended":[{"id":"moonshotai/kimi-k3","name":"kimi-k3","description":"flagship","tags":["NEW"]}],
			  "free":[{"id":"cline-free/deepseek-v4.1-flash","name":"Deepseek-v4.1-Flash","description":"fast","tags":[]}],
			  "clinePass":[{"id":"cline-pass/kimi-k3","name":"cline-pass/kimi-k3","description":"","tags":[]}],
			  "clineCloud":[{"id":"cline-cloud/kimi-k3","name":"cline-cloud/kimi-k3","description":"","tags":[]}]
			}`)
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
		"moonshotai/kimi-k3",             // recommended 原样暴露
		"cline-free/kimi-k3",             // 免费通道别名（用户实测可用）
		"cline-pass/kimi-k3",             // 通行证
		"cline-cloud/kimi-k3",            // 云通道
		"cline-free/deepseek-v4.1-flash", // free 清单
		"poolside/laguna-s-2.1:free",     // 公开目录 :free
	}
	for _, id := range wants {
		if byID[id] == nil {
			t.Errorf("模型列表缺少 %s（共 %d 个）", id, len(ml.Models))
		}
	}
	if m := byID["cline-free/kimi-k3"]; m != nil && !strings.Contains(strings.Join(m.Tags, ","), "免费通道") {
		t.Errorf("cline-free/kimi-k3 应带「免费通道」标签，实际 %v", m.Tags)
	}
	if m := byID["moonshotai/kimi-k3"]; m != nil && m.Description != "flagship" {
		t.Errorf("recommended 描述未透传: %+v", m)
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
