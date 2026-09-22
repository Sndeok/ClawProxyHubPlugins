package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// 线上联调测试：默认跳过，用 CPH_CLINE_LIVE=1 开启。
// 不依赖任何账号：只验证「设备授权链接能申请到」「轮询通道可达（未授权返回 pending）」
// 「公开模型目录可读」——这三条是插件能否工作的前提。
func requireLive(t *testing.T) {
	t.Helper()
	if os.Getenv("CPH_CLINE_LIVE") != "1" {
		t.Skip("设置 CPH_CLINE_LIVE=1 才跑线上联调")
	}
}

func TestLiveDeviceAuthStart(t *testing.T) {
	requireLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sess, err := startDeviceAuth(ctx, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("申请设备码失败: %v", err)
	}
	if sess.DeviceCode == "" || sess.AuthURL == "" {
		t.Fatalf("设备码/授权链接为空: %+v", sess)
	}
	if !strings.Contains(sess.AuthURL, "http") {
		t.Errorf("授权链接异常: %s", sess.AuthURL)
	}
	if sess.Interval < 5 {
		t.Errorf("轮询间隔应 ≥5s，实际 %d", sess.Interval)
	}
	t.Logf("授权链接=%s 设备码=%s 间隔=%ds", sess.AuthURL, sess.UserCode, sess.Interval)

	// 未授权时轮询应返回 pending（而不是报错）
	_, pending, err := pollWorkOS(ctx, &http.Client{Timeout: 30 * time.Second}, sess)
	if err != nil {
		t.Fatalf("轮询出错（应返回 pending）: %v", err)
	}
	if !pending {
		t.Error("尚未授权时应当 pending")
	}
}

func TestLivePublicCatalogs(t *testing.T) {
	requireLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}

	models, err := fetchModels(ctx, client)
	if err != nil {
		t.Fatalf("公开模型目录读取失败: %v", err)
	}
	if len(models) < 10 {
		t.Errorf("模型目录条数异常: %d", len(models))
	}
	t.Logf("公开模型目录 %d 条，例如 %s", len(models), models[0].ID)

	cat, err := fetchRecommended(ctx, client)
	if err != nil {
		t.Fatalf("推荐清单读取失败: %v", err)
	}
	if len(cat.Free) == 0 {
		t.Error("免费模型清单为空")
	}
	t.Logf("推荐 %d / 免费 %d / 通行证 %d / 云通道 %d；免费示例 %s",
		len(cat.Recommended), len(cat.Free), len(cat.ClinePass), len(cat.ClineCloud), cat.Free[0].ID)
}

// 真实目录回归：用公开推荐接口跑一遍 ListModels，确认客户端可见的模型
// （含 cline-free/<短名> 免费通道别名）都在插件输出里。不需要真实凭据。
func TestLiveListModelsRealCatalog(t *testing.T) {
	requireLive(t)
	p := &plugin{}
	blob := &pb.CredentialBlob{Blob: []byte(`{"refresh_token":"dummy-for-public-catalog"}`)}
	ml, err := p.ListModels(context.Background(), blob)
	if err != nil {
		t.Fatalf("ListModels 失败: %v", err)
	}
	ids := map[string]bool{}
	var freeAliases []string
	for _, m := range ml.Models {
		ids[m.Id] = true
		if strings.HasPrefix(m.Id, "cline-free/") {
			freeAliases = append(freeAliases, m.Id)
		}
	}
	t.Logf("目录共 %d 个模型；cline-free/* 别名 %d 个", len(ml.Models), len(freeAliases))
	if len(freeAliases) > 0 {
		t.Logf("免费通道别名示例: %s", strings.Join(freeAliases[:min(5, len(freeAliases))], ", "))
	}
	// 上游推荐清单里 kimi-k3 出现在 recommended / clinePass / clineCloud ——
	// 免费通道别名必须据此生成（用户实测 cline-free/kimi-k3 可用）
	if !ids["cline-free/kimi-k3"] {
		t.Errorf("缺少 cline-free/kimi-k3（免费通道别名）")
	}
	if !ids["cline-pass/kimi-k3"] && !ids["moonshotai/kimi-k3"] && !ids["cline-cloud/kimi-k3"] {
		t.Errorf("缺少 kimi-k3 的任一通道条目")
	}
}
