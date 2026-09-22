package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
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

	free, pass, err := fetchRecommended(ctx, client)
	if err != nil {
		t.Fatalf("推荐清单读取失败: %v", err)
	}
	if len(free) == 0 {
		t.Error("免费模型清单为空")
	}
	t.Logf("免费 %d 条 / 通行证 %d 条，免费示例 %s", len(free), len(pass), free[0].ID)
}
