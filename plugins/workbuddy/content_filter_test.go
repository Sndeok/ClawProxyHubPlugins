package main

import "testing"

// 审核拒绝文案识别：必须命中官方原文与标点/空白变体，且不误伤正文。
func TestIsContentFilterText(t *testing.T) {
	filter := []string{
		contentFilterReply,
		contentFilterReply + "。",
		contentFilterReply + "！",
		"抱歉,系统检测到您当前输入的信息存在敏感内容，我无法响应您的请求，请检查后重新输入",
		"  抱歉，系统检测到您当前输入的信息存在敏感内容，我无法响应您的请求，请检查后重新输入  ",
		"抱歉，系统检测到您当前输入的信息存在敏感内容，我无法响应您的请求，请检查后重新输入\n",
		// 前缀变体（上游偶尔在尾部追加提示）
		"抱歉，系统检测到您当前输入的信息存在敏感内容，我无法响应您的请求，请检查后重新输入。如需帮助请联系客服。",
	}
	for _, s := range filter {
		if !isContentFilterText(s) {
			t.Errorf("应识别为审核拒绝: %q", s)
		}
	}

	normal := []string{
		"",
		"   ",
		"好的，我来帮你分析这段敏感内容的处理方式。",
		"抱歉，我不太确定你的意思，能再说一下吗？",
		"系统检测到您当前输入的信息存在敏感内容", // 只有半句，正文里可能自然出现
	}
	for _, s := range normal {
		if isContentFilterText(s) {
			t.Errorf("不应误判为审核拒绝: %q", s)
		}
	}
}
