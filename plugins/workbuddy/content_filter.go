// content_filter.go — 识别上游内容审核的固定拒绝文案。
//
// 上游命中审核时回的是 HTTP 200 + 一段固定拒绝文案（不是错误码），直接透传会让
// 客户端把它当成模型回复。这里归一化比对后把 finish_reason 标成 OpenAI 标准值
// content_filter，并记一条 warn 日志，便于在调用日志里一眼看出「被审核拦了」。
package main

import "strings"

// contentFilterReply 上游审核拒绝文案（与官方客户端所见一致）。
const contentFilterReply = "抱歉，系统检测到您当前输入的信息存在敏感内容，我无法响应您的请求，请检查后重新输入"

// contentFilterPrefixes 兼容上游可能出现的同类前缀变体（只比前缀，避免误伤正文）。
var contentFilterPrefixes = []string{
	"抱歉，系统检测到您当前输入的信息存在敏感内容",
	"抱歉,系统检测到您当前输入的信息存在敏感内容",
}

// normalizeFilterText 归一化：去空白（含全角空格）、半角逗号转全角、去尾部标点。
func normalizeFilterText(s string) string {
	s = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r', '\u3000':
			return -1
		case ',':
			return '，'
		}
		return r
	}, s)
	return strings.TrimRight(s, "。.!！")
}

// isContentFilterText 回复是否就是审核拒绝文案（整体匹配或前缀匹配）。
func isContentFilterText(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	norm := normalizeFilterText(text)
	if norm == contentFilterReply {
		return true
	}
	for _, p := range contentFilterPrefixes {
		if strings.HasPrefix(norm, normalizeFilterText(p)) {
			return true
		}
	}
	return false
}
