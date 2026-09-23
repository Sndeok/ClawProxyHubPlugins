package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
	"google.golang.org/protobuf/encoding/protowire"
)

// gRPC 通道：千问办公 / Qoder 新 CLI 的 model-transport 走
// POST /model.chat.ChatService/ChatCompletionStream（proto: model.chat，
// 见官方 SDK 的 dist/_worker/proto/chat.proto）。相对 /algo 那条 SSE 老通道，
// 它是有正式 proto 契约的一等公民——CLI 里 QODER_MODEL_TRANSPORT 默认选 gRPC，
// 只在代理不支持时才降级到 HTTP。
//
// 这里不引 protoc 代码生成：请求/响应都用 protowire 手写编解码，字段号与官方 proto 对齐。
const (
	grpcChatStreamPath = "/model.chat.ChatService/ChatCompletionStream"
)

// ---------- HTTP/2 客户端（gRPC 必须 h2，不能复用抓 SSE 那个强制 h1 的客户端） ----------

var grpcClientCache sync.Map

func (p *plugin) grpcHTTPClient(cred *accountCred) *http.Client {
	key := ""
	if cred != nil {
		key = cred.proxyURL
	}
	if c, ok := grpcClientCache.Load(key); ok {
		return c.(*http.Client)
	}
	tr := &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true, // 关键：默认开 h2
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if key != "" {
		if u, err := url.Parse(key); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr.Proxy = http.ProxyFromEnvironment
	}
	c := &http.Client{Transport: tr, Timeout: 180 * time.Second}
	grpcClientCache.Store(key, c)
	return c
}

// ---------- protobuf 手写编码（ChatCompletionRequest 子集） ----------

type grpcMsg struct{ Role, Text string }

// encodeChatRequest 按 model.chat.ChatCompletionRequest 的字段号编码最小请求。
func encodeChatRequest(model string, msgs []grpcMsg, uid, machineID, clientType, sessionID, requestID string) []byte {
	var out []byte

	out = protowire.AppendTag(out, 1, protowire.BytesType) // model
	out = protowire.AppendString(out, model)
	for _, m := range msgs {
		var cm []byte
		cm = protowire.AppendTag(cm, 2, protowire.BytesType) // ChatMessage.role
		cm = protowire.AppendString(cm, m.Role)
		cm = protowire.AppendTag(cm, 3, protowire.BytesType) // ChatMessage.text_content
		cm = protowire.AppendString(cm, m.Text)
		out = protowire.AppendTag(out, 2, protowire.BytesType) // messages
		out = protowire.AppendBytes(out, cm)
	}
	out = protowire.AppendTag(out, 6, protowire.VarintType) // stream
	out = protowire.AppendVarint(out, 1)

	// metadata.context（字段 3）——CLI 会带机器码 / 客户端类型 / 会话标识
	var ctxMeta []byte
	ctxMeta = protowire.AppendTag(ctxMeta, 1, protowire.BytesType)
	ctxMeta = protowire.AppendString(ctxMeta, requestID)
	ctxMeta = protowire.AppendTag(ctxMeta, 2, protowire.BytesType)
	ctxMeta = protowire.AppendString(ctxMeta, sessionID)
	ctxMeta = protowire.AppendTag(ctxMeta, 5, protowire.BytesType)
	ctxMeta = protowire.AppendString(ctxMeta, machineID)
	ctxMeta = protowire.AppendTag(ctxMeta, 6, protowire.BytesType)
	ctxMeta = protowire.AppendString(ctxMeta, clientType)

	var meta []byte
	meta = protowire.AppendTag(meta, 3, protowire.BytesType)
	meta = protowire.AppendBytes(meta, ctxMeta)
	if uid != "" {
		var um []byte
		um = protowire.AppendTag(um, 1, protowire.BytesType)
		um = protowire.AppendString(um, uid)
		meta = protowire.AppendTag(meta, 4, protowire.BytesType) // metadata.user
		meta = protowire.AppendBytes(meta, um)
	}
	out = protowire.AppendTag(out, 22, protowire.BytesType)
	out = protowire.AppendBytes(out, meta)

	return out
}

// grpcFrame 加 gRPC 消息帧头：1 字节压缩标志 + 4 字节大端长度。
func grpcFrame(payload []byte) []byte {
	buf := make([]byte, 5+len(payload))
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

// grpcChunk 解析出的一个流式分片（只取关心的字段）。
type grpcChunk struct {
	Code      int64
	ID        string
	Model     string
	Role      string
	Content   string
	Reasoning string
	Finish    string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
}

// decodeChatChunk 解析 model.chat.ChatCompletionChunk 的一个分片。
func decodeChatChunk(b []byte) (grpcChunk, bool) {
	var c grpcChunk
	var delta []byte
	sawField := false
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return c, false
		}
		b = b[n:]
		switch num {
		case 1: // code
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return c, false
			}
			c.Code = int64(v)
			b = b[m:]
			sawField = true
		case 2, 3, 5: // id / object / model
			s, m := protowire.ConsumeString(b)
			if m < 0 {
				return c, false
			}
			if num == 2 {
				c.ID = s
			} else if num == 5 {
				c.Model = s
			}
			b = b[m:]
			sawField = true
		case 6: // choices
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return c, false
			}
			delta = append(delta, raw...)
			b = b[m:]
			sawField = true
		case 7: // usage
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return c, false
			}
			c.PromptTokens, c.CompletionTokens, c.TotalTokens = decodeUsage(raw)
			b = b[m:]
			sawField = true
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return c, false
			}
			b = b[m:]
		}
	}
	if delta != nil {
		c.Role, c.Content, c.Reasoning, c.Finish = decodeStreamChoice(delta)
	}
	return c, sawField
}

func decodeStreamChoice(b []byte) (role, content, reasoning, finish string) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		switch num {
		case 2: // delta
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return
			}
			role, content, reasoning = decodeResponseMessage(raw)
			b = b[m:]
		case 3: // finish_reason
			s, m := protowire.ConsumeString(b)
			if m < 0 {
				return
			}
			finish = s
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return
			}
			b = b[m:]
		}
	}
	return
}

func decodeResponseMessage(b []byte) (role, content, reasoning string) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if num >= 1 && num <= 3 {
			s, m := protowire.ConsumeString(b)
			if m < 0 {
				return
			}
			switch num {
			case 1:
				role = s
			case 2:
				content = s
			case 3:
				reasoning = s
			}
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return
		}
		b = b[m:]
	}
	return
}

func decodeUsage(b []byte) (prompt, completion, total int64) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if typ == protowire.VarintType && num >= 1 && num <= 3 {
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return
			}
			switch num {
			case 1:
				prompt = int64(v)
			case 2:
				completion = int64(v)
			case 3:
				total = int64(v)
			}
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return
		}
		b = b[m:]
	}
	return
}

// ---------- 探针 ----------

// grpcChatProbe 用真账号令牌打一次 model.chat.ChatService，返回可直接贴日志的诊断串。
//
// 签名口径有歧义（COSY 的 body 算 protobuf 负载还是含 5 字节帧头的整体），两种都试，
// 一次就能定下来，不用猜。
func (p *plugin) grpcChatProbe(ctx context.Context, cred *accountCred, model string) string {
	if err := p.fillFingerprint(cred); err != nil {
		return "探针失败(指纹): " + err.Error()
	}
	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return "探针失败(COSY): " + err.Error()
	}
	clientType := p.headerCfg().ClientType
	sessionID := qodersign.SessionID(qodersign.SeedFor(cred.UID, cred.DT), p.machineSalt())
	requestID := randomUUID()

	payload := encodeChatRequest(model,
		[]grpcMsg{{Role: "user", Text: "只回答两个字符：OK"}},
		cred.UID, cred.MachineID, clientType, sessionID, requestID)
	framed := grpcFrame(payload)

	type attempt struct {
		label string
		body  []byte
		sign  string
	}
	attempts := []attempt{
		{"sign=payload", framed, string(payload)},
		{"sign=framed", framed, string(framed)},
	}
	var out []string
	for _, a := range attempts {
		out = append(out, a.label+" → "+p.grpcChatOnce(ctx, cred, sess, a.body, a.sign, model))
	}
	return strings.Join(out, " ｜ ")
}

// grpcChatOnce 发一次请求，返回 HTTP 状态 + trailer grpc-status + 首个分片摘要。
func (p *plugin) grpcChatOnce(ctx context.Context, cred *accountCred, sess *qodersign.Session, body []byte, signBody, model string) string {
	rawURL := p.gatewayBaseURL() + grpcChatStreamPath
	req, err := http.NewRequestWithContext(ctx, "POST", rawURL, bytes.NewReader(body))
	if err != nil {
		return "建请求失败: " + err.Error()
	}
	if err := sess.ApplyHeaders(req, p.headerCfg(), signBody, cred.UID, model); err != nil {
		return "签名失败: " + err.Error()
	}
	// 覆盖成 gRPC 的线上形态（ApplyHeaders 设的是 SSE 那套）
	req.Header.Set("content-type", "application/grpc+proto")
	req.Header.Set("accept", "application/grpc")
	req.Header.Set("te", "trailers")
	req.Header.Set("grpc-accept-encoding", "identity")
	req.Header.Set("accept-encoding", "identity")

	resp, err := p.grpcHTTPClient(cred).Do(req)
	if err != nil {
		return "请求失败: " + err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	status := fmt.Sprintf("HTTP %d proto=%s len=%d", resp.StatusCode, resp.Proto, len(raw))
	if gt := resp.Trailer.Get("grpc-status"); gt != "" {
		status += " grpc-status=" + gt
	}
	if gm := resp.Trailer.Get("grpc-message"); gm != "" {
		status += " grpc-message=" + gm
	}
	if len(raw) >= 5 {
		if n := int(binary.BigEndian.Uint32(raw[1:5])); n > 0 && 5+n <= len(raw) {
			if chunk, ok := decodeChatChunk(raw[5 : 5+n]); ok {
				status += fmt.Sprintf(" chunk{code=%d model=%s role=%s content=%q reasoning=%d字 finish=%q usage=%d/%d/%d}",
					chunk.Code, chunk.Model, chunk.Role, chunk.Content, len([]rune(chunk.Reasoning)),
					chunk.Finish, chunk.PromptTokens, chunk.CompletionTokens, chunk.TotalTokens)
				return status
			}
		}
	}
	if len(raw) > 0 {
		status += " body=" + clip(strings.TrimSpace(string(raw)), 200)
	}
	return status
}