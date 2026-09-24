// chat_grpc.go —— 千问办公 gRPC 对话通道（model.chat.ChatService/ChatCompletionStream）。
//
// 为什么以 gRPC 为主通道：
//   - 真客户端 QwenWorkCN 1.2.1 内置的 qoderclicn 走的就是这条（HTTP/2 + protobuf 帧），
//     它把 SSE 路径（/algo/api/v2/service/pro/sse/agent_chat_generation）只留在 telemetry 里；
//   - 老 SSE 通道在千问办公账号上无论带不带 Encode=1 都回
//     503 {"code":"503","message":"Model catalog unavailable"}（已用 chat_probe 对照实测，
//     与请求体编码无关：发明文才报 decode 错误，发编码体则是这个 503）。
//
// 帧格式：5 字节头（1 字节压缩标志 + 4 字节大端长度）+ protobuf 负载。
// 注意 trailers-only 响应：Go 的 http2 客户端会把 grpc-status 放进 header 而不是 trailer，
// 两个位置都要读，否则错误会被当成「200 + 空流」。
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/Sndeok/ClawProxyHub-Next/sdk/openaiup"
	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
	"github.com/Sndeok/ClawProxyHubPlugins/internal/qodersign"
)

// chatTransport 取对话通道：sse（默认；抓包确认千问办公官方客户端走的就是这条）
// / grpc（gateway.qwenwork.cn 未实现该 service，只有指向 Qoder api2-v2 时才可用）
// / auto（先 gRPC，失败回退 SSE）。
func (p *plugin) chatTransport() string {
	switch strings.ToLower(strings.TrimSpace(p.settingStr("chat_transport", "sse"))) {
	case "sse":
		return "sse"
	case "auto":
		return "auto"
	default:
		return "grpc"
	}
}

// grpcStatusOf 取 grpc-status / grpc-message：trailers-only 响应在 header，正常响应在 trailer。
func grpcStatusOf(resp *http.Response) (string, string) {
	code, msg := resp.Trailer.Get("grpc-status"), resp.Trailer.Get("grpc-message")
	if code == "" {
		code = resp.Header.Get("grpc-status")
	}
	if msg == "" {
		msg = resp.Header.Get("grpc-message")
	}
	return code, msg
}

// grpcCodeToHTTP 把 gRPC 状态码映射成 CPH 错误码（核心据此决定是否换号重试）。
func grpcCodeToHTTP(code string) int32 {
	switch code {
	case "16": // UNAUTHENTICATED
		return 401
	case "7": // PERMISSION_DENIED
		return 403
	case "8": // RESOURCE_EXHAUSTED
		return 429
	case "4", "14": // DEADLINE_EXCEEDED / UNAVAILABLE
		return 502
	default:
		return 502
	}
}

// grpcMessages 把 CPH 信封里的消息压成 gRPC 请求的 role/text 序列。
func grpcMessages(body map[string]interface{}) []grpcMsg {
	raw, _ := body["messages"].([]interface{})
	out := make([]grpcMsg, 0, len(raw))
	for _, it := range raw {
		m, _ := it.(map[string]interface{})
		if m == nil {
			continue
		}
		role := str(m["role"])
		if role == "" {
			continue
		}
		text := contentText(m["content"])
		if text == "" && role != "assistant" {
			continue
		}
		out = append(out, grpcMsg{Role: role, Text: text})
	}
	return out
}

// readGRPCFrames 顺序读 gRPC 帧（压缩帧直接丢弃，请求侧已声明 identity）。
func readGRPCFrames(body io.Reader, onFrame func([]byte) error) error {
	hdr := make([]byte, 5)
	for {
		if _, err := io.ReadFull(body, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		n := binary.BigEndian.Uint32(hdr[1:5])
		if n == 0 {
			continue
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(body, buf); err != nil {
			return err
		}
		if hdr[0] != 0 {
			continue // 压缩帧：不解析
		}
		if err := onFrame(buf); err != nil {
			return err
		}
	}
}

// chatViaGRPC 跑一次 gRPC 对话。
// 返回 (是否已经向核心发过事件, 失败事件)：失败事件非空且未发过事件时，调用方可安全回退别的通道。
func (p *plugin) chatViaGRPC(req *pb.ChatRequest, stream pb.ClawPlugin_ChatServer) (bool, *pb.StreamEvent) {
	ctx := stream.Context()
	cred, err := credFrom(req.GetCredential())
	if err != nil {
		return false, failed(401, orHint(err))
	}
	if err := p.fillFingerprint(cred); err != nil {
		return false, failed(500, err.Error())
	}
	if needRefresh(cred) {
		if err := p.refreshDeviceToken(ctx, p.httpClient(cred), cred); err != nil {
			return false, failed(401, "设备令牌刷新失败，需重新授权: "+err.Error())
		}
	}
	modelKey := req.Model
	if modelKey == "" {
		return false, failed(400, "缺少模型名")
	}

	msgs := grpcMessages(openaiup.ChatBody(req))
	if len(msgs) == 0 {
		msgs = []grpcMsg{{Role: "user", Text: "ping"}}
	}

	sess, err := qodersign.NewSession(qodersign.Identity{
		Name: cred.Nickname, Aid: cred.UID, Uid: cred.UID,
		UserType: defaultUserType, SecurityOauthToken: cred.DT, RefreshToken: cred.DRT,
	}, cred.MachineID, cred.MachineToken, cred.MachineType)
	if err != nil {
		return false, failed(500, err.Error())
	}

	clientType := p.headerCfg().ClientType
	sessionID := qodersign.SessionID(qodersign.SeedFor(cred.UID, cred.DT), p.machineSalt())
	requestID := randomUUID()
	payload := encodeChatRequest(modelKey, msgs, cred.UID, cred.MachineID, clientType, sessionID, requestID)
	framed := grpcFrame(payload)

	// 签名口径：COSY 的 body 是算 protobuf 负载还是含 5 字节帧头的整体，线上两种都可能，
	// 默认按负载（grpc_sign=framed 可切换，配合 grpc_probe 的结果定）。
	signBody := string(payload)
	if strings.EqualFold(strings.TrimSpace(p.settingStr("grpc_sign", "")), "framed") {
		signBody = string(framed)
	}

	rawURL := p.gatewayBaseURL() + p.settingStr("grpc_path", grpcChatStreamPath)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", rawURL, bytes.NewReader(framed))
	if err != nil {
		return false, failed(500, err.Error())
	}
	// gRPC 的模型来自 protobuf 的 model 字段，不下发 x-model-key（真客户端也不发）。
	if err := sess.ApplyHeaders(httpReq, p.headerCfg(), signBody, cred.UID, ""); err != nil {
		return false, failed(500, err.Error())
	}
	httpReq.Header.Set("content-type", "application/grpc+proto")
	httpReq.Header.Set("accept", "application/grpc")
	httpReq.Header.Set("te", "trailers")
	httpReq.Header.Set("grpc-accept-encoding", "identity")
	httpReq.Header.Set("accept-encoding", "identity")

	resp, err := p.grpcHTTPClient(cred).Do(httpReq)
	if err != nil {
		return false, failed(502, "上游连接失败: "+err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		detail := fmt.Sprintf("HTTP %d %s proto=%s\n%s", resp.StatusCode, resp.Status, resp.Proto, string(raw))
		return false, failedDetail(mapUpstreamStatus(resp.StatusCode),
			fmt.Sprintf("gRPC HTTP %d: %s", resp.StatusCode, clip(string(raw), 300)), detail)
	}
	// trailers-only 的失败在 header 里直接给 grpc-status
	if code, msg := grpcStatusOf(resp); code != "" && code != "0" {
		detail := fmt.Sprintf("grpc-status=%s grpc-message=%s proto=%s", code, msg, resp.Proto)
		return false, failedDetail(grpcCodeToHTTP(code), "上游 gRPC 拒绝: "+firstNonEmpty(msg, "status "+code), detail)
	}

	started := false
	ensureStart := func() {
		if started {
			return
		}
		started = true
		_ = stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageStart{
			MessageStart: &pb.MessageStart{Model: modelKey},
		}})
	}
	var contentDeltas, toolDeltas int
	var finishReason string
	var usage *pb.Usage
	var chunkCount int
	var firstChunk string

	readErr := readGRPCFrames(resp.Body, func(frame []byte) error {
		chunk, ok := decodeChatChunk(frame)
		if !ok {
			return nil
		}
		chunkCount++
		if chunkCount == 1 {
			firstChunk = fmt.Sprintf("code=%d model=%s role=%s content=%d字 tool=%d finish=%s",
				chunk.Code, chunk.Model, chunk.Role, len([]rune(chunk.Content)), len(chunk.ToolCalls), chunk.Finish)
		}
		if chunk.Code != 0 && chunk.Code != 200 {
			return fmt.Errorf("上游分片 code=%d", chunk.Code)
		}
		if chunk.Reasoning != "" {
			// 思考增量：契约自 v1.3.2 起带独立标记（ContentDelta.reasoning=true），
			// 由核心按入口协议渲染（reasoning_content / thinking 块 / reasoning item），不再混进正文。
			// 但计数照旧：只有思考没有正文时也属于「上游有响应」，不能当成空响应暂停账号。
			contentDeltas++
			ensureStart()
			if err := stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_ContentDelta{
				ContentDelta: &pb.ContentDelta{Text: chunk.Reasoning, Reasoning: true},
			}}); err != nil {
				return err
			}
		}
		if chunk.Content != "" {
			contentDeltas++
			ensureStart()
			if err := stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_ContentDelta{
				ContentDelta: &pb.ContentDelta{Text: chunk.Content},
			}}); err != nil {
				return err
			}
		}
		for i := range chunk.ToolCalls {
			tc := chunk.ToolCalls[i]
			toolDeltas++
			ensureStart()
			if err := stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_ToolCallDelta{
				ToolCallDelta: &pb.ToolCallDelta{Id: tc.ID, Name: tc.Name, ArgumentsDelta: tc.Arguments},
			}}); err != nil {
				return err
			}
		}
		if chunk.Finish != "" {
			finishReason = chunk.Finish
		}
		if chunk.PromptTokens > 0 || chunk.CompletionTokens > 0 || chunk.TotalTokens > 0 {
			usage = &pb.Usage{InputTokens: chunk.PromptTokens, OutputTokens: chunk.CompletionTokens}
		}
		return nil
	})

	if readErr != nil {
		if code, msg := grpcStatusOf(resp); code != "" && code != "0" {
			detail := fmt.Sprintf("grpc-status=%s grpc-message=%s 首个分片=%s", code, msg, firstChunk)
			if !started {
				return false, failedDetail(grpcCodeToHTTP(code), "上游 gRPC 中断: "+firstNonEmpty(msg, "status "+code), detail)
			}
			return true, failed(grpcCodeToHTTP(code), "上游 gRPC 中断: "+firstNonEmpty(msg, readErr.Error()))
		}
		if !started {
			return false, failedDetail(502, "上游 gRPC 流中断（无有效内容）: "+readErr.Error(),
				fmt.Sprintf("读完 %d 个分片后中断：%v，首个分片=%s", chunkCount, readErr, firstChunk))
		}
		return true, failed(502, "上游 gRPC 流中断: "+readErr.Error())
	}
	if code, msg := grpcStatusOf(resp); code != "" && code != "0" {
		if !started {
			return false, failedDetail(grpcCodeToHTTP(code), "上游 gRPC 拒绝: "+firstNonEmpty(msg, "status "+code),
				fmt.Sprintf("grpc-status=%s grpc-message=%s 分片数=%d 首个分片=%s", code, msg, chunkCount, firstChunk))
		}
		return true, failed(grpcCodeToHTTP(code), "上游 gRPC 中断: "+firstNonEmpty(msg, "status "+code))
	}
	if contentDeltas == 0 && toolDeltas == 0 {
		if !started {
			return false, failedDetail(502, "上游 gRPC 返回空内容",
				fmt.Sprintf("分片数=%d 首个分片=%s grpc-status=%s", chunkCount, firstChunk, firstNonEmpty(mustStatus(resp), "-")))
		}
	}
	ensureStart()
	_ = stream.Send(&pb.StreamEvent{Event: &pb.StreamEvent_MessageFinish{
		MessageFinish: &pb.MessageFinish{FinishReason: firstNonEmpty(finishReason, "stop"), Usage: usage},
	}})
	return true, nil
}

func mustStatus(resp *http.Response) string {
	code, _ := grpcStatusOf(resp)
	return code
}

// ---------- google.protobuf.Struct → JSON（工具调用是 Struct 形状） ----------

// decodeStruct 解 google.protobuf.Struct。
func decodeStruct(b []byte) map[string]interface{} {
	out := map[string]interface{}{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return out
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			entry, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return out
			}
			b = b[m:]
			if k, v := decodeStructEntry(entry); k != "" {
				out[k] = v
			}
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return out
		}
		b = b[m:]
	}
	return out
}

func decodeStructEntry(b []byte) (string, interface{}) {
	var key string
	var val interface{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return key, val
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			s, m := protowire.ConsumeString(b)
			if m < 0 {
				return key, val
			}
			key = s
			b = b[m:]
		case num == 2 && typ == protowire.BytesType:
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return key, val
			}
			val = decodeProtoValue(raw)
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return key, val
			}
			b = b[m:]
		}
	}
	return key, val
}

// decodeProtoValue 解 google.protobuf.Value 的 oneof。
func decodeProtoValue(b []byte) interface{} {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil
		}
		b = b[n:]
		switch num {
		case 1: // null_value
			return nil
		case 2: // number_value
			if typ == protowire.Fixed64Type {
				v, m := protowire.ConsumeFixed64(b)
				if m < 0 {
					return nil
				}
				return math.Float64frombits(v)
			}
		case 3: // string_value
			if typ == protowire.BytesType {
				s, m := protowire.ConsumeString(b)
				if m < 0 {
					return nil
				}
				return s
			}
		case 4: // bool_value
			if typ == protowire.VarintType {
				v, m := protowire.ConsumeVarint(b)
				if m < 0 {
					return nil
				}
				return v != 0
			}
		case 5: // struct_value
			if typ == protowire.BytesType {
				raw, m := protowire.ConsumeBytes(b)
				if m < 0 {
					return nil
				}
				return decodeStruct(raw)
			}
		case 6: // list_value
			if typ == protowire.BytesType {
				raw, m := protowire.ConsumeBytes(b)
				if m < 0 {
					return nil
				}
				return decodeListValue(raw)
			}
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return nil
		}
		b = b[m:]
	}
	return nil
}

func decodeListValue(b []byte) []interface{} {
	out := []interface{}{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return out
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return out
			}
			out = append(out, decodeProtoValue(raw))
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return out
		}
		b = b[m:]
	}
	return out
}

// decodeToolCalls 从 ResponseChatMessage（delta）里取 repeated Struct tool_calls（字段 4）。
func decodeToolCalls(b []byte) []grpcToolCall {
	var out []grpcToolCall
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return out
		}
		b = b[n:]
		if num == 4 && typ == protowire.BytesType {
			raw, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return out
			}
			if tc, ok := toolCallFromStruct(decodeStruct(raw)); ok {
				out = append(out, tc)
			}
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return out
		}
		b = b[m:]
	}
	return out
}

func toolCallFromStruct(m map[string]interface{}) (grpcToolCall, bool) {
	var tc grpcToolCall
	if m == nil {
		return tc, false
	}
	tc.ID, _ = m["id"].(string)
	tc.Type, _ = m["type"].(string)
	if fn, ok := m["function"].(map[string]interface{}); ok {
		tc.Name, _ = fn["name"].(string)
		switch a := fn["arguments"].(type) {
		case string:
			tc.Arguments = a
		case nil:
		default:
			if b, err := json.Marshal(a); err == nil {
				tc.Arguments = string(b)
			}
		}
	}
	if tc.ID == "" && tc.Name == "" {
		return tc, false
	}
	return tc, true
}
