package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// fakeStream 收集插件发往核心的事件。
type fakeStream struct {
	ctx    context.Context
	events []*pb.StreamEvent
}

func (f *fakeStream) Send(ev *pb.StreamEvent) error { f.events = append(f.events, ev); return nil }
func (f *fakeStream) SetHeader(metadata.MD) error   { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)        {}
func (f *fakeStream) Context() context.Context      { return f.ctx }
func (f *fakeStream) SendMsg(any) error             { return nil }
func (f *fakeStream) RecvMsg(any) error             { return nil }

func testCredBlob(t *testing.T) *pb.CredentialBlob {
	t.Helper()
	c := &credential{AccessToken: "tk-test", RefreshToken: "rt-test", ExpiresAt: 4102444800000}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.CredentialBlob{Blob: raw}
}

func withUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handler))
	old := serverBase
	serverBase = srv.URL
	t.Cleanup(func() { serverBase = old; srv.Close() })
}

func runChat(t *testing.T, model string, anthropic bool) []*pb.StreamEvent {
	t.Helper()
	// 直接给方言缓存塞值，跳过模型目录拉取
	anthropicModels.Store(model, anthropic)
	t.Cleanup(func() { anthropicModels.Delete(model) })

	st := &fakeStream{ctx: context.Background()}
	p := &plugin{}
	req := &pb.ChatRequest{
		Model:      model,
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "hi"}},
		Credential: testCredBlob(t),
	}
	if err := p.Chat(req, st); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}
	return st.events
}

// 状态映射：402 额度 / 429 限流 / 401·403 凭据必须按语义上报。
func TestMapUpstreamStatus(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   int32
	}{
		{401, "", 401},
		{402, "", 402},
		{429, "", 429},
		{403, "insufficient credits", 402},
		{403, "forbidden", 401},
		{500, "", 502},
		{0, "余额不足", 402},
		{0, "rate limit exceeded", 429},
	}
	for _, c := range cases {
		if got := mapUpstreamStatus(c.status, c.body); got != c.want {
			t.Errorf("status=%d body=%q → %d，期望 %d", c.status, c.body, got, c.want)
		}
	}
}

// anthropic 方言：必须发 MessageStart（核心据此下发 message_start），否则流不合规。
func TestChatAnthropicSendsMessageStart(t *testing.T) {
	var gotEncoding string
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v1/messages") {
			t.Errorf("anthropic 方言应走 /v1/messages，实际 %s", r.URL.Path)
		}
		gotEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-x\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n")
		fmt.Fprint(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n")
		fmt.Fprint(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n")
		fmt.Fprint(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	})
	events := runChat(t, "claude-x", true)
	if gotEncoding != "identity" {
		t.Errorf("流式应带 Accept-Encoding: identity，实际 %q", gotEncoding)
	}
	if len(events) == 0 {
		t.Fatal("没有事件")
	}
	if _, ok := events[0].Event.(*pb.StreamEvent_MessageStart); !ok {
		t.Fatalf("anthropic 方言首事件必须是 MessageStart，实际 %T", events[0].Event)
	}
	var text strings.Builder
	for _, ev := range events {
		if e, ok := ev.Event.(*pb.StreamEvent_ContentDelta); ok {
			text.WriteString(e.ContentDelta.Text)
		}
	}
	if text.String() != "你好" {
		t.Errorf("正文拼接不符: %q", text.String())
	}
}

// OpenAI 方言：空流必须以 429 作为首事件（核心换号），且不能先发 MessageStart。
func TestChatEmptyStreamReports429AsFirstEvent(t *testing.T) {
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	events := runChat(t, "auto", false)
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d", len(events))
	}
	if _, isStart := events[0].Event.(*pb.StreamEvent_MessageStart); isStart {
		t.Fatal("空响应不应先发 MessageStart")
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("应为 TaskFailed，实际 %T", events[0].Event)
	}
	if fail.TaskFailed.Error.Code != 429 {
		t.Errorf("空响应应报 429，实际 %d", fail.TaskFailed.Error.Code)
	}
}

// 402 额度不足：按语义上报（核心暂停账号）。
func TestChatQuotaMapsTo402(t *testing.T) {
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		fmt.Fprint(w, `{"code":402,"msg":"额度不足"}`)
	})
	events := runChat(t, "auto", false)
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d", len(events))
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("应为 TaskFailed，实际 %T", events[0].Event)
	}
	if fail.TaskFailed.Error.Code != 402 {
		t.Errorf("应为 402，实际 %d", fail.TaskFailed.Error.Code)
	}
	if !strings.Contains(fail.TaskFailed.Detail, "额度不足") {
		t.Errorf("详情应含完整上游返回: %s", fail.TaskFailed.Detail)
	}
}

// 只有思考没有正文（max_tokens 太小、预算被思考吃光）不是空响应：
// 不能报 429（那会让核心暂停账号 10 分钟），思考按 Reasoning 标记下发、正常收尾。
func TestChatReasoningOnlyIsNot429(t *testing.T) {
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Let me think\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\" a bit more\"}}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	events := runChat(t, "auto", false)
	var reasoning, finish int
	for _, ev := range events {
		switch e := ev.Event.(type) {
		case *pb.StreamEvent_TaskFailed:
			t.Fatalf("只有思考不应报失败：code=%d msg=%s", e.TaskFailed.Error.Code, e.TaskFailed.Error.Message)
		case *pb.StreamEvent_ContentDelta:
			if !e.ContentDelta.GetReasoning() {
				t.Errorf("思考增量必须带 Reasoning 标记：%q", e.ContentDelta.Text)
			}
			reasoning++
		case *pb.StreamEvent_MessageFinish:
			finish++
		}
	}
	if reasoning != 2 {
		t.Errorf("思考增量数 = %d, want 2", reasoning)
	}
	if finish != 1 {
		t.Errorf("应以正常 MessageFinish 收尾，实际 %d", finish)
	}
}
