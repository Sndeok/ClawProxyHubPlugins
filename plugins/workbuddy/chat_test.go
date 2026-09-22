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

// fakeStream 收集插件发往核心的事件（grpc.ServerStreamingServer[StreamEvent] 的最小实现）。
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
	c := &credential{}
	c.Auth.AccessToken = "tk-test"
	c.Auth.Domain = "www.codebuddy.cn"
	c.Account.UID = "u-test"
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.CredentialBlob{Blob: raw}
}

// withUpstream 把 upstreamBase 指向 mock 上游并记录收到的请求。
func withUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handler))
	old := upstreamBase
	upstreamBase = srv.URL
	t.Cleanup(func() { upstreamBase = old; srv.Close() })
	return srv
}

func runChat(t *testing.T, cred *pb.CredentialBlob, model string) []*pb.StreamEvent {
	t.Helper()
	st := &fakeStream{ctx: context.Background()}
	p := &plugin{}
	req := &pb.ChatRequest{
		Model:      model,
		Messages:   []*pb.EnvelopeMessage{{Role: "user", Text: "hi"}},
		Credential: cred,
	}
	if err := p.Chat(req, st); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}
	return st.events
}

// 状态映射：402 额度 / 429 限流 / 401·403 凭据必须按语义上报，否则核心不会暂停或换号。
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
		{403, "额度不足", 402},
		{403, "forbidden", 401},
		{500, "", 502},
		{200, "余额不足", 402},
		{0, "rate limit exceeded", 429},
		{0, "登录已过期，请重新登录", 401},
	}
	for _, c := range cases {
		if got := mapUpstreamStatus(c.status, c.body); got != c.want {
			t.Errorf("status=%d body=%q → %d，期望 %d", c.status, c.body, got, c.want)
		}
	}
}

// 402：核心据此暂停账号并换号。
func TestChatQuotaExhaustedMapsTo402(t *testing.T) {
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		fmt.Fprint(w, `{"code":402,"msg":"额度不足"}`)
	})
	events := runChat(t, testCredBlob(t), "auto")
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d", len(events))
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("应为 TaskFailed，实际 %T", events[0].Event)
	}
	if fail.TaskFailed.Error.Code != 402 {
		t.Errorf("状态码应为 402，实际 %d", fail.TaskFailed.Error.Code)
	}
	if !strings.Contains(fail.TaskFailed.Detail, "额度不足") {
		t.Errorf("详情应含完整上游返回: %s", fail.TaskFailed.Detail)
	}
}

// 空流：必须以 429 作为**首事件**上报（核心才会换号），且不能先发 MessageStart。
func TestChatEmptyStreamReports429AsFirstEvent(t *testing.T) {
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	events := runChat(t, testCredBlob(t), "auto")
	if len(events) != 1 {
		t.Fatalf("应只发一个失败事件，实际 %d 个", len(events))
	}
	if _, isStart := events[0].Event.(*pb.StreamEvent_MessageStart); isStart {
		t.Fatal("空响应不应先发 MessageStart（那样核心不会换号）")
	}
	fail, ok := events[0].Event.(*pb.StreamEvent_TaskFailed)
	if !ok {
		t.Fatalf("首个事件应为 TaskFailed，实际 %T", events[0].Event)
	}
	if fail.TaskFailed.Error.Code != 429 {
		t.Errorf("空响应应报 429（触发换号），实际 %d", fail.TaskFailed.Error.Code)
	}
}

// 正常流：MessageStart 延迟到首个内容事件；请求头必须是 identity（否则 gzip 缓冲掉打字机效果）。
func TestChatHappyPathAndIdentityEncoding(t *testing.T) {
	var gotEncoding string
	withUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"你好\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"世界\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	events := runChat(t, testCredBlob(t), "auto")
	if gotEncoding != "identity" {
		t.Errorf("流式请求应带 Accept-Encoding: identity，实际 %q", gotEncoding)
	}
	if len(events) == 0 {
		t.Fatal("没有事件")
	}
	if _, ok := events[0].Event.(*pb.StreamEvent_MessageStart); !ok {
		t.Fatalf("首事件应为 MessageStart，实际 %T", events[0].Event)
	}
	var text strings.Builder
	var finished bool
	for _, ev := range events {
		switch e := ev.Event.(type) {
		case *pb.StreamEvent_ContentDelta:
			text.WriteString(e.ContentDelta.Text)
		case *pb.StreamEvent_MessageFinish:
			finished = true
		}
	}
	if text.String() != "你好世界" {
		t.Errorf("正文拼接不符: %q", text.String())
	}
	if !finished {
		t.Error("缺少 MessageFinish")
	}
}
