package qodersign

import (
	"io"
	"strings"
	"testing"
)

// 线上真实帧（用户实例 qoderwork-empty-stream 诊断抓取，2026-09-22）：
// 信封里 statusCode 是字符串 "OK"，statusCodeValue 才是数字 200。
// 旧实现把 statusCode 解到 *int → json.Unmarshal 返回 UnmarshalTypeError → 整帧被
// continue 丢弃 → 上游明明回了 6 段 content，插件判定空流并 429 换号。
const liveFrame = "data:{\"headers\":{\"Content-Type\":[\"application/json\"]},\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"Hello\\\",\\\"role\\\":\\\"assistant\\\"},\\\"index\\\":0}],\\\"created\\\":1790086435,\\\"id\\\":\\\"chatcmpl-1\\\",\\\"model\\\":\\\"auto\\\",\\\"object\\\":\\\"chat.completion.chunk\\\"}\",\"statusCodeValue\":200,\"statusCode\":\"OK\"}"

func TestLiveEnvelopeWithStringStatusCode(t *testing.T) {
	var got []string
	err := EachChunk(strings.NewReader(liveFrame+"\n\n"), func(chunk []byte) error {
		got = append(got, string(chunk))
		return nil
	})
	if err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("解包出 %d 帧，want 1（string statusCode 让整帧被丢弃 = 线上空流）", len(got))
	}
	if !strings.Contains(got[0], "\"content\":\"Hello\"") {
		t.Fatalf("内层 chunk 不对: %s", got[0])
	}
}

func TestLiveEnvelopeThroughWrapNested(t *testing.T) {
	out, err := io.ReadAll(WrapNested(strings.NewReader(liveFrame + "\n\n")))
	if err != nil {
		t.Fatalf("WrapNested: %v", err)
	}
	if !strings.Contains(string(out), "\"content\":\"Hello\"") {
		t.Fatalf("WrapNested 丢帧，输出: %q", string(out))
	}
	if !strings.Contains(string(out), "data: [DONE]") {
		t.Fatalf("缺少结束帧: %q", string(out))
	}
}

// 标准 OpenAI chunk（无 body 字段）必须透传，不能被当成「非信封」丢弃。
func TestPlainChunkPassthrough(t *testing.T) {
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	var got []string
	if err := EachChunk(strings.NewReader(stream), func(c []byte) error {
		got = append(got, string(c))
		return nil
	}); err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 1 || !strings.Contains(got[0], "\"content\":\"hi\"") {
		t.Fatalf("标准 chunk 未透传: %v", got)
	}
}

// statusCode 为字符串数字（"200"）也要正常解包。
func TestEnvelopeStringNumericStatus(t *testing.T) {
	stream := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"x\\\"}}]}\",\"statusCode\":\"200\"}\n\n"
	var got []string
	if err := EachChunk(strings.NewReader(stream), func(c []byte) error {
		got = append(got, string(c))
		return nil
	}); err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 1 || !strings.Contains(got[0], "\"content\":\"x\"") {
		t.Fatalf("字符串数字状态未解包: %v", got)
	}
}

// 非 OK 的字符串状态（如 InternalError）必须报 EnvelopeError，不能静默空流。
func TestEnvelopeStringErrorStatus(t *testing.T) {
	stream := "data: {\"body\":\"{\\\"code\\\":\\\"115\\\"}\",\"statusCodeValue\":200,\"statusCode\":\"InternalError\"}\n\n"
	err := EachChunk(strings.NewReader(stream), func(c []byte) error { return nil })
	ee, ok := err.(*EnvelopeError)
	if !ok {
		t.Fatalf("want *EnvelopeError, got %v", err)
	}
	if ee.StatusCode != 502 || !strings.Contains(ee.Detail, "InternalError") {
		t.Fatalf("EnvelopeError 内容不对: %+v", ee)
	}
}

// body 直接是对象（不做二次转义）也要支持。
func TestEnvelopeObjectBody(t *testing.T) {
	stream := "data: {\"body\":{\"choices\":[{\"delta\":{\"content\":\"obj\"}}]},\"statusCodeValue\":200}\n\n"
	var got []string
	if err := EachChunk(strings.NewReader(stream), func(c []byte) error {
		got = append(got, string(c))
		return nil
	}); err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 1 || !strings.Contains(got[0], "\"content\":\"obj\"") {
		t.Fatalf("对象 body 未解包: %v", got)
	}
}

// body 内是 [DONE]（字符串形态）→ 正常结束，不产生 chunk。
func TestEnvelopeStringDone(t *testing.T) {
	stream := "data: {\"body\":\" [DONE] \",\"statusCodeValue\":200}\n\n"
	var got []string
	if err := EachChunk(strings.NewReader(stream), func(c []byte) error {
		got = append(got, string(c))
		return nil
	}); err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("DONE 不应产出 chunk: %v", got)
	}
}
