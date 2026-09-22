package qodersign

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

// NestedSSE 把上游「嵌套 SSE」还原成标准 OpenAI chunk 行。
//
// 上游每帧形态：data:{"headers":{...},"body":"<OpenAI chunk JSON>","statusCodeValue":200}
// body 为字符串（需二次解析）；body == "[DONE]" 表示结束。
// 信封里 statusCodeValue != 200 时说明 HTTP 200 建流后 provider 出错（418/5xx），
// 以 EnvelopeError 返回，调用方据此换号重试。
type EnvelopeError struct {
	StatusCode int
	Detail     string
}

func (e *EnvelopeError) Error() string {
	return "upstream envelope error " + itoa(e.StatusCode) + ": " + e.Detail
}

// EachChunk 顺序读取嵌套 SSE，把每个内层 chunk 交给 onChunk。
// 返回 error 非 nil 时可能是 EnvelopeError（上游错误帧）或 IO 错误。
func EachChunk(r io.Reader, onChunk func(chunk []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		if payload == "[DONE]" {
			return nil
		}
		var env struct {
			Body          json.RawMessage `json:"body"`
			StatusCode    *int            `json:"statusCodeValue"`
			StatusCodeVal *int            `json:"statusCode"`
		}
		if err := json.Unmarshal([]byte(payload), &env); err != nil {
			continue // 非信封帧（心跳等）直接跳过
		}
		status := 200
		if env.StatusCode != nil {
			status = *env.StatusCode
		} else if env.StatusCodeVal != nil {
			status = *env.StatusCodeVal
		}
		if status != 200 {
			return &EnvelopeError{StatusCode: status, Detail: clip(string(payload), 400)}
		}
		body := strings.TrimSpace(string(env.Body))
		if body == "" || body == "null" {
			continue
		}
		// body 可能是 JSON 字符串（需反转义）或直接是对象
		chunk := body
		if strings.HasPrefix(body, "\"") {
			var s string
			if err := json.Unmarshal(env.Body, &s); err == nil {
				chunk = strings.TrimSpace(s)
			}
		}
		if chunk == "" || chunk == "[DONE]" {
			continue
		}
		if err := onChunk([]byte(chunk)); err != nil {
			return err
		}
	}
	return sc.Err()
}

// WrapNested 把嵌套 SSE 还原为「标准 OpenAI SSE 行」的 Reader，
// 直接喂给 sdk/openaiup 的解析器即可复用全部增量/工具调用逻辑。
func WrapNested(r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		err := EachChunk(r, func(chunk []byte) error {
			if _, werr := pw.Write([]byte("data: " + string(chunk) + "\n\n")); werr != nil {
				return werr
			}
			return nil
		})
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		_, _ = pw.Write([]byte("data: [DONE]\n\n"))
		_ = pw.Close()
	}()
	return pr
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
