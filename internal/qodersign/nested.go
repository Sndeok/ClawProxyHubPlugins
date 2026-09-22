package qodersign

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// NestedSSE 把上游「嵌套 SSE」还原成标准 OpenAI chunk 行。
//
// 上游帧有三种形态，都必须兼容：
//  1. 信封：data:{"headers":{...},"body":"<OpenAI chunk JSON>","statusCodeValue":200,"statusCode":"OK"}
//     body 是字符串，需要二次反转义；statusCode 线上是字符串 "OK"（不是数字！）。
//  2. 信封的 body 直接是对象（部分节点不做二次转义）。
//  3. 标准 OpenAI chunk：data:{"choices":[...]}（无 body 字段，直接透传）。
//
// body == "[DONE]" 表示结束；信封 status 非 200 说明 HTTP 200 建流后 provider 出错
// （418/5xx），以 EnvelopeError 返回，调用方据此换号重试。
type EnvelopeError struct {
	StatusCode int
	Detail     string
}

func (e *EnvelopeError) Error() string {
	return "upstream envelope error " + strconv.Itoa(e.StatusCode) + ": " + e.Detail
}

// errStreamDone 上游明确发了结束帧（[DONE]），用于中断后续读取。
var errStreamDone = errors.New("qodersign: upstream stream done")

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
		if err := handleFrame([]byte(payload), 0, onChunk); err != nil {
			if errors.Is(err, errStreamDone) {
				return nil
			}
			return err
		}
	}
	return sc.Err()
}

// handleFrame 处理单帧 payload：信封解包，或标准 chunk 透传。
// depth 限制「body 里还是 SSE 文本」的递归层数。
func handleFrame(payload []byte, depth int, onChunk func(chunk []byte) error) error {
	if depth > 3 {
		return nil
	}
	var env struct {
		Body       json.RawMessage `json:"body"`
		StatusCode json.RawMessage `json:"statusCode"`
		StatusVal  json.RawMessage `json:"statusCodeValue"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		// 非 JSON 心跳帧；数组帧可能是批量 chunk，直接透传
		return emitChunk(payload, onChunk)
	}
	// 没有 body 字段 = 不是信封：标准 OpenAI chunk 直接透传（旧实现会整帧丢弃）
	if len(env.Body) == 0 {
		return emitChunk(payload, onChunk)
	}
	if status, known, text := envelopeStatus(env.StatusVal, env.StatusCode); known && status != 200 {
		detail := clip(string(payload), 400)
		if text != "" {
			detail = text + " | " + detail
		}
		return &EnvelopeError{StatusCode: status, Detail: detail}
	}
	body := bytes.TrimSpace(env.Body)
	if len(body) == 0 || bytes.Equal(body, []byte("null")) {
		return nil
	}
	// body 可能是 JSON 字符串（需反转义），也可能直接是对象
	if body[0] == '"' {
		var s string
		if err := json.Unmarshal(env.Body, &s); err == nil {
			body = []byte(strings.TrimSpace(s))
		}
	}
	if len(body) == 0 {
		return nil
	}
	if bytes.Equal(body, []byte("[DONE]")) {
		return errStreamDone
	}
	// body 里可能又是一段 SSE 文本（"data: {...}\n"）
	if bytes.Contains(body, []byte("data:")) {
		for _, raw := range strings.Split(string(body), "\n") {
			ln := strings.TrimSpace(raw)
			if ln == "" || strings.HasPrefix(ln, ":") || strings.HasPrefix(ln, "event:") || !strings.HasPrefix(ln, "data:") {
				continue
			}
			p := strings.TrimSpace(strings.TrimPrefix(ln, "data:"))
			if p == "" {
				continue
			}
			if p == "[DONE]" {
				return errStreamDone
			}
			if err := handleFrame([]byte(p), depth+1, onChunk); err != nil {
				return err
			}
		}
		return nil
	}
	return emitChunk(body, onChunk)
}

// emitChunk 只把合法 JSON 对象/数组透传给调用方，其余（纯文本错误页等）跳过。
func emitChunk(b []byte, onChunk func(chunk []byte) error) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || (b[0] != '{' && b[0] != '[') {
		return nil
	}
	return onChunk(b)
}

// envelopeStatus 解析信封状态字段，兼容三种线上形态：
// 数字 200 / 字符串 "200" / 字符串 "OK"（Qoder 线上真实返回）。
// 只要任一状态字段明确表示非 200，就按错误返回（宁可报错详情，也不要静默空流）。
// 返回 (状态码, 是否识别到状态字段, 状态字段原文)。
func envelopeStatus(values ...json.RawMessage) (int, bool, string) {
	seenOK := false
	errCode, errText := 0, ""
	for _, raw := range values {
		s := strings.TrimSpace(string(raw))
		if s == "" || s == "null" {
			continue
		}
		var n float64
		if err := json.Unmarshal(raw, &n); err == nil {
			if int(n) == 200 {
				seenOK = true
			} else if errCode == 0 {
				errCode = int(n)
			}
			continue
		}
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			str = strings.TrimSpace(str)
			switch {
			case str == "" || isOKStatusString(str):
				seenOK = true
			case isDigitString(str):
				if n, convErr := strconv.Atoi(str); convErr == nil {
					if n == 200 {
						seenOK = true
					} else if errCode == 0 {
						errCode = n
					}
				}
			default:
				// 非 OK 的字符串状态（如 "InternalError"）：按上游错误处理
				if errCode == 0 {
					errCode = 502
					errText = "status=" + str
				}
			}
		}
	}
	if errCode != 0 {
		return errCode, true, errText
	}
	if seenOK {
		return 200, true, ""
	}
	return 0, false, ""
}

func isOKStatusString(s string) bool {
	switch strings.ToLower(s) {
	case "ok", "success", "succeed", "successful", "fine", "done", "200 ok":
		return true
	}
	return false
}

func isDigitString(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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
