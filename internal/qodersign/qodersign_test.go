package qodersign

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// 编码黄金向量：由参考实现（Sliverkiss/qoderwork2api internal/upstream.QoderEncode）
// 实跑输出，逐字节比对——算法改动会让本测试立刻失败。
func TestEncodeGoldenVectors(t *testing.T) {
	cases := []struct{ in, want string }{
		{"hi", "$HzP"},
		{"a", "$p$#"},
		{"hello world", "YuHp$Hq&J(WPHFru"},
		{`{"model":"auto","stream":true}`, "L#Sw*lECYJSFWOg.JHq*GoK^JZKmYKtu(CLuoByB"},
		{"中文 ✓", "Nzf$$)PZBlcQG*tQ"},
	}
	for _, c := range cases {
		got, err := Encode([]byte(c.in))
		if err != nil {
			t.Fatalf("Encode(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("Encode(%q) = %q, want %q（与参考实现不一致）", c.in, got, c.want)
		}
		back, err := Decode(got)
		if err != nil {
			t.Fatalf("Decode(%q): %v", got, err)
		}
		if string(back) != c.in {
			t.Errorf("Decode roundtrip(%q) = %q", c.in, string(back))
		}
	}
}

func TestEncodeEmpty(t *testing.T) {
	got, err := Encode(nil)
	if err != nil || got != "" {
		t.Fatalf("Encode(nil) = %q, %v; want empty", got, err)
	}
}

// 指纹黄金向量：来自参考实现内部交叉校验过的 hub 基线（seed=test-uid-123）。
func TestFingerprintGoldenVectors(t *testing.T) {
	fp := DeriveFingerprint("test-uid-123", "")
	if fp.MachineID != "005b8945c0659064f8d25299980a27b3" {
		t.Errorf("machineID = %s", fp.MachineID)
	}
	if fp.MachineType != "66ea01f7983702b088" {
		t.Errorf("machineType = %s", fp.MachineType)
	}
	if fp.MachineToken != "zzpUYGGMSPEfJVrGQWHj7SBYaRUMwPMK0B4QN_aqKP0" {
		t.Errorf("machineToken = %s", fp.MachineToken)
	}
	if got := SessionID("test-uid-123", ""); got != "e1527624132e0a39ebcb328440517365" {
		t.Errorf("sessionID = %s", got)
	}
}

func TestFingerprintSaltAndIsolation(t *testing.T) {
	plain := DeriveFingerprint("acct-a", "")
	salted := DeriveFingerprint("acct-a", "s1")
	if plain.MachineID == salted.MachineID {
		t.Error("加盐后指纹必须变化")
	}
	if DeriveFingerprint("acct-a", "s1") != salted {
		t.Error("加盐派生必须幂等")
	}
	if DeriveFingerprint("acct-a", "") != plain {
		t.Error("清空盐必须回到基线")
	}
	if DeriveFingerprint("acct-b", "").MachineID == plain.MachineID {
		t.Error("不同账号指纹必须隔离")
	}
	if len(plain.MachineID) != 32 || len(plain.MachineType) != 18 || len(plain.MachineToken) != 43 {
		t.Errorf("指纹长度不符: %d/%d/%d", len(plain.MachineID), len(plain.MachineType), len(plain.MachineToken))
	}
}

// 会话：签名头格式 + 全部必带头。
func TestSessionHeaders(t *testing.T) {
	sess, err := NewSession(Identity{Uid: "u1", UserType: "personal_standard"}, "mid", "mtoken", "mtype")
	if err != nil {
		t.Fatal(err)
	}
	if len(sess.TempKey) != 16 || sess.CosyKey == "" || sess.Info == "" {
		t.Fatalf("会话字段不完整: tempKey=%d cosyKey=%q info=%q", len(sess.TempKey), sess.CosyKey, sess.Info)
	}
	req, _ := http.NewRequest("POST", "https://gateway.qoder.com.cn/algo/api/v2/service/pro/sse/agent_chat_generation?Encode=1", strings.NewReader("body"))
	cfg := HeaderConfig{CosyVersion: "0.1.43", ClientIP: "169.254.198.161", DataPolicy: "AGREE"}
	if err := sess.ApplyHeaders(req, cfg, "body", "u1", "qmodel_preview"); err != nil {
		t.Fatal(err)
	}
	required := []string{
		"authorization", "cosy-key", "cosy-user", "cosy-date", "cosy-machinetype",
		"cosy-machineid", "cosy-machinetoken", "cosy-clienttype", "cosy-version",
		"login-version", "content-type", "accept", "accept-encoding",
	}
	for _, h := range required {
		if req.Header.Get(h) == "" {
			t.Errorf("缺少请求头 %s", h)
		}
	}
	if !strings.HasPrefix(req.Header.Get("authorization"), "Bearer COSY.") {
		t.Errorf("authorization 格式错误: %q", req.Header.Get("authorization"))
	}
	parts := strings.Split(strings.TrimPrefix(req.Header.Get("authorization"), "Bearer COSY."), ".")
	if len(parts) != 2 || len(parts[1]) != 32 {
		t.Errorf("authorization 应为 payload.sig(32 hex)，实际 %v", parts)
	}
	if req.Header.Get("x-model-key") != "qmodel_preview" || req.Header.Get("x-model-source") != "system" {
		t.Errorf("x-model-key/source 未设置: %q/%q", req.Header.Get("x-model-key"), req.Header.Get("x-model-source"))
	}
	if req.Header.Get("cosy-clientip") != "169.254.198.161" {
		t.Errorf("cosy-clientip = %q", req.Header.Get("cosy-clientip"))
	}
}

// 嵌套 SSE：信封解包 + 错误帧识别。
func TestNestedSSEUnwrap(t *testing.T) {
	stream := strings.Join([]string{
		": keep-alive",
		"",
		`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"你\"}}]}","statusCodeValue":200}`,
		"",
		`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"好\"}}]}","statusCodeValue":200}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	var got []string
	err := EachChunk(strings.NewReader(stream), func(chunk []byte) error {
		got = append(got, string(chunk))
		return nil
	})
	if err != nil {
		t.Fatalf("EachChunk: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("解包出 %d 帧，want 2：%v", len(got), got)
	}
	if !strings.Contains(got[0], `"content":"你"`) || !strings.Contains(got[1], `"content":"好"`) {
		t.Errorf("内容解包错误: %v", got)
	}

	bad := `data: {"body":"{\"code\":\"115\"}","statusCodeValue":418}` + "\n"
	err = EachChunk(strings.NewReader(bad), func(chunk []byte) error { return nil })
	ee, ok := err.(*EnvelopeError)
	if !ok {
		t.Fatalf("want *EnvelopeError, got %v", err)
	}
	if ee.StatusCode != 418 {
		t.Errorf("envelope status = %d, want 418", ee.StatusCode)
	}
}

// WrapNested 必须产出可被标准 SSE 解析器消费的行。
func TestWrapNestedProducesStandardSSE(t *testing.T) {
	stream := `data: {"body":"{\"a\":1}","statusCodeValue":200}` + "\n\n"
	out, err := io.ReadAll(WrapNested(strings.NewReader(stream)))
	if err != nil {
		t.Fatalf("WrapNested: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `data: {"a":1}`) {
		t.Errorf("缺少内层帧: %q", s)
	}
	if !strings.Contains(s, "data: [DONE]") {
		t.Errorf("缺少结束帧: %q", s)
	}
}
