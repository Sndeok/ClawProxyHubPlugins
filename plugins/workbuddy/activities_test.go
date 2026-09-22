package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// newTestCred 最小可用凭据（mock 上游不校验鉴权，只要求结构非空）。
func newTestCred() *credential {
	c := &credential{}
	c.Auth.AccessToken = "test-token"
	c.Auth.Domain = "www.codebuddy.cn"
	c.Account.UID = "u-test"
	return c
}

// withMockUpstream 临时把 upstreamBase 指向 mock 服务。
func withMockUpstream(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	old := upstreamBase
	upstreamBase = srv.URL
	t.Cleanup(func() {
		upstreamBase = old
		srv.Close()
	})
	return srv
}

// 新手门槛状态机：not_accepted → 接取 → 真实对话 → completed。
// 关键点：first_buddy 必须发「真实对话」（带 extra_vars.growthEvent），只发 /v2/report 埋点不算。
func TestEnsureFirstBuddyDone(t *testing.T) {
	var mu sync.Mutex
	status := "not_accepted"
	var accepted, chatted, reported bool
	var chatBody map[string]interface{}

	withMockUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == actTasksList:
			task := map[string]interface{}{
				"task_code": "first_buddy", "task_type": "single",
				"accept_status": status, "locked": false,
				"progress": map[string]interface{}{"current": 0, "target": 1},
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "data": map[string]interface{}{"tasks": []interface{}{task}},
			})
		case r.URL.Path == actTasksAccept:
			accepted = true
			status = "accepted"
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		case r.URL.Path == pathChat:
			chatted = true
			_ = json.NewDecoder(r.Body).Decode(&chatBody)
			status = "completed"
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		case r.URL.Path == reportPath:
			reported = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		default:
			t.Logf("未预期请求 %s %s", r.Method, r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		}
	})

	p := &plugin{}
	ok, err := p.ensureFirstBuddyDone(context.Background(), newTestCred())
	if err != nil {
		t.Fatalf("ensureFirstBuddyDone 报错: %v", err)
	}
	if !ok {
		t.Fatal("门槛应判定为已过（任务已 completed）")
	}
	if !accepted {
		t.Error("未走接取分支（not_accepted 应先 accept）")
	}
	if !chatted {
		t.Error("未发真实对话")
	}
	if reported {
		t.Error("真实对话成功时不应再走 /v2/report 兜底")
	}
	// 对话体必须带内嵌埋点与低上限，否则官方不认这是新手对话
	if _, has := chatBody["extra_vars"]; !has {
		t.Errorf("对话体缺少 extra_vars.growthEvent: %v", chatBody)
	}
	if mt, _ := chatBody["max_tokens"].(float64); mt != 32 {
		t.Errorf("max_tokens 应为 32（最小成本），实际 %v", chatBody["max_tokens"])
	}
}

// 任务已 completed：不应接取、不应发对话。
func TestEnsureFirstBuddyDoneAlreadyCompleted(t *testing.T) {
	var accepted, chatted bool
	withMockUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == actTasksList:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "data": map[string]interface{}{"tasks": []interface{}{map[string]interface{}{
					"task_code": "first_buddy", "task_type": "single",
					"accept_status": "completed", "locked": false,
					"progress": map[string]interface{}{"current": 1, "target": 1},
				}}},
			})
		case r.URL.Path == actTasksAccept:
			accepted = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		case r.URL.Path == pathChat:
			chatted = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		}
	})

	p := &plugin{}
	ok, err := p.ensureFirstBuddyDone(context.Background(), newTestCred())
	if err != nil {
		t.Fatalf("报错: %v", err)
	}
	if !ok {
		t.Error("已完成应返回 true")
	}
	if accepted || chatted {
		t.Errorf("已完成不应再操作（accepted=%v chatted=%v）", accepted, chatted)
	}
}

// 账号没有 first_buddy 任务：视为门槛与它无关（返回 true）。
func TestEnsureFirstBuddyDoneNoTask(t *testing.T) {
	withMockUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"code": 0, "data": map[string]interface{}{"tasks": []interface{}{}},
		})
	})
	p := &plugin{}
	ok, err := p.ensureFirstBuddyDone(context.Background(), newTestCred())
	if err != nil || !ok {
		t.Fatalf("无任务应返回 (true, nil)，实际 (%v, %v)", ok, err)
	}
}

// 旅行出发：领养被门槛挡住时自动补新手任务，然后重试成功。
func TestDepartTravelRecoversFromFirstBuddyGate(t *testing.T) {
	var mu sync.Mutex
	status := "accepted" // 已接取、未完成
	departed := false
	chats := 0 // 新手真实对话次数

	withMockUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == actTasksList:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "data": map[string]interface{}{"tasks": []interface{}{map[string]interface{}{
					"task_code": "first_buddy", "task_type": "single",
					"accept_status": status, "locked": false,
					"progress": map[string]interface{}{"current": 0, "target": 1},
				}}},
			})
		case r.URL.Path == actBuddyAgree:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		case r.Method == http.MethodGet && r.URL.Path == actBuddyInfo:
			// 还没有 Buddy（id=0）→ ensureBuddy 会继续走领养
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "data": map[string]interface{}{
				"buddy": map[string]interface{}{"id": 0},
			}})
		case r.URL.Path == actBuddyFirst: // 领养：未过门槛时报错
			if status != "completed" {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 400, "msg": "first_buddy task not completed"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "data": map[string]interface{}{"buddy_id": 7}})
		case r.URL.Path == pathChat: // 新手真实对话 → 门槛达成
			chats++
			status = "completed"
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		case r.URL.Path == actTravelCfg:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "data": map[string]interface{}{"locations": []interface{}{
				map[string]interface{}{"id": 3},
			}}})
		case r.URL.Path == actTravelGo:
			departed = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		default:
			t.Logf("未预期请求 %s %s", r.Method, r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		}
	})

	p := &plugin{}
	summary, err := p.departTravel(context.Background(), newTestCred(), 0)
	if err != nil {
		t.Fatalf("departTravel 报错: %v", err)
	}
	if !departed {
		t.Fatalf("未能派出旅行，summary=%q", summary)
	}
	if summary != "已派出 Buddy 旅行" {
		t.Errorf("summary 不符: %s", summary)
	}
	// 关键断言：门槛未达成时确实补了一次新手真实对话（而不是空转跳过）
	if chats != 1 {
		t.Errorf("新手真实对话次数应为 1，实际 %d", chats)
	}
}

// 门槛补不齐时：如实跳过而不是把错误抛给上层（任务页显示跳过而非失败）。
func TestDepartTravelSkipsWhenGateUnrecoverable(t *testing.T) {
	withMockUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == actTasksList:
			// 没有 first_buddy 任务可补 → 门槛无法自动恢复
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "data": map[string]interface{}{"tasks": []interface{}{}}})
		case r.URL.Path == actBuddyAgree:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		case r.Method == http.MethodGet && r.URL.Path == actBuddyInfo:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "data": map[string]interface{}{
				"buddy": map[string]interface{}{"id": 0},
			}})
		case r.URL.Path == actBuddyFirst:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 400, "msg": "first_buddy task not completed"})
		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
		}
	})
	// 没有 first_buddy 任务 → ensureFirstBuddyDone 返回 true → 会重试领养 → 仍被挡 → 跳过
	p := &plugin{}
	summary, err := p.departTravel(context.Background(), newTestCred(), 0)
	if err != nil {
		t.Fatalf("不应把门槛当故障返回: %v", err)
	}
	if summary == "" || summary == "已派出 Buddy 旅行" {
		t.Errorf("应如实跳过，实际 summary=%q", summary)
	}
}
