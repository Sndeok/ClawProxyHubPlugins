// activities.go — 成长活动：盲盒 / 猫猫旅行 / 成长任务（接取 + 领奖）。
// 协议移植自原 Python 项目：上游把业务错误放在 HTTP 400 的 JSON body 里，
// code 数字/字符串混用，统一走 actData 宽松判定；无猫 / 门槛未达属账号状态，归跳过不算失败。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

const (
	actEnergy      = "/activity/growth/energy"
	actBlindbox    = "/activity/growth/buddy/open"
	actQuota       = "/activity/growth/buddy/quota"
	actTravelStat  = "/activity/growth/buddy/travel/status"
	actTravelGo    = "/activity/growth/buddy/travel/depart"
	actTravelWin   = "/activity/growth/buddy/travel/claim"
	actTravelCfg   = "/activity/growth/buddy/travel/config"
	actBuddyInfo   = "/activity/growth/buddy/info"
	actBuddyFirst  = "/activity/growth/buddy/first"
	actBuddyAgree  = "/activity/growth/buddy/agreement"
	actTasksList   = "/v2/activity/growth/tasks"
	actTasksAccept = "/activity/growth/tasks/accept"
)

// actGet GET + 宽松 envelope。
func (p *plugin) actGet(ctx context.Context, cred *credential, path, label string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", upstreamBase+path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range p.headers(cred, true) {
		req.Header.Set(k, v)
	}
	resp, err := p.hc(cred).Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s接口网络失败: %w", label, err)
	}
	defer resp.Body.Close()
	return actData(resp, label)
}

// actPost POST + 宽松 envelope。
func (p *plugin) actPost(ctx context.Context, cred *credential, path string, body interface{}, label string) (json.RawMessage, error) {
	resp, err := postJSON(ctx, p.hc(cred), upstreamBase+path, p.headers(cred, true), body)
	if err != nil {
		return nil, fmt.Errorf("%s接口网络失败: %w", label, err)
	}
	defer resp.Body.Close()
	return actData(resp, label)
}

// actData 宽松判定：code 为 0（数字或字符串）即成功，data 缺失回空对象。
func actData(resp *http.Response, label string) (json.RawMessage, error) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var e struct {
		Code interface{}     `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		return nil, fmt.Errorf("%s响应非 JSON（HTTP %d）", label, resp.StatusCode)
	}
	switch v := e.Code.(type) {
	case float64:
		if v != 0 {
			return nil, fmt.Errorf("%s失败：code=%v %s", label, v, strings.TrimSpace(e.Msg))
		}
	case string:
		if v != "0" {
			return nil, fmt.Errorf("%s失败：code=%v %s", label, v, strings.TrimSpace(e.Msg))
		}
	}
	if len(e.Data) == 0 {
		return json.RawMessage("{}"), nil
	}
	return e.Data, nil
}

// isBuddyStateError 无猫 / 领养门槛未达——账号状态而非故障，应跳过不重试。
func isBuddyStateError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no active buddy") || strings.Contains(msg, "first_buddy task not completed")
}

// ---------- 盲盒 ----------

// runBlindbox 查能量与配额，能量足够则一次开完，返回抽到的伙伴名。
func (p *plugin) runBlindbox(ctx context.Context, cred *credential) (*pb.RunTaskResponse, error) {
	data, err := p.actGet(ctx, cred, actEnergy, "盲盒能量")
	if err != nil {
		return nil, err
	}
	var energy struct {
		Balance int `json:"balance"`
	}
	_ = json.Unmarshal(data, &energy)

	draws := 0
	if q, err := p.actGet(ctx, cred, actQuota, "盲盒配额"); err == nil {
		var quota struct {
			Affordable int `json:"affordable"`
		}
		if json.Unmarshal(q, &quota) == nil {
			draws = quota.Affordable
		}
	}
	if draws <= 0 && energy.Balance >= 10 { // quota 拿不到时按能量本地推导（单次 10）
		draws = energy.Balance / 10
	}
	if draws <= 0 {
		return &pb.RunTaskResponse{Summary: fmt.Sprintf("跳过：能量不足（余额 %d）", energy.Balance)}, nil
	}

	res, err := p.actPost(ctx, cred, actBlindbox, map[string]interface{}{"count": draws}, "开盲盒")
	if err != nil {
		return nil, err
	}
	var opened struct {
		Count int `json:"count"`
		Items []struct {
			Template struct {
				Name string `json:"name"`
			} `json:"template"`
		} `json:"results"`
	}
	_ = json.Unmarshal(res, &opened)
	names := ""
	for i, item := range opened.Items {
		if i >= 5 {
			names += "…"
			break
		}
		if i > 0 {
			names += "、"
		}
		if item.Template.Name != "" {
			names += item.Template.Name
		} else {
			names += "未知伙伴"
		}
	}
	n := opened.Count
	if n == 0 {
		n = draws
	}
	return &pb.RunTaskResponse{Summary: fmt.Sprintf("开盲盒 ×%d：%s", n, names)}, nil
}

// ---------- 猫猫旅行 ----------

// runTravel 按状态分派：空闲则出发（无猫先领养），到点则领奖，在途则报剩余时间。
func (p *plugin) runTravel(ctx context.Context, cred *credential) (*pb.RunTaskResponse, error) {
	data, err := p.actGet(ctx, cred, actTravelStat, "猫猫旅行状态")
	if err != nil {
		if isBuddyStateError(err) {
			return &pb.RunTaskResponse{Summary: "跳过：无可派出的 Buddy"}, nil
		}
		return nil, err
	}
	var st struct {
		State        string `json:"state"`
		RecordID     int64  `json:"record_id"`
		BuddyID      int64  `json:"buddy_id"`
		ArriveAt     int64  `json:"arrive_at"`
		ServerNow    int64  `json:"server_now"`
		DailyLimited bool   `json:"daily_limit_reached"`
		Reward       int    `json:"reward_credit"`
	}
	_ = json.Unmarshal(data, &st)

	switch {
	case st.State == "" || st.State == "idle":
		if st.DailyLimited {
			return &pb.RunTaskResponse{Summary: "跳过：今日旅行已达上限"}, nil
		}
		summary, err := p.departTravel(ctx, cred, st.BuddyID)
		if err != nil {
			return nil, err
		}
		return &pb.RunTaskResponse{Summary: summary}, nil

	case st.State == "arrived" || (st.State == "traveling" && st.ArriveAt > 0 && st.ServerNow >= st.ArriveAt):
		if st.RecordID == 0 {
			return &pb.RunTaskResponse{Summary: "跳过：缺少旅行记录，无法领奖"}, nil
		}
		res, err := p.actPost(ctx, cred, actTravelWin, map[string]interface{}{"record_id": st.RecordID}, "猫猫旅行领奖")
		if err != nil {
			return nil, err
		}
		var claim struct {
			Reward int `json:"reward_credit"`
		}
		_ = json.Unmarshal(res, &claim)
		return &pb.RunTaskResponse{Summary: fmt.Sprintf("旅行归来领奖，积分 +%d", claim.Reward)}, nil

	case st.State == "traveling" && st.ArriveAt > st.ServerNow:
		minutes := (st.ArriveAt - st.ServerNow + 59) / 60
		return &pb.RunTaskResponse{Summary: fmt.Sprintf("跳过：在途，约 %d 分钟后到达", minutes)}, nil

	default:
		return &pb.RunTaskResponse{Summary: "跳过：旅行状态未知（" + st.State + "）"}, nil
	}
}

// ensureBuddy 确保账号有可派出的 Buddy：协议幂等先调，无猫则领养。
func (p *plugin) ensureBuddy(ctx context.Context, cred *credential, buddyID int64) error {
	if _, err := p.actPost(ctx, cred, actBuddyAgree, map[string]interface{}{"agree": true}, "Buddy 协议"); err != nil {
		return err
	}
	if buddyID != 0 {
		return nil
	}
	data, err := p.actGet(ctx, cred, actBuddyInfo, "Buddy 档案")
	if err != nil {
		return err
	}
	var info struct {
		Buddy struct {
			ID int64 `json:"id"`
		} `json:"buddy"`
	}
	_ = json.Unmarshal(data, &info)
	if info.Buddy.ID != 0 {
		return nil
	}
	_, err = p.actPost(ctx, cred, actBuddyFirst, map[string]interface{}{}, "Buddy 领养")
	return err
}

// travelLocation 出发目的地：上游 config 排序第一，拿不到回退 1。
func (p *plugin) travelLocation(ctx context.Context, cred *credential) (int, error) {
	data, err := p.actGet(ctx, cred, actTravelCfg, "猫猫旅行配置")
	if err != nil {
		return 1, nil // 目的地解析失败不挡出发
	}
	var cfg struct {
		Locations []struct {
			ID   int `json:"id"`
			Sort int `json:"sort"`
		} `json:"locations"`
	}
	if json.Unmarshal(data, &cfg) != nil || len(cfg.Locations) == 0 {
		return 1, nil
	}
	return cfg.Locations[0].ID, nil
}

// ---------- 成长任务 ----------

// runGrowthTasks 状态机推进：批量接取 → 埋点推进 → 领奖。
// 每个阶段改变状态就重拉列表，让刚接取的任务本轮可推进、刚推进完成的本轮可领奖。
func (p *plugin) runGrowthTasks(ctx context.Context, cred *credential) (*pb.RunTaskResponse, error) {
	tasks, err := p.growthTasks(ctx, cred)
	if err != nil {
		return nil, err
	}

	// 1. 批量接取 single 型未接任务
	var toAccept []string
	for _, t := range tasks {
		if t.action() == "accept" {
			toAccept = append(toAccept, t.Code)
		}
	}
	if len(toAccept) > 0 {
		if _, err := p.actPost(ctx, cred, actTasksAccept, map[string]interface{}{"task_codes": toAccept}, "任务接取"); err != nil {
			return nil, err
		}
		tasks, err = p.growthTasks(ctx, cred)
		if err != nil {
			return nil, err
		}
	}

	// 2. 埋点推进（有已知链路的进行中任务）；单个失败只记录，不中断其它
	advanced, advanceFailed := 0, []string{}
	for _, t := range tasks {
		if t.action() != "advance" {
			continue
		}
		n, err := p.advanceTask(ctx, cred, t)
		if err != nil {
			advanceFailed = append(advanceFailed, orDefault(t.Title, t.Code))
			continue
		}
		advanced += n
	}
	if advanced > 0 {
		if tasks, err = p.growthTasks(ctx, cred); err != nil {
			return nil, err
		}
	}

	// 3. 领取已完成任务的奖励
	claimed, credit := 0, 0
	for _, t := range tasks {
		if t.action() != "claim" {
			continue
		}
		res, err := p.actPost(ctx, cred, "/activity/growth/tasks/"+t.Code+"/claim", map[string]interface{}{}, "任务领奖")
		if err != nil {
			continue // 单个失败不中断
		}
		var claim struct {
			Already bool `json:"already_claimed"`
			Credit  int  `json:"credit"`
		}
		_ = json.Unmarshal(res, &claim)
		if !claim.Already {
			claimed++
			credit += claim.Credit
		}
	}

	summary := fmt.Sprintf("接取 %d / 推进 %d / 领奖 %d（积分 +%d）", len(toAccept), advanced, claimed, credit)
	if len(advanceFailed) > 0 {
		summary += fmt.Sprintf("；%d 项推进失败", len(advanceFailed))
	}
	// 执行后的任务快照：结构化明细持久化到 task_runs，账号详情弹窗直接渲染
	detail := map[string]interface{}{"items": tasks}
	if b, err := json.Marshal(detail); err == nil {
		return &pb.RunTaskResponse{Summary: summary, DetailJson: string(b)}, nil
	}
	return &pb.RunTaskResponse{Summary: summary}, nil
}

type growthTask struct {
	Code         string `json:"task_code"`
	Title        string `json:"title"`
	TaskType     string `json:"task_type"`
	AcceptStatus string `json:"accept_status"`
	Locked       bool   `json:"locked"`
	RewardCredit int    `json:"reward_credit"`
	RewardEnergy int    `json:"reward_energy"`
	Progress     struct {
		Current int `json:"current"`
		Target  int `json:"target"`
	} `json:"progress"`
}

// action 下一步动作：accept / advance / claim / none。
func (t growthTask) action() string {
	switch {
	case t.AcceptStatus == "completed":
		return "claim"
	case t.AcceptStatus == "not_accepted" && t.TaskType == "single":
		return "accept"
	case (t.AcceptStatus == "accepted" || t.AcceptStatus == "in_progress") && !t.Locked && taskAdvanceable(t):
		return "advance"
	}
	return "none"
}

func (p *plugin) growthTasks(ctx context.Context, cred *credential) ([]growthTask, error) {
	data, err := p.actGet(ctx, cred, actTasksList, "成长任务")
	if err != nil {
		return nil, err
	}
	var list struct {
		Tasks []growthTask `json:"tasks"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("成长任务解析失败: %w", err)
	}
	return list.Tasks, nil
}

// departTravel 领养（必要时自动过 first_buddy 新手门槛）后派发旅行。
// 返回值：非空 summary = 本次跳过/已完成（不算错误）；err 非 nil = 真失败。
func (p *plugin) departTravel(ctx context.Context, cred *credential, buddyID int64) (string, error) {
	err := p.ensureBuddy(ctx, cred, buddyID)
	if err != nil && isBuddyStateError(err) {
		// 领养门槛几乎总是 first_buddy 新手任务未完成（需要一次真实对话 + 埋点）。
		// 用户通常只跑「猫猫旅行」，于是永远卡在门槛外 —— 这里主动补一次。
		ok, doneErr := p.ensureFirstBuddyDone(ctx, cred)
		if doneErr != nil {
			return "跳过：领养门槛未达成（自动补 first_buddy 失败：" + doneErr.Error() + "）", nil
		}
		if !ok {
			return "跳过：领养门槛未达成（first_buddy 新手任务未完成）", nil
		}
		err = p.ensureBuddy(ctx, cred, buddyID)
	}
	if err != nil {
		if isBuddyStateError(err) {
			return "跳过：无可派出的 Buddy（领养门槛未达成）", nil
		}
		return "", err
	}
	locID, err := p.travelLocation(ctx, cred)
	if err != nil {
		return "", err
	}
	if _, err := p.actPost(ctx, cred, actTravelGo, map[string]interface{}{"location_id": locID}, "猫猫旅行出发"); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already traveling") {
			return "已在途（状态重查确认）", nil
		}
		if isBuddyStateError(err) {
			return "跳过：无可派出的 Buddy", nil
		}
		return "", err
	}
	return "已派出 Buddy 旅行", nil
}

// ensureFirstBuddyDone 复用成长任务链路把 first_buddy 推到 completed：
// 接取（single 型）→ 发对话补齐次数（advanceTask 内部带 chat_request_send 埋点）→ 复查。
// 返回 true = 门槛已过（或账号本就没有该任务）。
func (p *plugin) ensureFirstBuddyDone(ctx context.Context, cred *credential) (bool, error) {
	tasks, err := p.growthTasks(ctx, cred)
	if err != nil {
		return false, err
	}
	find := func() *growthTask {
		for i := range tasks {
			if tasks[i].Code == "first_buddy" {
				return &tasks[i]
			}
		}
		return nil
	}
	t := find()
	if t == nil {
		return true, nil // 没有这个任务 = 门槛与它无关
	}
	if t.AcceptStatus == "completed" {
		return true, nil
	}
	if t.action() == "accept" {
		if _, err := p.actPost(ctx, cred, actTasksAccept,
			map[string]interface{}{"task_codes": []string{"first_buddy"}}, "新手任务接取"); err != nil {
			return false, err
		}
		if tasks, err = p.growthTasks(ctx, cred); err != nil {
			return false, err
		}
		if t = find(); t == nil {
			return true, nil
		}
	}
	if t.action() != "advance" {
		return false, nil
	}
	// first_buddy 的官方判定要求「一次真实对话」：只发 /v2/report 埋点通常不计入，
	// 这里直接按客户端行为发一条最小对话（growthEvent 内嵌在 extra_vars 里），
	// 与另一实现 codebuddy2api 的 first-Buddy onboarding 保持一致。
	if err := p.realFirstBuddyChat(ctx, cred); err != nil {
		// 真实对话不可用时退回埋点链路，至少推进依赖埋点的其它任务
		if _, aerr := p.advanceTask(ctx, cred, *t); aerr != nil {
			return false, err
		}
	}
	// 复查：官方结算可能是异步的，这里以当次结果为准（未完成则下次运行再补）
	if tasks, err = p.growthTasks(ctx, cred); err != nil {
		return false, err
	}
	if t = find(); t != nil && t.AcceptStatus == "completed" {
		return true, nil
	}
	return false, nil
}

// realFirstBuddyChat 发一条最小真实对话（Say OK，max_tokens 32），并把 chat_request_send
// 事件按官方客户端形态内嵌到 extra_vars.growthEvent —— 首只 Buddy 的新手任务据此判定完成。
func (p *plugin) realFirstBuddyChat(ctx context.Context, cred *credential) error {
	ids := newConversation()
	co := chatEventOpts{model: defaultModel, modelName: defaultModelName, agentType: "main"}
	growth, err := json.Marshal([]map[string]interface{}{chatRequestSendEvent(cred, ids, co)})
	if err != nil {
		return err
	}
	body := map[string]interface{}{
		"model": defaultModel,
		"messages": []map[string]string{
			{"role": "system", "content": "Reply with OK only. Do not use tools."},
			{"role": "user", "content": "Say OK."},
		},
		"stream":         true,
		"stream_options": map[string]interface{}{"include_usage": true},
		"max_tokens":     32,
		"extra_vars":     map[string]interface{}{"growthEvent": string(growth)},
	}
	h := p.headers(cred, true)
	h["Accept"] = "text/event-stream"
	h["X-Conversation-ID"] = ids.conversation
	h["X-Session-ID"] = ids.conversation
	h["X-Parent-Conversation-ID"] = ids.conversation
	h["X-Request-ID"] = ids.request
	h["X-Root-Request-ID"] = ids.request
	h["X-Conversation-Request-ID"] = ids.request
	h["X-Conversation-Message-ID"] = ids.message
	h["X-Agent-Purpose"] = "conversation"

	resp, err := postJSON(ctx, p.hc(cred), upstreamBase+pathChat, h, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("新手对话 HTTP %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	// 读到流结束（有界）后丢弃：只需官方服务端处理这次对话
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return nil
}
