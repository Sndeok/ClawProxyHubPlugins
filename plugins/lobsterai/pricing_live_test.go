package main

import (
	"encoding/json"
	"testing"

	pb "github.com/Sndeok/ClawProxyHub-Next/sdk/proto/cphv1"
)

// livePricingSample 取自线上公开接口 /api/models/pricing-catalog 的真实返回（前 3 条，
// 2026-09-22 抓取）。它保证「模型中心」展示的倍率 / 上下文 / 档位与官方客户端一致，
// 而不是对着我自己编的样例自说自话。
const livePricingSample = `[
  {
    "modelId": "deepseek-flash",
    "modelName": "DeepSeek-V4.1-Flash",
    "provider": "LobsterAI",
    "providerLabel": "LobsterAI",
    "description": "DeepSeek V4.1 Flash 采用了新的模型结构，原生多模态支持、能力更强、速度更快、且成本更低。；分时计价：当前空闲时段 x0.05；高峰时段 09:00-12:00、14:00-18:00（北京时间）",
    "freeAccess": true,
    "supportsImage": true,
    "supportsThinking": true,
    "thinkingConfig": {
      "options": [
        {
          "level": "off",
          "openclawLevel": "off"
        },
        {
          "level": "high",
          "openclawLevel": "high"
        },
        {
          "level": "max",
          "openclawLevel": "xhigh"
        }
      ],
      "defaultLevel": "high"
    },
    "contextWindow": 1000000,
    "costMultiplier": 0.05,
    "moreModel": false,
    "inputPrice": 1,
    "outputPrice": 4,
    "inputCredits": 100,
    "outputCredits": 400,
    "cacheInputCredits": 2,
    "pricingTiers": [],
    "timeBasedPricing": {
      "timezone": "Asia/Shanghai",
      "peakDays": [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY"
      ],
      "peakPeriods": [
        {
          "start": "09:00",
          "end": "12:00"
        },
        {
          "start": "14:00",
          "end": "18:00"
        }
      ],
      "currentPeriod": "offPeak",
      "offPeak": {
        "inputCredits": 100,
        "outputCredits": 400,
        "cacheInputCredits": 2
      },
      "peak": {
        "inputCredits": 200,
        "outputCredits": 800,
        "cacheInputCredits": 4
      }
    }
  },
  {
    "modelId": "deepseek-v4-pro",
    "modelName": "DeepSeek-V4-Pro",
    "provider": "LobsterAI",
    "providerLabel": "LobsterAI",
    "description": "推理编码顶尖，适合复杂逻辑分析、工程开发与超长文档理解；分时计价：当前空闲时段 x0.26；高峰时段 09:00-12:00、14:00-18:00（北京时间）",
    "freeAccess": true,
    "supportsImage": true,
    "supportsThinking": true,
    "thinkingConfig": {
      "options": [
        {
          "level": "off",
          "openclawLevel": "off"
        },
        {
          "level": "high",
          "openclawLevel": "high"
        },
        {
          "level": "max",
          "openclawLevel": "xhigh"
        }
      ],
      "defaultLevel": "high"
    },
    "contextWindow": 1000000,
    "costMultiplier": 0.26,
    "moreModel": false,
    "inputPrice": 4.5,
    "outputPrice": 13.5,
    "inputCredits": 450,
    "outputCredits": 1350,
    "cacheInputCredits": 15,
    "pricingTiers": [],
    "timeBasedPricing": {
      "timezone": "Asia/Shanghai",
      "peakDays": [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY"
      ],
      "peakPeriods": [
        {
          "start": "09:00",
          "end": "12:00"
        },
        {
          "start": "14:00",
          "end": "18:00"
        }
      ],
      "currentPeriod": "offPeak",
      "offPeak": {
        "inputCredits": 450,
        "outputCredits": 1350,
        "cacheInputCredits": 15
      },
      "peak": {
        "inputCredits": 900,
        "outputCredits": 2700,
        "cacheInputCredits": 30
      }
    }
  },
  {
    "modelId": "glm-5.3-flashx",
    "modelName": "GLM-5.3-FlashX",
    "provider": "LobsterAI",
    "providerLabel": "LobsterAI",
    "description": "GLM-5.3-FlashX ，推理速度达 200 tokens/s，提供更快、更流畅的模型体验。Coding 表现与 Claude Opus 4.8 相当，并强化了前端、游戏及 3D 仿真等视觉 Coding 能力。适合低成本的长上下文 Coding 与规模化 Agent 场景。",
    "freeAccess": true,
    "supportsImage": true,
    "supportsThinking": true,
    "thinkingConfig": {
      "options": [
        {
          "level": "off",
          "openclawLevel": "off"
        },
        {
          "level": "high",
          "openclawLevel": "high"
        },
        {
          "level": "max",
          "openclawLevel": "xhigh"
        }
      ],
      "defaultLevel": "max"
    },
    "contextWindow": 1000000,
    "costMultiplier": 0.15,
    "moreModel": false,
    "inputPrice": 2,
    "outputPrice": 7,
    "inputCredits": 200,
    "outputCredits": 700,
    "cacheInputCredits": 57,
    "pricingTiers": [],
    "timeBasedPricing": null
  }
]`

func TestLivePricingCatalogParses(t *testing.T) {
	var items []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(livePricingSample), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("样例应有 3 条，got %d", len(items))
	}
	for i, item := range items {
		id := rawString(lookup(item, "id"))
		info := &pb.ModelInfo{Id: id, Label: map[string]string{"en": id}}
		enrichMissing(info, item)
		if info.CreditsMultiplier <= 0 {
			t.Errorf("[%d] %s 倍率未解析：%v", i, id, info.CreditsMultiplier)
		}
		if info.ContextWindow <= 0 {
			t.Errorf("[%d] %s 上下文未解析：%v", i, id, info.ContextWindow)
		}
		if len(info.ReasoningEfforts) == 0 {
			t.Errorf("[%d] %s 推理档位未解析", i, id)
		}
		if info.DefaultReasoningEffort == "" {
			t.Errorf("[%d] %s 默认档位未解析", i, id)
		}
		if info.Series == "" || info.Series == "其他" {
			t.Errorf("[%d] %s 系列未推导：%q", i, id, info.Series)
		}
		t.Logf("[%d] %s → 系列=%s 倍率=x%v 上下文=%d 最大输出=%d 档位=%v 默认=%s",
			i, id, info.Series, info.CreditsMultiplier, info.ContextWindow,
			info.MaxOutputTokens, info.ReasoningEfforts, info.DefaultReasoningEffort)
	}
}
