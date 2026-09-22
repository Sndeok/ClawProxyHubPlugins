# ClawProxyHubPlugins

[ClawProxyHub-Next](https://github.com/Sndeok/ClawProxyHub-Next) 的插件仓库：每个子目录一个插件，合入 main 后由 CI 构建、发布 Release 并更新市场索引，核心的「插件市场」默认从本仓库安装。

| 插件 | 说明 |
| --- | --- |
| `lobsterai` | 网易有道 LobsterAI：浏览器 OAuth / 凭据文件登录，每日签到 |
| `workbuddy` | 腾讯 WorkBuddy / CodeBuddy：手机验证码 / 浏览器授权 / 凭据文件登录，签到、盲盒、旅行、成长任务（旅行会自动补 `first_buddy` 新手门槛；识别上游内容审核拒绝） |
| `qoder` | Qoder（qoder.sh / qoder.com.cn）：浏览器设备授权 / PAT / 粘贴 dt- 令牌，额度与每日签到 |
| `qoderwork` | QoderWork（CN）：浏览器设备授权 / 粘贴 dt- 令牌，额度与每日签到 |
| `cline` | Cline（cline.bot）：WorkOS 设备授权 / 粘贴 refreshToken，免费额度模型（含免费通道串行保护） |
| `opencode` | OpenCode Zen / Zen Go：Zen Key / Go 订阅 Key / 匿名免费通道（免账号），按模型原生协议走 chat / messages / responses |
| `commandcode` | Command Code（api.commandcode.ai）：user_ Key + 设备指纹伪装 + lifecycle 上报，AI-SDK 事件流 |
| `joycode` | 京东 JoyCode（joycode-api.jd.com）：京东账号扫码授权 / 粘贴 ptKey+userId，color 网关 HMAC 签名，GLM / Kimi / MiniMax / Doubao / Claude-Opus 等模型 |

## 目录约定

```
plugins/<name>/
├── manifest.json   # 版本号唯一来源：name（= 目录名）、version、author、label、icon
├── icon.png        # 可选，正方形 PNG 128–256px
└── main.go         # 入口：sdk.Serve(&plugin{})，Handshake 回报 version 变量
tools/pack/         # 打包器：交叉编译 + .cphplugin + index.json
internal/qodersign/ # Qoder 系共享：COSY 签名 / QoderEncoding / 设备指纹 / 嵌套 SSE 解包
index.json          # 市场索引（CI 生成并回写，勿手改）
```

插件实现 `pb.ClawPluginServer`（契约见核心 `sdk/proto/cph.proto`），复用 `sdk/openaiup` / `sdk/anthropicup` 适配 OpenAI / Anthropic 方言上游；宿主回调（日志 / 存储 / 代理）实现 `sdk.HostAware`。参考核心 `examples/stub` 与既有插件。

## 开发

SDK 来自核心模块 `github.com/Sndeok/ClawProxyHub-Next`（go.mod 固定到某个提交）。要对着本地核心源码开发，用 workspace 覆盖（`go.work` 已忽略，不入库）：

```bash
go work init .
go work edit -replace github.com/Sndeok/ClawProxyHub-Next=../ClawProxyHub-Next

go build ./... && go test ./...

# 编译当前平台并装进核心的插件目录（核心运行中会锁住二进制，先在插件页停止该插件）
go run ./tools/pack -install ../ClawProxyHub-Next/data/plugins
```

升级 SDK 版本：`GOWORK=off go get github.com/Sndeok/ClawProxyHub-Next@main && GOWORK=off go mod tidy`。

## 打包与发布

> fork 提示：仓库是 fork 出来的，Actions 的自动触发（push）在首次可能不生效，
> 表现为推送后 Actions 页面没有任何 run。此时在 Actions 页面「Run workflow」
> 手动触发一次，或命令行：
>
> ```bash
> gh workflow run build --repo <你的owner>/ClawProxyHubPlugins --ref main
> ```
>
> 工作流也会拉取 `CORE_REPO`（默认 `Sndeok/ClawProxyHub`）到 `.cph-core` 并用
> `go.work` 的 replace 对着它编译插件，所以核心仓库要先推送、且 CI 能读到。

```bash
go run ./tools/pack            # dist/<name>-<version>.cphplugin + dist/index.json
go run ./tools/pack -only workbuddy
```

- 包格式：zip，含 `manifest.json`、图标与 `plugin-<os>-<arch>[.exe]`（windows/amd64、linux/amd64、linux/arm64、darwin/amd64、darwin/arm64）；固定时间戳，同一输入产出同一 sha256
- 发布：改 `manifest.json` 的 `version` → 合入 main → CI 为每个新版本创建 Release `<name>-v<version>`（资产 `<name>-<version>.cphplugin`）并回写 `index.json`
- 已发布版本不可变：改代码必须升版本，否则 CI 跳过该插件
- 核心默认市场地址：`https://raw.githubusercontent.com/Sndeok/ClawProxyHubPlugins/main/index.json`

## 贡献

1. fork → `plugins/<你的插件>/` 开发（`manifest.json` 的 `author` 与 GitHub 用户名一致）
2. `go vet ./... && go test ./... && go run ./tools/pack -only <你的插件>` 确认可构建
3. 提 PR，CI 会完整交叉编译一遍

## 许可证

与核心相同，[AGPL-3.0](LICENSE)。
