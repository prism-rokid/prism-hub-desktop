# 06. SDK 功能设计与使用指南

更新时间：2026-08-01

本章定义 PluginBridge SDK 和 Hub <-> Plugin 的本地 stdio 协议，并提供开发新 Plugin 的统一流程。所有 JSON wire field 使用 `snake_case`；语言内类型可使用各自惯用命名。

## 1. SDK 能力与语言支持

| SDK | 位置 | 适用场景 |
| --- | --- | --- |
| Go | [prism-plugin-sdk](https://github.com/Rokid-Prism/prism-plugin-sdk) | 模块 `github.com/Rokid-Prism/prism-plugin-sdk`，正式 Plugin、长期维护和原生协议适配。 |
| Node | [prism-plugin-sdk/node](https://github.com/Rokid-Prism/prism-plugin-sdk/tree/main/node) | 包 `@rokid/pluginbridge-plugin-sdk`，同时提供 ESM/CJS。 |
| Python | [prism-plugin-sdk/python](https://github.com/Rokid-Prism/prism-plugin-sdk/tree/main/python) | 包 `rokid-pluginbridge-plugin-sdk`，导入名 `pluginbridge`。 |

三套 SDK 必须实现同一 PluginBridge 协议版本、RPC 语义和 capability gate。SDK 负责语言对象与 `snake_case` wire payload 的唯一转换；Plugin 不自行兼容另一套字段。Node SDK 可以复用 `desktop-automation-runtime` 与 `cdp-runtime`；目标产品特有的 DOM、菜单、启动参数和选择器仍属于 Plugin 自己。

## 2. manifest

每个 Plugin 包必须有 `pluginbridge-plugin.yaml`，最小内容如下：

```yaml
id: acme.tool
display_name: Acme Tool
version: 0.1.0
integration_mode: protocol-native
transport: process
mode: protocol
# Every Plugin declares at least one runtime implementation. The Hub validates
# `bridge.plugins.acme.tool.mode` against this list.
runtime_modes: [native, managed]
default_runtime_mode: native
runtime_controls:
  native:
    model: stable
    reasoning: stable
  managed: {}
runtime_operations:
  native:
    rename: direct
    pin: direct
    archive: direct
    delete: unsupported
  managed:
    rename: menu
    pin: menu
    archive: menu
    delete: menu
managed_terminal_modes: [managed]
command: ["./bin/acme-adapter"]
capabilities:
  can_list_sessions: true
  can_read_history: true
  can_attach_session: true
  can_start_session_with_message: true
  can_interrupt: true
  can_wait_run: true
```

`id` 是唯一 `plugin_id`；必须与 SDK adapter 的 `id()` / `ID()` 返回值和 probe 的 `plugin_id` 一致。`integration_mode` 必须与 probe 的 `integration_mode` 一致；`command` 相对 Plugin 根目录解析。manifest 是静态承诺，probe 是运行时真相；两者不一致时 Hub 必须拒绝将该 runtime 视为可用。

`runtime_modes` 是同一 `plugin_id` 下互斥的运行时实现名称，`PluginConfig.Mode` 对所有 Plugin 都只表示其中的当前选择。新 manifest 必须明确声明它们；旧 manifest 加载时从其现有 `mode` 自动生成唯一 mode，保证所有 Plugin 有统一模型且不破坏已安装插件。Hub 只接受 manifest 声明的 `bridge.plugins.<plugin_id>.mode`，使用受控 `PRISM_PLUGIN_MODE` 传给 Plugin，并在值变化时重建该 Plugin 进程；`plugin_env` 中同名字段会被过滤，不能覆盖。模式不是会话字段，不能把不同 mode 的 live session、approval 或 queue 合并。

`runtime_controls` 按 runtime 声明模型、推理和权限各自唯一的 wire 实现：`stable` 使用 `current_* + *_options + *.switch action`，`interactive` 使用带 `semantic_kinds[]` 的 `interactive_controls[]`，空值表示该 runtime 不支持。一个语义在一个 runtime 中只能选一种实现，不能同时发布两套让客户端猜测。旧 manifest 可缺省以保持安装兼容；新 manifest 和脚手架必须为每个 runtime 显式写一项，即使值为 `{}`。manifest 声明的是表示所有权，probe 的 `can_*` 仍是当前 runtime 的实际运行能力。

`runtime_operations` 按 runtime 声明标准会话操作 `rename` / `pin` / `archive` / `delete` 的唯一 wire 实现。`direct` 表示 Plugin 在当前 detail 的 `actions[]` 发布同名 action 并由 `controlSession` 处理；`menu` 表示 Plugin 只发布实时 `conversation_menu_available=true`，具体条目通过短期 opaque menu session 描述和执行；`unsupported` 或空值表示不支持。Hub 按当前 runtime 删除不属于 active owner 的 direct action，并在没有任何 menu owner 时强制关闭会话菜单入口；声明本身不会把不可用操作改成可用，实时 `available` 和菜单内容仍必须来自 Plugin 的权威原生状态。`pin` 使用幂等 action `pin + {enabled:boolean}`，不能只实现 toggle。

`managed_terminal_modes` 是可选的本地 Desktop 能力声明。满足条件的 mode 可以提供原生结构化 transport，或由 Plugin 独占真实 CLI 的 PTY master，并使本地 attach 客户端和远端操作都经过同一个输入仲裁器。普通裸 PTY、`screen` 多写和 Desktop/CDP 均不满足条件。Plugin 必须保证单一执行实例、会话身份稳定、输入所有权明确、断连时撤销过期操作权限，并发布权威的 `detail.terminal` 状态。实现 `adapter.openManagedTerminal({plugin_id,cwd,native_session_id?,native_thread_id?})` 时，指定 native ID 只可附着对应的已托管实例；实例不存在必须明确失败，不得暗中 `resume` 成另一个执行进程。该 RPC 只能由 Hub 本地 Dashboard 调用，返回 `{ok,message}`，不得返回 socket URL、token 或任何本机凭证；Mobile/Panel/API 不得调用。Hub 在调用前必须同时检查 manifest 当前 mode 与 probe `can_open_managed_terminal`，不能按 plugin ID 或 runtime 名称判断。

新增 runtime（例如以后为现有 Plugin 增加 `cli`）只需在 manifest 增加 mode/profile，并在 Plugin 内把原生行为适配到上述稳定 wire；API、Realtime、Hub 和客户端都不得按 runtime 名称增加分支。只有新增统一协议尚未定义的全新标准语义时，才需要先升级公共 schema。旧 manifest 可缺省 `runtime_operations` 以保持安装兼容；新 manifest 和脚手架必须为每个 runtime 显式声明，避免切换后沿用旧 runtime 的操作入口。

### 2.1 命名规范

新 Plugin 的 `id` 直接使用产品名，**不带 `.gateway` / `.automation` 等集成模式后缀**：

| 旧 id | 当前 id |
| --- | --- |
| `codex.automation` | `codex` |
| `openclaw.gateway` | `openclaw` |
| `hermes.automation` | `hermes` |

理由：`id` 是唯一主键，也是 `display_name` 缺省时的显示名来源。集成模式由 `integration_mode` 表达，不再编码在 id 中。

### 2.2 单一 Plugin 身份

`family_id` 已从 manifest、Hub/Plugin stdio contract、catalog/index payload、Go/Node/Python SDK 类型和 Plugin 返回对象中移除。规则固定为「一个 Plugin = 一个 `plugin_id`」；显示名仅由 `display_name` 或 `plugin_id` 派生。catalog/index 签名不再包含 family，升级后必须重新安装 Plugin 并重建本地状态，不能混用旧 wire payload。

### 2.3 安装产物

官方 Plugin 安装目标为 `~/.prism/pluginbridge-plugins/<id>/`，使用 `state.json` 指向 `versions/<active-version>/`，并保留上一版用于回滚。Hub 根据平台下载 Release 预构建资产，校验 metadata、manifest、协议、平台和安全解压规则后 probe，再原子激活。旧 `current` 安装继续兼容，并在第一次官方更新时迁移。

高级本地路径/Git 安装仍保留，但依赖必须来自正式 Go module、npm 或 PyPI 包；Hub 主仓不再提供或复制 SDK 副本。

## 3. 传输、版本和信封

PluginBridge v4 使用 JSON-over-newline stdio，一行一个 JSON 对象。Hub 发请求，Plugin 用同一 `id` 回最终响应；Plugin 可主动发事件。它不是 JSON-RPC 2.0，不带 `jsonrpc` 字段。Hub 通过 `PRISM_PLUGIN_PROTOCOL_VERSION=4` 启动 Plugin；版本不一致直接失败。

Hub 请求：

```json
{"id":"1","method":"adapter.probe","params":{}}
```

Plugin 成功响应：

```json
{"id":"1","ok":true,"payload":{"plugin_id":"acme.tool","available":true}}
```

Plugin 失败响应：

```json
{"id":"1","ok":false,"error":{"code":"plugin_unavailable","message":"Native application is unavailable"}}
```

Plugin 主动事件：

```json
{
  "event": "plugin.event",
  "payload": {
    "subscription_id": "sub-1",
    "event": {"id":"evt-1","type":"run","status":"running"}
  }
}
```

本地 stdio 使用 `ok` 是协议例外；对外 API/NDJSON 仍使用 `code/message/data`。v4 只接受 `snake_case` wire field；PascalCase payload、同一 payload 混用两套字段、或 `plugin_id` 与 manifest 不一致时直接失败，不做字段猜测或双字段 fallback。

v4 是一次完整破坏性升级：所有 Plugin 必须重新安装；v3 PascalCase stdio payload 不再接收，运行时不读取旧字段作为兼容路径。

### 3.1 `adapter.listSessions` 完整快照约束

`adapter.listSessions` 的成功响应是该 Plugin 可远程继续的**完整会话索引**。它必须包含全部符合条件的会话，不得固定截断为最近 N 条，也不得以桌面虚拟化侧边栏 DOM 作为数据源。Hub 会将成功结果投影为该 Plugin 的当前 `conversation.index.snapshot`，因此 Plugin 不得将本地读取失败、锁竞争、超时、截断输出或尚未稳定的中间状态表示为成功的空数组；这类情况必须返回 `ok=false` 和明确错误，Hub 不发布新索引，Redis 保留上一份完整投影直到下一次完整快照成功写入。

真实没有任何可继续会话时才允许成功返回 `[]`。存在多个并发读取来源时，Plugin 必须在自身将它们串行化或 single-flight 合并，避免对同一个本地状态库并发读取而产生不完整快照。这个规则是协议成功语义，不是 Hub 的空列表 fallback；所有 Plugin 都遵循相同约束。初始化后的单会话变更应由 watcher 上报，不能借每个 detail/watch 事件重新调用 `listSessions`。

## 4. 公共结构

### 4.1 `Capability`

```text
plugin_id
available
native_visible_input
native_visible_output
can_attach_session
can_start_session_with_message
can_list_sessions
can_read_history
can_interrupt
can_approval
can_forward_sync
can_reverse_sync
can_wait_run
can_read_status
can_control_session
integration_mode
visibility_surface
unavailable_reason
```

`available=false` 时必须给出 `unavailable_reason`。Hub 只调用已声明的能力，不能以“方法存在”尝试降级。`can_forward_sync`、`can_list_sessions`、`can_read_history`、`can_attach_session`、`can_start_session_with_message`、`can_wait_run` 和 `can_interrupt` 是远程会话完成态要求；原生有审批时还必须 `can_approval=true`。`can_reverse_sync` 不是替代要求，但 Plugin 仍须维护所需 watcher。

### 4.2 会话与消息

```text
StartSessionWithMessageRequest
  plugin_id
  cwd
  message (InboundMessage, prism_message_id non-empty)
  source_device
  metadata

StartSessionWithMessageResult
  Session (NativeSession)
  Receipt (SendReceipt)
  Visibility (VisibilityResult)

NativeSession
  plugin_id
  native_session_id
  native_thread_id
  surface
  endpoint
  cwd
  visible

InboundMessage
  prism_message_id
  text
  attachments[]
  source_device
  timestamp
  metadata

SendReceipt
  native_message_id
  canonical_native_session_id
  canonical_native_thread_id
  accepted
  visible
  pending_confirmation
  queue_pending
  detail

VisibilityResult
  visible
  marker
  evidence
  checked_at
  failure_reason

PluginEvent
  id
  type
  status
  summary
  payload
  created_at

HistoryMessage
  id
  role
  content
  created_at
  metadata

HistoryTurnRequest
  limit
  live

HistoryTurn
  turn_id
  order_key
  revision
  messages[]
```

`prism_conversation_id` 仅存在于已有会话的 Hub/Plugin 内部 binding，对外对应 canonical `conversation_id`；所有 native identity 只留在 Plugin/Hub 本地。`adapter.startSessionWithMessage` 的请求不得带 `prism_conversation_id`。启动成功必须同时满足 `accepted=true` 与 `visible=true`。`HistoryMessage` 只作为 `HistoryTurn` 的成员由 `readHistoryStream` 在线迭代，不成为任意中间层长期正文副本。

`InboundMessage.attachments[]` 只在 Hub 到 Plugin 的本地 RPC 中存在，字段固定为 `local_path`、`name`、`mime_type`。`local_path` 由 Hub 创建并授权，Plugin 只能读取该精确文件，不能把它写入事件、正文、日志或远程响应。原生 Composer 可能在提交确认后异步读取 `local_path`，因此 Plugin 明确失败时 Hub 立即释放文件，成功提交的文件保留至 24 小时 TTL；OSS 对象由既有上传服务按其自身策略回收，SDK、Hub、API 都不删除或刷新它。SDK 不提供 data URI/base64、OSS URL 或下载凭据字段。

`HistoryTurnRequest.limit` 由 API 服务端策略提供，当前默认 `5`，取值范围 `1..20`，且首次值带 `live=true`。Plugin 必须先建立正文订阅并记录源高水位，再 yield 首条为用户消息的最新 turn，再从近到远 yield 至多 `limit - 1` 个更早 turn，并在 live=true 时持续 yield 新增或替换事件；高水位之后、初始最新 turn 之前发生的 live 事件必须排队，不能丢失或抢在 initial 之前发出。SDK 不支持正文分页。`HistoryTurn.messages[]` 必须是一个完整 turn 的当前完整版本，带稳定 `turn_id`、原生排序 `order_key` 和同一 turn 内单调递增的 `revision`；不得把用户消息和其 assistant 输出拆到不同 turn。

`HistoryMessage.type` 只允许 `text` 或 `progress`。`progress` 是 assistant 的一个完整执行过程，不能用 metadata 或一组中间正文消息伪造。它的正式字段为 `progress.status`（`running`、`completed` 或 `failed`）、`progress.started_at`、可选 `progress.completed_at` 与按发生顺序排列的 `progress.steps[]`；每个 step 固定为 `id`、`kind`（`assistant_text`、`tool`、`tool_result` 或 `status`）、`title`、`detail`、`status` 与 `created_at`。运行中的 turn 固定为 `[user text, progress]`，`content` 是当前累积可见的 assistant 文本，工具和状态仅存在于 `steps`。终态同一 turn 的完整 replacement 固定为 `[user text, progress(completed|failed), final assistant text]`：progress 保留所有中间文本和工具步骤，`content` 是“已处理 Xm Xs”，终态最终回答才作为单独 text 消息。不得将中间 commentary、工具或 tool result 留为独立正文消息。

## 5. RPC 与能力 gate

### 5.1 单一权威源原则

`adapter.*` 方法集、参数 / 返回 schema 和 capability gate 的**唯一权威来源是本节**。三套 SDK（Go / Node / Python）必须实现本节定义的全部方法与 wire field；新增或迁移完成的 Plugin 必须通过对应 SDK 暴露这些方法，禁止自行新增 `adapter.*` 方法名。

历史背景：早期 Plugin 曾因 SDK 缺方法而在各自 `index.js` hand-roll `adapter.controlSession`、`adapter.readStatus` 等。本节把这些事实标准**正式收编为协议契约**；Codex 已迁回 SDK，仍保留手写 dispatcher 的 Plugin 在迁移前必须保持与 SDK 完全一致的 v4 wire 规则，不得扩展方法名或字段。

当本节与任何 SDK 实现或 Plugin 行为冲突时，**以本节为准**，SDK 和 Plugin 据此修正。

### 5.2 RPC 总表

| RPC | 必要条件 | 语义 |
| --- | --- | --- |
| `adapter.probe` | 始终 | 返回当前 capability 和直接不可用原因。 |
| `adapter.discover` | 始终 | 发现原生产品、数据源、窗口或 endpoint。 |
| `adapter.listSessions` | `can_list_sessions` | 返回用户可继续的原生会话轻摘要。 |
| `adapter.attachSession` | `can_attach_session` | 绑定用户指定的 native session。 |
| `adapter.openDraft` | `can_open_draft` | 打开且验证当前原生匿名草稿，返回仅本次生命周期有效的草稿证据和实际 controls；不得生成 native/canonical 会话身份。 |
| `adapter.controlDraft` | `can_open_draft` | 只控制同一个已验证匿名草稿；目标不再前台或不可辨认时返回 `draft_stale`。 |
| `adapter.startDraftWithMessage` | `can_open_draft` | 向同一个已验证草稿发送首条消息、验证可见性并返回真实 native identity；同一 `prism_message_id` 必须幂等。 |
| `adapter.startSessionWithMessage` | `can_start_session_with_message` | 兼容无草稿能力的 Plugin；Codex 不再作为 Mobile 新建主路径使用。 |
| `adapter.readDetail` | 可选 `DetailReader` | 不改变原生选择或前台 ownership，直接读取目标会话当前 canonical `detail_snapshot`；protocol-native Plugin 应实现，供详情查询和热更新后的缓存重建使用。 |
| `adapter.readHistoryStream` | `can_read_history` | 在线异步迭代完整 `HistoryTurn`。标准流先发第一条为 user 的最新 turn，再发最多 19 个更早 turn；不支持正文分页；live 更新必须为同一 turn 的更高 revision。 |
| `adapter.readStatus` | `can_read_status` | 读取当前 run 状态（含 `phase` / `preview` / `steps` / `context` 等 detail snapshot 不承载的字段）。 |
| `adapter.send` | `can_forward_sync` | 发送 `InboundMessage` 并返回 `SendReceipt`。 |
| `adapter.verifyVisibility` | `can_forward_sync` | 确认 marker 出现在用户可见历史。 |
| `adapter.subscribe` | 始终 | 维持原生 run、消息、审批和桌面观察。 |
| `adapter.subscribePlugin` | `can_plugin_wide_watch` | 维持一个 Plugin 级原生观察源；仅用于不按会话分别订阅的桌面自动化 Plugin。 |
| `adapter.waitForRun` | `can_wait_run` | 返回请求关联 run 的终态。 |
| `adapter.interrupt` | `can_interrupt` | 打断目标原生 run。 |
| `adapter.resolveApproval` | `can_approval` | 接收 `approval_request_id`、action 和必要输入。 |
| `adapter.controlSession` | `can_control_session` | 执行当前 detail 声明的 action，返回刷新后的 detail。 |
| `adapter.send` 的本地文件附件 | `can_local_file_attachments` | 接收 Hub materialize 后的受控 `attachments[]`，以原生 Agent 支持的方式注入。 |
| `adapter.close` | 始终 | 回收订阅、进程和临时资源。 |
| `adapter.openManagedTerminal` | `can_open_managed_terminal` 且当前 mode 在 `managed_terminal_modes` | 启动或附着本地受管 TUI；指定 native ID 时只可附着该实例。Plugin 独占 transport 或 PTY master 并仲裁所有输入；不得暴露 transport 凭证。 |

未声明 capability 时 Hub 不调用；Plugin 对不存在会话、过期审批、不可见发送和不支持 action 都必须返回明确错误，不能猜测或自行换目标。

受管 CLI 在注入输入后尚未观察到原生提交就失去确认能力时，`adapter.send` 应返回结构化错误码 `delivery_indeterminate`。Hub 只将这一明确错误码透传为 `conversation.send_failed.send_failure.code=delivery_indeterminate`；其他错误仍归类为 `send_failed`。Plugin 必须以 `PrismMessageID` 持久去重，同一 ID 的不确定投递不得自动再次注入，客户端也不得自动重试。Node SDK 可抛出 `new PluginAdapterError("delivery_indeterminate", message)`；不能靠错误文案匹配该状态。

### 5.3 新增 capability gate

历史 capability（`can_list_sessions` 等）见 4.1。结构化状态、控制与本地受管终端使用以下 gate：

| capability | gate 的 RPC | 声明 true 的要求 |
| --- | --- | --- |
| `can_read_status` | `adapter.readStatus` | 实现并返回结构化 `RunStatus`（含 `phase` / `preview` / `steps` / `context`） |
| `can_control_session` | `adapter.controlSession` | 实现，并支持当前 detail `actions[]` 声明的全部 action key |
| `can_plugin_wide_watch` | `adapter.subscribePlugin` | 实现一个 Plugin 级事件源；不得同时为同一原生变化建立无界的 per-session 订阅 |
| `can_open_managed_terminal` | `adapter.openManagedTerminal` | 仅本地 Dashboard 可调用；当前 mode 必须位于 manifest `managed_terminal_modes`，且 Plugin 对唯一原生执行实例具有独占 transport 或 PTY master，所有本地/远端写入均由同一仲裁器处理，断连后撤销过期权限 |
| `can_local_file_attachments` | `adapter.send` / `adapter.startSessionWithMessage` | 能处理 Hub 已 materialize 的受控本地文件；不得接收 data URI、base64 或远程下载 URL |

**声明一致性强制**：manifest 的 `can_*` 与 probe 返回的 `can_*` 必须一致；声明 `true` 必须真正实现对应方法。Hub 遇到声明 true 但 RPC 返回 "not implemented" 视为 Plugin 缺陷。

附件能力不声明 Agent 原生云端上传限制。Prism 的统一传输限制固定为每条最多 10 个、单文件最大 `100 MiB`、无单条总量上限；声明 `can_local_file_attachments=false` 时 API 必须在上传前返回 `attachment_not_supported`，不得将附件降级为文本或尝试伪造。

### 5.4 `adapter.readStatus` schema

读取一个 run 的当前状态。承载 detail snapshot **不包含**的字段（`phase` / `preview` / `steps` / 完整 `context`）。detail snapshot 只给"控制台视图"，`readStatus` 给"运行过程视图"，两者互补。

**请求**（wire field snake_case）：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `session` | `NativeSession` | 目标会话 binding |
| `run_id` | `string` | 可选；不传则取当前活跃 run |

**返回** `RunStatus`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `status` | `string` | `idle` / `running` / `waiting` / `waiting_approval` / `complete` / `error` |
| `phase` | `object\|null` | `{id, label, detail, source, updated_at}`，`id` ∈ `tool` / `thinking` / `null` |
| `preview` | `string` | 当前轮最终 / 预览 / 失败文本或等待提示 |
| `steps[]` | `array` | 最近步骤，每项 `{kind, label, text, time, call_id?}` |
| `context` | `object` | `{available, used_tokens, window_tokens, remaining_tokens, percent, updated_at}` |
| `model` / `reasoning_mode` | `any` | 当前模型 / 推理级别 |
| `interruptible` / `approval_blocked` | `bool` | |
| `primary_action` | `string` | `send` / `interrupt` / `approval` / `queue` |
| `turn_id` / `started_at` / `completed_at` / `duration_ms` | `string` / `int` | 运行元数据 |

`phase` 的跨 Plugin 固定枚举尚未定稿；拿不到结构化阶段时保留空骨架并提供 `summary` / `preview`。

`primary_action=queue` 只适用于原生当前主操作本身可证明为 queue 的 Plugin。Codex 的 Composer Fiber `isQueueingEnabled` 是运行中下一条带文本消息的原生默认提交偏好：Plugin 可以据此发布 `primary_action=queue` 与 `can_queue=true`，但不得据此发布 `queue` 或 `queue_pending`。Codex 的已有排队消息仍只以 detail 中真实 `queue.items[]` 为准；没有 item 时不得伪造队列。

### 5.5 `adapter.controlSession` schema

执行一个已声明的会话级控制动作（例如有稳定原生 ID 的模型 / 推理 / 权限），返回**刷新后的 detail**。这是统一能力契约里"可写"的一半。

**请求**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `session` | `NativeSession` | 目标会话 binding |
| `action` | `string` | action key，见下表（`snake_case` 与 `.` 形式都接受）|
| `target` | `any` | 稳定语义的模型、推理、权限为 `{option_id}`；临时 surface 为 `{surface_id,action_id,input?}`；动态 Desktop menu 通过下方 `menu_session` 入口，不进入普通 control target |
| `name` | `string` | 仅 `rename` 用 |
| `metadata` | `object` | 可选 `control_scope` / `source_device` / `request_id` |

**标准 action keys**（所有声明 `can_control_session` 的 Plugin 必须支持其在 `actions[]` 中声明的子集）：

| action key | 含义 | `target` |
| --- | --- | --- |
| `conversation.select` | 切换前台会话 | thread id |
| `controls.describe` | 返回当前 controls（不改动）| —— |
| `model.switch` | 有稳定原生语义 ID 的 Plugin 切换模型 | `{option_id}` |
| `reasoning.switch` | 有稳定原生语义 ID 的 Plugin 切换推理级别 | `{option_id}` |
| `permission.switch` | 有稳定原生语义 ID 的 Plugin 切换权限 / 审批模式 | `{option_id}` |
| `menu_session.describe` | 按当前 detail 的关闭态 menu trigger 创建短时候选 session；`kind=conversation` 必须由 `conversation_menu_available=true` 声明 | `{owner:{kind:"interactive_control",control_id}|{kind:"conversation"}|{kind:"queue_item",queue_item_id}}` |
| `menu_session.apply` | 透明重开并应用当前 menu session 的唯一 entry | `{menu_session_id,entry_id}` |
| `interactive.surface.apply` | 应用当前 detail 声明的临时 Desktop dialog/form action | `{surface_id,action_id,input?}` |
| `context.compact` | 可选：压缩上下文 | 仅原生 SDK/RPC 已提供稳定 action identity 时声明；Codex Desktop 当前不声明 |
| `rename` | 可选：重命名会话 | 仅原生 SDK/RPC 已提供稳定 action identity 时声明；动态 Desktop 菜单须走 `menu_session` |
| `pin` | 可选：置顶 / 取消置顶 | 仅原生 SDK/RPC 已提供稳定 action identity 时声明；动态 Desktop 菜单须走 `menu_session` |
| `archive` | 可选：归档会话 | 仅原生 SDK/RPC 已提供稳定 action identity 时声明；动态 Desktop 菜单须走 `menu_session` |

**返回** `ControlResult`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `ok` | `bool` | |
| `action` | `string` | 回显规范化后的 action |
| `thread_id` | `string` | |
| `message` | `string` | 面向用户的操作结果文案 |
| `details` | `object` | 刷新后的完整 `detail_snapshot` |
| `details_confirmed` | `bool` | 仅当稳定 Native RPC 已权威读取 detail 时为 `true`；变更操作还必须先写入成功。Hub 可立即发布 `details`，Desktop 点击或异步 UI 操作不得设置 |

#### 5.5.1 `details_confirmed` 是逐次结果断言，不是配置项

`details_confirmed` 不得写入 manifest、`capabilities`、`PluginConfig` 或 runtime mode 配置，也不需要不同 Plugin 预先声明。`can_control_session` 只决定 Hub 能否调用 `adapter.controlSession`；`details_confirmed` 决定 Hub 能否把**本次返回的完整 `details`**立即作为该会话的新 detail 发布。字段省略或为 `false` 是安全默认值，旧 Plugin 无需迁移即可继续由 watcher 收敛。

Hub 不根据 `plugin_id`、`integration_mode`、`runtime_modes` 或当前 mode 名称推断该字段。同一个 Plugin、同一个 action 在不同 mode 下可以返回不同值；Plugin 必须根据本次实际执行路径判断：

| 本次执行路径 | `details_confirmed` | 后续收敛方式 |
| --- | --- | --- |
| 稳定 Native RPC 只读操作已校验目标 session，并返回该 session 当前完整 detail，例如 `controls.describe` | `true` | Hub 立即发布；订阅只处理后续外部变化 |
| 稳定 Native RPC 变更操作已同步返回成功，且 Plugin 能据此构造或回读操作后的完整 detail | `true` | Hub 立即发布；订阅继续校准外部变化和后续状态 |
| Desktop/CDP/辅助功能点击、系统弹窗或其他异步 UI 操作 | 省略或 `false` | 等真实 watcher 观察到 UI 后发布 `desktop.state.changed` |
| CLI 子进程、异步 IPC、fire-and-forget 命令只报告 accepted，尚未证明原生状态已改变 | 省略或 `false` | 等结构化事件、持久化回读或 watcher；不得把 accepted 当完成 |
| 只有部分字段、旧缓存、估算值，或变更 RPC 尚未成功 | 省略或 `false` | 先补齐权威完整 detail；失败则返回 error |
| 操作失败、目标过期或 session identity 无法唯一验证 | 不返回成功结果 | 返回明确 error；不得携带 `details_confirmed=true` |

多运行模式 Plugin 必须在实现内逐次选择，而不是把 mode 当全局开关：

```text
desktop mode: click real UI -> details_confirmed=false -> watcher confirms later
native mode:  call stable RPC -> verify success/full detail -> details_confirmed=true
```

设置错误的影响：

- **误设为 `true`**：Hub 会立即把陈旧或不完整 detail 与当前 canonical detail 合并后发布，可能回退当前模型/推理、错误解锁操作，或延长已失效的 run/approval/queue。这是协议正确性缺陷，不能依赖后续 watcher “大概率修正”。
- **本应为 `true` 却省略/设为 `false`**：不会污染状态，但 Mobile/Panel 会继续等待 watcher，出现延迟、超时或“等待运行时同步”。
- **`true` 但 `details` 为空**：Hub 不会发布；该返回仍视为 Plugin 实现缺陷。
- **把它做成 manifest/capability 开关**：多 mode Plugin 会让 Desktop 异步结果被错误提前确认，或让 Native 同步结果永远等待；两者都不允许。

**强制**：`controlSession` 执行成功后，Plugin 必须让远端 detail 收敛。Desktop 点击和其他异步 UI 操作通过 `subscribe` 发出 `desktop.state.changed`，其 `details_confirmed` 必须保持 `false`；稳定 Native RPC 在权威读取同一 session 后可返回 `details_confirmed=true`，变更操作还必须先写入成功。Hub 立即发布该 `details`，后续订阅只负责外部变化与再次校准。只执行不回写 detail 视为未完成。`accepted` / 本地成功不代表 UI 已完成，只有 watcher detail 或已确认的 Native RPC detail 才是确认信号。稳定语义 control 的 `{option_id}`、动态 Desktop menu session 的 `{menu_session_id,entry_id}` 与 surface 的 `{surface_id,action_id,input?}` 无法在当前真实 UI 中精确且唯一验证时都返回 `control_target_stale`；不得词典匹配、自动选择其他项或使用 fixed menu index。UI 未提供内部 ID 时可生成 opaque runtime ID；仅允许以同一次 menu session 的完整原始 DOM/ARIA/Fiber identity 和展示值做唯一等值校验，不能按名称搜索目标。

### 5.6 detail snapshot 是统一契约

`detail_snapshot` 是 Plugin 对外控制状态的**单一投影**，承载稳定原生语义的 `current_model` / `current_reasoning` / `current_permission` / `*_options`、动态桌面菜单的 `interactive_controls[]`、临时 dialog/form 的 `interactive_surface`，以及 `approval` / `queue` / `run` / `actions` / `primary_action` / `context_window_*`。稳定 SDK control、动态菜单、request-backed approval、queue 与 transient surface 是不同的协议对象；SDK 可提供通用结构与派发辅助，但不得将它们压扁为一个 action 列表。正文只由同一详情 state-stream 上的 `readHistoryStream` turn 事件承载，不能塞回 detail snapshot，也不能恢复 `messages_signature` 拉取链路。

模型、推理和权限还受当前 `runtime_controls` 强制约束：`stable` 与 `interactive` 是互斥 owner。动态控件若代表这些标准语义，必须携带 `semantic_kinds: [model|reasoning|permission, ...]`；组合菜单可以同时声明多个，例如 Codex 的 `model + reasoning`。Hub 在发布前清除非当前 owner 的字段，给 detail 写入 `runtime_mode`，runtime 切换时先撤销旧 strict-live detail 并清空本地快照。未标 `semantic_kinds` 的动态控件只视为 Plugin 自定义控件，不能借此实现标准模型/推理/权限语义。

重命名、置顶、归档和删除受当前 `runtime_operations` 强制约束：`direct` 与 `menu` 也是互斥 owner。Direct action 的 ID 固定为 `rename` / `pin` / `archive` / `delete`；菜单 entry 仍是短期 opaque identity，Hub/API 不解析 label，也不根据 `plugin_id` 或 runtime 名称猜测操作。Plugin 若把某项声明为 `menu`，必须保证本次 conversation menu 只返回当前 runtime 真实、可安全执行的条目；Hub 只负责 gating 菜单入口，不能从菜单文案反向过滤某个操作。

错误影响是确定的：同一语义发布两套表示会在 Mobile/Panel 生成重复入口；把实际动态菜单声明成 `stable` 会令菜单被 Hub 丢弃；把稳定 options 声明成 `interactive` 会清空 `current_*` / `*_options` 和标准 switch action；切换 runtime 后仍复用旧 detail 会把上一模式的 control、approval 或 queue 混入新模式。因此这不是客户端去重规则，而是 Plugin/Hub 的协议一致性要求。

- 权威字段定义在 `internal/bridge/detail_snapshot.go` 的 `canonicalDetailSnapshot`，本协议不重复列举；Plugin 按 codex 现状为基线填充。
- Plugin 不支持某项时返回统一空字段或 `null`，不另起字段名。
- 事件类型统一为 `desktop.state.changed`，envelope `{event:"plugin.event", payload:{subscription_id, event:{type, payload:{detail_snapshot}}}}`；不拆成"消息事件"和"控制事件"。
- `readStatus` 与 `detail_snapshot` 互补：前者给运行过程（`phase` / `steps`），后者给控制台视图（model / options / approval）。Hub 不要求两者同时，但远程会话完成态两者都应可达。

## 6. 开发一个新 Plugin

### 6.1 选择实现方式

选择 `protocol-native`：原生应用有稳定正式协议、gateway 或结构化 CLI。选择 `desktop-automation`：原生应用没有正式接口，但可通过 CDP、深链或辅助功能实现可靠适配。选择后，所有原生差异都收在 Plugin 内，向 Hub 暴露统一协议。

### 6.2 创建骨架

按第 2 节的 manifest 和对应 SDK 的最小 adapter 示例创建目录。仓库的 `internal/plugin/scaffold` 仅是仓库测试支持代码，不发布为产品命令。生成目录应包含 manifest、语言入口、依赖文件和 README；普通用户安装、重装和移除只通过 Prism Desktop 完成。

### 6.3 最小入口与完成态

最小入口只需实现 `ID` 和 `Probe` 并启动 SDK server；它只能证明协议连通，不是远程完成态。开发者必须继续实现 list、attach、startSessionWithMessage、read history、send、visibility、subscribe、wait run、interrupt，以及适用的 approval/control。完整工作流为：发现原生应用，列真实会话，attach 已有会话；或以首条消息启动并返回真实 native identity；发送并验证可见性，订阅 run/正文变化，读取最终正文，处理打断和审批。

### 6.4 本地联调

```sh
go test ./internal/plugin/...
git clone https://github.com/Rokid-Prism/prism-plugin-sdk.git
(cd prism-plugin-sdk && go test ./...)
```

插件的单元测试和 conformance helper 由仓库测试直接调用，不通过已安装的 sidecar 二进制执行。面向用户的安装入口是 Prism Desktop；登录绑定和远程会话流程依赖 Prism Cloud 与客户端，不由本仓库单独实现。

## 7. SDK 验收

- manifest `id`、SDK adapter 的 `id()` / `ID()` 返回值、probe 的 `plugin_id` 一致，版本协商与 wire 字段单一。
- **所有 `adapter.*` 方法通过 SDK 分发暴露，不 hand-roll stdio switch**；方法集与 5.2 一致。
- capability gate 拦截未支持 RPC；首发以 `prism_message_id` 幂等，成功后全链路保持同一个 native binding。
- manifest `can_*` 与 probe 的 `can_*` 一致；声明 true 必须真正实现。
- 每个 runtime 显式声明 `runtime_controls`；conformance 验证每个标准语义只有一个 active owner，动态标准控件携带准确的 `semantic_kinds[]`，切换 runtime 后旧 detail 不再可见。
- 每个 runtime 显式声明 `runtime_operations`；conformance 分别验证 `direct`、`menu`、`unsupported`，`pin` 使用 `{enabled:boolean}`，切换 runtime 后旧 direct action 和 menu trigger 不再可见。
- send 只有可见性验证通过才报告送达，正文只由 `readHistoryStream` 在线迭代。
- `controlSession` 成功后必须让 detail 收敛：`details_confirmed=true` 的权威 Native 结果由 Hub 立即发布；其他结果必须经 subscribe 流回写刷新的 detail。
- conformance 必须分别覆盖 `details_confirmed=false` 不立即发布、`true` 立即发布完整 detail；多 mode Plugin 还必须覆盖同一 action 在 Native 与 Desktop 路径的不同返回值。
- `details_confirmed=true` 的测试必须校验 native session/thread identity、当前 option 与操作结果一致，并覆盖旧缓存或并发观察结果不能覆盖刚确认状态的竞态。
- subscribe 事件驱动 detail/index/run 与 history relay；正文仅由 `readHistoryStream` 的 turn 事件承载，不能写入 Redis snapshot。
- `subscribePlugin` 的订阅确认先于事件输出；stdout 不可写时，对应 session/plugin/history pump 必须停止并清理，不能继续消费原生事件。
- `controlSession` / `controlDraft` 的 `target` 与 `metadata` 是 Plugin 自有的 opaque wire object：SDK 只校验外层 v4 snake_case，不得递归改写其内部 key、补字段或猜测语义。
- Go、Node、Python 对同一行为输出等价 wire payload 和错误语义；SDK 仓库的 `conformance/` fixtures 覆盖 opaque target、Plugin-wide watch 与 history cancel。

## 8. SDK 缺口与演进

本节记录截至当前的 SDK 与契约差距，作为后续实施的路线标。**本节内容随 SDK 补齐而收缩，不作为长期规范。**

### 8.1 SDK 当前能力

Go、Node、Python SDK 均已实现 `adapter.readDetail`、`adapter.readStatus`、`adapter.controlSession`、`adapter.openManagedTerminal`、`adapter.subscribePlugin` 及对应的 capability。三套 SDK 的 history stream error 都支持 `retryable`。三套 SDK 都在 stdio 边界强制 v4 `snake_case`：任何 PascalCase request 或 response payload 都是协议错误，不存在兼容分支。普通 schema 会转换为各语言的本地字段名；`target` / `metadata` 这类 opaque 扩展对象必须原样交给 Plugin。

尚未完成的是 Plugin 迁移，而不是 SDK 契约：使用 SDK 的 Plugin 自动获得标准分发；保留手写 dispatcher 的旧 Plugin 必须在迁移前自行遵守完全相同的 v4 wire 规则。

### 8.2 Plugin 演进路线

| Plugin | 现状 | 目标 |
| --- | --- | --- |
| `codex` | Node，已迁移 Node SDK 标准分发 | 继续完善产品适配和验收 |
| `hermes` | Node，使用 Node SDK 标准分发；Gateway JSON-RPC + SQLite durable mirror | 继续按 `docs/05` §2.4 完成跨平台与 Desktop-originated 验收 |
| `openclaw` | Go，使用 `github.com/Rokid-Prism/prism-plugin-sdk`；已实现 `readDetail`、streaming history 和 native controls | 继续补真实危险操作 approval 样本与跨平台验收 |

演进顺序：保持本节契约稳定 → 旧 Plugin 迁回 SDK → 删除 hand-roll。Hermes 已完成 Node SDK 迁移；OpenClaw 可按既定 schema 继续实现读取、审批和会话控制，不需要等待 SDK 再变更。
