# Jessica 设计文档

> 本文档解释 Jessica 的总体架构、核心契约，以及已完成 Task 的设计决策。
>
> - `CLAUDE.md`：跨会话约束（做什么、不做什么）。
> - `PLAN.md`：任务清单与进度。
> - `DESIGN.md`：为什么这样设计，以及每个已完成 Task 的实现记录。

最后更新：2026-11-11
已完成：Task 0、Task 0.1、Task 0.2、Task 1.1、Task 1.1b、Task 1.2、Task 1.3、Task 2.1、Task 2.2、Task 2.3、Task 2.4、Task 3.1、Task 3.2、Task 3.3、Task 4.1、Task 4.2、Task 4.3
当前测试基线：`cargo test --workspace` → 221 passed（核心契约 30 个、Provider 57 个、Runtime 17 个、Tools 34 个、Harness 24 个、CLI 36 个、Sandbox 17 个、Memory 6 个）

---

## 1. 设计目标与信条

**定位**：Jessica 是一个确定性 Rust Agent 运行时，WASM 沙箱扩展，事件溯源持久化。

**信条**：不信模型自述，只信可验证轨迹。

由此派生五条设计原则：

1. **契约先行（contract-first）**
   `jessica-core` 只定义类型与 Trait，不包含任何实现层概念（WASM、SQLite、HTTP）。所有实现 crate 依赖 core，core 不依赖任何内部 crate。

2. **确定性（determinism）**
   相同输入与相同事件流必须产生相同输出。实现中避免 HashMap 的随机迭代顺序、避免在重放路径使用当前时间/随机数/网络。例如 `ToolRegistry` 与工具调用拼接都使用 `BTreeMap`。

3. **可验证（verifiable）**
   会话结果由 Sensor、AcceptanceCheck、命令退出码等外部证据判定，而不是模型自述“我完成了”。

4. **最小权限（least privilege）**
   默认拒绝：`DenyAll` 是 Policy 的默认实现；后续 WASM 沙箱默认无文件预打开、无网络。

5. **结构化可观测（observable by construction）**
   循环中的每个状态变化都以可序列化的 `AgentEvent` 表达；事件是事实来源，日志与 UI 只是消费者。事件模型为 Phase 4 的事件溯源与离线重放打基础。

**非目标**：不是通用聊天框架；不追求在 core 中兼容多家 Provider 的差异；不在守卫中调用 LLM。

---

## 2. 总体架构

### 2.1 分层与依赖方向

```text
┌──────────────────────────────────────────────────────────────┐
│ jessica-cli                                                  │
│ 参数解析 / REPL / 装配（Provider、Tools、Harness、Memory）    │
└───────────────┬──────────────────────────────────────────────┘
                │ 依赖所有实现层
┌───────────────▼──────────────────────────────────────────────┐
│ 实现层（只依赖 jessica-core）                                 │
│                                                              │
│  jessica-runtime   Agent 循环、会话恢复                       │
│  jessica-harness   Middleware / Guard / Policy / Sensor       │
│  jessica-providers OpenAI / Anthropic 流式 Provider           │
│  jessica-tools     原生工具、WASM 工具注册                    │
│  jessica-sandbox   wasmtime 沙箱、WASI 能力控制               │
│  jessica-memory    事件存储、分层记忆                         │
└───────────────┬──────────────────────────────────────────────┘
                │ 单向依赖
┌───────────────▼──────────────────────────────────────────────┐
│ jessica-core（冻结契约层）                                    │
│  types / llm / tool / policy / guard / sensor / hook /        │
│  acceptance / memory                                          │
│  —— 不依赖任何内部 crate，不含 WASM/SQLite/HTTP 概念          │
└──────────────────────────────────────────────────────────────┘
```

| Crate | 角色 | 允许的内部依赖 | 当前状态 |
| --- | --- | --- | --- |
| `jessica-core` | 公共类型与 Trait 契约（冻结层） | 无 | ✅ Task 0.2 |
| `jessica-runtime` | Agent 循环、会话恢复 | `jessica-core` | ✅ Task 1.2（循环）、4.2（事件流恢复与续跑） |
| `jessica-sandbox` | wasmtime 沙箱与 WASI 能力控制 | `jessica-core` | ✅ Task 3.2（加载/实例化/调用、WASI 只读能力、资源限制）、3.3（buffer ABI） |
| `jessica-harness` | Middleware、Guard、Sensor、Acceptance | `jessica-core` | ✅ Task 2.1（Hook）、2.2（Guards/Policy）、2.3（Sensors/Acceptance） |
| `jessica-providers` | OpenAI / Anthropic 兼容 Provider | `jessica-core` | ✅ Task 1.1、1.1b |
| `jessica-tools` | 原生工具与 WASM 工具注册 | `jessica-core`、`jessica-sandbox`（Task 3.3 起） | ✅ Task 1.3（原生工具）、3.3（WasmTool 适配器与混合注册） |
| `jessica-memory` | 事件存储与记忆 | `jessica-core` | ✅ Task 4.1（SQLite EventStore）、4.2（sessions 元数据；MemoryStore 待 Phase 5） |
| `jessica-cli` | 可执行入口与装配 | 所有实现层 | ✅ Task 1.3（基础 CLI）、2.4（完整 harness 装配）、4.2（事件持久化与 `--resume`）、4.3（离线确定性 `replay`） |

**为什么要单向**：core 是被所有 Phase 共享的稳定边界。实现 crate 可以替换（例如换 Provider、换存储），而 core 的类型与 Trait 不变。运行时只持有 `Arc<dyn LlmProvider>`、`Arc<ToolRegistry>` 等 trait object，不直接依赖具体实现 crate。

### 2.2 运行时数据流（目标形态）

```text
用户输入
   │
   ▼
┌─────────────┐    messages     ┌──────────────────┐
│  AgentLoop  │ ──────────────▶ │  LlmProvider     │──HTTP/SSE──▶ 模型
│ (runtime)   │ ◀────────────── │ (providers)      │
└─────┬───────┘   LlmEvent      └──────────────────┘
      │
      │ ToolCall
      ▼
┌─────────────┐   execute       ┌──────────────────┐
│ ToolRegistry│ ──────────────▶ │ native / WasmTool│
│  (core)     │ ◀────────────── │ (tools/sandbox)  │
└─────────────┘   ToolResult    └──────────────────┘
      │
      │ AgentEvent（SessionStarted / ModelDelta / ToolCallStarted /
      │             ToolCallFinished / PolicyDenied / BudgetExceeded /
      │             AcceptanceFailed / Error / SessionEnded）
      ▼
┌──────────────────────────────────────────────────┐
│ broadcast::Sender<AgentEvent>                    │
│   ├─▶ CLI / 日志消费者                            │
│   ├─▶ Harness Hooks（观察与治理）                  │
│   └─▶ EventStore（Phase 4，append-only 持久化）    │
└──────────────────────────────────────────────────┘
```

治理逻辑（Phase 2）不内联进 `AgentLoop`：Guard 在固定检查点读取 `GuardContext` 并做出确定性判断；Policy 在执行工具前检查 `ToolCall`；Sensor 在工具执行后观察结果；AcceptanceCheck 在模型停止后运行外部验收。Task 2.4 的实际形态是 CLI 侧的 `TokenCountingProvider` / `GovernedTool` 包装 + `EventPump` 检查点 + `AcceptanceRunner`，见 §3.10。

### 2.3 事件驱动生命周期

```text
SessionStarted
   │
   ├──▶ [round] ModelDelta* ──▶ ToolCallRequested* ──▶ ToolCallStarted*
   │                                                ──▶ ToolCallFinished*
   │                                                ──▶ Sensor / Guard 检查点
   │
   ├──▶ 无工具调用且 AcceptanceCheck 通过 ──▶ SessionEnded { Completed }
   ├──▶ 预算/守卫触发 ────────────────────▶ BudgetExceeded
   │                                       └▶ SessionEnded { BudgetExceeded }
   ├──▶ Policy 拒绝单次调用 ───────────────▶ PolicyDenied + 错误 ToolResult（会话继续）
   ├──▶ 验收失败且重试耗尽 ────────────────▶ AcceptanceFailed* ──▶ SessionEnded { AcceptanceFailed }
   └──▶ Provider/运行时错误 ───────────────▶ Error ──▶ SessionEnded { Error }
```

---

## 3. 已完成 Task 的设计记录

### 3.1 Task 0：项目宪法（CLAUDE.md）

**目标**：建立跨会话的持久约定，避免每次会话重新解释架构与纪律。

**实现**：根目录 `CLAUDE.md`，包含：

- 项目定位与信条；
- Workspace 8 个 crate 的职责与依赖单向规则；
- 编码纪律：公共 API 必须有 doc comment；`thiserror` 结构化错误；禁止 `unwrap`/`expect`（测试除外）；Tokio 异步、禁止阻塞调用混入 async；
- 禁止事项：未批准第三方 crate、提前实现后续 Phase、擅改冻结的 core 公共 API、范围外重构；
- 验收命令速查与 PLAN.md 进度更新要求；
- Claude Code 执行纪律（一次会话一个 Task、失败不得声明完成、行为测试等）。

**决策**：把“禁止事项”显式写死，是因为实现型 Agent 容易过度发挥（提前实现、顺手重构、自证式测试）。CLAUDE.md 是跨会话记忆载体。

**验收**：文件存在；后续会话在 Task 0.1/0.2/1.1 中均先引用了其约束（依赖方向、冻结 API、验收命令）。

---

### 3.2 Task 0.1：Workspace 骨架

**目标**：建立可编译、依赖方向可验证的 8-crate workspace。

**实现**：

- 根 `Cargo.toml`：`resolver = "2"`，8 个成员；
- 每个实现 crate 声明指向 `jessica-core` 的 path 依赖；`jessica-cli` 依赖全部 7 个实现层；
- `rust-toolchain.toml` 固定 Rust **1.99.1**，含 `rustfmt`、`clippy`；
- `.gitignore` 忽略 `/target/`；
- `[workspace.dependencies]` 集中管理版本：`tokio`、`serde`、`serde_json`、`thiserror`、`tracing`、`async-trait`、`futures`（Task 1.1 追加 `reqwest`）。

**验收**：

```text
cargo build --workspace                              ✅
cargo tree -p jessica-core --depth 1                 ✅ 仅自身
cargo tree -p jessica-runtime --depth 1              ✅ 仅 jessica-core
cargo fmt --check / clippy / test --workspace        ✅
```

**偏离**：环境原先无 Rust，直连 `static.rust-lang.org` 极慢，改用 rsproxy.cn 镜像安装并固定 1.99.1；`cargo new` 使用 `--vcs none` 避免嵌套 `.git`。这些属于环境操作，不写入仓库。

---

### 3.3 Task 0.2：jessica-core 冻结契约

**目标**：定义 Phase 1–5 共享的类型与 Trait，之后只允许通过单独 Task 修改。

**模块布局**：

| 文件 | 内容 |
| --- | --- |
| `jessica-core/src/types.rs` | `SessionId`、`Role`、`Message`、`ToolCall`、`ToolResult`、`Budget`、`BudgetKind`、`SessionEndReason`、`TokenUsage`、`AgentEvent` |
| `jessica-core/src/llm.rs` | `LlmProvider`、`LlmEvent`、`LlmStream`、`LlmError` |
| `jessica-core/src/tool.rs` | `Tool`、`ToolSchema`、`ToolRegistry`、`ToolRegistryError` |
| `jessica-core/src/policy.rs` | `Policy`、`PolicyDecision`、`DenyAll` |
| `jessica-core/src/guard.rs` | `Guard`、`GuardContext`、`GuardDecision` |
| `jessica-core/src/sensor.rs` | `Sensor`、`SensorContext`、`SensorResult` |
| `jessica-core/src/hook.rs` | `Hook` |
| `jessica-core/src/acceptance.rs` | `AcceptanceCheck`、`AcceptanceContext`、`AcceptanceResult` |
| `jessica-core/src/memory.rs` | `MemoryStore`、`MemoryEntry`、`MemoryError` |

#### 3.3.1 消息与工具类型

```rust
pub struct Message {
    pub role: Role,                 // System / User / Assistant / Tool
    pub content: String,
    pub tool_calls: Vec<ToolCall>,  // 仅 assistant 使用
    pub tool_call_id: Option<String>, // 仅 tool 使用
}

pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

pub struct ToolResult {
    pub tool_call_id: String,
    pub content: String,
    pub is_error: bool,
}
```

**关键决策**：

- **增加 `Message.tool_call_id`**：OpenAI 的 `role = "tool"` 消息必须携带 `tool_call_id` 才能把结果关联回 assistant 的工具调用。计划文本只列了 role/content/tool_calls，但缺少该字段会使 Phase 1.2 无法构造合法的回填消息。
- **`ToolResult` 用 `is_error` 而非 `Result`**：工具失败是模型可观察、可恢复的信息，不是运行时错误。运行时把失败结果回填给模型，让模型自行纠正。
- **`ToolCall.arguments` 使用 `serde_json::Value`**：Provider 负责把流式字符串拼接并解析为 JSON；Tool 直接消费结构化参数。
- `serde` 序列化时跳过空 `tool_calls` 与 `None` 的 `tool_call_id`，保持消息 JSON 简洁。

#### 3.3.2 事件模型

```rust
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    SessionStarted { session_id },
    ModelDelta { text },
    ToolCallStarted { call },
    ToolCallFinished { result },
    PolicyDenied { call, reason },
    BudgetExceeded { kind, message },
    AcceptanceFailed { attempt, feedback },
    Error { message },
    SessionEnded { session_id, reason },
}
```

**关键决策**：

- **所有变体可序列化**：Phase 4 的事件存储要求 `AgentEvent` 每个变体都能写入 append-only 表并原样读回；单元测试对每个变体做 JSON 往返。
- **内部标签 `type`**：事件流（JSONL）消费方可以不解析 payload 就按 `type` 分发。
- **补充 `Error`**：Task 1.2 明确要求 Provider 错误转为 `AgentEvent::Error` 并终止。
- **补充 `PolicyDenied`**：Phase 2 的 AllowlistPolicy 拒绝工具时需要一个结构化、可观测的事件，而不是复用 `BudgetExceeded` 或错误。
- **`SessionEnded.reason`**：区分正常完成、取消、预算超限、验收失败、错误，便于 Phase 4 重放与会话恢复判定终态。
- **`BudgetKind`**：与 Phase 2 的四种 Guard 对齐（TokenLimit / ToolTimeout / MaxRounds / Stuck / ResultSize）。

#### 3.3.3 LLM 契约

```rust
pub type LlmStream = BoxStream<'static, LlmEvent>;

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError>;
}
```

- `BoxStream<'static, _>`：流拥有自己的数据，可脱离 `stream_chat` 的借用存活；这是运行时消费流的前提。
- `#[async_trait]`：让 `Arc<dyn LlmProvider>` 保持 dyn-compatible（原生 async fn in trait 不能直接用于 dyn）。
- `LlmEvent::Done { usage: Option<TokenUsage> }`：TokenBudgetGuard 需要 token 计数；Provider 在支持时上报 usage，缺失时为 `None`。
- `LlmError` 分为 `Request` / `Stream` / `InvalidResponse`，不包含 HTTP 概念，Provider 负责把具体错误映射进来。

#### 3.3.4 Tool 与 ToolRegistry

```rust
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> ToolSchema;          // OpenAI function tool 对象
    async fn execute(&self, args: Value) -> ToolResult;
}
```

- `ToolSchema` 是 `serde_json::Value` 的别名，约定为完整的 OpenAI tool 对象（`{"type":"function","function":{...}}`），Provider 可直接放入请求的 `tools` 数组。
- `ToolRegistry` 使用 `BTreeMap<String, Arc<dyn Tool>>`：
  - 迭代顺序按名称排序，保证每次运行呈现给模型的工具顺序一致（确定性）；
  - 重复注册返回 `ToolRegistryError::Duplicate`，不静默覆盖；
  - 查找返回 `Arc<dyn Tool>`，便于在 await 期间持有工具。

#### 3.3.5 治理契约

| Trait | 方法 | 设计要点 |
| --- | --- | --- |
| `Policy` | `check(&ToolCall) -> PolicyDecision` | 同步、确定性；默认 `DenyAll` 保证最小权限 |
| `Guard` | `check(&GuardContext) -> GuardDecision` | 单一检查点快照，同时服务 Phase 2 的 Token/Timeout/Stuck/ResultSize 四种 Guard |
| `Sensor` | `observe(&SensorContext) -> SensorResult` | 工具执行后运行，返回 pass/fail + output |
| `Hook` | `on_session_start` / `on_event` / `on_session_end` | 全部默认 no-op，只覆写关心的生命周期点 |
| `AcceptanceCheck` | `check(&AcceptanceContext) -> AcceptanceResult` | 模型停止后运行；失败反馈将注入为新 message 并重入循环 |
| `MemoryStore` | `write` / `recall` | 会话作用域；`recall` 带 `query` 与 `limit`，为 Phase 5 的 FTS/向量检索预留 |

**Guard 为什么用 `GuardContext` 而不是多个回调**：
四种 Guard 需要的输入不同（轮次、token、输出、工具结果、耗时）。`GuardContext` 把这些可观察量集中成快照，Guard 只读快照、不做 I/O、不调用 LLM，从结构上保证“确定性判断”。`GuardDecision::Exceeded { kind, reason }` 直接映射到 `AgentEvent::BudgetExceeded`。

#### 3.3.6 冻结与兼容策略

- `jessica-core` 开启 `#![deny(missing_docs)]`，所有公共项有 doc comment。
- 公共 API 视为冻结；后续 Task 若必须修改，先询问用户并单独开 Task。
- 新增事件变体属于破坏性变更（下游 exhaustive match），因此 Phase 2 所需的 `PolicyDenied`、Phase 1 所需的 `Error` 在 Task 0.2 一次性定义。

#### 3.3.7 测试

30 个单元/行为测试，覆盖：

- 所有可序列化类型/枚举变体的 JSON 往返与 `type` 标签；
- 消息的可选字段省略、assistant tool_calls、tool 消息的 call id；
- `Duration` 的 Budget 往返；
- `ToolRegistry` 注册/查找/重复检测/确定性顺序/dyn 派发；
- `DenyAll` 拒绝行为；
- `LlmProvider`、`Sensor`、`AcceptanceCheck`、`Hook`、`MemoryStore` 的 mock 行为测试；
- `Missing docs`、clippy `-D warnings` 全绿。

---

### 3.4 Task 1.1：OpenAI 兼容 Provider

**目标**：在 `jessica-providers` 中实现 OpenAI chat-completions 流式协议，不依赖真实网络即可测试。

**模块布局**：

| 文件 | 职责 |
| --- | --- |
| `config.rs` | `OpenAiConfig`、`ApiConfigError`、环境变量加载、Debug 脱敏 |
| `request.rs` | `build_chat_request`：Message → OpenAI JSON，tools 组装 |
| `sse.rs` | `SseDecoder` + `parse_sse_line`：增量 SSE 解码 |
| `stream.rs` | `event_stream`、`StreamState`、`ToolCallAssembler`、响应 chunk 结构 |
| `provider.rs` | `OpenAiProvider`：reqwest 请求、Bearer 鉴权、HTTP 错误映射 |

#### 3.4.1 配置

| 环境变量 | 必需 | 默认值 |
| --- | --- | --- |
| `OPENAI_API_KEY` | 是 | — |
| `OPENAI_BASE_URL` | 否 | `https://api.openai.com/v1` |
| `OPENAI_MODEL` | 否 | `gpt-4o-mini` |

- API key 只从环境读取，不硬编码；`Debug` 输出固定为 `<redacted>`，避免日志泄漏。
- base URL 会 trim 空白与尾部 `/`；必须是 `http://` 或 `https://` 绝对 URL。
- `OpenAiConfig::new` 允许显式传参（测试、CLI 自定义配置）；`from_env` 负责环境加载。

#### 3.4.2 请求构造

请求体：

```json
{
  "model": "...",
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "", "tool_calls": [
      {"id": "call_1", "type": "function",
       "function": {"name": "read_file", "arguments": "{\"path\":\"Cargo.toml\"}"}}
    ]},
    {"role": "tool", "tool_call_id": "call_1", "content": "..."}
  ],
  "stream": true,
  "stream_options": {"include_usage": true},
  "tools": [ ... ]
}
```

- assistant 的 `ToolCall.arguments` 是结构化 `Value`，请求中必须序列化为 **JSON 字符串**（OpenAI 协议要求）。
- `role = "tool"` 的消息缺少 `tool_call_id` 时直接返回 `LlmError::Request`，不发送非法请求。
- `tools` 为空时省略该字段。
- `stream_options.include_usage` 用于在流末尾拿到 token usage，填充 `LlmEvent::Done.usage`。

#### 3.4.3 SSE 解码

OpenAI 流是 `text/event-stream`，每个事件通常是一行 `data: {json}`，以空行分隔，最后发送 `data: [DONE]`。

`SseDecoder` 的设计：

- 按 **字节** 缓冲（`Vec<u8>`），只在遇到 `\n` 时解析完整行；因此网络 chunk 即使从多字节 UTF-8 字符中间切断也不会损坏文本。
- 去掉行尾 `\r`（兼容 CRLF）。
- 忽略空行、注释行（`:` 开头）、`event:` / `id:` 等非 `data:` 行。
- 去掉 `data:` 后的一个可选空格（SSE 规范）。
- `finish()` 处理流结束时没有换行符的尾行。
- 非法 UTF-8 返回 `LlmError::Stream`。

#### 3.4.4 流状态机与工具调用拼接

```text
byte chunk
   │ SseDecoder
   ▼
data payload ──[DONE]──▶ emit ToolCallRequested* → Done { usage }
   │
   ├─ 解析 ChatChunk（serde default 容错）
   │    ├─ delta.content ──────────────▶ LlmEvent::TextDelta
   │    ├─ delta.tool_calls ───────────▶ ToolCallAssembler.apply
   │    └─ usage ──────────────────────▶ 暂存，供 Done 使用
   │
   ├─ finish_reason == "tool_calls" ───▶ emit ToolCallRequested*
   └─ 流意外结束（无 [DONE]）──────────▶ 先 flush 工具调用，再 emit Done
```

`ToolCallAssembler`：

- `BTreeMap<usize, PartialToolCall>` 按 OpenAI 的 `index` 聚合，输出顺序确定。
- 同一工具调用的 `id` / `function.name` 通常只在第一个 delta 出现；`arguments` 是字符串分片，按到达顺序拼接。
- 拼接完成后：
  - 缺少 `id` 或 `name` → `LlmError::InvalidResponse`；
  - `arguments` 解析失败 → `LlmError::InvalidResponse`；
  - 空 arguments 视为 `{}`。

**错误策略**：

- 请求阶段失败（网络、非 2xx）→ `stream_chat` 返回 `Err(LlmError)`。
- 流中途失败（传输错误、非法 JSON、工具调用不完整）→ 发出 `LlmEvent::Error` 并结束流，不 panic。
- 流正常结束但缺少 `[DONE]` → 视为正常完成并补发 `Done`（部分兼容实现不发 `[DONE]`）。

#### 3.4.5 HTTP 装配

- `reqwest::Client`，rustls TLS；请求带 `Authorization: Bearer <key>` 与 `Accept: text/event-stream`。
- 非 2xx 响应读取 body，映射为 `LlmError::Request { message: "provider returned HTTP {status}: {body}" }`。
- **未实现**重试、熔断、速率限制（Phase 5），也不在此处嵌入治理逻辑。

#### 3.4.6 测试

26 个测试，全部不依赖真实网络：

- 请求体：模型/messages/tools/stream_options 结构、assistant tool_calls 的 arguments 字符串化、tool 消息 call id、空 tools 省略、缺少 call id 报错；
- SSE：多行、分片、CRLF、多字节 UTF-8 跨界、尾行 flush、非法 UTF-8；
- 流管线（mock SSE 流）：
  - 文本增量 + Done；
  - 多个 tool call delta 拼成完整 `ToolCall`（arguments 分三段）；
  - 多工具按 index 排序；
  - usage 上报；
  - 非法 JSON / 缺 id → Error 事件；
  - 无 `[DONE]` 的正常结束；
  - 传输错误 → Error 事件；
  - 按 5/7 字节切分 SSE 的跨 chunk 测试。
- 配置：校验、URL 规整、Debug 脱敏、错误信息。

---

### 3.4b Task 1.1b：Anthropic 兼容 Provider（jessica-providers）

**目标**：新增与 `OpenAiProvider` 对称的 `AnthropicProvider`，把 Anthropic Messages API 的三类协议差异全部吸收在 Provider 内部，对 `jessica-core` 的 `Message` / `LlmEvent` / `ToolCall` 契约零改动。

**模块布局**：

| 文件 | 职责 |
| --- | --- |
| `src/anthropic/config.rs` | `AnthropicConfig`、默认 base URL / model、`ANTHROPIC_*` 环境变量、Debug 脱敏 |
| `src/anthropic/request.rs` | `build_anthropic_request`、system 抽取、block 映射、tool schema 解包 |
| `src/anthropic/sse.rs` | `parse_anthropic_sse_line`、字节级 `AnthropicSseDecoder`、强类型 `AnthropicSseEvent` |
| `src/anthropic/stream.rs` | `AnthropicStreamAssembler`、按 content-block index 聚合、`anthropic_event_stream` |
| `src/anthropic/provider.rs` | `AnthropicProvider`、HTTP 装配、错误事件映射 |
| `tests/fixtures/anthropic_sse_text_and_tool.txt` | 完整 mock 流（text + tool_use + usage） |
| `tests/fixtures/anthropic_sse_error.txt` | `event: error` mock 流 |

**请求映射**：

- 所有 `Role::System` 消息从 `messages` 中移出，按原顺序以 `\n\n` 合并到顶层 `system` 字段；空系统消息忽略。
- 消息 content 统一转为 block 数组：
  - user → `[{type: "text", text}]`；
  - assistant → 非空文本生成 `text` block，每个 `ToolCall` 生成 `{type: "tool_use", id, name, input}`；
  - tool → `{type: "tool_result", tool_use_id, content}`，连续的 tool 消息合并到同一个 user 消息的多个 block 中；缺少 `tool_call_id` 报 `LlmError::Request`。
- 工具 schema 同时接受 OpenAI 包装（`{type:"function", function:{name, description, parameters}}`）与直接形式（`{name, description, parameters|input_schema}`），输出 `{name, description?, input_schema}`；缺少 name 报 `LlmError::Request`。
- 请求体固定 `stream: true` 与 `max_tokens: 8192`（Messages API 必填；本 Task 未暴露配置项）；无工具时省略 `tools`。

**SSE 解码**：

- `parse_anthropic_sse_line` 识别 `event:`、`data:`、注释与其他字段；`AnthropicSseDecoder` 在字节层缓冲，按换行切行、空行结束一帧，支持 `\r\n`、跨 chunk 多字节 UTF-8 与 EOF 尾帧。
- 每帧将 data JSON 反序列化为强类型 `AnthropicSseEvent`（`MessageStart` / `ContentBlockStart` / `ContentBlockDelta` / `ContentBlockStop` / `MessageDelta` / `MessageStop` / `Ping` / `Error`）；若声明了 `event:` 名称，还要求与 payload 的 `type` 一致，否则报 `LlmError::Stream`。
- `content_block` 与 delta 使用 internally-tagged enum；未知 block/delta 类型落到 `Other` 并被忽略（为 thinking/image 等未来扩展留出空间）。

**流式聚合**：

- `AnthropicStreamAssembler` 用 `BTreeMap<usize, BlockState>` 以 content-block index 维护状态：
  - text block 累积文本，并在每个 `text_delta` 立即发出 `LlmEvent::TextDelta`；
  - tool_use block 在 start 时记录 id/name/初始 input，`input_json_delta.partial_json` 按到达顺序拼接；到 `content_block_stop` 时用 `serde_json::from_str` 得到完整参数；空片段回退到 start 时的 input（通常为 `{}`）；
- `message_start` 的 `input_tokens` 与 `message_delta` 的 `output_tokens` 合并成一个 `TokenUsage`；`message_stop` 发出 `Done`。
- `event: error` 发出 `LlmEvent::Error` 并结束；工具参数 JSON 损坏、未知 block index 的 delta、流在 `message_stop` 前结束同样发 `LlmEvent::Error`；`ping` 忽略。

**HTTP 与配置**：

- `AnthropicProvider` 使用 reqwest，请求头 `x-api-key` 与 `anthropic-version: 2023-06-01`，`Accept: text/event-stream`，body 走 `.json`。
- `messages_url` 兼容 base URL 带/不带 `/v1`，统一解析为 `.../v1/messages`；允许第三方 Anthropic 兼容网关。
- `ANTHROPIC_API_KEY` 必需；`ANTHROPIC_BASE_URL` 默认 `https://api.anthropic.com`；`ANTHROPIC_MODEL` 默认 `claude-sonnet-5-5`，常量集中于 config 便于替换；Debug 输出脱敏 API key。
- 错误映射：HTTP 非 2xx 读取响应体后返回只含 `LlmEvent::Error` 的流；SSE/JSON 解析失败与 `event: error` 同样转 Error 事件；配置与网络发送失败返回 `Err(LlmError::Request)`（与 OpenAI Provider 一致）。未实现重试/熔断/限流。

**测试（31 个新增）**：

- 请求：system 抽取与合并、tool_use/tool_result 往返、连续 tool_result 合并、OpenAI schema 解包、空 tools 省略、缺失 call id/name 报错。
- SSE：行分类、fixture 完整解码、逐字节切分含中文、多字节字符、非法 JSON、event 名与 type 不匹配、error 事件。
- 聚合：单文本 block、单工具调用多 delta、文本与工具交错、多工具并行、usage 合并、非法工具 JSON、缺 `message_stop`、ping。
- Provider：base URL 变体、配置错误包装、loopback mock HTTP 完整 SSE 流、HTTP 500 → `LlmEvent::Error`。上述测试全部不依赖真实网络。
- 真实端点冒烟（默认 `#[ignore]`）：`tests/anthropic_live.rs` 读取 `ANTHROPIC_API_KEY/BASE_URL/MODEL`，验证真实文本流、OpenAI 风格 schema → Anthropic 工具定义、流式工具参数聚合，以及回填 tool_result 后的第二轮文本回答；运行方式 `cargo test -p jessica-providers --test anthropic_live -- --ignored`。2026-11-11 对本机配置的 Anthropic 兼容端点实测 2 passed。

**结果与限制**：`cargo test -p jessica-providers` → 57 passed（新增 31，另有 2 个 live 测试默认 ignored）；workspace → 221 passed；未修改 `jessica-core`。Task 1.1b 本身只提供库级 Provider；后续应真实 hello world 运行请求，CLI 增加了 `JESSICA_PROVIDER` 选择（`openai` 默认 / `anthropic`），两种 Provider 均可在 CLI 装配，见 §5.6。Prompt caching（`cache_control`）、thinking block 与 image block 未实现，已记入 PLAN.md Deferred。

---

### 3.5 Task 1.2：Agent 循环（jessica-runtime）

**目标**：把 Provider 与 Tool 串成可取消、可观测的确定性循环，且不内联 Phase 2 治理逻辑。

**模块布局**：

| 文件 | 职责 |
| --- | --- |
| `jessica-runtime/src/lib.rs` | crate 文档、`#![deny(missing_docs)]`、导出 `AgentLoop` |
| `jessica-runtime/src/agent.rs` | `AgentLoop`、单轮模型消费、工具执行、事件发射 |
| `jessica-runtime/tests/agent_loop.rs` | 11 个行为测试（mock provider + mock tool + broadcast） |

**公共 API**：

```rust
pub struct AgentLoop { /* provider, tools, budget, events */ }

impl AgentLoop {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        tools: Arc<ToolRegistry>,
        budget: Budget,
        events: broadcast::Sender<AgentEvent>,
    ) -> Self;

    pub async fn run(
        &self,
        session_id: SessionId,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> SessionEndReason;
}
```

- `run` 返回终止原因，同时保证该原因已作为 `SessionEnded` 事件发出；错误详情在 `AgentEvent::Error` 中，不重复构造返回错误。
- 循环不持有会话可变状态：`messages` 在 `run` 内部增长，`AgentLoop` 可被多个会话复用。
- 广播发送失败（无订阅者）被有意忽略，循环不依赖观察者存在。

**循环语义**：

```text
emit SessionStarted
for round in 0..budget.max_rounds:
    if cancelled -> SessionEnded(Cancelled)
    stream_chat(messages, tools)
    for each LlmEvent:
        TextDelta       -> emit ModelDelta + accumulate assistant content
        ToolCallRequested -> collect call
        Done            -> end turn
        Error           -> emit Error + SessionEnded(Error)
    append assistant message (content + tool_calls)
    if no tool calls -> SessionEnded(Completed)
    for each call:
        emit ToolCallStarted
        execute tool (cancellable) or build "unknown tool" error result
        stamp result.tool_call_id = call.id
        emit ToolCallFinished
        append tool message
emit BudgetExceeded(MaxRounds)
emit SessionEnded(BudgetExceeded)
```

**关键决策**：

- **只强制 `max_rounds`**：Phase 2 已规划 TokenBudgetGuard / ToolTimeoutGuard / StuckDetectionGuard / ResultSizeGuard，Task 1.2 如果内联这些逻辑会与中间件重复。`max_rounds` 是循环结构的边界，不属于“治理判断”，因此在 runtime 内实现。
- **最后一轮仍执行工具**：计划规定“有工具调用则逐个执行 → 回填 → 重复，直到超限”。当最后一轮返回工具调用时，循环先执行并记录工具结果，然后因没有下一轮模型调用而终止为 `BudgetExceeded`。这个语义在 `AgentLoop` 的 rustdoc 中明确写出。
- **`ToolResult.tool_call_id` 由循环盖章**：`Tool::execute(&self, args)` 只能拿到参数，工具本身不知道调用 id。因此循环在发射 `ToolCallFinished` 与回填 `Message::tool` 前，用原始 `ToolCall.id` 覆盖工具返回值中的 id。测试中 mock tool 故意返回 `"tool-provided-id"`，断言最终输出为 `"call-1"`。
- **未知工具不终止会话**：返回 `ToolResult { is_error: true, content: "unknown tool ..." }`，让模型有机会改用已注册工具；发射的 `ToolCallFinished` 也带 `is_error`。
- **工具错误前缀**：`Message` 没有结构化错误位，回填给模型时把失败结果写成 `"tool error: {content}"`；`AgentEvent::ToolCallFinished.result` 保留原始 `is_error`。
- **取消的优先级与检查点**：`tokio::select!` 使用 `biased;`，取消分支优先。检查点覆盖（1）每次模型调用前、（2）等待下一个 `LlmEvent` 时、（3）工具执行中。工具被取消时不发 `ToolCallFinished`，直接 `SessionEnded(Cancelled)`。
- **流缺少 `Done` 视为错误**：`LlmEvent` 契约以 `Done` 结束；流提前结束会产生 `AgentEvent::Error` 并终止，避免把截断响应误判为完成。
- **不使用 `unwrap` 处理 Provider 错误**：`stream_chat` 的 `Err` 与流内 `LlmEvent::Error` 都转为 `AgentEvent::Error`，随后 `SessionEnded(Error)`。

**新增依赖**：

```toml
# Task 1.2: AgentLoop accepts a CancellationToken for cooperative cancellation.
tokio-util = { version = "0.7", features = ["rt"] }
```

`CancellationToken` 由计划明确要求；`tokio-util` 只用于取消令牌，不引入额外运行时能力。`jessica-runtime` 的内部依赖仍只有 `jessica-core`。

**测试（11 个行为测试）**：

- 纯文本终止：事件序列精确匹配，Provider 只调用一次且收到初始 messages；
- 单工具调用后终止：工具收到参数，结果以原始 call id 回填，第二次 Provider 调用看到 assistant tool_calls + tool message；
- 多轮 + 单轮多工具：执行顺序为 `call-1 → call-2 → call-3`，事件顺序一致；
- 最大轮次：`max_rounds = 2` 时 Provider 只调用 2 次、工具执行 2 次，发出 `BudgetExceeded(MaxRounds)` 与 `SessionEnded(BudgetExceeded)`；
- 预取消：不调用 Provider，仅 `SessionStarted` + `SessionEnded(Cancelled)`；
- 流中取消：spawn 循环，观察到 `ModelDelta` 后取消，断言在 1 秒内结束且事件序列精确；
- Provider 请求错误、流内错误事件、流缺少 `Done`：都产生 `Error` + `SessionEnded(Error)`；
- 未知工具：返回错误结果并继续，第二次 Provider 调用看到 `"tool error: unknown tool ..."`；
- 工具失败：`ToolCallFinished` 保留 `is_error`，回填内容带 `"tool error: "` 前缀。

**已知接口缺口（已记入 PLAN.md Deferred）**：

`LlmEvent::Done.usage` 在 Task 1.2 中被消费掉但不持久化，`AgentEvent` 也没有 usage 字段。Phase 2 的 TokenBudgetGuard 需要 token 计数，计划通过包装 `LlmProvider`（拦截 `Done`）或在 runtime 内部计数接入，不修改核心公共 API。

### 3.6 Task 1.3：原生工具与最小 CLI（jessica-tools / jessica-cli）

**目标**：提供可用的端到端入口：读取用户输入 → 调用 `AgentLoop` → 流式打印模型文本与工具摘要；同时装配三个带安全边界的原生工具。

**模块布局**：

| 文件 | 职责 |
| --- | --- |
| `jessica-tools/src/lib.rs` | crate 文档、导出三个工具、统一 `success` / `failure` 辅助函数 |
| `jessica-tools/src/file.rs` | `ReadFileTool`、`WriteFileTool`：工作目录限定、路径解析、大小与编码校验 |
| `jessica-tools/src/bash.rs` | `BashTool`：`bash -c`、超时、双流并发读取、每流输出上限 |
| `jessica-tools/tests/native_tools.rs` | 10 个文件工具行为测试（越界、symlink、二进制、超限等） |
| `jessica-tools/tests/bash_tool.rs` | 6 个 bash 行为测试（退出码、超时、截断、参数校验） |
| `jessica-cli/src/main.rs` | 装配 Provider、工具、Registry 与事件循环；one-shot / REPL 两种入口 |

**原生工具语义**：

- `read_file { path }`：先 canonicalize 工作区根，再 canonicalize `root.join(path)`，要求结果仍以根开头；只允许普通文件；上限 256 KiB；内容必须是 UTF-8。
- `write_file { path, content }`：已存在的目标先 canonicalize 并检查是否在根内；新目标则 canonicalize 父目录并检查，再拼接文件名。父目录必须存在，不自动创建。现有 symlink 指向工作区外时拒绝写入。
- `bash { command }`：以 `bash -c` 启动，stdin 为 null，stdout/stderr 分别 pipe；默认 10 秒墙钟超时与每流 64 KiB 上限，可通过 `BashTool::with_limits` 注入测试值。
- **输出背压**：`read_capped` 在达到上限后仍持续读到 EOF 并丢弃多余字节，避免子进程因子进程管道写满而阻塞，从而使 `wait` 永远无法返回。
- **失败语义**：非零退出码、超时、spawn 失败、参数解析失败都返回 `is_error: true` 的 `ToolResult`；非零退出仍保留捕获的输出，超时通过 `kill_on_drop(true)` 终止子进程。
- 三个工具的 `ToolResult.tool_call_id` 都留空，由 `AgentLoop` 按原始调用 id 盖章。

**CLI 结构**：

```text
main
 └─ run
     ├─ build_provider（JESSICA_PROVIDER=openai 默认 / anthropic）
     ├─ spawn_blocking(std::env::current_dir)
     ├─ ToolRegistry: bash / read_file / write_file（BTreeMap 排序）
     ├─ broadcast::channel(1024) + AgentLoop
     ├─ 无参数 → repl(stdin)
     └─ 有参数 → run_turn(args.join(" "))
```

- **事件泵**：`run_turn` 把 `agent.run(...)` pin 住，与 `broadcast::Receiver` 放在同一个 `tokio::select!` 中。模型 delta 到达即打印并 flush；`run` 结束后再 drain 一次 receiver，打印 select 未消费的事件。
- **Ctrl+C**：每轮创建一个 `CancellationToken` 和一个常驻信号监听任务；收到 SIGINT 时 `cancel()`。`run` 返回后 abort 监听任务。REPL 等待输入时的 Ctrl+C 直接退出 REPL。
- **REPL**：使用单个 `BufReader<Stdin>` 跨迭代复用，避免管道输入被第一个 reader 预读后丢失；`exit` / `quit` / EOF 退出。
- **退出码**：`Completed` 与 `Cancelled` → 0；`BudgetExceeded`、`AcceptanceFailed`、`Error` → 1。
- **错误展示**：`CliError` 用 `thiserror` 覆盖 Provider 配置、当前目录与工具注册失败；运行期的 `AgentEvent::Error` 打到 stderr，用户可见输出使用 `println!`/`eprintln!`（符合 CLI 例外）。

**关键决策**：

- **工具放 `jessica-tools` 而非 CLI**：符合 crate 角色，Phase 3 的 WASM 工具也注册到同一层；CLI 只做装配。
- **两个文件工具都限制在工作目录内**：计划只点名 `read_file`，但 `write_file` 若可越界会直接削弱安全边界，因此同样约束。
- **最小 REPL 无跨输入历史**：每次输入构造新的 system + user messages，并开启新的 session id；跨轮会话恢复属 Phase 4。
- **不做 TUI / 参数解析**：所有命令行参数拼成一个 prompt；这符合 Task 禁止项。
- **预算只传入不强制**：`Budget` 携带 token 上限与 tool_timeout，但运行时目前只执行 `max_rounds`；其余交给 Phase 2 Guards。

**验收（2026-11-08）**：

```text
cargo test -p jessica-tools                 16 passed
cargo test --workspace                      86 passed
cargo clippy --workspace --all-targets -D warnings   clean
cargo fmt --all --check                     clean
```

无真实 API key，端到端使用本地 Python mock SSE server（`OPENAI_BASE_URL=http://127.0.0.1:<port>/v1`）：

- 第 1 次请求返回 `read_file {"path":"Cargo.toml"}` 的工具调用；
- CLI 打印 `[tool] read_file ...` 与 `[tool:ok] [workspace] members = ...`；
- 第 2 次请求包含 `role: "tool"`、`tool_call_id: "call_1"` 与真实 `Cargo.toml` 内容；
- mock 返回流式中文总结，CLI 打印 `SessionEnded: Completed`，退出码 0；
- 缺失 `OPENAI_API_KEY`：退出码 1，stderr 明确指名缺失变量；
- 流式响应中途发送 SIGINT：CLI 打印 `SessionEnded: Cancelled`，退出码 0。

**已知限制（已记入 PLAN.md Deferred）**：

- 路径 canonicalize 与后续读写之间存在 TOCTOU 竞态；Phase 2 Policy / Phase 3 沙箱继续收窄。
- bash 超时只杀直接子进程，不杀进程组；后台后代可能残留。
- REPL 无跨输入对话历史。
- token 与 tool_timeout 预算尚未被运行时强制。

### 3.7 Task 2.1：中间件框架（jessica-harness）

**目标**：定义 Hook 的装配与固定调用顺序，证明加入空中间件不会改变 Agent 的事件序列或返回原因，为 Phase 2 的 Guard/Policy/Sensor 提供挂载点。

**依赖约束**：`CLAUDE.md` 规定 `jessica-harness` 只能依赖 `jessica-core`，因此 Harness **不能**直接引用 `jessica-runtime::AgentLoop`。实现采用泛型 agent future + harness 自持的 broadcast 通道：

- CLI/调用方从 `Harness::events()` 取得 Sender，交给 `AgentLoop`；
- `Harness::run` 接收任意 `Future<Output = SessionEndReason>`，一边 poll agent，一边消费同一通道的事件；
- 真实 `AgentLoop` 的接线已由 Task 2.4 在 `jessica-cli` 完成，见 §3.10。

**模块布局**：

| 文件 | 职责 |
| --- | --- |
| `jessica-harness/src/lib.rs` | crate 文档、`#![deny(missing_docs)]`、导出 `Harness` / `LoggingHook` |
| `jessica-harness/src/harness.rs` | `Harness`：通道所有权、生命周期调度、事件 drain、lag 告警 |
| `jessica-harness/src/logging.rs` | `LoggingHook`：每个回调发一条 `tracing` DEBUG 事件 |
| `jessica-harness/tests/harness_chain.rs` | 5 个行为测试 |

**公共 API**：

```rust
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

pub struct Harness { /* hooks: Vec<Arc<dyn Hook>>, events: broadcast::Sender<AgentEvent> */ }

impl Harness {
    pub fn new(hooks: Vec<Arc<dyn Hook>>) -> Self;
    pub fn with_event_capacity(hooks: Vec<Arc<dyn Hook>>, event_capacity: usize) -> Self;
    pub fn events(&self) -> broadcast::Sender<AgentEvent>;
    pub fn subscribe(&self) -> broadcast::Receiver<AgentEvent>;

    pub async fn run<F>(&self, session_id: &SessionId, agent: F) -> SessionEndReason
    where
        F: Future<Output = SessionEndReason>;
}
```

**固定调用顺序**：

```text
for hook in hooks (registration order): on_session_start(session_id)
loop:
    poll agent future
    for each broadcast AgentEvent (arrival order):
        for hook in hooks (registration order): on_event(event)
drain events still buffered after agent resolves
for hook in hooks (registration order): on_session_end(session_id)
return agent's SessionEndReason unchanged
```

- `on_session_start` 在 agent 被首次 poll 之前调用；`SessionStarted` 事件随后通过 `on_event` 到达。
- agent future resolve 后必须 drain 通道：mock agent 可以连续发送事件且不让出执行权，若只依赖 `select!` 会漏掉末尾事件。
- Hook 逐个 await，保证同一事件内跨 Hook 的确定性顺序；Hook 不得 panic 或永久阻塞，Harness 不做隔离。
- 广播通道有界（默认 1024）。Hook 阻塞导致接收端落后时，`recv` 返回 `Lagged`，Harness 用 `tracing::warn!` 报告并继续；事件可能被跳过。这符合当前“观察层”定位，但治理层若要求每条事件必达，需要后续改为同步回调或显式背压。

**LoggingHook**：

- `name()` 返回 `"logging"`；
- `on_session_start` / `on_session_end` 记录 session id；
- `on_event` 只记录事件类型（与 serde tag 一致的稳定名称），不记录 `ModelDelta.text`，避免模型内容进入日志；
- 全程使用 `tracing::debug!`；无 subscriber 时自然无输出。

**测试（5 个行为测试）**：

1. **空中间件**：同一事件序列分别直接运行与经 `Harness::new(Vec::new())` 运行，返回原因与独立订阅者看到的事件序列完全一致；
2. **观察 Hook 透明**：带 `RecordingHook` 时，Hook 记录的事件序列等于 baseline，外部订阅者也仍收到相同序列；
3. **注册顺序**：两个 Hook 在 start → 每个 event → end 各点严格按 `first, second` 调用；
4. **非 Completed 原因透传**：`Cancelled` / `BudgetExceeded` / `AcceptanceFailed` / `Error` 均原样返回；
5. **LoggingHook**：用最小 `tracing::Subscriber` 计数，验证 `on_session_start` + 每个事件 + `on_session_end` 各产生一条 tracing event。

**依赖变化**：

```toml
# jessica-harness/Cargo.toml
async-trait = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
serde_json = { workspace = true }
```

三者均为 workspace 已批准依赖，未新增 workspace 级 crate。`Cargo.lock` 因 `tracing` 启用 attributes 而加入 `tracing-attributes` / `syn`（传递依赖）。

**已知限制（已记入 PLAN.md Deferred）**：

- Harness 目前只观察，无法阻止工具执行或改变循环；Guards/Policy/Sensor 的实际介入方式需在 Task 2.2/2.4 解决，同时不得破坏 `harness → core` 单向依赖。
- 有界 broadcast 在慢 Hook 下会 lag，当前仅告警；未定义治理层所需的背压/必达语义。

### 3.8 Task 2.2：Guards 与 Policy（jessica-harness）

**目标**：实现四种确定性 Guard 与白名单 Policy，把超限和越权转换为结构化 `AgentEvent`，且不在判断中调用 LLM、不 panic。

**装配方式**：

`Harness` 在 Task 2.1 的 Hook 之外增加两个治理组件：

```rust
pub struct Harness {
    hooks: Vec<Arc<dyn Hook>>,
    guards: Vec<Arc<dyn Guard>>,
    policy: Arc<dyn Policy>,
    events: broadcast::Sender<AgentEvent>,
}

impl Harness {
    pub fn with_guards(self, guards: Vec<Arc<dyn Guard>>) -> Self;
    pub fn with_policy(self, policy: Arc<dyn Policy>) -> Self;
    pub fn check_guards(&self, context: &GuardContext<'_>) -> Option<AgentEvent>;
    pub fn authorize_tool_call(&self, call: &ToolCall) -> Option<AgentEvent>;
}
```

- 默认 policy 是 `DenyAll`，与“最小权限”原则一致；必须显式安装 `AllowlistPolicy`。
- `check_guards` 按注册顺序求值，返回第一个 `GuardDecision::Exceeded` 对应的 `AgentEvent::BudgetExceeded { kind, message: reason }`。多个 guard 同时超限时首命中者胜，结果确定。
- `authorize_tool_call` 把 `PolicyDecision::Deny(reason)` 转为 `AgentEvent::PolicyDenied { call: call.clone(), reason }`；允许时返回 `None`。
- **检查点显式化**：`Harness::run` 只负责 Hook 生命周期，不自动调用 guard/policy。原因见“已知限制”。

**Guard 语义**：

| Guard | 触发条件 | `BudgetKind` | 边界处理 |
| --- | --- | --- | --- |
| `TokenBudgetGuard::new(max_tokens)` | `context.tokens_used > max_tokens` | `TokenLimit` | 等于上限仍允许 |
| `ToolTimeoutGuard::new(timeout)` | `context.tool_elapsed > timeout` | `ToolTimeout` | 未提供耗时时允许；等于上限仍允许 |
| `StuckDetectionGuard::new(max_stalled_rounds)` | 连续无进展轮数达到上限 | `Stuck` | 首次观察轮建立基线；同一 round 重复检查幂等 |
| `ResultSizeGuard::new(max_bytes)` | `result.content.len() > max_bytes` | `ResultSize` | 按 UTF-8 字节数，等于上限仍允许 |

`StuckDetectionGuard` 的细节：

- 状态为 `Mutex<StuckState>`，仅保存 `last_round`、`last_output`、`stalled_rounds`，不读取时钟、不做 I/O。
- 一轮算“有进展”当且仅当：带 `tool_call`、带 `tool_result`，或 `last_output` 与上一轮不同。
- 无进展时 `stalled_rounds` 饱和加一；有进展时清零。达到 `max_stalled_rounds` 即返回 `Exceeded`。
- round 回退视为新 session，重置状态；但若新 session 从相同 round 开始，必须新建实例或调用 `reset()`，否则旧基线仍会生效。
- `max_stalled_rounds` 小于 1 时按 1 处理。锁中毒时取回内部值继续，避免把前一个 panic 传播成新的 panic。

**AllowlistPolicy**：

```rust
pub struct AllowlistPolicy { allowed: BTreeSet<String> }

impl AllowlistPolicy {
    pub fn new(allowed_tools: impl IntoIterator<Item = String>) -> Self;
}
```

- 名称大小写敏感，使用 `BTreeSet` 保证确定性；
- 空名单（`Default`）拒绝全部工具，与 `DenyAll` 等价；
- 拒绝原因包含 `tool \`name\` is not in the allowlist`，便于事件审计；
- 本 Task 只提供内存构造器；配置文件读取已由 Task 2.4 的 CLI 装配实现，见 §3.10。

**无 panic 保证**：

- 库代码无 `unwrap`/`expect`，唯一可能失败的 `Mutex::lock` 显式恢复中毒锁；
- 所有比较使用整数/字节长度，不做可能溢出的算术（`saturating_add`）；
- `guards_and_policy_do_not_panic_on_empty_contexts` 用 `catch_unwind` 在最小上下文（空输出、无工具、无耗时）上验证四个 guard 与默认 policy 均正常返回结构化结果。

**测试（10 个行为测试，`tests/governance.rs`）**：

1. Token 边界：等于上限允许，超过一 token 触发 `TokenLimit`；
2. 工具超时边界：无耗时时允许，等于上限允许，超过 1ms 触发 `ToolTimeout`；
3. 结果大小边界：等于上限允许，超过 1 字节触发 `ResultSize`；
4. 连续 2 轮无进展后触发 `Stuck`；
5. 输出变化或出现工具调用会清零计数，之后重新累计并触发；
6. Allowlist 放行名单内工具、拒绝名单外工具并给出原因；
7. 未安装 policy 时默认 `DenyAll` 拒绝所有工具调用；
8. 多个 guard 同时超限时按注册顺序返回第一个；
9. 空上下文下 guard/policy 不 panic 且返回类型正确；
10. 同一 round 重复检查幂等，`reset()` 后以新 round 重建基线并可再次触发。

**依赖变化**：无。`std::sync::Mutex`、`BTreeSet` 均为标准库；测试使用已有 `serde_json` dev-dependency。

**已知限制（已记入 PLAN.md Deferred）**：

- `Harness::run` 不自动执行治理：`AgentEvent` 不携带 token usage、round、工具耗时，Policy 又必须在工具执行前调用。调用方需在检查点显式调用；Task 2.4 已通过 CLI 包装与 EventPump 解决真实接入（§3.10）。
- Token 计数在 Task 2.4 已通过 CLI 的 `TokenCountingProvider` 接入；`TokenBudgetGuard` 本身仍只消费调用方提供的 `tokens_used`。
- `StuckDetectionGuard` 是单 session 状态，跨 session 复用必须 `reset()`；CLI 在每个 attempt 前 reset。
- Allowlist 的配置文件解析与示例已由 Task 2.4 提供（§3.10）。

### 3.9 Task 2.3：Sensors 与 Acceptance（jessica-harness）

**目标**：在工具执行后运行一组确定性 sensor；在模型停止后运行 AcceptanceCheck，失败则注入反馈并重入会话，直到通过或重试耗尽。整个治理路径不调用 LLM，也不执行自动修复。

**Sensor 管线**：

```rust
pub struct SensorPipeline { /* Vec<Arc<dyn Sensor>> */ }
pub struct SensorReport { /* Vec<SensorObservation> */ }

pub struct SensorObservation {
    pub sensor: String,
    pub result: SensorResult,
}

impl SensorPipeline {
    pub fn new(sensors: Vec<Arc<dyn Sensor>>) -> Self;
    pub fn is_empty(&self) -> bool;
    pub async fn run(&self, context: &SensorContext<'_>) -> SensorReport;
}

impl SensorReport {
    pub fn observations(&self) -> &[SensorObservation];
    pub fn passed(&self) -> bool;
    pub fn failures(&self) -> impl Iterator<Item = &SensorObservation>;
}
```

- sensor 严格按注册顺序串行 `await`，结果顺序确定；
- 单个 sensor 失败不会短路，后续 sensor 仍会运行，报告包含完整证据；
- 空管线的 `passed()` 为 `true`（没有失败）；
- `Harness` 增加 `with_sensors` 与 `run_sensors(&ToolCall, &ToolResult)`，把工具调用和结果包装成 `SensorContext`。`Harness::run` 仍不自动调用 sensor，因为事件流没有直接提供结果对象。

**CommandSensor**：

```rust
pub struct CommandSensor { /* name, program, args, current_dir */ }

impl CommandSensor {
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self;
    pub fn arg(self, arg: impl Into<String>) -> Self;
    pub fn args<I, S>(self, args: I) -> Self
        where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn with_current_dir(self, directory: impl Into<PathBuf>) -> Self;
}
```

- 直接 exec program + args，默认不经过 shell；需要 shell 时显式配置 `sh -c`；
- stdin 为 null，stdout/stderr 均捕获，`kill_on_drop(true)`；
- 退出码 0 为 pass；非零/信号终止为 fail；
- 启动失败（程序不存在、工作目录非法等）转为 `SensorResult::fail`，不 panic；
- pass/fail 输出都带 `stdout:` / `stderr:` 标签，空输出显示 `(no output)`。

**Acceptance 编排**：

```rust
pub struct SessionAttempt {
    pub messages: Vec<Message>,
    pub last_output: String,
    pub reason: SessionEndReason,
}

pub struct AcceptanceOutcome {
    pub reason: SessionEndReason,
    pub messages: Vec<Message>,
    pub last_output: String,
    pub attempts: u32,
}

pub struct AcceptanceRunner { /* check, max_retries, events */ }

impl AcceptanceRunner {
    pub fn new(
        check: Arc<dyn AcceptanceCheck>,
        max_retries: u32,
        events: broadcast::Sender<AgentEvent>,
    ) -> Self;

    pub async fn run<F, Fut>(
        &self,
        initial_messages: Vec<Message>,
        attempt: F,
    ) -> AcceptanceOutcome
    where
        F: FnMut(Vec<Message>) -> Fut,
        Fut: Future<Output = SessionAttempt>;
}
```

**重入算法**：

```text
messages = initial_messages
attempt_number = 0
loop:
    attempt_number += 1
    outcome = attempt(messages)
    if outcome.reason != Completed: return outcome.reason
    result = check.check(outcome.messages, outcome.last_output)
    if result.passed: return Completed
    emit AcceptanceFailed { attempt: attempt_number, feedback: result.feedback }
    if attempt_number > max_retries: return AcceptanceFailed
    messages = outcome.messages + Message::user(result.feedback)
```

- `max_retries` 是首次检查失败后允许的额外尝试数；`max_retries == 0` 表示首次失败即结束，总尝试数上限为 `max_retries + 1`；
- `AcceptanceFailed.attempt` 是产生失败输出的 one-based attempt 编号，事件在下一 attempt 开始前发出；
- 只有 `Completed` 的 attempt 才运行验收；`Cancelled` / `BudgetExceeded` / `Error` 原样透传，不注入反馈、不重试；
- 失败反馈只作为 `Message::user` 注入，不存在任何 harness 内部 LLM 调用或自动修复逻辑；
- `AcceptanceRunner` 不依赖 `jessica-runtime`：`attempt` 闭包由上层提供，因此可以用真实 AgentLoop、mock 或任意会话实现驱动。这保持了 `harness → core` 的单向依赖。

**测试（9 个行为测试，`tests/sensors_acceptance.rs`）**：

1. 两个 mock sensor 全 pass：报告通过、顺序正确、每个 sensor 各调用一次；
2. 经 `Harness::run_sensors` 运行：一个 fail 不阻止后续 sensor，`failures()` 返回失败项；
3. CommandSensor 退出 0：pass 且输出包含 stdout；
4. CommandSensor 非零退出：fail 且输出包含 stderr 与状态；
5. CommandSensor 程序不存在：返回 fail，不 panic；
6. Acceptance 首次通过：attempt 调用一次、检查一次、无 `AcceptanceFailed`；
7. Acceptance 第一次失败、第二次通过：第二次输入包含 feedback 的 user message，且恰好一个 `AcceptanceFailed { attempt: 1 }`；
8. 重试耗尽：`max_retries=2` 时共 3 次 attempt，发出 attempt 1/2/3 三个事件，最终 `AcceptanceFailed`；
9. attempt 非 `Completed`：跳过验收、不重试、不发事件。

**依赖变化**：无。`tokio::process`、`std::process::Stdio` 均在已批准依赖范围内；测试复用已有 `async-trait`、`tokio`、`serde_json`。

**已知限制（已记入 PLAN.md Deferred）**：

- Sensor/acceptance 仍需调用方显式驱动，尚未并入 `Harness::run`；CLI 的接入方式见 §3.10。
- `AcceptanceRunner` 需要完整的 messages 与最后输出，而 `AgentLoop::run` 只返回 `SessionEndReason`；Task 2.4 已通过 EventPump 从事件重建 transcript，未修改 runtime。
- `AcceptanceRunner` 只发 `AcceptanceFailed`；Task 2.4 统一为每次 attempt 发自己的 `SessionEnded`，再由 CLI 外层会话发最终 `SessionEnded`。
- `CommandSensor` 暂无超时与输出大小上限，挂起的命令会阻塞 sensor 管线；CommandAcceptanceCheck 已限制反馈输出为 8 KiB，但同样无超时。

### 3.10 Task 2.4：CLI 接入与端到端验证（jessica-cli）

**目标**：在 `jessica-cli` 中装配 Guard、Policy、Sensor、Acceptance，使治理链可从配置文件启用，并用真实 `AgentLoop` 完成端到端验证。本 Task 明确禁止修改 `jessica-runtime` 的循环代码，因此所有接入都在 CLI 边界完成。

```text
CliRuntime::run_turn
  │
  ├─ TokenCountingProvider ── LlmEvent::Done(usage) ──▶ GovernanceState
  │                                                       └─ TokenBudgetGuard
  ├─ AgentLoop::run(provider, registry, Budget, cancel)
  │     └─ ToolCallStarted / ToolCallFinished / ModelDelta / SessionEnded
  │                          │ broadcast
  │                          ▼
  │                     EventPump ── print / Transcript
  │                          ├─ check_guards(ToolCallStarted)
  │                          └─ run_sensors(ToolCallFinished)
  │
  ├─ GovernedTool (registry 包装) ── Policy → timeout → tool → ResultSizeGuard
  │
  └─ AcceptanceRunner ── CommandAcceptanceCheck
        └─ 每次 attempt: run_attempt → EventPump 重建 SessionAttempt
```

**新增文件与职责**：

| 文件 | 职责 |
| --- | --- |
| `jessica-cli/src/config.rs` | TOML schema、默认值、严格字段校验、`Budget` 转换 |
| `jessica-cli/src/governance.rs` | `GovernanceState`、`TokenCountingProvider`、`GovernedTool` |
| `jessica-cli/src/pump.rs` | 事件打印、transcript 重建、Guard 检查点、Sensor 驱动、终态等待 |
| `jessica-cli/src/acceptance_check.rs` | `CommandAcceptanceCheck`：直接 exec、无 shell、8 KiB 输出截断 |
| `jessica-cli/src/app.rs` | 单 turn 装配、acceptance 重试编排、外层终态事件、Ctrl+C |
| `jessica-cli/src/main.rs` | 参数解析、配置加载、Provider 创建、one-shot / REPL |
| `examples/harness.toml` | 示例：预算、allowlist、git diff sensor、README 拼写验收 |
| `README.md` | 示例验收夹具（首句故意拼错）与使用说明 |

**关键设计决策**：

1. **不修改 runtime**：`AgentLoop::run` 只返回 `SessionEndReason`，也不在事件中携带 token usage、round、工具耗时或结果大小。CLI 用两层包装补齐：Provider 包装观察 `LlmEvent::Done` 计费；Tool 包装在真实执行前后做 Policy/超时/ResultSize 检查。`EventPump` 从事件重建 round 与 transcript，不改变冻结契约。

2. **Token 计费**：`TokenCountingProvider` 在把 `Done` 交给循环前调用 `GovernanceState::charge_tokens`，累计本 turn 所有 attempt 的 `usage.total()`。超过限制时发出 `BudgetExceeded { TokenLimit }` 并取消 `CancellationToken`。只有 Provider 报告 usage 时才能计费；缺失 usage 是已知限制。

3. **Policy 拒绝不终止会话**：`GovernedTool` 在调用前用探针 `ToolCall`（空 id）调用 `Harness::authorize_tool_call`，拒绝时发 `PolicyDenied` 并返回错误 `ToolResult`。模型因此能看到拒绝原因并改写计划，而不是让整个 session 因一次越权调用直接结束。测试断言 inner tool 调用次数为 0、`CancellationToken` 未取消、事件类型正确。

4. **超时与结果大小**：`tokio::time::timeout` 包住 `inner.execute`，超时值用 `timeout + 1ns` 保证跨过 Guard 的严格 `>` 边界；超时与 ResultSize 超限都发 `BudgetExceeded` 并取消 session。首个 trip 胜出，后续 trip 不重复发事件（`AtomicBool::swap`）。

5. **Transcript 重建**：`ModelDelta` 开启/延续当前 round；`ToolCallStarted` 把 call 放入 pending；`ToolCallFinished` 按 `tool_call_id` 配对；round 切换/结束时 flush 为 `assistant_with_tool_calls` + `tool` 消息。错误结果加 `tool error: ` 前缀，与 runtime 回填给模型的格式一致；被中断的调用生成合成错误消息。`SessionEnded` 原子地把 active transcript 移入 ended 快照并 `Notify`。这样 `AcceptanceRunner` 不需要 runtime 返回 messages。

6. **验收重试与终态语义**：每次 attempt 使用独立 `SessionId`，注册初始 messages；`run_attempt` 在开始前 `StuckDetectionGuard::reset()`，等待 pump 处理完该 session 的终态（30s 上限）。`AcceptanceRunner` 只在 `Completed` 后检查，失败把 feedback 作为 `Message::user` 注入。每次 attempt 自己发 `SessionEnded`，CLI 最后再发一个 outer `SessionStarted` + `SessionEnded` 承载整个 turn 的最终 reason；预算 trip 会把内层矛盾的 `Completed/Cancelled` 映射为 `BudgetExceeded`，并抑制其打印。

7. **配置**：`[budget]` 对应 `Budget`，另有 `max_stuck_rounds` 与 `max_result_bytes`；`[policy]` 的 `allowed_tools` 缺省为原生三工具，显式空数组表示拒绝全部；`[sensor]` / `[acceptance]` 可选，`acceptance.max_retries` 默认 2。`deny_unknown_fields` 拒绝拼写错误，避免治理配置被静默忽略；round/timeout/stuck 必须为正，命令与名称不能为空。

8. **示例配置**：allowlist 故意不含 `bash`，因此第一次模型调用 bash 会产生可观察的 PolicyDenied；sensor 运行 `git diff --check`；acceptance 用 `sh -c` 检查 README 是否包含正确短语。README 首句保留 `determinstic`，使第一次 attempt 必然验收失败，从而演示 feedback 重试。示例中的 `sh -c` 是显式配置，CLI 本身不通过 shell 执行命令。

**测试（新增 22 个 CLI 行为测试）**：

- `config.rs` 7 个：示例配置解析、缺省 allowlist、显式空 allowlist、默认重试数、未知字段拒绝、零 round/timeout 拒绝、空白命令/目录拒绝；
- `governance.rs` 6 个：Policy 拒绝不调用 inner 且不取消、超限结果 trip、慢工具超时 trip、token 恰好等于上限允许、超过上限 trip、首个 trip 去重；
- `pump.rs` 6 个：完整文本/工具/最终输出重建、被中断工具合成错误、错误前缀、纯文本 round、工具结果后的新 round、多个调用共享同一 round；
- `acceptance_check.rs` 3 个：命令通过、非零退出带回 stderr、程序不存在时返回失败而不 panic。

`cargo test --workspace` 共 131 个测试全绿（在 Task 2.3 的 109 个基础上增加 CLI 22 个）。

**端到端验收（无真实 API key，使用本地脚本化 OpenAI SSE server）**：

1. `cargo run -p jessica-cli -- --config examples/harness.toml "修改 README 中的拼写错误并验证"` → exit 0。输出依次显示：`[tool] bash`、`[policy] denied bash`、`[tool:error]`、`[sensor:pass] diff-check`、attempt 1 `Completed`、`[acceptance] attempt 1 failed`（带 feedback）、attempt 2 `write_file` 修复 README、sensor pass、attempt 2 `Completed`、outer session `Completed`。mock 请求日志确认第 3 个请求包含前一轮的 assistant tool_calls、`tool error: tool 'bash' was denied by policy` 与 feedback user message。
2. token 预算场景（`max_tokens=10`，mock usage=100）→ exit 1。输出 `[budget] TokenLimit: token usage 100 exceeds limit 10`，最终 outer `SessionEnded: BudgetExceeded`。
3. E2E 后 README 已恢复为故意拼错的首句；脚本化 server 仅用于验证，未提交到仓库。

**依赖变化**：根 workspace 增加 `toml = "0.9"`，理由是 CLI 需要从显式 TOML 文件读取治理配置；`jessica-cli` 增加 `async-trait`、`futures`、`serde`、`serde_json`、`toml` 的 workspace 依赖。`jessica-core` 与 `jessica-runtime` 未改动。

**已知限制（已记入 PLAN.md Deferred）**：Token 计费依赖 Provider 上报 usage；EventPump 依赖有界 broadcast 不 lag；StuckDetectionGuard 在 CLI 的检查点把工具调用视为进展，纯工具循环实际由 `max_rounds` 兜底；CommandSensor/CommandAcceptanceCheck 无超时；`max_rounds` 按 attempt 而非整个 turn 计数；Policy 拒绝不终止会话；尚未用真实 API key 联调。

### 3.11 Task 3.1：wasmtime 基础加载（jessica-sandbox）

**目标**：在 `jessica-sandbox` 中引入 wasmtime，打通“读取 `.wasm` → 编译 → 实例化 → 调用导出函数”的最小可信路径。Task 3.1 不接 `Tool` Trait，也不启用 WASI。

```text
WasmEngine::load_file(path) ── std::fs::read ──▶ Module::new
        │
        └──▶ WasmModule::instantiate ── 空 Linker ──▶ WasmInstance { Store<()>, Instance }
                                                         │
                                                         └── call_i32(name) ──▶ 42
```

**公开 API**：

| 类型/方法 | 语义 |
| --- | --- |
| `WasmEngine::new()` | 创建 wasmtime `Engine`，使用默认配置，不注册 WASI |
| `WasmEngine::load_file(path)` | 读取并编译 `.wasm` 文件；读取失败保留路径 |
| `WasmEngine::load_bytes(bytes)` | 编译内存中的 WebAssembly 二进制（不是 `.wat`） |
| `WasmModule::instantiate()` | 用空 `Linker` 和 `Store<()>` 实例化；声明 import 的模块会失败 |
| `WasmInstance::call_i32(name)` | 调用签名为 `() -> i32` 的导出函数 |
| `SandboxError` | `Engine` / `Read` / `Compile` / `Instantiate` / `UnknownExport` / `Signature` / `Trap` |

**关键设计决策**：

1. **最小 wasmtime feature**：根依赖使用 `wasmtime = "50.0"`、`default-features = false`、features 仅 `cranelift`/`runtime`/`std`。默认 features 中的 WASI、component model、async、GC 等全部关闭，减少攻击面与编译体积；Task 3.1 不需要 WASI，空 Linker 进一步保证模块无法解析到宿主能力。
2. **同步低层 API**：wasmtime 的编译/实例化/调用本质是同步的，沙箱 crate 提供同步接口；Task 3.3 的异步 `Tool` 适配器必须用 `spawn_blocking` 调用，避免阻塞 Tokio worker。
3. **错误结构化且不 panic**：文件读取、编译、实例化、缺失导出、签名不匹配、WASM trap 各有独立错误变体。trap 只返回 `SandboxError::Trap`，不终止宿主；测试在 trap 后继续调用同一实例，确认 store 未被 poison。
4. **无隐式能力**：不启用 wasmtime 的 WASI feature，Linker 为空；未来 Task 3.2 才通过显式 `WasiCtxBuilder` 授予目录只读等能力。
5. **预编译 fixture**：`tests/fixtures/fixed_value.wat` 是源文件，`fixed_value.wasm` 是 51 字节预编译产物，`build.sh` 用 `wasm-tools parse` 重建。测试直接读取 `.wasm`，不依赖测试时安装 wasm-tools。本次已用 `wat` crate 的解析器验证 `.wasm` 与 `.wat` 输出逐字节一致。

**测试（6 个行为测试，`tests/wasm_loading.rs`）**：

1. 从文件加载、实例化并调用 `answer`，连续两次都返回 42；
2. 从 `include_bytes!` 的已编译字节加载并调用；
3. 缺失文件返回 `Read`，错误中保留请求路径；
4. 非法二进制返回 `Compile`；
5. 不存在的导出返回 `UnknownExport` 且带导出名；
6. 调用 `trap` 返回 `Trap`，随后同一实例仍能成功调用 `answer`。

`cargo test -p jessica-sandbox` 6 passed；`cargo test --workspace` 136 passed。

**依赖变化**：根 workspace 增加 `wasmtime = { version = "50.0", default-features = false, features = ["cranelift", "runtime", "std"] }`；`jessica-sandbox` 增加 `wasmtime` 与 `thiserror`（workspace 继承）。未启用 WASI，未改动 `jessica-core`/`jessica-runtime`。

**已知限制（已记入 PLAN.md Deferred）**：调用 ABI 仅 `() -> i32`，无参数/内存/复杂返回值；无 fuel、墙钟、内存/表/实例限制，Task 3.2 加入；空 Linker 拒绝所有 import；同步 API 在异步上下文中需 `spawn_blocking`；`build.sh` 依赖 wasm-tools，当前环境未安装，但预编译 fixture 已提交。

### 3.12 Task 3.2：WASI 能力控制与资源限制（jessica-sandbox）

**目标**：在 Task 3.1 的加载路径上加入 WASI preview 1 能力控制和每 Store 资源限制。默认拒绝一切宿主能力；插件清单只能授予只读目录；fuel、墙钟、内存、表、实例均有上限。

```text
WasmModule::instantiate(policy)
  ├─ WasiCtxBuilder: 无 args/env、stdin closed、stdout/stderr sink
  │                   allow_tcp/udp/ip_name_lookup = false
  ├─ policy.read_only_dirs → preopened_dir(host, guest, FsPerms::ReadOnly)
  ├─ StoreLimits { memory_size, table_elements, instances, trap_on_grow_failure }
  └─ run_limited(fuel, wall_timeout)
        ├─ store.set_fuel(fuel)
        ├─ store.set_epoch_deadline(1)
        └─ Watchdog thread ── timeout ──▶ Engine::increment_epoch()
                                  └─ Trap::Interrupt
WasmInstance::call_i32(name)
  └─ 每次重置 fuel/deadline；OutOfFuel → FuelExhausted，Interrupt → DeadlineExceeded
```

**公开 API**：

| 类型/方法 | 语义 |
| --- | --- |
| `SandboxPolicy` | 一个插件的资源与能力策略；默认无目录、无网络、有限资源 |
| `SandboxPolicy::with_fuel / with_wall_timeout` | 设置每次 instantiate/call 的 fuel 与墙钟预算 |
| `SandboxPolicy::with_memory_limit / with_table_limit / with_instance_limit` | 设置每内存字节数、每 table 元素数、每 Store 实例数 |
| `SandboxPolicy::grant_read_only_dir(host, guest)` | 唯一的能力授予入口，固定 `FsPerms::ReadOnly` |
| `ReadOnlyDir` | 校验/传递 host→guest 目录映射 |
| `WasmModule::instantiate(&policy)` | 装配 WASI 上下文、StoreLimits、fuel 与墙钟后实例化 |
| `WasmInstance::call_i32(name)` | 每次调用重置 fuel/deadline，返回结构化错误 |
| `SandboxError` | 新增 `Capability` / `WasiLinker` / `FuelConfig` / `Watchdog` / `FuelExhausted` / `DeadlineExceeded` |

**关键设计决策**：

1. **WASI 默认拒绝**：`WasiCtxBuilder::new()` 的默认值已经无 preopen、无 args/env、stdin closed、stdout/stderr sink；实现再显式关闭 TCP、UDP 与 IP 名称解析。WASI preview 1 本身没有 `sock_open`，没有 preopen fd 时网络结构性不可达。
2. **只读能力不可绕过**：`SandboxPolicy` 只暴露 `grant_read_only_dir`，没有写权限 API；preopen 使用 `FsPerms::ReadOnly`。`../` 逃逸由 wasmtime 的 capability-based `Dir` 拒绝，测试用 preopen 外的同名内容验证。
3. **limits 与 guest 失败分离**：`StoreLimits` 通过 `ResourceLimiter` 委派到 Store 数据，`trap_on_grow_failure(true)` 使越限的内存/表增长直接 trap，而不是让 guest 用 `-1` 继续执行。实例上限设 1，`0` 会在实例化时被拒绝。
4. **每次操作重置预算**：`instantiate` 与 `call_i32` 都先 `set_fuel`，再 `set_epoch_deadline(1)`。因此一次 fuel 耗尽或超时不会污染后续调用；测试在错误后继续调用 `answer`，验证实例仍可用。
5. **watchdog 生命周期**：每次操作创建一个线程，通过 `mpsc::Receiver::recv_timeout` 等待；操作结束在 `Drop` 中发送取消并 `join`。这样不会残留 epoch 增量误杀下一次调用；代价是每次调用一个 OS 线程。
6. **错误必须可区分**：`Trap::OutOfFuel` 映射为 `FuelExhausted`，`Trap::Interrupt` 映射为 `DeadlineExceeded`，其他 trap 保留 `Trap`；宿主不会忽略 fuel 耗尽。能力配置失败、WASI linker 失败、watchdog 启动失败也各有结构化变体。
7. **同步 shim 的 async 约束**：wasmtime-wasi 的 p1 同步实现内部使用 Tokio；在 Tokio worker 上直接触发 WASI hostcall 会 panic（已用临时探针验证），而 `spawn_blocking` 与独立 OS 线程正常。`instantiate`/`call_i32` 的文档明确要求 async 调用方使用 `spawn_blocking`。
8. **fixture 策略**：新增 `wasi_fs.wat`（path_open/fd_read/fd_close/逃逸/写尝试）、`spin.wat`（死循环）、`limits.wat`（memory.grow/table.grow）。`.wasm` 仍预编译提交，`build.sh` 改为遍历全部 `.wat`；本次用 `wat` crate 生成，测试不依赖 wasm-tools。

**测试（17 个行为测试，jessica-sandbox）**：

- `tests/wasm_loading.rs` 6 个：Task 3.1 的加载/编译/调用/错误测试，全部改用 `SandboxPolicy::default()`。
- `tests/wasi_capabilities.rs` 5 个：默认 policy 无 preopen 时文件读取返回 errno 且实例仍可调用；只读授权能读取真实文件内容；`../outside.txt` 被拒；写/创建被拒且宿主不产生文件；缺失 host 目录返回 `Capability` 并保留路径。
- `tests/resource_limits.rs` 6 个：死循环耗尽 fuel 返回 `FuelExhausted` 且后续调用可用；100ms 墙钟返回 `DeadlineExceeded` 且后续调用可用；内存/表超过限额 trap；限额内增长成功（返回旧大小）；`max_instances=0` 拒绝实例化。

`cargo test -p jessica-sandbox` 17 passed；Task 3.2 完成时 `cargo test --workspace` 147 passed，Task 3.3 完成后为 169 passed，Task 4.1 完成后为 173 passed。

**依赖变化**：根 workspace 增加 `wasmtime-wasi = { version = "50.0", default-features = false, features = ["p1"] }`；`jessica-sandbox` 增加 `wasmtime-wasi`。`p1` 内部依赖 `p2`，会启用 wasmtime 的 `component-model`/`async` feature，这是 wasmtime-wasi 的硬性要求，不构成 guest 能力。未改动 `jessica-core`/`jessica-runtime`。

**已知限制（已记入 PLAN.md Deferred）**：Task 3.2 完成时调用 ABI 仍仅 `() -> i32`，Task 3.3 已增加 buffer ABI；manifest JSON 解析已由 Task 3.3 实现；p1 同步 shim 不能在 Tokio worker 上直接调用；每次调用创建/join 一个 watchdog 线程；`guest_path` 未做格式校验；`build.sh` 仍依赖 wasm-tools。

### 3.13 Task 3.3：WASM Tool 适配器（jessica-tools / jessica-sandbox / jessica-cli）

**目标**：把编译后的 wasm 模块包装成 `jessica_core::tool::Tool`。manifest 提供名称、参数 schema、模块路径、ABI 导出和沙箱权限；每次调用把 JSON 参数写入 guest、读回 JSON 结果；trap 必须成为 `ToolResult { is_error: true }`，不能吞掉。

**Buffer ABI**：sandbox 新增 `BufferAbi` 与 `WasmInstance::call_buffer`。

```text
manifest.abi = { alloc, invoke }
host:  alloc(len) -> ptr            // guest 提供至少 len 字节的可写区间
       memory[ptr..ptr+len] = JSON args
       invoke(ptr, len) -> i64      // high 32 = output length, low 32 = output pointer
       bounds-check + max_result_bytes
       memory[out_ptr..out_ptr+out_len] -> host Vec<u8>
```

- 每个导出调用各自重置 policy 的 fuel 与墙钟，因此一次 execute 最多消耗两份调用预算（instantiate 另有一份）。
- 输出长度在读取前先与 `max_result_bytes` 比较，防止恶意 guest 让宿主分配巨量内存；随后检查 `ptr + len` 是否落在当前线性内存内。
- 新增结构化错误：`MissingMemory`、`InvalidAllocation`、`OutOfBounds`、`InputTooLarge`、`OutputTooLarge`、`MemoryWrite`；`FuelExhausted`/`DeadlineExceeded`/`Trap` 沿用 Task 3.2。

**Manifest**（`examples/plugins/echo/manifest.json`）：

```json
{
  "name": "echo_json",
  "description": "Echo the JSON arguments back.",
  "schema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
  "module": "echo.wasm",
  "abi": {"alloc": "jessica_alloc", "invoke": "jessica_invoke"},
  "permissions": {
    "fuel": 10000000,
    "wall_timeout_ms": 5000,
    "max_memory_bytes": 131072,
    "max_table_elements": 1000,
    "max_instances": 1,
    "max_result_bytes": 65536,
    "read_only_dirs": []
  }
}
```

- `module` 与 `read_only_dirs[].host` 相对 manifest 所在目录解析；CLI 的 manifest 路径则相对 workspace root。
- 缺省 permissions 继承 `SandboxPolicy::new()` 的默认值；`max_result_bytes` 缺省 64 KiB。
- `deny_unknown_fields` 明确拒绝 `write_dirs` 等未支持权限；零 fuel/墙钟/内存/实例、相对 guest path、缺失只读目录都在加载期报 `InvalidManifest`。
- `WasmTool::schema()` 把 `description` 与 `schema` 包成 OpenAI function schema，name 来自 manifest。

**执行路径**：

```text
WasmTool::execute(args)
  └─ spawn_blocking
       ├─ Mutex<()>                       // 串行化同一工具的调用
       ├─ module.instantiate(&policy)     // 每次调用新实例：trap 不污染下一次调用
       ├─ call_buffer(BufferAbi, JSON)
       └─ UTF-8 + JSON 校验
  任意 Err → ToolResult::error(空 call_id, "wasm tool `name` failed: ...")
```

- 每次调用新实例，插件状态不跨调用保留；代价是 instantiate 与 start function 每次都执行。
- `WasmTool` 是 `Send + Sync`，但同一工具的调用用标准 `Mutex` 串行化：wasmtime epoch 是 Engine 级共享状态，并发 watchdog 可能互相误杀。不同 manifest 各自创建独立 `WasmEngine`。
- `register_wasm_tool(&mut ToolRegistry, manifest)` 是混合注册入口；native 与 wasm 共享 `BTreeMap` 名称空间，重名返回 `WasmToolError::Registry`。

**CLI 装配**：

- `HarnessConfig` 新增 `[wasm] manifests = [...]`，路径相对 workspace root，空路径在配置校验期拒绝。
- CLI 启动时在一次 `spawn_blocking` 中加载并编译全部 manifest；`CliRuntime` 持有 `Vec<Arc<dyn Tool>>`，每个 turn 再包一层 `GovernedTool`，policy/timeout/result-size 与 native 工具完全一致。
- `examples/wasm_tool.toml` 只允许 `echo_json`；注册 wasm 工具不会自动授予执行权限。

**示例插件**：`examples/plugins/echo/echo.wat` 是源码，`echo.wasm` 预编译提交，`build.sh` 用 wasm-tools 重建，`README.md` 说明 ABI。当前环境未安装 wasm-tools 或 wasm32 target，因此 `.wasm` 用缓存中的 `wat` crate 生成；WAT 即示例源码。

**测试（新增 22 个）**：

- `jessica-tools/tests/wasm_tool.rs` 18 个：echo JSON 往返；trap、fuel 耗尽、输出超限、越界、非法 JSON、非法 UTF-8、负分配指针、缺失导出均返回 `is_error` 且带原因；manifest 的写权限/零资源/缺失目录/相对 guest path/缺失 module/坏 JSON 校验；native+wasm 混合注册与重复名拒绝；示例文件存在。
- `jessica-cli/src/config.rs` 新增 3 个：wasm 配置解析、空路径拒绝、未知字段拒绝。
- `jessica-cli/tests/wasm_tool_e2e.rs` 1 个：真实 CLI 二进制 + 本地脚本化 SSE server。首轮模型调用未授权的 `read_file`，CLI 发 `PolicyDenied` 并回填错误结果；第二轮调用 `echo_json`，wasm 结果进入下一轮请求；最终输出 `wasm tool completed` 且 exit 0。

`cargo test --workspace` → 169 passed。手动 E2E 输出同样显示 `[policy] denied read_file`、`[tool:ok] {"text":"hello"}`、`SessionEnded: Completed`。

**依赖变化**：`jessica-tools` 增加路径依赖 `jessica-sandbox`（Task 3.3 允许）和 workspace 已有的 `thiserror`。未新增第三方 crate，未改动 `jessica-core`/`jessica-runtime`。

**已知限制（已记入 PLAN.md Deferred）**：每次调用新实例，无状态；ABI 无 dealloc/typed values；manifest schema 未做 JSON Schema 校验；三次 guest 操作各创建一个 watchdog 线程；CLI 外层 timeout 不能取消已进入 spawn_blocking 的 sandbox 调用；WASI 时钟/随机数仍非确定性。

### 3.14 Task 4.1：事件存储（jessica-memory）

**目标**：把 `AgentEvent` 以 append-only 方式持久化到 SQLite，为 Task 4.2 的会话恢复和 Task 4.3 的确定性重放提供唯一事实来源。

**依赖选择**：workspace 新增 `rusqlite = { version = "0.41", features = ["bundled"] }`。选择 rusqlite 而非 sqlx 的原因：

1. 事件存储只需要同步 SQL；rusqlite 的 API 更直接，不引入 async 运行时与迁移框架。
2. `bundled` 编译 SQLite amalgamation，构建不依赖系统 libsqlite3/开发头文件；代价是首次编译较慢。
3. 该 crate 仅被 `jessica-memory` 使用，不进入 `jessica-core`。

**Schema**：

```sql
CREATE TABLE IF NOT EXISTS events (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 1),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (session_id, seq)
) WITHOUT ROWID;
```

- `seq` 是 session 内从 1 开始的位置，不是全局序列；`PRIMARY KEY (session_id, seq)` 允许不同 session 各自从 1 开始。
- 两个 `BEFORE UPDATE` / `BEFORE DELETE` 触发器执行 `RAISE(ABORT, 'events are append-only...')`，在数据库层拒绝改写和删除，而不仅是依赖 API 约定。
- schema 在每次 `open` / `open_in_memory` 时用 `CREATE ... IF NOT EXISTS` 安装，没有版本表；Phase 4 内结构稳定，后续迁移需单开 Task。

**API 与语义**：

| API | 语义 |
| --- | --- |
| `EventStore::open(path)` | 打开/创建文件数据库并安装 schema |
| `EventStore::open_in_memory()` | 私有内存数据库，供测试与短生命周期工具 |
| `EventStore::append(&mut self, session, event, timestamp)` | 事务内计算 `MAX(seq)+1` 并插入，返回 `StoredEvent` |
| `EventStore::events(&self, session)` | 按 `seq ASC` 读回该 session 的全部事件 |
| `EventStore::last_seq(&self, session)` | 返回末尾序列；无事件时 `None` |

`StoredEvent` 同时保留：

- `session_id`、`seq`、`event_type`、`timestamp` 的列值；
- `payload_json` 的**原始字节串**，避免重放时重新序列化导致字节差异；
- 反序列化后的 `AgentEvent`，供恢复逻辑直接消费。

读取时校验 `event_type` 必须等于 payload 的 serde `type` 标签；不一致返回 `EventTypeMismatch`，损坏 JSON 返回带 session/seq 上下文的 `Decode`，不会静默跳过坏行。

**时间戳**：`Timestamp` 是调用方显式传入的 Unix 毫秒 newtype（`from_millis` / `as_millis`）。库内不读取 `SystemTime`，因此写路径的时间完全由调用方控制；Task 4.3 重放只读取已记录的时间戳。`Timestamp` 只用于列值，不进入 `AgentEvent`。

**错误模型**：`EventStoreError` 区分 `Open { path, source }`、`InMemory`、`Schema`、`Serialize`、`Append { session_id, source }`、`Read { session_id, source }`、`Decode { session_id, seq, source }`、`EventTypeMismatch { session_id, seq, stored, payload }`、`Sequence { session_id, value }`。库代码无 panic。

**并发与阻塞**：`append` 取 `&mut self`，单个 store 的写入串行；跨进程并发依赖 SQLite 的文件锁。Task 4.2 起连接配置了 5 秒 `busy_timeout`，锁冲突会先等待再报 `Append` 错误；仍未启用 WAL。API 是同步的；在 Tokio 上下文中调用方必须使用 `spawn_blocking`。

**测试（5 个行为测试，jessica-memory）**：

- `every_event_variant_round_trips_in_sequence_order`：全部 9 个 `AgentEvent` 变体写入后按 seq 读回；逐字段断言 session、seq、event_type、timestamp，payload 与直接 `serde_json::to_string` 字节一致，且 `type` 标签与列值一致。
- `sessions_have_independent_sequences_and_survive_reopen`：两个 session 交错追加得到各自的 1,2 / 1；关闭并重新打开文件数据库后事件仍在；未知 session 的 `last_seq` 为 `None`。
- `the_schema_has_the_required_columns_and_rejects_mutation`：`PRAGMA table_info` 断言五列；UPDATE 与 DELETE 均被触发器拒绝且原行保留。
- `reads_follow_sequence_order_even_for_out_of_order_inserts`：用原始 SQL 乱序插入 seq 2、1，`events` 仍按 `[1, 2]` 返回。
- `corrupt_payloads_are_reported_with_their_session_and_sequence`：坏 JSON 报 `Decode`，event_type 与 payload 不一致报 `EventTypeMismatch`，错误均带 session 与 seq。

`cargo test -p jessica-memory` → 5 passed；`cargo test --workspace` → 173 passed。未改动 `jessica-core`/`jessica-runtime`。

**已知限制（已记入 PLAN.md Deferred）**：同步 API；未启用 WAL；schema 无版本迁移；只实现 EventStore，core 的 `MemoryStore` 分层记忆仍未实现；时间戳由调用方提供，库不提供 `now()` 便捷方法。Task 4.2 为恢复新增了 `sessions` 元数据表与一个行为测试，见 §3.15。

### 3.15 Task 4.2：会话恢复（jessica-runtime / jessica-memory / jessica-cli）

**目标**：给定 session id，从 EventStore 重放到某个 seq，重建 messages 与循环状态并继续执行；恢复路径禁止重新调用 LLM 或工具，所有已完成工作的结果必须来自事件流。CLI 增加 `--resume <session_id>`。

**分层**：`jessica-runtime` 只允许依赖 `jessica-core`，不能直接读 SQLite，因此恢复拆成两层：

- runtime 的 `recover_session` 是对 `RecordedEvent` 的纯 reducer，不触碰存储、网络或时钟；
- CLI 的 `EventRecorder` 从 `jessica-memory::EventStore` 读取 `StoredEvent`，转换为 `RecordedEvent` 后交给 runtime；
- `jessica-memory` 仍只依赖 core，runtime 仍只依赖 core，没有新增反向依赖。

**初始消息缺口**：冻结的 `AgentEvent` 没有“用户消息”变体，system/user 引导消息无法从事件流重建。新增事件变体会修改 core 公共 API，因此本 Task 采用不可变 side table：

```sql
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    messages_json TEXT NOT NULL,
    started_at INTEGER NOT NULL
) WITHOUT ROWID;
```

UPDATE/DELETE 触发器拒绝修改，语义仍是 append-only。`record_session` 用 `INSERT OR IGNORE` 加回读比较实现幂等：同一 session 以相同消息重复登记成功，消息不同返回 `SessionConflict`；`session_messages` 在 JSON 损坏时返回带 session 的 `SessionDecode`。由此恢复 API 显式接收 `initial_messages`，core 保持不变。

**公共 API（jessica-runtime）**：

```rust
pub struct RecordedEvent { /* seq + AgentEvent */ }
pub struct SessionCheckpoint { /* session_id, messages, rounds_completed, last_seq, end_reason */ }
pub fn recover_session(
    session_id: &SessionId,
    initial_messages: Vec<Message>,
    events: &[RecordedEvent],
    through_seq: Option<u64>,
) -> Result<SessionCheckpoint, RecoveryError>;
```

`SessionCheckpoint` 通过只读访问器暴露 `session_id`、`messages`、`rounds_completed`、`last_seq`、`end_reason`；`into_parts` 为 crate-private，避免把内部布局固化成公共契约。

**Reducer 语义**：

- 只处理 `seq <= through_seq`（inclusive；`None` 表示全部）；seq 必须严格递增，否则 `NonIncreasingSequence`。
- 切片首事件必须是 `SessionStarted`（`MissingSessionStart`），session id 必须匹配（`SessionMismatch`）；未结束前再次 start 报 `UnexpectedSessionStart`；终态之后任何非 start 事件报 `EventAfterSessionEnd`。
- `ModelDelta` 累积当前轮文本；`ToolCallStarted` 登记 pending call；`ToolCallFinished` 配对结果。重复 call id、未知 result、重复 result 分别报 `DuplicateToolCall`、`UnknownToolResult`、`DuplicateToolResult`。
- `PolicyDenied`/`BudgetExceeded`/`AcceptanceFailed`/`Error` 是信息性事件，不改变消息，也不破坏 call/result 配对。
- 完整工具批次（所有 call 都有 result）在下一个 model delta 到来时 flush 成 `assistant_with_tool_calls` 加每条 `tool` 消息；`is_error` 结果重新加 `tool error: ` 前缀，与 CLI transcript 一致。
- `SessionEnded { Completed }`：无 pending 时，若有未 flush 的文本轮则追加 assistant 消息；有 pending 但缺结果则拒绝（`IncompleteRound`，runtime 不会以未完成工具调用正常结束）；完整批次则 flush。
- 其他终止原因（Cancelled/BudgetExceeded/AcceptanceFailed/Error）允许停在 call 与 result 之间：flush 时为缺失结果合成 `tool error: tool execution was interrupted`，并丢弃未完成文本。
- 无终态的切片必须停在稳定点：pending 缺结果报 `IncompleteRound`；只有文本、未形成完整轮报 `IncompleteModelTurn`；完整工具批次则 flush。绝不猜测缺失的模型输出或工具结果。

**稳定检查点规则**：只有“完整工具批次之后”或“终态事件”可作为恢复点。半个模型轮或半批工具调用的切片会被拒绝，因为继续执行必然要重新调用模型或工具，违反本 Task 禁令。

**rounds_completed 的确定性欠计数**：事件模型没有显式轮边界；连续多个“无文本、只发工具调用”的模型轮，与“一轮请求多个工具”在事件流中不可区分。恢复按 EventPump 的可观察批次计数，因此对同一事件流总是同一结果，且只可能少算、不可能多算。续跑使用 `max_rounds.saturating_sub(rounds_completed)`，宁可多给预算也不重复计费；该限制已记入 Deferred。

**AgentLoop::resume**：`run` 委托 `run_loop(..., rounds_used = 0, emit_started = true)`；`resume(checkpoint, cancel)` 取出 checkpoint 四元组后：

- `end_reason` 为 `Some` 时直接返回记录的原因，不构造任何 Provider/Tool 调用，也不发事件；
- 否则沿用 checkpoint 的 session id 与 messages，`emit_started = false`（同一 session 不重复发 `SessionStarted`），剩余轮数为 `max_rounds - rounds_completed`。

恢复后的 Provider 请求必须与不中断执行的对应请求逐字段一致，这是验收测试的核心断言。

**CLI 接入**：

- 配置新增 `[events] path`（可选，相对路径按 workspace root 解析）；缺省时不持久化，`--resume` 报 `ResumeRequiresEventStore`。
- `EventRecorder` 持 `Arc<std::sync::Mutex<EventStore>>`；打开、登记、追加、加载全部在 `spawn_blocking` 中执行；时间戳由 CLI 用 `SystemTime` 读取后传给 `EventStore`，库仍不读宿主时钟。
- `EventPump::handle` 在分发前持久化每个事件；`register` 同时设置 `current`，因此恢复流（没有新的 `SessionStarted`）仍能归到正确 session 并写回事件库。
- 新会话在第一次 attempt 前调用 `record_session` 登记 system/user 引导消息；acceptance 重试产生的外层 session 用空引导消息登记。
- `resume_turn` 加载事件并 `recover_session`；已终态则只打印原因并返回，否则重新装配同一 harness 后用 `AgentLoop::resume` 继续；恢复不经过 acceptance 重试循环。
- 参数支持 `--resume <id>` 与 `--resume=<id>`；重复或与 prompt 同时出现分别报 `DuplicateResume`/`ResumeWithPrompt`。恢复沿用原 session id，seq 继续递增，不写第二条 `SessionStarted`。

**测试（runtime 6 个行为测试 + CLI 2 个配置测试 + 1 个 E2E）**：

- `resumed_execution_matches_the_uninterrupted_tail_without_replaying_work`（验收主测试）：不中断执行三轮（工具 step1、工具 step2、文本 `finished`）；在第一次 `ToolCallFinished` 处恢复，断言 checkpoint 的 messages 与不中断执行第二次 Provider 请求完全一致；用全新的 Provider/Tool 续跑，断言续跑事件序列与不中断尾部逐事件相等、Provider 只看到剩余两轮、工具只执行 step2。
- `checkpoints_in_the_middle_of_a_turn_are_rejected`：缺 result 报 `IncompleteRound`，尾部半个文本轮报 `IncompleteModelTurn`。
- `an_ended_checkpoint_returns_the_recorded_reason_without_calling_anything`：完整流恢复得到 `Completed` 与最终 assistant 消息；续跑零 Provider/工具调用、零事件。
- `cancelled_rounds_keep_their_interrupted_tool_and_stop_resuming`：取消留下的半批调用生成与 CLI 相同的合成错误消息。
- `policy_and_budget_events_are_informational_during_replay`：治理事件不破坏 call/result 配对。
- `inconsistent_streams_are_rejected`：session 不匹配、seq 不递增、终态后事件、缺 `SessionStarted`。
- CLI 配置：`[events] path` 可选且能解析；空白路径与未知字段被拒绝。
- CLI `resume_e2e`：预置 SQLite（引导消息、`SessionStarted`、一个完整工具批次，无终态），运行真实 CLI `--resume`；断言只发一次模型请求、请求包含已记录的工具结果、stdout 含新答案、数据库 seq 4/5 追加 `ModelDelta` 与 `SessionEnded { Completed }`。

**结果**：`cargo test -p jessica-runtime` → 17 passed；`cargo test --workspace` → 183 passed；未修改 `jessica-core`。EventStore 的 `busy_timeout` 由“未配置”改为 5 秒（见 §3.14）。

**已知限制（已记入 PLAN.md Deferred）**：引导消息在 side table 而非事件流；轮边界不可观察导致 `rounds_completed` 欠计数；`--resume` 不能与新 prompt 组合；恢复不重跑 acceptance；attempt 之间的 `AcceptanceFailed` 没有 current session，不持久化；EventPump 广播 lag 可能丢事件；恢复使用当前配置，检查点之后的配置变更会改变预算/策略。

---

### 3.16 Task 4.3：离线确定性重放（jessica-cli）

**目标**：`replay --session <id> --out <path>` 读取事件流，用记录中的 LLM 响应与工具结果驱动循环，输出新的 JSONL 事件流；同一事件库两次重放必须字节级一致，路径中禁止当前时间、随机数与网络。

**分层**：全部实现在 `jessica-cli`，复用公开契约而不修改任何下层 crate：

- `EventRecorder` 从 SQLite 读出 `StoredSession`（初始消息 + `RecordedEvent` 列表）；
- `replay` 模块把事件流翻译成“模型脚本 + 工具结果队列”，再用真实 `AgentLoop::run` 驱动；
- `jessica-core`、`jessica-runtime`、`jessica-memory` 均无改动，没有新增依赖边。

`replay` 分支在 `OpenAiProvider::from_env` 与 wasm 插件加载之前分派，因此不需要 API key、不构造 provider、不读取网络配置。E2E 测试显式移除 `OPENAI_API_KEY`/`OPENAI_BASE_URL`/`OPENAI_MODEL` 后运行真实 CLI。

**脚本重建（`ScriptBuilder`）**：

- 校验 seq 严格递增、首事件为 `SessionStarted`、session id 匹配、拒绝重复 start 与终态后事件；任何不一致都返回结构化 `ReplayError`，而不是猜测。
- `ModelDelta` → `LlmEvent::TextDelta`；`ToolCallStarted` → `LlmEvent::ToolCallRequested`；`ToolCallFinished` → 按工具名与调用顺序入队；`Error` → `LlmEvent::Error` 并标记该轮失败。
- 工具批次完成（所有 call 都有 result）后，下一个模型事件开启新一轮；未完成批次遇到轮边界或终态报 `IncompleteRound`。
- 每轮末尾补 `LlmEvent::Done { usage: None }`：冻结的 `AgentEvent` 不持久化 usage，因此不能还原 token 计数（已记入 Deferred）。
- 终止原因按可观察轨迹重建：
  - `Completed`：最后一个可观察轮是文本轮；若记录停在完整工具批次（真实的 Completed 需要之后一次空响应），补一个空模型轮；空会话同样补空轮。
  - `Error`：要求记录中确实出现过 `Error` 事件。
  - `BudgetExceeded`：仅支持 max-rounds 型，用脚本轮数作为 `max_rounds`，让真实循环自行产生 `BudgetExceeded` 与终态。
  - `Cancelled`、`AcceptanceFailed` 与无终态流明确拒绝（`UnsupportedEndReason`/`MissingSessionEnd`），不伪造不可重建的状态。

**脚本化 Provider 与工具**：

- `ScriptedProvider` 每次 `stream_chat` 弹出一轮预录 `LlmEvent`；脚本耗尽返回 `LlmError::Request` 并置 `exhausted`，事后映射为 `ScriptExhausted`。
- 按记录中出现的工具名各注册一个 `ReplayTool`。`execute` 只弹出该工具下一条记录结果，绝不执行真实工具；队列按调用顺序，保证 AgentLoop 生成的 `ToolCallFinished` 与记录逐字段一致。
- 重放结束做四项闭合检查：脚本轮全部消耗、工具结果全部消耗、无缺失结果、循环终止原因与记录一致；任一不满足即报错，避免“重放悄悄偏离”。

**JSONL 输出**：每行一个紧凑 `AgentEvent` JSON 对象，不写 seq/timestamp，顺序即记录顺序。serde 字段顺序与内部标签固定，`render_jsonl` 是纯函数，因此两次重放字节一致。写文件用 `spawn_blocking` + `std::fs::write`，不在 async 上下文阻塞；广播缓冲 1024 个事件，lag 时报 `EventLag` 而不是静默截断。

**CLI 参数**：支持 `--session <id>`/`--session=<id>` 与 `--out <path>`/`--out=<path>`；缺失或重复分别报 `MissingReplaySession`/`DuplicateReplaySession`/`MissingReplayOut`/`DuplicateReplayOut`；`replay` 需要 `[events]` 配置（`ReplayRequiresEventStore`），未知 session 报 `UnknownSession`。成功时输出 `[replay] wrote N events for session ...`。

**测试（CLI 3 个单元 + 1 个 E2E）**：

- `replay_reproduces_recorded_events_without_running_tools_or_the_model`：完整“工具轮 + 文本轮”会话重放两次，断言事件完全相等且 JSONL 字节一致。
- `replay_covers_empty_final_turns_budget_and_error_terminals`：覆盖空最终轮、max-rounds `BudgetExceeded` 与 provider `Error` 三种终止。
- `incomplete_or_unsupported_recordings_are_rejected`：缺工具结果、`Cancelled`、无终态分别被拒绝。
- `replay_e2e`：预置 SQLite（system/user 引导消息与 6 个事件），移除全部 `OPENAI_*` 后运行真实 CLI 两次；断言两次 JSONL 字节相同、stdout 报告 6 个事件、内容等于记录事件。

**结果**：`cargo test --workspace` → 187 passed（CLI 33）；手动验收中两次 `replay` 输出 `diff` 退出 0，JSONL 6 行与记录事件一致；未修改 `jessica-core`。

**已知限制（已记入 PLAN.md Deferred）**：只重建 AgentLoop 层，CLI 侧的 `PolicyDenied`、`AcceptanceFailed` 与 token/result-size 型 `BudgetExceeded` 不重新发出（其效果已体现在记录的工具结果或终止原因中）；usage 未持久化导致 token 计量不可重放；只支持 Completed/Error/max-rounds BudgetExceeded；JSONL 无 seq/timestamp；1024 缓冲对大 session 可能 lag；重放的是运行时事件而非 provider 原始 wire log。

---

## 4. 横切设计

### 4.1 错误模型

| 层 | 类型 | 语义 |
| --- | --- | --- |
| core | `LlmError::{Request, Stream, InvalidResponse}` | Provider 请求/流/响应错误 |
| core | `ToolRegistryError::Duplicate` | 工具注册冲突 |
| core | `MemoryError::Backend` | 记忆后端错误 |
| providers | `ApiConfigError::{MissingEnv, Empty, InvalidUrl}` | 配置加载/校验错误 |
| 工具执行 | `ToolResult { is_error: true }` | 工具失败，回填给模型，不终止循环 |

约定：

- 所有错误用 `thiserror` 定义结构化 enum；
- 库代码无 `unwrap`/`expect`（测试除外）；
- 运行时把 Provider 错误转换为 `AgentEvent::Error` 并终止，而不是 panic；
- 不吞掉 WASM trap（Phase 3 要求转为 `ToolResult { is_error: true }`）。

### 4.2 确定性

- `ToolRegistry` 与 `ToolCallAssembler` 使用 `BTreeMap`，迭代/输出顺序稳定。
- 消息与事件的 serde 字段顺序固定；内部标签 `type` 固定。
- `AgentEvent` 本身不含时间戳或随机数；`EventStore::append` 要求调用方显式传入 Unix 毫秒时间戳并原样持久化，库不读取宿主时钟（Task 4.1）。
- `replay` 路径只读事件库：脚本化 Provider 不发请求、ReplayTool 不执行工具、输出不写时间戳或随机数；同一库两次重放必得字节一致的 JSONL（Task 4.3）。
- `recover_session` 是纯 reducer：只读 `RecordedEvent`、引导消息与 `through_seq`，不读时钟、不发网络请求、不调用 LLM/工具；同一事件切片必得同一 checkpoint（Task 4.2）。CLI 只在“追加新事件”这一持久化边界读取宿主时钟，恢复本身不读。
- Provider 的 SSE 解析不依赖 chunk 边界，输入相同字节流必得相同事件序列；OpenAI 与 Anthropic 两个 Provider 分别按 `tool_calls[].index` 与 `content_block.index` 用有序 map 聚合，工具输出顺序稳定。
- Anthropic 请求映射是纯函数：system 合并、content block 转换与 tool schema 解包均不读时钟/随机数；同一 `Message` 序列产生相同 JSON body。

### 4.3 安全与权限

- API key 仅从环境变量读取，Debug 脱敏，不写入事件或日志。
- `DenyAll` 是默认 Policy，能力必须显式授予。
- `ToolResult` 的 `is_error` 让越权/失败可被模型观察，但最终权限判定在 Policy/Guard，不依赖模型自觉。
- Phase 3 WASM：默认无文件预打开、无网络；按清单授予只读目录；fuel + 墙钟 + 内存/表/实例上限。
- 工具输出大小与超时由 Phase 2 的 ResultSizeGuard / ToolTimeoutGuard 约束。

### 4.4 可观测性与事件溯源

- `AgentEvent` 是唯一事实来源；CLI、日志、UI 都是消费者。
- 所有事件可序列化；Task 4.1 的 EventStore 按 `(session_id, seq, event_type, payload_json, timestamp)` 追加写入，并用数据库触发器拒绝 UPDATE/DELETE（见 §3.14）。
- 会话恢复与离线重放只读事件流，不重新调用 LLM/工具（Task 4.2/4.3）。`replay` 把事件流解释成模型脚本与工具结果队列，再经真实 `AgentLoop` 重放并输出 JSONL；输出不含 seq/timestamp，顺序由行序表达。
- 结构化日志使用 `tracing`；CLI 面向用户的输出除外。

### 4.5 依赖与工具链

- 第三方依赖集中在根 `[workspace.dependencies]`，成员通过 `workspace = true` 继承。
- 新增依赖必须修改根清单并说明理由（目前已追加 `reqwest` 与 `tokio-util`；Task 1.3 为 CLI 读取 stdin 启用 tokio `io-std` feature；Task 2.4 增加 `toml`，仅用于 CLI 解析 harness 配置；Task 3.1 增加 `wasmtime`，关闭默认 features，只启用 `cranelift`/`runtime`/`std`；Task 3.2 增加 `wasmtime-wasi`，只启用 `p1`，其内部依赖 `p2` 并因此启用 wasmtime 的 `component-model`/`async`；Task 3.3 未新增第三方 crate，只给 `jessica-tools` 增加内部 `jessica-sandbox` 路径依赖并使用已有的 `thiserror`；Task 4.1 增加 `rusqlite` 0.41，启用 `bundled` 编译内置 SQLite，仅由 `jessica-memory` 使用；Task 4.2 未新增第三方 crate，只给 `jessica-runtime` 增加对 workspace 已声明的 `thiserror` 的依赖；Task 4.3 未新增第三方 crate，重放只使用 CLI 已有的 tokio/async-trait/futures/serde_json/thiserror 与 runtime 公共 API；Task 1.1b 未新增第三方 crate，Anthropic Provider 复用 Task 1.1 已批准的 reqwest，测试额外使用 workspace 已有的 tokio dev-dependency）。
- 工具链固定 Rust 1.99.1；验收门槛：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。
- 当前工作区外部依赖：tokio、serde、serde_json、thiserror、tracing、async-trait、futures、reqwest、tokio-util、toml、wasmtime、wasmtime-wasi、rusqlite。

### 4.6 测试策略

- 行为测试优先：构造真实边界条件，断言可观察行为；禁止“调用后断言返回 Ok”。
- mock 必须模拟真实协议边界：SSE 分片、跨 chunk 多字节字符、缺失 `[DONE]`、非法 JSON、重复注册、缺 id 的工具调用。
- 序列化契约用 JSON 往返 + `type` 标签断言。
- async Trait 用 `futures::executor::block_on` + mock 实现验证 dyn 派发与行为。

---

## 5. 后续阶段的接口预留与实现约束

> 以下是计划中尚未实现的阶段；本节只记录已冻结契约如何支撑它们，以及已知的待决点。

### 5.1 Phase 1.2：Agent 循环（✅ 已实现，见 §3.5）

- 输入：初始 `Vec<Message>`、`Arc<dyn LlmProvider>`、`Arc<ToolRegistry>`、`Budget`、`CancellationToken`。
- 循环：`stream_chat` → 消费 `LlmEvent` → 有工具调用则通过 `ToolRegistry` 执行并回填 `Message::tool` → 重复；无工具调用或 `max_rounds` 用尽终止。
- 所有状态变化通过 `broadcast::Sender<AgentEvent>` 发出。
- `CancellationToken` 由 `tokio-util`（`rt` feature）提供，已在 Task 1.2 的 workspace 依赖中说明理由。
- **遗留接口缺口**：token usage 尚未进入事件或运行时状态；Phase 2 的 TokenBudgetGuard 需通过包装 Provider 或扩展 runtime 接入，见 PLAN.md Deferred。

### 5.2 Phase 1.3：CLI REPL（✅ 已实现，见 §3.6）

- 已装配 `OpenAiProvider` + 原生工具（read_file / write_file / bash）。
- bash 自带超时与输出大小上限；Phase 2 的 ToolTimeoutGuard / ResultSizeGuard 将接管治理层。
- 已订阅 `AgentEvent` 流式打印文本与工具摘要；Ctrl+C 通过 `CancellationToken` 触发取消。
- 当前为最小 REPL：每次输入独立 session，无跨轮历史；TUI 与复杂参数解析未实现。

### 5.3 Phase 2：Harness 治理

- **Task 2.1 已实现**：`Harness` 持有 `Vec<Arc<dyn Hook>>`，自持 broadcast 通道并包裹任意 agent future；Hook 按注册顺序在 session 生命周期与每个事件上调用。见 §3.7。
- **Task 2.2 已实现**：`Harness` 增加 `Vec<Arc<dyn Guard>>` 与 `Arc<dyn Policy>`（默认 `DenyAll`），提供 `check_guards` / `authorize_tool_call` 显式检查点；四种 Guard 与 `AllowlistPolicy` 见 §3.8。
- **Task 2.3 已实现**：`Harness` 增加 `SensorPipeline`、`with_sensors` / `run_sensors`；`CommandSensor` 与 `AcceptanceRunner` 提供工具后观察和模型停止后的验收重入，见 §3.9。
- **Task 2.4 已实现完整装配**：CLI 的 `TokenCountingProvider`、`GovernedTool`、`EventPump`、`AcceptanceRunner` 共同接入 Guard/Policy/Sensor/Acceptance，见 §3.10。
- 因 harness 只能依赖 `jessica-core`，Harness 不直接引用 runtime 的 `AgentLoop`；`Harness::run` 仍不会自动执行 Guard/Policy/Sensor/Acceptance，CLI 通过显式检查点驱动。新入口需自行完成同样的装配（见 PLAN.md Deferred）。
- 实际顺序：`ToolCallStarted` 事件 → EventPump 的 `check_guards` → `GovernedTool` 的 Policy 授权 → 超时受控的工具执行 → ResultSizeGuard → `ToolCallFinished` 事件 → CommandSensor → attempt 结束后 AcceptanceRunner。
- 所有超限以 `AgentEvent::BudgetExceeded { kind, message }` 表达并取消 session；策略拒绝以 `AgentEvent::PolicyDenied { call, reason }` 表达，返回错误 `ToolResult` 但不终止会话。
- 验收失败：把 `AcceptanceResult.feedback` 作为新 `Message::user` 注入并重入循环；`AcceptanceRunner` 发 `AcceptanceFailed`，最终由 CLI 外层 `SessionEnded` 承载最终 reason。

### 5.4 Phase 3：WASM 沙箱

- **Task 3.1 已实现**：`jessica-sandbox` 引入 wasmtime 并完成加载/实例化/`call_i32`，见 §3.11。
- **Task 3.2 已实现**：WASI preview 1 默认无 preopen/无网络，按 policy 授予只读目录；每 Store 配置 fuel、墙钟、内存/表/实例上限，见 §3.12。
- **Task 3.3 已实现**：`jessica-sandbox` 增加 buffer ABI（guest `alloc`/`invoke`，返回指针与长度打包的 `i64`）；`jessica-tools::WasmTool` 从 JSON manifest 读取 name、schema、ABI 与权限并适配 `Tool` Trait，所有 trap/资源超限/非法输出都转为 `ToolResult { is_error: true }`，见 §3.13。
- `register_wasm_tool` 让 native 与 wasm 工具共享同一个 `ToolRegistry`；CLI 的 `[wasm]` 配置在启动时加载插件，但工具名仍须出现在 `policy.allowed_tools` 中才可执行。
- `examples/plugins/echo/` 提供 WAT 源码、manifest 与构建脚本，`examples/wasm_tool.toml` 支撑 CLI 端到端验收（含未授权 native 工具被拒绝）。

### 5.5 Phase 4：事件溯源与重放

- **Task 4.1 已实现**：`jessica-memory` 使用 rusqlite（bundled SQLite）实现 append-only `EventStore`，按 `(session_id, seq, event_type, payload_json, timestamp)` 存储，保留 payload 原始字节并在读取时校验 serde 标签，见 §3.14。
- **Task 4.2 已实现**：`jessica-runtime::recover_session` 从事件流重建 messages 与循环状态，`AgentLoop::resume` 从稳定检查点续跑且不重复调用已完成的工作；CLI 持久化事件并支持 `--resume <session_id>`。恢复只读 RecordedEvent，禁止重新调用 LLM/工具，见 §3.15。
- **Task 4.3 已实现**：CLI `replay --session <id> --out <path>` 用记录中的模型响应与工具结果驱动真实 `AgentLoop`，输出 JSONL；两次重放字节级一致，路径中没有时间/随机数/网络，见 §3.16。Phase 4 至此完成。

### 5.6 已知遗留

- Anthropic Provider 未实现 Prompt caching（`cache_control`）、thinking block 与多模态 image block；未知 content block 被忽略。`max_tokens` 固定为 8192，尚无配置项。CLI 通过 `JESSICA_PROVIDER=openai|anthropic` 选择 Provider（默认 `openai`），该开关是 Task 1.1b 之外为真实运行新增的，尚未写入正式 PLAN Task。
- `jessica-memory` 只实现 EventStore；core 的 `MemoryStore` 分层记忆（短期/FTS5/向量）留待 Phase 5。EventStore 同步 API，异步调用方必须 `spawn_blocking`；SQLite 已配置 5 秒 `busy_timeout` 但未启用 WAL，schema 无版本迁移。
- 引导消息（system/user）不在 `AgentEvent` 流中，而存于不可变 `sessions` side table。EventStore 的事件流因此不是“完整请求”的充分来源；Task 4.3 的重放也必须同时读取该表。
- `replay` 只重建 AgentLoop 层：CLI 侧的 `PolicyDenied`、`AcceptanceFailed` 与 token/result-size 型 `BudgetExceeded` 不会重新发出，其可观察效果已固化在记录的工具结果或终止原因中。
- `LlmEvent::Done` 的 usage 未持久化，重放统一使用 `usage: None`，因此 token 计量与依赖 usage 的 Guard 不能靠重放复现。
- `replay` 只接受 `Completed`、`Error` 与 max-rounds 型 `BudgetExceeded` 终止；`Cancelled`、`AcceptanceFailed` 与无终态流被明确拒绝，不做猜测性补全。
- 重放 JSONL 不含 seq 与 timestamp，只以行序表达顺序；1024 事件缓冲对大 session 可能 lag 并报 `EventLag`；输出是运行时事件而非 provider 原始 wire log。
- `rounds_completed` 只能按可观察工具批次计数：连续的无文本工具轮会被合并计为一轮。该欠计数是确定性的，但恢复后的 `max_rounds` 可能比不中断执行多出若干轮预算。
- `--resume` 不能与新 prompt 组合，且恢复路径不经过 acceptance 重试循环；需要 acceptance 的会话不能靠 `--resume` 续跑。
- acceptance 重试之间的 `AcceptanceFailed` 事件发生时没有 current session，EventPump 不会持久化它们；恢复后的重放看不到这些失败尝试。
- EventPump 广播通道有界；lag 丢事件不仅使 transcript 不完整，也会使持久化的事件流缺行，恢复时可能报配对/序列错误。当前只在 persist 失败时打印警告，不中止会话。
- 恢复使用当前配置（`max_rounds`、policy 等）；检查点之后修改配置会改变续跑行为，检查点本身不携带配置快照。
- Buffer ABI 只传递一段 UTF-8 JSON 字节（输出为 JSON 文本），不支持 component model、typed values、跨调用状态或显式 dealloc；guest 自行管理输入缓冲区，宿主只校验指针/长度范围。
- Sandbox 的加载/实例化/调用仍是同步 API 且 `load_file` 使用 `std::fs`；wasmtime-wasi 的 p1 同步 shim 在 Tokio worker 上直接执行 WASI hostcall 会 panic。`WasmTool::load` 由 CLI 用 `spawn_blocking` 调用，`WasmTool::execute` 每次实例化与调用也在 `spawn_blocking` 中执行。
- manifest 的权限声明会映射到 `SandboxPolicy`，并拒绝未知字段（包括任何写目录声明）；但不做 JSON Schema 校验，`schema` 只要求是 JSON object 并原样传给 Provider。
- CLI 外层工具超时无法取消正在执行的 `spawn_blocking`；manifest 的 `wall_timeout_ms` 应不大于 CLI 的 `tool_timeout_ms`，避免外层先超时后 wasm 仍占用线程。
- manifest 的 `max_result_bytes`（默认 64 KiB）与 CLI `ResultBudget` 同时生效，实际可见结果受两者较小值约束。
- 每次 sandbox 操作创建并 join 一个 watchdog OS 线程；高频调用场景可考虑共享 epoch ticker，但需重新验证不会残留 stale epoch。
- wasmtime 的 epoch 是 Engine 级共享状态：多个实例共享同一 `WasmEngine` 并发执行时，一个实例的墙钟 watchdog 可能让另一个实例提前 trap。`WasmTool` 因此为每个工具持有一把 `Mutex` 串行化调用，并为每个 manifest 创建独立 `WasmEngine`；不要把同一个 Engine 共享给多个 `WasmTool`。
- 每次 `WasmTool::execute` 的 instantiate、alloc、invoke 各创建一个 watchdog OS 线程（共 3 个），高频工具调用场景需要合并 epoch ticker。
- WASI 默认使用宿主时钟与随机数；若 wasm 工具结果要参与确定性重放，需要单开 Task 注入确定性 clocks/random。
- `jessica-sandbox/tests/fixtures/build.sh` 与 `examples/plugins/echo/build.sh` 依赖 wasm-tools（当前环境未安装）；预编译 `.wasm` 已提交，测试与 CLI 示例不依赖该工具（示例 `.wasm` 由缓存的 `wat` crate 离线编译）。
- Provider 尚未做真实网络联调（环境无 API key）；Task 1.3 已用本地 mock SSE server 完成端到端验收，真实 key 联调仍待进行。
- 文件工具存在 canonicalize 与实际读写之间的 TOCTOU 竞态；bash 超时只杀直接子进程而非进程组。两者由 Phase 2 Policy / Phase 3 沙箱进一步约束。
- Task 2.4 已在 CLI 中通过 Provider/Tool 包装与 EventPump 接入完整治理链；`Harness::run` 本身仍不自动执行 Guard/Policy/Sensor/Acceptance，新入口需重复装配。
- TokenBudgetGuard 依赖 Provider 上报 usage；缺失 usage 时无法计费。广播通道有界，EventPump lag 会导致 transcript 不完整（恢复场景的后果见上方事件溯源条目）。
- StuckDetectionGuard 在 CLI 的 ToolCallStarted 检查点把任何工具调用视为进展，纯工具循环实际由 runtime 的 `max_rounds` 兜底；`max_rounds` 按 attempt 计数，acceptance 重试会重置。
- CommandSensor 与 CommandAcceptanceCheck 暂无超时；AcceptanceCheck 输出截断 8 KiB，CommandSensor 无输出上限；挂起的检查命令可无限阻塞。
- Allowlist 拒绝以错误 `ToolResult` 返回并继续会话，而不是终止；若需要“拒绝即终止”的语义，需单独定义 Policy 行为。
- `README.md` 首句故意保留拼写错误，作为示例 acceptance 的夹具；真实 API key 联调仍待进行。
- 工具调用参数在 Provider 侧一次性解析为 `Value`；若后续需要保留原始字符串用于审计，可在事件层追加，而不修改 `ToolCall`。

---

## 6. 目录结构

```text
jessica/
├── CLAUDE.md               # 跨会话约束
├── PLAN.md                 # 任务清单与进度
├── DESIGN.md               # 本文档
├── Cargo.toml              # workspace + 集中依赖版本
├── Cargo.lock
├── rust-toolchain.toml     # Rust 1.99.1
├── .gitignore
├── jessica-core/           # 冻结契约层
│   └── src/
│       ├── lib.rs
│       ├── types.rs
│       ├── llm.rs
│       ├── tool.rs
│       ├── policy.rs
│       ├── guard.rs
│       ├── sensor.rs
│       ├── hook.rs
│       ├── acceptance.rs
│       └── memory.rs
├── jessica-providers/      # OpenAI / Anthropic 兼容 Provider
│   ├── src/
│   │   ├── lib.rs
│   │   ├── config.rs
│   │   ├── request.rs
│   │   ├── sse.rs
│   │   ├── stream.rs
│   │   ├── provider.rs
│   │   └── anthropic/
│   │       ├── mod.rs
│   │       ├── config.rs
│   │       ├── request.rs
│   │       ├── sse.rs
│   │       ├── stream.rs
│   │       └── provider.rs
│   └── tests/
│       └── fixtures/
│           ├── anthropic_sse_text_and_tool.txt
│           └── anthropic_sse_error.txt
├── jessica-runtime/        # Agent 循环与会话恢复
│   ├── src/
│   │   ├── lib.rs
│   │   ├── agent.rs        # AgentLoop::run / resume
│   │   └── recovery.rs     # RecordedEvent、SessionCheckpoint、recover_session
│   └── tests/
│       ├── agent_loop.rs
│       └── session_recovery.rs
├── jessica-harness/        # Middleware 装配
│   ├── src/
│   │   ├── lib.rs
│   │   ├── harness.rs
│   │   ├── logging.rs
│   │   ├── guards.rs
│   │   ├── policy.rs
│   │   ├── sensors.rs
│   │   └── acceptance.rs
│   └── tests/
│       ├── harness_chain.rs
│       ├── governance.rs
│       └── sensors_acceptance.rs
├── jessica-sandbox/        # wasmtime 沙箱与 WASI 能力控制
│   ├── src/
│   │   ├── lib.rs          # Engine/Module/Instance、limits、watchdog、错误
│   │   └── policy.rs       # SandboxPolicy / ReadOnlyDir
│   └── tests/
│       ├── wasm_loading.rs
│       ├── wasi_capabilities.rs
│       ├── resource_limits.rs
│       └── fixtures/
│           ├── fixed_value.wat / fixed_value.wasm
│           ├── wasi_fs.wat / wasi_fs.wasm
│           ├── spin.wat / spin.wasm
│           ├── limits.wat / limits.wasm
│           └── build.sh
├── jessica-tools/          # 原生工具与 WASM 工具适配器
│   ├── src/
│   │   ├── lib.rs
│   │   ├── file.rs
│   │   ├── bash.rs
│   │   └── wasm.rs         # WasmTool、manifest 解析、register_wasm_tool
│   └── tests/
│       ├── native_tools.rs
│       ├── bash_tool.rs
│       ├── wasm_tool.rs
│       └── fixtures/
│           ├── abi_behaviors.wat / abi_behaviors.wasm
│           └── build.sh
├── jessica-memory/         # 事件存储（Phase 4）
│   ├── src/
│   │   └── lib.rs          # EventStore、StoredEvent、Timestamp、sessions 元数据、触发器
│   └── tests/
│       └── event_store.rs  # 9 种事件往返、重开、append-only、顺序、损坏数据、会话元数据
├── jessica-cli/            # 可执行入口与完整 harness 装配
│   ├── src/
│   │   ├── main.rs         # 参数解析、Provider 创建、one-shot / REPL
│   │   ├── app.rs          # 单 turn 装配、wasm 插件加载、acceptance 重试、resume
│   │   ├── config.rs       # TOML 配置与校验（含 [wasm]、[events]）
│   │   ├── events.rs       # EventRecorder：SQLite 加载/追加与 spawn_blocking 桥接
│   │   ├── governance.rs   # TokenCountingProvider / GovernedTool
│   │   ├── pump.rs         # 事件打印、transcript、持久化、Guard/Sensor 检查点
│   │   ├── replay.rs       # 离线确定性重放：脚本化 Provider/工具、JSONL
│   │   └── acceptance_check.rs
│   └── tests/
│       ├── wasm_tool_e2e.rs
│       ├── resume_e2e.rs
│       └── replay_e2e.rs
├── examples/
│   ├── harness.toml        # 示例治理配置
│   ├── wasm_tool.toml      # wasm 工具示例配置
│   └── plugins/
│       └── echo/
│           ├── echo.wat / echo.wasm
│           ├── manifest.json
│           ├── build.sh
│           └── README.md
└── README.md               # 使用说明与验收夹具
```

---

## 7. 构建与验收

```bash
# 完整验收
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# 单 crate
cargo test -p jessica-core
cargo test -p jessica-providers
```

当前基线（2026-11-11）：

```text
jessica-core       30 tests passed
jessica-providers  57 tests passed
jessica-runtime    17 tests passed
jessica-tools      34 tests passed
jessica-harness    24 tests passed
jessica-cli        36 tests passed
jessica-sandbox    17 tests passed
jessica-memory      6 tests passed
workspace         221 tests passed, 0 failed
clippy/fmt         clean
```

> 注：workspace 的 221 个测试均为已实现部分的行为测试。Task 4.1 用 5 个 EventStore 行为测试替换了 memory 模板自带的 1 个 `it_works`；Task 4.2 新增 6 个 runtime 恢复测试、1 个 memory 会话元数据测试、2 个 CLI 配置解析测试和 1 个 CLI `--resume` E2E（净增 10）；Task 4.3 新增 3 个 replay 单元测试与 1 个 CLI `replay` E2E（净增 4）；Task 1.1b 新增 31 个 Anthropic Provider 测试（净增 31）；CLI 的 `JESSICA_PROVIDER` 选择新增 3 个解析测试（净增 3）；另有 2 个需真实 key/网络的 Anthropic live 测试默认 ignored，不计入 221。

---

## 8. 提交记录（已完成 Task）

| Commit | Task | 内容 |
| --- | --- | --- |
| `36550bd` | Task 0 + 0.1 | CLAUDE.md、workspace 骨架、工具链固定 |
| `3227cd5` | Task 0.2 | jessica-core 冻结契约与 30 个测试 |
| `79bdda7` | Task 1.1 | OpenAI 兼容流式 Provider 与 26 个测试 |
| `6dc544a` | Task 1.2 | jessica-runtime Agent 循环与 11 个行为测试 |
| `3458ac3` | Task 1.3 | 三个原生工具、最小 CLI、16 个工具行为测试与 mock SSE 端到端验收 |
| `68a5850` | Task 2.1 | Harness Hook 装配、LoggingHook 与 5 个行为测试 |
| `8397b4a` | Task 2.2 | 四种 Guard、AllowlistPolicy、显式治理检查点与 10 个行为测试 |
| `6515e63` | Task 2.3 | Sensor 管线、CommandSensor、AcceptanceRunner 与 9 个行为测试 |
| `49214e7` | Task 2.4 | CLI 完整 harness 装配、TOML 配置、22 个测试与 mock SSE 端到端验收 |
| `49765f3` | Task 3.1 | wasmtime 加载/实例化/调用、预编译 fixture 与 6 个行为测试 |
| `677bffa` | Task 3.2 | WASI 只读能力、fuel/墙钟/内存/表/实例限制、3 组 fixture 与 17 个 sandbox 测试 |
| `8c4f522` | Task 3.3 | WASM buffer ABI、manifest 驱动的 `WasmTool`、混合注册与 18 个 wasm 工具测试 |
| `85d6c0b` | Task 4.1 | append-only SQLite EventStore、payload 原样保存与 5 个行为测试 |
| `3336527` | Task 4.2 | 事件流恢复、`AgentLoop::resume`、`sessions` 元数据、CLI 持久化与 `--resume`、183 个测试 |
| `7c2f5e5` | Task 4.3 | 离线确定性重放、脚本化 Provider/工具、JSONL 输出与 `replay` 子命令、187 个测试 |
