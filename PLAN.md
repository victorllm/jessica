Jessica 开发计划（Claude Code 可执行版）

Jessica — a deterministic agent runtime that converges execution to the verifiable upper bound under budget, permission, and acceptance constraints.

中文定位：Jessica —— 一个确定性 Agent 运行时：在预算、权限与验收约束下，把执行收敛到可验证结果的上界。

---

📋 使用方式

把这份文档保存为仓库根目录的 PLAN.md。先让 Claude Code 执行 Task 0 生成 CLAUDE.md，之后每次会话只执行一个 Task，执行完由你 review 并 commit，再开始下一个。每个 Task 都包含可直接运行的验收命令，Claude Code 应自己跑通再声明完成。

---

Task 0：项目宪法（CLAUDE.md）

目标：建立跨会话的持久约定，让后续每个 Task 都有一致的约束来源。

操作：在仓库根目录创建 CLAUDE.md，内容包含：

· 项目一句话定位：Jessica 是一个确定性 Rust Agent 运行时，WASM 沙箱扩展，事件溯源持久化。它的信条是：不信模型自述，只信可验证轨迹。
· Workspace 布局与依赖单向规则：jessica-core 不得依赖任何其他内部 crate；jessica-runtime/jessica-sandbox/jessica-harness 只依赖 jessica-core；jessica-cli 依赖所有实现层。
· 编码纪律：所有公共 API 必须有 doc comment；错误用 thiserror 定义结构化 enum，禁止 unwrap/expect（测试除外）；异步用 Tokio，禁止阻塞调用混入 async 上下文。
· 禁止事项：不得引入未在 Cargo.toml 中显式批准的第三方 crate；不得提前实现后续 Phase 的 Trait 方法；不得修改已冻结的 jessica-core 公共 API（变更需走单独 Task）。
· 验收命令速查：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace。
· 每个 Task 完成后必须更新 PLAN.md 中的复选框状态。

验收：CLAUDE.md 存在，且被 Claude Code 在后续会话中主动引用（可在 Task 1 开始时让它复述约束来验证）。

禁止：不要在 Task 0 中写任何 Rust 代码。

---

Phase 0：Workspace 与契约（2 个 Task）

Task 0.1：Workspace 骨架

操作：

· 创建根 Cargo.toml，[workspace] members = ["jessica-core", "jessica-runtime", "jessica-sandbox", "jessica-harness", "jessica-providers", "jessica-tools", "jessica-memory", "jessica-cli"]，resolver = "2"。
· 每个 crate 用 cargo new --lib jessica-xxx 创建（jessica-cli 用 --bin），Cargo.toml 中按 CLAUDE.md 的依赖规则声明内部依赖。
· 根目录加 rust-toolchain.toml 固定版本，.gitignore 忽略 target/。
· 根 Cargo.toml 的 [workspace.dependencies] 集中管理版本：tokio、serde、serde_json、thiserror、tracing、async-trait、futures。

验收：

```bash
cargo build --workspace
cargo tree -p jessica-core --depth 1   # 确认 jessica-core 无内部依赖
cargo tree -p jessica-runtime --depth 1 # 确认只依赖 jessica-core
```

禁止：不要在 Phase 0 引入 wasmtime、sqlx 等后续才需要的重依赖。

Task 0.2：jessica-core Trait 契约

操作：在 jessica-core 中定义以下内容，每个 Trait 方法先只定义最小必要集合：

· types.rs：Message（role/content/tool_calls）、ToolCall、ToolResult（含 is_error）、AgentEvent（enum，至少含 ModelDelta、ToolCallStarted、ToolCallFinished、SessionStarted、SessionEnded、BudgetExceeded、AcceptanceFailed）、SessionId、Budget（token 上限、工具超时、最大轮次）。
· llm.rs：LlmProvider Trait，方法 stream_chat(&self, messages, tools) -> Result<BoxStream<'static, LlmEvent>>，LlmEvent 为 TextDelta/ToolCallRequested/Done/Error。
· tool.rs：Tool Trait，方法 name()、schema() -> serde_json::Value、async execute(&self, args) -> ToolResult；以及 ToolRegistry 的注册/查找接口。
· policy.rs：Policy Trait，方法 check(&self, call: &ToolCall) -> PolicyDecision，PolicyDecision 为 Allow/Deny(String)；提供默认实现 DenyAll。
· guard.rs、sensor.rs、hook.rs、acceptance.rs：各自定义 Trait 骨架，方法签名在 Phase 2 之前保持不变。
· memory.rs：MemoryStore Trait，先只定义 write(&self, session, entry) 和 recall(&self, session, query, limit)。

为每个公共类型写单元测试（序列化往返、enum 变体构造）。

验收：

```bash
cargo test -p jessica-core
cargo clippy -p jessica-core -- -D warnings
```

禁止：不要在任何 Trait 中加入 WASM、SQLite、HTTP 相关概念；不要提前定义 Phase 2 的 Guard 实现，只留 Trait。

---

Phase 1：裸循环（4 个 Task）

Task 1.1：OpenAI 兼容 Provider

操作：在 jessica-providers 中实现 OpenAiProvider，支持流式 SSE 解析。请求体构造、SSE 行解析、TextDelta/ToolCallRequested 的映射各拆成独立函数并单测。工具调用的流式增量拼接（多个 delta 拼成一个完整 ToolCall）必须有测试覆盖。

验收：

```bash
cargo test -p jessica-providers
# 提供一个 mock SSE 流的单元测试，不依赖真实网络
```

禁止：不要在 Provider 中实现重试/熔断（Phase 5 的事）；不要硬编码 API key，从环境变量读取。

Task 1.1b：Anthropic 兼容 Provider 【新增】

目标：新增 AnthropicProvider，实现与 OpenAI Provider 等价的 LlmProvider 契约，使运行时能在两家模型供应商之间确定性切换。这是与 Task 1.1 对称的 Task，不改变任何下游契约。

背景：Anthropic Messages API 与 OpenAI Chat Completions API 在三处关键差异，必须在 Provider 内部吸收，不得泄漏到 jessica-core 的 Message/LlmEvent 契约中：

1. 消息结构：Anthropic 用 system 顶层字段（非 messages 内的 system role）；content 是 block 数组（text / tool_use / tool_result block），而非字符串 + 独立 tool_calls 字段。
2. 工具定义：tools[] 的元素是 { name, description, input_schema }，不是 OpenAI 的 { type: "function", function: {...} }。
3. 流式事件：SSE 使用具名事件（message_start / content_block_start / content_block_delta / content_block_stop / message_delta / message_stop），文本增量是 content_block_delta 中的 text_delta，工具参数增量是 input_json_delta 的 partial_json，需要按 content_block 的 index 聚合，而不是按 tool_calls[].index。

操作：

在 jessica-providers 中新增 anthropic 模块，实现 AnthropicProvider，结构与 OpenAiProvider 对称：

- 请求构造（独立函数 + 单测）：build_anthropic_request(messages, tools) -> serde_json::Value。需正确完成：system 消息抽取到顶层 system 字段；ToolResult 映射为 tool_result block 且 tool_use_id 与之前的 tool_use 关联；ToolCall 映射为 tool_use block；工具 schema 从 JSON Schema 直接放入 input_schema。
- SSE 解析（独立函数 + 单测）：parse_anthropic_sse_line，识别 event: 与 data:，把每一行解析为强类型 enum AnthropicSseEvent。
- 流式聚合（独立函数 + 单测）：AnthropicStreamAssembler，按 content_block 的 index 维护状态机——text block 累积文本，tool_use block 累积 partial_json 并在 content_block_stop 时尝试 serde_json::from_str 得到完整 ToolCall。必须测试：单文本 block、单工具调用（参数分 2+ 个 delta）、文本与工具交错、多工具并行、JSON 参数被拆断在任意字节边界（含 Unicode 多字节字符）。
- LlmProvider 实现：stream_chat 装配请求 + HTTP 客户端（用 reqwest，加入 [workspace.dependencies] 和批准列表）+ SSE 聚合，输出标准 LlmEvent 流。
- 配置：从环境变量 ANTHROPIC_API_KEY、ANTHROPIC_BASE_URL（默认 https://api.anthropic.com）、ANTHROPIC_MODEL（默认取一个当前可用模型名，作为常量集中定义便于替换）读取；支持自定义 base_url 以兼容第三方 Anthropic 兼容网关。
- 错误映射：HTTP 非 2xx、event: error、JSON 解析失败，全部映射为 LlmEvent::Error，不得 unwrap。

验收：

```bash
cargo test -p jessica-providers anthropic
# 至少覆盖：
#  1. build_anthropic_request：system 抽取、tool_use/tool_result 往返、schema 映射
#  2. SSE 解析：message_start → text delta → tool_use delta → message_stop 的完整 mock 流
#  3. 聚合器：工具参数被拆断在多字节字符中间仍能正确拼接
#  4. 错误事件：event: error 与 HTTP 500 都产生 LlmEvent::Error
cargo clippy -p jessica-providers -- -D warnings
```

提供一个 tests/fixtures/anthropic_sse_*.txt 的 mock SSE 流样本文件，测试读取本地文件，不依赖真实网络。

禁止：

- 不要在 Provider 内部实现重试/熔断/限流（Phase 5）。
- 不要把 Anthropic 特有的 block 结构泄漏到 jessica-core 类型中；ToolResult/ToolCall 的映射只在 Provider 内完成。
- 不要硬编码 API key。
- 不要在 Task 1.1 未完成时开工（复用 SSE 行读取与 HTTP 骨架，避免重复造轮子）。

Deferred 记录点：Prompt caching（cache_control）、thinking block、多模态 image block 不在本 Task 范围，记入 PLAN.md 的 Deferred 列表，由用户决定是否单开 Task。

Task 1.2：jessica-runtime 的 Agent 循环

操作：在 jessica-runtime 中实现 AgentLoop：接收初始 messages、Arc<dyn LlmProvider>、Arc<ToolRegistry>、Budget、CancellationToken。循环逻辑：调用 provider → 消费流 → 若有工具调用则逐个执行 → 回填 ToolResult → 重复，直到无工具调用或超限。所有状态变更通过 broadcast::Sender<AgentEvent> 发出。

验收：

```bash
cargo test -p jessica-runtime
```

测试用 mock provider（返回预设的 LlmEvent 序列）和 mock tool，覆盖：纯文本终止、单工具调用后终止、多轮工具调用、最大轮次触发、取消信号中断。

禁止：不要在循环中内联治理逻辑（Phase 2 通过中间件加）；不要用 unwrap 处理 provider 错误，错误应转为 AgentEvent::Error 并终止。

Task 1.3：jessica-cli REPL

操作：在 jessica-cli 中实现最小 REPL：读 stdin → 调 AgentLoop → 流式打印文本 → 打印工具调用摘要。装配 3 个原生工具：read_file（限当前目录）、write_file、bash（带超时）。订阅 AgentEvent 打印结构化日志。

验收：

```bash
cargo run -p jessica-cli -- "读取当前目录的 Cargo.toml 并总结"
```

人工确认输出合理、循环正常终止、Ctrl+C 能中断。

禁止：不要在 CLI 中实现 TUI 或复杂参数解析；bash 工具必须有超时和输出大小上限。

---

Phase 2：Harness 治理（4 个 Task）

Task 2.1：中间件框架

操作：在 jessica-harness 中定义 Middleware 的装配机制：Harness 结构持有 Vec<Arc<dyn Hook>> 等，按固定顺序在循环的关键节点调用。先只搭骨架和一个 LoggingHook，验证中间件链能正确包裹 AgentLoop 而不改变其行为。

验收：cargo test -p jessica-harness，测试证明加空中间件前后 AgentEvent 序列一致。

禁止：不要在 Task 2.1 实现具体 Guard/Sensor 逻辑。

Task 2.2：Guards 与 Policy

操作：实现 TokenBudgetGuard、ToolTimeoutGuard、StuckDetectionGuard（连续 N 轮无新工具调用或输出无变化则终止）、ResultSizeGuard。Policy 的 AllowlistPolicy 实现：从配置读取允许的工具名列表，默认 DenyAll。所有超限以 AgentEvent::BudgetExceeded 等结构化事件终止，不 panic。

验收：cargo test -p jessica-harness，覆盖每种 guard 触发一次、policy 拒绝一次。测试断言终止事件类型正确、进程无 panic。

禁止：不要在 guard 中调用 LLM 判断是否超限，必须确定性判断。

Task 2.3：Sensors 与 Acceptance

操作：实现 Sensor Trait 的执行管线：工具执行后运行一组 sensor（先实现一个 CommandSensor，跑指定命令，返回 pass/fail + 输出）。实现 AcceptanceCheck 的执行：模型停止后运行检查，失败则把反馈作为新 message 注入并重入循环（有最大重试次数）。

验收：cargo test -p jessica-harness，用 mock sensor 验证 pass 路径、fail 后重入路径、重试耗尽路径。

禁止：不要让 auto-fix 调用 LLM；机械性修复只允许执行预定义命令。

Task 2.4：接入 CLI 并端到端验证

操作：在 jessica-cli 装配完整 harness（Guards + AllowlistPolicy + 一个 CommandSensor + 一个 AcceptanceCheck），提供一个示例配置文件。

验收：

```bash
cargo run -p jessica-cli -- --config examples/harness.toml "修改 README 中的拼写错误并验证"
```

人工确认：越权工具被拒、验收失败会重试、预算超限会终止。同时 cargo test --workspace 全绿。

禁止：不要在这个 Task 中修改 jessica-runtime 的循环代码。

---

Phase 3：WASM 沙箱（3 个 Task）

Task 3.1：wasmtime 基础加载

操作：在 jessica-sandbox 中引入 wasmtime，实现加载 .wasm 文件、实例化、调用导出函数的完整路径。先不接 Tool Trait，用测试验证“加载一个返回固定值的模块并读到结果”。

验收：cargo test -p jessica-sandbox，测试在 tests/fixtures/ 下放置预编译的 .wasm（在 Task 中提供构建脚本）。

禁止：不要启用 WASI 的文件/网络能力（Task 3.2 再做）。

Task 3.2：WASI 能力控制与资源限制

操作：实现 WasiCtxBuilder 装配：默认无文件预打开、无网络。实现按插件清单授予目录只读访问。为每个 Store 配置 fuel、墙钟截止时间、内存/表/实例上限。

验收：cargo test -p jessica-sandbox，两个关键测试：（1）越权读文件返回错误且宿主存活；（2）死循环插件 fuel 耗尽后返回 Trap，宿主进程不崩溃。

禁止：不要默认授予写权限；不要在宿主侧忽略 fuel 耗尽的错误。

Task 3.3：WASM Tool 适配器

操作：实现 WasmTool 适配 Tool Trait：从 manifest（JSON）读取名称、schema、权限声明，调用时序列化 args 进 WASM、读回结果。在 jessica-tools 中提供注册入口，让 ToolRegistry 同时持有 native 和 wasm 工具。提供示例插件源码和构建脚本。

验收：cargo test --workspace；CLI 端到端跑一次调用 WASM 工具的任务，并验证未授权工具被 policy 拒绝。

禁止：不要在适配器中吞掉 WASM trap，必须转为 ToolResult { is_error: true }。

---

Phase 4：事件溯源与重放（3 个 Task）

Task 4.1：事件存储

操作：在 jessica-memory 中引入 rusqlite（或 sqlx + SQLite），实现 EventStore：append-only 表，字段含 session_id、seq、event_type、payload_json、timestamp。AgentEvent 的每个变体都要能序列化写入。

验收：cargo test -p jessica-memory，测试写入后按 seq 顺序读回、schema 往返一致。

禁止：不要在事件中存可变引用或非 Serialize 类型。

Task 4.2：会话恢复

操作：在 jessica-runtime 中实现从 EventStore 重建会话状态的能力：给定 session_id，重放事件直到某个 seq，构造出对应的 messages 和循环状态，然后继续执行。CLI 增加 --resume <session_id> 参数。

验收：cargo test -p jessica-runtime，测试：执行到中途 → 导出状态 → 从检查点恢复 → 后续执行的事件序列与不中断执行一致。

禁止：不要在恢复时重新调用 LLM 或工具，必须从事件流读取结果。

Task 4.3：离线确定性重放

操作：实现 replay 子命令：读事件流，用记录中的 LLM 响应和工具结果驱动循环，输出新的 JSONL 事件流。测试要求两次重放产生字节级一致的 JSONL。

验收：

```bash
cargo run -p jessica-cli -- replay --session <id> --out a.jsonl
cargo run -p jessica-cli -- replay --session <id> --out b.jsonl
diff a.jsonl b.jsonl
```

禁止：重放路径中不得有任何依赖当前时间、随机数或网络的部分。

---

Phase 5：能力扩展与加固（按需拆 Task）

这一阶段每个子方向独立成 Task，按你的优先级排序：

· Task 5.1 分层记忆：短期（会话近期）/中期（FTS5 关键词）/长期（向量）三层召回，MemoryStore 的完整实现，Guide 注入真实记忆。拆分为 5.1.1–5.1.6，详见本节之后的“Task 5.1 拆分”。
· Task 5.2 技能沉淀：复杂任务完成后异步提炼，写入技能库，后续通过 Guide 注入。触发条件必须是确定性规则（如工具调用次数 ≥ N），不得用 LLM 判断。拆分为 5.2.1–5.2.6，其中 5.2.2 再拆为 5.2.2a / 5.2.2b，详见本节之后的“Task 5.2 拆分”。
· Task 5.3 子 Agent 委派：delegate_task 工具，子 Agent 工具集只能收窄父级权限；子 Agent 间不共享可变状态。拆分为 5.3.1–5.3.5，其中 5.3.3 再拆为 5.3.3a / 5.3.3b，详见本节之后的“Task 5.3 拆分”。
· Task 5.4 生产化加固：provider 层重试/熔断、速率限制、配置热加载、优雅关闭。拆分为 5.4.1–5.4.5，其中 5.4.3 再拆为 5.4.3a / 5.4.3b，详见本节之后的“Task 5.4 拆分”。
· Task 5.5 可观测性：OTLP 导出、会话回放可视化工具。

每个 Task 的验收都要求：cargo test --workspace 全绿 + cargo clippy --workspace -- -D warnings + 一个端到端场景。

Task 5.1 拆分：分层记忆

5.1 全局约束（适用于 5.1.1–5.1.6）：

· 记忆层只处理确定性数据：原始文本、既有事件、结构化 metadata。不得在记忆层实现 LLM 摘要、LLM 相关性判断、LLM 记忆提取或 LLM 改写；这些语义加工属于 Task 5.2 技能沉淀，且 5.2 的触发条件也必须是确定性规则。
· 不修改 jessica-core 已冻结的公共 API。MemoryStore / MemoryEntry / MemoryError 的签名保持不变；需要更多召回参数时，在 jessica-memory 内提供固有方法（如 recall_with(RecallRequest)），Trait 方法只做默认参数委派。
· 不引入未在 workspace Cargo.toml 中批准的新第三方 crate。5.1.3 的 sha256 已获用户批准新增 `sha2`；5.1.3 开工时必须在 workspace Cargo.toml 中声明并在 Task 记录中说明理由。其他新增依赖仍需逐项批准。
· 记忆层不得把 LLM 调用、网络或宿主时钟带入确定性路径。条目时间戳由调用方显式提供（与 EventStore 的 Timestamp 一致）；库本身不读时钟。
· jessica-memory 的 SQLite 操作是同步阻塞的；所有异步入口必须把连接操作放进 tokio::task::spawn_blocking，禁止在 async worker 上直接执行 rusqlite 调用。

基线（5.1.1 开工前已确认）：

· jessica-core 的 MemoryStore 只有 write(session, entry) 与 recall(session, query, limit)，无 scope/权重/字节上限参数。
· jessica-memory 目前只有 EventStore；rusqlite 0.41（bundled）已在 workspace 中，FTS5 已在 libsqlite3-sys 构建中启用。
· SessionId 接受任意字符串，可直接作为 SQLite TEXT。
· CLI 已有 EventRecorder、EventPump、replay 与 --resume 装配，可作为 5.1.5 的接入点。

依赖顺序：5.1.1 → 5.1.2 → 5.1.3 → 5.1.4 → 5.1.5；5.1.6 在 5.1.5 稳定后单开，不阻塞 5.1 验收。

每个子 Task 完成前，除下方自身 crate 的验收命令外，都必须运行 `cargo test --workspace` 与 `cargo clippy --workspace --all-targets -- -D warnings`。5.1 的端到端场景由 5.1.5 提供一个；5.1.1–5.1.4 不单独要求 CLI E2E。

Task 5.1.1：SQLite 记忆基础 + 短期/中期召回（FTS5）

目标：在 jessica-memory 中建立可迁移、只追加的记忆存储与全文检索，提供“会话近期”与“工作区关键词”两种召回。本 Task 不实现 MemoryStore Trait，不接 CLI。

操作：

1. 开工先做 FTS5 tokenizer spike：在内存库创建 `fts5(content, tokenize='trigram')` 并执行一次中文/英文 MATCH。若当前 bundled SQLite 不支持 trigram，退回 unicode61，并把“trigram 不可用/中文子串检索受限”记入 PLAN Deferred；不得因此阻塞 Task。
2. 新建 `memory_entries` 表：`id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, workspace_scope TEXT NOT NULL, content TEXT NOT NULL, metadata_json TEXT NOT NULL, created_at INTEGER NOT NULL, deleted_at INTEGER NULL`；另建部分唯一索引 `CREATE UNIQUE INDEX ... ON memory_entries(session_id, workspace_scope, content) WHERE deleted_at IS NULL`，使软删除后的相同内容可以重新写入。
   - `session_id` 用于短期召回；`workspace_scope` 用于中期/长期召回（5.1.3 由规范化工作区路径的 sha256 前 16 个 hex 字符生成）。
   - `deleted_at` 为预留的可空列；本 Task 只允许“软删除”，物理 UPDATE/DELETE 一律拒绝。
3. 创建外部内容 FTS5 表 `memory_fts USING fts5(content, content='memory_entries', content_rowid='id', tokenize=<spike 结果>)`，并用 INSERT/UPDATE/DELETE 触发器同步；触发器必须允许后续的软删除路径正确移除索引（软删除 = 从 FTS 可见集合排除，同时保留原行）。
4. 追加写约束：BEFORE UPDATE 触发器只允许 `OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL` 且其余列逐列相等的一次性软删除；其他 UPDATE 与所有 DELETE 用 RAISE(ABORT)。已软删除的行再次软删除必须被拒绝。
5. 打开数据库时设置 `PRAGMA busy_timeout = 5000`；用 `PRAGMA user_version` 做 schema 版本：0 → 建表并置为 1；大于当前支持版本 → 结构化错误，不自动降级。
6. 去重：写入走 `INSERT OR IGNORE`，依赖部分唯一索引（session_id, workspace_scope, content）；重复写入返回 Ok 且不新增行。
7. 提供内部同步 API（名称可调整）：`append(NewMemoryEntry)`、`recent_for_session(session, workspace_scope, limit)`、`search_keyword(workspace_scope, query, limit)`、`soft_delete(id, deleted_at)`。短期召回也必须同时过滤 workspace_scope，避免同一 session id 在不同工作区之间泄漏。
8. FTS 查询清洗：不得把用户输入原样拼进 MATCH。按空白/标点切词，逐词加双引号并转义内部双引号，再以 AND 连接；空查询或清洗后无 token 返回空结果。查询有效 token 长度 < 3（trigram 无法命中）时退回 `LIKE '%escaped%' ESCAPE '\'`，转义 `%`、`_`、`\`；LIKE 分支同样按 workspace_scope 过滤并保持确定性排序。
9. 错误用 thiserror 定义结构化 enum（Open/Schema/Migration/UnsupportedVersion/Serialize/Query/Corrupt 等），库代码不得 unwrap/expect。
10. `metadata_json` 用 serde_json 序列化；反序列化失败作为 Corrupt/Decode 错误，不吞掉。

行为测试（至少）：

· schema 创建后 user_version=1；重开数据库数据仍在；未来版本号被拒绝。
· 九类特殊查询（`"`、`*`、`AND`、`NEAR`、`-`、括号、冒号、`%`、`_`）不报错且返回确定结果。
· 中文 2 字/3 字以上查询、英文前缀、子串查询分别走 trigram 或 LIKE 回退，命中符合预期。
· 同一 (session, workspace) 重复写入只保留一行；不同 session 或不同 workspace 的相同内容各自保留。
· 软删除后从 recent/search 中消失、FTS 无残留；再次软删除报错；对 content 的 UPDATE 与 DELETE 均被触发器拒绝且原行不变。
· session A 的近期召回不会返回 session B 的条目；同一 session id 在不同 workspace 下互不可见；workspace 召回不会泄漏到另一个 workspace。
· created_at 相同（同一毫秒）时顺序仍确定（created_at DESC, id DESC）。

验收：

```bash
cargo test -p jessica-memory
cargo clippy -p jessica-memory --all-targets -- -D warnings
```

禁止：不实现向量层、RRF、MemoryStore Trait、Guide 或 CLI 接入；不修改 jessica-core。

Task 5.1.2：Embedder 抽象 + DeterministicEmbedder + 向量层

目标：在不引入任何新的第三方 crate 的前提下，提供可替换的 Embedder 抽象、一个确定性演示 Embedder，以及可暴力扫描的向量索引。

操作：

1. 定义 `Embedder` Trait（jessica-memory）：`async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>`；另提供 `fn model_id(&self) -> &str` 与 `fn dims(&self) -> usize`。Trait 用 async_trait（workspace 已有），为 5.1.6 的 HTTP/本地实现留出异步接口。
2. 实现 `DeterministicEmbedder`：
   - 固定 256 维；对 UTF-8 字节取 n=2..=4 的字节 n-gram。
   - 每个 n-gram 用项目内实现的 FNV-1a 64 位哈希：bucket = hash % 256，符号取 hash 的最高位；vector[bucket] += ±1。
   - 最后做 L2 归一化；空串/纯空白返回全零向量（不 panic、不返回 NaN）。
   - `model_id = "deterministic-v1"`。doc comment 必须明确：仅用于测试与演示，不具备语义理解，碰撞会导致相似度噪声，不得作为生产语义检索质量依据。
3. 新建 `memory_embeddings` 表：`entry_id INTEGER NOT NULL REFERENCES memory_entries(id), model TEXT NOT NULL, dims INTEGER NOT NULL, vector BLOB NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY(entry_id, model)`。向量只随条目追加，条目软删除后查询必须过滤。
4. 手写 f32 小端编解码：`encode_vector` / `decode_vector`；长度不是 4 的倍数、dims 与 BLOB 长度不一致、NaN/Inf 全部返回结构化错误，拒绝写入。
5. 定义 `VectorIndex` Trait 与 `BruteForceIndex` 实现：
   - `add(entry_id, model, vector)` 写入/覆盖该 (entry, model) 的向量；
   - `search(query, model, workspace_scope, limit)` 只扫描同 workspace、同 model、dims == query.len() 且未软删除的行；
   - 余弦相似度 = dot / (||a|| * ||b||)；零范数相似度定义为 0.0；分数降序，分数相同时按 entry_id 升序，保证确定性；
   - 空 query、维度不匹配或 model 无数据返回空结果，不报错。
6. 所有 SQLite 访问仍为同步实现；5.1.3 的 async MemoryStore 包装负责 spawn_blocking。

行为测试（至少）：

· 同一文本多次嵌入逐字节一致；硬编码一条样本向量的字节，锁住算法与字节序（防止无意改动破坏已存数据）。
· 256 维、L2 范数 ≈ 1.0、空串为全零；不同文本产生不同向量。
· 编码→解码逐值相等且为小端；非法长度、dims 不匹配、NaN、Inf 分别报错。
· 余弦排序：完全相同 > 部分重叠 > 无关；同分时 entry_id 升序。
· 查询只返回同 model、同 dims、同 workspace 且未删除的条目；软删除条目不出现。
· `model_id`/`dims` 不匹配时返回空而不是错。

验收：

```bash
cargo test -p jessica-memory
cargo clippy -p jessica-memory --all-targets -- -D warnings
```

禁止：不实现 RRF、不实现 MemoryStore、不接 CLI；不新增第三方 crate；不得把 DeterministicEmbedder 描述为语义模型。

Task 5.1.3：分层召回融合（RRF）+ MemoryStore 完整实现

目标：把短期、中期、长期三路召回融合成稳定的排序，实现冻结的 MemoryStore Trait，并定义确定性的工作区作用域与字节上限。

操作：

1. 定义 `RecallRequest`（jessica-memory 固有类型）：session、query、limit、三路权重（short/keyword/vector）、rrf_k（默认 60）、max_bytes。workspace_scope 由 store 固定，不在请求中覆盖。Trait 的 recall(session, query, limit) 用默认权重、k=60、max_bytes=4096 委派到 `recall_with`。
2. 三路候选：
   - 短期：`recent_for_session(session, workspace_scope, limit)`，按 created_at DESC, id DESC；
   - 中期：`search_keyword(workspace_scope, query, limit)`；
   - 长期：`BruteForceIndex::search(embed(query), "deterministic-v1", workspace_scope, limit)`（embedder 为 none 时跳过）。
3. RRF 融合：对每个候选条目累加 `weight_i / (k + rank_i)`（rank 从 1 开始）；按融合分降序，分数相同按 entry_id 升序；再按 limit 截断。权重可配置但必须为非负有限数；k 必须 > 0。
4. 降级：任一路失败（FTS 语法/不支持、向量表缺失、embedder 错误）只跳过该路并 tracing::warn，其余路继续；只有数据库本身不可用才返回 MemoryError::Backend。降级不得改变 query 或静默伪造结果。
5. 字节上限：融合后逐条累计 content 字节 + `---` 分隔符；总返回内容不得超过 max_bytes。边界处按 UTF-8 char boundary 截断，绝不产生非法 UTF-8；单条即超限时截断该条而不是整体返回空。max_bytes=0 返回空。
6. 实现 `SqliteMemoryStore`（或等价命名）并实现 `MemoryStore`：
   - 构造时接收 canonical workspace path，计算 `workspace_scope = hex(sha256(canonical_path))[..16]` 并固定；构造后 chdir 不影响作用域；
   - `write(session, entry)`：写入 session_id + 固定 workspace_scope；content 为空拒绝，metadata 原样保存；
   - `recall(session, query, limit)`：按 RecallRequest 默认值执行；
   - 每条目写入后同步生成 embedding（embedder 为 none 时跳过），embedding 失败按降级处理并 warn，不回滚条目；
   - 所有 rusqlite 调用在 spawn_blocking 中执行，Connection 用 Arc<Mutex<_>> 串行化；async 方法不得直接做阻塞 IO。
7. workspace scope 使用 `sha2` 计算 sha256（已获用户批准）：开工时在 workspace Cargo.toml 增加 `sha2`，在 Task 记录中说明理由，并固定 `workspace_scope = hex(sha256(canonical_path))[..16]`。

行为测试（至少）：

· 写入后 recall 能取回；不同 session 的短期召回互不干扰；同 workspace 的其他 session 可通过中期/长期召回。
· 不同 workspace、不同 session 的隔离：构造同内容条目，断言不会跨 workspace 泄漏。
· RRF：构造一条同时出现在短期与关键词榜单的条目与只出现在单榜的条目，断言双榜条目排前；调整权重可改变顺序；同分按 entry_id。
· 字节上限：多字节中文内容在 max_bytes 边界被截断为合法 UTF-8 且总字节 <= max_bytes；max_bytes 极小/为 0 的行为确定。
· 降级：无 embedding 的条目仍可被关键词召回；伪造 embedder 错误时 recall 仍返回关键词结果且不 panic。
· 写入去重、软删除后不再召回、重开数据库仍可召回。
· MemoryStore Trait 方法（write/recall）可直接通过 `Arc<dyn MemoryStore>` 使用。

验收：

```bash
cargo test -p jessica-memory
cargo clippy -p jessica-memory --all-targets -- -D warnings
```

禁止：不实现 Guide/CLI；不修改 core Trait；不引入 sqlite-vec/Qdrant；不新增 sha2 之外的依赖。

Task 5.1.4：Guide Trait + MemoryGuide（jessica-harness）

目标：在 jessica-harness 中定义与具体记忆实现解耦的 Guide 抽象，并实现把召回结果渲染为一条 system Message 的 MemoryGuide。

操作：

1. 在 jessica-harness 定义：
   ```rust
   #[async_trait]
   pub trait Guide: Send + Sync {
       async fn render(&self, session: &SessionId, query: &str) -> Result<Option<Message>, GuideError>;
   }
   ```
   GuideError 用 thiserror；Guide 不得依赖 jessica-memory，只依赖 jessica-core。
2. 实现 `MemoryGuide { store: Arc<dyn MemoryStore>, limit: usize, max_inject_bytes: usize }`。冻结的 `MemoryStore::recall` 没有字节上限参数，因此 store 侧先按 5.1.3 的默认上限返回候选，Guide 再对最终完整消息（标题 + 条目 + 分隔符）做一次 `max_inject_bytes` 截断。
3. render 行为：
   - 调用 store.recall(session, query, limit)；
   - 空结果返回 Ok(None)，不产生空 system message；
   - 非空时构造一条 `Message::system`，首行为：`[记忆参考 — 以下内容来自历史会话，仅供参考，不代表当前事实]`；
   - 每条记忆之间用单独一行 `---` 分隔；总字节（含标题与分隔符）不超过 max_inject_bytes，UTF-8 安全截断；
   - 顺序遵循 MemoryStore 的返回顺序，不重排；
   - fail-open：MemoryError 记录 tracing::warn 后返回 Ok(None)，绝不把记忆故障升级为 turn 失败；
   - 不写记忆、不调用 LLM、不读时钟。
4. 测试必须用行为化 mock（可记录调用与返回错误/条目），不得只断言 Ok。

行为测试（至少）：

· 空结果 → None；有结果 → 恰好一条 system message，内容含标题、条目与 `---`。
· 顺序与 recall 返回一致；超过 max_inject_bytes 时截断为合法 UTF-8 且不超限。
· store 返回 MemoryError → Ok(None)，不 panic、不传播错误。
· render 期间 mock 的 write 调用次数为 0。
· 构造含多字节中文的条目，断言截断不破坏字符。

验收：

```bash
cargo test -p jessica-harness
cargo clippy -p jessica-harness --all-targets -- -D warnings
```

禁止：不修改 Harness 的 Guard/Policy/Sensor 语义；不自动把 Guide 接入 AgentLoop；不依赖 jessica-memory；不实现 CLI。

Task 5.1.5：CLI 装配 + 写入策略 + E2E

目标：把记忆接入真实 CLI：配置、召回注入、Completed 后写入、失败降级，并验证 resume/replay 的确定性边界。

操作：

1. `HarnessConfig` 新增 `[memory]`：
   ```toml
   [memory]
   enabled = false
   path = ".jessica/memory.db"
   recall_limit = 5
   max_inject_bytes = 4096
   embedder = "deterministic"   # "deterministic" | "none"
   write_policy = "completed"   # "completed" | "explicit"
   ```
   path 相对 CLI 的 root 解析（与 [events] 一致）；enabled=false 时完全不创建/打开数据库文件；非法枚举值报结构化配置错误。
2. 新增 `memory.rs` 装配模块：enabled 时在 spawn_blocking 中打开 SqliteMemoryStore、按 embedder 选择 DeterministicEmbedder 或 none、构造 MemoryGuide；打开失败按 fail-open 处理并 warn（非法配置值除外，仍应报错）。
3. 注入：fresh turn 的消息顺序固定为 `system_prompt → Guide 返回的 memory system message（若有）→ user`。memory disabled / Guide 返回 None / Guide 失败时退化为原顺序。resume 绝不重新注入：checkpoint 的 messages 已包含首次 turn 注入的 guide 消息，重复注入会破坏恢复一致性。
4. 写入策略：
   - `completed`：仅当本次 turn 最终 Completed（acceptance 重试后的最终结果）后写入一条 `kind=turn` 记忆；
   - 内容 = 用户输入截断 2KB + 最终 assistant 回答截断 4KB；两处均按 UTF-8 char boundary 截断；
   - metadata = `{ "kind": "turn", "truncated": bool, "tool_call_count": N, "turn_index": N }`；不写入任何工具结果/工具输出；
   - 依赖 5.1.1 的唯一约束去重；重复 turn 不新增行；
   - 写入失败只 warn，不改变 SessionEndReason/退出码。
   - `explicit`（已确认）：不自动写入；本 Task 只保留配置解析与“无自动写入”的行为，显式写入入口记入 Deferred，不发明 LLM 意图判断。
5. replay：replay 路径绝不打开、创建或读取记忆数据库；在 replay 分支提前返回，保证无 memory 文件也能重放。
6. E2E（必须新增）：用动态脚本化 SSE server 跑两次真实 CLI（同一临时 workspace，memory enabled）：
   - 第一次：模型回复一个独特事实（如 `the launch code is ORANGE-7`），CLI Completed 后断言 DB 中存在 kind=turn 条目；
   - 第二次：新 session 输入能命中该事实的问题，断言发往 provider 的请求体中出现 `[记忆参考` 和 ORANGE-7，且该消息位于 system prompt 之后、user 之前；
   - 断言第二次的最终回答来自脚本化模型，退出码 0；
   - 追加断言：`--resume` 恢复不会产生第二次 recall/注入；`replay` 在无 memory 路径时仍可运行且不创建 DB。
7. 测试：配置解析默认值/相对路径/非法值；注入顺序；disabled 无注入；Completed 写入与截断/metadata/去重；resume 不注入；replay 不触库；fail-open；E2E 上述场景。

验收：

```bash
cargo test -p jessica-cli
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

并执行上述两次 CLI 的端到端场景（测试内自动执行，不依赖真实 API key）。

禁止：不修改 jessica-core；不让 memory 故障改变退出码；不在 resume/replay 中读记忆；不写工具结果；不实现 5.1.6。

Task 5.1.6：（后续、非阻塞）真实 Embedder

目标：在 5.1.5 稳定后单开，接入真实 HTTP embeddings（OpenAI 兼容）或本地模型，替换/补充 DeterministicEmbedder。

范围（届时细化）：`embedder="http"` 配置、超时/重试/失败降级、模型维度与 model_id 迁移、批量写入、成本与速率限制。不得改变已冻结的 MemoryStore 契约；不得把网络失败升级为 turn 失败。5.1.6 未完成不阻塞 5.1.1–5.1.5 的验收。

Task 5.2 拆分：技能沉淀

5.2 全局约束（适用于 5.2.1–5.2.6）：

· 本阶段所有 5.2 子 Task 都不修改 jessica-core 已冻结的公共 API。需要新 Trait 时放在 jessica-harness。5.2.2b 采用方案 B：不改 core、不新增 AgentEvent；resume 的入队幂等依赖 skills.db 唯一索引，并显式记录双真相源边界与迁移到方案 A 的 TODO(reconciliation)。
· 不新增第三方 crate。复用现有 rusqlite、tokio、serde_json、thiserror、async-trait、tracing、futures、sha2。
· 触发规则只读结构化计数：最终 Completed、tool_call_count、assistant_rounds、acceptance_failures。不得用 LLM 判断是否触发。
· LLM 只用于提炼技能内容，绝不参与触发判断、技能检索、排序或注入决策。
· 新技能默认 draft，人工激活后才注入；注入消息必须标注来源与“仅供参考”。
· 任何“LLM 判断技能是否值得注入”的改动需单开 Task 并经 review。
· skill_jobs 的持久化与幂等以 skills.db 的 (workspace_scope, session_id, turn_index) 唯一索引为准；本阶段不把 job 状态写入事件流。必须在 skill_jobs 模块留下 `TODO(reconciliation)` 与文档段落，说明“双真相源”是已知取舍、replay 不触库、以及未来迁移到方案 A 的路径；不得声称 skill_jobs 已是事件流的物化视图。
· 单 worker 假设必须在文档中明确；claim_next 必须用 BEGIN IMMEDIATE 原子领取；多进程并发不损坏数据但不保证不重复处理。
· 所有技能故障 fail-open：打开、入队、提炼、写入、检索、注入失败只 tracing::warn，不改变 SessionEndReason、退出码，也不向事件流注入 Agent-level Error；job 状态仅记录在 skills.db。
· 提炼输入中的用户可控内容（用户目标、工具参数）必须经过转义再截断，防止注入字符被截断后逃逸。转义在截断之前。
· replay 绝不打开技能库或调用模型；resume 绝不重新注入技能。
· 不得把密钥或完整敏感输出写入技能库。
· 每子 Task 完成前必须运行 cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace；5.2.6 另外运行 CLI E2E。

依赖顺序：5.2.1 → 5.2.2a → 5.2.2b → 5.2.3 → 5.2.4 → 5.2.5 → 5.2.6。5.2.2b 采用方案 B，不修改 core；未来迁移到方案 A 必须单开 Task 并经 core review；5.1.6 不阻塞 5.2。

Task 5.2.1：技能库存储 + FTS（jessica-memory，70–90 分钟）

目标：建立工作区隔离、只追加、可软删除、可人工激活的技能库与全文检索；本 Task 不接 Harness/CLI，不调用 LLM。

操作：

1. 新建 `jessica-memory/src/skill_log.rs`，使用独立 `skills.db`（与 `memory.db` 分离）；打开时设置 `PRAGMA busy_timeout = 5000`，`PRAGMA user_version = 1`，未来版本报 UnsupportedVersion。
2. `skills` 表字段：`id INTEGER PRIMARY KEY AUTOINCREMENT`、`workspace_scope TEXT NOT NULL`、`source_session TEXT NOT NULL`、`source_turn_index INTEGER NOT NULL`、`source_tool_calls INTEGER NOT NULL`、`source_rounds INTEGER NOT NULL`、`source_acceptance_failures INTEGER NOT NULL`、`name TEXT NOT NULL`、`description TEXT NOT NULL`、`instructions TEXT NOT NULL`、`tags_json TEXT NOT NULL`、`content_hash TEXT NOT NULL`、`status TEXT NOT NULL CHECK(status IN ('draft','active'))`、`created_at INTEGER NOT NULL`、`deleted_at INTEGER NULL`、`activated_at INTEGER NULL`。
3. `content_hash = sha256(name || '\0' || description || '\0' || instructions || '\0' || canonical_tags_json)`，排除 source 字段；tags 先排序再序列化，`tags_json` 存储也使用同一 canonical JSON，保证同一技能跨来源稳定去重。
4. 唯一约束：部分唯一索引 `(workspace_scope, source_session, source_turn_index) WHERE deleted_at IS NULL`；部分唯一索引 `(workspace_scope, content_hash) WHERE deleted_at IS NULL`。`INSERT OR IGNORE`；命中时返回 `Duplicate(id)`，保留首条来源与状态，不得因重复插入而自动激活。
5. FTS5 trigram 外部内容表 `skills_fts`（name/description/instructions/tags）与同步触发器；把 5.1.1 的 FTS 查询清洗逻辑抽取到 `jessica-memory/src/fts_query.rs` 并在 memory/skill 两处复用，禁止复制两套实现。短 token 的 LIKE 回退、`%`/`_`/`\` 转义与确定性排序必须保持一致。
6. 追加写约束：BEFORE UPDATE 只允许两类转换：`deleted_at` 由 NULL 单向置值且其余列不变；或 `status` 由 `draft` 变为 `active` 且同时设置 `activated_at`、其余列不变。其他 UPDATE 与所有 DELETE 用 RAISE(ABORT)。`append` 可以按 `auto_activate` 直接插入 active（INSERT 不受 BEFORE UPDATE 限制），但重复插入不得把已有 draft 自动激活。
7. 同步 `SkillLog` + 异步 `SqliteSkillStore`；所有 rusqlite 调用在 `spawn_blocking` 中执行，异步入口不得直接阻塞 runtime。
8. API（名称可微调，语义固定）：`append(NewSkill) -> Result<SkillId, SkillLogError>`、`get(id)`、`list_recent(workspace_scope, limit)`、`list_by_status(workspace_scope, status, limit)`、`search_keyword(workspace_scope, query, limit, min_score) -> Vec<(Skill, f64)>`、`soft_delete(id, deleted_at)`、`activate(id, activated_at)`、`find_by_source(workspace_scope, session, turn_index)`。
9. 字段上限：`name ≤ 128` 字节、`description ≤ 512` 字节、`instructions ≤ 8192` 字节、`tags ≤ 16` 个且每个 `≤ 64` 字节；超限返回结构化校验错误，不截断静默写入。
10. workspace scope 复用 5.1.3 的 `hex(sha256(canonical_workspace))[..16]` 生成逻辑，把私有 helper 提升为共享实现；不得让技能跨 workspace 可见。
11. 错误用 thiserror 定义结构化 enum（Open/Schema/Migration/UnsupportedVersion/Validation/Serialize/Query/Duplicate/NotFound/Corrupt 等），库代码不得 unwrap/expect。

行为测试（至少）：

· user_version=1、重开保留数据、未来版本拒绝。
· 相同 source 重复 append 返回 Duplicate 且只保留首条；软删除后同一 source 可重新写入。
· 相同内容不同 source 去重，`content_hash` 对 tags 顺序不敏感；不同 workspace 相同内容各自保留。
· name/description/instructions/tags 上限边界与超限错误。
· FTS 特殊字符、中文 trigram、2 字 LIKE 回退、英文前缀均不报错且结果确定。
· draft 默认不可见（检索只返回 active）；activate 后可见；重复 activate 的行为必须锁定并测试；软删除后检索/FTS 均不可见。
· 物理 UPDATE/DELETE 及非法状态转换被触发器拒绝且原行不变。
· `find_by_source` 命中/未命中、跨 workspace 隔离、limit/排序、损坏 tags_json/未知 status 报错。
· 所有异步 API 的 SQLite 操作确实走 spawn_blocking（至少一个并发/阻塞行为测试）。

验收：

```bash
cargo test -p jessica-memory
cargo clippy -p jessica-memory --all-targets -- -D warnings
```

禁止：不实现 Harness/Guide/Worker/CLI；不调用 LLM；不修改 jessica-core；不新增 crate；不把技能写入 memory.db。

Task 5.2.2a：确定性触发 + 提炼源（jessica-harness，30–45 分钟）

目标：只根据结构化计数判断是否触发，并把可用于提炼的最小上下文整理为有界、已转义、不可信内容。

操作：

1. 定义 `SkillTrigger { min_tool_calls: usize, min_rounds: usize }`，默认 5/2（可在 5.2.6 配置覆盖）。
2. 触发条件必须是：最终 `Completed` 且 `acceptance_failures == 0` 且 `tool_call_count >= min_tool_calls` 且 `assistant_rounds >= min_rounds`。不得读取模型文本、不得调用 LLM、不得使用时间/随机数。
3. `acceptance_failures` 只能来自结构化结果：优先使用最终 `AcceptanceOutcome.attempts.saturating_sub(1)`；若改用事件计数，必须在本 Task 锁定唯一来源并测试。不得把 `attempts` 直接当失败数。
4. 定义 `DistillSource`：`user_goal`、`final_answer`、`tool_summaries`、`tool_call_count`、`assistant_rounds`、`acceptance_failures`、`session_id`、`turn_index`。默认不包含工具输出；可选包含每个工具摘要，单个工具输出/摘要上限 512 字节。
5. `escape_untrusted()`：在截断之前转义用户可控内容中的 `</source>`、`<|im_end|>`、`\n\nSystem:`、`\n\nHuman:`、`\n\nAssistant:` 等边界标记；采用一套锁定的方法并文档化，测试必须锁定逐字节输出，防止转义字符被截断后逃逸。
6. 预算不足时的截断优先级：先丢弃 `tool_summaries` 尾部，再截断 `final_answer`，最后才截断 `user_goal`；所有截断在转义之后执行，且按 UTF-8 字符边界。`max_source_bytes` 的默认值由 5.2.6 配置（16384），本 Task 接收显式预算。
7. `to_untrusted_block()` 把来源包装为明确的不可信块（如 `<source>...</source>`），供 5.2.3 作为独立 user message 使用；块内不得出现可跳出边界的未转义标记。

行为测试（至少）：

· 阈值恰好满足/差 1 不触发；非 Completed、acceptance_failures>0 不触发。
· 只读计数：提供一个会 panic/计数的 LLM mock，断言触发判断期间零调用。
· 五种以上注入标记在转义后不可逃逸；跨截断边界的半个标记不产生有效边界。
· 预算不足时按“tool_summaries 尾部 → final_answer → user_goal”顺序削减，且不破坏 UTF-8。
· 默认不含工具输出；开启后单个工具摘要不超过 512 字节。

验收：

```bash
cargo test -p jessica-harness
cargo clippy -p jessica-harness --all-targets -- -D warnings
```

禁止：不调用 LLM；不写技能库；不接 CLI；不修改 core；不新增 crate。

Task 5.2.2b：技能任务队列（方案 B，jessica-memory，60–90 分钟）

目标：建立可持久化、可跨进程恢复的技能任务队列。采用方案 B：job 状态只存 skills.db，入队幂等依赖 `(workspace_scope, session_id, turn_index)` 唯一索引；不修改 jessica-core、不新增 AgentEvent。模块必须显式记录双真相源边界与迁移到方案 A 的路径。

操作：

1. `skill_jobs` 表：`id`、`workspace_scope`、`session_id`、`turn_index`、`source_json`、`status`、`attempts`、`last_error`、`created_at`、`updated_at`、`claimed_at`；唯一约束 `(workspace_scope, session_id, turn_index)`；`status` 用 CHECK 限制为 `pending|processing|done|failed`。`source_json` 必须是 5.2.2a 产出的有界 `DistillSource`，不得直接存原始会话或完整工具输出。
2. 状态机：`pending → processing → done|failed`。入队幂等：同一 `(workspace_scope, session_id, turn_index)` 重复入队返回已有 job，不新增行；done/failed 的 job 不得被重新入队覆盖。实现必须并发安全（`INSERT OR IGNORE` + 回读，或等价事务）。
3. `claim_next` 必须用 `BEGIN IMMEDIATE` + `UPDATE ... RETURNING` 原子领取，设置 `claimed_at` 与 `attempts`；只领取 `pending` 或超过 stale 阈值的 `processing`；stale claim 可恢复。
4. SQLite 启用 WAL 与 `busy_timeout=5000`；文档明确单 worker 假设。多进程并发不得损坏数据，但不保证同一 job 不被重复处理；重复处理必须由 5.2.1 的幂等技能写入兜底。
5. `last_error` 必须脱敏、截断（建议 ≤512 字节）后再写入；不得把 provider 原始响应、密钥、完整工具输出写入 skills.db。
6. 所有 SQLite 操作同步 API，异步调用方使用 `spawn_blocking`；错误用 thiserror 结构化 enum（Open/Schema/Serialize/Query/Claim/Corrupt/Event 等）。
7. 在 `skill_log.rs` / skill_jobs 模块头加入文档段落与显式 `TODO(reconciliation)` 注释，至少写明：
   - 本阶段采用方案 B：job 状态不在 AgentEvent 中；resume 的入队幂等读 skills.db 唯一索引；事件流与 skills.db 是双真相源，不假装单一真相。
   - replay 不打开 skills.db，因此 replay 的确定性不受 job 表状态影响；但 resume 的行为依赖 skills.db 的外部可变状态。
   - 迁移触发条件：如果 resume 在不同 skills.db 初始状态下需要产生完全一致的事件/决策，或需要从事件流重建 job 状态，则迁到方案 A。
   - 迁移路径：单开 Task → 在 jessica-core 新增 `AgentEvent::SkillJobEnqueued` / `SkillJobCompleted` 及 payload → enqueue/complete 发事件 → 实现 `rebuild_from_events` 物化 skill_jobs → resume 改为读事件流/物化视图 → replay 跳过物化 → 补双真相源一致性测试。该 Task 必须单独 review core 变更。
8. 不实现 `rebuild_from_events`；不得在文档或注释中声称 skill_jobs 可随时从事件流重建。`source_json` 是 skills.db 的持久化数据，不是事件的派生物。

方案 B 的已知限制（必须写入模块文档）：

· resume 的入队幂等依赖 skills.db，而不是事件流；如果 skills.db 被删除/替换，resume 可能重复入队，最终由唯一索引与幂等写入兜底。
· 事件流重放不能恢复 job 状态；这是本阶段接受的取舍，不是“已解决”。
· 迁移到方案 A 前，不得把 skill_jobs 用于任何“必须从事件流确定性重建”的场景。

行为测试（至少）：

· 重复 enqueue 幂等；不同 turn 各自入队；done/failed 后重复 enqueue 不覆盖状态。
· claim 顺序确定（created_at/id），BEGIN IMMEDIATE 下并发 claim 不重复分配；stale claim 可恢复。
· 成功/失败状态转换只写 skills.db，断言 EventStore 中没有新增 skill job 事件。
· 损坏 `source_json` 返回 Corrupt，不 panic。
· WAL 与 busy_timeout 生效；错误脱敏截断。
· 关闭重开后 job 状态与唯一索引仍生效；删除 skills.db 后 resume 式幂等依赖失效（以文档/测试锁定该限制）。

验收：

```bash
cargo test -p jessica-memory
cargo clippy -p jessica-memory --all-targets -- -D warnings
```

禁止：不修改 jessica-core；不新增 AgentEvent；不实现事件流物化；不声称 skill_jobs 是事件流物化视图；不长期保存敏感输出；不在本 Task 接 CLI。

Task 5.2.3：LLM 技能提炼器（jessica-harness，60–80 分钟）

目标：只用一个受控 LLM 调用把 `DistillSource` 提炼成结构化技能草稿；LLM 不参与任何触发/检索/注入决策。

操作：

1. 定义 `SkillDistiller` Trait：`async fn distill(&self, source: &DistillSource) -> Result<DistilledSkill, DistillError>`；`DistilledSkill { name, description, instructions, tags }`。
2. `DistillError` 区分 `Retryable { message }`（超时、流错误、临时 provider 错误）与 `Permanent { message }`（非法 JSON、schema 不符、内容超限、工具调用）。
3. `LlmSkillDistiller` 接收 `Arc<dyn LlmProvider>`、超时、最大输出字节；固定 system prompt；把 `source.to_untrusted_block()` 作为独立 user message；`tools` 始终为空。
4. 必须拒绝 `LlmEvent::ToolCallRequested`；必须收到 `Done` 才算成功；流中 `Error` 映射为 Retryable；超时用 `tokio::time::timeout`；缺少 `Done` 视为不完整流，按 Retryable 处理。
5. 严格解析模型输出：只接受纯 JSON 对象，拒绝 Markdown fence、额外字段、空字段、未知字段、超长内容；字段上限与 5.2.1 一致；tags 去重并排序。
6. 不得写技能库、不得写事件流、不得发通知；只返回 `DistilledSkill` 或结构化错误。
7. 技能内容不得包含密钥/完整敏感输出；提示词明确要求只提炼可复用步骤，不复制工具原始输出。

行为测试（至少）：

· 合法 JSON → 正确字段与 canonical tags；空/超长/未知字段/fence → Permanent。
· provider 返回 `ToolCallRequested` → Permanent；流 `Error`/超时 → Retryable；缺少 `Done` → Retryable。
· 注入内容作为不可信块传递，模型响应不能改变 system prompt 边界；provider 收到的 tools 为空、消息顺序固定。
· 失败路径零存储写入、零事件写入。

验收：

```bash
cargo test -p jessica-harness
cargo clippy -p jessica-harness --all-targets -- -D warnings
```

禁止：不把 LLM 用于触发/检索/排序/注入；不接 CLI；不新增 crate。

Task 5.2.4：单 worker 提炼队列（jessica-harness，60–90 分钟）

目标：用单 worker 异步消费技能任务，失败重试与幂等写入，不阻塞 turn 结束。

操作：

1. 定义 `SkillJobQueue`（claim/complete/fail）、`SkillSink`（接收 job 来源与 `DistilledSkill`，幂等写入）、`WorkerClock`（时间戳由调用方注入）。队列/存储的具体 SQLite 实现由 CLI 装配时注入，Harness 不依赖 jessica-memory。
2. `SkillWorker` 循环：claim → distill → sink.write → complete；捕获错误并按 `DistillError` 分类处理。
3. Retryable 错误最多重试 `max_attempts`（默认 2，含首次尝试）；Permanent 错误直接 failed；重试耗尽后 failed，并记录脱敏错误。不得用 panic 作为控制流。
4. 写入必须幂等：同一 `(workspace_scope, source_session, source_turn_index)` 或同一 content_hash 重复写入不得产生重复技能；写入成功后才标记 done。
5. 支持 shutdown/drain：收到取消后停止 claim，等待当前 job 完成或超时；`drain_timeout` 由调用方配置。worker 必须可被测试驱动（不读宿主时钟）。
6. 每次状态转换写回 `SkillJobQueue`（complete/fail）；不得静默改变状态，也不修改事件流。单 worker 假设写入文档。
7. 提供可注入的 `WorkerClock` 与确定性测试时钟；生产环境由 CLI 提供当前时间。

行为测试（至少）：

· 成功路径：claim → 一次 distill → 幂等 write → done 状态。
· Retryable 首次失败、第二次成功；超过 max_attempts → failed；Permanent 不重试。
· 重复 job/重复写入不产生重复技能；sink 失败可重试或 failed（锁定语义）。
· shutdown 后不再 claim；drain 在超时内完成；超时返回结构化错误但不 panic。
· queue 状态转换顺序与次数正确；不写 EventStore；不读宿主时钟。

验收：

```bash
cargo test -p jessica-harness
cargo clippy -p jessica-harness --all-targets -- -D warnings
```

禁止：不在 harness 中直接依赖 jessica-memory；不阻塞 turn；不把 worker 变成多线程并发处理；不修改 core；不把 job 状态写入事件流。

Task 5.2.5：技能检索 + SkillGuide（jessica-harness，45–60 分钟）

目标：只检索 active 技能，渲染为带固定标题、来源标注与预算截断的 system message；失败 fail-open。

操作：

1. 定义 `SkillSource` Trait：`async fn recall(&self, query: &str, limit: usize, min_score: Option<f64>) -> Result<Vec<SkillContent>, SkillSourceError>`；`SkillContent { name, description, instructions }`。
2. 检索只返回 `status=active` 且未软删除的技能；按 bm25 升序（数值越小越相关）返回；`limit=0` 返回空。
3. `min_score` 语义必须在 5.2.1 锁定：`None` 或 `0.0` 表示禁用；正值使用与 bm25 方向一致的归一化口径；LIKE 回退没有 bm25 时使用固定哨兵分数并测试。不得把 `0.0` 当字面阈值。
4. `SkillGuide { source, limit, min_score, max_inject_bytes }` 实现既有 `Guide` Trait；render 调用 `source.recall(query, limit, min_score)`；本 Trait 的 query 来自用户输入，session 不用于过滤（技能是 workspace 级）。
5. 输出恰好一条 `Message::system`：固定标题 `[技能参考 — 以下内容由历史任务自动提炼，仅供参考，不代表当前事实]`；每个技能 `## <name>`、description、instructions；技能之间 `\n---\n`；完整消息按 UTF-8 字符边界截断到 `max_inject_bytes`（默认 2048）。
6. 空结果返回 `Ok(None)`；source 错误只 `tracing::warn` 并返回 `Ok(None)`；不得写库、不得调用 LLM、不得读时钟。零预算返回 `Ok(None)` 而非空 system message。
7. 注入消息必须包含“自动提炼/仅供参考”的来源标注，避免模型把技能当作用户指令。

行为测试（至少）：

· 空结果 → None；单条/多条顺序与 `## name`/`---` 格式正确。
· 只返回 active，draft/软删除不可见；limit 与 min_score 生效。
· max_inject_bytes 恰好等于完整长度时不截断；少 1–2 字节时按 UTF-8 边界截断；零预算 → None。
· source 返回错误 → Ok(None) 且不 panic；render 期间零写入、零 LLM 调用。
· 标题与“仅供参考”标注逐字节锁定。

验收：

```bash
cargo test -p jessica-harness
cargo clippy -p jessica-harness --all-targets -- -D warnings
```

禁止：不注入 draft；不让检索失败改变 turn；不实现 LLM 相关性判断；不依赖 jessica-memory 具体类型（通过 Trait）。

Task 5.2.6：CLI 装配 + 人工审核命令 + E2E（jessica-cli，90–120 分钟）

目标：把技能触发、异步提炼、人工激活与 Guide 注入接入真实 CLI，并保持 replay/resume 的确定性边界。

操作：

1. `HarnessConfig` 新增 `[skills]`：

```toml
[skills]
enabled = false
path = ".jessica/skills.db"
min_tool_calls = 5
min_rounds = 2
max_source_bytes = 16384
recall_limit = 3
max_inject_bytes = 2048
min_score = 0.0          # 0.0 = 禁用
distill_timeout_ms = 30000
drain_timeout_ms = 30000
max_attempts = 2
include_tool_outputs = false
auto_activate = false
```

相对路径按 CLI root 解析；`enabled=false` 时完全不创建/打开 skills.db；非法值在配置加载期报结构化错误。

2. 新增 `skills.rs` 装配模块，位于 replay 提前返回之后：打开 SqliteSkillStore，构造 `SkillTrigger`、`LlmSkillDistiller`、`SkillWorker`、`SkillGuide`，并用适配器实现 `SkillSource`/`SkillJobQueue`/`SkillSink`。打开失败 fail-open 并 `tracing::warn`，不改变退出码。
3. fresh turn 注入顺序固定为 `system_prompt → memory guide（若有）→ skill guide（若有）→ user`。技能 guide 只注入 active 技能；draft 不注入。resume 绝不重新注入技能（checkpoint 已包含首次注入的消息）。
4. 最终 Completed 后计算结构化计数（tool_call_count、assistant_rounds、acceptance_failures），调用 `SkillTrigger`；命中则按 `(workspace_scope, session_id, turn_index)` 幂等入队并通知 worker，`turn_index` 必须来自稳定推导（不得用进程内计数器）。不写 AgentEvent，job 状态仅由 skills.db 记录。不得 await 提炼完成；turn 结束与 worker 解耦。
5. `auto_activate=false` 时新技能一律 draft；`auto_activate=true` 仅影响新写入技能的初始状态，仍必须遵守 5.2.1 的插入语义。人工激活通过子命令完成。
6. 退出前 drain worker：最多等待 `drain_timeout_ms`；超时只 warn，不改变退出码。replay 分支必须在打开 skills.db 之前返回，且不调用 provider；resume 可以入队但不得注入技能；resume 的入队幂等依赖 skills.db 唯一索引（方案 B），不依赖事件流。
7. 新增 `jessica skills` 子命令：`list`（按状态/最近）、`show <id>`、`activate <id>`、`delete <id>`（软删除，不物理删除）。这些子命令不得初始化 provider、不得加载 wasm 工具、不得触网；输出面向用户可用 stdout；失败返回结构化错误与非零退出码；不得在输出中打印密钥或完整敏感内容。
8. E2E（脚本化 SSE，不依赖真实 API key）：
   - 低于阈值（工具调用 < min_tool_calls 或 rounds < min_rounds）不产生 job；
   - 达到阈值 → skills.db 产生 job、worker 写入 draft，但下一次 fresh turn 不注入；断言事件流中没有 skill job 事件；
   - 人工 activate 后下一次 fresh turn 注入，顺序为 system → memory → skills → user，且包含固定标题与“仅供参考”；
   - 模型返回非法 JSON → job failed、无技能写入、turn 仍 Completed/exit 0；
   - resume 不重新注入技能；replay 不打开 skills.db、不调用 provider；
   - 重复 job 不产生重复技能；resume 再次完成同一 turn 时入队幂等（skills.db 唯一索引），不依赖事件流；连续 replay 两次不受 skills.db 状态影响。
9. 测试：配置默认值/相对路径/非法值；触发边界；draft/active 注入；注入顺序；低阈值；失败 fail-open；resume/replay 边界；worker drain；skills 子命令；方案 B 双真相源边界（resume 幂等读 skills.db、replay 不触库）；E2E 上述场景。

验收：

```bash
cargo test -p jessica-cli
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

并执行上述脚本化 E2E（测试内自动执行）。

禁止：不让技能故障改变 SessionEndReason/退出码；不在 replay 中打开技能库；不在 resume 中注入技能；不自动激活 draft（除非配置显式 auto_activate）；不把 LLM 用于触发/检索/排序/注入判断；不新增 crate；不修改 core；不把 job 状态写入事件流（本阶段方案 B）。

5.2 开工前必须锁定的实现细节（不改变上述拆分范围）：

1. 方案 B 的双真相源边界：5.2.2b 不修改 core、不新增 AgentEvent；resume 的入队幂等读 skills.db 唯一索引，事件流不恢复 job 状态。必须在 skill_jobs 模块写入 `TODO(reconciliation)` 与迁移到方案 A 的文档；不得声称 skill_jobs 是事件流物化视图。迁移触发条件与路径见 5.2.2b。
2. 触发计数口径：`tool_call_count` 与 `assistant_rounds` 必须从最终 attempt 的结构化 messages/事件流计算，跨 acceptance 重试的统计口径必须锁定并测试，不得从文本猜测。`AcceptanceOutcome` 暴露的是 `attempts`，不是失败计数；`acceptance_failures` 必须锁定 `attempts.saturating_sub(1)` 或事件计数中的唯一来源，禁止混用。
3. bm25 与 LIKE 回退：bm25 数值越小越相关；LIKE 回退没有 bm25，必须在 5.2.1 定义固定哨兵分数及 `min_score` 的应用方式，并用测试锁定。
4. min_score=0.0：配置默认 0.0 表示禁用，不得当作字面阈值；`None` 与 `0.0` 必须等价处理。
5. 错误脱敏：`last_error` 以及任何 job 失败输出写入 skills.db 前必须有界、脱敏；不得写入密钥、provider 原始响应或完整工具输出。
6. 重复 activate：5.2.1 必须锁定“幂等成功”或“结构化错误”之一并测试，避免 CLI 子命令行为漂移。
7. turn_index 稳定性：方案 B 的入队幂等键包含 turn_index，必须从会话/checkpoint 稳定推导，不得使用进程内 AtomicU64；resume 必须复用或按确定规则递增，并测试跨进程重启后的重复入队。

Task 5.3 拆分：子 Agent 委派

5.3 全局约束（适用于 5.3.1–5.3.5，违反任一条即验收失败）：

A. 子 Agent 轨迹不持久化。父事件流只保存 `delegate_task` 的 `ToolCallStarted` / `ToolCallFinished`（即调用的 args 与最终 JSON `ToolResult`）；子 Agent 的模型增量、子工具调用、子 `SessionEnded` 一律只进入子 Agent 私有通道并在结束后丢弃。因此子轨迹不可 replay；replay/resume 只读取父事件流中的 `ToolResult`，绝不重新执行子 Agent。v1 明确接受该折中，子会话 EventStore 列为 Deferred（5.3.6 候选）。
B. 权限必须结构性构造。`NarrowedTools` 是唯一允许的子工具集合；`ChildToolFactory` 返回的工具名集合必须与 `NarrowedTools` 完全相等（多一个或少一个都是集成错误）。不得依赖 `Policy`、提示词、模型自述或运行期名称检查来“保证”权限。
C. 终止语义必须区分：子工具超时/失败、子 Agent rounds 耗尽、`max_child_tool_calls` 耗尽 → 子结果是 `ToolResult::error`，父 Agent 继续，父 `SessionEndReason` 保持 `Completed`（或父自身原因）；只有子 Agent 消耗导致父 token 预算耗尽 → 父 `BudgetExceeded`，父 Ctrl-C → `Cancelled`。子 Agent 的失败不得改变父退出码。
D. 超时嵌套：子内部单工具超时 5000ms < `delegation.timeout_ms`（默认 8000ms）< 父 `budget.tool_timeout_ms`（默认 10000ms）。5.3.4 必须在配置加载期验证；运行时不得依赖父 `GovernedTool` 兜底子 Agent 超时。
E. WASM 默认排除：v1 子工具集只允许 native 工具；`narrow_tools` 无条件过滤 WASM（即使父 allowlist 已授权），`ChildToolFactory` / CLI 也不得构造 WASM 子实例。原因：`WasmTool` 有每工具 Mutex 与共享 guest 状态，子 Agent 间不共享可变状态无法保证。
F. 子 Agent 不继承父历史：子初始 messages 必须恰好是 `[system_prompt, user(task)]`，`system_prompt` 是固定常量并声明“独立执行单元、没有外部上下文、只能依据 task 与提供的工具”。不得注入父 transcript、memory guide、skill guide、父任务或父工具结果。
G. 结果必须是合法 JSON：成功/错误/截断三种结构见 5.3.2；成功 answer 超限时不得截断已序列化 JSON，必须重新构造固定结构的 `status=truncated` 对象；错误结果在序列化前先把已脱敏 error 文本缩短到预算内，因此不得出现超限的错误 JSON，也不得把错误伪装成 truncated；`max_result_bytes` 按最终序列化字节计算。错误信息必须有界脱敏，不得包含密钥、provider 原始响应或完整子 transcript。
H. v1 只允许单层委派：`narrow_tools` 无条件排除 `delegate_task`，即使父 allowlist 包含它且模型显式申请；子 Agent 的 registry 永远没有 `delegate_task`。不得实现嵌套、深度参数或子 Agent 再委派。

补充全局约束：

· 不修改 jessica-core。不新增 crate。5.3.2 将 workspace 已批准的 serde_json 从 jessica-runtime 的 dev-dependencies 提升为 dependencies（无新第三方 crate）；不新增其他依赖。
· 子 Agent 使用裸 `AgentLoop`，不接入 Harness、memory、skills、sensors、acceptance、CLI EventPump；子 Agent 的 provider 仍使用父级 `TokenCountingProvider`（共享 token 预算），但子事件写入独立有界 broadcast 通道且无人持久化。
· “不共享可变状态”指：每次委派新建子 transcript、子 registry、子工具实例、子 CancellationToken、子事件通道；父 provider 的 token 计数与取消是唯一有意共享的状态。不得复用子 transcript、子工具实例或子事件通道。
· 子 Agent 与父 Agent 工作目录/文件系统相同；v1 不提供子级 sandbox 或额外隔离，文件工具仍受父级 root 边界约束（子工具由 CLI 以同一 root 构造）。该限制必须写入 5.3.4 的模块文档。
· 所有权限/触发/计数/排序均为确定性集合运算或整数计数，不得调用 LLM 判断。
· 每个子 Task 必须运行自身验收命令；5.3 整体完成后再运行 workspace 全量验收。测试必须是行为测试：用脚本化 provider/tool 构造真实边界，禁止“调用返回 Ok”式自证。
· 若实现发现 5.3.1–5.3.5 之外的问题，记录到 Deferred，不在本 Task 顺手重构。

依赖顺序：5.3.1 → 5.3.2 → 5.3.3a → 5.3.3b → 5.3.4 → 5.3.5。

Task 5.3.1：委派权限收窄与参数边界（jessica-runtime，45–60 分钟）

目标：只实现委派权限模型、限制类型与输入边界，不执行子 Agent、不接 CLI。

操作：

1. 在 jessica-runtime 新增 delegation 模块，导出 `DELEGATE_TASK_TOOL_NAME`、`ParentCapabilities`、`NarrowedTools`、`narrow_tools`、`DelegationLimits` 与结构化 `DelegationError`。
2. `ParentCapabilities` 至少包含两个 `BTreeSet<String>`：父级 allowlist，以及父级可用的 native 工具名集合；构造函数接收并去重。WASM 工具名不在 native 集合中。`DELEGATE_TASK_TOOL_NAME` 即使出现在两个集合中也必须被排除。
3. `NarrowedTools` 内部只保存排序去重后的工具名；提供只读迭代/`names()`，不得暴露可变集合。
4. `narrow_tools(parent, requested) -> Result<NarrowedTools, DelegationError>` 固定规则：
   - `requested` 中有重复名称 → `DuplicateTool`；
   - 任一名称不在父 allowlist → `ToolNotPermitted`（申请父级未授权工具必须是本地验证错误）；
   - 结果 = `requested ∩ parent.native`，再无条件删除 `delegate_task`；
   - WASM 或其他非 native 名称即使父级已授权也静默排除；
   - 空请求合法，返回空集合；空 narrowed 集合也合法（子 Agent 无工具）。
5. `DelegationLimits` 含 `max_rounds: u32`、`timeout: Duration`、`max_task_bytes: usize`、`max_result_bytes: usize`、`max_child_tool_calls: u32`；默认值固定为 4、8000ms、8192、32768、20。定义固定常量 `CHILD_TOOL_TIMEOUT = 5000ms` 与 `MIN_RESULT_BYTES = 128`，不得把子工具超时做成配置项。`max_result_bytes >= 128` 是在“字节上限为正”之上新增的必要下界：空 answer 的 truncated 骨架与空 error 的 error 骨架都必须能放下，否则无法同时满足固定 JSON 结构与字节上限；`max_task_bytes` 只要求 >=1。
6. `DelegationLimits::validate` 拒绝：`max_rounds == 0`、`max_child_tool_calls == 0`、`max_task_bytes == 0`、`max_result_bytes < MIN_RESULT_BYTES`、`timeout <= CHILD_TOOL_TIMEOUT`。父 `budget.tool_timeout` 的嵌套校验放在 5.3.4（runtime 不知道 CLI budget）。
7. 所有公共 API 有 doc comment；错误用 thiserror；不得 unwrap/expect。

行为测试至少覆盖：请求是父 allowlist 子集时原样保留；申请未授权工具返回 `ToolNotPermitted`；重复名称返回 `DuplicateTool`；父 allowlist 同时含 `delegate_task` 与 WASM 且模型显式申请时两者都被过滤，仅 native 保留；仅申请 WASM / 仅申请 `delegate_task` 得到空集合；空请求得到空集合；输出排序确定；`DelegationLimits` 默认值与每个非法边界。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-runtime --all-targets -- -D warnings
cargo test -p jessica-runtime
```

禁止：在本 Task 执行子 Agent、注册工具、接 CLI、修改 jessica-core、新增依赖；不得把 WASM 名称当作 native 放行；不得用提示词或 Policy 代替 `narrow_tools`。

Task 5.3.2：delegate_task 参数、JSON 结果与截断契约（jessica-runtime，60–75 分钟）

目标：锁定 `delegate_task` 的 schema、参数校验、结果 JSON、错误脱敏和超限截断；仍不执行子 Agent。

操作：

1. 将 workspace 已批准的 `serde_json` 加入 jessica-runtime 的 `[dependencies]`（原来只在 dev-dependencies；无新第三方 crate），用于构造和校验 JSON。
2. 定义 `delegate_task` 的固定 `ToolSchema`：`type=function`、`name=delegate_task`、`additionalProperties=false`、`required=["task","tools"]`、`task` 为 string、`tools` 为 string 数组且 `uniqueItems=true`。description 必须写明：子 Agent 是独立执行单元、无父历史、只能使用请求的 native 工具子集、不能委派。
3. 定义 `DelegateTaskArgs` 的纯解析/校验函数：只接受 JSON object；未知字段、缺失字段、类型错误、空 task（trim 后）→ 结构化错误；task 按 UTF-8 字节计不得超过 `max_task_bytes`；tools 中的重复由 `narrow_tools` 拒绝；解析不调用 LLM、不读时钟。
4. 定义成功/错误 outcome 的内部结构和渲染函数，输出严格为：
   - 成功：`{"status":"ok","rounds":<u32>,"tool_calls":<u32>,"answer":"...","child_session_hash":"<8 hex>"}`；
   - 错误：字段同上，`"status":"error"`，`"answer":""`，额外 `"error":"<有界脱敏文本>"`；
   - 截断：固定且仅含 `{"status":"truncated","answer":"<answer 的 UTF-8 前缀>","original_bytes":<原完整 JSON 字节数>}`。
   三种结构不得增加可选字段；`error` 只在 `status=error` 时存在；成功/截断不得有 `error`。
5. `child_session_hash` 为 FNV-1a 32 位（8 位小写十六进制），输入为 trim 后 task + `\0` + 排序后的 narrowed 工具名。参数校验或收窄失败时，使用可解析的 trim task 与排序去重后的 requested 名称做 best-effort hash；只保证格式与确定性，不保证身份含义。它只是 trace 标识：不持久化、不作幂等键、不作安全凭证，且不包含父 session id。原因必须写入文档：fresh turn 的 `build_turn()` 早于 `next_session_id()`，且同一工具跨 acceptance attempts 复用，无法绑定唯一父 session；父/子关联留给子会话 EventStore Deferred。
6. 结果渲染必须按最终序列化字节执行 `max_result_bytes`：
   - 先序列化成功或错误结果；
   - 成功结果超限时，在 answer 的 UTF-8 char boundary 上取最长前缀，重新序列化为固定 truncated 结构，保证 `<= max_result_bytes`；不得对已序列化 JSON 做字节截断；
   - `original_bytes` 是截断前完整成功 JSON 的字节数；truncated 的 answer 前缀必须仍是原 answer 的前缀；
   - 错误结果不转成 truncated：先对 error 文本脱敏（至少覆盖 `sk-`、`Bearer`、`api_key=`/`token=` 等键值形式），再按 UTF-8 边界缩短 error，使完整 error JSON 仍 `<= max_result_bytes` 且保持合法；若连空 error 的骨架都放不下，构造期已由 `MIN_RESULT_BYTES=128` 拒绝；
   - `max_result_bytes` 恰好等于完整 JSON 长度时不截断。
7. `ToolResult.is_error` 固定为：`ok` 和 `truncated` 为 false，`error` 为 true；`truncated` 的 JSON 仍可被父模型直接解析。成功 answer 不做密钥扫描/改写（由子 system prompt 禁止输出凭据），但 error 必须脱敏。
8. 行为测试至少覆盖：三种 JSON 的精确 key 集合与字段类型；8 位 hex 且工具顺序无关、task 不同则 hash 不同；成功 JSON 的 `max_result_bytes` 恰好够时保持 ok、少 1 字节时变 truncated 且 JSON 可解析、长度不超限、answer 是 UTF-8 前缀（含中文/emoji 边界）、`original_bytes` 精确；truncated 恰为 3 个 key；error 的 `sk-`/Bearer/键值凭据被替换且长度有界，且极小的 `max_result_bytes` 下仍是合法 error 而非 truncated；`is_error` 映射；schema 的 required/additionalProperties；task 超长、未知字段、空 task、错误类型全部拒绝。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-runtime --all-targets -- -D warnings
cargo test -p jessica-runtime
```

禁止：在本 Task 执行子 Agent、注册 `delegate_task`、接 CLI；不得用字符串拼接生成 JSON；不得截断已序列化 JSON；不得让 truncated 夹带 error/rounds/hash 等额外字段；不得把完整 provider 原始响应写入 error。

Task 5.3.3a：子 Agent 执行器与 run_with_transcript（jessica-runtime，90–120 分钟）

目标：实现真正的单层子 Agent 执行、工具工厂、精确结果计数与 `delegate_task` Tool。

操作：

1. 给 `AgentLoop` 增加 `run_with_transcript(session_id, messages, max_tool_calls: Option<u32>, cancel) -> TranscriptRun`。`TranscriptRun` 至少含 `reason: SessionEndReason`、最终 messages、`rounds: u32`、`tool_calls: u32`、`tool_errors: u32`、`first_tool_error: Option<String>`、`error: Option<String>`（provider/stream/loop 的终止错误，来自循环结构化返回而非事件字符串）、`tool_call_limit_hit: bool`。`run`/`resume` 的签名和行为保持不变，内部复用同一循环并丢弃 transcript。错误/取消/tool-call-limit 路径的 messages 允许停在未完成的工具批次；文档必须声明 executor 只在 `Completed` 且 `tool_errors==0` 时读取 answer，其他路径只使用计数与错误摘要。
2. 子 Agent 的 rounds 精确计为已完成的模型回合数；`tool_calls` 计为模型请求的工具调用总数（含未知工具）；`tool_errors` 计为 `ToolResult.is_error=true` 的调用数（含子工具超时与未知工具），`first_tool_error` 记录第一个失败工具名与最多 512 字节的非空错误摘要。`max_tool_calls` 在执行每个请求前递增并检查：第 N+1 个请求不得执行，返回 `tool_call_limit_hit=true`，reason 为 `Error`，并只在子私有通道发 Error/SessionEnded；父事件流不可见。没有该限制时 `tool_call_limit_hit=false`。
3. 定义 `ChildToolFactory: Send + Sync`，同步方法 `build(names: &[String]) -> Result<Vec<Arc<dyn Tool>>, DelegationError>`。每次委派都必须重新调用（不得缓存子工具实例）。构造子 `ToolRegistry` 后校验 `registry.names()` 与 `NarrowedTools` 完全相等；额外或缺失工具都返回 error JSON，且不得开始子模型调用。
4. 定义 `AgentChildExecutor`：持有父级 `Arc<dyn LlmProvider>`（即 CLI 的 TokenCountingProvider）、`Arc<dyn ChildToolFactory>`、`ParentCapabilities`、`DelegationLimits`。构造时验证 limits 与 `max_result_bytes >= MIN_RESULT_BYTES`。
5. `AgentChildExecutor::execute(args) -> ToolResult` 固定流程：解析参数 → `narrow_tools` → factory.build → 子 registry 精确校验 → 用固定 `CHILD_TOOL_TIMEOUT=5000ms` 包装每个子工具（超时返回子模型可见的 `ToolResult::error`，不取消父；native 三件套必须可安全 drop）→ 构造 `AgentLoop`（`Budget.max_rounds=delegation.max_rounds`，其余 budget 字段不被 loop 使用）→ 初始 messages 恰为固定 `CHILD_AGENT_SYSTEM_PROMPT` + `Message::user(task)` → 子 session id 使用 `child_session_hash` → 在 `delegation.timeout` 内调用 `run_with_transcript(..., Some(max_child_tool_calls), 子 CancellationToken)` → 从最终 transcript 取最后一个 assistant 文本作为候选 answer → 按 5.3.2 渲染。
6. 固定 `CHILD_AGENT_SYSTEM_PROMPT` 必须声明：你是独立执行单元；没有父对话/记忆/历史；只依据 current task 与提供的工具；不要请求更多工具；直接给出最终答案；不得再委派。不得将父任务、父消息或任何动态内容插入该常量。
7. 结果映射（与全局约束 C 一致）：
   - `Completed`、未触发 tool call limit、且 `tool_errors == 0` → `status=ok`；
   - 任意子工具超时/失败（`tool_errors > 0`，含未知工具）→ `status=error`，即使子 Agent 后续仍 `Completed`；父继续；
   - rounds 耗尽、`max_tool_calls` 触发、provider/stream 错误、子 Agent 非 Completed → `status=error`；
   - delegation 超时 → `status=error`，丢弃子 future 并返回，不得等父 GovernedTool 的 10s 超时；
   - 所有子失败都包装为 `ToolResult`，不得 panic、不得向父传播 Err；错误结果不得使用 truncated 结构。
8. 定义 `DelegateTaskTool` 实现 core `Tool`：`name=DELEGATE_TASK_TOOL_NAME`，schema 来自 5.3.2，`execute` 调用 executor，并按 `status` 设置 `ToolResult.is_error`（ok/truncated=false，error=true）。call id 仍由 AgentLoop 回填。
9. 子事件通道使用独立有界 broadcast（建议容量 256），不订阅、不转发、不落库；子 Agent 的 SessionStarted/SessionEnded 只存在于该通道。
10. 行为测试至少覆盖：单工具成功（rounds/tool_calls/answer/hash 精确）；子工具报错但模型随后回答 → 仍为 error（tool_errors 计数）；子工具超时 → tool_errors 计数且最终 error；rounds 耗尽 → error 且父不 panic；`max_child_tool_calls` 恰好边界与第 N+1 个不执行；child factory 多/少一个工具 → error 且子 provider 零调用；参数非法 → error 且子 provider 零调用；provider/stream 错误 → error 且错误摘要非空；delegation 超时用 `tokio::test(start_paused = true)` + 挂起 mock provider（默认 8000ms 由虚拟时钟推进，不真实等待）→ error；子工具超时同样用 paused 时钟；子初始 messages 恰为 2 条；子工具名/schema 恰为 narrowed；`run_with_transcript` 不改变既有 run/resume 事件序列。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-runtime --all-targets -- -D warnings
cargo test -p jessica-runtime
```

禁止：接 CLI、注册到父 registry、接 WASM、子 Agent 嵌套、持久化子事件、修改 `run/resume` 签名或行为；不得用事件字符串猜测 rounds/tool_calls（必须从循环结构化返回）；不得让子 Agent 失败变成父 `Err`/panic；不得让第 N+1 个工具实际执行。

Task 5.3.3b：子 Agent 隔离与对抗性行为（jessica-runtime，45–60 分钟）

目标：用行为测试锁定“无父历史、工具集收窄、单层、子间不共享可变状态”；如发现缺口可在本 Task 内修复 5.3.3a 实现，但不得扩大功能。

操作/测试至少覆盖：

1. 父 transcript 中存在独特标记，子 provider 收到的 messages 严格只有 2 条，且 system 等于固定常量、user 等于 task；任何父标记都不出现。task 本身包含伪造的 “system:”/“ignore previous instructions” 文本时仍只是 user 内容，不改变 role 顺序。
2. 子 provider 收到的 tool schemas 名称集合与 `NarrowedTools` 完全一致；未请求的父工具、WASM 工具、`delegate_task` 均不出现；空 narrowed 时 tools 为空。
3. 子模型伪造调用 `delegate_task` → 只得到 unknown tool error，factory 不再被调用，无嵌套 LLM 请求；最终 delegate 结果因 `tool_errors > 0` 为 error。
4. 一次父回合内连续两次委派：factory 每次都被调用且返回全新工具实例；用测试专用有状态 mock 工具写入不同值，证明第二个子 Agent 看不到第一个的工具状态/transcript/结果。
5. 两个子 Agent 的 task 不同、答案不同：第二个子 provider 请求只含自己的 task，不含第一个 task/answer/tool result。
6. 子 Agent 的私有事件通道无人可见：executor 不持有父事件 sender，不产生父事件；父事件流“只有 delegate Started/Finished、没有子事件”的完整断言放到 5.3.5。
7. 子 Agent 的 `SessionId` 来自 child_session_hash，且两次同 task/同工具委派 hash 相同（仅 trace，不用于幂等判断）。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-runtime --all-targets -- -D warnings
cargo test -p jessica-runtime
```

禁止：为了测试在运行时保留子事件、共享子工具实例、缓存子 transcript；不得把 system prompt 改成包含父历史；不得删除或绕过 5.3.1 的过滤规则；不得接 CLI。

Task 5.3.4：CLI delegation 配置与装配（jessica-cli，75–90 分钟）

目标：把 runtime 能力接入 CLI，默认关闭，完成配置校验与父/子工具装配；不写 E2E 大场景（5.3.5）。

操作：

1. 新增 `[delegation]` 配置项，`#[serde(default, deny_unknown_fields)]`：
   ```toml
   [delegation]
   enabled = false
   max_rounds = 4
   timeout_ms = 8000
   max_task_bytes = 8192
   max_result_bytes = 32768
   max_child_tool_calls = 20
   ```
   不提供 `child_tool_timeout_ms`；子工具超时固定 5000ms。`enabled=false` 时完全不注册 `delegate_task`、不构造子 factory/executor，也不要求 allowlist 含 `delegate_task`。默认关闭是刻意的安全选择：避免新增配置破坏既有小 `tool_timeout_ms` 配置，也避免在用户未显式授权 `delegate_task` 时自动扩权。
2. 配置校验：
   - `max_rounds >= 1`、`max_child_tool_calls >= 1`、`max_task_bytes >= 1`、`max_result_bytes >= 128`；
   - 仅当 `enabled=true`：`policy.allowed_tools` 必须包含 `"delegate_task"`，否则配置加载期结构化错误；
   - 仅当 `enabled=true`：`CHILD_TOOL_TIMEOUT(5000ms) < delegation.timeout_ms < budget.tool_timeout_ms`；默认 5000 < 8000 < 10000；
   - 仅当 `enabled=true`：`delegation.max_result_bytes <= budget.max_result_bytes`；
   - 非法值给出字段级错误信息；不得在运行期 panic 或静默截断。
3. 实现 CLI 侧 `NativeChildToolFactory`：按 narrowed 名称每次新建 `ReadFileTool`/`WriteFileTool`/`BashTool`（同一 workspace root）；只支持这三个 native 名称，其他名称返回结构化错误；绝不构造 `WasmTool`，绝不返回 `delegate_task`。
4. 在 `build_turn` 中：provider（TokenCountingProvider）先构建，然后把同一个 `Arc<dyn LlmProvider>` 传给 `AgentChildExecutor`；`ParentCapabilities` 的 allowlist 来自 `config.policy.allowed_tools`，native 集合固定为 `["bash","read_file","write_file"]`。子 Agent 的 token 消耗必须计入父预算并触发父取消。
5. 当 `enabled=true` 时构造 `DelegateTaskTool`，用现有 `GovernedTool` 包装（父 policy、`budget.tool_timeout`、`budget.max_result_bytes`），以固定名 `delegate_task` 注册到父 registry；父 allowlist 必须已包含该名。子工具不走 CLI `GovernedTool`，只走 runtime 的 5000ms 超时包装。
6. 保持装配单向：CLI 依赖 runtime；runtime 不依赖 CLI/harness/memory/sandbox。`delegation.enabled=false` 的既有配置行为、schema 列表与退出码不得变化。
7. 模块文档写明：子 Agent 与父共享文件系统/root 边界；不提供子 sandbox；只有 provider token 预算是有意共享的可变状态；子事件不持久化。
8. 行为测试至少覆盖：配置默认值；每个非法基础边界；enabled 但 allowlist 缺 `delegate_task`；三条超时/结果大小嵌套违规；disabled 时仍校验基础字段，但不做跨字段（超时嵌套/result 大小/allowlist）校验；registry 仅 enabled 时含 `delegate_task`；父 schema 数量变化；factory 只构造三个 native 工具、未知名称报错、永不返回 WASM/delegate；provider 计数包装被传给 executor（可用测试 provider 观察子调用扣减父 token）。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-cli --all-targets -- -D warnings
cargo test -p jessica-cli
```

禁止：默认启用委派；在 allowlist 缺 `delegate_task` 时静默注册；子 Agent 使用 WASM；让子工具经过父 `GovernedTool` 或以子失败触发父预算；修改 jessica-core；新增第三方 crate；在 replay/resume 路径构造 executor 或执行子 Agent。

Task 5.3.5：委派端到端验收（jessica-cli，90–120 分钟）

目标：用脚本化 SSE（不依赖真实 API key）证明父/子边界、失败语义、超时嵌套、token 预算、truncation、replay/resume 与不持久化。

E2E 场景至少覆盖：

1. 正常委派：父模型调用 `delegate_task(task, ["read_file"])`；子请求恰有 2 条 messages、tools 只有 `read_file`；子调用 read_file 后回答；父收到 `status=ok` JSON；父事件流只出现一次 `delegate_task` 的 Started/Finished，子 read_file 事件不在父流；exit 0。
2. 子 rounds 耗尽：子每轮都请求工具直到 `delegation.max_rounds`；父收到 `status=error`；父随后正常回答并 Completed/exit 0。
3. `max_child_tool_calls`：子连续请求超过上限；第 N+1 个工具未执行（用只读 marker 或 mock 计数断言）；父收到 `status=error` 且 Completed。
4. 子工具失败/超时：子工具返回错误后子模型正常回答 → 父仍收到 `status=error`（tool_errors 语义），父继续 Completed/exit 0；子 provider 挂起 → `delegation.timeout_ms=5001`（父 tool timeout 使用默认 10000ms）下约 5s 返回 `status=error`，父继续 Completed/exit 0。若该用例耗时明显，可只保留一个真实超时用例。
5. 父 token 预算被子 Agent 耗尽：子响应 usage 使父 `max_tokens` 越界；父最终 `SessionEndReason=BudgetExceeded`、exit 1；断言存在 token-limit BudgetExceeded、无子事件写入父库。
6. 结果截断：配置 `delegation.max_result_bytes=256`，子回答含中文/emoji 且超限；父收到合法 JSON `status=truncated`，只含 answer/original_bytes/status，answer 是 UTF-8 前缀且总字节 ≤256。
7. 权限：父 allowlist 无 `bash` 时模型请求 `["bash"]`；本地返回 `status=error`（父级未授权），子 provider 零请求，父继续 Completed。
8. 单层与隔离：父一次响应请求两个 `delegate_task`（不同 task/tools）；两个子请求各自只有自己的 task，互不包含对方结果；子工具集不含 `delegate_task`；子模型伪造委派只得到 unknown tool error。
9. disabled：未启用 delegation 时父 provider 收到的 tools 不含 `delegate_task`，模型即使请求也只得到 unknown tool。
10. replay/resume：完成一个有委派的 session 后 replay 两次逐字节一致，replay 不调用 provider、不执行子 Agent；resume 已完成 session 零模型/子调用；若父事件流停在 delegate ToolCallStarted 而无 Finished，resume 按既有 `IncompleteRound` 拒绝，绝不重新执行子 Agent。
11. 两个子 Agent 不共享可变状态：用不同 task 证明第二个子请求不包含第一个的答案/工具结果；父事件流中的两次 delegate 结果分别对应各自 task。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-runtime
cargo test -p jessica-cli
cargo test --workspace
```

并执行上述脚本化 E2E（测试内自动执行）。

禁止：让子事件进入父事件库；让子 Agent 失败改变父退出码；让父 token 预算耗尽被吞掉；在 replay/resume 中执行子 Agent；依赖真实 API key/网络；WASM 子实例；嵌套委派；修改 jessica-core。

Task 5.4 拆分：生产化加固

5.4 全局约束（适用于 5.4.1–5.4.5，违反任一条即验收失败）：

A. 不修改 jessica-core 已冻结的公共 API。所有能力通过实现 `jessica_core::llm::LlmProvider` 的包装层完成；`ClassifiedProvider`、`ClassifiedStream`、`ProviderFailure`、`ReliableProvider` 只放在 jessica-providers，不进入 core。
B. 不新增第三方 crate。重试、熔断、令牌桶、配置轮询、信号处理复用现有 tokio/std/reqwest/serde；新增依赖必须先修改 workspace Cargo.toml 并在对应 Task 说明理由。
C. `[reliability].enabled` 默认 false。默认行为、错误文案、事件序列、退出码不变；只有 enabled=true 才构造 `ReliableProvider`。
D. 调用链固定为：base provider → `ReliableProvider`（进程级共享）→ `TokenCountingProvider`（每 turn）→ AgentLoop / 子 Agent；技能提炼器直接使用 `ReliableProvider`。父、子、技能共享同一熔断与限流状态，不得各自新建可靠性实例。
E. 重试只发生在 Provider 层。不得重试工具调用、acceptance、AgentLoop 轮次或技能 JSON 解析错误；工具与 acceptance 的既有语义不变。
F. 流式提交点固定为第一个 `TextDelta`。首个 `TextDelta` 之前可重试；`ToolCallRequested` 必须缓冲到 `Done` 才交付；首个 `TextDelta` 之后绝不重试，错误原样以 `LlmEvent::Error` 结束。
G. 所有等待（退避、限流令牌、半开冷却）必须可被取消；不得在 async 上下文阻塞。测试用 `tokio::time::pause` 或注入时钟，不真实 sleep。
H. 日志只用 tracing，字段只含 attempt、failure kind、status、backoff、circuit state、reload/shutdown 事件；不得记录 prompt、完整响应、密钥或工具输出。
I. 热加载只作用于 REPL 的下一个 turn。one-shot、resume、replay、skills 子命令不启动热加载；每个 turn 取一份 `Arc<HarnessConfig>` 快照，`initial_messages` 与 `build_turn` 共用同一快照。
J. 热加载不改变消息构造流程：顺序固定为 `[system prompt, memory_guide_message, skill_guide_message, user]`；只替换 system prompt 内容与工具 schema；memory/skills 仍每 turn 独立召回一次，热加载不得触发额外召回，也不得重建 memory/skills。
K. 信号语义：SIGINT=130、SIGTERM=143；第一次信号停止接收新 turn、取消当前 turn、等待终态事件落库并 drain 技能队列；第二次信号立即退出。现有 Ctrl+C=0 断言必须同步改为 130。
L. 可靠性配置 schema 随 Task 增量增长：5.4.2 引入 enabled/max_attempts/backoff/timeouts；5.4.3a 引入 circuit_*；5.4.3b 引入 rate limit 字段。最终完整 `[reliability]` 示例在 5.4.3b 后才全部可用。
M. 每个子 Task 必须运行自身验收命令；5.4 整体完成后再运行 workspace 全量验收。测试必须是行为测试：脚本化 SSE server、故障注入、paused clock；不得只断言 Ok，不得依赖真实 API key/网络。
N. 若发现 5.4.1–5.4.5 之外的问题，记录到 Deferred，不在本 Task 顺手重构。

依赖顺序：5.4.1 → 5.4.2 → 5.4.3a → 5.4.3b → 5.4.4 → 5.4.5。5.4.3a/5.4.3b 逻辑独立，但共享 reliability 模块与 CLI 配置，串行执行避免冲突。

已确认决策（实现不得推翻）：

1. reliability 默认关闭。
2. SIGINT=130、SIGTERM=143。
3. 热加载五段：budget / policy / delegation / sensor / acceptance；其余重启生效。
4. 重试边界：首个 TextDelta 前可重试；ToolCallRequested 缓冲到 Done。
5. 5.4.3 拆为 5.4.3a 熔断、5.4.3b 限流。

Task 5.4.1：Provider 错误分类与请求/流空闲超时（jessica-providers，60–90 分钟）

目标：把 HTTP/网络/解析/流中断错误映射为结构化分类，并为 Provider 请求与流事件间隔增加可配置超时；本 Task 不实现重试、熔断、限流，不接 CLI。

操作：

1. 在 jessica-providers 新增 reliability 模块，导出 `ProviderFailureKind`、`ProviderFailure`、`ClassifiedStream`、`ClassifiedProvider`、`ReliabilityConfig`、`ReliableProvider` 与结构化 `ReliabilityConfigError`。
2. `ProviderFailureKind` 固定为：`Transport`、`Timeout`、`RateLimited`、`Server`、`Client`、`InvalidResponse`、`StreamInterrupted`。`ProviderFailure` 至少含 `kind`、`message`、`retry_after: Option<Duration>`；提供 `is_retryable()`：Transport/Timeout/RateLimited/Server 为 true，StreamInterrupted 由 5.4.2 按提交点决定，Client/InvalidResponse 为 false。
3. `ClassifiedStream` 固定为 `BoxStream<'static, Result<LlmEvent, ProviderFailure>>`；`ClassifiedProvider` 的 `stream_chat_classified` 返回 `Result<ClassifiedStream, ProviderFailure>`。这是 provider crate 内部扩展，core 的 `LlmProvider`/`LlmStream` 不变：`OpenAiProvider`/`AnthropicProvider` 的既有 `stream_chat` 把分类错误映射回 `LlmError` 或 `LlmEvent::Error`，错误文案保持现有测试断言。
4. HTTP 分类：408/425/429 为可重试 `Client`/`RateLimited`（429 使用 `RateLimited` 并解析 `Retry-After` 整数秒）；500/502/503/504 为 `Server`；其他 4xx 为不可重试 `Client`；reqwest 的 timeout 为 `Timeout`，连接失败/重置为 `Transport`；JSON/SSE 结构错误为 `InvalidResponse`；流提前结束为 `StreamInterrupted`。
5. `ReliabilityConfig` 本 Task 只含 `request_timeout_ms: u64`、`stream_idle_timeout_ms: u64`；0 表示禁用；校验上限与溢出，拒绝不可用值。`ReliableProvider::new(inner: Arc<dyn ClassifiedProvider>, config: ReliabilityConfig)`。
6. 请求超时覆盖 `inner.stream_chat_classified(...)` 的完整 setup 阶段，超时返回 `ProviderFailureKind::Timeout`；流空闲超时覆盖相邻两个 `ClassifiedStream` 事件之间的等待，超时产生 `ProviderFailureKind::Timeout`。本 Task 中该失败最终映射为 `LlmError::Request` 或 `LlmEvent::Error`，不做重试。
7. 不得修改 base provider 的请求体、SSE 聚合或既有错误文案；不得把分类依赖建立在字符串匹配上。
8. 所有公共 API 有 doc comment；错误用 thiserror；库代码不得 unwrap/expect。

行为测试（至少）：

· 两个 Provider 的 HTTP 状态分类表逐项覆盖 400/401/403/404/408/409/422/425/429/500/502/503/504，断言 kind 与 retry_after。
· `Retry-After` 缺失、整数秒、非法值分别得到 None、正确 Duration、None；不得 panic。
· 连接失败、请求超时、流空闲超时（首个事件前、首个事件后）分别得到 Transport/Timeout；使用 loopback server 或故障 `ClassifiedProvider`，不依赖真实网络。
· 两个 timeout 为 0 时禁用；用 paused clock 推进超过默认时间仍不产生超时。
· 既有 `LlmProvider` 的 HTTP 500/非 2xx 错误文案与事件序列不回归。
· `ProviderFailure` 的 `is_retryable` 表与 5.4.2 的预期一致。

验收：

```bash
cargo fmt --check
cargo clippy -p jessica-providers --all-targets -- -D warnings
cargo test -p jessica-providers
cargo test --workspace
```

禁止：实现重试/熔断/限流；修改 jessica-core；新增第三方 crate；改变默认 Provider 错误文案；把分类逻辑放进 CLI 或 runtime。

Task 5.4.2：重试状态机 + `[reliability]` 配置 + CLI 装配（jessica-providers + jessica-cli，90–120 分钟）

目标：实现首个 TextDelta 前重试、确定性退避、`Retry-After` 处理与 CLI 配置/装配；默认关闭，不改变既有行为。

操作：

1. 扩展 provider 的 `ReliabilityConfig`：`max_attempts`（含首次，默认 3）、`initial_backoff_ms`（默认 250）、`max_backoff_ms`（默认 4000）；校验 `max_attempts >= 1`、`initial_backoff_ms >= 1`、`max_backoff_ms >= initial_backoff_ms`，建议 `max_attempts <= 10`。
2. `ReliableProvider::stream_chat` 的 setup 阶段：对 `is_retryable()` 的失败重试；退避为 `min(initial * 2^(attempt-1), max)`，确定性、无 jitter；`Retry-After` 大于 `max_backoff` 时停止重试并返回该错误，否则等待 `max(backoff, retry_after)`。
3. 流式重试状态机：
   - `committed=false`，`tool_calls` 缓冲，不向上游交付；
   - 收到 `TextDelta`：立即向上游交付并置 `committed=true`；
   - 收到 `ToolCallRequested`：只缓冲，不交付；
   - 收到 `Done`：先按到达顺序交付缓冲的 tool calls，再交付 `Done`；
   - 收到 `Err(ProviderFailure)` 或流提前结束：若 `committed=false` 且还有 attempt，丢弃缓冲并重试完整请求；若 `committed=true`，交付 `LlmEvent::Error` 并结束，绝不重试；
   - 若 `ClassifiedStream` 仍产生 `LlmEvent::Error`（实现错误），按终止错误处理，不重试；
   - 成功只交付最终 attempt 的事件，禁止重复 TextDelta/tool calls。
4. 最终错误保留最后一次失败的原始 message；若 total attempts > 1，前缀为 `provider request failed after N attempts: ...`。所有退避等待与流等待可被 drop 取消；取消后不得继续发起请求。
5. CLI 新增 `[reliability]` 配置段（`#[serde(default, deny_unknown_fields)]`）：

```toml
[reliability]
enabled = false
max_attempts = 3
initial_backoff_ms = 250
max_backoff_ms = 4000
request_timeout_ms = 30000
stream_idle_timeout_ms = 120000
```

相对路径无关；`enabled=false` 时不构造 `ReliableProvider`，完全走现有 base provider。校验失败返回字段级 `ConfigError`。

6. CLI 的 reliability 配置结构与 providers 的 `ReliabilityConfig` 分离：CLI 负责 serde 解析与 `enabled`，校验后转换为 provider 配置。`build_provider` 在 enabled=true 时构造 base `Arc<dyn ClassifiedProvider>` 并包装为 `ReliableProvider`，返回 `Arc<dyn LlmProvider>`；enabled=false 时直接返回 base。
7. 该实例必须同时用于：`SkillsAssembly` 的提炼器、`CliRuntime` 每 turn 的 `TokenCountingProvider`、子 Agent 的共享 provider。不得在 `build_turn` 或子 executor 内重新构造可靠性包装。
8. 模块文档写明：重试可能在首个 TextDelta 前造成 provider 侧重复计费；客户端不重复输出文本、不重复执行工具；首个 TextDelta 后不重试。

行为测试（至少）：

· 500 → 500 → 200：底层恰好 3 次请求，最终事件只含第三次的 TextDelta/tool calls/Done，无重复。
· 401/400：只 1 次请求，原错误文案；429 带 `Retry-After` 时等待/停止规则正确。
· attempts 耗尽：错误含尝试次数与最后 message；`max_attempts=1` 不重试。
· TextDelta 后流 Error/EOF：只 1 次请求，已输出文本不重复，最终 `LlmEvent::Error`。
· 仅 ToolCallRequested 后、Done 前 EOF：允许重试；最终只交付成功 attempt 的工具调用；失败 attempt 的工具调用不泄漏。
· 流在 TextDelta 前空闲超时：重试；TextDelta 后空闲超时：不重试并产生 Error。
· 退避序列精确（250/500/1000/... cap）；`Retry-After > max_backoff` 停止；paused clock 无真实等待。
· 取消/丢弃 future 后不再发起新请求；重试中的 tool calls 缓冲不会提前进入 AgentLoop。
· CLI 配置默认值、每个非法边界、`enabled=false` 不包装且行为不变。
· CLI E2E（脚本化 SSE）：500/500/200 产生 3 次请求、1 个 Completed session、无重复文本；401 只 1 次请求并以 Error/exit 1 结束；[reliability] 缺省时现有 E2E 全部不回归。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-providers
cargo test -p jessica-cli
cargo test --workspace
```

禁止：默认启用可靠性包装；在 TokenCountingProvider/CLI 内实现重试；首个 TextDelta 后重试；把 tool calls 提前交付；新增第三方 crate；修改 jessica-core；重试工具或 acceptance。

Task 5.4.3a：熔断器（jessica-providers + jessica-cli，60–90 分钟）

目标：实现进程级、内存态、闭/开/半开三态熔断；只统计重试耗尽的逻辑调用失败，不涉及限流。

操作：

1. provider 的 `ReliabilityConfig` 增加 `circuit_failure_threshold`（默认 5）与 `circuit_cooldown_ms`（默认 30000）；校验 threshold >= 1、cooldown > 0。
2. 在 `ReliableProvider` 内持有 `Arc<CircuitBreaker>`，与 5.4.2 的重试状态机组合；状态用 `std::sync::Mutex` 保护，不跨 await 持锁。
3. 状态机：
   - Closed：允许调用；一次逻辑调用在重试耗尽后仍失败且 failure kind 可重试 → failure_count += 1；达到 threshold → Open；
   - Open：立即返回 `LlmError::Request`，文案含 `circuit breaker open` 与剩余冷却；不得调用底层 Provider；
   - 冷却到期 → HalfOpen；
   - HalfOpen：只允许一个探测；探测成功（最终 Done）→ Closed 并清零；探测失败 → Open；
   - 取消/drop 且未产生终态 → 中性，不计数也不释放为成功；HalfOpen 探测占位必须释放。
4. 计数单位是逻辑调用，不是单次 attempt；一次调用重试 3 次仍失败只记 1 次。不可重试的 4xx 说明端点可达，按中性/成功处理，不得打开熔断。
5. 流式调用的 permit 必须随 `ClassifiedStream` 生命周期移动：`Done`/终止错误时显式记录结果；流被 drop 时走中性释放；不得在 `stream_chat` 返回后丢失状态。
6. CLI 配置增加 `circuit_failure_threshold`、`circuit_cooldown_ms`，校验并转换到 provider 配置；`enabled=false` 时不构造熔断器。
7. 模块文档写明：熔断状态只在内存、进程重启重置；不持久化、不进入事件流；Open 是快速失败而非重试。

行为测试（至少）：

· Closed → threshold 次逻辑失败 → Open；Open 时底层 provider 零调用且返回明确错误。
· 冷却前 Open、冷却后 HalfOpen；HalfOpen 单探测，其他并发调用快速失败；探测成功 → Closed；探测失败 → Open。
· 一次调用重试 3 次后失败只计 1 次；可重试失败计数，401/400 不计数；成功调用清零。
· 取消/drop 不改变计数；HalfOpen 探测 drop 后可再次探测。
· 流式 Done 记成功、流式终止失败记失败、流式 drop 记中性。
· 配置默认值、threshold/cooldown 非法边界。
· CLI E2E：`max_attempts=1`、threshold=2，连续两个 503 的 turn 后第三个 turn 零 HTTP 请求并快速失败；4xx turn 不打开熔断。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-providers
cargo test -p jessica-cli
cargo test --workspace
```

禁止：把单次 attempt 计为一次失败；Open 时仍调用底层；HalfOpen 放行多个探测；持久化熔断状态；用熔断开改变工具/acceptance 语义；修改 jessica-core；新增第三方 crate。

Task 5.4.3b：速率限制（jessica-providers + jessica-cli，60–90 分钟）

目标：实现进程级请求速率令牌桶与并发上限；只限制 Provider 请求，不影响工具执行；不实现 TPM。

操作：

1. provider 的 `ReliabilityConfig` 增加 `requests_per_minute`（默认 0 = 不限）、`burst`（默认 1）、`max_concurrent`（默认 0 = 不限）；校验 rpm/burst/max_concurrent 的边界与上限，避免 Semaphore 过大。
2. 令牌桶：
   - 按每次物理请求 attempt 扣 1 个令牌；重试也占额度；
   - 桶容量 = burst；按 rpm 线性补充；核心为纯状态函数，接收注入的 `Instant`，返回“可立即获取/需等待的时长”；
   - 使用整数微令牌或等价的整数累计，避免浮点误差；等待用 `tokio::time::sleep`，可被 drop 取消；
   - `requests_per_minute=0` 时不等待。
3. 并发上限使用 `tokio::sync::Semaphore`：每个物理请求 attempt 在发出前获取 permit，持有到该 attempt 的流结束/失败/drop；重试时释放后重新获取；`max_concurrent=0` 时不构造 Semaphore。
4. 获取顺序固定为：先等待令牌，再获取并发 permit，再调用底层 Provider；避免持有 permit 等待令牌。令牌与 permit 必须在所有成功、错误、取消、drop 路径释放。
5. 同一个 `ReliableProvider` 实例共享令牌桶与 Semaphore，因此父、子、技能提炼器共用额度；不得每 turn 新建。
6. CLI 配置增加 `requests_per_minute`、`burst`、`max_concurrent`，校验并转换；`enabled=false` 时不构造限流器。
7. 模块文档写明：v1 只做 RPM 与并发，不做 TPM；usage 仅在 Done 可见，精确 TPM 需单独设计。

行为测试（至少）：

· 令牌桶：burst 内立即通过；超出后按 rpm 计算等待；paused clock 推进后恢复；rpm=0 永不等待。
· 非法 rpm/burst/max_concurrent 边界报配置错误；burst=0 在 rpm>0 时拒绝。
· 并发上限：max_concurrent=2 时第三个请求等待；两个完成后第三个进入；成功/失败/取消/drop 都释放 permit。
· 重试每次 attempt 都消耗一个令牌；重试不会绕过并发上限。
· 父、子、技能共享同一限流器（用计数型 mock 观察总请求数）。
· CLI E2E：请求间隔/并发峰值符合配置（使用宽松时间边界）；rpm=0 时行为与未启用一致。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-providers
cargo test -p jessica-cli
cargo test --workspace
```

禁止：实现 TPM；每 turn 新建限流器；持有 permit 等待令牌；用真实 sleep 写易抖动的精确时间断言；修改 jessica-core；新增第三方 crate。

Task 5.4.4：配置热加载（jessica-cli，90–120 分钟）

目标：REPL 模式下修改配置文件后，下一个 turn 使用新配置；不改变消息构造、Guide 注入或记忆/技能召回逻辑。

操作：

1. `CliRuntime` 从固定 `Arc<HarnessConfig>` 改为持有 `tokio::sync::watch::Receiver<Arc<HarnessConfig>>`；启动时仍加载一次配置并校验。
2. 仅 REPL 模式启动后台配置监听任务：每 1 秒检查配置文件的 `(mtime, len)` 指纹；不使用 `notify` 等新 crate。one-shot、resume、replay、skills 子命令不启动。
3. 指纹变化时重新加载并完整校验：
   - 只修改可热加载段（budget/policy/delegation/sensor/acceptance）→ 通过 `watch::Sender` 发布新 `Arc<HarnessConfig>`，日志 `[config] reloaded`；
   - 修改不可热加载段（events/memory/skills/wasm/reliability）→ 整体拒绝，日志指出字段，旧配置继续生效；
   - 解析/校验失败 → 日志 `config reload failed`，旧配置继续生效；
   - 无论成功与否更新指纹，同一份坏文件不重复刷屏。
4. 每个 turn 开始时 clone 一份 `Arc<HarnessConfig>` 快照；`initial_messages` 与 `build_turn` 必须接收同一快照，禁止一个用新配置、一个用旧配置。
5. `system_prompt` 不再缓存在 `CliRuntime`，改为每 turn 从快照调用 `system_prompt(root, config)`；工具 schema、policy、guards、delegation、sensor、acceptance 均从同一快照构造。
6. 消息构造保持现有实现：仍调用 `fresh_conversation`，顺序严格为 `[system prompt, memory_guide_message, skill_guide_message, user]`；memory/skills 仍每 turn 独立召回一次。热加载只改变 system prompt 内容与工具 schema，不得触发额外召回、不得重建 memory/skills 连接。
7. 后台监听任务在关闭时停止；监听任务本身不得阻塞 REPL 或 turn。
8. 若需要比较配置段，给相关结构补 `PartialEq` 或实现显式的 `ReloadableConfig`/`ImmutableConfig` 比较；不得用字符串序列化比较。

行为测试（至少）：

· 两轮 REPL 之间修改 `policy.allowed_tools`：第二轮 system prompt 与 provider tools schema 立即变化；memory/skills recall 次数仍各为每 turn 1 次。
· 修改 `budget.max_rounds`/`tool_timeout_ms`：第二轮按新值生效。
· 修改 `delegation.enabled` 且 allowlist 合法：第二轮注册 `delegate_task`；allowlist 缺失时整体拒绝并保留旧配置。
· 修改不可热加载段：旧配置继续生效，日志含字段名。
· 非法 TOML/非法边界：旧配置继续生效，REPL 不退出。
· turn 执行中修改文件：当前 turn 使用旧快照，下一个 turn 才生效。
· one-shot/resume/replay/skills 不启动监听、不读第二次配置。
· 消息顺序与 Guide 注入逻辑的既有测试保持通过。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-cli
cargo test --workspace
```

并执行一个脚本化 REPL E2E：启动 REPL、修改配置文件、输入第二个请求，断言第二次 provider 请求的 system prompt/tools schema 已变化且消息顺序不变。

禁止：热加载 events/memory/skills/wasm/reliability；在 turn 中途替换快照；额外召回 memory/skills；重建后台 worker；新增第三方 crate；修改 jessica-core。

Task 5.4.5：优雅关闭与端到端验收（jessica-cli，90–120 分钟）

目标：统一处理 SIGINT/SIGTERM，停止新 turn、取消当前 turn、等待事件落库与技能 drain，并按 130/143 退出。

操作：

1. 新增 jessica-cli 的 shutdown 模块与 `ShutdownController`：进程级 `CancellationToken`，每个 turn 使用 `child_token()`；提供 `request_shutdown`、`is_shutting_down`、`force` 等最小 API。
2. 启动时安装 SIGINT 与 Unix SIGTERM 监听；移除 `app.rs` 中每 turn 重复创建的 Ctrl+C 监听任务。REPL 的 `read_line` 在关闭信号到达时必须被唤醒并停止接受新输入。
3. 第一次信号：设置 shutdown 标志、取消当前 turn 的 child token、停止 REPL、开始 drain。第二次信号：立即退出，不再等待，记录 stderr 警告。
4. drain 顺序固定：
   - 停止创建新 turn；
   - 取消当前 turn；
   - 等待当前 turn 的终态事件由 EventPump 持久化（复用 `PUMP_TIMEOUT=30s`）；
   - 调用 `SkillsAssembly::drain()`，受 `skills.drain_timeout_ms` 限制；
   - flush stdout，返回退出码。
5. 退出码：正常完成 0；SIGINT 130；SIGTERM 143；第二次信号立即返回对应信号码；技能 drain 超时只 warn，不改变信号退出码。现有 `Cancelled => 0` 的断言与实现同步调整。
6. 模块文档写明已知限制：
   - 取消产生的 `SessionEnded(Cancelled)` 在当前恢复语义下是终态，不能 `--resume`；可恢复关闭需单开 Task；
   - CommandSensor/CommandAcceptanceCheck 无超时，挂起时只能靠第二次信号强制退出；命令超时另开 Task；
   - 不处理 SIGHUP 等其他信号。
7. 关闭期间不得写入未经脱敏的日志；不得 panic；不得在 async 上下文阻塞等待。

行为测试（至少）：

· 单元测试用注入的信号通道驱动 `ShutdownController`：第一次信号取消 child token、第二次信号 force、重复信号幂等。
· 两个 turn 的 child token 隔离：取消一个不影响另一个。
· Unix E2E spawn CLI，发送 SIGTERM：退出码 143；事件库存在当前 session 的 `SessionEnded`；技能 worker 已 drain 或超时后可恢复；stdout 无 panic。
· SIGINT 路径退出码 130；第二次信号强制退出单独测试。
· 空闲 REPL 收到信号立即退出，不创建新 turn。
· 现有 Ctrl+C/exit code 测试更新后全部通过。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p jessica-cli
cargo test --workspace
```

并执行上述 Unix 信号 E2E（测试内自动 spawn，不依赖真实 API key）。

禁止：把 SIGINT/SIGTERM 退出码混为 0；信号处理散落在每个 turn；取消后继续发起模型请求；在 drain 期间新建 turn；修改 jessica-core；新增第三方 crate。

5.4 已知限制与非目标：

· 重试可能造成 provider 侧重复计费；客户端不重复输出/执行工具，但不保证服务端不计费。
· 无 jitter；单进程 CLI 场景接受同步退避。
· 熔断状态只在内存，进程重启重置。
· 只做 RPM + 并发，不做 TPM。
· 热加载不覆盖 events/memory/skills/wasm/reliability。
· 取消后的 session 不可 resume；命令 sensor/acceptance 无超时；这些是既有 Deferred，不在 5.4 顺手修改。
· 不修改 jessica-core，不做 OTLP/指标导出（5.5）。

---

Claude Code 执行纪律（写入 CLAUDE.md 末尾）

1. 一次会话只做一个 Task，完成后主动运行该 Task 的所有验收命令，把结果贴出来。
2. 验收命令失败时不得声明完成，应修复后重跑，或明确报告阻塞点。
3. 修改 jessica-core 的公共 API 前必须先停下询问，因为这会破坏跨 Phase 契约。
4. 测试必须是行为测试，不得写“调用后断言返回 Ok”这类自证式测试；mock 必须构造真实的边界条件。
5. 不要在 Task 范围外顺手重构，发现的问题记录到 PLAN.md 的 “Deferred” 列表，由你决定是否单开 Task。
6. 每个 Task 结束前更新 PLAN.md：勾选完成、记录实际耗时和偏离计划的地方。

---

进度追踪

Task 状态 实际耗时 偏离说明
Task 0 ☑ 约 5 分钟 无；仅创建 CLAUDE.md 与更新本表，未写 Rust 代码
Task 0.1 ☑ 约 20 分钟 环境原先无 Rust：经 rsproxy.cn 镜像安装 rustup 并固定 Rust 1.99.1；cargo new 用 --vcs none 避免嵌套 git；外部依赖仅在 workspace.dependencies 集中声明，尚未被成员使用
Task 0.2 ☑ 约 25 分钟 契约补充：Message 增加 tool_call_id（OpenAI tool 消息必需）；LlmEvent::Done 带可选 TokenUsage；AgentEvent 增加 Error/PolicyDenied，SessionEnded 增加 reason；Guard 统一为 check(GuardContext) 签名；ToolRegistry 用 BTreeMap 保证确定性迭代顺序；crates.io 依赖经 rsproxy 镜像拉取（环境配置）
Task 1.1 ☑ 约 25 分钟 新增 reqwest（rustls-tls/json/stream）用于流式 HTTP（已在 workspace.dependencies 说明理由）；SSE 解码自行实现（字节级缓冲，处理跨 chunk 的 UTF-8/换行）；请求带 stream_options.include_usage 以填充 TokenUsage；环境变量 OPENAI_API_KEY 必需，OPENAI_BASE_URL/OPENAI_MODEL 可选；未做真实网络调用（无 API key），按计划用 mock SSE 流验收
Task 1.1b ☑ 约 40 分钟 jessica-providers 新增 anthropic 模块（config/request/sse/stream/provider），实现 AnthropicProvider。请求构造把 system 消息合并到顶层 system；Message content 转 block 数组，assistant text + tool_calls 转 text/tool_use blocks，连续 Tool 消息合并为一个 user 消息的 tool_result blocks；OpenAI 风格 function schema 解包为 {name, description, input_schema}，同时接受直接 name/parameters 形式。SSE 层自行实现按行缓冲的 AnthropicSseDecoder（字节级缓冲，保留跨 chunk 多字节 UTF-8），parse_anthropic_sse_line 识别 event:/data:，完整帧反序列化为强类型 AnthropicSseEvent 并校验 event 名与 payload type 一致。AnthropicStreamAssembler 以 content_block index 维护 BTreeMap 状态：text block 累积并逐 delta 发出 TextDelta；tool_use block 累积 input_json_delta.partial_json，在 content_block_stop 解析为 ToolCall（空参数回退 start 时的 input），usage 从 message_start/message_delta 合并为 TokenUsage。stream_chat 用 reqwest 发送 x-api-key + anthropic-version: 2023-06-01，base_url 兼容带/不带 /v1；HTTP 非 2xx、event: error、SSE/JSON 解析失败全部转为 LlmEvent::Error，网络发送与配置错误仍为 Err(LlmError::Request)（与 OpenAI Provider 一致）。配置 ANTHROPIC_API_KEY 必需，ANTHROPIC_BASE_URL 默认 https://api.anthropic.com，ANTHROPIC_MODEL 默认 claude-sonnet-5-5（常量集中定义），Debug 脱敏；max_tokens 固定 8192（Messages API 必填，本 Task 未做配置项）。新增 tests/fixtures/anthropic_sse_text_and_tool.txt 与 anthropic_sse_error.txt；31 个新增测试覆盖 system 抽取、tool_use/tool_result 往返、连续 tool_result 合并、schema 映射、完整具名事件流、1 字节切分（含中文）、text/tool 交错、多工具并行、非法工具 JSON、event: error、缺少 message_stop、loopback mock HTTP 完整流与 HTTP 500 错误事件。未新增第三方 crate：reqwest 复用 Task 1.1 的 workspace 声明；仅为测试增加 workspace tokio dev-dependency。jessica-core 未改，Anthropic block 结构未泄漏。workspace 共 218 个测试全绿。补充真实端点验证：新增 tests/anthropic_live.rs 两个 #[ignore] 测试（需 ANTHROPIC_API_KEY/ANTHROPIC_BASE_URL/ANTHROPIC_MODEL 与真实网络），覆盖文本流与工具调用 + tool_result 两轮往返；2026-11-11 对本机配置的 Anthropic 兼容端点实测 2 passed（文本回复 pong；工具调用 get_weather {"city":"Paris"}；回填结果后返回 21°C/clear 的文本回答），默认套件仍为 218 passed + 2 ignored。应“用 Jessica 跑一个 hello world Python 程序”的真实运行请求，CLI 增加 JESSICA_PROVIDER 开关（openai 默认 / anthropic，读取对应 OPENAI_* 或 ANTHROPIC_*），新增 3 个选择解析测试；以 JESSICA_PROVIDER=anthropic 真实跑通：模型调用 write_file 写入 hello.py（23 字节），再调用 bash 执行 python3 hello.py，实际输出 Hello, World!、exit 0、session Completed。该 CLI 接入属于 Task 1.1b 范围外的用户临时请求，是否正式立 Task 由用户决定；workspace 共 221 个测试全绿。
Task 1.2 ☑ 约 30 分钟 新增 tokio-util（rt feature）提供 CancellationToken；循环只强制 max_rounds，token/tool_timeout 按计划留给 Phase 2 guards；ToolResult.tool_call_id 由循环按原始 ToolCall 重写（Tool::execute 只能拿到 args，拿不到 call id）；未知工具以错误 ToolResult 回填而非终止；LlmEvent::Done 的 usage 暂未消费，已记入 Deferred
Task 1.3 ☑ 约 45 分钟 原生工具实现在 jessica-tools，CLI 只负责装配；read_file 与 write_file 均限制在当前目录内（计划仅要求 read_file），用 canonicalize 拒绝 `..`、绝对路径与 symlink 逃逸，仍存 TOCTOU 竞态；bash 默认 10s 超时、每流 64 KiB 上限，kill_on_drop 只杀直接子进程；workspace 为 CLI 增加 tokio `io-std` feature；REPL 每次输入是独立 session，不保留跨输入历史；无参数进入 REPL，有参数时全部拼接为一个 prompt；无真实 API key，验收改用本地 mock SSE server，并额外验证缺失 key 报错与 SIGINT 中断
Task 2.1 ☑ 约 30 分钟 按依赖单向规则 harness 只能依赖 core，不能直接引用 jessica-runtime 的 AgentLoop；Harness 改为自持 broadcast 通道并用泛型 agent future 包裹，真实 AgentLoop 在 Task 2.4 由 CLI 接入。固定检查点仅 Hook 已定义的 on_session_start/on_event/on_session_end，未提前实现 Guard/Sensor/Policy。新增 harness 依赖 async-trait/tokio/tracing（均为 workspace 已有依赖），测试另加 serde_json dev-dependency；LoggingHook 用 tracing debug 且只记录事件类型，避免泄露模型文本；广播容量有界，hook 过慢时接收端 lag 会丢事件并告警
Task 2.2 ☑ 约 40 分钟 Harness 新增 with_guards/with_policy 与 check_guards/authorize_tool_call 显式检查点；四种 Guard 与 AllowlistPolicy 已实现，10 个行为测试覆盖每种 guard 触发、allowlist 拒绝、默认 DenyAll、首命中顺序、同轮幂等/reset 与空上下文不 panic；无新增依赖；因 AgentEvent 不携带 token usage、round 或 tool elapsed，Harness::run 仍不能自动治理，真实接入留待 Task 2.4；StuckDetectionGuard 有跨轮状态，每 session 需新建或 reset；AllowlistPolicy 由调用方注入名单，配置文件在 Task 2.4
Task 2.3 ☑ 约 40 分钟 新增 SensorPipeline/SensorReport 与 CommandSensor（直接 exec、无 shell、采集 stdout+stderr、spawn 失败转为 fail）；Harness 增加 with_sensors/run_sensors；新增 AcceptanceRunner：仅在 Completed 尝试后运行 AcceptanceCheck，失败时发 AcceptanceFailed 事件、把 feedback 作为 user Message 注入并重入，max_retries 为额外重试次数；9 个行为测试覆盖 sensor pass/fail、command pass/fail/spawn 失败、acceptance 首次通过/失败重入/重试耗尽/非 Completed 跳过；无新增依赖；harness 不能依赖 runtime，acceptance 通过泛型 attempt 闭包驱动，真实 AgentLoop 接入与最终 SessionEnded 事件留待 Task 2.4；CommandSensor 暂未设超时或输出上限
Task 2.4 ☑ 约 30 分钟 CLI 新增 config/app/governance/pump/acceptance_check 五个模块与 examples/harness.toml。因本 Task 禁止修改 runtime，且 AgentLoop::run 只返回 SessionEndReason，治理在 CLI 边界显式装配：TokenCountingProvider 在 LlmEvent::Done 计费并触发 TokenBudgetGuard；GovernedTool 在调用前查 AllowlistPolicy、执行时强制超时、执行后查 ResultSizeGuard，拒绝/超限均发结构化事件；EventPump 从 AgentEvent 重建 transcript、在 ToolCallStarted 调用 check_guards、在 ToolCallFinished 运行 CommandSensor；AcceptanceRunner 用重建的 messages 重试，失败反馈作为 user message 注入；每个 attempt 前 reset StuckDetectionGuard；trip 后取消 CancellationToken 并把最终 reason 统一为 BudgetExceeded，外层再发一个 SessionEnded 承载最终 turn 语义。新增 workspace 依赖 toml = "0.9"（仅 CLI 解析配置）。越权工具以错误 ToolResult 返回并继续会话，模型可改写；预算 trip 后抑制内层矛盾的 Completed/Cancelled。无真实 API key，E2E 用本地脚本化 SSE server 验收：越权 bash 被 PolicyDenied、sensor pass、首次 acceptance 失败并带 feedback 重试、第二次 write_file 修复后通过（exit 0）；max_tokens=10 的预算场景输出 BudgetExceeded 并以 exit 1 终止。README 首句保留故意拼写错误作为验收夹具，E2E 后已恢复。新增 22 个 CLI 测试，workspace 共 131 个测试全绿；jessica-runtime 未改动
Task 3.1 ☑ 约 25 分钟 jessica-sandbox 引入 wasmtime 50.0（default-features=false，仅启用 cranelift/runtime/std，不含 WASI）；新增 WasmEngine/WasmModule/WasmInstance 的同步 API，完成 load_file/load_bytes → 空 Linker 实例化 → call_i32 导出调用，错误用 SandboxError 区分 Engine/Read/Compile/Instantiate/UnknownExport/Signature/Trap。测试在 tests/fixtures/ 放置 fixed_value.wat、预编译 fixed_value.wasm（answer 返回 42、trap 触发 unreachable）和 build.sh；已用 `wat` crate 验证 .wasm 与 .wat 解析结果逐字节一致。6 个行为测试覆盖文件加载、字节加载、缺失文件、非法二进制、缺失导出、trap 后宿主/store 仍可用。环境未安装 wasm-tools，build.sh 需要时可用 `cargo install wasm-tools` 后重建，测试直接读取已提交的预编译 fixture，不依赖该工具。未接入 Tool Trait，未启用 WASI；jessica-core/runtime 未改动
Task 3.2 ☑ 约 55 分钟 jessica-sandbox 新增 workspace 依赖 wasmtime-wasi 50.0（default-features=false，features=["p1"]；p1 内部依赖 p2，因此同时启用 wasmtime 的 component-model/async，已在 Cargo.toml 注明理由）。WasmEngine 打开 consume_fuel 与 epoch_interruption。新增 SandboxPolicy/ReadOnlyDir：默认无 preopen、无网络，仅提供 grant_read_only_dir（没有写权限 API）；默认 fuel 10M、单次墙钟 5s、每内存 64MiB、每 table 10000 元素、每 store 1 instance。instantiate 用 WasiCtxBuilder 显式关闭 TCP/UDP/IP 名称解析，按 policy 以 FsPerms::ReadOnly 预打开目录；StoreLimits 通过自定义 ResourceLimiter 委派，trap_on_grow_failure=true。instantiate 与 call_i32 每次先 set_fuel、set_epoch_deadline(1)，再启动可取消的 watchdog 线程，操作结束后发送取消并 join，避免 stale epoch 影响后续调用；call_i32 将 Trap::OutOfFuel 映射为 FuelExhausted、Trap::Interrupt 映射为 DeadlineExceeded，其余保留 Trap。新增 wasi_fs/spin/limits 三组 .wat 与预编译 .wasm（仍用 wat crate 生成并逐字节核对；build.sh 改为遍历全部 .wat，环境仍无 wasm-tools）。测试：wasi_capabilities 5 个（默认无 preopen 拒绝且实例存活、只读目录读取真实内容、`../` 逃逸被拒、写/创建被拒且无宿主文件、缺失目录报 Capability），resource_limits 6 个（fuel 耗尽→FuelExhausted 且后续调用可用、100ms 墙钟→DeadlineExceeded 且后续调用可用、内存/表超限 trap、限额内增长成功、instance limit 0 拒绝实例化），wasm_loading 6 个改用默认 policy；jessica-sandbox 共 17 个测试，workspace 147 个全绿。jessica-core/runtime 未改；仍未接 Tool Trait。
Task 3.3 ☑ 约 30 分钟 jessica-sandbox 新增 BufferAbi 与 WasmInstance::call_buffer：manifest 选择 alloc/invoke 两个导出，host 通过 alloc 写入 JSON 参数，invoke 返回高 32 位长度、低 32 位指针的 i64，host 校验输出上限与内存边界后读回；新增 MissingMemory/InvalidAllocation/OutOfBounds/InputTooLarge/OutputTooLarge/MemoryWrite 错误，所有 trap 仍区分 FuelExhausted/DeadlineExceeded/Trap。jessica-tools 新增 jessica-sandbox（Task 3.3 起允许）与 thiserror 依赖，实现 WasmTool：manifest JSON 提供 name/description/schema/module/abi/permissions，缺省资源限制继承 SandboxPolicy，相对 module 与只读目录按 manifest 目录解析，未知/写权限字段、零 fuel/墙钟/内存/实例、相对 guest path、缺失只读目录均在加载期拒绝，max_result_bytes 默认 64 KiB。每次 execute 在 spawn_blocking 内新建实例，JSON 参数写入 guest、输出经 UTF-8/JSON 校验；trap、fuel 耗尽、越界、非法输出全部转为 is_error=true 的 ToolResult，不吞错；每个工具用 Mutex 串行化调用，避免 Engine 级 epoch watchdog 互相干扰。新增 register_wasm_tool 注册入口，native 与 wasm 共用 ToolRegistry。提供 examples/plugins/echo（echo.wat 源码、预编译 echo.wasm、manifest.json、build.sh、README）与 examples/wasm_tool.toml；环境无 wasm-tools 与 wasm32 target，.wasm 仍用缓存中的 wat crate 生成并核对。CLI 新增 [wasm].manifests 配置，启动时用一次 spawn_blocking 加载并编译，随后与 native 工具一起注册并统一经 GovernedTool 治理。测试新增 18 个 wasm_tool 行为测试（echo 往返、trap/fuel/越界/非法 JSON/非法 UTF-8/负分配指针/缺失导出均返回错误结果、manifest 校验、混合注册与重复名）、3 个 CLI 配置测试和 1 个真实 CLI E2E（脚本化 SSE server：read_file 被 PolicyDenied 后 echo_json 成功，结果回填模型，exit 0）；workspace 共 169 个测试全绿；jessica-core/runtime 未改。偏离：采用每次调用新实例而非复用实例以保证 trap 隔离；WAT 作为示例源码（未安装 wasm32 target）；ABI 固定为两个导出，不支持 dealloc/有状态插件
Task 4.1 ☑ 约 20 分钟 jessica-memory 新增 workspace 依赖 rusqlite 0.41（bundled，编译内置 SQLite，不依赖系统 libsqlite3/头文件），移除 cargo new 模板。新增 EventStore：`events(session_id, seq, event_type, payload_json, timestamp, PRIMARY KEY(session_id, seq)) WITHOUT ROWID`；open/open_in_memory 时安装 schema，并用 BEFORE UPDATE/DELETE 触发器 RAISE(ABORT) 在数据库层强制 append-only。append 在事务内按 session 计算 MAX(seq)+1，序列从 1 开始且按 session 独立，返回 StoredEvent（session_id/seq/event_type/payload_json/timestamp/decoded event）；events 按 seq ASC 读回并在读时反序列化 AgentEvent，同时校验 event_type 与 payload 的 serde tag 一致；last_seq 返回末尾序列或 None。Timestamp 是调用方显式提供的 Unix 毫秒 newtype，库不读宿主时钟，保证事件捕获与重放可控；错误统一为 EventStoreError（Open/InMemory/Schema/Serialize/Append/Read/Decode/EventTypeMismatch/Sequence）。同步 API，文档要求 async 调用方用 spawn_blocking。5 个行为测试：全部 9 个 AgentEvent 变体按 seq 往返且 payload 字节/event_type/timestamp 完全一致；session 独立序列 + 关闭重开后仍可读 + 未知 session last_seq=None；schema 列名正确且 UPDATE/DELETE 被触发器拒绝、原行保留；乱序 INSERT 后仍按 seq 返回；损坏 JSON 与 type 不一致分别报 Decode/EventTypeMismatch（含 session/seq）。jessica-core 未改；MemoryStore（分层记忆）仍留 Phase 5。
Task 4.2 ☑ 约 60 分钟 jessica-memory 新增 sessions 元数据表（session_id, messages_json, started_at）及 UPDATE/DELETE 拒绝触发器：AgentEvent 没有 user/system message 变体且 core 已冻结，恢复所需的初始消息无法从事件流重建，因此由 CLI 在 fresh session 开始前写入不可变 bootstrap；EventStore 增加 record_session/session_messages 与 Session/SessionDecode/SessionConflict 错误，并设置 5s busy_timeout。jessica-runtime 新增 recovery 模块（RecordedEvent/SessionCheckpoint/RecoveryError/recover_session），纯函数重放事件到指定 seq：校验 session id 与严格递增 seq，按 ModelDelta/ToolCallStarted/ToolCallFinished 重建 assistant+tool messages，SessionEnded 立即终结；模型回合中途或工具批次缺结果时以 IncompleteModelTurn/IncompleteRound 拒绝，绝不重放 LLM 或工具。AgentLoop 抽出 run_loop 并新增 resume：已终结 checkpoint 直接返回记录 reason 且零调用；未终结时用 checkpoint.messages 与 rounds_completed 继续，不重复发 SessionStarted，剩余 round 预算 = max_rounds - rounds_completed。验收测试用三段脚本 provider/tool：不中断跑到 Completed，取第一个 ToolCallFinished 为 checkpoint，用全新 provider/tool 恢复，断言恢复事件序列与不中断尾部逐事件相等、provider 第二次调用 messages 与 checkpoint 完全一致、工具只执行未完成的 step2。CLI 新增 [events].path 与 events.rs（EventRecorder 全部走 spawn_blocking，时间戳由 CLI 读取宿主时钟）；EventPump 按事件顺序持久化并在 register 时设置 current 以归属没有 SessionStarted 的恢复流；--resume <id>/--resume=<id> 只能单独使用（不可与 prompt 同用），恢复后沿用同一 session id 续写。E2E：预置 SQLite 中停在完整工具批次的中断会话，真实 CLI --resume 只发 1 次模型请求，请求体含既有 user message/tool call/tool result，结束后同一 session 追加到 seq 5。已知限制：事件模型无显式 round 边界，连续无文本的纯工具模型回合会被计为一个 round（只会低估，不会高估）；恢复路径不经过 acceptance 重试；AcceptanceFailed 发生在两个 attempt 之间、无 current session，不会持久化。workspace 共 183 个测试全绿。
Task 4.3 ☑ 约 50 分钟 jessica-cli 新增 replay 模块（Script/ScriptBuilder/ScriptedProvider/ReplayTool）。从 recorder.load 的 RecordedEvent 重建模型轮次：ModelDelta→LlmEvent::TextDelta，ToolCallStarted→LlmEvent::ToolCallRequested，ToolCallFinished→按工具名与调用顺序入队的记录结果，Error→LlmEvent::Error；因事件不保存 Done usage，每轮结束时合成 Done { usage: None }。用真实 AgentLoop::run + 脚本化 Provider/ReplayTool 驱动循环，Budget.max_rounds 取脚本轮数：不发起任何模型请求，不执行任何真实工具，不读宿主时钟/随机数/网络，工具结果完全来自事件流。支持 Completed、Error 与 max-rounds BudgetExceeded：Completed 若末尾是工具批（事件流看不到随后空回复的模型轮），补一个仅含 Done 的空轮以让循环自然 Completed；Error 由脚本中的 Error 事件复现；BudgetExceeded 由 round 预算复现；Cancelled/AcceptanceFailed 与缺少 SessionEnded 的中断流明确拒绝（中断请用 --resume）。输出 JSONL 每行一个紧凑 AgentEvent，无 seq/timestamp；write_jsonl 在 spawn_blocking 中写文件。CLI 解析 `jessica [--config <path>] replay --session <id> --out <path>`：--session/--out 支持 `=` 形式，重复/缺失/未知参数均结构化报错，且 replay 分支在创建 OpenAiProvider/加载 wasm 工具之前返回，因此无需 OPENAI_API_KEY，也不会触网。测试：replay 模块 3 个行为测试（三次终端复现、精确事件/字节一致、拒绝半批与不支持的终止原因）与 1 个真实 CLI E2E（无 API key、无 server，连续跑两次并逐字节比较，同时断言输出等于记录的 6 个事件）。手工验收：用 Python 生成含 sessions/events 的 SQLite，执行 `cargo run -p jessica-cli -- --config ... replay --session manual-replay --out a.jsonl` 两次，`diff a.jsonl b.jsonl` 退出 0，输出 6 行且与记录事件一致。workspace 共 187 个测试全绿；jessica-core/runtime/memory 未改。偏离：replay 只重建 runtime 循环层，不重放 CLI 治理事件（PolicyDenied/AcceptanceFailed/CLI 侧 BudgetExceeded 不会重新发出，其可观察结果已在记录的 ToolResult/终止原因中）；replay 使用脚本轮数而非配置的 max_rounds。

Task 5.1.1 ☑ 约 30 分钟 jessica-memory 新增 memory_log 模块与 MemoryLog 同步存储：memory_entries(id, session_id, workspace_scope, content, metadata_json, created_at, deleted_at) + WHERE deleted_at IS NULL 的部分唯一索引 + FTS5 trigram 外部内容表与同步触发器；BEFORE UPDATE 只允许 deleted_at 由 NULL 单向置值且其余列逐列不变，其他 UPDATE/DELETE 均 RAISE(ABORT)；INSERT OR IGNORE 去重，软删除后可重新写入；busy_timeout=5000ms、user_version=1（未来版本报 UnsupportedVersion）。API：NewMemoryEntry（校验空内容）、MemoryRecord、MemoryEntryId、WorkspaceScope（拒绝空 scope）、append/get/recent_for_session/search_keyword/soft_delete。搜索先把输入切成字母数字 token，长 token 逐个加引号后 AND 进 FTS5 MATCH，<3 字符 token 用转义 LIKE 追加过滤，纯符号查询返回空而非报错；排序固定 created_at DESC, id DESC。FTS5 trigram spike 在 bundled SQLite 上通过（中文全文检索/英文前缀均命中），因此未实现 unicode61 回退路径；若未来构建移除 trigram，打开会报 Schema 错。14 个行为测试覆盖 user_version/重开、未来版本拒绝、13 类特殊字符查询（含 LIKE 下划线转义）、中文 trigram 与 2 字 LIKE、作用域隔离、去重、软删除与 FTS 清理、软删除后重写、物理 UPDATE/DELETE 及“软删除+改内容”拒绝、同时间戳排序、limit、metadata 往返与损坏 JSON、空内容/空 scope、Send。新增测试未引入第三方依赖；jessica-core 未改。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（235 passed, 2 ignored；jessica-memory 6 event_store + 14 memory_log）。
Task 5.1.2 ☑ 约 35 分钟 jessica-memory 新增 embedding 模块：async Embedder Trait（embed/model_id/dims，async-trait 复用 workspace 依赖，为 5.1.6 HTTP/本地实现留接口）与 EmbedError；DeterministicEmbedder 固定 256 维，对 UTF-8 字节取长度 2..=4 的 n-gram，项目内 FNV-1a 64 哈希取低 8 位为 bucket、最高位为符号，累加后 L2 归一化，空串/纯空白（含单字符无 n-gram）返回全零；doc 明确标注无语义理解、碰撞噪声，不得作为生产语义检索质量依据。新增 encode_vector/decode_vector 手写小端 f32 编解码，VectorError 覆盖 InvalidByteLength/DimensionMismatch/NonFinite/DimensionsTooLarge，编码与解码均拒绝 NaN/Inf。memory_entries 之外新增 memory_embeddings(entry_id REFS memory_entries(id), model, dims, vector BLOB, created_at, PK(entry_id,model))，打开时启用 foreign_keys；schema user_version 1→2，旧库重开自动建表且保留原条目。MemoryLog 增加 pub(crate) put_embedding（UPSERT 覆盖同一 entry/model）与 scan_embeddings（JOIN 条目过滤 workspace/model/dims/deleted_at，entry_id 升序）。定义 VectorIndex Trait 与借用 &MemoryLog 的 BruteForceIndex：add 写入/覆盖，search 做余弦相似度（零范数为 0.0，非有限中间值降级 0.0），分数降序、同分 entry_id 升序，空 query/limit=0/维度不匹配/无 model 数据返回空。8 个 embedding 行为测试覆盖同一文本逐字节一致、硬编码 "abc" 的 1024 字节样本锁定算法/字节序、256 维与 L2≈1、空串全零、不同文本不同向量、小端往返、非法长度/dims/NaN/Inf、余弦排序与同分 id、workspace/model/dims/软删除过滤、UPSERT 覆盖、缺失条目外键拒绝、零范数；memory_log 新增 1 个 v1→v2 迁移测试（原 14 个测试更新为版本 2/未来版本 3）。未新增 workspace 第三方依赖（async-trait 已有；futures 仅测试 dev-dependency）；jessica-core 未改。偏离：为容纳 embeddings 表将 user_version 从 1 升到 2；VectorIndex::add 增加调用方 Timestamp 参数以遵守“库不读时钟”；BruteForceIndex 借用 MemoryLog 而非自持连接，便于 5.1.3 在单个 Arc<Mutex<Connection>> 上串行化。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（244 passed, 2 ignored；jessica-memory 6 event_store + 15 memory_log + 8 embedding）。注：清理了发布 v0.2.0 时共享 target 目录留下的 worktree 缓存，避免测试二进制引用已删除的 /tmp 路径。
Task 5.1.3 ☑ 约 30 分钟 jessica-memory 新增 memory_store 模块，新增 workspace 依赖 sha2 0.10（用户已批准，仅用于 workspace scope），并启用已在 workspace 中的 tokio/tracing（无版本变化）。RecallRequest 含 session/query/limit/short_weight/keyword_weight/vector_weight/rrf_k/max_bytes，默认三路权重 1.0、rrf_k=60、max_bytes=4096；workspace scope 由 store 固定、请求不可覆盖。RRF 对每路候选累加 weight/(k+rank)，rank 从 1 开始，融合分降序、同分 entry_id 升序，再按 limit 截断；权重必须有限非负、k>0。三路候选：短期 recent_for_session(session,scope,limit)、中期 search_keyword(scope,query,limit)、长期 BruteForceIndex::search(embed(query), embedder.model_id(), scope, limit)（无 embedder 或权重 0 时跳过）。任一路失败只 tracing::warn 并继续；若有失败，用 recent_for_session(...,0) 做健康检查，仅当健康检查也失败才返回 MemoryError::Backend。字节上限按融合顺序累计 content 字节与条目间 3 字节 `---` 分隔符，边界回退到 UTF-8 char boundary；条目被截断后停止追加；max_bytes=0/limit=0 返回空；单条超限时截断而非整体返回空。SqliteMemoryStore 构造时接收 canonical workspace path，固定 workspace_scope=hex(sha256(path))[..16]；内部 Arc<Mutex<MemoryLog>>，所有 rusqlite 调用（写入、embedding 写、三路召回、健康检查）均在 tokio::task::spawn_blocking 中执行。write_at(session, entry, Timestamp) 保存 metadata 原样、拒绝空 content，写入后按配置 embedder 生成并存储向量，embedding/向量存储失败只 warn、不回滚条目；冻结 Trait 的 write 无时间戳，委派 write_at 并使用 Unix epoch 0；Trait recall 委派默认 RecallRequest。16 个行为测试覆盖：Arc<dyn MemoryStore> 写入/召回与 metadata、session 短期隔离与 workspace 关键词跨 session、不同 workspace 在短/关键词/向量三路均隔离（同内容用 metadata 区分）、RRF 双榜条目优先与同分 entry_id、权重调整改变顺序、长期路跨 session、20/9/4/0 字节上限与中文 UTF-8 截断/单条超限/limit=0、失败 embedder 降级但关键词仍可召回、删除 FTS 表后关键词路降级而短期路返回、删除条目表后返回 Backend、重复写入去重/软删除后三路均不召回/重写后可召回且审计行保留、重开库后条目与向量均保留、非法权重/NaN/Inf/k=0、空 content（固有与 Trait 两个入口）、scope 为 sha256 前 16 hex 且同路径稳定/不同路径不同/空路径拒绝、Send+Sync。偏离：1) 冻结 write 无时间戳，故新增固有 write_at 并让 write 使用 epoch 0，5.1.5 写入真实 turn 时必须用 write_at 传调用方时间戳；2) 长期路使用 embedder.model_id() 而非硬编码 deterministic-v1，保证查询与写入模型一致并兼容 5.1.6；3) 非法召回参数只能复用 MemoryError::Backend（core 错误枚举仅此变体）；4) `---` 按条目间 3 字节计（n-1 个），5.1.4 Guide 会对最终消息再次做 max_inject_bytes；5) 增加 embedder 输出维度校验，维度不符按该路降级；6) embedder 返回向量前先提交条目，向量写入失败不回滚。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（260 passed, 0 failed, 2 ignored；jessica-memory 6 event_store + 15 memory_log + 8 embedding + 16 memory_store = 45）。jessica-core 未改。
Task 5.1.4 ☑ 约 10 分钟 jessica-harness 新增 guide 模块与 workspace 既有依赖 thiserror（仅用于结构化 GuideError，无新第三方 crate）。定义 `Guide: Send + Sync` 与 `async fn render(&self, session: &SessionId, query: &str) -> Result<Option<Message>, GuideError>`，只依赖 jessica-core 的 MemoryStore/Message/SessionId，不依赖 jessica-memory；GuideError 用 thiserror 的 Render { message }。实现 MemoryGuide { store: Arc<dyn MemoryStore>, limit, max_inject_bytes }（Clone）与构造函数；render 调用 store.recall(session, query, limit)，空结果返回 Ok(None)，非空时按 store 返回顺序渲染一条 Message::system：首行为固定标题 `[记忆参考 — 以下内容来自历史会话，仅供参考，不代表当前事实]`，首条记忆前换行，条目之间用单独一行 `---`（`\n---\n`）分隔。完整消息（标题+条目+分隔符）构造后统一按 UTF-8 char boundary 截断到 max_inject_bytes，截断为空时返回 Ok(None)；不重排、不写记忆、不调用 LLM、不读时钟。recall 出错时 tracing::warn 后返回 Ok(None)，绝不传播为 turn 失败。6 个行为测试用记录调用的 MemoryStore mock 覆盖：空结果→None 且 recall 只调 1 次、write 0 次、session/query/limit 正确转发；三条乱序条目→恰好一条 System 消息且内容严格按返回顺序含标题/条目/---；max_inject_bytes 恰等于完整长度时不截断；比完整长度少 2 字节时回退到 UTF-8 边界、保留前缀、末尾多字节中文整字丢弃且不超限；零预算→None；store 返回 MemoryError 时 Ok(None) 且 write 0 次、不 panic。偏离/边界：截断对完整消息执行，若 max_inject_bytes 小于标题长度则保留标题的合法 UTF-8 前缀；预算为 0 时不产生空 system message，而是返回 None。未接入 AgentLoop、未改 Guard/Policy/Sensor、未实现 CLI。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（266 passed, 0 failed, 2 ignored；jessica-harness 10 governance + 6 guide + 5 harness_chain + 9 sensors_acceptance = 30）。jessica-core 未改。
Task 5.1.5 ☑ 约 8 分钟 jessica-cli 新增 `[memory]` 配置（enabled/path/recall_limit/max_inject_bytes/embedder/write_policy，默认 false、`.jessica/memory.db`、5、4096、deterministic、completed），相对路径按 CLI root 解析，空 path 与未知字段被拒绝，非法枚举值报结构化配置错误；enabled=false 时完全不创建/打开数据库。新增 memory.rs 装配模块：启用时在 spawn_blocking 中创建父目录、按 canonical workspace 打开 SqliteMemoryStore，按 embedder 选择 DeterministicEmbedder 或 none，构造 MemoryGuide；打开失败 fail-open 并 warn，非法配置仍在加载期报错。fresh turn 顺序固定为 system → Guide memory system message（若有）→ user，disabled/None/Guide 失败退化为原顺序；先分配 session id 再传给 AttemptStart::Fresh，使 recall 与实际 attempt 的 session 一致，acceptance 重试使用新 session，最终 Completed 写入使用最后一个 attempt 的 session。resume 从 checkpoint 提取首个 user message，绝不调用 recall。写入策略 completed：仅最终 Completed 后写一条 kind=turn；内容 = 用户输入按 UTF-8 char boundary 截断 2KB + `\n---\n` + 最终回答截断 4KB；metadata 为 kind/truncated/tool_call_count/turn_index；不写工具结果；唯一约束去重；explicit 不自动写入；写入或召回失败只 warn，不改变 SessionEndReason/退出码。replay 分支在 MemoryAssembly::open 之前返回，绝不打开、创建或读取记忆库。测试新增 4 个配置测试、8 个 memory 行为测试（disabled 不建库、相对路径与父目录、打开失败 fail-open、UTF-8 截断与 metadata、边界不截断、去重、explicit 无写入、无 embedder 仍可短期召回）与 2 个 app 注入顺序测试；新增 tests/memory_e2e.rs 5 个真实 CLI E2E（同 workspace 两次运行写入 ORANGE-7 并在第二次请求中位于 system 之后/user 之前且 exit 0、resume 不注入但仍写入 Completed turn、replay 不创建 DB、disabled 无注入、打开失败 warn 且 exit 0）。偏离：启用时自动创建 memory.path 的父目录；turn 内容使用 `\n---\n` 分隔；turn_index 为进程内 AtomicU64（跨进程从 0 重置）；resume 不召回但完成后的 turn 仍按 completed 策略写入；warn 走 CLI 既有 eprintln 约定。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings 通过；cargo test -p jessica-cli 55 passed（47 unit + 5 memory_e2e + 1 replay_e2e + 1 resume_e2e + 1 wasm_tool_e2e）；cargo test --workspace 285 passed, 0 failed, 2 ignored。jessica-core 未改。
Task 5.2.1 ☑ 约 20 分钟 jessica-memory 新增 skill_log 模块与独立 skills.db（user_version=1、busy_timeout=5000ms）及 fts_query/workspace 共享模块。skills 表包含来源计数、内容字段、tags_json、content_hash、draft/active 状态、created_at/deleted_at/activated_at；两个部分唯一索引分别锁定 live 的 source tuple 与 content_hash，`INSERT OR IGNORE` 命中返回 `Duplicate(id)` 且保留首条来源/状态，重复插入不会自动激活已有 draft。content_hash 严格为 sha256(name || '\0' || description || '\0' || instructions || '\0' || canonical_tags_json)，tags 先 trim、排序、去重；单测用独立 SHA-256 计算逐字节锁定公式。BEFORE UPDATE 仅允许单向软删除或 draft→active（其余列不变），所有 DELETE 及其他 UPDATE 由触发器 RAISE(ABORT)。FTS5 trigram 外部内容表 `skills_fts(name, description, instructions, tags_json)` 与同步触发器；软删除会从 FTS 移除。同步 `SkillLog` 提供 append/get/list_recent/list_by_status/search_keyword/soft_delete/activate/find_by_source；异步 `SqliteSkillStore` 固定 workspace scope，所有 async 方法的 rusqlite 调用均在 spawn_blocking 内，并用一个阻塞写锁测试证明等待 SQLite 时不阻塞 Tokio worker。`search_keyword` 只返回 active live 行，返回原始 bm25（越小越相关）；`min_score=None/0.0` 禁用，正值按 `-bm25 >= min_score` 过滤，短 token LIKE 回退使用哨兵 0.0，因此正阈值会过滤短查询命中；上述语义已写入模块文档与测试。重复 activate 幂等且保留首次 activated_at；软删除后再 activate/soft_delete 返回 AlreadyDeleted。字段上限按 UTF-8 字节校验（name 128/description 512/instructions 8192、tags≤16 且每个≤64），超限返回结构化错误不截断。新增 14 个行为测试覆盖 user_version/重开/未来版本、source 与 content 去重、软删除后重插、tags 顺序与跨 workspace、字节边界、特殊字符/中文 trigram/2 字 LIKE/英文前缀、draft 可见性/activate/重复 activate/软删除 FTS、物理 UPDATE/DELETE 与非法状态转换、find_by_source/limit/排序、损坏 tags_json/未知 status、异步 workspace 隔离与 spawn_blocking。偏离/说明：FTS 外部内容列名为 `tags_json` 以与内容表列名一致（语义仍为 tags）；`list_recent` 同时返回 draft 与 active，`list_by_status` 显式筛选，检索只返回 active；`SkillLog::get(id)` 是底层按 id 读取，跨 workspace 隔离由 `SqliteSkillStore::get/soft_delete/activate` 负责；`SqliteSkillStore::open` 构造期同步建库，已文档要求启动期或 blocking 上下文调用。未新增第三方 crate，未改 jessica-core，未接 Harness/Guide/Worker/CLI，未调用 LLM。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（299 passed, 0 failed, 2 ignored；jessica-memory 新增 14 个 skill_log 测试）。
Task 5.2.2a ☑ 约 30 分钟 jessica-harness 新增 skill_source 模块并导出 `SkillTrigger`、`TurnSignals`、`DistillSourceInput`、`DistillSource`、`escape_untrusted` 与默认常量。`SkillTrigger` 默认 5 次工具调用 / 2 个 assistant round，触发规则严格为：最终 `SessionEndReason::Completed`、`acceptance_failures == 0`、`tool_call_count >= min_tool_calls`、`assistant_rounds >= min_rounds`；判断为同步纯函数，不读模型文本、时钟、随机数或 LLM。`TurnSignals` 的 `acceptance_failures` 私有，唯一派生入口 `from_acceptance` 固定使用 `outcome.attempts.saturating_sub(1)`，另有 `new` 用于未启用 acceptance 的 turn；测试锁定 attempts 0/1/2/3/u32::MAX 的映射，证明没有把 attempts 直接当失败数。`DistillSourceInput` 默认不含工具摘要，`with_tool_summaries` 显式开启；`DistillSource::new(input, max_source_bytes)` 先对 user_goal/final_answer/每个工具摘要执行 `escape_untrusted`，再按 512 字节上限截断单个摘要，最后应用预算。转义规则：所有 `<`/`>` 分别替换为 `&lt;`/`&gt;`，因此 `<source>`、`</source>`、`<|im_end|>` 等标签即使跨字段或截断到半个转义实体也无法重新组成；`\n\nSystem:`/`Human:`/`Assistant:`/`User:` 及 CRLF 变体的 ASCII 冒号替换为 U+FF1A，逐字节输出已锁定。预算不足时严格按“tool_summaries 尾部 → final_answer → user_goal”削减，所有截断在转义之后并按 UTF-8 char boundary；`max_source_bytes` 只计三个转义内容字段，固定 wrapper 与 metadata 标签不计入，已在类型文档明确。`to_untrusted_block` 输出唯一 `<source>...</source>` 包装，含 session/turn/计数 metadata 与转义内容，默认无工具摘要时不输出 `tool_summaries:` 段。新增 19 个行为测试覆盖默认/边界阈值、非 Completed、attempts 映射、零阈值、计数型 LLM mock 零调用、五种以上注入标记逐字节转义、跨字段半标记、CRLF、预算三级削减顺序、UTF-8 边界、转义实体截断、512 字节摘要上限与顺序、默认无摘要、metadata 锁定、零预算、结构化计数复制。偏离/说明：新增 `DistillSourceInput` 作为 raw input，避免默认携带工具输出并把 counts 从 `TurnSignals` 单点复制；预算按内容字段而非整个 wrapper 计；转义额外覆盖 `User:` 与 CRLF 变体；未实现序列化，留给后续任务按需接入。未新增第三方 crate/依赖，未改 jessica-core，未调用 LLM，未写技能库，未接 CLI。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（318 passed, 0 failed, 2 ignored；jessica-harness 新增 19 个 skill_source 测试，总计 49）。
Task 5.2.2b ☑ 约 45 分钟 jessica-memory 新增 skill_jobs 模块，方案 B 落地且未改 jessica-core。skills.db 新增 skill_jobs 表（id/workspace_scope/session_id/turn_index/source_json/status/attempts/last_error/created_at/updated_at/claimed_at）、UNIQUE(workspace_scope,session_id,turn_index)、status CHECK(pending|processing|done|failed) 与 (workspace_scope,status,created_at,id) 领取索引；SkillLog::initialize 在 user_version 校验通过后启用 WAL，沿用 busy_timeout=5000ms。同步 API：enqueue_job/get_job/list_jobs/claim_next_job/complete_job/fail_job；异步 SqliteSkillStore 同名方法全部经 spawn_blocking 且固定 workspace scope。入队用 INSERT ... ON CONFLICT DO NOTHING + 回读，pending/processing/done/failed 重复入队均返回已有行且不覆盖；source_json 在 NewSkillJob 与 decode 两侧校验为 5.2.2a DistillSource 的 8 个字段、类型正确、session_id/turn_index 与队列键一致、≤64 KiB。claim 用 BEGIN IMMEDIATE + UPDATE ... RETURNING，按 created_at,id 升序领取 pending 或 claimed_at <= now-stale_after 的 processing，设置 claimed_at/updated_at=now 且 attempts+1，出错 ROLLBACK；complete/fail 仅允许 processing→done/failed，否则 InvalidTransition 且不改行；fail 先对 sk-/Bearer/key=value 等凭据形式脱敏，再按 512 字节 UTF-8 边界截断。模块文档明确 Plan B 双真相源边界、replay 不打开 skills.db、resume 依赖 skills.db 外部可变状态、不得声称事件流可重建 job，并写入 TODO(reconciliation) 的迁移触发条件与 6 步迁移路径及已知限制。15 个行为测试覆盖重复/不同 turn 入队、done/failed 不覆盖、created_at/id 领取顺序、双连接并发不重复领取、stale 阈值含边界恢复、成功/失败转换零 EventStore 写入、损坏 source_json 报 Corrupt 不 panic、非法/超长 source 拒绝、脱敏与 UTF-8 截断、真实持锁验证 busy_timeout 与 WAL、重开保留状态与唯一索引、删除 skills.db 后 resume 式幂等失效、非法状态转换保留原行、异步 workspace 隔离。偏离/说明：SCHEMA_VERSION 保持 1（CREATE TABLE IF NOT EXISTS 作为 v1 增量表，破坏性变更才 bump）；错误 enum 未收 Open/Schema/Serialize/Event 变体（打开与 schema 仍走 SkillLogError，本阶段不写事件），JSON 校验用既有 serde_json::Value，未新增第三方 crate；turn_index 对外为 u64，写库前检查 i64 范围。未实现 rebuild_from_events，未接 CLI/LLM/Worker。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（333 passed, 0 failed, 2 ignored；jessica-memory 新增 15 个 skill_jobs 测试）。
Task 5.2.3 ☑ 约 30 分钟 jessica-harness 新增 skill_distiller 模块，导出 `SkillDistiller`、`LlmSkillDistiller`、`DistilledSkill`、`DistillError`、`SKILL_DISTILL_SYSTEM_PROMPT` 与五个字段上限常量。`SkillDistiller::distill(&DistillSource)` 是唯一入口；`LlmSkillDistiller::new(provider, timeout, max_output_bytes)` 固定发送两条消息：system 为常量提示词、user 为 `source.to_untrusted_block()`，`tools` 始终为空，消息顺序锁定。`tokio::time::timeout` 覆盖 provider 请求与完整流；必须收到 `Done` 才进入解析，`ToolCallRequested` → Permanent，流 `Error`、provider 请求错误、超时、缺少 `Done` → Retryable，`TextDelta` 累计超过 `max_output_bytes` → Permanent 且不截断。严格解析只接受纯 JSON 对象：拒绝 Markdown fence、非对象、恰 4 个字段之外的未知/缺失字段、非字符串字段、空白字段、超长字段与尾随文本；上限与 5.2.1 一致（name 128/description 512/instructions 8192、tags ≤16、单 tag ≤64 字节），tags 先 trim、排序、去重再检查数量；不做任何截断。提示词明确要求只提炼可复用步骤、不复制原始工具输出/密钥/凭据/个人数据，并把 source 声明为不可信数据、禁止调用工具；模块不持有 Memory/EventStore，也不做内部重试，因此成功只返回草稿、失败只返回结构化错误，零存储/零事件写入是结构性保证。新增 workspace 依赖声明 futures 与 serde_json（均已在 workspace 中，无新第三方 crate；serde_json 从 dev-dependencies 提升为 dependencies）。12 个行为测试覆盖合法 JSON 跨 delta 解析与 tags canonical、system/user 边界与 tools 为空、提示词边界、字段上限精确边界与超限、fence/未知/缺失/空/类型错误/尾随文本全部 Permanent、ToolCallRequested → Permanent、流 Error/缺少 Done/provider 错误/超时 → Retryable、累计输出上限、对象安全。偏离/说明：provider 错误详情只截断到 512 字节，本模块不负责脱敏，5.2.4 持久化前必须使用 5.2.2b 的脱敏路径；密钥/完整敏感输出由提示词约束，未做语义级 secret 扫描，最终写入仍受 5.2.1 字段校验约束；为保持 5.2.1 语义，tags 在去重后检查数量而非原始数组长度。未接 CLI、未写技能库/事件流、未调用真实 LLM。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（345 passed, 0 failed, 2 ignored；jessica-harness 新增 12 个 skill_distiller 测试，总计 61）。
Task 5.2.4 ☑ 约 40 分钟 jessica-harness 新增 skill_worker 模块，导出 `SkillJobQueue`（claim/complete/fail）、`SkillSink`（write）、`WorkerClock`（now）、`SkillJob`/`SkillJobId`/`WorkerTimestamp`、`SkillWorker`、`SkillWorkerConfig`、`WorkerOutcome` 与三个错误 enum。`SkillWorker::new` 校验 `max_attempts >= 1`（默认 2，含首次尝试）与 `idle_interval > 0`（默认 100ms，`drain_timeout` 默认 30s），否则返回 `InvalidConfig`；`run_once` 是确定性入口，执行 claim → distill → sink.write → complete。一次 attempt = 一次 distill + 一次 write；`DistillError::Retryable` 与 `SkillSinkError::Retryable` 都重启整个 attempt（包括重新 distill），直到用完 `max_attempts`；`Permanent` 立即 failed；终态只调用一次 `queue.fail`，错误按 5.2.2b 同一组模式（sk-/Bearer/authorization/api_key/access_token/secret/password/token 等 key=value）脱敏后按 UTF-8 边界截断到 512 字节。只有 sink 写入成功后才调用 `complete`；`complete` 自身失败以 `SkillWorkerError::Queue` 返回，不静默改成 fail，sink 已落盘的内容由 stale claim 重投 + sink 幂等吸收。`run(shutdown)` 在每次新 claim 前检查取消；取消到达时已在途的 claim 允许返回并作为 drain 目标，最多等待 `drain_timeout`，超时返回结构化 `DrainTimeout { millis }` 且不 panic、不 complete/fail，job 留给 queue 的 stale-claim 恢复；空队列按 `idle_interval` 轮询。模块文档明确单 worker 假设：不协调多个消费者，重复投递由 sink 幂等兜底。时间戳只来自注入的 `WorkerClock`，仅在终态转换读取；worker 不持有 EventStore、不写事件流，harness 也不依赖 jessica-memory。Cargo.toml 仅为 CancellationToken 增加已在 workspace 中获批的 tokio-util（Task 1.2 引入，无新第三方 crate）。18 个行为测试覆盖：成功路径的 claim→distill→write→complete 顺序与 clock 只读一次；空队列 Idle 零副作用；Retryable 首次失败后成功且 retry 重新 distill；默认 max_attempts 耗尽后 failed 且记录最后一次错误；Permanent distill 不重试；Retryable sink 重启 attempt；Permanent sink 不重试；sink 重试耗尽记录最后错误；同一 job 重复投递经 provenance 幂等只存一个技能；不同来源相同内容经 content hash 幂等只存一个；complete 失败向上返回且不写 fail；预取消零 claim；空闲 shutdown 停止循环；drain 在超时内完成且取消后不再 claim；drain 超时返回 `DrainTimeout` 且零 complete/fail/sink；非法配置在 claim 前拒绝；失败错误脱敏且 ≤512 字节；结构性测试锁定 worker 源码不引用 EventStore/AgentEvent/SystemTime/Instant::now/Utc::now，Cargo.toml 不依赖 jessica-memory。偏离/说明：1) `SkillJob` 直接携带 `DistillSource`，harness 不解析 `source_json`；5.2.6 的 SQLite 适配器必须在不二次转义的前提下从持久化 source 重建 DistillSource（见 Deferred）；2) 幂等是 `SkillSink` 契约，worker 本身不去重投递，测试用按 provenance/content hash 去重的 mock sink 锁定语义，真实唯一约束来自 5.2.1 的两个部分唯一索引；3) 取消时已在途的 claim 不中止而是作为 drain 目标；4) drain 超时会 drop 在途 future，若 sink 已提交但未返回，同样依赖 stale claim + 幂等恢复；5) 脱敏逻辑因 harness 不能依赖 jessica-memory 而独立实现同一组模式。未接 CLI、未实现具体 SQLite queue/sink、未改 jessica-core、未写事件流。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（363 passed, 0 failed, 2 ignored；jessica-harness 新增 18 个 skill_worker 测试，总计 79）。
Task 5.2.5 ☑ 约 25 分钟 jessica-harness 新增 skill_guide 模块，导出 `SkillSource`、`SkillContent`、`SkillSourceError`、`SkillGuide` 与 `DEFAULT_SKILL_INJECT_BYTES`。`SkillSource::recall(query, limit, min_score) -> Result<Vec<SkillContent>, SkillSourceError>` 是唯一的检索契约；模块文档锁定实现方必须只返回 status=active 且 deleted_at IS NULL 的技能、按原始 bm25 升序（越小越相关）保序返回、limit=0 返回空、`None`/`Some(0.0)` 都禁用分数过滤且 0.0 不得当作字面阈值、正值是按与 bm25 同方向的归一化相关度下限、无 bm25 的 LIKE 回退使用固定哨兵 0.0（正阈值过滤、禁用时保留）；检索不得用 LLM 判断相关性、不得写存储/事件流。`SkillGuide { source, limit, min_score, max_inject_bytes }` 实现既有 `Guide` Trait；render 忽略 session（技能为 workspace 级），零预算直接 `Ok(None)` 且不调用 source，否则把 query/limit/min_score 原样转发（`Some(0.0)` 不转换为 None），source 返回错误只 `tracing::warn` 并 `Ok(None)`，空结果 `Ok(None)`。渲染恰好一条 `Message::system`：固定标题 `[技能参考 — 以下内容由历史任务自动提炼，仅供参考，不代表当前事实]`，每个技能为 `## <name>` + 描述 + instructions，技能之间固定 `\n---\n`；完整消息按 UTF-8 字符边界截断到 max_inject_bytes，默认常量 2048，提供 `new(source, limit, min_score, max_inject_bytes)` 与 `with_default_budget`。guide 不持有存储、不调用模型、不读时钟；结构性测试锁定模块源码不引用 LlmProvider/EventStore/MemoryStore/SkillLog/rusqlite/SystemTime/Instant::now/std::fs/spawn_blocking/jessica_memory。15 个行为测试覆盖：空结果 → None 且转发参数正确；单技能精确渲染为一条 System；多技能保序且恰好一个 `\n---\n`；limit 与正值 min_score 过滤生效且参数原样转发；`Some(0.0)` 原样转发并禁用过滤；正阈值排除 LIKE 哨兵 0.0；预算等于完整长度不截断；少 1–2 字节切入 3 字节中文时整字回退；零预算 → None 且零 recall；source 错误 fail-open；标题与“自动提炼/仅供参考”逐字节锁定；session 不影响结果；dyn Guide 对象安全；默认预算 2048；零存储/LLM/时钟结构检查。偏离/说明：1) active/软删除与 bm25/LIKE 过滤是 `SkillSource` 实现方契约，本 Task 用遵守契约的 mock 验证 guide 的转发、保序与渲染，真实 SQLite 过滤已在 5.2.1 测试，5.2.6 负责装配适配器；2) 零预算在 recall 前短路（文档明确）；3) 正预算小于标题长度时保留标题的合法 UTF-8 前缀（与 MemoryGuide 一致），只有零预算返回 None；4) 标题与分隔符为私有常量，通过渲染输出逐字节锁定。未新增依赖、未改 jessica-core、未接 CLI、未调用 LLM/数据库。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test --workspace 全绿（378 passed, 0 failed, 2 ignored；jessica-harness 新增 15 个 skill_guide 测试，总计 94）。
Task 5.2.6 ☑ 约 150 分钟 jessica-cli 新增 `[skills]` 配置（13 项文档默认值；相对路径按 CLI root 解析；`enabled=false` 不创建/打开 skills.db；非法值加载期返回结构化错误）、`skills.rs` 装配模块与 `jessica skills` 子命令。装配位于 replay 早退之后：SqliteSkillStore + StoreSkillSource/StoreSkillQueue/StoreSkillSink 适配器 + LlmSkillDistiller + SkillWorker + SkillGuide；打开失败只 `tracing::warn` 并 fail-open，不改变退出码。fresh turn 固定 `system_prompt → memory guide（若有）→ skill guide（若有）→ user`，只注入 active 且未软删除技能，draft 永不注入；resume 复用 checkpoint 不重新注入。最终 Completed 后从最终 attempt 的 messages 计算 tool_call_count/assistant_rounds/acceptance_failures（`attempts.saturating_sub(1)`），`turn_index` 由会话 user message 数稳定推导而非进程内计数器；命中阈值按 `(workspace_scope, session_id, turn_index)` 幂等入队并 notify worker，不 await 提炼、不写 AgentEvent，job 状态仅在 skills.db。`auto_activate` 仅影响新写入技能的初始状态。shutdown 先取消再 drain 队列至 Idle，最多 `drain_timeout_ms`，超时只 warn。replay 在打开 skills.db 与 provider 之前返回；resume 可入队但不注入，幂等依赖 skills.db 唯一索引。子命令 `list [--status draft|active] [--limit N]`、`show/activate/delete <id>`（软删除）走独立路径，不初始化 provider、不加载 wasm、不触网、不打开事件库；失败结构化错误 + 非零退出；不打印 source JSON 或密钥。为落实 5.2.4 Deferred，jessica-harness 的 `DistillSource` 增加 `to_persisted_json`/`from_persisted_json`（原样复制已转义/已预算内容，不二次转义），skills.rs 经此重建 job source。11 个脚本化 SSE E2E 覆盖：低于阈值零 job；达阈值写 draft、下一 fresh turn 不注入、事件流无 skill 事件；人工 activate 后 memory→skill 顺序、固定标题与“仅供参考”；非法 JSON → job failed、无技能、turn Completed/exit 0；resume 不注入且同 turn 重复入队幂等（source/created_at 不变）；连续 replay 不触库不调 provider；打开失败 fail-open；drain 超时 job 可恢复且 exit 0；重复 sink 写入不重复技能；auto_activate=true 无需人工命令即可注入。另有配置默认值/非法值、参数解析、注入顺序、最终 attempt 计数、稳定 turn_index、软删除、禁用子命令等单元测试。偏离/说明：1) 5.2.4 要求适配器在不二次转义前提下重建 DistillSource，为此在 harness 增加持久化 JSON 方法（未改 core）；2) 工具结果状态沿用事件泵 `content.starts_with("tool error: ")` 约定，因 `Message` 无 is_error 字段；3) FTS 长 token 全部 AND，E2E 用短 token 触发召回；4) worker 取消时先 drain 至 Idle，避免 shutdown 丢掉已入队 job；5) `skills` 子命令模式同时跳过事件记录器；6) 计数、入队、drain 全程 fail-open，不改变 SessionEndReason 与退出码。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-cli、cargo test --workspace 全绿（418 passed, 0 failed, 2 ignored；jessica-cli 新增 11 个 E2E 及配置/装配/解析测试）。未提交、未改 jessica-core、未新增第三方 crate。
Task 5.3.1 ☑ 约 2 分钟 jessica-runtime 新增 delegation 模块并导出 DELEGATE_TASK_TOOL_NAME/CHILD_TOOL_TIMEOUT/MIN_RESULT_BYTES/ParentCapabilities/NarrowedTools/narrow_tools/DelegationLimits/DelegationError。ParentCapabilities 用两个 BTreeSet 去重保存父 allowlist 与 native 工具；narrow_tools 先全量拒绝重复名称，再拒绝未授权名称，结果取 requested ∩ native 并无条件移除 delegate_task，已授权但非 native 的 WASM 名称静默排除；NarrowedTools 只读、按名称排序。DelegationLimits 默认 4/8000ms/8192/32768/20；validate 拒绝零 max_rounds/max_child_tool_calls/max_task_bytes、max_result_bytes<128、timeout<=5000ms。12 个行为测试覆盖子集保留与排序、重复/未授权错误、重复检测优先于权限、未授权非 native 不被静默过滤、delegate_task/WASM 过滤、仅过滤项与空请求、父集合去重、默认值与全部非法边界、最小合法边界。偏离：显式锁定“重复检测先于权限检查”的错误优先级（计划未规定两种错误同时出现时的顺序）；错误文案中的 128/5000ms 为固定值。未新增依赖、未改 jessica-core、未执行子 Agent、未接 CLI。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-runtime、cargo test --workspace 全绿（430 passed, 0 failed, 2 ignored；jessica-runtime 新增 12 个 delegation 测试）。
Task 5.3.2 ☑ 约 3 分钟 jessica-runtime 的 delegation 模块新增 `DelegateTaskArgs`/`parse_delegate_task_args`、固定 `delegate_task_schema`、`child_session_hash`、内部 `ChildOutcome` 与 `render_child_outcome`；将 workspace 已批准的 serde_json 从 dev-dependencies 提升为 dependencies（无新第三方 crate）。参数解析拒绝非对象、未知字段、缺失字段、类型错误、trim 后空 task 与超长 task；tools 保留顺序与重复，重复由 narrow_tools 拒绝。成功/错误/截断 JSON 严格为 5/6/3 个字段；成功超限时按 UTF-8 边界取最长 answer 前缀并重新序列化为 truncated，`original_bytes` 为截断前完整成功 JSON 字节数；错误先脱敏（Bearer、sk-、api_key/token/authorization/access_token/password/secret 等键值及 JSON 形式）再按 UTF-8 边界缩短，始终保持 status=error；成功 answer 不扫描。hash 为 FNV-1a 32 位 8 位小写 hex，输入 trim task + NUL + 排序工具名；错误路径有 best-effort hash。16 个新行为测试覆盖 schema 契约、参数边界、三种 JSON 精确字段、精确字节边界、中文/emoji 截断、脱敏与超小预算、is_error、hash 顺序无关/名称分隔、best-effort。偏离：1) 每个工具名后追加 NUL 作为分隔符（计划只写 task + `\0` + 工具名），避免 `["ab","c"]` 与 `["a","bc"]` 的拼接碰撞；2) task 统一 trim 并按 trim 后字节计数；3) `ChildOutcome`/render/best-effort hash 暂以 `#[allow(dead_code)]` 标注，5.3.3 接入 executor 时移除。未接 CLI、未执行子 Agent、未改 jessica-core。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-runtime、cargo test --workspace 全绿（446 passed, 0 failed, 2 ignored；jessica-runtime 单测 28 个，全部在 delegation 模块 = 5.3.1 的 12 + 本 Task 16，另有 agent_loop 11 / recovery 6 集成测试）。
Task 5.3.3a ☑ 约 5 分钟 jessica-runtime 的 agent.rs 新增 `TranscriptRun` 与 `AgentLoop::run_with_transcript`；run/resume 签名与事件序列保持不变并复用同一循环丢弃 transcript。TranscriptRun 用结构化计数：rounds=完成模型回合数，tool_calls=模型请求数（含未知工具与触发上限的第 N+1 个），tool_errors=is_error 结果数，first_tool_error=首个失败工具名+最多 512 字节 UTF-8 摘要，error=provider/stream/loop 结构化终止错误，tool_call_limit_hit 记录上限触发；第 N+1 个工具不执行，子通道发 Error+SessionEnded。delegation.rs 新增 `ChildToolFactory`、`AgentChildExecutor`、`DelegateTaskTool`、`CHILD_AGENT_SYSTEM_PROMPT` 与 `DelegationError::ToolFactory`；每次 execute 重新调用 factory、构造 registry/transcript/取消令牌/容量 256 私有 broadcast，factory 名称集合与 NarrowedTools 完全一致（多/少/重复/构造失败均在子模型调用前返回 error JSON）；每工具以固定 5000ms 包装，子循环 budget.max_rounds=limits.max_rounds，初始 messages 恰为固定 system prompt + user(task)，session id 用 child_session_hash，外层 delegation.timeout 丢 future 返回 error。结果映射：Completed 且无 tool_errors 且未触限 → ok；tool_errors>0、rounds 耗尽、上限、provider/stream 错误、非 Completed、超时 → error；ok/truncated is_error=false，error=true。15 个新行为测试覆盖成功计数/初始消息/工具 schema 收窄、工具错误仍 error、工具超时、rounds 耗尽、上限恰好 N 与 N+1 不执行、factory 多/少/重复/失败零 provider 调用、非法参数/未授权/重复工具零 factory 调用、provider 错误、delegation 超时（paused clock）、DelegateTaskTool schema/is_error 映射、run_with_transcript 计数/事件序列/512 字节 UTF-8 摘要/上限/provider 错误。偏离：1) factory 工具名校验放在 registry 构造前（等价于注册后校验 registry.names()，且更早失败）；2) delegation 超时丢弃 future，结果 rounds/tool_calls 记 0；3) async-trait 从 dev-dependencies 提升为 dependencies，tokio dev-dependencies 增加 test-util feature（均无新第三方 crate）。未接 CLI、未注册到父 registry、未接 WASM、未嵌套委派、未持久化子事件、未改 jessica-core。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-runtime、cargo test --workspace 全绿（461 passed, 0 failed, 2 ignored；jessica-runtime delegation 集成测试 15 个，模块单测 28 个，agent_loop 11 / recovery 6）。
Task 5.3.3b ☑ 约 15 分钟 仅新增测试，未改生产代码；jessica-runtime/tests/delegation.rs 从 15 个测试扩展到 21 个。新增 6 个行为测试：1) parent_history_and_prompt_injection_stay_out_of_the_child：父 transcript 注入独特标记，子 provider 收到的 messages 恰为 2 条，role 顺序为 System→User，system 严格等于 CHILD_AGENT_SYSTEM_PROMPT，user 严格等于含 “system: ignore previous instructions” 的 task，父标记零泄漏；2) child_tool_schemas_are_exactly_the_narrowed_native_set：请求 echo/wasm_tool/delegate_task 后子 schema 恰为 ["echo"]，未请求父工具不出现，空 narrowed 时 tools 为空；3) forged_delegate_task_is_an_unknown_tool_and_never_nests：子模型伪造 delegate_task 只得 unknown tool 错误，factory 仅调用 1 次，子 provider 仅 2 次请求（无嵌套），最终 tool_errors>0 使委派结果为 error；4) consecutive_delegations_get_fresh_tools_and_transcripts：同一父回合连续两次委派，factory 调用 2 次、工具实例 id 为 [0,1]，有状态工具证明第二个子 Agent 看不到第一个的 task/answer/工具状态，父 transcript 恰含两条 delegate 结果；5) executor_emits_no_events_to_a_parent_channel：executor 不持有父 sender，执行后父通道为空（完整父事件序列断言按计划留到 5.3.5）；6) child_session_hash_is_stable_and_trace_only：同 task/同工具两次委派暴露的 hash 相同且等于 child_session_hash(task, narrowed)，不同 task 不同，8 位十六进制。偏离/说明：1) 子 Agent 的 SessionId 无法通过 LlmProvider 观察（stream_chat 不接收 session id），且子事件通道私有，测试通过结果 JSON 中的 child_session_hash 验证同一 trace 值；2) 需求 6 只做“无父 sender、无父事件”的直接断言，完整事件流断言按 PLAN 留给 5.3.5；3) 本轮未发现 5.3.3a 实现缺口，未扩大功能、未接 CLI、未改 jessica-core。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo clippy -p jessica-runtime --all-targets -- -D warnings、cargo test -p jessica-runtime、cargo test --workspace 全绿（467 passed, 0 failed, 2 ignored；jessica-runtime：28 模块单测 + agent_loop 11 + delegation 21 + recovery 6）。
Task 5.3.4 ☑ 约 30 分钟 jessica-cli 新增 `[delegation]` 配置（`serde(default, deny_unknown_fields)`；默认 enabled=false、max_rounds=4、timeout_ms=8000、max_task_bytes=8192、max_result_bytes=32768、max_child_tool_calls=20；不提供 child_tool_timeout_ms）。配置校验：基础边界（max_rounds/max_child_tool_calls/max_task_bytes≥1、max_result_bytes≥128）始终执行；仅 enabled=true 时校验 policy.allowed_tools 含 `delegate_task`、`CHILD_TOOL_TIMEOUT(5000ms) < timeout_ms < budget.tool_timeout_ms`、`max_result_bytes <= budget.max_result_bytes`，均返回字段级 ConfigError。新增 jessica-cli/src/delegation.rs：`NativeChildToolFactory` 按 narrowed 名称每次新建 `ReadFileTool`/`WriteFileTool`/`BashTool`（同一 workspace root），其他名称返回结构化 ToolFactory 错误，绝不构造 WASM 或返回 delegate_task；模块文档写明共享文件系统/root、无子 sandbox、仅 provider token 预算共享、子事件不持久化。app.rs 的 `build_turn(allow_delegation)` 先构建 TokenCountingProvider，再把同一 `Arc<dyn LlmProvider>` 传给 `AgentChildExecutor`；ParentCapabilities 的 allowlist 来自 config.policy.allowed_tools，native 固定为 ["bash","read_file","write_file"]；enabled 且允许委派时用 GovernedTool 按父 policy/timeout/max_result_bytes 包装 `DelegateTaskTool` 并注册为 `delegate_task`；子工具不经过 GovernedTool。15 个新单测：9 个配置（默认值、全字段解析、4 个基础边界、allowlist 缺失、3 条嵌套违规与边界合法性、disabled 跳过跨字段、拒绝 child_tool_timeout_ms）、4 个 factory（恰三个 native、每次新实例、未知/WASM/delegate 拒绝、空请求）、2 个 app 行为测试（fresh enabled 父 schema 为 4 个且含 delegate_task，disabled 与 resume 路径为 3 个；父+子共享 provider 使 3 次调用共 30 token 全部计入父治理，子请求恰 2 条 messages、system 为固定常量、tools 仅 read_file，父收到 status=ok）。偏离/说明：1) 为满足“不在 resume 路径构造 executor”，`build_turn(false)` 在 resume 时跳过 factory/executor 与 delegate_task 注册；配置校验仍按 enabled 规则执行；2) 子工具超时不提供配置项，固定使用 runtime 的 CHILD_TOOL_TIMEOUT；3) 未写 E2E 大场景（留给 5.3.5）；4) disabled 时既有 schema 数量与行为不变，现有 CLI E2E 全部通过。未改 jessica-core、未新增第三方 crate、replay 不构造 executor、resume 不重放子 Agent。验收：cargo fmt --check、cargo clippy -p jessica-cli --all-targets -- -D warnings、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-cli（106 passed：87 单测 + 5 memory_e2e + 1 replay_e2e + 1 resume_e2e + 11 skills_e2e + 1 wasm_tool_e2e）、cargo test --workspace（482 passed, 0 failed, 2 ignored）全绿。
Task 5.3.5 ☑ 约 25 分钟 仅新增端到端测试，未改生产代码；新增 `jessica-cli/tests/delegation_e2e.rs`（13 个脚本化 SSE E2E，无真实 API key/网络，覆盖计划 11 个场景）：1) 正常委派：父请求 `delegate_task(task, ["read_file"])`，子请求恰 2 条 messages（system 严格等于固定常量、user 为 task）且 tools 仅 `read_file`，父历史标记零泄漏；子 read_file 后回答；父收到 status=ok/rounds=2/tool_calls=1 与匹配的 child_session_hash；父事件流恰一次 delegate Started/Finished，无 read_file 事件，子 session 无事件/元数据；exit 0。2) 子 rounds 耗尽（max_rounds=2）：子连续两轮请求 read_file，父收到 status=error 且 error 含“2 round limit”，父继续 Completed/exit 0，事件结果 is_error=true。3) max_child_tool_calls=1：子一轮请求两个 write_file，first.txt 已写而 second.txt 不存在（第 N+1 个未执行），父 results 为 status=error 且含“tool call limit of 1”，父事件流无 write_file。4) 子工具失败：read_file 缺失文件后子仍回答，父仍收到 status=error“child tool failure”并 Completed/exit 0；子 provider 挂起 5400ms：delegation.timeout_ms=5001、父 tool_timeout=10000，父收到 status=error“timed out after 5001ms”后正常完成/exit 0（仅保留一个真实超时用例）。5) 子 usage=8+8 超过父 max_tokens=10：父 exit 1，父事件库存在 BudgetExceeded(TokenLimit)，外层 session 的 SessionEnded=BudgetExceeded，子 session 无事件写入。6) max_result_bytes=256：子回答为超长中文/emoji，父收到恰 3 字段的 status=truncated、answer 为 UTF-8 前缀、序列化总字节 ≤256，is_error=false。7) 父 allowlist 缺 bash：子请求 `["bash"]` 在本地被拒，父 status=error“not permitted by the parent allowlist”，子 provider 零请求，父 Completed/exit 0。8) 一轮两个 delegate：四个子请求分别只含自己的 task/工具结果（ALPHA 不含 BETA，BETA 不含 ALPHA），父事件两个 Started/Finished 按 call id 配对，两个子 session 均无持久化；子伪造 delegate_task 只得 unknown tool error，子工具集仅 read_file，无孙 Agent provider 调用，父 status=error 且 Completed。9) disabled：父 tools 恰为 bash/read_file/write_file（无 delegate_task），模型请求只得到 unknown tool，无子请求，exit 0。10) replay/resume：正常委派落库后连续两次 replay 逐字节一致且等于记录的父事件流（无 provider 环境、无 server，子事件不在输出中）；已完成 session 的 resume 在零计划 server 上零请求、输出 already ended、事件不追加；父流停在 delegate ToolCallStarted 无 Finished 时 resume 以 IncompleteRound 拒绝（exit 1，stderr 含“leaves tool call … without a result”），零请求且 seq 不变。11) 两个子 Agent 不共享可变状态与父事件结果按 task 分别对应，已由场景 8 覆盖。偏离/说明：1) 事件泵把失败工具结果重建为 `tool error: <content>`，测试解析委托 JSON 时仅剥除此固定前缀，生产代码未改；2) replay/resume“零调用”通过在无计划脚本 server 上断言零请求来行为化，而非仅依赖未配置 provider；3) 超时用例真实等待约 5.4s 以验证 5001ms 边界；4) 未接 WASM 子工具、未嵌套委派、未改 jessica-core、未新增第三方 crate、未改生产代码。验收：cargo fmt --check、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-runtime（66 passed：28 单测 + 11 agent_loop + 21 delegation + 6 recovery）、cargo test -p jessica-cli（119 passed：87 单测 + 13 delegation_e2e + 5 memory_e2e + 1 replay_e2e + 1 resume_e2e + 11 skills_e2e + 1 wasm_tool_e2e）、cargo test --workspace（495 passed, 0 failed, 2 ignored）全绿。
Task 5.4.1 ☑ 约 70 分钟 jessica-providers 新增 reliability 模块，导出 `ProviderFailureKind`、`ProviderFailure`、`ClassifiedStream`、`ClassifiedProvider`、`ReliabilityConfig`、`ReliabilityConfigError`、`ReliableProvider` 与 `MAX_TIMEOUT_MS`。`ProviderFailureKind` 固定为 Transport/Timeout/RateLimited/Server/Client/InvalidResponse/StreamInterrupted；`ProviderFailure` 含 kind/message/retry_after/is_retryable，408/425 为可重试 Client，429 为 RateLimited 并解析 `Retry-After` 整数秒，5xx 为 Server，其他 4xx 为永久 Client；`is_retryable` 对 Transport/Timeout/RateLimited/Server/StreamInterrupted 为 true，Client/InvalidResponse 为 false。`ClassifiedStream` 为 `BoxStream<'static, Result<LlmEvent, ProviderFailure>>`；两个 Provider 均实现 `ClassifiedProvider`，HTTP 状态、reqwest timeout/transport、SSE/JSON 结构错误与流提前结束均类型化分类；`ReliableProvider` 同时实现 `ClassifiedProvider`（暴露类型化 Timeout）与 `LlmProvider`（映射为 frozen 的 `LlmError` / `LlmEvent::Error`）。既有 `LlmProvider::stream_chat` 行为保持不变：OpenAI 非 2xx 仍返回 `LlmError::Request`，Anthropic 非 2xx 仍返回 `Ok(error_stream)`，legacy 解析器仍保留 OpenAI EOF 无 `[DONE]` 合成 Done、Anthropic 缺 `message_stop` 产生 `LlmEvent::Error`。`ReliabilityConfig` 仅含 `request_timeout_ms`、`stream_idle_timeout_ms`，0 禁用，超过 24h 拒绝；`ReliableProvider` 用 `tokio::time::timeout` 覆盖 setup 与相邻流事件间隔，超时以 `ProviderFailureKind::Timeout` 产生，并在 `LlmProvider` 路径映射为 `LlmError::Request` / `LlmEvent::Error`，本 Task 不做重试。classified 模式更严格：OpenAI EOF 无 `[DONE]`、Anthropic 缺 `message_stop` 均返回 StreamInterrupted；Anthropic error event 按 `error_type` 分类（rate_limit_error → RateLimited；authentication/permission/invalid_request/not_found → Client；其余 → Server）。行为测试覆盖：两个 Provider 逐项覆盖 400/401/403/404/408/409/422/425/429/500/502/503/504 的 kind 与 retryability；`Retry-After` 缺失/整数秒/非法值分别为 None/Duration/None；loopback 连接拒绝为 Transport；paused clock 下请求超时、首事件前与首事件后的流空闲超时；两个 timeout=0 时延迟 setup/stream 不超时；既有 OpenAI/Anthropic HTTP 500 文案与事件序列不回归；`is_retryable` 全表。偏离/说明：1) `tokio` 从 dev-dependencies 提升为 dependencies 以使用 `tokio::time::timeout`，dev 额外启用 `test-util` feature；tokio 已是 workspace 依赖，无新第三方 crate；2) `ProviderFailure` 增加私有 retryable 标记，表达 408/425 可重试但 kind 仍为 Client；3) StreamInterrupted 在分类层标记可重试，5.4.2 必须再按首个 TextDelta 提交点 gate；4) classified 模式比 legacy 严格，legacy 错误文案与事件序列保持不变；5) 未接 CLI、未实现重试/熔断/限流、未改 jessica-core。验收：cargo fmt --check、cargo clippy -p jessica-providers --all-targets -- -D warnings、cargo clippy --workspace --all-targets -- -D warnings、cargo test -p jessica-providers（81 单测 + 7 集成 + 2 ignored live）、cargo test --workspace（526 passed, 0 failed, 2 ignored）全绿。
Task 5.4.2 ☑ 约 100 分钟 jessica-providers 扩展 `ReliabilityConfig`：`max_attempts`（默认 3，含首次）、`initial_backoff_ms`（250）、`max_backoff_ms`（4000），新增 `DEFAULT_MAX_ATTEMPTS` / `DEFAULT_INITIAL_BACKOFF_MS` / `DEFAULT_MAX_BACKOFF_MS` / `RECOMMENDED_MAX_ATTEMPTS` 导出；`validate` 拒绝 `max_attempts < 1`、`initial_backoff_ms < 1`、`max_backoff_ms < initial_backoff_ms`，timeout 仍上限 24h（0 禁用）。`ReliableProvider` 在 `stream_chat_classified` 内实现统一重试状态机，`LlmProvider::stream_chat` 经 frozen 契约映射后复用：setup 失败仅对 `is_retryable()` 重试，退避为 `min(initial * 2^(attempt-1), max)`，无 jitter；`Retry-After > max_backoff` 直接停止，否则等待 `max(backoff, retry_after)`。流状态机在首个 `TextDelta` 前保留 `committed=false`，`ToolCallRequested` 只缓冲，`Done` 时按到达顺序交付缓冲 tool calls 再交付 `Done`；`TextDelta` 立即交付并提交，此后所有 `Err(ProviderFailure)`/EOF/空闲超时都终止为 `LlmEvent::Error` 且不重试；未提交失败丢弃整批缓冲并重试完整请求，最终只交付成功 attempt 的事件。最终 `ProviderFailure` 保留最后 message，尝试数 > 1 时加 `provider request failed after N attempts: ` 前缀；`LlmEvent::Error` 实现错误按终止错误处理不重试；所有 backoff/setup/stream 等待随 future/stream drop 取消，不启动后台任务。模块文档明确重试可能在首个 TextDelta 前造成 provider 重复计费，但客户端不重复文本、不重复执行工具，首个 TextDelta 后不重试。jessica-cli 新增 `[reliability]` 配置（`serde(default, deny_unknown_fields)`；默认 enabled=false、max_attempts=3、initial_backoff_ms=250、max_backoff_ms=4000、request_timeout_ms=30000、stream_idle_timeout_ms=120000），校验失败均带 `reliability.<field>` 字段名；CLI 配置与 provider `ReliabilityConfig` 分离，通过 `to_provider_config()` 转换。`build_provider` 接收 reliability 配置；新增泛型 `assemble_provider<T: LlmProvider + ClassifiedProvider>`，enabled=false 直接返回 base `Arc`（不构造 pass-through wrapper），enabled=true 只构造一次 `ReliableProvider`。同一返回实例继续用于 SkillsAssembly 提炼器、每 turn 的 `TokenCountingProvider` 与子 Agent executor；`build_turn`/child executor 不重建可靠性包装。测试：jessica-providers 98 单测 + 7 HTTP 集成 + 2 ignored live；jessica-cli 94 单测 + 新增 3 个脚本化 SSE E2E（500/500/200 恰好 3 次请求且 1 个 Completed session、仅 1 份最终文本；400/401 各恰好 1 次请求且 SessionEnded Error/exit 1；缺省 `[reliability]` 时 500 仍只请求 1 次）。workspace 553 passed, 0 failed, 2 ignored。偏离/说明：1) 重试状态机放在 `stream_chat_classified` 内，`stream_chat` 复用同一实现，因此 classified 调用方也获得一致的提交点语义；2) `ProviderFailure` 新增私有 `after_attempts` 前缀构造，未改 frozen core；3) 为让 disabled 路径零包装返回且避免 trait upcasting，CLI 使用具体类型泛型装配；4) 新增 `jessica-cli/tests/reliability_e2e.rs`；5) 未实现熔断/限流、未修改 jessica-core、未新增第三方 crate。
Task 5.4.3a ☑ 约 85 分钟 jessica-providers 为 `ReliabilityConfig` 增加 `circuit_failure_threshold`（默认 5）与 `circuit_cooldown_ms`（默认 30000），新增 `DEFAULT_CIRCUIT_FAILURE_THRESHOLD` / `DEFAULT_CIRCUIT_COOLDOWN_MS` 导出；`validate` 拒绝 threshold < 1、cooldown < 1。`ReliableProvider` 持有 `Arc<CircuitBreaker>`，状态由 `std::sync::Mutex` 保护且不跨 await：Closed 统计逻辑调用失败（仅可重试 kind 且重试耗尽后计 1 次，与 attempt 数无关），达到 threshold 转 Open；Open 立即返回 `LlmError::Request`，文案含 `circuit breaker open` 与剩余冷却，底层零调用；冷却到期后首个调用转 HalfOpen 并独占探测，其他并发调用快速失败；探测最终 `Done` → Closed 并清零，探测可重试失败 → Open，不可重试失败 → 中性并释放探测位。`CircuitAdmission` 随 `ClassifiedStream` 生命周期移动：`Done` 显式记成功、终止 `Err` 按 kind 记失败或中性、流/ future 被 drop 时中性释放且不清零；不可重试 4xx 为中性，不打开熔断。模块文档写明熔断状态仅在内存、进程重启重置、不持久化、不进入事件流，Open 是快速失败而非重试。jessica-cli `[reliability]` 增加 `circuit_failure_threshold`、`circuit_cooldown_ms` 及字段级校验，`enabled=false` 时不构造包装或熔断器。测试：jessica-providers 106 单测（新增 8 个：threshold→Open 且零底层调用、3 attempt 逻辑调用只计 1 次、401/400 不计数、成功清零、流终止失败计数、setup/stream drop 中性、HalfOpen 单探测与 drop 释放、探测失败重开与成功关闭）；jessica-cli 94 单测 + reliability E2E 5 个（新增 2 个 REPL E2E：max_attempts=1、threshold=2 连续两个 503 后第三 turn 零 HTTP 且 stderr 含 circuit breaker open；两个 401 后第三 turn 仍请求并 Completed）。workspace 563 passed, 0 failed, 2 ignored。偏离/说明：1) 熔断失败计数只在逻辑调用终态结算，retry 过程中的 attempt 不计数；2) 非探测的陈旧调用在 HalfOpen/Open 期间结算时不会改变状态；3) 未实现限流、未修改 jessica-core、未新增第三方 crate。
Task 5.4.3b ☑ 约 100 分钟 jessica-providers 为 `ReliabilityConfig` 增加 `requests_per_minute`（默认 0 = 不限）、`burst`（默认 1）、`max_concurrent`（默认 0 = 不限），新增 `DEFAULT_REQUESTS_PER_MINUTE` / `DEFAULT_BURST` / `DEFAULT_MAX_CONCURRENT` / `MAX_REQUESTS_PER_MINUTE` / `MAX_BURST` / `MAX_CONCURRENT` 导出；`validate` 拒绝超过上限的 rpm/burst/max_concurrent，并在 rpm>0 且 burst=0 时返回 `ValueTooSmall { field: "burst" }`（rpm=0 时允许 burst=0）。实现 `TokenBucket`：容量 = burst，按 rpm 线性补充，核心 `try_consume(Instant)` 为注入时钟的纯状态转换，返回立即通过或等待时长；内部使用整数微令牌（`MICROTOKENS_PER_TOKEN = 1_000_000`），补充余数跨调用保留、不产生浮点漂移，且不会累积超过容量。`TokenGuard` 在尚未 dispatch 时被 drop 会退还令牌；`RateLimiter` 在 rpm=0 时不构造令牌桶，在 max_concurrent=0 时不构造 Semaphore。`ReliableProvider` 持有单个 `Arc<RateLimiter>`，同一实例被父 turn、子 Agent 与技能提炼器共享，不在每 turn 重建。每个物理 attempt（含重试）固定顺序：先 `wait_for_token`（`tokio::time::sleep`，drop 可取消），再 `acquire_owned` permit，再 commit 令牌并调用底层 provider，绝不持有 permit 等待令牌；permit 保存在 `RetryStreamState` 中随该 attempt 的流生命周期持有，在 `Done`、终止 `Err`、`LlmEvent::Error` 或 stream drop 时释放；重试在 backoff 前释放 permit，下一 attempt 重新走令牌与 permit 获取；熔断 fast-fail 发生在等待令牌之前。模块文档写明 v1 只限制 RPM 与并发、不做 TPM（usage 仅在终止 `Done` 可见，精确 TPM 需单独设计）。jessica-cli `[reliability]` 增加三个字段、默认值与字段级校验（含 rpm>0/burst=0 及三个上限），`to_provider_config()` 完整转换；`enabled=false` 仍直接返回 base provider，不构造限流器。测试：jessica-providers 119 单测（新增 13：burst 内立即通过且超出按 rpm 等待、补充容量封顶、整数余数跨区间无漂移、rpm=0 永不等待、同一实例下父/子/提炼器三个 caller 共享令牌桶、重试每次消耗令牌、重试重新获取 permit 而不绕过并发、max_concurrent=2 时第三个等待并在此前两个释放后进入、成功/setup 失败/流失败/取消 drop 均释放 permit、等 permit 时取消会退还令牌、三个字段边界与 burst=0 规则）；jessica-cli 94 单测 + reliability E2E 7 个（新增 2：REPL 两 turn 在 rpm=60/burst=1 下请求间隔 >= 800ms；rpm=0/burst=0 两 turn 间隔 < 800ms 且均 Completed）。workspace 578 passed, 0 failed, 2 ignored。偏离/说明：1) E2E 时间戳由脚本化 server 在读完请求时记录，按要求只用宽松上下界；2) 令牌在熔断检查之后、permit 之前扣除，若在 dispatch 前取消由 `TokenGuard` 退还，避免取消请求永久占用额度；3) 未实现 TPM、未修改 jessica-core、未新增第三方 crate。
Task 5.4.4 ☑ 约 15 分钟 jessica-cli 新增 REPL 配置热加载。`config.rs` 为 Budget/Policy/Delegation/Reliability/Events/Memory/Skills/Wasm/Sensor/Acceptance 增加 `PartialEq`（不用序列化字符串比较），并新增 `changed_immutable_section`：按 events→memory→skills→wasm→reliability 固定顺序返回首个变化段名。新增 `hot_reload.rs`：`ConfigWatcher` 在 spawn 返回前读取初始 `(mtime, len)` 指纹，后台每 1s 轮询（`MissedTickBehavior::Delay`，测试注入 20ms），指纹不变则不读文件；变化时先 `HarnessConfig::load` 完整解析并校验，再重读指纹，若读取期间文件又变化则丢弃本次样本且不推进已接受指纹（避免非原子写入把截断/空文件解析成默认配置发布）；稳定后无论成功失败都更新指纹。成功且不可热加载段未变 → `sender.send(Arc::new(config))` 并输出 `[config] reloaded`；不可热加载段变化 → 整份拒绝并输出 ``[config] reload rejected: `{section}` is not hot-reloadable``；解析/校验失败 → 输出 `config reload failed: {error}`；元数据不可读 → `config reload failed: cannot read metadata for {path}`；receiver 关闭时任务退出；`Drop` abort JoinHandle。`app.rs` 的 `CliRuntime` 改为持有 `watch::Receiver<Arc<HarnessConfig>>`，删除缓存的 `system_prompt` 字段；新增 `config_snapshot()`，`run_turn`/`resume_turn` 每次仅克隆一次快照并同时传给 `initial_messages` 与 `build_turn`，system prompt 每 turn 由 `system_prompt(root, config)` 计算，工具 schema、policy、guards、delegation factory/executor、sensors、acceptance、AgentLoop budget 全部取同一快照；消息构造仍为 `fresh_conversation` 的 `[system, memory guide, skill guide, user]`，memory/skills 仍每 turn 各召回一次、无额外召回或连接重建。`main.rs` 启动时仍只加载/校验一次配置，之后建立 `watch::channel`；仅 REPL（`Mode::Turn { resume: None, prompt: None }` 且有 config path）经 `watched_config_path` 启动 watcher，one-shot/resume/replay/skills 不启动；退出前 drop watcher 再 shutdown。测试：hot_reload 模块 7 个（可热加载发布、不可热加载拒绝且 watcher 继续工作、非法 TOML 保留旧快照且继续工作、非法 delegation allowlist 整体拒绝、仅可热加载段接受、五段名称与顺序、首个变化段优先）；app 4 个（发布后下 turn 使用、turn 中途 reload 只影响下一 turn、新 max_rounds 生效、新 tool_timeout 使 `sleep 0.2` 在下 turn 以 BudgetExceeded 终止且 provider 恰 2 次调用）；main 1 个模式谓词；新增 `tests/hot_reload_e2e.rs` 3 个脚本化 SSE E2E（两轮之间改配置：第二轮 system prompt 与 tools schema 变化、消息仍 `[system,system,user]` 且每 turn 恰一条 memory guide、stderr 含 `[config] reloaded`；events 不可热加载与非法 TOML 后 REPL 存活并最终成功 reload；one-shot 在响应期间改配置不产生 reload 日志、只有 1 次请求）。偏离/说明：1) 现有架构下 tools schema 不按 `policy.allowed_tools` 过滤（registry 广播全部工具，allowlist 在执行期拒绝），因此 E2E 同时修改 policy 与 delegation 才能同时观察 system prompt 与 schema 变化，未改生产过滤逻辑；2) 增加指纹稳定性复核和测试原子 `rename` 写入，避免非原子写入的瞬时文档被发布；3) 非 REPL 模式用 one-shot E2E + 谓词单测覆盖，未为 resume/replay/skills 各写独立 E2E；4) 日志走 CLI 既有 stderr `eprintln` 约定。未新增第三方 crate、未修改 jessica-core、未重建后台 worker、未在 turn 中途替换快照。验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test -p jessica-cli` 148 passed（106 unit + 13 delegation + 3 hot_reload + 5 memory + 7 reliability + 1 replay + 1 resume + 11 skills + 1 wasm）；`cargo test --workspace` 593 passed, 0 failed, 2 ignored。
Task 5.4.5 ☐ — 未开始

---

Deferred（待办，由用户决定是否单开 Task）

· Anthropic Provider 暂不支持 Prompt caching（cache_control）、thinking block 与多模态 image block；未知 content block 目前按 Other 忽略（thinking/image 内容不会进入 LlmEvent），若需要这些能力须单开 Task。
· TokenBudgetGuard 的计数已由 CLI 的 TokenCountingProvider 接入，但依赖 Provider 在 LlmEvent::Done 报告 usage；Provider 未报告 usage 时无法计费或限额。若需在 runtime 内强制，必须先单开 Task 修改冻结契约或事件模型。
· jessica-memory 当前只实现 EventStore（SQLite 事件日志）；core 的 MemoryStore Trait（分层记忆）尚未实现，留待 Phase 5。EventStore 为同步 API，异步调用方必须使用 spawn_blocking；SQLite 的 append 目前单连接串行，跨进程并发写入依赖 SQLite 锁，已设置 5s busy_timeout，但仍未启用 WAL 或迁移机制。
· WasmInstance 支持 `() -> i32` 与 BufferAbi 的 alloc/invoke 调用；尚无 component model、typed values、dealloc 或有状态复用。WasmTool 的 ABI 固定为 JSON bytes + 高 32 位长度/低 32 位指针的 packed i64；需要更丰富接口时应单开 Task，避免在适配器里扩协议。
· WasmEngine/WasmModule/WasmInstance 仍是同步 API（load_file 使用 std::fs，wasmtime-wasi 的 p1 同步 shim 在 Tokio worker 上执行 WASI hostcall 会 panic）。WasmTool::load 与 execute 已分别放在 CLI 启动时和每次调用的 spawn_blocking 中；其他调用方必须遵守同一约束。
· jessica-sandbox/tests/fixtures/build.sh 与 examples/plugins/echo/build.sh 均依赖 wasm-tools（当前环境未安装）；预编译 .wasm 已提交且测试不依赖该工具，示例 .wasm 由缓存的 wat crate 生成。
· manifest 的 permissions 使用 deny_unknown_fields，write_dirs 等未知权限在加载期被拒绝，当前没有写权限表示；参数与输出只做 JSON 语法校验，未按 manifest schema 做 JSON Schema 校验（需新增依赖，单开 Task）。
· 每次 instantiate、alloc、invoke 都各创建一个 watchdog OS 线程并 join；WasmTool 一次 execute 至少启动 3 个线程。高频工具可考虑共享 epoch ticker，但需重新验证并发安全。
· wasmtime epoch 是 Engine 级共享状态；WasmTool 已用每工具 Mutex 串行化自身调用，但多个 WasmTool 不应共享同一 Engine（当前每个 manifest 独立 WasmEngine，CLI 无此问题）。若未来共享 Engine，需引擎级串行化或每调用独立 Engine。
· WASI 默认使用宿主时钟与随机数；若 wasm 工具结果需要参与确定性重放，需要单开 Task 注入确定性 clocks/random，不能依赖宿主环境。
· SandboxPolicy 的 guest_path 仍未校验；WasmTool manifest 已要求绝对 guest path。只读性由 FsPerms::ReadOnly 强制，API 不存在授予写权限的路径。WASI preview 1 本身没有创建 socket 的接口，网络在无 preopen fd 时结构性不可达，allow_tcp/udp(false) 作为额外防线保留。
· WasmTool 每次 execute 新建实例，插件无法跨调用保存状态；需要有状态插件时应单开 Task 设计实例生命周期与恢复语义。
· CLI 的 GovernedTool 超时是外层 tokio timeout；若 manifest 的 wall_timeout_ms 大于 CLI tool_timeout_ms，超时后 spawn_blocking 任务不会被取消，会继续运行到 sandbox 墙钟结束。配置上应保持 manifest 墙钟不超过 CLI tool_timeout；若需强制截断，需在 sandbox 暴露取消句柄。
· WasmTool 的 max_result_bytes（manifest）与 CLI ResultSizeGuard 的 budget.max_result_bytes 独立生效，两者取先触发者；配置时需同时留意。
· Harness::run 仍不会自动执行 Guards/Policy；CLI 已在 ToolCallStarted / authorize_tool_call / GovernedTool 中显式接入。后续新增入口（TUI、服务端等）需复用这套装配，或单开 Task 设计统一 executor，且不得破坏 harness→core 单向依赖。
· Harness 的 broadcast 通道有界（CLI 使用默认 1024）；Hook 或 EventPump 处理慢时会 lag 并漏事件。EventPump 目前只告警，漏掉 ModelDelta 会导致 acceptance transcript 不完整。若治理要求“每个事件必达”，后续需改为同步回调/独立无界队列并定义背压。
· CLI REPL 每次输入仍开启全新 session；`--resume` 只恢复中断的同一个 loop，不能与 prompt 组合，因此跨输入对话历史仍未实现。
· 恢复所需的初始 user/system messages 存在 jessica-memory 的 `sessions` 侧表，因为 AgentEvent 没有 user message 变体且 jessica-core 已冻结。若未来允许扩展 core，应改为显式的用户消息事件并删除该侧表；任何 core 变更必须先单开 Task。
· 事件流没有显式的 round 边界：连续两个都不产生文本、只请求工具的模型回合，与一个回合请求多个工具在事件序列上不可区分。recover_session 将其计为一个 round，因此 resume 的 `rounds_completed` 只会低估（resume 可能多获得一次模型调用），不会高估。要精确对齐 max_rounds，必须在 core 增加 round 边界事件（需单开 Task）。
· `--resume` 目前不允许与 prompt 组合；已终结的 session 只能读取记录 reason，不能据此开启新的用户回合。若需要“恢复后继续对话”，应以新 session + 旧 messages 重新开始并单独定义语义（单开 Task）。
· 恢复路径直接调用 `AgentLoop::resume`，不经过 CLI 的 acceptance 重试编排；中断会话恢复完成后的验收检查语义尚未定义，需单开 Task。
· EventPump 只在存在 current session 时持久化事件；AcceptanceRunner 在两个 attempt 之间发出的 AcceptanceFailed 没有归属 session，因此不会写入事件库。若要求完整审计，需要为 turn 级事件建立 outer session 归属或显式 session 关联（单开 Task）。
· 恢复流的事件持久化依赖 EventPump 的 broadcast 接收；通道 lag 或丢事件时，恢复所需的尾部事件也会缺失（与 transcript 风险相同）。若要强一致，需同步持久化 hook 或改事件总线。
· 恢复时 `rounds_completed` 只来自事件流；若原 session 的 max_rounds 很小且末尾批次已用尽预算，resume 会正确发出 BudgetExceeded + SessionEnded；但若 checkpoint 之后用户修改了配置（例如增大 max_rounds），恢复会使用新配置，不保证与原计划一致。
· replay 只重建 AgentLoop 层的事件流：CLI 治理发出的 PolicyDenied、AcceptanceFailed 以及 token/result-size 导致的 BudgetExceeded 不会重新生成；它们对循环的影响已经体现在记录的 ToolResult 与终止原因中。若需要完整 turn 级治理重放，必须重新设计 CLI 装配并解决命令 sensor/acceptance 的外部依赖（单开 Task）。
· 技能任务队列（5.2.2b）当前采用方案 B：job 状态只存 skills.db、不在事件流；resume 的入队幂等依赖 skills.db 唯一索引，事件流无法恢复 job 状态。若未来要求 resume 完全由事件流确定性决定，需单开 Task 迁移到方案 A：在 core 新增 SkillJobEnqueued/SkillJobCompleted 及 payload、实现 rebuild_from_events 物化、resume 改读事件流/物化视图、replay 跳过物化；core 变更必须单独 review。
· AgentEvent 不保存 LlmEvent::Done 的 TokenUsage，replay 只能合成 usage=None，因此 token 计费无法重放或校验。要支持计费重放，需要在事件模型增加 usage（修改冻结 core，需单开 Task）。
· replay 要求事件流以 SessionEnded 结尾，只支持 Completed、Error 与 max-rounds BudgetExceeded；Cancelled、AcceptanceFailed 以及中断的流会被拒绝（中断恢复用 --resume）。若需要重放中断会话，应先定义“无终态时如何合成终态”。
· replay 输出为紧凑 JSONL，每行一个 AgentEvent，不含 seq/timestamp；事件顺序隐含在行序中，消费者不应依赖文件元数据做确定性判断。
· Task 5.1.5 的 `[memory].write_policy = "explicit"` 只定义为“不自动写入”；显式记忆写入入口（CLI 命令或工具）尚未设计，需单开 Task，且不得用 LLM 判断何时写入。
· SqliteMemoryStore 对重复内容写入直接返回 Ok，不补写向量：若首次写入时 embedding 失败、之后再次写入相同内容，不会重试生成 vector。若要支持补偿，需要让 MemoryLog::append 返回已有条目 id（会改变 5.1.1 API）或新增只读查询接口，留待需要时单开 Task。
· replay 通过容量 1024 的 broadcast 通道收集事件，超大 session 可能因 lag 报 EventLag；如需支持任意长度，应改为同步观察者或可配置容量。
· replay 事件流是运行时事件的再序列化，不是对 Provider 原始请求/响应的逐字节录制；若要审计原始响应，需要额外记录 provider wire log（单开 Task）。
· 文件工具的路径校验存在 TOCTOU 竞态（canonicalize 与实际读写之间路径可被替换），需 Phase 2 policy / Phase 3 sandbox 进一步约束。
· bash 超时只杀死直接子进程，不杀进程组，后代进程可能残留；后续可考虑 process group / setsid。
· CLI 的 token 预算按整个 turn（含所有 acceptance attempts）累计；tool timeout 与 result size 按单次调用；max_rounds 仍由 runtime 按单 attempt 计数，验收重试会重置。若需要跨 attempt 的全局 round 预算，需单开 Task。
· StuckDetectionGuard 已在每次 attempt 前 reset；但 CLI 只在 ToolCallStarted 检查，且 GuardContext 携带 tool_call，guard 把任何工具调用视为进展，因此纯工具循环实际由 max_rounds 兜底，Stuck 很难触发。调整该 guard 的“进展”语义需另开 Task，避免修改 Task 2.2 已验收行为。
· SensorPipeline 与 AcceptanceRunner 仍不会被 Harness::run 自动驱动；CLI 已通过 EventPump 在 ToolCallFinished 运行 sensors、在 attempt 结束后驱动 acceptance。其他消费方需自行接入。
· Acceptance transcript 由 EventPump 从 AgentEvent 重建（ModelDelta/ToolCallStarted/ToolCallFinished/SessionEnded），未改 runtime 返回类型；广播 lag 时 transcript 可能不完整。
· 同一 turn 的每次 acceptance attempt 都发出自己的 SessionEnded；CLI 最后再发一个 outer SessionEnded 承载最终 reason（如 AcceptanceFailed），并抑制被预算覆盖的内层 Completed/Cancelled。消费方需按 session_id 区分 attempt 与最终 turn。
· CommandSensor 与 CommandAcceptanceCheck 均直接运行配置命令，暂无超时；AcceptanceCheck 的输出截断到 8 KiB，但 CommandSensor 无输出上限，挂起的检查命令可无限阻塞。需要单开 Task 增加超时与进程组清理。
· Allowlist 拒绝会以错误 ToolResult 返回并继续会话，而不是终止；Task 2.4 的“越权工具被拒”通过 PolicyDenied 事件 + tool error 观察。若要求拒绝即终止，需调整 Policy 语义。
· README.md 首句故意保留拼写错误 `determinstic`，作为 examples/harness.toml 的验收夹具；修正它会使示例 acceptance 首次即通过，无法演示重试。真实 API key 联调仍待进行。
· Task 5.2.4 的 `SkillJob` 直接携带 `DistillSource`，而 5.2.2b 的 `skill_jobs.source_json` 保存的是已转义/已预算的内容字段。5.2.6 的 CLI 适配器需要一条从 `source_json` 重建 `DistillSource` 的路径（例如新增 `DistillSource::from_persisted` 或等价构造函数），且不得再次执行 `escape_untrusted` 或预算截断，否则会二次转义并破坏 `to_untrusted_block` 的字节稳定性；该 API 若加入 5.2.3 模块应由 5.2.6 显式实现并测试。
· 子 Agent 轨迹目前不持久化（5.3 的 v1 折中）：父事件流只有 `delegate_task` 的调用与结果，子会话的模型增量、工具调用与终止原因均丢弃，因此子轨迹不可 replay、不可审计、也无法从 checkpoint 恢复。若要保存子会话，需新增子 EventStore、父子 session 关联（含父/子 trace 关联；当前 `child_session_hash` 只由 task + narrowed tools 派生，不含父 session id）以及 replay/resume 的边界规则；5.3.6 候选，必须先定义事件模型与存储 schema，不得把子事件混入父事件流。
· 5.3 v1 只支持单层委派；嵌套委派被 `narrow_tools` 结构性禁止。若未来允许嵌套，需单开 Task 定义深度上限、权限链传递（每层只能收窄）、父 token/工具调用预算切分、循环检测、子会话 EventStore 与失败语义；不得仅删除 `delegate_task` 过滤就放开。
· 5.3 v1 子 Agent 不加载 WASM 工具；`narrow_tools` 与 CLI factory 双重排除。未来若允许子 WASM 实例，需单开 Task 验证每子实例独立 guest 状态/fuel/epoch、模块重复加载的性能与失败隔离、以及不得复用带 Mutex 的 `WasmTool`；当前共享实例会破坏“子 Agent 间不共享可变状态”。
· 5.3 将子工具超时固定为 5000ms，未提供配置项。若未来需要独立配置，需单开 Task 增加 `delegation.child_tool_timeout_ms`，并在配置加载期验证 `child_tool_timeout_ms < delegation.timeout_ms < budget.tool_timeout_ms`，同时保留默认值不变；不得在运行期按模型输入动态调整。

---

与原始计划的差异说明

这份计划把整体架构拆成 17 个 Task，每个 Task 对应 Claude Code 的一次会话。项目名统一为 Jessica，crate 前缀统一为 jessica-。关键设计是：每个 Task 的验收命令都是可执行的，Claude Code 能自己确认是否完成；“禁止事项”明确列出，因为 Claude Code 倾向于过度发挥（提前实现、顺手重构、把测试写成自证式）；Task 0 的 CLAUDE.md 是跨会话的记忆载体，弥补单次上下文窗口的限制。Phase 2 的四个 Task 是整个计划的价值核心，如果资源有限，Phase 3-5 可以推迟，但 Phase 2 不可压缩。
