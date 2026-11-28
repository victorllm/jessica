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
· Task 5.5 可观测性：OTLP 导出、会话回放可视化工具。**暂缓，见文末 Deferred。**

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

Phase 6：ACP 支持（ACP-0 → ACP-4；ACP-5 stdio MCP；ACP-7a/7b；ACP-6 可选）

背景：ACP（Agent Client Protocol）是编辑器/客户端与 Agent 之间的 JSON-RPC 2.0 over stdio 协议。Jessica 目前没有长驻 Agent 服务和跨 prompt 会话状态；本 Phase 把 `jessica acp` 做成一个长驻 stdio 服务，让 ACP 客户端可以创建会话、发送 prompt、接收流式更新并取消执行，同时保持 jessica-core 冻结、现有事件持久化与治理链不变。Phase 6 为 2026-11-15 新增，不在最初的 17 个 Task 内。

Phase 6 全局约束（适用于 ACP-0–ACP-4）：

· 只做 ACP stable v1；不实现 v2 alpha，不启用 SDK 的 `unstable_protocol_v2`，不启用 `process` feature。
· 依赖门禁：仅允许新增 `agent-client-protocol` 5.3.0（`default-features = false, features = ["stdio"]`）。ACP-0 开工前必须由用户显式批准该 crate；批准后在 workspace Cargo.toml 声明并记录理由（含 `cargo tree` 审查结果）。其余 Task 不得新增任何第三方 crate。（2026-11-15：用户以“请执行 Task ACP-0”指令批准该依赖，记录见进度表 ACP-0。）
· 不修改 jessica-core 公共 API。允许的实现层改动：jessica-cli 内部装配、EventPump、配置；jessica-tools 仅为 BashTool 增加显式 cwd 的加法 API。jessica-runtime、jessica-harness、jessica-sandbox、jessica-providers、jessica-memory 原则上不改；确需改动必须停下询问用户并单开 Task。
· ACP 模式下 stdout 只允许出现 JSON-RPC；禁止 `print!`/`println!` 业务输出；日志、警告、错误一律 stderr；不启用 ACP 传输层 TRACE 日志。
· ACP session 的 `cwd` 必须为绝对路径、存在、是目录，canonicalize 后必须位于启动 workspace root 之内；memory、recorder、skills、wasm 仍按启动 root 作用域。多 workspace 服务不在本 Phase 范围。
· ACP-0–ACP-4 不实现：MCP、`session/request_permission`、`session/load`/`list`/`delete`/`resume`/`close`、客户端 `fs/*` 与 `terminal/*`、图片/音频/embedded resource、session modes/config options/plan/usage update、ACP v2。后续 ACP-7a/7b 已分别实现 `load` 与 `list`/`delete`/`resume`/`close`（受持久化能力门控）；ACP-5 已实现 `session/new.mcpServers` 的 stdio MCP（HTTP/SSE 仍拒绝，load/resume 不启动 MCP）；ACP-6 的 core/runtime 前置 ACP-6a 已实现（`Tool::execute_with_call`），ACP 权限请求已在 ACP-6 实现（固定敏感集 bash/write_file/delegate_task，allow_once/reject_once，客户端 cancelled/固定 5 分钟超时/传输失败均 fail-closed）；archive/fork/status/set_title 仍未实现。
· ACP 首期禁用 acceptance 重试（每个 prompt 只跑一次 attempt）；acceptance 失败返回 JSON-RPC error。原因：v1 客户端对多段 assistant 输出的 `messageId` 支持不一致，重试输出可能被拼接成一条消息。若 ACP-2 开工前用户决定启用重试，必须同时定义 `messageId` 与 notice 行为。
· 历史上限默认：`max_history_messages = 200`、`max_history_bytes = 2 MiB`、`max_prompt_bytes = 1 MiB`、`max_sessions = 8`；全部可在 `[acp]` 配置中调整。
· 每个 Task 完成前必须运行 `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`，并贴出结果。

Task ACP-0：依赖门禁、传输骨架、initialize、stdout 纯净与优雅退出

目标：建立 `jessica acp` 入口和 ACP v1 stdio 传输，只实现 `initialize`，证明协议可连通、stdout 不被污染、进程可优雅退出。本 Task 不实现任何 session 方法。

操作：

1. 依赖门禁：取得用户对 `agent-client-protocol` 5.3.0 的显式批准；在 workspace Cargo.toml 增加 `agent-client-protocol = { version = "5.3.0", default-features = false, features = ["stdio"] }`，jessica-cli 继承该依赖；用 `cargo tree -p agent-client-protocol` 审查新增传递依赖并记录。不得启用 `process`、`unstable_protocol_v2` 或任何默认 feature。
2. 新增 CLI 模式 `Mode::Acp`：支持 `jessica acp` 与 `jessica acp --config <path>`；复用现有配置加载、root 解析、runtime 装配、`ShutdownController` 与 `SignalListener`。ACP 分支不得进入 REPL、replay、skills 的用户输出路径。
3. 用官方 SDK 建立 `Agent.v1()` 连接（`Stdio::new()`），实现 `initialize` handler：
   - 只支持协议版本 1；客户端传 1 时回 1；传其他版本时按 v1 schema 返回服务器最新支持版本 1，由客户端决定是否断开。
   - 返回 `agentInfo`（name = `jessica`，version 取 crate 版本）。
   - `agentCapabilities` 显式声明：`loadSession = false`、`promptCapabilities.image/audio/embeddedContext = false`、`mcpCapabilities.http/sse = false`、`sessionCapabilities = {}`；`authMethods = []`。
4. stdout 纯净：ACP 路径禁用所有业务 `print!`/`println!`；EventPump 的人类可读输出在 ACP 模式下必须关闭（ACP-1 完成 EventPump 输出策略；ACP-0 至少保证 initialize 路径不打印）。不安装写 stdout 的 tracing subscriber。
5. 优雅退出：stdin EOF、SIGINT、SIGTERM 都停止接受新请求，在限定时间内退出；本 Task 没有 active session，只需验证不会挂起。保留现有第二次信号强制退出的语义。
6. 本 Task 只搭 transport/initialize；不要提前实现 session/new、session/prompt 或事件映射。

行为测试（至少）：

· 用内存 duplex 或脚本化 stdio 客户端发送 `initialize`，断言 `protocolVersion = 1`、capabilities 全为 false、agentInfo 名称正确。
· 客户端发送非 1 版本，断言服务器仍返回 1（而不是 panic 或断开前崩溃）。
· 捕获整个进程 stdout，断言只有合法 JSON-RPC 行；stderr 允许诊断输出。
· stdin EOF 后进程在超时内退出；SIGTERM 路径退出码沿用现有 shutdown 语义。
· 未实现的方法返回 `-32601 Method not found`，进程不崩溃。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli acp
```

并执行上述 initialize/stdout/EOF/SIGTERM 行为测试（测试内自动运行，不依赖真实网络）。

禁止：未取得用户批准就修改 Cargo.toml；启用 v2/process/default features；实现 session/new 或 session/prompt；修改 jessica-core；用 stdout 打印任何非 JSON-RPC 内容；新增本 Task 之外的依赖。

Task ACP-1：Turn 执行上下文重构（仅重构，不接 ACP 会话）

目标：为 ACP 准备好 per-prompt 的 workspace root、取消父 token、静默事件输出和可复用的 turn 执行入口；本 Task 结束时现有 CLI 行为必须完全不变。

操作：

1. 引入 `TurnScope { root: PathBuf, cancel_parent: CancellationToken }`（名称可调整），贯穿 `build_turn`、`build_sensors`、`build_child_executor`、`build_tool_registry`、`initial_messages`、system prompt、acceptance working_dir 与 `resolve_dir`。
2. 保留 `CliRuntime.root` 作为启动 workspace root，继续用于 memory、recorder、skills、wasm 的进程级打开；per-turn 的文件工具、bash、sensor、acceptance、delegation 子工具使用 `TurnScope.root`。`TurnScope.root` 必须 canonicalize。
3. `BashTool` 增加显式 `with_current_dir(...)`（jessica-tools 加法 API），构建注册表时设置 per-turn cwd；现有 `BashTool::new()` 行为保持不变。
4. EventPump 增加输出策略：`HumanConsole`（默认，现有 CLI）与 `Silent`（ACP）。Silent 模式不得写 stdout。为 ACP 增加“lag 视为错误”的可观察状态（现有 `Lagged` 只 warn 并继续的行为不得用于 ACP 路径）。
5. 从 `CliRuntime::run_turn` 提取可复用入口（名称可调整）：输入 `TurnRequest { scope, history, user_input, update_sink, acceptance_retries }`，输出 `TurnOutcome { reason, messages }`；`run_turn` 用启动 root、空 history、现有 acceptance 行为委派，保持 REPL/one-shot/resume 行为不变。
6. 初始消息顺序改为 `system prompt → memory guide（若有）→ skill guide（若有）→ history → user`；现有 CLI 的 history 为空，因此输出不变。acceptance 重试复用同一份初始消息。
7. 计数与信号只统计“本次 turn 新增的消息切片”，不得把历史消息里的工具调用计入本 turn 的 structured_counts / skill 触发计数。
8. 本 Task 不新增 ACP handler，不改变 `--resume`、replay、skills 子命令语义。

行为测试（至少）：

· 同一 runtime 用两个不同 turn root 执行 read_file/write_file，互不串目录；越界路径仍被拒绝。
· `BashTool` 显式 cwd 生效；未设置 cwd 时保持旧行为。
· 取消父 token 能取消子 turn；取消一个 turn 不影响另一个。
· EventPump Silent 模式不产生 stdout；HumanConsole 模式输出与改造前一致。
· 现有 CLI 的 one-shot/REPL/resume/acceptance 测试全绿；run_turn 返回的最终 messages 与事件库/原有快照一致。
· history 非空时，structured_counts 只统计新增切片；把历史工具调用构造进 messages 也不会误触发 skill 阈值。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli
```

禁止：接 ACP session 方法；修改 jessica-core；改变现有 CLI 的可观察行为；把 EventPump 改成同步持久化；新增第三方 crate；顺手重构 Task 范围外的模块。

Task ACP-2：session/new、session/prompt、事件映射与多轮历史

目标：ACP 客户端可以创建 session、连续发送多个 prompt，收到流式文本与工具调用更新，并获得正确的 stopReason。本 Task 不做取消/并发/关闭（ACP-3）与限制打磨（ACP-4）。

操作：

1. `HarnessConfig` 增加可选 `[acp]` 配置段：`max_sessions`、`max_prompt_bytes`、`max_history_messages`、`max_history_bytes`；默认值按 Phase 6 全局约束；非法值在配置加载期返回结构化错误，不静默回退。
2. 新增 AcpSessionRegistry 与 AcpSession：`session_id`、canonical `cwd`、`history: Vec<Message>`、当前 prompt 状态与取消 token（本 Task 创建 token，ACP-3 接管取消）。session id 使用 `acp-{pid}-{seq}`；内部 turn id 使用该 id 加 `-t{turn_seq}`。
3. `session/new`：校验 `cwd` 绝对、存在、目录、位于启动 root 内；`mcpServers` 非空返回 `InvalidParams`（首期不做 MCP）；`additionalDirectories`/`systemPrompt` 非空返回 `InvalidParams`；超过 `max_sessions` 返回结构化错误；返回 `{ sessionId }`。
4. prompt 内容：支持 `text` 与 `resource_link`（baseline），按顺序拼接；`resource_link` 转成一行 `[resource] name=... uri=...`；`image`/`audio`/`resource` 返回 `InvalidParams`；空文本或超过 `max_prompt_bytes` 返回 `InvalidParams`。
5. `session/prompt`：查 session、原子检查 active prompt（同一 session 第二个 prompt 返回“session busy”）、创建 prompt token、在 spawned task 中执行后把 responder 移入 task；handler 不得阻塞 SDK 分发循环。
6. prompt worker：取配置快照；构建 bootstrap（system/memory/skill）；组装 `bootstrap + history + user`；用 session root 与 prompt token 构建 turn；禁用 acceptance 重试（单次 attempt）；运行 AgentLoop；等待 EventPump 终态；按规则提交 history；发送 prompt response。
7. 事件映射：
   - `ModelDelta` → `agent_message_chunk`（text；过滤空串），使用本 prompt 唯一的 `messageId`。
   - `ToolCallStarted` → `tool_call`，`status = in_progress`，带 title/kind/rawInput/locations。
   - `ToolCallFinished` → `tool_call_update`，成功 completed、失败 failed，带截断后的文本 content/rawOutput。
   - `PolicyDenied` 不重复产生 failed 更新（`ToolCallFinished` 已覆盖）。
   - `SessionStarted`/`SessionEnded` 不直接映射；`Error`/`AcceptanceFailed` 通过 JSON-RPC error 返回。
   - 不发 `agent_thought_chunk`（Jessica 无 reasoning 流）。
   - tool kind：read_file → read、write_file → edit、bash → execute、wasm/delegate → execute/other；title 截断约 120 字符；rawInput/rawOutput 各截断 16 KiB。
8. 顺序与屏障：所有 `session/update` 必须先于 prompt response；prompt worker 在响应前等待 update writer 的屏障确认。EventPump 出现 lag 时不得静默输出残缺文本，按内部错误处理并取消该 prompt。
9. stopReason 与错误：`Completed → end_turn`；`Cancelled → cancelled`（ACP-3 完整接管）；token `BudgetExceeded → max_tokens`；rounds/其它预算 → `max_turn_requests`；provider/pump/内部错误 → `-32603`。错误响应不得泄露密钥。
10. history 提交：成功提交本次新增消息；错误不提交；取消时保留部分输出但必须补齐未完成 tool call 的合成 tool result（不能留下 assistant tool_calls 无 tool result）。history 超过上限时丢弃最旧完整 turn，绝不拆开 tool call 与 result 对。
11. 不得把完整 prompt、工具参数或密钥写入 stderr/tracing。ACP 模式不启用 TRACE。

行为测试（至少）：

· 脚本客户端创建 session 后连续发两个 prompt；用 ScriptedProvider 断言第二次 provider 请求包含第一次的 user/assistant 消息，且顺序为 bootstrap → history → 新 user。
· read_file 调用产生 `tool_call`（kind=read、status=in_progress）与 `tool_call_update`（completed），id/title/rawInput 正确；失败工具产生 failed。
· 所有 `agent_message_chunk` 在 prompt response 之前到达；空 delta 不发送。
· `Completed`、token/rounds BudgetExceeded 的 stopReason 映射正确；provider 错误返回 JSON-RPC error 且 history 不变。
· 图片/embedded resource 被拒绝且零 provider 调用；空 prompt、超长 prompt 被拒绝。
· 同一 session 第二个并发 prompt 返回 busy；不同 session 可同时创建。
· history 裁剪后不出现孤立 tool result；history 超限时丢弃最旧完整 turn。
· cwd 为相对路径、不存在、文件、或启动 root 之外时返回 `InvalidParams`；非空 `mcpServers` 返回 `InvalidParams`。
· 捕获 stdout，断言只有 JSON-RPC，且 update 与 response 的顺序正确。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli acp
```

禁止：实现 cancel/并发关闭（留给 ACP-3）；实现 MCP/权限/load；修改 jessica-core；在 handler 中同步执行模型或工具；静默忽略不支持的内容块；把 lag 当正常情况继续；新增第三方 crate。

Task ACP-3：session/cancel、并发与关闭

目标：取消只影响目标 session 的当前 prompt；多个 session 可并发运行；进程关闭/客户端断开时所有 prompt 被取消且进程可退出。

操作：

1. 取消 token 链：进程 shutdown → session lifetime token → 当前 prompt token → 本次 turn token。`session/cancel` 只 cancel 当前 prompt token；session 本身仍可接受下一个 prompt；没有 active prompt 时幂等忽略。
2. 按 v1 schema：只要客户端发送过 `session/cancel`，该 prompt 的响应必须是 `stopReason: cancelled`，即使底层操作刚好完成或抛异常。不要返回 `end_turn` 掩盖取消。
3. 取消/错误时补齐 in-flight 工具：对已发出 `ToolCallStarted` 但未收到 `ToolCallFinished` 的调用发送 `tool_call_update`，状态 failed，内容说明被取消；客户端不能留下永远 in_progress 的工具调用。
4. 同一 session 的第二个 active prompt 返回错误；不同 session 的 prompt 可并发执行。所有共享服务（provider、recorder、memory、skills）必须保持线程安全；EventStore 写入仍通过 spawn_blocking 串行化，不新增阻塞调用。
5. 进程关闭：停止接受新请求，取消所有 session lifetime token，等待 active prompt worker 收尾（限定超时），drain skills，退出。超时只 warn，不无限挂起；第二次信号沿用强制退出语义。
6. stdin EOF / stdout 写失败：取消所有 session，停止接受新 prompt，限定时间内退出；不得在客户端断开后继续执行工具。
7. history 提交规则：取消时保留已产生的部分输出并补齐合成 tool result；内部错误不提交本次 turn。pump 失败/lag 时不提交。

行为测试（至少）：

· 两个 session 并发 prompt；cancel 其中一个，另一个正常完成并返回 end_turn。
· cancel 后目标 prompt 返回 cancelled；同一 session 之后还能再发新 prompt。
· cancel 时 in-flight 工具收到 failed 更新；没有遗留 in_progress。
· 无 active prompt 时 cancel 幂等；同一 session 第二个 prompt 返回 busy。
· SIGTERM/EOF 时两个 session 都被取消，进程在超时内退出；skills drain 执行。
· stdout 写失败模拟：prompt 被取消，进程不 panic、不无限写。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli acp
```

禁止：把 cancel 实现成全局取消；取消后 session 永久不可用；在取消路径丢事件或 panic；修改 jessica-core；新增第三方 crate；用 sleep 轮询代替 token。

Task ACP-4：限制、内容/错误策略、隐私与配置加固

目标：把 ACP 服务从“能跑”加固到“可控”：session/prompt/history 上限、notice 能力门控、错误码与消息、日志隐私、配置热加载与文档。

操作：

1. 强制执行并测试 `max_sessions`、`max_prompt_bytes`、`max_history_messages`、`max_history_bytes`；超限行为确定且错误消息可操作；history 裁剪只丢最旧完整 turn，不拆 tool call/result 对。
2. `notice` 只允许在客户端 `clientCapabilities.session.notices` 声明时发送；否则不得发送 notice。BudgetExceeded/PolicyDenied 等提示优先通过 stopReason/tool_call_update 表达，不强行用 notice。
3. 统一错误码与消息：method not found、invalid params、session busy、session not found、internal error；错误消息不得包含密钥、完整 prompt 或完整工具参数。JSON-RPC 错误与 prompt 内工具失败要区分。
4. 隐私与日志：ACP 模式 stdout 纯净；stderr 只输出必要诊断；不启用 ACP TRACE；不在日志中打印 prompt、工具参数、env 值、密钥。客户端断开/写失败路径同样遵守。
5. 配置热加载：ACP 复用现有 ConfigWatcher；每个 prompt 取最新配置快照；events/memory/skills/wasm/reliability 等不可热改段按现有规则拒绝或保留启动值；wasm 工具仍启动时加载（与 REPL 一致）。若实现发现热加载会破坏 cwd/memory 作用域，记录 Deferred，不在本 Task 强行扩大。
6. 文档：在 README 或 examples 中给出 ACP 启动命令、编辑器配置示例、能力/非目标、cwd 限制、无 MCP/无权限/无 session load 的明确说明。
7. 汇总 Phase 6 已知限制到文末 Deferred，不把可选能力偷偷塞进本 Task。

行为测试（至少）：

· 超过 session 上限、prompt 上限、history 条数/字节上限的请求返回确定错误；合法边界不误拒。
· history 裁剪后 provider 请求仍然合法（tool call/result 成对），且最旧 turn 被丢弃。
· 客户端未声明 notices 时零 notice；声明时按 schema 发送。
· 各错误路径返回预期 JSON-RPC 错误码/消息，stdout 仍只有 JSON-RPC。
· 配置热加载：修改允许的配置后，下一个 prompt 使用新值；修改不可热改段的行为与现有 REPL 一致。
· 日志隐私测试：构造含敏感字符串的 prompt/工具参数，断言 stderr 不出现该完整内容（允许出现工具名/错误类别）。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli acp
```

并执行真实 ACP 客户端手工互通，至少覆盖 **Paseo**（getpaseo/paseo）：initialize → session/new → 两轮 prompt → session/cancel，记录 Paseo 版本、启动命令/配置方式、每一阶段的观察结果与失败日志。若本机无法安装 Paseo，必须在 Task 记录中说明阻塞原因，并至少用官方 SDK 客户端或另一真实 ACP 客户端完成同等验收；脚本化客户端只用于自动化测试，不能替代真实客户端验收。若 Paseo 互通暴露 MCP、权限请求、session/load 等超出 ACP-0–4 范围的需求，记录到 Deferred 并由用户决定是否单开 ACP-5/6/7，不得在 ACP-4 顺手实现。不依赖真实模型时可用脚本化 provider。

禁止：在本 Task 实现 MCP/权限/session load；修改 jessica-core；把 notice 用于未声明能力的客户端；在错误消息中泄露敏感内容；新增第三方 crate；顺手重构 ACP-0–3 之外的行为。

Task ACP-7a：session/load 与 ACP 会话索引（2026-11-16）

目标：跨进程恢复 ACP 会话。复用现有 append-only 事件库保存对话，只新增一个小型 ACP 会话索引；仅在持久化成功打开时声明 `loadSession` 并实现 `session/load`。本 Task 不做 `list`/`delete`/`resume`/`close`（ACP-7b）。

操作：

1. `[acp]` 增加可选 `state_dir: Option<PathBuf>`；相对路径按启动 workspace root 解析。配置校验要求 `state_dir` 存在时 `[events].path` 也必须存在，否则启动失败。`state_dir` 与 `[acp]` 其余限制一样为启动冻结，不参与热加载。
2. 新增 `AcpPersistence`：打开/创建 canonical state dir，并复用 `EventRecorder` 打开事件库。索引打不开、事件库打不开或配置非法时 ACP 启动失败。
3. 每个 ACP session 一个 JSON 索引文件（version=1，文件名由 session id 十六进制编码）；字段只含 `sessionId`、canonical `cwd`、`createdAt`/`updatedAt`、已提交 turn id 列表。不得写入 prompt、模型输出、工具参数、env 或密钥。写入采用同目录临时文件 + `sync_all` + `rename` 原子替换。
4. `session/new` 成功创建内存 session 后写索引；索引写失败则移除该 session 并返回 `-32603`，避免出现“客户端拿到 id 但无法持久恢复”的半成功状态。
5. prompt 在 update writer barrier 成功且本次 turn 可提交时，把内部 `turn_id` 追加到索引；索引写失败只 warn，不改变已成功 prompt 的响应或内存 history（load 时回退到更旧的可恢复 turn）。turn id 与 session id 均包含进程 nonce，确保重启后恢复的 session 不会复用旧事件 key。
6. `session/load`：拒绝非空 `mcpServers`/`additionalDirectories`/`systemPrompt`（`-32602`）；未知 session 返回 `-32002`；cwd 必须通过既有 absolute/exists/directory/root 校验且等于索引中的 canonical cwd，否则 `-32602`；同一进程重复 load 返回 `-32602`；未配置持久化时返回 `-32601`。
7. 历史重建：从索引最新的 turn 开始读取事件流并调用 `jessica_runtime::recover_session`；某 turn 缺失/损坏时回退到更旧 turn；全部不可恢复返回 `-32603`。重建后过滤 `Role::System`，执行 `complete_tool_pairs` 与既有 `trim_history`，再注册为可继续 prompt 的 session。
8. 重放：在 load response 之前通过 `session/update` 按顺序发送 user 文本、assistant 文本、tool call（in_progress）与 tool result（completed/failed）；每条消息使用独立 synthetic `messageId`。发送失败即关闭 transport 并移除刚恢复的 session。
9. 文档明确：只有配置本功能后创建的会话可恢复；旧 ACP 对话没有索引，不可恢复；`list`/`delete`/`resume`/`close` 仍不支持。

行为测试（至少）：

· 无持久化时 `initialize.loadSession=false` 且 `session/load` 返回 `-32601`；配置有效时 `loadSession=true`，并实际创建 state dir 与事件库。
· 真实进程重启：创建 session → 含工具调用的 prompt → 关闭进程 → 同一 config 重启 → `session/load` 在响应前重放 user/tool/tool result/assistant 更新；随后新 prompt 的 provider 请求包含恢复的 user、assistant tool call、tool result 与 assistant 回答。
· 空 session（无已提交 turn）可 load 且零重放更新。
· 未知 id `-32002`、cwd 不匹配 `-32602`、重复 load `-32602`、非空 `mcpServers` `-32602`。
· 索引 round-trip、重复创建不覆盖、损坏/版本不匹配/未知字段拒绝、turn 上限、系统消息过滤、最新 turn 损坏时回退、全部损坏 fail-closed。

验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p jessica-cli acp
```

并执行 Paseo 0.15.3 真实客户端验收：关闭并重启 Paseo daemon 后重新打开同一 Jessica 会话，确认历史重放与后续 prompt 上下文。记录 Paseo 版本、配置方式、阶段观察与失败日志。

禁止：实现 `session/list`/`delete`/`resume`/`close`；修改 jessica-core 或 jessica-memory；把对话正文写入索引；修改事件库历史；新增第三方 crate；把旧会话伪装成可恢复；在 load response 之后补发重放更新。

ACP-5（可选，默认不做）：最小 MCP stdio 客户端

目标：补齐 ACP v1 对 stdio MCP server 的 MUST 要求。只有在用户明确要求“完全合规”或目标编辑器必须传 MCP server 时单开 Task。

操作：按 `session/new.mcpServers` 启动 stdio 子进程；MCP initialize、tools/list、tools/call；工具按 server 命名空间注册并经过现有 Policy/Guard；session 关闭时回收子进程；HTTP/SSE MCP 继续不支持；env/密钥不落日志；失败返回结构化错误。验收需用 stub MCP server 覆盖初始化、工具发现、调用、超时、关闭与 env 脱敏。禁止在 ACP-0–4 中顺手实现。

ACP-6（可选，默认不做）：session/request_permission

目标：实现 ACP 权限请求。开工前必须由用户决定 core API 方案：

· A）批准给 `jessica_core::tool::Tool` 增加带默认实现的 `execute_with_call`（仍需单开 core Task）；
· B）接受 jessica-runtime task-local 传递当前 ToolCall（不改 core，但引入隐式上下文）；
· C）不做权限请求（未采用；2026-11-16 已按方案 A 实现 ACP-6）。

禁止使用空/合成 `toolCallId` 糊弄客户端。实现时需覆盖 allow_once/reject_once、客户端 cancelled、超时、错误路径，并保证工具未获批准时绝不执行。

ACP-6a（ACP-6 前置，2026-11-16）：Tool::execute_with_call 加法 API

目标：让 runtime 在调用工具时携带 provider 分配的真实 `ToolCall`，为 ACP-6 的 `session/request_permission` 提供非合成 `toolCallId`，同时不改变现有工具的默认行为。

实现范围：

1. `jessica-core::tool::Tool` 增加带默认实现的 `async fn execute_with_call(&self, call: &ToolCall) -> ToolResult`，默认转发 `self.execute(call.arguments.clone())`；`execute` 仍是必需方法，现有实现无需修改。
2. `jessica-runtime` AgentLoop 在策略/权限/超时外层调用 `tool.execute_with_call(&call)`，保留既有 `ToolCallStarted` → 工具执行 → `ToolCallFinished` 顺序、取消 `select!`、结果 id stamping 与 unknown-tool 行为。
3. 本 Task 不实现 ACP 权限请求、不修改 `GovernedTool`/ACP handler、不新增依赖；不允许任何工具通过默认实现丢失 `call.id` 后仍被 ACP 当作真实 id 使用。
4. 行为测试：core 默认实现转发原始 arguments；override 收到原始 id/name/arguments；runtime 把 provider 的 `call.id` 传给 `execute_with_call` 并用该 id 回填 `ToolResult`；既有单/多工具调用、未知工具、取消测试不回归。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。

Task ACP-6（2026-11-16）：ACP session/request_permission

用户确认的范围：敏感工具固定为 `bash`、`write_file`、`delegate_task`，`read_file`、WASM 与 MCP 工具仍只受 policy 约束；只有顶层 `delegate_task` 询问，子代理工具在该次 delegation 批准下运行；答复超时固定 5 分钟、无配置项；敏感工具先以 `pending` 公布，`allow_once` 后先发送同一 `toolCallId` 的 `in_progress` 再执行。

实现范围：

1. `jessica-cli/src/governance.rs`：新增 `ToolPermissionDecision`（Allow/Reject/Cancelled/TimedOut/TransportFailed）与 `ToolPermissionGate` trait；`GovernedTool` 通过 `with_permission_gate` 持有可选 gate。执行顺序固定为预算触发 → policy 授权 → 权限询问（仅固定集且有 gate）→ 启动工具超时 → `inner.execute_with_call(call)` → 结果大小守卫。空 `call.id` 直接返回错误且不联系客户端；Reject/Cancelled/TimedOut/TransportFailed 全部 fail-closed、不执行工具；权限等待不消耗 `budget.tool_timeout_ms`，批准后才开始计时。
2. `jessica-cli/src/app.rs`：`TurnRequest` 新增 `permission_gate`；`build_tool_registry` 只把 gate 交给 `write_file`、`bash`、`delegate_task`，CLI 与 resume 路径传 `None`。
3. `jessica-cli/src/acp_events.rs`：敏感工具的 `tool_call` 状态为 `Pending`（ACP 序列化省略默认值），其他工具仍为 `InProgress`；新增 `permission_request_update` 与 `permission_granted_update`，复用 provider `toolCallId`、title/kind/rawInput/locations。
4. `jessica-cli/src/acp.rs`：新增 `AcpPermissionGate` 与 `AnnouncedToolCalls`；gate 在 `tool_call` 更新进入同一 writer FIFO 后才入队 `session/request_permission`，保证客户端先看到 pending 调用再收到询问。请求仅提供 `allow_once`（Allow this execution）与 `reject_once`（Reject this execution）；固定 `TOOL_PERMISSION_TIMEOUT=300s`。客户端 `cancelled` outcome 取消整个 prompt；超时/传输失败只失败该工具；client `session/cancel`、close、delete、disconnect 均不会执行工具。每请求使用 child token，取消时 writer 丢弃 `SentRequest` 并发出 `$/cancel_request`；本地连接终止与 peer JSON-RPC 错误区分处理；未知 option id/outcome 一律按 Reject fail-closed。
5. 文档：README 新增 Permission requests 章节，删除“No permission requests”，记录固定敏感集、一次性选项、5 分钟答复期限、pending→in_progress 顺序、delegation 限制与 fail-closed 语义。

行为测试：

1. `governance` 17 个：固定敏感集、批准后原始 call 传入并执行、非 Allow 决策不执行、策略拒绝先于权限、预算耗尽跳过权限、权限等待不占用工具超时、批准后工具超时仍生效、policy-only 工具从不询问 gate、空 id fail-closed、无 gate 时敏感工具仍按 policy 执行。
2. `acp_events` 14 个：敏感工具 pending/其他工具 in_progress、权限请求复用 call id 与展示字段、`in_progress` 更新只作用于同一 call。
3. `acp` 28 个：新增 9 个 gate 单测覆盖 announcement 顺序、allow_once 的 id/选项/in_progress、reject_once 无执行更新、客户端 cancelled 结束 prompt、未知 option fail-closed、超时仅取消该请求、announcement 等待期间取消、pending 时 prompt 取消、writer 队列关闭。
4. `tests/acp_stdio.rs` 42 个真实进程 E2E：新增 allow_once（顺序、选项、同 id、副作用）、reject_once（不执行且模型收到拒绝原因）、客户端 cancelled（turn cancelled 且 session 可继续）、pending 时 session/cancel（不执行且 session 可继续）、delegation（仅父级询问、子 bash 无第二次请求）；既有 bash 用例改为 pending→in_progress，close/delete/cancel 用例容忍 `$/cancel_request`。

验收结果：

· `cargo fmt --check` 通过。
· `cargo clippy --workspace --all-targets -- -D warnings` 通过。
· `cargo test --workspace`：757 passed, 0 failed, 2 ignored（50 suites）。
· `cargo test -p jessica-cli --test acp_stdio`：42 passed。
· `cargo test -p jessica-cli --bin jessica-cli acp::tests`：28 passed；`governance` 17；`acp_events` 14。

偏离/说明：

1. 固定敏感集与 5 分钟答复期限按用户决定，不提供配置项；只实现一次性批准，不持久化、不提供 allow_always/reject_always。
2. 答复超时只失败当前工具并把错误返回模型；只有客户端 `cancelled`、prompt 取消、close/delete/disconnect 才结束整个 turn。
3. `delegate_task` 的子工具由 raw `NativeChildToolFactory` 构造，不复用父级 `GovernedTool`，因此不会二次询问；delegation 失败边界沿用既有实现。
4. 未修改 jessica-core（ACP-6a 已提供 `execute_with_call`）、jessica-memory 或 workspace Cargo.toml，未新增第三方 crate。
5. ACP `ToolCallStatus::Pending` 是协议默认值，wire 上省略 `status` 字段；测试与 README 均按“缺省即 pending”断言。

Task ACP-7b：session/list、delete、resume、close（2026-11-16）

目标：在 ACP-7a 的会话索引与 `session/load` 之上补齐剩余 ACP 会话生命周期方法：`session/list`、`session/delete`、`session/resume`、`session/close`。保持 jessica-core 公共 API 与 jessica-memory 不变，事件库保持 append-only，不新增第三方 crate，不实现 ACP-5/6、archive/fork/status/set_title、MCP、权限请求或 ACP v2。

实现范围与语义：

1. 能力矩阵：`initialize.agentCapabilities.loadSession` 与 `sessionCapabilities.list/delete/resume` 仅在 `[events].path` + `[acp].state_dir` 持久化成功打开时为 true/存在；`sessionCapabilities.close` 始终存在，因为 close 只释放内存 session，不需要索引。未配置持久化时 load/list/delete/resume 返回 `-32601`，close 仍可关闭 live session。
2. `session/list`：只枚举 canonical state_dir 下文件名 stem 为小写十六进制、内容通过 version=1/非空 id/绝对 cwd/非空 turn id 校验、且文件名与 session id 一致的普通 `.json` 索引；损坏、版本不符、错名、目录和符号链接跳过并 warn。结果再过滤为启动 workspace root 内的索引；可选 `cwd` 必须通过既有 absolute/exists/directory/root 校验并精确匹配。排序为 `updatedAt` 降序、session id 升序，页大小固定 100，`nextCursor` 为 `acp-list-v1:{updatedAtMillis}:{sessionId}` 的不透明游标，最大 512 字节；非法/超长游标返回 `-32602`。响应只含 `sessionId`、canonical `cwd`、UTC RFC 3339 毫秒 `updatedAt`，不返回 title、对话内容、turn id 或事件路径。
3. `session/resume`：要求持久化与索引存在；拒绝非空 `mcpServers`/`additionalDirectories`/`systemPrompt`（`-32602`）；cwd 必须与索引 canonical cwd 一致（`-32602`）；未知 id `-32002`；历史重建与 ACP-7a 的 load 完全相同（最新可恢复 turn、过滤 System、`complete_tool_pairs`、`trim_history`），但恢复后**不发送任何 replay notification**。已 live 的同 id + 同 cwd resume 幂等成功；cwd 不符仍 `-32602`。
4. `session/close`：不要求持久化。live session 先从 registry 移除，取消其 lifetime token，并通过每 session 的 `watch` 活动信号最多等待 5 秒让 active prompt 释放；确认后返回。配置持久化时索引保留，因此 close 后仍在 list 中且可 load/resume；对存在索引但未 live 的 session 幂等成功。无持久化或索引不存在且未 live 时返回 `-32002`。
5. `session/delete`：逻辑删除。先在 `AcpPersistence.mutation` 锁下删除索引，再移除 live session 并最多等待 5 秒；事件库行一律不删除、不修改。索引不存在但 session live 时删除仍成功；两者都不存在时 `-32002`。mutation 锁串行化 create/record/delete，确保删除开始后晚到的 prompt commit 只会看到索引缺失并 warn，不会重建索引。
6. 索引存储新增 `list`/`exists`/`delete`/`read_index`：`exists` 使用 `symlink_metadata` 且 NotFound 为 false；`delete` 使用 `remove_file` 且 NotFound 为 false/幂等；`read_index` 统一承担结构校验，load 仍对直接请求的损坏索引 fail-closed，list 才跳过损坏项。继续使用临时文件 + `write_all` + `sync_all` + `rename` 原子替换。
7. `AcpSession` 新增 `activity: watch::Sender<()>`；`finish_prompt` 清 active 后广播，`wait_idle` 先订阅再检查，避免 close/delete 与 prompt 完成之间的丢失唤醒。
8. JSON-RPC 错误映射：未知 session `-32002`；非法 cwd/cursor/lifecycle 字段 `-32602`；无持久化 `-32601`；索引读/写/删/列表内部失败 `-32603` 且消息固定为 `session load/list/delete/resume/close failed`，不泄露路径或事件内容；shutdown 期间返回既有 `agent is shutting down`。

行为测试（禁止自证式 Ok 断言）：

1. `acp_store` 新增 2 个，当前 9 个：list 只返回合法索引，跳过无关文件、损坏 JSON、版本不符与错名文件；delete 只删索引、幂等且不影响其他索引。
2. ACP 服务单元新增覆盖：持久化/非持久化 initialize 能力矩阵；list 游标版本前缀、最大长度与非法值；UTC RFC 3339 毫秒格式（含闰年/边界日期）；lifecycle 字段拒绝与错误映射。
3. `tests/acp_stdio.rs` 新增 8 个真实进程 E2E（总数 23→31）：list 只暴露元数据并跳过损坏索引；close 移出 live state、保留索引并可 reload；close 取消 active work 且等其停止后才响应；resume 恢复上下文但不 replay；delete 逻辑删除且事件库文件仍在；delete 取消 active work 后 state_dir 不被晚到 commit 重建；101 个 session 分页无重复/丢失（首页 100 + 剩余 1）；无持久化时 load/list/delete/resume 均 `-32601` 而 close 成功。
4. 既有 `new/prompt/cancel/load`、stdout 纯净、隐私、reload、shutdown 测试全部保留并通过。

真实客户端验收（2026-11-16 UTC）：

· 官方 TypeScript ACP SDK `@agentclientprotocol/sdk` 2.7.0（临时目录 `/tmp/jessica-acp7b-sdk`，不进入仓库、不作为依赖）：连接 `target/debug/jessica-cli` + 本地脚本化 OpenAI-compatible mock，验证 initialize 能力矩阵；两个 live session 的 list 元数据/精确 cwd 过滤/非法游标 `-32602`；close 后索引仍在 list 且可重复 close；delete 后索引消失、`events.db` 大小不变、load 返回 `-32002`；进程重启后 list 保留；resume 零 replay，且随后 prompt 的 provider 请求包含恢复前 user/assistant 上下文；live resume 幂等；再次重启后 load 在响应前 replay 首轮与 resume 后轮次，且不包含已删除 session 内容。
· Paseo 0.15.3：隔离 home `/tmp/jessica-acp7b-paseo/home`、daemon `127.0.0.1:6769`、从 `/home/ubuntu` 启动；workspace `/home/ubuntu/jessica-acp7b-paseo-workspace`；provider 为临时 wrapper，exec `target/debug/jessica-cli acp --config <harness>`，`supportsMcpServers=false`，env 指向本地 mock `127.0.0.1:46669`（不使用真实密钥/模型）。`provider diagnostic jessica` 为 Ready（ACP spawn/initialize/session/new/cleanup 全 ok）；首轮 `paseo run` 成功；`daemon stop/start` 后 agent 状态 closed，`agent send` 触发 `session/load`，logs 保留首轮且 provider 第二次请求包含首轮上下文，索引更新为 2 个 turn；`paseo delete` 后 ACP 子进程退出、索引仍保留 2 个 turn；随后 `paseo import <sessionId>` 成功恢复两轮历史且未新增 provider 请求。Paseo CLI 没有直接调用 listImportableSessions 的命令，list/delete/resume 的客户端行为由官方 SDK 覆盖；Paseo catalog probe 会创建若干空索引，属于既有 probe 行为。

验收结果：

· `cargo fmt --check` 通过。
· `cargo clippy --workspace --all-targets -- -D warnings` 通过。
· `cargo test --workspace`：713 passed, 0 failed, 2 ignored（50 suites）。
· `cargo test -p jessica-cli acp`：61 unit + 31 E2E 全通过。

偏离/说明：

1. close/delete 的 drain 上限为 5 秒；若 provider 或工具不响应取消，响应仍会返回，后台 worker 可能继续到自身退出。强保证需要另开 Task 设计强制终止与资源回收。
2. list 会列出所有有效索引，包括尚无 committed turn 的空会话；这类 session 在进程重启后无法 load/resume，但可 delete/close。Paseo catalog probe 产生的空索引因此会出现在 list 中；若需隐藏，必须单开 Task 定义“可恢复”过滤和 live 空会话语义。
3. load/resume 与 delete/close 之间没有全局请求串行化；mutation 锁只覆盖 create/record/delete 对索引的写。客户端不应并发对同一 session 发 load/resume 与 delete/close；若需线性一致性，须单开 Task 设计 per-session 状态机。
4. list 是目录扫描快照，不保证跨并发索引写入的原子性；分页游标基于 `(updatedAt, sessionId)`，跨页期间新增/删除会产生正常漂移，但不会因基于 offset 而重复或跳过同一稳定集合。
5. 时间格式未引入日期 crate，使用 Hinnant civil-date 算法；毫秒时间和本进程会话 id 足够稳定，若未来需要纳秒/时区扩展须单开 Task。
6. 未修改 jessica-core、jessica-memory、workspace Cargo.toml 或部署的 `~/.local/bin/jessica-cli`。

Phase 6 已知限制（同时记录到文末 Deferred）：

· 首期不是完全 ACP 合规：不支持 MCP stdio（v1 MUST）；非空 `mcpServers` 直接报错。
· 权限请求只覆盖固定敏感集 `bash`/`write_file`/`delegate_task`，且只提供一次性 `allow_once`/`reject_once`（无 allow_always/reject_always、无持久批准）；`read_file`、WASM 与 MCP 工具仍只受 AllowlistPolicy 约束。答复期限固定 5 分钟且无配置项；超时只失败当前工具，不结束 turn。子代理工具不单独询问，只受顶层 `delegate_task` 批准覆盖。客户端必须在取消 turn 时按 ACP 协议对 pending 请求回复 `cancelled`。
· 仅在同时配置 `[events].path` 与 `[acp].state_dir` 时支持 `session/load`/`list`/`delete`/`resume`；`session/close` 始终可用（只释放内存 session）。功能上线前创建的旧会话没有索引，不可恢复。
· `session/list` 只返回启动 workspace root 内通过校验的索引，并跳过损坏、版本不符、错名或非普通文件；响应只含 `sessionId`、canonical `cwd`、`updatedAt`，不返回 title 或对话内容。页大小固定 100，游标格式版本化为 `acp-list-v1:...`；尚无 committed turn 的空会话也会被列出，但进程重启后无法 load/resume。
· `session/delete` 只删 ACP 索引并释放 live session，不物理删除 append-only 事件库；删除后该 id 不再 list/load/resume，也没有事件空间回收。若需要隐私删除或 GC，必须单开 Task 设计 tombstone/引用策略。
· `session/close` 与 `session/delete` 会先取消 active prompt，再最多等待 5 秒；provider 或工具不响应取消时，响应仍会返回，后台 worker 可能继续到自身退出。`load`/`resume` 与 `delete`/`close` 之间没有全局请求串行化，客户端不应并发对同一 id 发互斥生命周期请求。
· 恢复以最新可恢复 turn 的事件流为快照；最新 turn 缺失时回退到更旧 turn，因此可能丢失其后已提交的对话。索引追加与事件提交不是跨存储事务，进程崩溃可能使索引少记录一个 turn（可回退）或记录到尚未完整落盘的事件流（恢复失败后继续回退）。
· ACP 索引没有跨进程文件锁；同一 `state_dir` 不应被多个 ACP 进程同时写入，否则后写的 turn 列表可能覆盖先写的内容。list 是目录扫描快照，跨页期间并发写入会造成正常漂移。
· cwd 必须在启动 workspace root 内；memory/recorder/skills/wasm 仍是进程级作用域，不支持多 workspace。
· ACP 首期禁用 acceptance 重试；与 REPL/one-shot 的行为不同。
· 每 turn 记录完整历史，事件库存储随对话增长呈 O(n²)；后续可改为增量 + 引用。
· EventPump 的 broadcast lag 仍是风险；ACP 路径必须把 lag 当错误，不能静默丢文本。
· 不支持 ACP v2 alpha；不保证与所有客户端/所有 v1 次要版本完全互通，需要真实客户端手工验收，至少覆盖 Paseo。

---

Phase 7：交付地基（2026-11-17 新增；仅 7.0a/7.0b/7.0c）

背景：Phase 0–6 与 ACP-0..7b 已完成，功能面已足够。当前主要风险从“缺功能”转为“不可交付、不可复现、不可信”：仓库没有 CI；8 个 crate 版本仍为 `0.1.0`，而 tag 已到 `v0.9.0`；二进制名为 `jessica-cli` 且没有 `--version`；发布只能本地编译；README 引用的 `DESIGN.md` 仍未入库。Phase 7 先只做三个不修改 `jessica-core` 的交付 Task；Trace Integrity、执行安全与能力扩展全部暂缓，记录到文末 Deferred，由用户单开 Task。

Phase 7 全局约束：

· 三个 Task 均不得修改 `jessica-core` 的公共 API；不得提前实现 Deferred 中的 Trace Integrity、安全加固或能力扩展。
· 需要新增第三方 crate 或其他依赖（如 GitHub Action）时，先在 Task 中说明用途、版本与替代方案，并由用户批准；不得顺手升级无关依赖。
· 每个 Task 独立验收；验收命令失败不得声明完成；完成后更新进度追踪与偏离说明。
· 不顺手重构、不改 ACP 协议行为、不改现有测试语义来“让 CI 变绿”。

依赖关系：7.0a 可独立执行；7.0b 不依赖 7.0a；7.0c 依赖 7.0b 的版本号与二进制命名。建议按 7.0a → 7.0b → 7.0c 顺序执行。

Task 7.0a：CI 基线

目标：把现有测试套件变成 push/PR 的强制门禁，防止后续改动回归；不引入任何 secrets 或外部服务依赖。

操作：

1. 新建 `.github/workflows/ci.yml`（仓库托管在 GitHub；若用户实际使用其他平台，则创建等价配置，不新增付费服务）。
2. 触发条件：push 到 main、pull_request、workflow_dispatch；同一分支/PR 的并发运行取消旧任务。
3. 步骤：
   · checkout；
   · 按 `rust-toolchain.toml` 安装工具链（含 rustfmt/clippy 组件）；
   · 缓存 Cargo registry/git 与 target（key 含 `Cargo.lock`、工具链版本、OS）；
   · `cargo fmt --check`；
   · `cargo clippy --workspace --all-targets -- -D warnings`；
   · `cargo test --workspace --locked`；
   · `sh -n install.sh`。
4. 不配置 API key；测试必须继续使用现有 mock/scripted provider。若测试需要 `wasm-tools`，确认仓库中的预编译 `.wasm` 足够，不在 CI 中临时安装未固定版本的工具。
5. 在 workflow 注释或 README 中写明本地等价命令。

验收：

· PR 上 CI 全绿；日志中 `cargo test` 为 0 failed（测试总数可随代码变化）。
· 本地故意加入一个失败断言并推送临时分支，CI 必须红；回滚后必须绿（不把该提交合入 main）。
· `cargo fmt --check`、clippy、test 与本地执行结果一致；`sh -n install.sh` 通过。

禁止：

· 不设置 `continue-on-error`；不跳过 clippy/test；不通过删改测试让 CI 变绿；不引入需要 secrets 的步骤；不在 CI 中访问真实模型 API。

Task 7.0b：版本与二进制命名统一

目标：让 crate 版本、tag、安装产物和用户命令名一致；提供真实 `--version`。

现状：8 个 crate 全部 `version = "0.1.0"`，tag 已到 `v0.9.0`；可执行文件名为 `jessica-cli`；自定义参数解析没有 `--version`；Paseo launcher 自行应答 `--version`。

操作：

1. 版本单一来源：workspace 使用 `[workspace.package] version = "0.10.0"`（最终版本号在 Task 开始时由用户确认），成员 crate 改为 `version.workspace = true`；更新 `Cargo.lock`。
2. 二进制命名：在 `jessica-cli/Cargo.toml` 增加 `[[bin]] name = "jessica"`。兼容策略二选一并在 Task 记录中选择：
   · 推荐：安装 `jessica` 主二进制，同时由 `install.sh` 创建 `jessica-cli` 兼容符号链接（或副本），至少保留一个发布周期；
   · 或全量改名，并同步更新 `install.sh`、README、Paseo launcher、examples、测试与脚本，提供迁移说明。
3. 在 `jessica-cli/src/main.rs` 的自定义参数解析中识别 `--version`/`-V`，输出 `jessica <CARGO_PKG_VERSION>` 并退出 0；不新增 clap 依赖。
4. 更新 `install.sh`：安装目标名改为 `jessica`；`--paseo` 生成的 launcher 调用 `jessica acp ...`；保留自定义 launcher 的 `--config` 注入与重复检测逻辑。
5. 对 `install.sh` 生成的 Paseo launcher，`--version` 不应再硬编码版本，应执行 `jessica --version`；保留的用户自定义 launcher 不覆盖，只在 README 说明版本探测要求。更新 README、示例命令和测试中的二进制路径。
6. 不修改 `jessica-core`；不自行打 tag 或发布 release。

验收：

· `cargo build --workspace --locked` 通过；`cargo run -p jessica-cli --bin jessica -- --version` 输出 `jessica 0.10.0` 且退出码 0。
· 临时 HOME 安装后 `~/.local/bin/jessica --version` 正确；若保留兼容入口，`jessica-cli --version` 输出一致。
· `./install.sh --paseo` 在临时 HOME 下生成的 launcher 使用新命令；真实 Paseo 的 `paseo provider diagnostic jessica` 仍为 Ready（如环境可用，人工验收）。
· `cargo test --workspace --locked` 全绿。

禁止：

· 不硬编码版本号到多个文件；不删除 `jessica-cli` 兼容入口而不提供迁移说明；不改 ACP 协议行为；不改 `jessica-core` 公共 API；不在未询问用户时打 tag。

Task 7.0c：发布自动化与仓库卫生

目标：让用户不依赖本地 Rust 工具链即可安装；让 README 引用的文件真实存在；保留本地编译安装路径。

操作：

1. 新增 tag 驱动的 release workflow：`v*` tag 触发，构建 release 二进制（至少 Linux x86_64；条件允许时增加 aarch64/macOS），生成 SHA-256 校验和，创建 GitHub Release 并上传产物。
2. `install.sh` 增加下载安装模式（如 `--from-release`）：按平台选择产物，下载后校验 SHA-256，校验通过再原子安装；网络失败时给出清晰错误并提示 `./install.sh` 本地编译。保留现有 `--paseo` 集成。
3. 确保 `DESIGN.md` 已入库且 README 第 19 行的引用有效；若仍未入库则提交。若设计文档暂不公开，则修改 README 删除引用；二选一，必须让链接有效。
4. 处理 README 首行 `determinstic`：把 acceptance 夹具移到独立文件（如 `examples/typo.md`），修正 README，并更新 `examples/harness.toml` 指向新夹具，保证“首次验收失败→重试→修正”的演示仍可复现。
5. 可选：增加 `CHANGELOG.md` 或 release notes 模板；不强制。

验收：

· 在没有本地 Rust 构建产物的临时目录中，用 release 产物安装成功；`jessica --version` 正确；篡改产物后校验失败并中止安装。
· `git status` 干净；README 的 `DESIGN.md` 链接有效；`examples/harness.toml` 的 acceptance 演示仍能失败→重试→通过。
· `sh -n install.sh`、`cargo fmt --check`、clippy、test 全绿。

禁止：

· 不跳过校验和；不把未签名产物说成已签名；不删除本地编译安装路径；不提交 `target/`、临时文件或无关改动；不在未询问用户时打 tag/发布 release。

Phase 7 完成标准：7.0a/7.0b/7.0c 全部勾选，PR 上 CI 全绿，安装产物可复现；Trace Integrity 等后续项仍留在 Deferred，不得在本 Phase 顺手实现。

---

Phase 7.1：Trace Integrity 无损记录器（2026-11-18 新增；Task 7.1）

背景：Phase 7 的 Deferred 把 Trace Integrity v2 列为 P1 候选。用户在本 Task 开始前确认：采用新增 `TraceEvent` 包络（不改冻结的 `AgentEvent`），一次性完成核心包络 + 存储（A）与 CLI 编排（B），并把 trace、legacy 事件、`sessions` 元数据写入全部纳入 fail-closed；启用 recorder 时 broadcast lag 必须写 gap、取消 turn 并以非零退出，未启用 recorder 时保留既有 warn-and-continue。

Task 7.1：无损记录器与 turn 完整性不变量

目标：在持久化启用时，让“turn 成功返回 ⇒ 轨迹完整持久化”成为可验证的不变量；不完整轨迹必须显式标记为未关闭，而不是被当作完整记录。

操作：

1. `jessica-core` 新增 additive `trace` 模块：`TRACE_SCHEMA_VERSION=1`、`TurnId`、`TraceEvent`（schema_version/turn_id/attempt/session_id/payload）、`TracePayload`（Agent/Gap/TurnStarted/TurnEnded）、`TraceTurnMode`、`TraceGapReason`。既有 `AgentEvent` 与全部已冻结契约保持不变。
2. `jessica-memory` 新增 `trace_events`/`trace_heads` 表与 `PRAGMA user_version` 0→1 迁移；未来版本 fail-closed。每次 append 在一个 SQLite 事务中同时写入可选 legacy 事件与 trace 条目；`trace_heads` 提供 O(1) 序号分配，append-only trigger 禁止修改/删除。读取时校验序号连续、首条 `turn_started`、`turn_ended` 之后无条目、head 的 `closed` 与终态一致；不完整轨迹可读且 `closed=false`。
3. CLI 在每 turn 生成 `TurnId`，所有 acceptance attempt 共享同一 turn；`begin_attempt` 记录 attempt→session 归属。`AcceptanceFailed` 只写 trace、不写 legacy（否则会落在该 attempt 的 `SessionEnded` 之后，破坏 recovery/replay），归属使用 payload 的 attempt 号而非 pump 处理时序。
4. fail-closed：`sessions` 元数据、trace 写入或 legacy 镜像任一失败即停止后续写入、取消 agent、不写 `turn_ended`，turn 返回 `CliError::TraceIncomplete`。正常返回（含预取消 turn）必须有 `TurnStarted` 与 `TurnEnded`。
5. broadcast lag：存在 recorder 时写入 `Gap{BroadcastLagged}`、取消 turn、禁止 `TurnEnded`，进程以非零退出；不存在 recorder 时保持既有 HumanConsole warn-and-continue / Silent 行为。
6. `--resume` 写入 `TurnStarted{mode=Resumed}`，初始消息取 checkpoint 消息，不重放已恢复的工具批次。

验收：

· `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。
· 行为测试覆盖：完整轨迹无损读取与 legacy 镜像一致；metadata/trace/legacy 注入失败时轨迹不关闭且无 `TurnEnded`；acceptance 重试的每个失败归属被判 attempt，失败事件只在 trace；lag 写 gap 并阻止关闭；v0 迁移与未来版本拒绝；append-only trigger；resume 轨迹为 Resumed 且工具批次不重放。
· 成功 turn 的 trace 首条必须是 `TurnStarted`、末条必须是 `TurnEnded`，否则测试失败。

禁止：

· 不修改 `AgentEvent` 或既有 EventStore 公共语义；不实现事件模型 v2（user/usage/round/permission/子会话）、EventStore v2 WAL/增量历史、trace verify CLI 或 replay parity；不新增第三方 crate；不顺手重构或改变 legacy recovery/replay 语义。

Task 7.2：生产者侧无损记录器

目标：把持久化从有损的 `EventPump` 消费侧移到事件生产侧。持久化启用时，“turn 成功返回 ⇒ 该 turn 的全部生产事件与 `TurnEnded` 已在返回前提交”成为可验证不变量；广播滞后只影响观察者，不再决定 trace 是否完整。

操作：

1. `jessica-core` 新增 additive `events` 模块：`EventSink`（异步 `emit(AgentEvent)`，满时必须等待容量而非丢事件）与结构化 `EventSinkError`；`lib.rs` 仅新增 `pub mod events;`。该 additive core 变更已由用户在设计确认时明确批准，其余冻结契约不变。
2. `AgentLoop` 增加可选 `EventSink`：每个事件先 `emit`，成功后才广播；sink 拒绝时停止 run，被拒绝事件及其后的终态事件都不得广播。`AcceptanceRunner` 同样在广播 `AcceptanceFailed` 前 `emit`，拒绝即停止重试并按 `Error` 返回。
3. CLI 新增 turn 级 `TurnJournal`：单写任务、容量 1024 的有界队列、`try_send` 快路径、队列满时 `send().await` 背压（即使 observation gap 已取消 token 也等待容量，不丢生产尾部）；写任务按 FIFO 调用 `EventJournal::record_traced`，在单个 SQLite 事务中写 legacy + trace。`begin_turn`、session 元数据、outer session 事件对与 `finish_turn` 使用 acked 命令；`finish_turn` 只有在全部先前事件与 `TurnEnded` 提交成功后才返回成功。
4. 首个写失败保留原始原因、停止写入、取消 agent，并通过 `watch` 通知 attempt 编排；非停止型失败（观察者 lag）允许继续记录生产侧尾部，但所有生命周期命令与 `finish_turn` 仍拒绝关闭。`flush` 在失败后仍可等待已入队事件落盘，用于保留完整前缀/尾部。
5. `EventPump` 降为纯观察者：不再写 trace、legacy 或 `Gap`。存在 recorder 时 lag 仍失败该 turn（transcript/sensor/hook 不完整），但生产者 trace 保留且 `closed=false`；不存在 recorder 时保持既有 Silent 失败 / HumanConsole warn-and-continue。
6. 不修改 `AgentEvent`/`TraceEvent` schema，不新增 SQLite 迁移；`TracePayload::Gap`/`TraceGapReason::BroadcastLagged` 保留定义，但 CLI 不再产生 gap 条目。

验收：

· `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。
· 行为测试覆盖：成功 turn 的写入顺序与 FIFO（`TurnEnded` 最后）；写失败后拒绝后续事件、取消、无 `TurnEnded` 且轨迹保持未关闭；小容量 + 阻塞写任务下生产者背压且零丢失；acceptance 失败的 attempt 归属与 trace-only 语义；outer session 元数据与事件对先落盘再广播；关闭后拒绝迟到事件且不污染已提交轨迹；lag 后仍记录生产侧尾部、不写 `Gap`、不能关闭；runtime/harness 验证 sink 先于广播且拒绝后不广播、不重试。
· 无 recorder 的 lag 行为与既有测试保持一致。

禁止：

· 不实现 EventStore v2/WAL/fsync 级掉电保证、子 Agent 轨迹、trace verify/replay parity、trace 保留/GC、hook/UI fan-out 的无损化；不新增第三方 crate；除已批准的 additive `events` 模块外不修改 `jessica-core` 冻结 API；不顺手重构或改变 legacy recovery/replay 语义。

Task 7.3：事件模型 v2（round/usage/permission/child/session input）

背景：Task 7.1/7.2 的 trace 只有有损的 legacy 事件词汇，无法证明一次模型调用的轮次、usage、权限决策或子会话生命周期。2026-11-19 用户确认在 Task 7.3 中一次性补齐这些事实，并保持 v1 轨迹与旧事件流可读。

目标：在不破坏 v1 读取与既有 replay/resume 语义的前提下，让 runtime 的模型轮次、token usage、权限决策和父侧子会话生命周期成为可验证、可持久化的显式事实；bootstrap 消息优先由事件本身携带。

操作：

1. `jessica-core` additive 扩展（用户已批准）：`SessionStarted` 增加 `messages`（serde default，空值省略）；新增 `RoundStarted{round}`、`RoundFinished{round,usage}`、`PermissionDecided{call,decision}`、`ChildSessionStarted{child_session_id,call_id}`、`ChildSessionEnded{child_session_id,call_id,reason,rounds,tool_calls,tool_errors}` 与 `PermissionDecision`；`AgentEvent::required_trace_schema_version` 对 v2-only 词汇返回 2。`TRACE_SCHEMA_VERSION` 升为 2，新增 `MIN_SUPPORTED_TRACE_SCHEMA_VERSION=1` 与范围判断；`TraceEvent::new` 写 v2，包络字段与 payload 变体不变。
2. round 语义：一轮 = 一次模型调用。`AgentLoop` 在调用 provider 前发 `RoundStarted`；模型响应完成后、执行工具前发 `RoundFinished{usage}`；工具调用仍为 `ToolCallStarted`/`ToolCallFinished`。失败/取消的模型调用不发 `RoundFinished`。`--resume` 的轮次从恢复的 `rounds_completed` 继续编号。
3. trace 兼容：v1 轨迹可读；v1 包络拒绝任何需要 v2 的 payload；同一 turn 内混用 schema 版本在 append 与读取两侧都 fail-closed；v3+ 拒绝。`SessionStarted.messages` 非空即要求 v2。
4. bootstrap 迁移：CLI `EventRecorder::load` 优先使用非空 `SessionStarted.messages`，为空时回退 `sessions.messages_json`；两者同时存在且不一致时 `RecorderError::Integrity`。resume/replay 继续双写，7.3 不删除元数据。
5. 权限：`GovernedTool` 在工具执行/拒绝前发 `PermissionDecided`；Allow 的 sink 录制失败必须阻止工具执行并取消 turn；Reject/Cancelled/TimedOut/TransportFailed 分别映射为 Deny/Cancelled/TimedOut/Failed。
6. 子会话：`AgentChildExecutor` 通过父侧 sink/广播发 `ChildSessionStarted`（子模型调用前）与 `ChildSessionEnded`（完成或超时）；start sink 失败不启动子会话，end sink 失败把 delegation 变为 error result；子 Agent 内部事件仍私有、不持久化。
7. 持久化/回放：`PermissionDecided` 与两个 child 生命周期事件只写 trace、不写 legacy mirror，因为 replay 不会重跑权限门或子会话，写入 legacy 会破坏既有“replay 输出等于 legacy 事件流”的契约；它们仍归属父 turn 当前 attempt/session。replay 在有显式 round 时按记录边界重建，无显式 round 时保留 v1 文本/工具启发式；v1 源不合成 round 事件、不带 `SessionStarted.messages`，usage 为 `None`。recovery 优先使用显式 round 和事件内 messages，v1 流保持原启发式。
8. CLI pump/journal/ACP：新事件参与 transcript 轮次渲染；ACP 不新增 update；journal 对 trace-only 审计事实显式归属；不改变既有 SessionEnded/recovery 顺序。

验收：

· `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿（2026-11-19：812 passed, 0 failed, 2 ignored）。
· 行为测试覆盖：v2 事件序列、round/usage 传播；v1 trace 可读、v1 拒绝 v2-only payload、mix 版本拒绝、v3 拒绝；event-first messages/元数据回退/冲突 fail-closed；explicit round 正常与畸形序列；usage 不影响恢复；权限五种决策的录制顺序与 Allow 录制失败不执行；child start/end 的 sink/广播、start 拒绝不调用模型、end 拒绝转 error；v1 replay 启发式与 `Done{usage:None}`；legacy replay/delegation E2E 保持逐字节可复现。

禁止：

· 不实现 EventStore v2/WAL/fsync 掉电保证、trace verify CLI、完整 replay parity、子会话内部持久化、trace 保留/GC、权限持久化 v2；不新增第三方 crate；除本 Task 明确列出的 additive core 变更外不修改冻结 API；不顺手重构或改变既有 v1 legacy recovery/replay 语义。

Phase 7.4：Trace 验证 CLI 与 trace↔legacy 镜像一致性（2026-11-19 新增；Task 7.4）

背景：Task 7.1–7.3 已让 trace 无损记录结构、round/usage/权限/子会话事实，但没有离线验证入口，也没有证明 trace 声称的 legacy 镜像确实存在。2026-11-19 用户确认将 Task 7.3 的 Deferred 项“trace verify CLI”作为独立 Task 7.4 执行；方案先经用户确认。

目标：提供只读的 `jessica trace verify`，对单个 turn 或全部 turn 验证结构完整性、v2 语义和 trace↔legacy 镜像一致性；不执行 replay，不创建/迁移数据库，不初始化 provider/wasm/memory/skills/runtime。

操作：

1. CLI：新增 `trace verify --turn <turn_id> [--json]` 与 `trace verify --all [--json] [--allow-open]`；`--turn` 与 `--all` 互斥且必须二选一；`--allow-open` 只允许与 `--all` 组合。`[events].path` 必填，相对路径按当前工作目录解析。校验路径不安装 schema，只复用 `EventStore::trace_turn`/`trace_turns`/`events`。
2. `jessica-memory` additive：新增 `EventStore::open_read_only`（`SQLITE_OPEN_READ_ONLY`，不创建文件、不迁移、不修改）与结构化 `EventStoreError::TraceSchemaMissing`；不改 SQLite schema、不改既有 API 语义。
3. 结构层：`trace_turn` 的 `TraceSequence`/`TraceInvalid`/`TraceLifecycle`/`TraceDecode` 记为 invalid（`trace_structure`），不中断 `--all`；存储/SQLite 错误仍使命令以错误退出。无 `TurnEnded` 且无问题记为 `open`。
4. v2 语义：`SessionStarted`/`SessionEnded` 内部 session id 必须匹配包络；`RoundStarted`/`RoundFinished`/`PermissionDecided`/`ChildSession*` 必须有 attempt+session 归属；按 `(attempt, session)` 校验 round 不嵌套、后一轮严格 +1、resumed 首轮可 >1、`RoundFinished` 必须匹配打开轮、`ModelDelta` 必须在打开轮内、`ToolCallStarted` 必须在轮结束后、`Completed` 的 `SessionEnded` 不得遗留打开轮；`PermissionDecided` call id 非空，正常 `Completed` 下 Allow/Deny 必须有匹配 `ToolCallFinished`（取消/异常终止可缺失）；child 按 `(child_session_id, call_id)` 配对，禁止重复 start/无 start 的 end，`Completed`/`AcceptanceFailed` 不得遗留打开 child，`tool_errors <= tool_calls`。
5. 镜像层：除 `AcceptanceFailed`/`PermissionDecided`/`ChildSessionStarted`/`ChildSessionEnded` 外，带 session 的 AgentEvent 必须按 `(session_id, timestamp, payload_json)` 与 legacy 行做多重集匹配；resumed session 的额外 legacy 前缀不影响匹配。
6. 输出/退出码：文本输出每个 turn 一行（status/schema/entries/problems）及 summary；`--json` 输出稳定结构 `{report_version, turns:[{turn_id,schema_version,entries,status,problems:[{seq,kind,message}]}], summary:{turns,ok,open,invalid,problems}}`。`0` 表示全部通过（`--allow-open` 时 open 也通过）；`1` 表示 invalid、未允许的 open、未知 turn 或存储/配置错误。`--allow-open` 不放宽 invalid。

测试：memory 新增 4 个只读行为测试（读取完整 trace、拒绝写、缺失文件不创建、无 trace schema 不迁移、未来版本不改写）；CLI verifier 新增 13 个纯行为测试（完整 v2、round 序号/嵌套/0、delta/tool 相位、session 身份、permission 解析、child 生命周期、v1 跳过 v2、gap、open/invalid 优先级、镜像多重集、trace-only、allow-open 只放宽 open）与 3 个参数解析测试；E2E 新增 7 个（JSON ok/open/gap/corrupt、allow-open、单 turn 与未知 turn、空库、不创建数据库、无 schema 不修改、缺 `[events]`）。测试使用真实 SQLite 与真实 CLI，不访问网络。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。

禁止：不实现完整 replay execution parity、EventStore v2/WAL/fsync、trace 保留/GC、子会话内部轨迹持久化、权限持久化 v2；不修改冻结 `jessica-core` 公共 API；不新增第三方 crate（测试仅使用 workspace 已有 `rusqlite` 作为 CLI dev-dependency）；不在 async 中阻塞；不顺手重构既有 recovery/replay 语义。

Phase 7.5：Trace replay execution parity（2026-11-19 新增；Task 7.5）

背景：Task 7.4 的 verify 只证明 trace 结构与 legacy 镜像，不证明 trace 声称的事件流真的可以由 `AgentLoop` 在相同输入下复现。2026-11-19 用户确认将 Task 7.4 Deferred 的“完整 replay execution parity”作为独立 Task 7.5 执行；方案先经用户确认。

目标：为 `jessica trace verify --turn` 增加 opt-in `--replay`，用 trace 自身的 `SessionStarted.messages` 与已记录运行事件重建每个 fresh attempt，驱动既有确定性 replay engine，逐事件比较 `AgentLoop` 可复现事件；不修改生产记录、不访问 provider/网络、不执行真实工具。

操作：

1. CLI：`trace verify --turn <turn_id> [--json] [--replay]`；`--replay` 只能与 `--turn` 组合，`--all --replay` 明确报错。报告 `report_version` 1→2；每个 turn 增加 `replay: null | {status, attempts, events, reason?}`，文本输出增加 replay 摘要。
2. 可重放子集（其余一律 unsupported、exit 1，不得静默跳过）：closed v2 fresh turn、无 `Gap`、attempt 从 1 连续、每个 attempt 恰好一个 session、`SessionStarted` 首条且 `SessionEnded` 末条、attempt 终态为 `Completed`/`Error` 或仅 `MaxRounds` 的 `BudgetExceeded`。v1、Resumed、Cancelled、AcceptanceFailed、缺失 start/end、非连续 attempt、非 `MaxRounds` budget → unsupported。
3. 过滤：只比较 `AgentLoop` 可复现事件（`SessionStarted`、`RoundStarted`/`RoundFinished`、`ModelDelta`、`ToolCallStarted`/`ToolCallFinished`、`Error`、`SessionEnded`、`MaxRounds`）；排除 trace-only（`PermissionDecided`、`ChildSessionStarted`/`ChildSessionEnded`、`AcceptanceFailed`）与 CLI governance（`PolicyDenied`、非 `MaxRounds` 的 `BudgetExceeded`）。每次 attempt 以 `SessionStarted.messages` 为初始对话，录制的 `ToolCallFinished` 作为 replay tool 结果。
4. 比较：按原 trace seq 逐事件相等；长度或首个差异生成 `replay_mismatch`（含 seq 与截断 JSON），turn 置 invalid、exit 1。replay 自身脚本错误按 mismatch 处理；`EventLag` 记 unsupported。外层 session（若存在）静态校验为空 messages 的 `SessionStarted`+`SessionEnded` 且终态等于 `TurnEnded`；无外层时最后一个 attempt 的终态必须等于 `TurnEnded`。
5. 保持只读：数据库仍经 `open_read_only` + `spawn_blocking`；不修改 SQLite schema、`jessica-memory`、既有 `replay --session` 行为或冻结的 `jessica-core` API；不新增第三方 crate。

测试：`trace_replay` 新增 11 个行为测试（Completed 逐事件、governance/trace-only 过滤、MaxRounds、provider Error、篡改 budget 定位 seq、终态不一致、acceptance 双 attempt + 外层校验、外层 reason 不一致、v1/Resumed/Cancelled/非 MaxRounds、缺失 start/非连续 attempt）；CLI 参数测试新增 `--replay` 接受与 `--all --replay` 拒绝；E2E 新增 4 个（可重放 trace 的文本/JSON 成功、非 parity trace 变 invalid、v1 unsupported 明确报错、batch 拒绝），并更新既有 `report_version` 断言为 2。测试使用真实 SQLite 与真实 CLI，不访问网络。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。

禁止：不实现 `--all --replay` 批量 parity、EventStore v2/WAL/fsync、trace 保留/GC、子会话内部轨迹持久化、非 `MaxRounds` 预算重放、Cancelled/Acceptance 外部编排重放；不修改冻结 `jessica-core` 公共 API；不新增第三方 crate；不在 async 中阻塞；不顺手重构既有 replay/recovery 语义。

---

Phase 8：可靠性与可验证性（2026-11-19 新增；Task 8.1、8.2a、8.2b、8.3、8.4、8.5、8.6 已完成；8.7 待逐项确认）

背景：Task 7.5 已交付单 turn replay parity，但 2026-11-19 的 main CI run 44431198725 暴露 shutdown 诊断行与进程退出的竞态；同时 EventStore 尚未启用 WAL、没有事务化迁移框架、trace 不具备防篡改摘要、验证不能离线携带、replay 不支持批量/Resumed/v1。用户确认先做 8.1，随后确认 8.2 方案并要求拆成 8.2a（WAL 与事务化迁移）和 8.2b（只读审计契约与崩溃/磁盘满测试），再确认 8.3（trace 哈希链、`trace digest`、`verify --expect`）、8.4（可移植证据包 export 与离线 verify）与 8.5（`--all --replay` 及 v1/Resumed 覆盖扩展），其余按依赖顺序逐 Task 确认。用户于 2026-11-19 对 8.6 先选择“先只保留设计，不实现”，随后确认“继续Task 8.6”并批准冻结 API 变更，Task 8.6 已完成。

Task 8.1：修复 shutdown 诊断竞态（2026-11-19）

目标：保证第一条 SIGINT/SIGTERM 诊断行在 shutdown 状态对主流程可见之前写入 stderr；不改变退出码和关闭语义。

操作：

1. `ShutdownController` 移除有副作用的 `on_signal`，新增只读 `classify_next() -> SignalAction`；调用方先分类、写诊断并 flush stderr，再调用 `request_shutdown`（第一条）或 `force`（第二条）。
2. `handle_signal` 按上述顺序执行；强制退出仍先打印再 `process::exit`；`Ignore` 不写日志。
3. 模块文档明确“先写诊断、后提交状态”的顺序；更新 shutdown 单元测试，断言分类阶段不记录 signal、不取消 token、不置 forced。
4. 新增真实进程压力测试 `every_idle_sigint_writes_its_diagnostic_before_exiting`：重复 20 次 idle REPL + SIGINT，每次断言 exit 130、stderr 含诊断行、无 panic；原 idle 测试抽取 `run_idle_sigint` helper 复用。

测试：shutdown 单测 5 个；shutdown E2E 5 个（原 4 + 压力 20 轮）。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`；main/PR 的 GitHub Actions 全绿。

禁止：不改变 SIGINT=130/SIGTERM=143/第二次信号强制退出语义；不新增信号；不修改 `jessica-core`、`jessica-memory` 或 SQLite schema；不新增第三方 crate；不在本 Task 顺手实现 8.2–8.5。

Task 8.2a：EventStore WAL 与事务化迁移（2026-11-19 确认，8.2 拆分）

目标：为 EventStore 启用 WAL 与 `synchronous=FULL`，并把 schema 安装改为事务化迁移；只读审计契约留给 8.2b，不改变 CLI 命令面或冻结 API。

操作：

1. `EventStore::initialize` 先读 `user_version`，未来版本直接拒绝且不写库；随后设置 `PRAGMA journal_mode=WAL`（文件库必须返回 `wal`，内存库接受 `memory`，否则返回结构化错误）与 `PRAGMA synchronous=FULL`。
2. 用 `migrate` 取代 `install_schema` 的非事务安装：`user_version < SCHEMA_VERSION` 时在单个 SQLite transaction 内执行迁移 DDL 并写 `PRAGMA user_version`，失败整体回滚。`SCHEMA_VERSION` 仍为 1；迁移 1 = 现有 legacy `SCHEMA` + `trace_store::TRACE_SCHEMA`；v1 库保持现有幂等 `IF NOT EXISTS` 安装行为，避免回退。
3. 文档说明 WAL sidecar、clean close checkpoint、单写者模型和 `synchronous=FULL` 的耐久性含义。
4. 不修改 `open_read_only` 的 immutable/只读目录行为（8.2b），但确保现有 read-only 测试在 clean WAL 库上仍通过。

测试：WAL 模式（文件库 `wal`、内存库 `memory`）、`synchronous=FULL`、clean close 无 `-wal`/`-shm`、v0→v1 保留事件、v1 幂等、未来版本不写库即拒绝、失败迁移回滚 DDL 与 `user_version`；现有 workspace 测试全绿。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿；PR CI 全绿。

禁止：不实现 8.2b 的 immutable 只读 URI、pending WAL 拒绝、SIGKILL/磁盘满测试；不 bump schema 版本、不加表/索引；不改 `jessica-core`/CLI/trace 语义；不新增第三方 crate；不把 EventStore 改成异步。

Task 8.2b（2026-11-19 确认并完成）：只读审计契约与崩溃/磁盘满测试

- `open_read_only` 对非空 `-wal` 返回结构化错误；对 clean 库用 `immutable=1` URI 打开，支持只读目录且不产生 sidecar。
- SIGKILL 提交恢复、磁盘满回滚、chmod 0555 只读目录与 URI 分隔符路径测试。

Task 8.3（2026-11-19 确认并完成）：trace 哈希链、`trace digest` 与 `verify --expect`

目标：为每个 turn 的已存 trace 建立可重算的 SHA-256 哈希链，提供只读 digest 输出与外部摘要锚定，使 trace 字节级篡改可被发现；不改变追加速度、不引入签名或新依赖。

操作：

1. `jessica-memory`：新增 `trace_chain(turn_id, seq, prev_hash, entry_hash)` 表（`WITHOUT ROWID`、长度 `CHECK`）及 UPDATE/DELETE 拒绝触发器；`SCHEMA_VERSION` 1→2。迁移 2 在单个事务内建表并按 `(turn_id, seq)` 顺序回填全部既有 `trace_events`，每条 turn 从 32 零字节 genesis 起链；失败整体回滚，`user_version` 不前进。普通 reopen 的 `repair_schema` 只补建缺失结构（`IF NOT EXISTS`），绝不重算或回填链数据，缺失链保持可检测。
2. 哈希格式：领域分离 + u64 big-endian 长度前缀；`entry_hash = SHA-256(entry domain || prev_hash || turn_id || seq || schema_version || payload_kind || optional(event_type) || optional(attempt) || optional(session_id) || timestamp || payload_json)`，`optional(None)` 与 `optional(Some)` 使用不同存在标记；genesis 为 32 零字节；`digest = SHA-256(digest domain || turn_id || last_entry_hash)`，输出小写 hex。追加在同一事务写 `trace_events` + `trace_chain` + `trace_heads`；既有 turn 追加前读取最后链节并要求 `seq == head.last_seq`。
3. `trace_turn` 在既有结构/生命周期校验全部通过后验证链：链节数等于事件数、seq 对齐、prev 链接一致、从已校验 envelope 重算 entry hash 一致；任一不符返回 `TraceInvalid`。`StoredTraceTurn::digest()` 暴露该 turn 的 digest。
4. `open_read_only` 要求 schema v2：v1 返回可操作的 `TraceSchemaOutdated { path, found, required }`，提示先用当前读写构建迁移一次；只读路径仍不迁移、不建 sidecar。
5. CLI：新增 `trace digest --turn <id> | --all [--json]`；单 turn 文本只输出 digest，`--all` 按 turn_id 升序输出 `turn_id<TAB>digest`；digest JSON 为 `report_version: 1`、`digest_format: "jessica.trace.digest.v1"`、per-turn `digest/closed/entries`。`trace verify` 新增 `--expect <64-hex>`（仅 `--turn`，大小写不敏感并规范化为小写）；不匹配新增 `digest_mismatch` problem，结构失败导致无 digest 时新增 `digest_unavailable`；verify `report_version` 2→3，per-turn 增加 `digest`，文本模式打印 digest。解析器与 usage 同步。
6. README 记录命令、schema v2 审计要求与威胁模型：链只在期望 digest 存于库外时可作为防篡改证据；能重写整库者可重算链，因此不是签名。

测试：`trace_chain` 单测 6（golden vector、长度前缀边界碰撞、optional 有无、逐字段敏感性、prev 敏感性、hex 往返/拒绝）；`tests/trace_chain.rs` 6（链接与 reopen 稳定、迁移回填 digest 一致且 v1 只读被拒、payload 编辑/删链节/插链节被检出、失败回填整体回滚并可重试）；CLI 解析 4（digest turn/all/equals、expect 大写规范化、非法 digest、非法 expect）；E2E 18 个（新增 7 个：digest 单 turn 与 verify digest 一致、all 排序/JSON、空库、未知 turn、链被删、expect 匹配/大写/不匹配、结构损坏时 `digest_unavailable`；既有 `report_version` 断言更新为 3 并断言 per-turn digest）；`durability`/`trace_store` 的未来版本夹具改为 3、表清单加入 `trace_chain`、legacy 升级断言 `user_version=2`。未改 `jessica-core`，未新增第三方 crate（复用已有 sha2），未实现跨 turn 聚合 digest 或 8.4+。

验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 891 passed, 0 failed, 2 ignored（56 suites）。

偏离/说明：1) digest 覆盖精确存储列（`payload_json` 文本与 mirrored i64），不重新序列化，保证迁移回填与在线追加一致；2) `--all` 仍通过 `trace_heads` 枚举 turn，未被 head 索引的孤儿 `trace_events` 行不会出现在 digest/verify 中，已记入 Deferred；3) 未提供跨 turn 聚合摘要或外部时间戳，留待 8.4；4) 本地 `DESIGN.md` 保持未跟踪。

Task 8.4（2026-11-19 确认并完成）：可移植证据包 export 与离线 verify

目标：把已验证的 turn trace、哈希链和 mirror 所需的 legacy 证据导出为单个 JSON 文件，并能在没有 `[events]` 配置、没有事件库的机器上离线复算链、重跑结构/语义/mirror 校验与聚合摘要；不引入签名、TSA 或新依赖。

操作：

1. `jessica-memory/src/trace_chain.rs`：新增 `jessica.trace.bundle.v1` 聚合领域与 `bundle_digest(&[(&str, [u8; 32])])`。编码为 `SHA-256(field(domain) || field(u64_be(turn_count)) || 按 turn_id 升序逐 turn: field(turn_id) || field(raw_digest))`，`field(x) = u64_be(len(x)) || x`；golden vector 锁定 `19c7f90e4a9178e211e37be8c7a1405acce610a4f0587ab83071e31e3e993205`，空聚合锁定 `caa75444df5a644018913defe9dbc552d8ff3fc759eec5c72f95007a73e5c647`。
2. `jessica-memory/src/trace_store.rs`：新增公共 `TraceEvidenceEntry`（私有字段 + `new/seq/timestamp/payload_json` 访问器）、`trace_bundle_digest(&[&StoredTraceTurn])`（按 turn_id 排序、拒绝重复 turn、只接受规范小写 64-hex digest）与 `verify_exported_trace(turn_id, closed, entries)`。离线重建打开私有内存 `EventStore`，要求 entry 非空、`seq` 从 1 连续、逐条解码 `TraceEvent` 并校验 envelope 与 payload turn_id 一致，写入 `trace_events`/`trace_chain`/`trace_heads`，最后调用既有 `EventStore::trace_turn` 复用 8.3 的结构、生命周期、链完整性校验并返回重算 digest 的 `StoredTraceTurn`；不复制验证逻辑，不改 schema。
3. `jessica-cli/src/trace_bundle.rs`：新增 bundle DTO（`bundle_version: 1`、`bundle_format: "jessica.trace.bundle.v1"`、`aggregate_digest`、按 turn_id 升序的 turns，每 turn 含 `turn_id/closed/digest/entries/legacy`）。`trace export --turn <id> | --all --out <file> [--allow-open]` 通过 `open_read_only` 读取，逐个重跑 `verify_stored_turn`；invalid 一律拒绝，open 仅在 `--all --allow-open` 下允许；校验通过后才写文件，拒绝路径不产生输出。legacy 证据只包含 mirror 校验实际消费的 `(session_id, timestamp, payload_json)` 多重集行，顺序与 trace entry 一致。`--all` 空库导出 0 turn；同一库重复导出字节一致。
4. `trace verify-bundle --bundle <file> [--json] [--allow-open] [--expect <64-hex>]` 不读取配置的 `[events]`、不打开/创建/迁移事件库。读取前以 metadata 强制 64 MiB 上限；校验 version、format、聚合/逐 turn digest 的规范小写 64-hex 与重复 turn_id；随后在内存中重建每条链，重跑 mirror 校验，比较声明的逐 turn digest 与重算值，并只在全部 turn 结构有效时重算聚合。`--expect` 与聚合不匹配、声明聚合不匹配、结构失败（`digest_unavailable`）、turn digest 不匹配、mirror 失败、open 且未 `--allow-open` 均使命令退出 1；JSON 报告 `report_version: 1`，文本共享既有逐 turn 摘要。`--replay` 显式拒绝。
5. `trace_verify` 暴露可复用内部项，并新增 `exported_legacy_rows`（按 verifier 的精确序列化与多重集消费规则返回证据行）；参数解析、usage、`app.rs` 结构化错误与 README（命令、bundle 格式、聚合编码、离线语义、64 MiB 上限、退出码、威胁模型）同步。

测试新增 19 个：`trace_chain` 单测 +1（聚合 golden vector 与空/单 turn 区分）；`jessica-memory/tests/trace_evidence.rs` 6（导出 entry 复算在线 digest、时间戳编辑改变 digest、seq gap/重复拒绝、畸形 JSON 与跨 turn payload 拒绝、生命周期不符/空证据/ended 后 entry 拒绝、聚合与顺序无关且对集合敏感并拒绝重复 turn）；CLI 解析 3（export turn/all/out/allow-open、verify-bundle 离线参数、畸形 bundle 参数拒绝）；`jessica-cli/tests/trace_bundle_e2e.rs` 9（真实 SQLite + 真实 CLI：export→无 `[events]` 配置离线验证往返、确定性字节与排序、open 导出/离线 `--allow-open`、invalid trace 导出失败且不写文件、trace/legacy/aggregate 三类篡改检出、turn digest 与 `--expect` 锚定、缺失/版本/格式/重复/非法 digest/超限 bundle 拒绝、空库空 bundle、缺失事件库不创建）。

验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 910 passed, 0 failed, 2 ignored（58 suites）。

偏离/说明：1) 离线校验复用私有内存 `EventStore` + `trace_turn`，而不是另写一套结构校验，保证与在线 verify 逐项同源；该内存库不接触用户数据库；2) 聚合只覆盖 turn digest，legacy 证据通过 mirror 校验绑定到 digest 覆盖的 trace payload，不单独纳入聚合；3) 导出只接受可验证的正面证据，invalid trace 不能作为“失败证据”导出，遗漏某 turn 本身不可证伪，除非外部锚定 turn 集合/聚合；4) `verify-bundle` 不支持 `--replay`，离线重放语义未定义，已在解析层拒绝；5) bundle 不是签名，能重写整个 bundle 者可重算全部摘要，必须用 `--expect` 与库外记录锚定；6) 既有 `trace digest --all` 输出契约不变，未加入聚合；7) 导出用 `std::fs::write` 一次性写文件，未做临时文件 + fsync 的原子替换；8) 本地 `DESIGN.md` 保持未跟踪；未改 `jessica-core`，未新增第三方 crate。

Task 8.5（2026-11-19 确认并完成）：`--all --replay` 与 v1/Resumed 覆盖扩展

目标：让 `trace verify --replay` 覆盖批量选择、version-1 fresh trace 与 version-2 resumed trace，同时保持只读、确定性、逐 turn 内存有界与既有报告契约。

操作：

1. `jessica-runtime/src/recovery.rs`：新增 additive 公共构造器 `SessionCheckpoint::from_recorded_state(session_id, messages, rounds_completed)`，用于 trace 只记录 checkpoint 会话与已完成轮数、不保留产生它们的事件时重建 resume 起点；`last_seq=0`、`end_reason=None`。新增行为测试断言四个可观察字段。
2. `jessica-cli/src/replay.rs`：抽出 fresh/resumed 共用的 `replay_from`；新增 `replay_resumed_session(session_id, stored, rounds_completed)`，用上述 checkpoint 调用真实 `AgentLoop::resume`，预算设为 `rounds_completed + 脚本轮数`，从而复现 resumed 原始 `MaxRounds` 消息；`ScriptBuilder` 增加 no-`SessionStarted` 模式，resumed 流直接解析 `RoundStarted` 及后续显式轮次。保留 v1 heuristic 与 `restore_recorded_vocabulary` 行为。
3. `jessica-cli/src/trace_replay.rs`：`prepare` 接受 schema v1/v2。fresh v1 单 attempt 使用 `turn_started.messages` 作为 bootstrap，并要求 `SessionStarted.messages` 为空；fresh v2 继续使用每个 attempt 的 `SessionStarted.messages`。resumed 单 attempt 要求无 `SessionStarted`、首事件为 `RoundStarted`，以 `round - 1` 作为 checkpoint 的 `rounds_completed`，并用 `turn_started.messages` 作为会话；无模型轮的 resumed 流与 v1/resumed 多 attempt 流显式 unsupported。其余结构性、终态与 trace-only/governance 过滤规则不变。
4. `jessica-cli/src/trace_verify.rs`：`--all --replay` 先只读枚举 turn id，再逐个 `spawn_blocking` 打开只读库、加载并完整验证单 turn，异步 replay 后立即丢弃该 turn entries；报告累积后统一输出，内存由最大单 trace 而非全库决定。mismatch 仍把该 turn 标为 invalid 并继续批量；unsupported 合法 trace 保持 `ok`，但命令退出 1。新增 `CliError::TraceReplayUnsupportedBatch { count, turns }`；验证失败优先于 unsupported。`report_version` 保持 3，单 turn 与非 replay `--all` 行为不变。
5. `jessica-cli/src/main.rs`：允许 `--all --replay`，拒绝 `--all --replay --allow-open`（replay 要求 closed trace）、继续拒绝 `--expect` 搭配 `--all`；usage 与 `--replay` 说明更新。README 更新命令、v1/resumed 语义、批量内存模型与退出语义。`jessica-core` 未改，SQLite schema 未改，未新增第三方 crate。

测试：runtime checkpoint 构造 1；`replay` 单测 2（resumed 完成流从 round 3 继续且不伪造 `SessionStarted`、resumed 预算耗尽保留原始 `MaxRounds(4)` 消息）；`trace_replay` 测试更新/新增 3（v1 fresh 从 turn bootstrap 重放、resumed 从 checkpoint 重放、无模型轮 resumed unsupported）；CLI 解析 2（`--all --replay` 接受、`--all --replay --allow-open` 拒绝）；`trace_verify_e2e` 8（v1 成功、fresh/v1/resumed 批量全 ok、mismatch 后继续、unsupported 不 invalidate、拒绝 allow-open、空库批量成功，以及既有 v1/batch 期望更新）；`resume_e2e` 在真实 `--resume` 后追加真实 CLI `trace verify --turn --replay`，断言 4 个 resumed 事件逐一对齐。

验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。

偏离/说明：1) resumed 的原始 `Budget.max_rounds` 未写入 trace，批量 replay 以 `rounds_completed + 脚本轮数` 作为总预算；这对 `Completed`/`Error` 足以复现，对 `BudgetExceeded` 恰好等于原始总预算，但无法验证未达到预算时的原始上限数值；2) `--all --replay` 顺序执行而非并发，以换取内存有界与明确错误顺序；3) v1 与 resumed 均只支持单 attempt，多 attempt 缺少逐 attempt 的 bootstrap/checkpoint 归属；4) resumed 无 `RoundStarted` 时无法可靠推导 `rounds_completed`，记录为 Deferred；5) 批量执行期间若外部写入新 turn，本次枚举不会包含它（只读快照语义与既有 `--all` 一致）；6) 未改 `jessica-core`、schema、既有 `replay --session` 或 bundle replay 语义；7) 本地 `DESIGN.md` 保持未跟踪；未提交、未推送。

Task 8.6（2026-11-19 确认并完成）：子 Agent 轨迹证据

目标：把委托子 Agent 的内部事件（模型轮次、工具调用、终止原因与计数）作为嵌套证据写入父 turn 的同一条哈希链，使 `trace verify`、`trace digest`、`trace export` / `verify-bundle` 自动覆盖；父 replay 仍不执行子 Agent。

操作：

1. `jessica-core`（经用户批准）：`TRACE_SCHEMA_VERSION` 2→3，`MIN_SUPPORTED_TRACE_SCHEMA_VERSION` 保持 1，支持范围 1..=3；新增 `TracePayload::ChildAgent { call_id, event }`，`kind() == "child_agent"`、`required_trace_schema_version() == 3`。envelope 语义：`attempt` = 父 attempt，`session_id` = 子 session，`call_id` 链接父侧 `delegate_task` 调用；该 payload 永远 trace-only，不写 legacy mirror；v1/v2 继续可读，v2 读者因版本不识别拒绝 v3。
2. `jessica-runtime`：新增公共 `ChildEventSink` trait（`emit(child_session_id, call_id, event) -> Result<(), EventSinkError>`）与 `AgentChildExecutor::with_child_event_sink`；子 `AgentLoop` 的每个事件先交给 child sink，再走原有私有 broadcast。始终包装内部 `CountingChildSink`，统计已接受的 `RoundFinished`、`ToolCallStarted` 与错误 `ToolCallFinished`；timeout 时父 `ChildSessionEnded` 报告已观测计数而非固定 0/0/0，正常路径继续使用 transcript 计数。child sink 失败会中止子执行并按 `Cancelled` 报告；父侧 lifecycle sink 行为与“子事件不进入父 broadcast”不变。
3. `jessica-cli` journal：`TurnJournal` 实现 `ChildEventSink` 并提供 `child_sink()`；CLI 同时挂父 sink 与 child sink。强制括号顺序：父 `ChildSessionStarted` → 子 `SessionStarted` → 子轨迹 → 子 `SessionEnded`（timeout 可缺失）→ 父 `ChildSessionEnded`。子事件记录为 `TracePayload::ChildAgent`，父 attempt + 子 session + parent `call_id`，无 legacy；拒绝无括号、重复 start、end 后事件、括号外事件、parent-only 事件、缺失终态（Cancelled 除外）、父 session 提前结束、未闭合时 `finish_turn` 与过期 attempt。
4. `jessica-memory`：接受并校验 `ChildAgent`：必须同时有 attempt 与 session、`call_id` 非空、禁止嵌套 child lifecycle、禁止 legacy mirror；`payload_kind = "child_agent"`，`event_type` 存内部事件 tag；读取校验、链哈希与 `verify_exported_trace` 同步支持；SQLite schema 不变。
5. verifier：严格子轨迹校验以“该 turn 是否含至少一个 `ChildAgent`”为开关。子 `SessionStarted` 必须是该 `(child_session_id, call_id)` 的首个证据，bootstrap 精确等于 `[system(CHILD_AGENT_SYSTEM_PROMPT), user(task)]`；task 来自同一父 attempt/session 中尚未 finished 的 `delegate_task` 参数，且同一父调用不得链接多个子 session。校验子轮次从 1 连续、`ModelDelta` 在开放轮内、tool start 在 round finish 后并成对，禁止 parent-only 与嵌套 lifecycle 事件。子终态 reason 与父一致，rounds/tool_calls/tool_errors 与观测前缀一致；timeout 无子终态仅允许父 `Cancelled`；工具限额被拒调用允许父计数比观测多 1。生命周期-only v2/v3 trace 继续按 v2 规则通过。
6. replay / digest / bundle：`trace replay` 接受 v1–v3，`ChildAgent` 在校验后被忽略，父 replay 不执行子 Agent、不产生子 provider 调用；哈希链、`trace digest`、`trace export`、`verify-bundle` 自动覆盖子轨迹原始 JSON，bundle 格式、`report_version` 与既有 digest 文本/JSON 契约不变。
7. 文档与测试：README 记录 v3 子轨迹语义、严格校验、replay 忽略子轨迹、digest/bundle 自动覆盖，以及子 prompt/模型输出/工具参数现在会持久化的隐私与留存提示（保留期删除仍留给 8.7）。新增/更新 27 个测试：core 1、runtime 3、journal 11、memory 2、verifier 8、replay 1、真实 delegation E2E 1（含 export/verify-bundle 与子轨迹篡改），并更新既有版本/报告断言。

验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 945 passed, 0 failed, 2 ignored（58 suites）。

偏离/说明：1) child sink 拒绝会中止子执行，此时 transcript 计数可能大于已接受证据；CLI journal 自身失败，正常路径不会产生可完成 turn，未为该异常路径定义“部分证据”验证；2) 严格子轨迹校验以含 `ChildAgent` 为开关，纯生命周期 v3 trace 仍可验证，CLI 生产者总会写 `ChildAgent`；3) replay 只忽略子证据，不重放子 Agent，也不核对子 provider 请求/工具结果的可复现性，子执行一致性留待后续 Task；4) 不支持递归嵌套委派，子工具集仍由 `delegate_task` 参数收紧；5) 子对话与父 trace 同库持久化并进入 bundle，保留/删除策略未实现，留给 8.7；6) 未改 SQLite schema、`delegate_task` 结果契约或 bundle 格式，未新增第三方 crate，未提交/推送，`DESIGN.md` 保持未跟踪。

后续候选（未确认，不执行）：

- 8.6 子 Agent 轨迹证据已完成（见上）。
- 8.7 保留期/隐私删除（需先确认 append-only 契约变更）。

---

Phase 9：自动配置与工具能力层（2026-11-22 新增；Task 9.0、9.1、9.2、9.3a、9.4、9.5、9.6、9.7a、9.8 已完成，9.3b/9.7b 待依赖审批）

背景：用户希望 Jessica 更自动、更简洁，并为后续工具能力扩展与多 Agent 协同打基础。Phase 9 先交付不影响工具执行语义的配置地基，再按依赖顺序逐 Task 确认 Catalog、执行监督、策略、结果管理与协议扩展。

Phase 9 任务序列（依赖顺序；每项仍须单独确认后实施）：

| Task | 内容 | 改 core | 新依赖 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| **9.0** | Profile preset、effective config、默认 trace | 否 | 无 | 无 | ☑ |
| **9.1** | Tool Catalog、descriptor、per-tool limits、schema 过滤、`tools list/inspect` | 否 | 无 | 无 | ☑ |
| **9.2** | Supervisor A：env allowlist、per-tool timeout 落地、输出上限、acceptance/sensor 超时 | 否 | 无 | 9.1 | ☑ |
| **9.3a** | Supervisor B（无依赖子集）：bash 启用门、`sandbox = "process"`、迁移说明 | 否 | 无 | 9.2 | ☑ |
| **9.3b** | Supervisor B（依赖门）：进程组 kill、SIGTERM→SIGKILL、rlimits | 否 | `libc`/`nix` 审批 | 9.3a | ☐ |
| **9.4** | Result Manager、artifact store、`read_artifact` | 否 | 无 | 9.1 | ☑ |
| **9.5** | 原生工具 A：read ranges、`list_dir`、literal grep、`git status`/`git diff`、`exec` | 否 | 无 | 9.2/9.4 | ☑ |
| **9.6** | Policy v2、approval modes、`policy simulate` | 否 | 无 | 9.1/9.5 | ☑ |
| **9.7** | 原生工具 B：`apply_patch`、`edit_file`（9.7a 已完成）；glob、regex grep（9.7b 待审批） | 否 | 9.7b 需 `globset`/`regex` 审批 | 9.5/9.6 | ☑（9.7a） |
| **9.8** | MCP/WASM descriptor、子 Agent 工具安全对齐 | 否 | 无 | 9.1/9.4/9.6 | ☑ |

说明：9.2 与 9.4 都只依赖 9.1，可互换顺序；推荐先 9.2 再 9.4，以便 `exec` 和结果管理在 9.5 前同时就绪。9.3b 与 9.7b 受第三方依赖审批门约束，未批准前只交付对应的无依赖子集，不得用自实现或 unsafe 绕过审批。

Task 9.0（2026-11-22）：Profile preset、effective config、默认 trace

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.1 的 Tool Catalog、每工具限额、执行监督、策略 v2、结果管理、额外原生工具、MCP/WASM descriptor 或多 Agent 嵌套。
- 配置解析采用 raw `toml::Table` 合并，确保“显式字段 > preset > 内建默认”可区分；不能依赖反序列化后的 `HarnessConfig` 推断用户是否显式设置。
- 默认 `profile.preset = "dev"`、`profile.trace = "strict"`、`events.path = ".jessica/events.db"`（相对启动 workspace root）。
- preset 只提供安全默认值，不自动启用 memory、skills 或 reliability；`safe` 把默认 allowlist 收窄为 `read_file`，`dev`/`ci`/`lab` 使用原生工具集。显式字段（包括显式空 allowlist）始终优先。
- `trace = "off"` 清除隐式事件库路径并禁用 `--resume`/`replay`/ACP recovery；与显式 `events.path` 同时出现时启动失败，不静默覆盖。
- `EventRecorder::open` 在打开 SQLite 前创建缺失父目录，使默认 `.jessica/events.db` 与显式嵌套路径都可落盘；目录创建失败 fail-closed。
- `profile` 为不可热更新 section，变更整份拒绝。
- `jessica config` 输出 resolved effective config 的 pretty JSON，且不得打开 provider、event store、memory/skills DB 或 wasm 模块；`Serialize` 仅用于配置输出。
- 计划文本中“effective config 和 preset 名写入 trace”不在本 Task 实现：冻结的 `jessica-core` trace payload 没有承载配置的变体，复用 `TurnStarted.messages`/`AgentEvent`/`Gap` 会污染审计语义。记录到 Deferred，待单独批准 core schema change。
- 默认 trace 会改变所有未配置 `[events]` 的运行；README、热加载不可变 section 列表与受影响测试必须同步更新；`.jessica/` 加入 `.gitignore`。`install.sh` 已显式生成绝对 `[events].path`，本 Task 不改其行为。

完成标准：fmt/clippy/test 全绿；新增行为测试覆盖默认值、preset 覆盖、safe allowlist、trace off/冲突、非法 preset、effective JSON、`jessica config` 无副作用、父目录创建与热加载拒绝；README 与进度追踪更新。

---

Task 9.1（2026-11-22）：Tool Catalog、descriptor、per-tool limits、schema 过滤、tools list/inspect

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate（`sha2` 已在 workspace 依赖中，本 Task 仅为 `jessica-cli` 显式引用）；不实现 9.2–9.3 的执行监督、9.4 的结果管理、9.5/9.7 的原生工具、9.6 的工具策略 v2、9.8 的 MCP/WASM descriptor 或嵌套委派。
- `ToolDescriptor` 是宿主侧治理分类，不进入 core；core 继续只暴露 `Tool`、`ToolSchema`、`ToolRegistry`、`ToolCall`、`ToolResult`。第三方工具不得自报能力，source/capabilities/approval/policy hints 由宿主按来源与配置赋值。
- 新增 `[tools]`：`profile`（`readonly`/`coding`/`ops`/`full`，默认 `full`）、`max_exposed_tools`（默认 64）、`max_tool_schema_bytes`（默认 131072）、`[tools.read_file]`/`[tools.write_file]`/`[tools.bash]` 的可选 `timeout_ms` 与 `max_args_bytes`。profile 默认采用 `full` 而不是设计示例的 `coding`，以保持既有未配置行为；收窄必须显式 opt-in，并记入偏离。
- 校验：所有新数值必须 ≥1；per-tool `timeout_ms` 不得超过 `budget.tool_timeout_ms`；`bash` 的 per-tool `timeout_ms` 不得超过 BashTool 内部 10s 上限。非法值在配置加载期返回字段级 `ConfigError`。
- Catalog 可用来源：native、已配置 WASM、当前 session 的 MCP extra tools、启用且允许委派时的 `delegate_task`；再与 `policy.allowed_tools` 和 profile 取交集。第三方工具（WASM/MCP）只允许在 `full` 下暴露，且仍必须在 allowlist 中。未暴露工具不注册进 `ToolRegistry`，因此不发送给模型；模型幻觉调用得到 `unknown tool`，不会到达 policy/permission/执行。
- descriptor 至少包含 name、source、version、schema、schema digest、capabilities、limits、approval、policy hints、result format；排序稳定。schema digest 为 canonical schema 字节的 `sha256:<64 hex>`，重复构建可复现。重复工具名在目录构建期 fail-closed。
- 暴露预算：`max_exposed_tools` 与 `max_tool_schema_bytes` 任一超限时 turn 启动失败并给出可行动错误，绝不静默丢弃；`tools list` 对同一超限只提示而不失败，便于诊断。
- per-tool limit 执行：有效 timeout = per-tool 覆盖或全局 `budget.tool_timeout_ms`，仍由既有 `GovernedTool` 超时 wrapper 强制；`max_args_bytes` 在执行前按序列化参数长度检查，超限返回错误结果且不执行工具、不触发预算 trip、不进入 policy/permission/执行。
- 委派能力收窄使用 Catalog 实际暴露的 native 名称，profile 隐藏的工具不能授予子 Agent。
- `jessica tools list [--all]` 与 `jessica tools inspect <name>`：只加载配置与（如有）WASM manifest，不创建 provider、事件库、memory/skills DB，不需要 API key，不写 `.jessica`；输出顺序稳定，`--all` 显示隐藏原因，inspect 显示 descriptor、schema digest、limits、policy hints。未知名返回非零结构化错误。
- `[tools]` 是每 turn 重新构建 Catalog 的可热加载 section，不加入 `changed_immutable_section`；修改 profile 必须影响下一 turn 的 provider schema 与可执行工具。
- 既有“越权工具进入 schema 后被 policy 拒绝”的行为改为“不进入 schema、调用得到 unknown tool”；受影响 E2E、examples 注释与 README 必须同步。事件流的 PolicyDenied 语义只保留给已暴露但被 policy 拒绝的调用（正常情况下不应发生，仍作为纵深防御）。

完成标准：fmt/clippy/test 全绿；新增行为测试覆盖配置默认/边界、descriptor digest 稳定性、Catalog 过滤/排序/隐藏原因/重复名/预算、`max_args_bytes` 边界、per-tool timeout、readonly profile 的 schema 与 unknown tool、委派能力收窄、`tools list/inspect` 真实 CLI 无副作用、`[tools]` 热加载；README、examples 与受影响 E2E 更新。

---

Task 9.2（2026-11-22）：Execution Supervisor A — env allowlist、输出上限、acceptance/sensor 超时

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.3 的进程组 kill/rlimits/bash 默认关闭，不实现 9.4 的 artifact/Result Manager，不新增原生工具，不实现 Policy v2、MCP/WASM descriptor 或嵌套委派。
- 复用 9.1 的 descriptor 与 per-tool limit；本 Task 只把 timeout 真正落到命令进程、sensor 与 acceptance，不重新定义 Catalog，也不重复实现 `GovernedTool` 的 wrapper 超时。
- 所有命令执行统一 `env_clear()`，仅注入显式 allowlist（默认至少 `PATH`、`HOME`、`LANG`、`TERM`，可评估加入 `TMPDIR`）；provider API key、`OPENAI_*`/`ANTHROPIC_*`、`JESSICA_*` 与其他宿主凭证永不继承。allowlist 可通过配置扩展，但不得用通配符或空项，默认安全项不得被静默移除。
- 新增 supervisor 配置（字段名可按现有配置风格调整，语义不得缩水）：`[tools.supervisor] env_allowlist`、`command_timeout_ms`、`max_output_bytes`、`sensor_timeout_ms`、`acceptance_timeout_ms`。所有数值必须 ≥1；sensor/acceptance 超时不得超过全局 `budget.tool_timeout_ms`；默认值不得放宽现有 bash 10s / 每流 64 KiB 边界。
- `CommandSensor` 与 `CommandAcceptanceCheck` 用 `tokio::time::timeout` 包裹完整的 spawn+wait；超时返回明确的失败结果（sensor fail / acceptance fail 并带可行动 feedback），不得挂死 turn；stdin 固定为 null，cwd 固定在 workspace 或显式配置目录，`kill_on_drop` 至少终止直接子进程（进程组清理留给 9.3）。
- 输出上限覆盖 bash、sensor、acceptance 的 stdout/stderr；超限截断并在结果中注明 `truncated` 与原始字节数，不得触发 `BudgetExceeded`、panic 或静默丢错。9.4 之前不落 artifact，只返回截断后的预览。
- 失败语义保持：spawn 失败、超时、输出读取失败是工具/检查失败；命令非零退出仍是“命令执行完成但失败”，保留 exit code，不得把非零退出误报为 spawn 错误。错误信息与审批摘要不得包含环境变量值或 secrets。
- `[tools.supervisor]` 与 `[tools]` 一样每 turn 重建，可热加载；env allowlist 变更只影响后续 turn，不得影响已启动子进程。
- 子 Agent 的 bash/exec 同源约束在 9.8 对齐；本 Task 只保证 CLI 主 turn 的 supervisor 装配，不提前修改 runtime 委派路径。

完成标准：fmt/clippy/test 全绿；行为测试覆盖 provider key 不进入子进程、默认与扩展 allowlist、bash/sensor/acceptance 超时不挂死、输出截断与非零退出语义、配置边界与热加载；真实 CLI E2E 至少一个用 `env` 检查子进程环境。README、examples 更新 supervisor 配置与 bash 环境说明；未改 core、未新增第三方 crate。

实施记录（2026-11-22，实际耗时约 14 分钟，18:13–18:27）：

- 完成情况：新增 `jessica-harness/src/supervisor.rs`（`CommandSpec`、`run_supervised`、输出上限与截断工具），`CommandSensor` 与 `CommandAcceptanceCheck` 复用同一实现；`BashTool` 改为 `env_clear()` + allowlist、渲染结果总量上限并记录原始字节数；新增 `[tools.supervisor]` 配置、字段级校验、resolved 视图与热加载测试；新增真实 CLI E2E `jessica-cli/tests/supervisor_e2e.rs`。
- 偏离/说明：1) `jessica-tools` 按依赖规则不能依赖 `jessica-harness`，`BashTool` 保留一份语义一致的 allowlist/敏感名过滤实现，两侧以注释标明必须同步，未新增 crate 或反向依赖；2) 默认 allowlist 采用计划允许评估的 `TMPDIR`，最终为 `PATH`、`HOME`、`LANG`、`TERM`、`TMPDIR`；3) `max_output_bytes` 额外校验不得超过 `budget.max_result_bytes`，默认取 `min(64 KiB, max_result_bytes)`，bash 最终渲染结果按该值二次截断，保证截断结果不触发 `BudgetExceeded`；4) sensor/acceptance 超时默认取解析后的 command timeout，且校验不超过全局 `budget.tool_timeout_ms`；bash 的 descriptor 超时仍封顶既有 10s，默认值不放宽既有 bash 10s / 每流 64 KiB 边界；5) `HOME`/`TMPDIR` 仅传递宿主值，未重定向到 workspace 内受控目录，目录隔离如需要留待后续 Task 评估；6) 进程组 kill/rlimits 与 bash 默认关闭属 9.3，子 Agent 同源约束属 9.8，本 Task 未触及；7) `[tools.supervisor]` 嵌套于 `[tools]`，每 turn 重建并随 `[tools]` 热加载，已启动子进程不受影响；8) 未改 `jessica-core`，未新增第三方 crate，本地 `DESIGN.md` 保持未跟踪。

---

Task 9.3（9.3a 已完成；9.3b 待依赖审批）：Execution Supervisor B — 进程组 kill、rlimits、bash 启用条件

范围与约束：

- 不修改 `jessica-core`；除 `libc` 或 `nix` 外不新增第三方 crate；不实现 9.4+ 的 Result Manager、额外原生工具、Policy v2 或 MCP/WASM 对齐。
- 本 Task 分两阶段，必须分别验收：
  - **9.3a（无新依赖）**：新增 `[tools.bash] enabled`（默认 `false`）与 `sandbox = "process"` 要求。未显式启用时 `bash` 不进入 Catalog（即使 allowlist/profile 包含），`tools list --all` 与 `inspect` 必须说明隐藏原因；直接子进程超时只能保证直接子进程，不得在文档或错误信息中声称进程组隔离。既有依赖 bash 默认可用的配置需要显式 opt-in 与迁移说明。
  - **9.3b（仅在用户批准 `libc`/`nix` 后）**：为 bash/exec/sensor/acceptance 创建独立进程组；超时先 SIGTERM，宽限期后 SIGKILL 整个进程组；取消/父进程退出时清理；按配置设置 rlimits（至少 CPU、FSIZE、NOFILE、NPROC，CORE=0）；覆盖 `bash -c 'sleep 60 & wait'` 超时后无孤儿进程的测试。
- 依赖审批门：未批准前不得把 `libc`/`nix` 写入 workspace `Cargo.toml`，不得用 `unsafe` 手工 syscall 或自实现进程组管理绕过审批；9.3b 保持未开始，9.3a 仍可独立交付。
- 进程组/rlimits 同时适用于 bash、9.5 引入的 exec、sensor 与 acceptance；平台不支持时必须显式报错或拒绝启动，不得静默降级为“仅直接子进程”后仍声称隔离。
- `bash` 仍是逃逸面：不做命令字符串解析，policy 只能按工具/审批控制；必须明确记录“Policy 不是完整安全边界，真正隔离依赖 process/container”。
- 配置：`[tools.bash] enabled`、`sandbox`、`rlimits`；默认 `enabled = false`、`sandbox = "process"`；非法组合（例如未实现的 container、rlimit 为 0、未知字段）在加载期结构化拒绝；`[tools.bash]` 可热加载。
- 测试：bash 未启用时 provider schema 不含 bash 且幻觉调用得到 `unknown tool`；显式启用后可用；超时后进程组无孤儿（通过 `/proc`、`ps` 或子进程标记文件验证）；SIGTERM 宽限后 SIGKILL；rlimit 触发返回结构化失败；取消路径清理；9.3b 未批准时不得出现依赖相关代码。

完成标准：9.3a 在无新依赖下 fmt/clippy/test 全绿并完成迁移说明；9.3b 在依赖批准后完成进程组/rlimit 行为测试。README 更新 bash 默认关闭、启用条件与安全边界；未改 core。

实施记录（2026-11-22，实际耗时约 10 分钟，20:22–20:32，仅 9.3a）：

- 完成情况：`[tools.bash]` 升级为 `BashToolConfig`（`enabled` 默认 false、`sandbox` 仅接受 `process`，保留 `timeout_ms`/`max_args_bytes` 并复用既有校验）；`ToolCatalog::build` 增加 native bash 启用门，未显式启用时即使 policy/profile 允许也隐藏，并记录 `HiddenReason::BashDisabled`（显示为 `disabled; set [tools.bash].enabled = true`），`tools list --all` 与 `inspect` 可解释原因；system prompt 过滤未启用的 bash，避免向模型宣传不可用工具；`[tools.bash]` 随 `[tools]` 热加载。新增行为测试覆盖：默认 provider schema 无 bash、幻觉调用得到 `unknown tool` 且无文件副作用、显式启用后可执行、CLI 默认 list/`--all`/inspect 的隐藏与启用行为、REPL 与 ACP 热加载启用、`sandbox = "container"`/`rlimits`/非布尔 `enabled` 在加载期结构化拒绝。README、`examples/harness.toml`、`install.sh` 增加默认关闭、显式 opt-in 与迁移说明，并明确当前仅保证直接子进程。
- 偏离/说明：1) 按用户决定仅交付 9.3a，未引入 `libc`/`nix`，未实现进程组 `SIGTERM`→`SIGKILL`、rlimits、unsafe syscall 或任何 9.3b 代码；2) `rlimits` 字段未加入配置，当前由 `deny_unknown_fields` 在加载期整体拒绝，避免暴露未实现的安全能力；9.3b 获批后再添加并校验 rlimit > 0；3) `sandbox = "container"` 由 serde 枚举拒绝，错误信息包含 `container`；4) 系统提示不再静态提及 bash，仅当 bash 已启用且被 allowlist 允许时通过 allowlist 行体现；5) 启用门位于 CLI Catalog 装配层，`BashTool` 本身不持门，保持 `jessica-tools` 可独立使用/测试；6) 既有依赖 bash 的单元测试与 E2E 配置均显式加入 `[tools.bash] enabled = true`；7) 未改 `jessica-core`，未新增第三方 crate，`DESIGN.md` 保持未跟踪。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 999 passed, 0 failed, 2 ignored。

---

Task 9.4（已完成）：Result Manager、artifact store、`read_artifact`

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.5 原生工具、9.6 Policy v2、9.8 MCP/WASM/子 Agent 对齐，也不改 SQLite schema 或 trace schema。
- Result Manager 位于 CLI 装配层或宿主侧共享结果模块；core 仍只认 `ToolResult` 的既有字段。工具输出必须先经过 Result Manager，再进入 transcript、trace 与模型；不得让 core 依赖实现层。
- 新增 `[tools]` 结果阈值：`inline_threshold_bytes`（默认 32768）、`max_inline_bytes`（默认 65536）、`max_artifact_bytes`（默认 16777216）、`artifact_dir`（默认 `.jessica/artifacts`）、`artifact_retention_days`、`artifact_max_bytes`。校验所有数值 ≥1，且 `inline_threshold_bytes ≤ max_inline_bytes ≤ max_artifact_bytes`；artifact 目录必须经 canonicalize 后位于 workspace 内，拒绝 `..` 与 symlink 逃逸。
- 小于 `inline_threshold_bytes` 直接返回原文；超过时写 artifact，返回 preview、sha256 digest、原始字节数、`truncated` 标志与 next cursor；超过 `max_artifact_bytes` 截断并标记；`max_inline_bytes` 是进入模型的硬上限。正常大输出不得再触发 `BudgetExceeded`；`ResultSizeGuard` 只作为内部不变量/最后防线。
- artifact 内容寻址 `.jessica/artifacts/<sha256 前两位>/<sha256>`；临时文件 + fsync + rename 原子写；目录 0700、文件 0600；sidecar JSON 记录 size、tool、created_at、digest；不得写入 provider key、环境变量或已知 secrets；文档必须标注 artifact 可能含敏感内容。
- `read_artifact` 原生工具支持 `offset`/`limit` 分页，返回内容与 digest；只读，路径必须位于 artifact 目录且 canonicalize 后不逃逸；未知 artifact、越界 offset、损坏 sidecar 返回结构化错误；不得递归产生新 artifact。
- 保留与清理：按 `artifact_retention_days` 与 `artifact_max_bytes` 做有界清理，只处理本工具创建的路径；清理失败只 warn，不影响 turn；trace 只记录 digest/截断标志，不记录完整大内容。导出 trace bundle 时可选包含 artifact manifest，不得默认打包内容。
- 为 exec/bash/搜索工具预留 rich result JSON（status、tool、exit_code、duration_ms、stdout/stderr 的 inline/bytes/artifact/next、truncated）；`read_file` 正常仍返回文本，仅在截断时附分页；实现不得修改 core `ToolResult` 字段，rich result 以 JSON 字符串放入 content。
- 测试：100 KiB `read_file` 返回 preview+artifact 且 turn 正常完成；200 KiB bash 输出截断落 artifact 且不触发 `BudgetExceeded`；artifact digest 可校验；分页边界；路径逃逸/未知 artifact/损坏 artifact；文件权限 0600；清理边界与失败降级；`read_artifact` 不执行真实工具。

完成标准：fmt/clippy/test 全绿；真实 CLI E2E 至少一个大输出场景与一个分页读取场景；README 更新阈值、artifact 目录、权限与隐私说明；未改 core、未新增第三方 crate。

实施记录（2026-11-22，实际耗时约 20 分钟）：

- 完成情况：新增 `jessica-cli/src/artifacts.rs`，实现 `ArtifactStore`、`ResultManager` 与 `ReadArtifactTool`。`[tools]` 新增 `inline_threshold_bytes`（默认 32768）、`max_inline_bytes`（默认 65536）、`max_artifact_bytes`（默认 16777216）、`artifact_dir`（默认 `.jessica/artifacts`）、`artifact_retention_days`（默认 7）、`artifact_max_bytes`（默认 256 MiB），加载期校验所有数值 ≥1、阈值有序、artifact 目录为 workspace 内相对路径且只含 normal component。`effective_max_inline_bytes`/`effective_inline_threshold` 再把有效上限钳制到 `budget.max_result_bytes`。
- Result Manager 接入 `GovernedTool`：工具返回后、`ResultSizeGuard` 前统一处理。文本工具收到文本 envelope，JSON 工具收到 JSON envelope；超大结果写入内容寻址 artifact，并向模型返回 digest、原始字节数、stored bytes、`truncated`、next cursor 与不超过 `max_inline_bytes` 的 preview。写入或清理失败时降级为有界 fallback，不使 turn 失败；`ResultSizeGuard` 仍为最后防线。Catalog 暴露的原生、WASM、MCP 与委派工具都经过该 wrapper。
- Artifact 存储：路径为 `<artifact_dir>/<sha256 前两位>/<64 hex sha256>`；同目录临时文件 + `write_all` + `sync_all` + rename；目录 0700、数据与 sidecar 文件 0600；超 `max_artifact_bytes` 时按 UTF-8 边界截断，`size`/digest 描述实际存储字节，`original_bytes` 保留源长度，sidecar 额外记录版本、tool、created_at 与 `truncated`。sidecar 不写 provider key、环境变量、tool 参数或命令文本。读取时校验 digest、sidecar 一致性、文件类型与大小，并拒绝 shard、数据文件或 sidecar 的 symlink 逃逸；分页按 UTF-8 边界返回。
- 清理：每个 turn 在第一次需要写 artifact 前最多执行一次有界清理（最多扫描 4096 个条目）；先删除超过 `artifact_retention_days` 的 artifact，再按最旧优先删除至总量不超过 `artifact_max_bytes`。目录不存在时视为 no-op；符号链接、损坏 sidecar 与无关文件跳过；删除失败只 `tracing::warn!`。
- `read_artifact`：native、只读，参数 `digest` 必填，`offset`/`limit` 可选；只按 digest 在 artifact 目录内定位，不接受路径。单页长度以 `max_inline_bytes - 512` 的 header 预留为上限，返回 digest、实际 offset、page bytes、total_bytes、next_offset 与内容；未知/非法 digest、越界 offset、损坏 sidecar、symlink 逃逸均返回结构化错误。其自身结果绕过 Result Manager，分页不会递归产生新 artifact。
- 文档与示例：README 新增 result manager/artifact 章节、profile 表补 `read_artifact`、阈值/有效钳制/目录布局/0700·0600/原子写/清理语义/结构化错误与 artifact 可能含敏感内容的隐私警告；`examples/harness.toml` 增加 `[tools]` 结果阈值并将 `read_artifact` 加入示例 allowlist；相关 `config`、`tools list` 计数与 E2E 期望同步更新。
- 测试：新增 8 个 artifact 行为测试（小结果旁路、100 KiB 落盘与 digest/权限/sidecar、UTF-8 截断、JSON envelope、分页不递归、未知/损坏/逃逸、保留与总量边界、写入失败降级），2 个 app 行为测试（100 KiB `read_file` preview+artifact、200 KiB bash 输出不触发 `BudgetExceeded`），1 个真实 CLI E2E `result_manager_e2e`（大输出 + `read_artifact` 分页 + 0600 + 不递归），以及配置边界/示例解析测试。
- 偏离/说明：1) 为兼容既有小 `budget.max_result_bytes` 配置，没有在加载期强制 `max_inline_bytes <= budget.max_result_bytes`，而是在运行期把 inline 上限与阈值钳制到预算内，避免正常大输出触发 `BudgetExceeded`；2) 默认 allowlist 保持 `bash`/`read_file`/`write_file` 不变，`read_artifact` 虽在 `readonly`/`coding`/`ops`/`full` profile 中允许，仍必须显式加入 `policy.allowed_tools` 才会暴露；3) 清理只在 turn 内首次超限写入前执行一次且扫描有界，不做常驻定时清理；4) sidecar 在任务要求的最小字段外增加 `version`/`original_bytes`/`truncated` 以支持完整性与截断校验，仍不包含参数或环境信息；5) trace bundle 可选 artifact manifest 未实现，现有 bundle 不打包 artifact 内容；6) 超 `max_artifact_bytes` 时 digest 针对截断后的存储内容，源长度由 `original_bytes` 表达；7) 时间路径（超时、policy/permission 拒绝、guard 拒绝）不产生工具输出，行为保持不变；8) 未修改 `jessica-core`、SQLite schema 或 trace schema，未新增第三方 crate，`DESIGN.md` 保持未跟踪。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 1011 passed, 0 failed, 2 ignored（含 `result_manager_e2e`）。

---

Task 9.5（已完成）：原生工具 A — read ranges、`list_dir`、literal grep、`git status`/`git diff`、`exec`

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.6 Policy v2、9.7 globset/regex/apply_patch/edit_file、9.8 MCP/WASM 对齐。
- 工具实现放在 `jessica-tools`，由 CLI 装配 Catalog；所有新工具必须有 descriptor（source、capabilities、limits、approval、policy hints、result format）、稳定 schema digest、默认 timeout/max_args_bytes，并进入 `tools list/inspect` 与 profile/allowlist 计算。
- `read_file` 增强：支持 offset/limit（明确按行还是字节并在 schema 中说明）、输出行号、二进制检测；保持 workspace 边界、`..`/绝对路径/symlink 拒绝与 UTF-8 处理；截断走 9.4 Result Manager。
- `list_dir`：稳定排序、跳过 `.git`/`target`（可配置忽略）、条目上限、不跟随 symlink 逃逸；输出目录/文件类型与大小摘要；大目录截断并提示。
- `grep`：先做字面量搜索，不使用 regex；跳过二进制、超大文件与忽略目录；结果含文件、行号、匹配行；限制文件数、单文件大小与总输出；特殊字符按字面量处理。
- `git_status`/`git_diff`：直接调用 `git`（无 shell，参数固定为只读子命令），cwd 必须在 workspace；超时、输出上限、非零退出语义复用 supervisor；拒绝任意 git 子命令或用户可控参数注入；无 git 仓库时返回结构化提示而非 panic。
- `exec`：结构化参数 `{program, args, cwd?, timeout_ms?}`，不经过 shell；program/args 是数组，便于 9.6 做 program/args prefix 策略；退出码非零表示“执行完成但失败”，`is_error = false` 且结果带 `exit_code`；spawn 失败、超时、sandbox 拒绝才是 `is_error = true`；必须走 9.2 supervisor，9.3b 后走进程组。
- profile：`read_file`、`list_dir`、`grep`、`git_status`、`git_diff` 可进入 `readonly`/`coding`/`ops`/`full`；`exec` 只进入 `coding`/`ops`/`full`，且仍需 policy 与 approval；`bash` 条件按 9.3。扩展 profile 表与隐藏原因，不得让新工具绕过 allowlist。
- 写工具安全：本 Task 不新增写工具；`write_file` 的原子写、原文件 checkpoint 与受保护路径留给后续 Task，不得顺手修改。
- 测试：read offset/limit/行号/二进制/越界；list_dir 排序/上限/忽略目录/symlink；grep 字面量特殊字符/大小写/二进制跳过/输出上限；git status/diff 真实临时仓库；exec env 清理/超时/非零退出/无 shell/参数数组；readonly profile 不暴露 exec；allowlist 拒绝；`tools list/inspect` 覆盖新工具。

完成标准：fmt/clippy/test 全绿；真实 CLI E2E 至少一个 `exec` 与一个 `read_file` offset 场景；README 工具表、profile 与安全说明更新；未改 core、未新增第三方 crate。

实施记录（2026-11-22，实际耗时约 90 分钟，含一次上下文压缩后继续）：

- 完成情况：`jessica-tools` 新增 `workspace`、`command`、`exec`、`git`、`grep`、`list_dir` 与测试辅助模块。`read_file` 增强为 1-based 行 offset、行 limit（默认 2000、上限 10000）、始终带 `行号\t内容` 输出、NUL/非法 UTF-8 二进制拒绝、16 MiB 扫描上限、超出 EOF 的结构化错误与 next-offset footer；路径仍经 canonicalize 并拒绝 workspace 外与 symlink 逃逸。
- 命令执行分层：`jessica-tools` 定义 `CommandRunner`/`CommandRequest`/`CommandOutput`/`CommandError`，避免反向依赖 `jessica-harness`；CLI 新增 `HarnessCommandRunner`，把请求映射到 9.2 `run_supervised`，因此生产路径实际复用 env 清理、凭据过滤、统一 deadline、双流输出上限与 drop kill。`git_status`/`git_diff` 使用固定只读参数数组且不接受用户参数；`exec` 使用 `{program,args,cwd?,timeout_ms?}` 数组，不经过 shell。
- 结果语义：`exec` 非零退出返回 `is_error=false` 的 JSON（`status`/`exit_code`/`duration_ms`/stdout/stderr 的字节数与 truncated 标志）；spawn、timeout、I/O 失败返回 `is_error=true` 的结构化 JSON。`git` 在非仓库时返回可操作提示，超时/非零按 supervisor 处理。`list_dir`/`grep` 有界遍历、稳定排序、可配置忽略目录与条目/文件/字节/匹配/输出上限，跳过 symlink、二进制、超大与不可读条目并汇总；grep 仅做字面量匹配。
- CLI 装配：Catalog 新增 9 个 native candidate；descriptor 为只读工具与 `exec` 分配 capabilities、Text/JSON result format、path/command policy hints，并为 `read_file`/`list_dir`/`grep` 报告 `max_output_lines`；profile 表扩展为 readonly/coding/ops 的只读集合，`exec` 仅 coding/ops/full；`permission_required` 加入 `exec`；ACP 的 kind/title/location 与系统提示同步覆盖新工具。新工具默认仍不在 `policy.allowed_tools` 中，必须显式 opt-in。
- 配置：新增 `[tools.list_dir]`、`[tools.grep]`、`[tools.git_status]`、`[tools.git_diff]`、`[tools.exec]`；校验忽略目录必须是单一 normal component，条目/文件/字节/匹配/输出上限必须 ≥1，per-tool timeout 不得超过全局预算。`timeout_ms` 默认继承 `budget.tool_timeout_ms`，`max_args_bytes` 默认不设上限、可按工具覆盖。
- 测试：`jessica-tools` 新增/扩展 read、list_dir、grep、git、exec 行为测试（含真实临时 git 仓库、参数注入拒绝、env 清理、超时、非零退出、symlink 逃逸、二进制/超大跳过、输出上限）；CLI 增加 catalog profile/隐藏原因/输出行数、permission 集合、ACP 映射、config 边界、runner 真实进程测试，以及真实 CLI E2E `native_tools_e2e`（exec 字面量参数、env 清理、非零退出、read_file offset/行号）与更新后的 `result_manager_e2e`/`tools_cli_e2e`/`acp_stdio`/`delegation_e2e`。
- 偏离/说明：1) `read_file` 的 offset 明确定义为行号而非字节偏移，避免多字节字符边界问题；2) `read_file` 保持纯文本输出，超阈值时由 9.4 Result Manager 转 artifact，未改成 rich JSON；3) `exec` 成功/失败统一返回 JSON 字符串，便于模型与 trace 读取 `exit_code`；4) `git_status`/`git_diff` 刻意不接受任何参数，牺牲灵活性换取注入面最小化；5) 新增原生工具默认不在 allowlist，且 `exec` 没有类似 bash 的独立 enable 门，安全边界由 profile、allowlist 与 ACP approval 共同承担；6) `examples/harness.toml` 未启用新工具，保持既有示例行为；7) 进程组/rlimits 仍属 9.3b，本次未实现。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 1061 passed, 0 failed, 2 ignored（含 `native_tools_e2e`、`result_manager_e2e`、`tools_cli_e2e`、`acp_stdio`）。

---

Task 9.6（已完成）：Policy v2、approval modes、`policy simulate`

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.7 的 globset/regex 工具，不实现 9.8 MCP/WASM descriptor；不改变 9.1 的 Catalog 暴露语义——policy 只对已暴露工具做参数级决策，未暴露工具仍是 `unknown tool`。
- `[policy]` 新增 `[[policy.rules]]`：`id`、`tool`（字符串或数组）、`path_glob`、`program`、`args_prefix`、`action`（`deny`/`ask`/`allow`）、`reason`。校验：id 唯一且非空；每条规则至少一个匹配条件；action 合法；path_glob 非空；program/args_prefix 只对 exec/bash 有意义，非法组合在加载期拒绝；未知字段拒绝。
- 评估顺序固定为：所有 deny → 所有 ask → 所有 allow → 默认拒绝；同一优先级按配置顺序取第一条。路径匹配前必须 canonicalize，并拒绝 symlink 逃逸；exec 匹配 program + args prefix + cwd；bash 不做命令字符串解析，只能按工具/审批控制；`delegate_task` 视为 destructive，必须审批；MCP/WASM 按 descriptor capabilities 保守处理。
- `path_glob` 在 9.7 依赖未批准前只能使用无依赖的最小匹配器（明确支持的 `*`/`**`/`?` 子集），不得声称支持未实现的字符类、转义或平台差异；9.7 批准 globset 后可替换实现，但必须保持既有测试语义或明确迁移。
- `[approval] mode = "ask" | "deny" | "allow"`：`ask` 在 CLI/ACP 发起一次性 allow/reject；`deny` 为 CI 默认，未显式 allow 的敏感工具直接拒绝；`allow` 自动批准 policy 允许的敏感工具，但仍记录审批事件。审批请求只含工具名、参数摘要、write_file/apply_patch 的 diff 摘要、exec 的 program/args/cwd，不含完整环境变量或 secrets。
- `jessica policy simulate --tool <name> --args '<json>'`：输出命中的规则 id、最终决策与原因；不得执行工具、不得创建 provider/事件库/memory/skills/MCP；未知工具、非法 JSON、无法 canonicalize 的路径返回结构化错误。`jessica governance explain` 可同 Task 提供，展示 effective policy/tools/profile/approval，但不得泄露 secrets。
- 兼容性：既有 `policy.allowed_tools` 继续作为顶层 allowlist，与 v2 规则的 deny/ask 组合；未配置 rules/approval 时保持 9.1 行为不变；默认拒绝仍是最终兜底。新增字段与模式必须有迁移说明和边界测试。
- 测试：deny 优先于 ask/allow；ask 优先于 allow；同优先级首条命中；path canonicalize/symlink 逃逸；exec program/args prefix；bash 不做字符串解析；delegate destructive；approval ask/deny/allow 的可观察行为；simulate 不执行工具且输出命中 id；配置重复 id/空条件/未知 action/未知字段拒绝。

完成标准：fmt/clippy/test 全绿；真实 CLI `policy simulate` E2E；README policy v2、approval 与迁移说明；未改 core、未新增第三方 crate。

实施记录（2026-11-22，实际耗时约 100 分钟，含一次上下文压缩后继续）：

- 完成情况：`jessica-cli` 新增 `policy_v2.rs`、`approval.rs`、`policy_command.rs`。`[policy]` 新增 `[[policy.rules]]`，支持 `tool` 字符串/数组、`path_glob`、`program`、`args_prefix`、`action`、`reason`，`deny_unknown_fields`；加载期校验 id 唯一非空、至少一个条件、action 合法、path_glob/program/args_prefix 非空、program/args_prefix 仅用于 exec/bash。`[approval] mode` 支持 ask/deny/allow，默认 ask。
- 评估：宿主侧 `PolicyV2` 在 harness allowlist 之后、permission gate 与执行之前评估。固定顺序为全部 deny → 全部 ask → 全部 allow → 敏感工具批准模式；同一优先级取配置顺序第一条。路径先做词法归一化，再 canonicalize 最近存在祖先；拒绝 `..`/symlink 逃逸、悬空 symlink、workspace 外路径与非 UTF-8。文件工具按 descriptor 的 `path_field` 匹配，`exec` 按 `program` + `args` 前缀 + canonical `cwd`，`bash` 从不解析命令字符串。最小 glob 仅实现 `*`/`**`/`?`，字符类、花括号与反斜杠按字面量处理并在 README 明示。
- 批准行为：无 `[approval]` 时保持 9.1 行为（ACP 对固定敏感集请求，CLI 无 gate 时运行已允许工具）；显式 `ask` 或任一 ask 规则在 CLI TTY 上启用专用 ask gate，以 stderr 一次性 `y/yes` 询问（120 s 超时），ACP 的 ask verdict 复用协议 gate，无 gate 时 fail closed。ask gate 与 legacy sensitive gate 分离，因此仅添加一条 ask 规则不会让未被规则提及的敏感工具开始询问。`deny` 拒绝敏感工具与 ask 规则，除非显式 allow；`allow` 自动批准但仍先写入 `PermissionDecided { Allow }`，写入失败则不执行。显式 `[approval]` 下 destructive MCP 也视为敏感；WASM 维持既有只读能力分类；`delegate_task` 保持 legacy 敏感且 destructive。
- 审批摘要：`exec` 仅 program/args/cwd，`bash` 仅 command，`delegate_task` 仅 task，`write_file` 返回 path、字节/行数与有界公共前后缀 diff（最多读取 256 KiB，且 canonical 后必须在 workspace 内）；未知/MCP 工具递归脱敏 secret/token/password/api_key/credential 等键并截断。ACP `rawInput` 改用该摘要，不再传原始完整参数。
- `policy simulate`：同一 inspection catalog（含 WASM manifest）评估合成 `ToolCall`，输出 tool/source/decision/rule/approval/reason。未知/隐藏工具、非法 JSON、非对象参数、不可 canonicalize 路径均结构化失败。命令在 `assemble_runtime` 之前处理，不创建 provider、事件库、memory/skills DB、MCP 连接，也不执行工具。
- 测试：config 规则/批准模式边界；`policy_v2` glob、优先级、首条命中、路径规范化与 symlink 逃逸、exec 匹配、bash 不解析、MCP/approval 组合；`approval` 摘要与脱敏；`governance` 集成测试覆盖 deny 不询问不执行、ask allow/reject、ask 无 gate fail closed、ask-only gate 不改变 legacy 敏感工具、deny/allow 模式、自动批准先记录后执行、allow 规则覆盖 deny 模式、MCP 敏感性与 9.1 兼容、symlink 逃逸、`delegate_task` 的 Delegation capability 为 destructive 且显式 ask 模式下拒绝后不执行；真实 CLI E2E `policy_cli_e2e` 覆盖 deny 命中输出、write_file 不落盘、非法 JSON/未知/隐藏工具、symlink 逃逸、exec program/args、重复 id 配置拒绝，并断言不创建 events/memory/skills/.jessica/artifacts。
- 偏离/说明：1) `jessica-core` 的冻结 `PolicyDecision` 只有 Allow/Deny，故 `ask` 作为 CLI 宿主内三态判定实现，未改 core，也未替换 harness allowlist；2) 计划中的“默认拒绝”按既有 allowlist/未知工具语义落地：未暴露工具仍拒绝，已暴露的非敏感工具在无规则命中时保持 9.1 的允许行为，避免破坏兼容性；敏感工具由显式批准模式或 legacy gate 决定；计划文字中的“`deny` 为 CI 默认”未做成 preset 的隐式默认，否则会改变未写 `[approval]` 的既有 `ci` 配置行为；当前默认仍为 `ask`，CI 需显式写 `[approval] mode = "deny"`；3) 校验允许 `program`/`args_prefix` 与 `bash` 组合以匹配计划文字，但评估器只在 `exec` 上匹配，bash 规则因不解析命令而永不命中；4) `jessica governance explain` 为可选项，本 Task 未实现；5) `path_glob` 仅对 Native 工具求值，MCP/WASM 的参数不当作 workspace 路径；6) 显式 `ask` 需要 gate，非 TTY CLI 无内置 gate 时 fail closed；7) `build_turn` 改为 async 以 canonicalize workspace root，属 CLI 内部装配变化；8) `DESIGN.md` 保持未跟踪，未修改 `jessica-core`，未新增第三方 crate。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 1098 passed, 0 failed, 2 ignored（含 `policy_cli_e2e` 6 个 E2E 与 `governance` 新增集成测试）。

---

Task 9.7a（已完成）：原生工具 B（无依赖子集）— `apply_patch`、`edit_file`

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 9.8 MCP/WASM 对齐；不改变 9.5 已交付工具语义。
- `apply_patch` 使用最小 unified diff 解析，先校验全部 hunk 再原子应用，失败不写文件；`edit_file` 做字面量 old/new 替换并要求显式出现次数（0 次报错、1 次可省略、多次必须显式次数且不匹配即报错），保持 UTF-8 边界与原子写。
- 写安全：同目录 `create_new` 临时文件 + `flush`/`sync_all` + `rename` 原子替换，保留原权限，失败清理临时文件且不触碰原文件；`apply_patch` 的 `dry_run` 只校验和描述结果；`edit_file` 在多处匹配且未指定次数时拒绝而非猜测。
- 两者都必须接入 9.6 policy 的路径规则、approval、descriptor、limits，并拒绝 `.git`、`.jessica`、`.ssh` 组件、`.env`/`.env.*`、SSH 私钥名与 `.pem`/`.key`/`.p12`/`.pfx` 扩展名等受保护路径，除非 policy 显式 allow。
- `apply_patch` 仅支持单文件、LF、`---`/`+++`（可带 `a/`/`b/` 前缀）与 `@@` hunk；拒绝多文件、rename、`/dev/null`、CRLF、`\ No newline at end of file` 与 `.`/`..` 组件；patch 上限 1 MiB，目标文件上限 16 MiB。
- 不实现 9.7b 的 `glob` 与 regex grep；不得把 `globset`/`regex` 写入 workspace `Cargo.toml`，也不得用自实现正则/glob 冒充。

完成标准：无新依赖下 fmt/clippy/test 全绿；apply_patch/edit_file 的真实行为测试与真实 CLI E2E；README 工具表、审批与安全边界更新；未改 core、未新增依赖。

实施记录（2026-11-22，实际耗时约 90 分钟）：

- `jessica-tools` 新增 `atomic.rs`（同级临时文件、`write_all`/`flush`/`sync_all`、复制原权限、`rename`、unix 下 best-effort 父目录 fsync、失败清理）、`edit.rs`（`EditFileTool`、16 MiB 上限、UTF-8 校验、`match_indices` 计数、`replacen`、显式出现次数）与 `patch.rs`（`ApplyPatchTool`、1 MiB patch 上限、unified diff 解析/校验/应用、`dry_run`、`PatchSummary`/`summarize_patch`）。
- CLI：`policy_v2` 的 `matching_path` 对 `apply_patch` 解析 patch 目标路径，格式错误映射为结构化 `InvalidPatch`；新增受保护路径默认拒绝（在 deny 规则之后、ask 规则与批准兜底之前；存在匹配的显式 allow 规则时跳过，因此 ask 规则不能把默认拒绝降级为提示，`approval.mode = "allow"` 也不能自动批准）。`approval` 新增两种仅含元数据的摘要，绝不回显 patch/文件内容。`descriptor` 将两者标记为 `writes_files`、`destructive`，`edit_file` 的 `path_field` 为 `path`，`apply_patch` 为 `patch`。`governance` 将两者加入固定敏感集。`config` 新增 `[tools.apply_patch]`、`[tools.edit_file]` 的 timeout/max_args_bytes 校验。`catalog` 的 `coding` profile 纳入 `write_file`、`apply_patch`、`edit_file`。系统提示词同步列出新工具。
- 默认 `NATIVE_TOOLS` allowlist 仍为 `bash`、`read_file`、`write_file`，因此既有配置的工具面不变；新工具需同时在 `policy.allowed_tools` 与兼容 profile 中才会暴露。
- 测试：`jessica-tools` 覆盖成功替换、0/1/多次出现、次数不匹配、UTF-8 emoji、非法 UTF-8、symlink 逃逸、无临时文件残留；patch 成功、上下文不匹配、dry-run 不写、插入、无结尾换行保持、`..` 逃逸、symlink 逃逸、多文件拒绝与摘要计数。CLI 覆盖 descriptor/capability、敏感集审批拒绝后不执行、审批摘要不泄露内容、config 边界，以及真实 `policy simulate` E2E：`.env` 对 `edit_file`/`apply_patch` 默认 deny 且不写盘、显式 allow 规则覆盖后只报告 allow、不创建运行时状态。
- 偏离/说明：1) `apply_patch` 限定单文件以保持原子性与路径匹配可判定，多文件留待后续；2) 受保护路径门只加在 9.7a 两个编辑工具上，`write_file` 保持 9.5/9.6 语义不变；3) 受保护路径判定基于 canonical 后的 workspace 相对路径组件，不读取文件内容或环境；4) `dry_run` 只做校验与结果描述，不写文件、不写 trace；5) 未新增依赖，`glob`/regex grep 留给 9.7b；6) `DESIGN.md` 保持未跟踪。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 1125 passed, 0 failed, 2 ignored（含 `policy_cli_e2e` 7 个 E2E、ask 规则不得绕过受保护路径的回归测试，以及 `jessica-tools` 新增 edit/patch 行为测试）。

---

Task 9.7b（待依赖审批）：`glob`、regex grep

范围与约束：

- 仅在用户批准 `globset`/`regex` 后实施：`glob` 使用 `globset`；`grep` 增加 regex 模式，限制 pattern 长度、文件数、单文件大小、总输出与匹配耗时，避免灾难性回溯；未批准前只保留 9.5 的字面量 grep 与无 glob 的目录遍历，不得用自实现正则或 glob 冒充。
- `glob`/regex 不得跟随 symlink 逃逸 workspace；结果必须有条目/字节上限并截断提示；regex 编译失败、超时、输出超限均返回结构化错误，不 panic。
- 依赖审批门：未批准前不得把 `globset`/`regex` 写入 workspace `Cargo.toml`。
- 测试：glob/regex 仅在批准后加入测试；依赖未批准时 `cargo tree`/Cargo.toml 不得出现相关 crate。

完成标准：在依赖批准后完成 glob/regex 行为测试，并保持既有字面量 grep 语义或明确迁移。

---

Task 9.8（2026-11-22 完成）：MCP/WASM descriptor、子 Agent 工具安全对齐

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；不实现 MCP HTTP/SSE、WASM 签名/发布者验证、嵌套委派、动态发现或预算分片（全部留 Deferred）。
- MCP：从 tool annotations 推导 descriptor：`readOnlyHint` → `read_only`，`destructiveHint` → `destructive` 且必须审批，`openWorldHint` → `network` 且保守审批；缺失或冲突时按最保守解释。MCP 大结果走 9.4 Result Manager；仍只支持 stdio；MCP 工具不自动 allow，必须进入 allowlist；CLI 配置后置，不阻塞本 Task。
- WASM：Manifest v2 增加 descriptor 元数据（version、annotations、limits）；保持默认安全边界：无写目录、无网络、只读 preopen、每次调用新实例、fuel/wall/memory 限制；大结果走 9.4 Result Manager；v1 manifest 继续可用并按保守 capability 处理；签名与发布者验证后置。
- 子 Agent：保持单层委派；`delegate_task` descriptor 标记 destructive 且必须审批；子 Agent 的 bash/exec 必须走同一 9.2/9.3 Supervisor（env allowlist、timeout、输出上限、进程组），不得继续使用裸工具 + 固定超时；子工具结果同样走 9.4 Result Manager；不做嵌套委派、动态发现或预算分片。
- 子 Agent 实现边界：不改 core；如需 runtime 支持，只做 additive 内部 API（不是 core 公共契约），并在 Task 中说明；CLI 负责把受治理的 child tool registry 注入 `DelegateTaskTool`，确保子 Agent 不能通过 profile/allowlist 获得父 Agent 未暴露的工具。
- 审批与审计：MCP destructive、WASM 声明 destructive、`delegate_task` 的审批请求与决策必须可观察（CLI 事件/trace 既有能力范围内）；不得把 annotations 当作可信自述，宿主保留最终分类权。
- 测试：MCP `readOnlyHint` 映射、`destructiveHint` 默认审批/拒绝、大结果 artifact；WASM manifest v2 校验与 v1 兼容、大结果 artifact；子 bash 环境不含 provider key、子 bash 超时被 supervisor 终止、子结果截断/artifact、`delegate_task` 审批记录；子 Agent 不能获得父未暴露工具。

完成标准：fmt/clippy/test 全绿；真实 CLI/ACP E2E 至少覆盖一个 MCP 工具与一个子 Agent 工具调用；README/示例更新 MCP/WASM descriptor、子 Agent 安全边界；未改 core、未新增第三方 crate。

实施记录（2026-11-22，实际耗时约 150 分钟）：

- `jessica-tools`：`WasmTool` manifest 支持可选 `version`（缺省 1，仅接受 1/2）与 v2 `annotations`（三项必填、拒绝未知字段）；v1 带 annotations、未知版本、`read_only` 与 `destructive`/`open_world` 冲突均在加载期拒绝；新增 `manifest_version()`/`annotations()`，沙箱策略（无写目录、无网络、只读 preopen、每次调用新实例、fuel/wall/memory/table/instance 限制）不变。
- CLI descriptor/catalog：新增 `ToolAnnotations`/`ToolMetadata`；`Capabilities::for_metadata` 按来源分配：MCP 仅在 `readOnlyHint=true` 且 `destructiveHint=false` 时视为只读，缺失或冲突 destructive 保持 destructive，缺失 `openWorldHint` 保持 network；WASM 以 `read_only = !destructive && !open_world` 从 v2 声明升级，v1 保持只读基线；Native/Delegation 忽略 annotations。Catalog 对 MCP/WASM 的 destructive 或 network descriptor 强制 `ApprovalPolicy::Interactive`，`PolicyTool::from_descriptor` 将其纳入 sensitive；WASM descriptor 版本报告为 `manifest-v1`/`manifest-v2`。
- MCP：`parse_tool` 读取 `readOnlyHint`/`destructiveHint`/`openWorldHint`（malformed annotations 视为缺失，不阻断发现）；`McpSession::tools()` 返回带 metadata 的 `CatalogCandidate`，ACP 原样传入每轮 catalog；MCP 仍仅支持 stdio、仍必须进入 `policy.allowed_tools`，结果继续经 `GovernedTool` 的 Result Manager/ResultSizeGuard。
- 子 Agent：`NativeChildToolFactory` 接收 `HarnessConfig` 与父轮 `ResultManager`；child `bash` 使用 supervisor 的 env allowlist、per-tool deadline、per-stream output cap，child `exec` 额外通过 `HarnessCommandRunner`；每个 child 工具结果由 `ManagedChildTool` 交给 Result Manager。父能力集来自 `base.exposed_names()`/`base.exposed_native_names()`，子 Agent 只能拿到父已暴露的 native 子集，`delegate_task` 与 WASM/MCP 始终被移除；runtime 未新增公共契约。
- 测试：`jessica-tools` 覆盖 v1 兼容、v2 注解、版本/冲突/未知字段拒绝；CLI 单测覆盖 MCP annotations 保守映射、WASM 注解升级、descriptor 版本、child factory 的 supervisor 输出上限/超时/artifact/cleared env；`acp_stdio` 新增 destructive MCP 审批拒绝（server 未收到 tools/call）、read-only MCP 大结果 artifact、`delegate_task` 审批的 `PermissionDecided` trace 记录；`delegation_e2e` 新增 child bash 无 provider key、supervisor deadline 终止、child 大结果 artifact，父未暴露工具被拒的既有用例继续通过；`wasm_tool_e2e` 新增大结果 artifact。
- 偏离/说明：1) 进程组 `SIGTERM`→`SIGKILL` 与 rlimits 仍属未获批的 9.3b，本次 child bash/exec 复用现有 supervisor（env allowlist/timeout/output cap），未实现进程组，README 与 PLAN 明确该边界；2) runtime 仍保留固定的 5s child tool 外层超时，child bash/exec 的实际截止时间取配置值与 5s 的较小者；3) malformed MCP annotations 按“缺失”处理并采用保守基线，而非拒绝整个 server；4) WASM v2 的 `read_only` 仅用于冲突校验，descriptor 的 read-only 由 `!destructive && !open_world` 推导，避免单独声明 `read_only: false` 反而虚增能力；5) 子 Agent 沿用既有单次 `delegate_task` 审批语义：CLI 构造的受治理 registry 负责父已暴露 native 子集的收窄、supervisor 限制与 Result Manager，不在每次 child 工具调用上重新执行 CLI policy v2 规则或受保护路径门，与“单次委派审批覆盖子运行”的既有契约一致；6) 未修改 `jessica-core`，未新增第三方 crate，`DESIGN.md` 保持未跟踪。
- 验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 1145 passed, 0 failed, 2 ignored。

---

Phase 9 完成后仍保留在 Deferred（不在上述 Task 内提前实现）：

- 并行工具执行（需要 core 变更）；
- 多 Agent 深度、静态 manifest、预算分片；
- MCP HTTP/SSE、MCP resume/per-session workspace；
- WASM 插件签名与发布者验证；
- 工具进度流；
- 自动技能激活；
- `write_file` 原文件 checkpoint 与 `revert_file`（除非后续单开 Task）。

---

Phase 10：Web Search 能力（2026-11-22 规划；用户决定暂缓，尚未实施）

当前状态：Deferred。2026-11-22 用户明确要求暂缓 Phase 10，原因是当前没有 Brave Search API key；在恢复条件满足并重新确认前，不开始 10.1a/10.1b，不向 `jessica-tools` 新增 `reqwest`/`tracing` 依赖，不新增搜索工具、配置或第三方 crate。恢复条件：用户提供可用 Brave API key，或明确选择 SearXNG endpoint/自托管方案，并重新确认任务边界。

背景：用户希望增强 Jessica 的联网检索能力。方案采用宿主内置的原生 `web_search` 工具，而不是让模型通过 `exec`/`curl` 或非官方 HTML 抓取访问搜索服务；搜索后端可插拔，先交付 Brave，再补 SearXNG。该能力属于 open-world 网络访问，必须保持默认关闭、显式 allowlist、审批与父 Agent 隔离。

目标：

- 提供稳定 JSON schema 的 `web_search`，模型只需提交 `query` 与可选 `max_results`。
- 复用 workspace 已批准的 `reqwest` 与 `tracing`；不修改 `jessica-core`，不新增第三方 crate。
- 搜索结果经过宿主的 Catalog、Policy v2、approval、timeout、Result Manager，与现有工具治理一致。
- API key 只从环境变量读取；query/结果明确标记为不可信外部内容；不自动抓取结果页面。

非目标：

- 不实现 `web_fetch`、HTML/JS 渲染、页面正文抓取或搜索引擎页面爬取。
- 不实现搜索缓存、限流、重试/熔断、结果排序增强或引用格式。
- 不允许子 Agent 使用 `web_search`。
- 不接入 Tavily/Exa/Google CSE 或 provider 原生搜索；这些留 Deferred。

Phase 10 任务序列（暂缓；恢复后仍须逐项确认与验收）：

| Task | 内容 | 改 core | 新依赖 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| **10.1a** | 原生 `web_search`、Brave 后端、配置/治理/审批、父 Agent 隔离、测试与文档 | 否 | 无（复用 workspace 已有 `reqwest`、`tracing`） | 9.1/9.4/9.6/9.8 | 暂缓 |
| **10.1b** | SearXNG 后端（可自托管、可选 Bearer），复用 10.1a 的工具与治理 | 否 | 无 | 10.1a | 暂缓 |

说明：本 Phase 当前整体暂缓，不执行任何实现。恢复时默认先做 Brave；若本地只有 SearXNG，可在恢复开工前把 10.1a 的目标后端改为 SearXNG、10.1b 改为 Brave；任务边界、治理、配置与验收不变。`web_search` 默认不启用，也不加入默认 allowlist；必须显式配置 `[web_search].enabled = true`、加入 `policy.allowed_tools`，并使用兼容的 `tools.profile`（仅 `full`）。

Task 10.1a（暂缓）：原生 `web_search` 工具与 Brave 后端

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate。`jessica-tools` 仅新增对 workspace 已有 `reqwest.workspace = true` 与 `tracing.workspace = true` 的引用；不得引入新的搜索/HTML/HTTP 库。
- 新增 `jessica-tools/src/web_search/`：`WebSearchTool`、provider-agnostic backend trait、Brave adapter、结构化错误与有界 HTTP 读取。工具名固定为 `web_search`，schema 和 description 由工具提供，结果格式为 JSON。
- 不实现搜狗/Google/DuckDuckGo 页面抓取，不实现 SearXNG（10.1b），不实现 `web_fetch`、页面正文抓取、缓存、限流或重试，不允许子 Agent 使用搜索，不改变任何既有工具语义。
- 参数：`query` 必填，trim 后非空且不超过 512 UTF-8 字节；`max_results` 可选，1–10，实际不超过配置的 `max_results`。拒绝未知字段、控制字符和非法类型；不能由模型指定 endpoint、URL 或 HTTP header。
- 结果 JSON 至少包含：`provider`、`query`、`untrusted: true`、`results[]`（`title`、`url`、`snippet`，可选 `published_at`）、`result_count`、`truncated`。title ≤ 300 B、URL ≤ 2048 B、snippet ≤ 1024 B；只接受 `http`/`https` URL，拒绝带用户名/密码的 URL；去重后按 provider 顺序输出。
- 原始 provider 响应上限默认 2 MiB，工具输出上限默认 64 KiB；超限返回结构化错误或截断并标记 `truncated`，不得 panic，不得把原始 HTML/超大 body 回显给模型。大结果继续经过 9.4 Result Manager。
- 配置 `[web_search]`：`enabled` 默认 `false`；`backend = "brave"`；`endpoint` 可选（Brave 默认 `https://api.search.brave.com/res/v1/web/search`）；`api_key_env` 默认 `BRAVE_SEARCH_API_KEY`；`max_results` 默认 5、范围 1–10；`safe_search` 为 `off`/`moderate`/`strict`，默认 `moderate`；`max_response_bytes` 默认 2 MiB；`max_output_bytes` 默认 64 KiB 且不得超过 `budget.max_result_bytes`；`allow_private_endpoint` 默认 `false`。所有数值在加载期校验；未知字段拒绝。
- 新增 `[tools.web_search]` 的 `timeout_ms` 与 `max_args_bytes`，接入现有 `timeout_for`；timeout 默认继承 `budget.tool_timeout_ms`，不得超过全局预算。`[web_search]` 与 `[tools.web_search]` 均为可热加载配置，下一 turn 生效。
- Brave 请求：GET，`X-Subscription-Token` 从 `api_key_env` 读取，`Accept: application/json`，参数包含 `q`、`count`、`safesearch` 与 `result_filter=web`；响应必须是 JSON，解析 `web.results` 并归一化。401/403、429、5xx、非 JSON、超时、超大响应、缺少 key 都返回结构化 `ToolResult` 错误；错误信息不得包含 key、请求头或完整响应 body。
- 密钥与隐私：key 只存在环境变量；工具的 Debug 必须脱敏；日志只记录 backend、status、耗时、结果数，不记录 query 原文、key 或完整结果；query 会进入既有 trace/event store，README 必须明确这一隐私边界。
- 安全：endpoint 只由宿主配置指定；只允许 `http`/`https`，拒绝 userinfo、fragment 和非法 URL；默认拒绝 loopback/私网地址，`allow_private_endpoint = true` 才允许本地/内网 SearXNG/测试服务器；禁用 HTTP 重定向；不自动访问结果 URL，不执行结果中的指令；工具描述和结果均明确标记外部内容不可信。
- 治理：将 `web_search` 加入固定敏感集与 Policy v2 的 legacy sensitive 判定；descriptor 标记 `network = true`、`destructive = false`、result format 为 JSON；Catalog 仅在 `tools.profile = "full"`、allowlist 允许且 `[web_search].enabled = true` 时暴露，未启用时 `tools list --all` 输出新增的 `WebSearchDisabled` 原因。审批沿用 9.6 的 `[approval]` 语义；README 与示例配置必须显式 `mode = "ask"`，并说明未配置 approval 的 CLI 会在 allowlist 后直接执行，因此生产配置不得省略审批配置。Policy v2 的 deny/ask/allow 规则可覆盖默认敏感判定。
- 父 Agent 隔离：从子 Agent 可用的 native 集合中排除 `web_search`；`NativeChildToolFactory` 也显式拒绝该名称作为纵深防御；测试必须证明子 Agent 请求搜索时 mock 搜索服务器收到 0 次请求。
- 测试：配置默认值/边界/未知字段/热加载；本地 `TcpListener` mock Brave 服务器验证方法、header、query 编码、`count`、safe search、结果解析；401/403、429、5xx、非 JSON、超大响应、超时、缺少 key；空/超长 query、`max_results` 边界；URL 校验/去重/字段截断/总输出截断；Debug 与错误信息不泄露 key；Catalog 的 hidden reason、full-only profile、approval、`tools list/inspect`；`policy simulate` 返回 ask；子 Agent 零请求；真实 CLI E2E 使用 mock provider + mock Brave server，不依赖真实网络。
- 文档：更新 README 的工具表、profile、审批、安全边界、`[web_search]` 配置与隐私说明；更新 `examples/harness.toml`（注释示例，默认不启用）；在 PLAN 完成记录中写实际耗时与偏离；不把 key 写入示例或测试文件。

完成标准：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 全绿；新增行为测试和真实 CLI E2E 通过；未修改 `jessica-core`，未新增第三方 crate，默认工具面不变。

禁止：不得用 `unwrap`/`expect` 处理配置、HTTP 或 JSON；不得使用阻塞 IO；不得用 `println!` 输出工具日志；不得把搜索结果当可信指令执行；不得自动抓取结果 URL；不得通过 `exec`/`curl` 绕过本工具治理；不得修改 9.1–9.8 已验收语义。

Task 10.1b（暂缓）：SearXNG 后端

范围与约束：

- 不修改 `jessica-core`；不新增第三方 crate；复用 10.1a 的 `WebSearchTool`、结果归一化、配置校验、审批与测试辅助。
- `backend` 增加 `searxng`；SearXNG 必须显式配置 `endpoint`（例如 `http://127.0.0.1:8080/search`），否则加载期失败。`api_key_env` 可选；配置后以 Bearer header 发送，不写入配置值。
- 请求参数使用 `format=json`、`q`、`safesearch`（0/1/2）与分页/数量参数（以 SearXNG 实际 JSON API 为准）；解析 `results[]` 的 `title`/`url`/`content` 与可选 `publishedDate`。SearXNG 未启用 JSON 格式时返回清晰错误提示。
- 本地/内网 endpoint 必须配合 `allow_private_endpoint = true`；默认仍拒绝私网。不得默认使用公共 SearXNG 实例，不实现实例发现或自动切换。
- 测试使用本地 mock SearXNG server，覆盖无 key、Bearer key、safe search 映射、结果解析、非 JSON/HTML 错误、超时、超大响应、私网开关、endpoint 缺失；不得依赖真实公网实例。
- README 增加 SearXNG 配置示例、自托管安全边界与隐私说明；示例配置默认关闭。
- 完成标准：fmt/clippy/workspace test 全绿；Brave 既有测试不回退；未修改 core、未新增第三方 crate。

Phase 10 恢复实施后仍保留在 Deferred（不在上述 Task 内提前实现）：

- `web_fetch` 与 SSRF 安全的页面正文抓取、HTML 转文本、DNS 重绑定防护。
- Tavily/Exa/Google CSE 等其他搜索后端，以及 provider 原生搜索。
- 搜索缓存、限流、重试/退避、配额计量与成本展示。
- 子 Agent 搜索权限链与逐子工具审批。
- 语言/地区/时间范围/站点过滤与结果排序策略。
- 搜索结果引用格式、信任边界/脱敏/trace 保留策略，以及需要真实 API key 的 live smoke 测试。

---

Phase 11（E0 已完成；E1a/E1b engine 侧切片、E1c CLI 治理原语接入与 E1d descriptor/patch/artifact 对齐已完成；E2 journal/pump/trace、E3a engine 侧完整 turn/acceptance/resume、E3b delegation/CLI 委托/config 拆分与 E4 单一装配已完成；E1 剩余 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属、E5 API 冻结为候选，未排期）：jessica-engine 受治理运行时抽取

当前状态：E0 已于 2026-11-28 完成，仅新增/加强 `jessica-cli` 测试并做 mutation check，未改生产路径。2026-11-28 用户确认方向并要求写入 `PLAN.md`，随后分别授权开始 E0、E1、E1c 与 E1d；E1 因范围较大按计划拆为 E1a/E1b，E1a 已完成 crate/API/端口/配置骨架，E1b 已完成 engine 侧治理原语与最小 `run_turn` 垂直切片，E1c 已完成 CLI 治理原语接入，E1d 已完成 descriptor 元数据迁移、engine/CLI patch 解析等价性行为测试与 engine artifact retention/GC（均为 2026-11-28）。E1c 后 CLI 依赖 engine，并复用 engine 的 `GovernanceState`/`TokenCountingProvider`；CLI `GovernedTool` 保留为宿主适配层，把 allowlist、CLI `PolicyV2`、CLI `ResultManager` 与权限门适配为 engine 端口，治理顺序由 engine 执行。E1d 后 CLI descriptor 类型改为 re-export engine 类型，engine `ToolCatalog` 保留 descriptor metadata/limits，engine `PolicyV2` 的 patch 头与 hunk 结构校验已通过 CLI/engine 等价行为测试，engine `ArtifactStore` 支持 retention 与字节预算 cleanup。CLI/ACP 可观察行为未变；全 workspace 1193 个测试通过（CLI 单元 407、engine 47）。E1 仍未完成：CLI 仍保有 `PolicyV2`/`ArtifactStore`/`ResultManager`/`ToolCatalog` 的具体实现，其中 catalog profile 过滤/hidden reason/exposure budget、artifact sidecar/record 与 `ReadArtifactTool` 尚未迁入 engine；engine policy 只做结构与路径校验，不把 hunk 应用到文件，实际上下文匹配与写入仍由执行工具负责。E2 已完成 journal/pump/EventRecorder 迁入与 `TraceStore`/`TracePolicy::Required`（2026-11-28）。E3 因范围较大按 PLAN 允许项拆为 E3a/E3b：E3a 已完成 engine 侧 `run_turn_request`/`resume_turn`、固定初始消息顺序、memory/skill 每轮各 recall 一次与完成后 fail-open 后处理、acceptance 重试与每 attempt 独立 session、最终失败/治理 trip 的外层 session、budget reason 覆盖与基于已记录证据的 resume（2026-11-28）；E3b 已完成 delegation 父侧装配、`ChildToolFactory`、CLI `CliRuntime` 委托 engine、`HarnessConfig`/`EngineConfig` 拆分、sensors/`PumpConsole` 适配器并删除 CLI 并行 turn 装配，E3 全部完成（2026-11-28）。E4 已完成：删除 CLI 的 `events`/`pump`/`replay` 垫片，离线确定性 replay 迁入 engine `pub mod replay`，CLI/ACP 只经 `Engine` 执行 turn；`rg` 证明 `build_turn`/`GovernedTool`/治理链只在 engine，CLI 不再直接调用 `AgentLoop`（2026-11-28）。E1 剩余对齐与 E5 仍是候选、未排期、未开工；每个 Task 开工前必须单独确认范围、依赖与验收，且一次会话只做一个 Task。E0–E4 不得修改 `jessica-core` 公共 API；若实施中发现必须修改，先停下询问用户并单开 Task。E1 新增 workspace crate 与更新 `CLAUDE.md` 依赖表已获用户授权并完成。

背景：当前完整受治理流程只存在于 `jessica-cli`。`jessica-cli` 是 binary-only crate（`Cargo.toml` 只有 `[[bin]]`，`src/main.rs` 以 `mod` 声明模块，没有 `lib.rs`），`build_turn`、`run_turn_request`、`run_attempt`、`GovernedTool`、`TurnJournal`、`EventPump`、acceptance、memory/skill 调用点、delegation 装配、resume 与 shutdown 均为私有或 `pub(crate)`。`jessica-runtime` 只提供裸 `AgentLoop`，`jessica-harness` 只提供 middleware/guard/policy/sensor 骨架；二者都不定义“一轮受治理会话”的完整顺序与不变量。结果是 embedder 只能依赖 CLI 内部、复制装配逻辑，或绕过治理直接调用 `AgentLoop`；治理是否生效取决于调用方的装配纪律，而不是类型/API 合同。

目标：

- 新增 `jessica-engine`：拥有单轮/会话生命周期、治理链顺序、journal/pump、acceptance、memory/skill 调用点、delegation 父侧装配、resume 与 shutdown。
- CLI 与 ACP 降为适配器；`jessica-runtime` 保持裸内核，`jessica-harness` 保持通用中间件。
- 外部 crate 只通过 `jessica-engine` 的公共 API 就能运行完整受治理 turn；绕过治理在安全 API 上写不出来。
- 行为保持不变：事件顺序、trace schema（v1–v3）、legacy+trace 同事务、错误分类、CLI/ACP 输出与恢复语义均不得改变。

非目标：

- 不把终端渲染、TOML 解析、provider 构造、WASM/MCP 构造、ACP 协议或信号处理搬进 engine；这些仍由适配器/CLI 负责。
- 不新增 context compression/lineage、prompt caching、并行工具执行、远程执行后端、plugin/eval/telemetry、gateway/TUI/cron 等能力；这些是独立能力 Task，不借 E0–E5 夹带。
- 不新增第三方 crate；不修改 `jessica-core`；不改变现有配置格式或默认值。
- 不为“库化”顺手重构无关模块；发现的问题记录 Deferred。

Engine 目标合同（11 条不变量；E0 固化当前可观察子集，E1–E3 迁入目标字段，E5 冻结）：

1. 每轮一次配置快照：`run_turn` 开头取一次配置，`build_turn`、`initial_messages`、治理、pump 共用；热更新从下一轮生效。
2. 初始消息顺序固定：system prompt → memory recall → skill recall → history → user input；memory/skills 每轮各 recall 一次，不因 acceptance retry 重放。
3. `begin_turn` 必须先于任何 provider 调用；trace 写入失败不得继续模型工作。
4. Acceptance 重试复用同一份初始消息；每次 attempt 使用独立 session；最终失败时外层 session 记录该失败。
5. 每个顶层工具调用必须经过 `GovernedTool`，顺序固定：`max_args_bytes` → allowlist policy → PolicyV2 → approval gate → tool timeout → `ResultManager` → `ResultSizeGuard`；任一步拒绝都不得执行 inner tool。子工具按第 10 条收窄，顶层 `delegate_task` 的审批覆盖整段子 run。
6. Provider 必须经过 `TokenCountingProvider`；budget 触发后 `SessionEndReason` 必须覆盖为 `BudgetExceeded`，不得被后续 reason 掩盖。
7. `GovernanceState::emit` 先 sink 后 broadcast；`TurnJournal::finish_turn` 的 ack 是 turn 成功边界；未 ack 不得报告成功。
8. `EventPump` 从事件重建 transcript、运行 guards/sensors、检测 broadcast lag；lag 必须标记并在 `TurnOutcome` 暴露。
9. Memory/skills 只在 `Completed` 且未 tripped 后调用；失败 fail-open（tracing warning），不得改变 turn reason。
10. 子工具必须收窄到父 Agent 暴露的 Catalog 能力；顶层 `delegate_task` 的 approval 覆盖整段子 run。逐子工具 policy/approval 是单独 Task，不在 E0–E5 内改变。
11. `TurnOutcome` 携带 reason/messages/last_output/turn_start/turn_id/lagged；`TraceIncomplete` 是 `EngineError`，不是 outcome；不得把未闭合 trace 静默当作成功。

边界与依赖方向：

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| `jessica-engine` | turn 生命周期、治理顺序、journal/pump、acceptance、memory/skill 调用点、delegation 父侧装配、resume | TOML、provider 构造、WASM/MCP 构造、终端/ACP 协议、信号处理、工具实现 |
| `jessica-cli` / ACP 适配器 | 参数与配置、provider/tools/sandbox/MCP/WASM 构造、终端与 ACP 输出、审批 UI、端口实现 | 第二轮受治理装配、绕过 engine 的 turn 路径 |
| `jessica-runtime` | 裸 `AgentLoop`、provider/tool 循环、delegation 执行器 | engine 级顺序与不变量 |
| `jessica-harness` | middleware、guard、policy、sensor 的通用骨架 | 具体装配与 turn 生命周期 |

```text
jessica-cli ──> jessica-engine ──> jessica-core
   │                 ├──> jessica-runtime
   │                 ├──> jessica-harness
   │                 └──> jessica-memory
   └──> jessica-providers / jessica-tools / jessica-sandbox
```

端口（由适配器实现，engine 只定义 trait 并决定调用顺序）：

- `LlmProvider`（`jessica-core` 已有）、`ToolProvider`、`TraceStore`、`MemoryPort`、`SkillPort`。
- `ToolPermissionGate`、`TurnObserver`（UpdateSink）、`PumpConsole`。
- `ChildToolFactory`（`jessica-runtime` 已有）、`AcceptanceCheck`（`jessica-core` 已有）、`SystemPromptSource`、`IdSource`。

Phase 11 任务序列（E0、E1a–E1d、E2、E3 与 E4 已完成；E1 剩余对齐与 E5 候选，未排期）：

| Task | 内容 | 改 core | 新依赖 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| **E0** | 行为基线：审计并补齐 11 条不变量的 characterization tests，最终不改生产路径 | 否 | 无 | 无 | 已完成（2026-11-28） |
| **E1** | 新增 `jessica-engine` 骨架、端口、治理原语与最小 `run_turn` 垂直切片 | 否 | 无（仅 workspace 已批准依赖） | E0 | 部分完成（E1a/E1b engine 切片、E1c CLI 治理接入、E1d descriptor/patch/artifact 对齐 2026-11-28；CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 仍属 CLI） |
| **E2** | 迁入 journal/pump/EventRecorder，落地 `TraceStore` 与 `TracePolicy::Required` | 否 | 无 | E1 | 已完成（2026-11-28） |
| **E3** | 完整 turn 编排、acceptance、memory/skills、delegation、resume，CLI 委托 engine | 否 | 无 | E2 | 已完成（2026-11-28；E3a engine turn/acceptance/memory/skills/resume，E3b delegation/CLI 委托/config 拆分与适配器） |
| **E4** | CLI/ACP 适配器收薄，删除重复装配，确保只有一条受治理路径 | 否 | 无 | E3 | 已完成（2026-11-28） |
| **E5** | 冻结 engine 公共 API、文档、compile-fail doctest 与依赖检查 | 否 | 无 | E4 | 候选 |

说明：E0 已完成，E1–E4 的重构已有行为基线；E5 只在 E4 完成后开始。E1–E4 任一步发现必须修改 `jessica-core` 时立即停止，记录原因并单开 Task；不得用 CLI 侧包装绕过 core 契约。

Task E0（已完成，2026-11-28）：行为基线——把受治理流程固化为 characterization tests

范围与约束：

- 最终交付只改 `jessica-cli` 的测试代码与测试支持代码，不改生产路径；优先复用现有测试（`app.rs`、`governance.rs`、`journal.rs`、`pump.rs` 已有覆盖），只补 11 条合同中缺失或断言不足的部分。
- 测试必须断言可观察行为：事件顺序、工具是否执行/是否产生副作用、trace 行与事务边界、`TurnOutcome` 字段、错误分类、provider 调用次数；禁止“调用后断言 Ok”。
- 审计并补齐：配置快照跨轮生效、初始消息顺序、`begin_turn` 先于 provider、acceptance 重试复用初始消息、工具治理顺序、budget 覆盖 reason、sink 先于 broadcast、`finish_turn` ack 边界、pump lag、memory/skill fail-open、子工具收窄、resume 不重放已记录工作。
- 不新增第三方 crate；不新增 `lib.rs`；不修改 `jessica-core`；不改变任何 CLI/ACP 行为。
- 对关键不变量做 mutation check（临时反转顺序或去掉 guard，确认测试失败后还原）；mutation 不得提交，最终生产代码必须与 E0 开始前一致。

完成标准：当前可观察的 11 条合同均有行为断言（`turn_id`/`last_output` 等 engine 目标字段在 E1/E3 落地时补断言）；有意破坏任一关键顺序时至少一条测试失败；`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。

禁止：把测试写成对实现细节的自证；为了测试暴露 `pub(crate)` API 之外的公共接口；在 E0 顺手重构生产代码。

E0 完成记录（2026-11-28）：

- 实际耗时：约 15 分钟（2026-11-28 02:31:45Z 开始，02:46Z 收尾；不含此前 Phase 11 设计/PLAN 编写时间）。
- 改动范围：仅 `jessica-cli` 的 `#[cfg(test)]` 测试代码与测试支持代码；未改 `jessica-core`、`Cargo.toml`、`CLAUDE.md`、生产路径或 CLI/ACP 行为。新增 7 个行为测试：
  - `app.rs::a_token_budget_trip_overrides_completed_and_skips_memory_writes`：token trip 覆盖 `Completed` 为 `BudgetExceeded`，且不写 memory。
  - `app.rs::an_acceptance_failed_turn_does_not_write_memory`：`AcceptanceFailed` 不写 memory。
  - `app.rs::an_acceptance_retry_reuses_the_initial_prefix_and_recalls_memory_once`：两次 provider 调用的前缀完全相同，memory 只在首轮注入一次，retry 从首次 assistant 结果继续。
  - `app.rs::top_level_delegate_approval_covers_the_child_run`：自定义 gate 只被 `delegate_task` 调用一次，子 `bash` 副作用真实落地，子 Catalog 只含被授予工具。
  - `governance.rs::an_accepted_governance_event_is_broadcast_only_after_the_sink`：sink 执行时广播通道为空，sink 接受后 observer 才收到事件。
  - `memory.rs::recall_failure_is_fail_open_and_injects_nothing`：guide 失败降级为不注入，不改变 turn。
  - `skills.rs::recall_failure_is_fail_open_and_injects_nothing`：skill guide 失败同样 fail-open。
- 11 条不变量覆盖映射（现有测试 + 新增）：
  1. `a_reload_during_a_turn_is_observed_only_by_the_next_turn`。
  2. `the_initial_conversation_follows_the_fixed_order`（E3b 前为 CLI 的 `guides_are_injected_in_system_memory_skill_user_order`）、`history_is_replayed_before_the_new_input_and_excluded_from_counts`、新增 acceptance retry 前缀测试。
  3. `a_session_metadata_conflict_fails_closed_without_agent_events`（provider 未调用）、`journal::a_writer_failure_is_observable_and_permanently_prevents_closure`。
  4. `an_acceptance_retry_trace_keeps_failures_trace_only_and_attributes_each_attempt`、新增前缀测试。
  5. engine 的 `max_args_bytes_is_checked_before_policy_and_tool_execution`、`the_allowlist_denies_before_host_policy_or_gate`、`policy_denial_precedes_permission_and_never_asks_the_client`、`the_permission_wait_does_not_consume_the_tool_timeout`、`the_tool_timeout_starts_after_approval_and_cancels`、`an_unmanaged_oversized_result_trips_the_result_size_guard_and_cancels`、`a_non_allow_permission_decision_fails_closed_without_running_the_tool`、`a_large_read_file_result_becomes_a_preview_and_artifact`、新增 delegate approval 测试（原 CLI 治理测试于 E3b 迁入 engine）。
  6. engine 的 `token_counting_provider_trips_the_budget_and_cancels_the_turn`、`a_token_budget_trip_overrides_completed_and_skips_memory_writes`。
  7. engine 的 `a_governance_event_reaches_the_sink_before_observers`、`an_approval_that_cannot_be_recorded_never_executes_the_tool`、`a_rejected_governance_event_is_not_broadcast_and_cancels`、`journal::a_successful_turn_records_attributed_events_and_closes_last`、`journal::a_writer_failure_is_observable_and_permanently_prevents_closure`、`journal::finish_turn_refuses_an_open_child_until_the_bracket_closes`。
  8. engine 的 `pump::a_failing_integrity_policy_marks_the_current_session`、`pump::a_continue_policy_exposes_the_lag_and_still_reaches_the_terminal_event`、`pump::a_warn_policy_keeps_the_original_console_behavior`；`turn_outcome_matches_the_event_rebuilt_transcript` 断言健康路径 `lagged == false`。
  9. 新增 token/acceptance memory 跳过测试；`memory::open_failure_is_fail_open_and_leaves_the_invalid_path_alone`、`skills::open_failure_is_fail_open_and_leaves_the_invalid_path_alone`、新增 memory/skills recall 失败测试。
  10. `delegation_cannot_grant_a_tool_the_parent_catalog_hides`、`child_calls_charge_the_parent_token_budget_through_the_shared_wrapper`、新增 delegate approval 测试。
  11. `turn_outcome_matches_the_event_rebuilt_transcript`（messages/turn_start/lagged）、`a_session_metadata_conflict_fails_closed_without_agent_events`（`TraceIncomplete` 而非 outcome）、acceptance 闭环与 `resume_continues_from_the_recorded_checkpoint_without_replaying_tools`（resume 不重放）。
- Mutation checks（临时修改生产代码，确认测试失败后逐字还原）：
  1. `GovernanceState::emit` 改为先 broadcast 后 sink → sink-before-broadcast 测试失败。
  2. 删除 `run_attempt` 的 budget reason 覆盖 → token budget 测试失败（`Completed` ≠ `BudgetExceeded`）。
  3. 强制 `governed_completed = true` → `AcceptanceFailed` memory 测试失败（出现 memory 记录）。
  4. Acceptance retry 改为重置到初始消息 → 前缀测试失败（retry 从反馈而非首次 assistant 结果继续）。
- 已知覆盖边界（不阻塞 E1，记录以便 E2/E3 补强）：pump 的 broadcast 容量为 1024，未在 app 层确定性制造溢出；第 8 条 lag 行为由 `pump.rs` 单元测试断言，app 只断言健康路径 `TurnOutcome.lagged == false`，E2 引入可控 `TraceStore`/pump 测试端口后再补 true 路径。`turn_id`/`last_output` 等 engine 目标字段按计划在 E1/E3 落地时补断言。
- 验收结果（E0 收尾时）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：全部 `test result: ok`，0 failed（仅既有 2 个 ignored）；`jessica-cli` 单元测试 413 passed。
  - `git diff -- jessica-harness/src/acceptance.rs` 为空；`app.rs`/`governance.rs`/`memory.rs`/`skills.rs` 的生产代码 diff 为空，改动均位于 `mod tests`。
- 偏离计划：无功能性偏离；仅将 app 层 `lagged=true` 的确定性测试留到 E2 的可控端口，避免为测试暴露生产接口。

Task E1（进行中：E1a/E1b/E1c/E1d 已完成；剩余 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属）：`jessica-engine` 骨架、端口与治理原语

范围与约束：

- 经用户明确同意后，新增 workspace 成员 `jessica-engine`，同步更新 `CLAUDE.md` 的 Workspace 表/依赖规则与 `Cargo.toml` 成员列表。只允许依赖 `jessica-core`、`jessica-runtime`、`jessica-harness`、`jessica-memory`，以及 workspace 已批准的 `async-trait`、`serde`、`serde_json`、`sha2`、`thiserror`、`tokio`、`tracing`、`tokio-util`；不得依赖 `jessica-cli`、`jessica-providers`、`jessica-tools`、`jessica-sandbox`、`toml`、`reqwest`、`wasmtime`、`agent-client-protocol`。
- 定义公共 API 骨架：`Engine`、`EngineBuilder`、`EngineConfig`、`TurnRequest`、`TurnOutcome`、`EngineError`、`TracePolicy` 与端口 traits；从 CLI 迁入 `GovernedTool`、`GovernanceState`、`TokenCountingProvider`、`PolicyV2`、`ToolCatalog`、`ResultManager` 与 artifact store 类型及其所需配置类型；`ReadArtifactTool` 可留在 CLI，engine 只导出其依赖的 store/manager；CLI 改为从 engine 引用，行为不变。
- 最小 `run_turn`：provider → `AgentLoop` → `GovernedTool` 的垂直切片，覆盖 `max_args_bytes`、allowlist、PolicyV2、approval、tool timeout、ResultManager、ResultSizeGuard；不包含 journal/pump/acceptance/memory/skills/delegation/resume。
- E1 只接受显式 `TracePolicy::Disabled`；`Required` 在 E2 前以配置错误拒绝，禁止静默降级；默认值在 E2 改为 `Required`。
- `EngineBuilder` 必须要求 provider、tool provider、必要时的 approval gate 与 result 配置；不得接受预装配的 `Harness` 或 `ToolRegistry`。
- 所有公共项有 `///`；错误用 `thiserror` 结构化 enum；不使用 `unwrap`/`expect`（测试除外）；crate 启用 `#![deny(missing_docs)]`。
- E1 范围较大；开工前若确认无法在单个会话内完成，可拆 E1a（crate/API/端口/配置骨架）与 E1b（治理原语迁移 + 最小 turn），但不得改变总验收口径。

完成标准：engine-only 行为测试证明 PolicyV2 拒绝时 inner tool 未执行、`max_args_bytes` 先于 policy、ResultSizeGuard 在 ResultManager 后；`cargo tree -p jessica-engine --edges normal` 无禁止依赖；CLI/ACP 行为测试不回退；完整验收命令全绿。

禁止：在 E1 迁入 journal/pump/acceptance/memory/skills/delegation/resume；让 CLI 成为 engine 的依赖；用“先跑通再补治理”的方式绕过顺序；新增第三方 crate。

E1a 完成记录（2026-11-28）：

- 实际耗时：约 20 分钟（E0 收尾后开始；2026-11-28 03:12:45Z 完成 E1a 骨架与验收）。范围按 PLAN 允许的 E1a 拆分执行，未宣称 E1 完成。
- 交付：
  - 新增 workspace 成员 `jessica-engine`；根 `Cargo.toml` 成员列表与 `CLAUDE.md` 的 Workspace 表/依赖规则同步更新。依赖仅 `jessica-core`、`jessica-runtime`、`async-trait`、`serde_json`、`thiserror`、`tokio-util`；未引入新第三方 crate，未依赖 CLI/providers/tools/sandbox/memory/harness。
  - 公共 API 骨架：`Engine`、`EngineBuilder`、`EngineConfig`、`TurnRequest`、`TurnOutcome`、`EngineError`、`TracePolicy`。
  - 端口骨架：`ToolProvider`、`TraceStore`、`MemoryPort`、`SkillPort`、`ToolPermissionGate`、`TurnObserver`、`PumpConsole`、`SystemPromptSource`、`IdSource`；`AcceptanceCheck` 与 `ChildToolFactory` 复用 core/runtime 既有契约。
  - 工具元数据骨架：`ToolCatalog`、`ToolEntry`、`ToolDescriptor`、`ToolSource`、`Capabilities`、`ApprovalPolicy`、`ResultFormat`。
  - 配置/构建不变量：`EngineConfig::new` 只接受显式 `TracePolicy::Disabled`，`Required` 返回 `EngineError::UnsupportedTracePolicy`；builder 逐项要求 provider、tool provider、config、result config；含 ask 规则或显式 ask 模式时缺少 gate 返回 `EngineError::MissingApprovalGate`；`ToolCatalog` 拒绝 descriptor/tool 名称不一致与重复名。
- 行为测试：新增 12 个 engine-only 测试（配置拒绝、fail-closed 默认预算、gate 需求边界、builder 依赖缺失、catalog 名称/重复/排序）。未写“调用后断言 Ok”的自证测试。
- 验收结果（E1a 收尾时）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：全部 `test result: ok`，0 failed，2 ignored；`jessica-cli` 仍为 413 passed，`jessica-engine` 12 passed。
  - `cargo tree -p jessica-engine --edges normal`：无 `jessica-cli`/providers/tools/sandbox/harness/memory、`toml`、`reqwest`、`wasmtime`、`agent-client-protocol` 等禁止依赖。
- 偏离/边界：本会话只完成 E1a，未实现 `run_turn` 垂直切片，也未迁入 `GovernedTool`/`GovernanceState`/`TokenCountingProvider`/`PolicyV2`/`ResultManager`；这些留给 E1b。`Engine` 目前只暴露配置快照，端口依赖以私有结构保留供 E1b 消费；E1b 开工前不得把 E1a 骨架误当作可用受治理运行时。

E1b 完成记录（2026-11-28，engine 侧垂直切片；E1 仍未完成）：

- 实际耗时：约 10 分钟（2026-11-28 03:12:45Z E1a 收尾后开始，03:23Z 完成全量验收；不含 PLAN 更新）。
- 改动范围：仅 `jessica-engine`；未改 `jessica-cli`、`jessica-core` 或其他 crate 的生产路径。`Cargo.toml` 新增 `jessica-harness`、`futures`、`sha2`、`tokio`、`tracing`，均为 workspace 已批准依赖，未引入新第三方 crate。
- 交付：
  - `tooling.rs`：新增 `ToolLimits` 与 `ToolDescriptor.limits`；`ToolCatalog` 保持名称一致/重复校验与排序。
  - `policy.rs`：新增 `PolicyError`、`PolicyTool`、`RuleMatch`、`PolicyVerdict`、`PolicyV2`、`glob_matches`。顺序为 deny → protected editor paths → ask → allow → legacy sensitive/approval mode → allowlist fallback；workspace 逃逸与非 UTF-8/不存在路径返回结构化错误。engine 不依赖 `jessica-tools`，patch 目标解析为轻量投影。
  - `artifacts.rs`：新增 `ArtifactError`、`StoredArtifact`、`ArtifactPage`、`ArtifactStore`、`ResultManager`。使用 SHA-256 内容寻址、分片原子写、目录/文件权限收紧、UTF-8 边界截断与有界分页；`ResultManager` 将 inline 阈值/上限夹到 result-size guard 内。
  - `governance.rs`：新增 `GovernanceState`、`TokenCountingProvider`、`GovernedTool`。`GovernedTool` 固定顺序为 tripped → `max_args_bytes` → allowlist policy → `PolicyV2` → approval → tool timeout → `ResultManager` → `ResultSizeGuard`；每步拒绝均不执行 inner tool。E1 的 `GovernanceState::emit` 没有 producer-side sink，sink-before-broadcast 留到 E2。
  - `api.rs`：新增最小 `Engine::run_turn` 垂直切片：每轮配置快照、workspace canonicalize、allowlist 过滤、provider → `AgentLoop` → `GovernedTool`、budget reason 覆盖、`messages`/`last_output`/`turn_start` 输出。不包含 journal/pump/acceptance/memory/skills/delegation/resume；`TracePolicy::Required` 仍被拒绝。
  - `EngineBuilder` 仍要求 provider、tool provider、result 配置；存在 ask 规则或显式 ask 模式时要求 approval gate。
- 行为测试：`jessica-engine` 29 passed。E1b 新增/覆盖的关键断言：
  - `governance::tests::policy_v2_denial_returns_an_error_and_never_runs_the_tool`：拒绝时 inner tool 未执行。
  - `governance::tests::max_args_bytes_is_checked_before_policy_and_tool_execution`：超限参数在 policy 前返回，且无 policy 事件。
  - `governance::tests::result_manager_runs_before_the_result_size_guard`：原始结果会触发 guard，但 manager 限幅后通过。
  - `governance::tests::policy_ask_verdict_runs_only_after_the_gate_allows`、`policy::tests::*`、`artifacts::tests::*`、`api::tests::run_turn_executes_an_allowlisted_tool_and_returns_the_transcript`、`api::tests::policy_denial_reaches_the_model_without_running_the_tool`、`api::tests::a_token_trip_overrides_a_completed_runtime_reason` 等。
- 验收结果（2026-11-28 03:23Z）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1181 passed，0 failed，2 ignored；`jessica-cli` 仍为 413 passed，`jessica-engine` 29 passed。
  - `cargo tree -p jessica-engine --edges normal`：内部依赖仅为 `jessica-core`/`jessica-runtime`/`jessica-harness`；无 `jessica-cli`、`jessica-providers`、`jessica-tools`、`jessica-sandbox`、`jessica-memory`、`toml`、`reqwest`、`wasmtime`、`agent-client-protocol` 等禁止依赖。
- 偏离/边界（重要，E1 因此不能标记完成）：
  1. **E1b 时未完成 E1 的 CLI 接入（后由 E1c 完成治理原语接入）**：CLI 仍使用自有 `governance.rs`/`policy_v2.rs`/`artifacts.rs`/`tools/descriptor.rs`/`tools/catalog.rs`。CLI 版本绑定 `EventSink`/journal、`Harness` middleware、profile/hidden-reason/descriptor metadata、artifact retention/`ReadArtifactTool` 等 E1 明确不迁入的能力；直接替换会改变 CLI/ACP 行为，或需要提前实现 E2 的 sink/journal 顺序。
  2. engine 的 PolicyV2 patch 解析是轻量投影，未证明与 CLI 的 `jessica-tools` patch 摘要完全一致；CLI 接入前必须做行为对齐测试。
  3. `ArtifactStore` 未迁入 retention/GC、`CleanupReport`、artifact metadata/record；`ResultManager` 未关联 CLI 的 artifact record；`ReadArtifactTool` 仍在 CLI。
  4. E1b 时 engine `GovernanceState` 没有 `EventSink`（E1c 已补齐）；sink-before-broadcast 与 sink 失败取消仍由 CLI/E0 测试覆盖，journal 化留待 E2。
  5. engine `ToolCatalog` 是简化投影，不含 CLI 的 profile 过滤、hidden reason、descriptor metadata、schema digest、per-tool timeout 等。
- 后续建议：将“CLI 从 engine 引用治理原语且行为不变”单列为后续 Task（暂记 E1c，后由 E1c 于 2026-11-28 完成，见下）；开工前需用户确认是否提前引入 `EventSink`（触及 E2）以及 descriptor/catalog/artifact 的对齐范围。用户确认前不继续改 CLI。

E1c 完成记录（2026-11-28，CLI 治理原语接入；E1 仍未完成）：

- 实际耗时：约 15 分钟（2026-11-28 03:23Z E1b 收尾后开始，03:38Z 完成全量验收；不含 PLAN 更新）。
- 改动范围：`jessica-engine`、`jessica-cli` 与 `jessica-cli/Cargo.toml`；未改 `jessica-core`、其他 crate 或任何第三方依赖。`jessica-cli` 新增对 `jessica-engine` 的内部依赖（方向为 CLI → engine，符合 workspace 单向规则）。
- 交付：
  - `GovernanceState`：新增可选 `EventSink`、`with_event_sink`；`emit`/`trip` 改为先 sink 后 broadcast，sink 拒绝时取消 turn 且不广播，与 CLI/E0 的既有语义一致。
  - 新端口：`ToolPolicy`（宿主策略求值，失败 fail-closed）与 `ToolResultManager`（结果进入 guard/transcript 前限幅）；engine `GovernedTool` 改存端口对象，engine `PolicyV2`/`ResultManager` 通过适配结构实现端口；新增 `with_tool_policy`/`with_legacy_sensitive`，保留 `with_policy_v2` 便捷入口。
  - CLI 接入：`jessica-cli/src/governance.rs` 删除自有 `GovernanceState`/`TokenCountingProvider` 与治理执行逻辑，改为 re-export engine 类型；CLI `GovernedTool` 保留为薄适配层，把 `Harness` allowlist、CLI `PolicyV2`、CLI `ResultManager`、CLI/ACP 权限门适配为 engine 端口，并委托 engine 执行固定顺序。
  - 为保持 CLI 行为，engine allowlist 拒绝的模型可见错误不带 reason（事件仍带 reason），与旧 CLI 文案一致；CLI 的 `permission_required` legacy 敏感集通过 `with_legacy_sensitive` 保留。
- 行为测试：`jessica-engine` 32 passed（E1c 新增 3 个：sink 先于 broadcast、sink 拒绝不广播并取消、宿主 `ToolPolicy` 拒绝时 inner tool 不执行）；`jessica-cli` 413 passed，其中 governance 模块 34 个原有行为测试全部通过，覆盖 deny 不执行、顺序、sink、permission、timeout、result guard 与 token budget。未写“调用后断言 Ok”的自证测试。
- 验收结果（2026-11-28 03:38Z）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1184 passed，0 failed，2 ignored；`jessica-cli` 413 passed，`jessica-engine` 32 passed。
  - `cargo tree -p jessica-engine --edges normal`：内部依赖仅 `jessica-core`/`jessica-runtime`/`jessica-harness`；无 `jessica-cli`、`jessica-providers`、`jessica-tools`、`jessica-sandbox`、`jessica-memory`、`toml`、`reqwest`、`wasmtime`、`agent-client-protocol` 等禁止依赖；`jessica-cli` 的 normal 依赖树包含 `jessica-engine`。
- 偏离/边界（E1 仍不能标记完成）：
  1. 选择适配端口而非把 CLI 的 `PolicyV2`/`ResultManager`/`ArtifactStore`/descriptor/catalog 具体类型迁入 engine，以避免在 E1c 内改变已由 34+16 个测试固化的 CLI 行为；因此这些类型仍由 CLI 拥有，engine 的对应实现仍是简化投影。
  2. engine `PolicyV2` 的 patch 目标解析仍为轻量投影，未证明与 CLI 的 `jessica-tools::summarize_patch` 完全等价；E1c 通过 CLI 适配层实际使用 CLI 解析器，但等价性仍需后续行为测试或迁移。
  3. artifact retention/GC、`CleanupReport`、artifact metadata 与 `ReadArtifactTool` 仍在 CLI；engine `ResultManager` 仅通过端口被 CLI 适配使用。
  4. engine `ToolCatalog` 仍缺 CLI 的 profile 过滤、hidden reason、descriptor metadata、schema digest、per-tool timeout 等；CLI 仍使用自有 catalog/descriptor。
  5. journal/pump/acceptance/memory/skills/delegation/resume 未在 E1c 引入；`TracePolicy::Required` 仍被拒绝，E2/E3 继续。
- 后续建议：E1 剩余工作聚焦“CLI descriptor/catalog/artifact 与 engine 对齐”——先补 CLI/engine policy patch 解析等价性与 catalog 元数据行为测试，再决定是否迁入具体类型；该工作需用户明确单开 Task（暂记 E1d，候选，未开工）。E2 仍应优先实现 journal/pump/trace，不应与 E1d 混做。

E1d 完成记录（2026-11-28，descriptor/patch/artifact 对齐；E1 仍未完成）：

- 实际耗时：跨会话续作且经历上下文压缩，累计约 45 分钟（起始时刻未精确记录；2026-11-28 完成全量验收）。
- 改动范围：`jessica-engine` 与 `jessica-cli`；未改 `jessica-core`、其他 crate 或第三方依赖，未新增公共依赖。
- 交付：
  - descriptor 元数据迁入 engine：新增 `ToolAnnotations`/`ToolMetadata`/`PolicyHints`，扩展 `Capabilities`（按 source 分类、MCP/WASM annotation 收窄/升级）、`ToolLimits`（timeout、max_args_bytes、max_inline_bytes、max_artifact_bytes、max_output_lines 与 `timeout_ms`）和 `ToolDescriptor`（version、canonical `schema_digest`、`schema_bytes`、`policy_hints`、`result_format`）；新增 `digest_schema`/`policy_hints_for`/`result_format_for` 公共函数。CLI `tools/descriptor.rs` 改为 re-export engine 类型并只保留 CLI 默认 version 标签；CLI catalog 用 engine `ToolDescriptor::new` 装配 per-tool limits 与 metadata，CLI 行为不变。
  - patch 目标解析对齐：engine `PolicyV2` 的 patch target 解析改为与 `jessica_tools::summarize_patch` 相同的头与 hunk 结构语义——跳过前导空行/`diff --git`/`index`，要求 `---` + `+++`，去除 tab 时间戳，处理 `a/`/`b/` 前缀；hunk 部分按声明行数遍历并校验 truncated/over-count、`@@ -old +new @@` 范围、`\ No newline` 标记与第二个文件头；拒绝 rename、`/dev/null`、绝对路径、`.`/`..` 组件、CRLF 与超过 1MiB。修复了此前带时间戳的 `+++ b/src/app.txt\t...` 可能绕过 `src/**` glob 的差异。
  - artifact cleanup：engine `ArtifactStore` 新增 `CleanupReport`、`with_retention(retention_days, max_bytes)` 与异步 `cleanup`（内部 `spawn_blocking`）；按 mtime 过期清理、剩余按最老优先驱逐至字节预算、单次最多检查 4096 个目录项、删除失败计数并继续；跳过非 64 位小写 hex 文件、目录与符号链接；artifact 目录不存在时 cleanup 为成功 no-op。`resolve_existing_root` 改为返回 `Option`，目录缺失时读取返回 `ArtifactError::Unknown` 而不是 I/O 错误。
- 行为测试：`jessica-engine` 47 passed（E1c 32 → +15）：descriptor/schema digest/annotation/catalog metadata 与 per-tool limits 测试迁入 engine（CLI 对应 7 个单元测试移除）；新增 patch 头/hunk 规范化的接受/拒绝矩阵（含 hunk body 以 `---`/`+++` 开头、多 hunk、truncated、no-newline 标记、非法范围、0 起始行）与带时间戳 patch 仍命中 `src/**` 拒绝规则共 3 个；新增 catalog metadata 行为测试 1 个；新增 artifact cleanup 4 个（过期删除且保留新鲜、按最老优先满足字节预算、目录缺失 no-op、跳过非 artifact/目录/符号链接）。`jessica-cli` 单元测试 407 passed，并新增 `policy_v2::tests::engine_and_cli_policy_agree_on_patch_targets`，对标准 patch、带时间戳 patch、`a/`/`b/` 同路径、`diff --git`/`index` 前导、hunk body 以 `---`/`+++` 开头、多 hunk，以及 rename、`/dev/null`、`..`、CRLF、无 hunk、truncated hunk、no-newline 标记、非法 hunk 范围、0 起始行、非 diff 输入逐项断言 CLI/engine 的 Deny 规则与 `InvalidPatch` 分类一致。未写“调用后断言 Ok”的自证测试。
- 验收结果（2026-11-28）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1193 passed，0 failed，2 ignored；`jessica-cli` 单元 407 passed、`jessica-engine` 47 passed，其余集成/E2E 套件全绿。
  - `cargo tree -p jessica-engine --edges normal`：内部依赖仍仅 `jessica-core`/`jessica-runtime`/`jessica-harness`；无 CLI/providers/tools/sandbox/memory 与禁止第三方依赖。
- 偏离/边界（E1 仍不能标记完成）：
  1. CLI `ToolCatalog` 未迁入 engine：profile 过滤、hidden reason、exposure budget、tool candidate 装配仍是 CLI 配置特有行为；engine catalog 现在保留 descriptor metadata/limits，但不做 profile 过滤。
  2. CLI `ArtifactStore`/`ResultManager`/`ReadArtifactTool` 仍归 CLI：CLI artifact store 的 sidecar schema、record 与 cleanup 没有被替换；engine 新增的 cleanup 基于 mtime、没有 sidecar，且尚未接入 CLI 运行路径。`CleanupReport` 已从 engine 导出，供后续适配。
  3. patch 解析在 engine policy 内校验与执行解析器相同的头与 hunk 结构/行数，但不把 hunk 应用到文件；上下文匹配与原子写入仍由执行工具负责。两处实现无共享 crate（engine 不能依赖 `jessica-tools`），后续若修改 patch 语法需同时更新并维持 CLI/engine 等价测试。
  4. `PolicyV2`、`ResultManager`、`ArtifactStore` 具体类型仍由 CLI 拥有；E1d 只迁移 descriptor 元数据并补等价性测试。
  5. journal/pump/acceptance/memory/skills/delegation/resume 未在 E1d 引入；`TracePolicy::Required` 仍被拒绝，E2/E3 继续。
- 后续建议：E1 剩余对齐（CLI catalog 过滤迁入 engine，或 CLI artifact sidecar/`ReadArtifactTool` 迁入 engine）需用户明确单开 Task，且应先补 CLI/engine 行为等价测试再替换具体类型；E2 仍应优先实现 journal/pump/trace，不应与剩余 E1 对齐混做。

Task E2（已完成，2026-11-28）：journal、pump 与 trace 持久化

范围与约束：

- 迁入 `TurnJournal`、`EventPump`、`EventRecorder`、`EventJournal`、`RecorderError`、`SessionSnapshot`、`UpdateSink`、`PumpConsole`、`PumpOutput` 及其测试；engine 定义 `TraceStore` 端口，首版 `SqliteTraceStore` 复用 `jessica-memory::EventStore` 的 canonical 实现。
- 保持 legacy+trace 同事务、SHA-256 链、trace schema v1–v3、bundle/digest/replay 兼容性；不新增存储后端。
- `TracePolicy::Required` 落地并成为默认；`Disabled` 仅供明确关闭 trace 的 embedder。trace 写入失败、`finish_turn` ack 缺失、pump lag 必须分别映射为 `EngineError::TraceIncomplete` 或明确的 outcome 标记，不能把未闭合 turn 当成功。
- `EventPump` 从事件重建 transcript、运行 guards/sensors、检测 broadcast lag；sink 必须先于 broadcast；`finish_turn` ack 是成功边界。
- 不修改 `jessica-core`；不改变 CLI/ACP 的输出与恢复语义。

完成标准：新增行为测试覆盖 trace 写失败→`TraceIncomplete` 且无 `turn_ended`、pump lag 在 outcome 暴露、sink 失败先于 broadcast、`finish_turn` 未 ack 不算成功；`cargo test -p jessica-engine` 与 workspace 全绿。

禁止：静默降级 trace；把 `TraceIncomplete` 映射为普通 outcome；修改 core trace schema；为 engine 引入 `toml`/`reqwest`/`wasmtime`/ACP 依赖。

E2 完成记录（2026-11-28）：

- 实际耗时：约 25 分钟（2026-11-28 13:44 +08 开始，14:07 +08 完成全量验收；不含 PLAN 更新）。
- 改动范围：`jessica-engine`、`jessica-cli` 与 `jessica-engine/Cargo.toml`；未改 `jessica-core`、其他 crate 或任何第三方依赖。engine 新增内部依赖 `jessica-memory`（符合单向规则与 E2 范围），未新增第三方 crate。
- 交付：
  - `pump.rs`：`EventPump`、`SessionSnapshot`、`PumpOutput`、`UpdateSink`（即 `TurnObserver`）迁入 engine；新增 `PumpIntegrity` 端口与 `LagHandling::{Continue, FailSession, Warn}`，由宿主决定 broadcast lag 的处理方式。pump 从事件重建 transcript、运行 guards/sensors、检测 lag；`has_lagged`/`register`/`run`/`wait_for` 公开。
  - `journal.rs`：`TurnJournal` 及其全部行为测试迁入 engine；保留单写者、有界 1024 队列、begin/finish ack、attempt/session 归属、legacy mirror、child bracket 与 fail-closed integrity。`TurnJournal` 实现 `PumpIntegrity`：journal 启用时 lag 记入 integrity 失败但继续消费（`Continue` + `lagged`），journal 禁用时 `Silent`→`FailSession`、`HumanConsole`→`Warn`，与 CLI 原行为一致。
  - `recorder.rs`：`EventRecorder`、`EventJournal`、`RecorderError`、`StoredSession` 迁入 engine；新增 `SqliteTraceStore` 实现 `TraceStore`。它直接复用 `jessica_memory::EventStore::append_traced`，保持 legacy+trace 同事务、SHA-256 链、trace schema v1–v3、bundle/digest/replay 兼容；未新增存储后端。`begin_turn` 先写 `turn_started` 再写 session metadata；`record` 保留 trace-only 事件与 `AcceptanceFailed` attempt 覆盖；`finish_turn` 写 `turn_ended` 并以其 `Ok` 作为成功 ack。
  - `ports.rs`：engine 定义 `TraceStore`/`TurnTrace`；`TurnObserver` 继续作为 `UpdateSink` 来源；`PumpConsole` 由 engine 定义。
  - `config.rs`/`error.rs`：`TracePolicy::Required` 成为默认值；`EngineConfig::new` 不再拒绝 Required；删除 `UnsupportedTracePolicy`，新增 `MissingTraceStore` 与 `TraceIncomplete { turn_id, cause }`。
  - `api.rs`：builder 新增 `trace_store`/`observer`，Required 缺 store 在装配期失败；`run_turn` 在 provider 前 `begin_turn`，为 `AgentLoop` 与 `GovernanceState` 安装同一个 producer-side `TraceEventSink`，保证 sink 先于 broadcast。首个 trace 写失败会取消 agent、立即返回 `TraceIncomplete`、不调用 `finish_turn`；pump 未收到 terminal event 或超时同样返回 `TraceIncomplete`；`finish_turn` 失败也返回 `TraceIncomplete`，不返回伪成功 outcome。pump lag 不丢弃 producer-complete trace：`TurnOutcome.lagged=true`，并用 runtime transcript 提供完整 messages/last_output。
  - CLI：`events.rs`/`journal.rs`/`pump.rs` 改为 re-export engine 类型，保留原 import 路径；`app.rs` 显式构造 `Arc<dyn PumpIntegrity> = journal.clone()` 并传给新增 `EventPump::new` 的 integrity 参数。CLI/ACP 输出与恢复语义未变。
- 行为测试：`jessica-engine` 89 passed。E2 新增 7 个关键行为测试：
  - `api::tests::a_required_trace_without_a_store_fails_assembly`：Required 是默认策略，缺 store 返回 `MissingTraceStore`。
  - `api::tests::a_trace_that_cannot_open_never_reaches_the_provider`：begin 失败返回 `TraceIncomplete`，provider 调用 0 次，record/finish 均为 0。
  - `api::tests::a_trace_write_failure_returns_trace_incomplete_without_turn_ended`：第 3 次 record 失败后返回 `TraceIncomplete`，已记录仅前两个事件，无 `SessionEnded`，`finish_turn` 未调用（无 `turn_ended`）。
  - `api::tests::a_rejected_event_is_never_broadcast_and_no_later_event_follows`：故障 record 等待 observer 看到 3 个已持久化事件后才失败；observer 只看到 3 个事件，被拒事件与后续事件均未广播，recorded 与 observed 前缀一致。
  - `api::tests::an_unacknowledged_turn_end_is_not_a_successful_turn`：producer 流已完整（末事件 `SessionEnded`），但 `finish_turn` 失败仍返回 `TraceIncomplete`，不返回 outcome。
  - `api::tests::a_pump_gap_is_exposed_in_an_otherwise_complete_turn`：2000 个 delta 超过 1024 broadcast 容量，`outcome.lagged==true`，runtime transcript 完整，trace 记录全部 2000 个 delta 且 turn 正常闭合。
  - `api::tests::the_sqlite_store_closes_a_trace_and_keeps_the_legacy_mirror`：真实 SQLite 文件 reopen 后 `closed()` 为 true，共 7 条 trace（首 `turn_started` Fresh、末 `turn_ended` Completed），legacy 5 条事件与 session metadata 一致。
  - 同时迁入并通过 journal（19）、pump（11）与 recorder（5）的原有行为测试；CLI 的对应测试随实现迁出，CLI 二进制单元测试 373 passed。
- 验收结果（2026-11-28 14:07 +08）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1201 passed，0 failed，2 ignored；`jessica-engine` 89 passed，`jessica-cli` 二进制单元 373 passed，其余集成/E2E 套件全绿。
  - `cargo tree -p jessica-engine --edges normal`：内部依赖仅 `jessica-core`/`jessica-runtime`/`jessica-harness`/`jessica-memory`；无 `jessica-cli`、`jessica-providers`、`jessica-tools`、`jessica-sandbox`、`toml`、`reqwest`、`wasmtime`、`agent-client-protocol` 等禁止依赖。
- 偏离/边界：
  1. `TurnJournal::record_failure` 原 CLI 版向 stderr 重复打印失败行；迁入后按 CLAUDE.md 改为 `tracing::error!`。CLI 仍通过 `TraceIncomplete`/journal integrity 向用户报告失败，唯一可观察差异是少一条重复 stderr 行。
  2. engine 自有 `TraceStore` 在 broadcast 前记录每个事件，因此 pump lag 只影响 observer 侧 transcript/sensor，不代表 trace 不完整；engine 通过 `TurnOutcome.lagged` 暴露 gap（符合 Task 的“明确的 outcome 标记”），CLI 仍通过 `PumpIntegrity` 保持原 journal 语义。只有 trace 写/关失败或 pump 未观测到 terminal 才映射为 `TraceIncomplete`。
  3. `TraceStore` 首版面向单 session/单 attempt：`begin_turn`/`record`/`finish_turn` 使用一个 `TurnTrace`。多 attempt acceptance、child session 归属与外层失败记录留到 E3；`SqliteTraceStore` 已保留 `AcceptanceFailed` attempt 覆盖与 child 事件 trace-only 规则。
  4. `TracePolicy::Disabled` 会忽略已配置的 `trace_store`（builder 文档已说明）；但 pump 未观测到 terminal 仍返回 `TraceIncomplete`，属于有意 fail-closed，不把未闭合 turn 当成功。
  5. `EventPump::new` 新增 `integrity` 参数；CLI 仅新增该装配参数，未改变输出、journal 记录或恢复路径。E1 剩余的 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属未在 E2 处理，仍待用户单开 Task。

Task E3（已完成：E3a/E3b，2026-11-28）：完整 turn 编排、acceptance、memory/skills、delegation、resume

范围与约束：

- 迁入/重写 `run_turn_request`、`build_turn`、`run_attempt`、`initial_messages`、outer session、`finish_turn`、memory/skill post-processing、delegation 父侧装配与 `resume_turn`；`CliRuntime` 改为调用 `Engine`，ACP 仍走同一路径。
- 配置拆分：`EngineConfig` 承载 budget/policy/approval/acceptance/delegation/result/trace 等决策；`HarnessConfig` 保留 TOML、provider、路径、profile；engine 不解析 TOML。
- 端口落地：`ToolProvider`、`MemoryPort`、`SkillPort`、`ToolPermissionGate`、`TurnObserver`、`PumpConsole`、`ChildToolFactory`、`AcceptanceCheck`、`SystemPromptSource`、`IdSource`；CLI 提供实现。
- 行为不变：每轮一次配置快照、初始消息顺序、每 attempt 独立 session、最终失败外层 session、budget reason 覆盖、memory/skills 仅在 `Completed` 且未 tripped 后 fail-open、子工具收窄、resume 不重放已记录工作。
- 范围较大；开工前若确认无法在单个会话内完成，可拆 E3a（turn/acceptance/resume）与 E3b（memory/skills/delegation/config/prompt），但不得改变总验收口径。实际拆分为：E3a 交付 engine 侧 turn/acceptance/resume、memory/skills 后处理与 prompt/id 端口；E3b 交付 delegation、CLI 委托、config 拆分与适配器。memory/skills 后处理从建议的 E3b 前移到 E3a，总验收口径不变。

完成标准：E0 的 11 条行为测试全部经 engine 路径通过；ACP 现有测试通过；外部 `jessica-engine` 使用方（不依赖 CLI）能完成带工具与 trace 的完整 turn；CLI 内不再存在与 engine 并行的第二套 turn 装配。

禁止：在 engine 内解析 TOML 或构造 provider/WASM/MCP；改变事件顺序或 trace 归属；把 memory/skills 失败变成 turn 失败；为 delegation 引入逐子工具 policy（另开 Task）。

E3a 完成记录（2026-11-28）：

- 实际耗时：约 25 分钟（2026-11-28 14:07 +08 E2 收尾后开始，14:30 +08 完成 E3a 全量验收；不含本次 PLAN 与 crate 文档更新）。
- 改动范围：`jessica-engine/src/{api.rs, config.rs, error.rs, lib.rs, ports.rs, recorder.rs}`；未改 `jessica-core`、`jessica-cli`、其他 crate、`Cargo.toml` 或任何第三方依赖。E3a 未新增 workspace 依赖。
- 交付：
  - `api.rs`：`TurnRequest` 成为完整 turn 的 engine 请求对象，承载 `input`、`workspace_root`、`history`、`session_id`、`acceptance_retries`、`cancellation`、`observer`、`output`；提供 builder/accessor 与自定义 `Debug`，并实现 `Clone`/`Default`。
  - `Engine::run_turn_request`：每轮一次配置快照；canonicalize workspace；从请求或 `IdSource` 取得首个 session；`initial_messages` 固定为 system prompt → memory recall → skill recall → history → user input，memory/skills 每轮各 recall 一次；`journal.begin_turn` 先于任何 provider 调用；acceptance 分支使用 `AcceptanceRunner`，每次 attempt 独立 session，失败反馈追加到下一次 attempt；非 acceptance 分支只跑一次 attempt；budget trip 覆盖 `Completed`；journal integrity/写入失败映射为 `TraceIncomplete` 且不返回成功 outcome；最终 acceptance 失败或治理 trip 时写外层 session；`journal.finish_turn` 是成功边界；memory/skills 后处理仅在 `Completed` 且未 tripped 时执行。
  - `Engine::resume_turn`：要求 `SessionStore`；未知 session 返回 `UnknownSession`；用 `recover_session` 重建 checkpoint，已完成/已结束的 session 直接返回记录原因且不调用 provider、不写 trace；恢复 turn 以首个 user message 覆盖输入并清空调用方 history，`journal.begin_turn(Resumed, checkpoint messages)` 后从剩余 round 继续；不重放已记录的工具工作；不重跑 memory/skill recall；完成后按同一 fail-open 规则做后处理；`turn_start` 为 0。
  - `build_turn_setup`/`run_attempt`：装配 `TokenBudgetGuard`、`ToolTimeoutGuard`、`StuckDetectionGuard`、`ResultSizeGuard`、`ResultManager`、`AllowlistPolicy`、`PolicyV2`、`TurnJournal`、`GovernanceState`、`TokenCountingProvider`、allowlist 过滤后的 `ToolCatalog`、`GovernedTool` 与 `EventPump`；pump 输出默认 `Silent`（有 observer）或 `HumanConsole`；attempt 开始时重置 stuck guard、校验 journal integrity、记录 session start、注册 pump。
  - `EngineBuilder`：新增 `event_journal`、`session_store`、`system_prompt`、`id_source`、`memory`、`skills`、`acceptance_check`；装配期校验 acceptance 配置必须有 check，`TracePolicy::Required` 下允许 event journal 代替 `trace_store`；`run_turn_request`/`resume_turn` 调用时若缺 event journal 返回 `MissingEventJournal`。
  - `config.rs`：新增 `AcceptanceConfig`（`max_retries`，默认 0），`EngineConfig::acceptance` 决策归 engine。
  - `error.rs`：新增 `MissingEventJournal`、`MissingAcceptanceCheck`、`MissingSessionStore`、`UnknownSession`，并接入 `Recorder`/`Recovery` 错误源。
  - `ports.rs`：扩展 `MemoryPort`/`SkillPort`（加入 session 归属与完成后钩子），新增 `SessionStore`；并消费既有 `SystemPromptSource`、`IdSource`、`TurnObserver`、`PumpConsole`。engine 只决定调用时机与顺序，CLI 侧实现留待 E3b。
- 行为测试：`jessica-engine` 97 passed（E2 原有 89 + E3a 新增 8）。新增：
  - `acceptance_retries_reuse_the_turn_prefix_and_isolate_attempt_sessions`：两次 attempt 复用 system→memory→skill→history→input 前缀；memory/skills 各 recall 一次；失败反馈只进入下一次 attempt；每次 attempt 独立 session；trace 以 `TurnStarted{Fresh}` 开始、`AcceptanceFailed` 归属首个 attempt、`TurnEnded{Completed}` 结束；后处理使用最终 attempt 的 session/消息。
  - `exhausted_acceptance_labels_the_outer_session_and_skips_post_processing`：retry 耗尽时 outcome 为 `AcceptanceFailed`，生成只有 session 边界的空外层 session，不执行 memory/skills 后处理，trace 以 `TurnEnded{AcceptanceFailed}` 结束。
  - `a_budget_trip_skips_acceptance_and_post_processing`：budget 在 `Completed` 后被覆盖为 `BudgetExceeded`，acceptance check 与 memory/skills 后处理均不执行，外层 session 记录 `BudgetExceeded`。
  - `memory_and_skill_failures_are_fail_open`：memory/skill 实现返回错误时 turn 仍为 `Completed`，消息与正常路径一致，错误不改变 outcome。
  - `the_initial_conversation_follows_the_fixed_order`：断言初始消息逐项顺序与 `turn_start` 索引。
  - `resume_continues_the_unfinished_round_without_replaying_recorded_work`：从已记录 checkpoint 继续，provider 只收到 checkpoint 消息，已完成的 tool round 不重放，trace 从 `TurnStarted{Resumed}` 开始且不重复 `SessionStarted`。
  - `resuming_an_ended_session_returns_the_recorded_reason_without_calling_the_model`：已结束 session 返回记录原因，provider 调用 0 次，journal 无新写入。
  - `a_journal_write_failure_makes_the_turn_trace_incomplete`：注入 journal 写失败返回 `TraceIncomplete`，已写入首个 `TurnStarted`，无 `TurnEnded`。
- 验收结果（2026-11-28 14:30 +08）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1209 passed，0 failed，2 ignored；`jessica-engine` 97 passed，`jessica-cli` 二进制单元 373 passed，ACP 44 passed，其余集成/E2E 套件全绿。
- 偏离/边界：
  1. E3a 使用 `TurnJournal`/`EventJournal` 作为完整 turn 的 trace 缝；E2 的 `Engine::run_turn` 仍使用 `TraceStore`。两条缝暂并存，统一留到 E3b/E5；当前外部 embedder 若只用 `TraceStore`，只能走不含 acceptance/memory/skills/resume 的 `run_turn`。
  2. `TurnRequest` 不再实现 `PartialEq`/`Eq`（`observer` 与 `cancellation` 不可比较），改为自定义 `Debug` 并实现 `Clone`/`Default`；这是 engine 公共 API 的破坏性调整，未触及已冻结的 `jessica-core`。
  3. 外层 session/后台失败在 engine 内用 `tracing` 记录，不再复制 CLI 的 stderr 警告；CLI 仍通过 journal integrity 与 `TraceIncomplete` 向用户暴露失败。
  4. 原计划的 engine 路径 broadcast-lag 确定性测试未保留：journal 启用后 producer 经有界 mpsc 队列产生 backpressure，pump 可及时排空 broadcast，无法在 engine 路径确定性制造 1024 溢出。改用注入 journal 写失败覆盖 `TraceIncomplete` 映射；lag→integrity 仍由 `journal::tests::a_pump_gap_fails_the_journal_without_writing_a_gap_entry` 覆盖，`run_turn` 的 lag 暴露仍由 `a_pump_gap_is_exposed_in_an_otherwise_complete_turn` 覆盖。
  5. E3 总完成标准尚未满足：CLI 仍保留并行 turn 装配，delegation、`ChildToolFactory`、`HarnessConfig`/`EngineConfig` 拆分、sensors/`PumpConsole` 适配器与 CLI `CliRuntime` 委托 engine 属 E3b；外部 embedder 的完整 turn 示例留到 E3b/E5。

E3b 完成记录（2026-11-28）：

- 实际耗时：约 65 分钟（2026-11-28 14:30 +08 E3a 收尾后开始，15:36 +08 完成全量验收；含上下文接续后对 CLI 测试与 engine 治理测试的修复）。
- 改动范围：`jessica-engine`（`api.rs`、`config.rs`、`error.rs`、`lib.rs`、`ports.rs`、`pump.rs`、`governance.rs` 测试、新增 `tests/embedder_turn.rs`）与 `jessica-cli`（`app.rs`、`acp.rs`、新增 `engine_ports.rs`、`events.rs`、`governance.rs`、`main.rs`、`pump.rs`、`tools/catalog.rs`，删除 `journal.rs`）；未改 `jessica-core`、其他 crate、`Cargo.toml` 或任何第三方依赖。
- 交付：
  - engine：`run_turn_request`/`resume_turn` 成为完整 turn 的唯一入口；delegation 父侧装配（`ChildToolFactory` + `DelegateTaskTool`）只在 fresh turn 且启用时加入，resume 永不暴露 `delegate_task`；子工具收窄到父 Catalog；budget/policy/approval/acceptance/delegation/result/trace 决策全部来自 `EngineConfig`。
  - CLI 委托：`CliRuntime` 每轮通过 `engine_ports::build_engine` 装配一个 `Engine`，fresh 调用 `run_turn_request`，resume 调用 `resume_turn`；删除 `build_turn`、`run_attempt`、`initial_messages`、并行的 governance/pump/journal 装配与 `journal.rs`；REPL 与 ACP 走同一路径。
  - 配置拆分：`HarnessConfig` 保留 TOML、provider、路径与 profile；`EngineConfig` 承载 governance 决策；engine 内不解析 TOML、不构造 provider/WASM/MCP。
  - 端口适配器：`CliToolProvider`、`CliSystemPrompt`、`CliIdSource`、`CliMemoryPort`、`CliSkillPort`、`RecorderSessionStore`、`EngineGate`、`EngineResultManager`、ACP silent `TurnObserver`、`ChildToolFactory`、acceptance check 与 sensors；保持 CLI ID 格式 `cli-{pid}-{counter}`/`turn-{pid}-{nanos}-{counter}` 与用户可见输出。
  - 外部 embedder 示例：`jessica-engine/tests/embedder_turn.rs` 只依赖 `jessica-core`/`jessica-engine`/`jessica-memory`，从公开端口装配 engine，完成一次带工具与 Required trace 的 turn，断言工具结果进入模型消息、trace 以 `turn_started`/`turn_ended` 闭合。
- 行为测试：`jessica-engine` 104 passed；`jessica-cli` 单元 334 passed，ACP 44 passed，集成/E2E 全绿。
  - engine 新增 7 个治理行为测试：
    - `the_allowlist_denies_before_host_policy_or_gate`：allowlist 外的工具不会咨询 host policy 或审批 gate。
    - `policy_denial_precedes_permission_and_never_asks_the_client`：policy 拒绝先于审批，gate 调用 0 次。
    - `a_non_allow_permission_decision_fails_closed_without_running_the_tool`：Reject/Cancelled/TimedOut/TransportFailed 均不执行工具，并记录映射后的 `PermissionDecided`。
    - `the_permission_wait_does_not_consume_the_tool_timeout`：审批等待 80ms、工具超时 20ms，工具仍执行成功。
    - `the_tool_timeout_starts_after_approval_and_cancels`：审批通过后 200ms 工具触发 5ms 超时，trips `ToolTimeout` 并取消。
    - `an_unmanaged_oversized_result_trips_the_result_size_guard_and_cancels`：512B 未受管结果触发 64B guard，trips `ResultSize` 并取消。
    - `an_approval_that_cannot_be_recorded_never_executes_the_tool`：sink 拒绝 `PermissionDecided` 时工具不执行并取消。
  - 迁出/重写 CLI 测试：删除与 engine 内部实现重复的 6 个 helper-only 测试；`delegate_tool_is_present_only_on_enabled_fresh_turns`、`child_calls_charge_the_parent_token_budget_through_the_shared_wrapper` 改为经公共 `run_turn` 路径断言；`history_is_replayed_before_the_new_input_and_excluded_from_counts` 改为断言 `TurnOutcome.turn_start` 与本轮消息计数。
- 验收结果（2026-11-28 15:36 +08）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1178 passed，0 failed，2 ignored；`jessica-engine` 104 passed，`jessica-cli` 二进制单元 334 passed，ACP 44 passed，`embedder_turn` 1 passed，其余集成/E2E 套件全绿。
  - `cargo tree -p jessica-engine --edges normal`：内部依赖仅 `jessica-core`/`jessica-runtime`/`jessica-harness`/`jessica-memory`；无 `jessica-cli`、`jessica-providers`、`jessica-tools`、`jessica-sandbox`、`toml`、`reqwest`、`wasmtime` 等禁止依赖。
  - E3 完成标准：E0 的 11 条不变量均有经 engine 路径的行为断言（CLI E2E 测试经 `CliRuntime`→`Engine`，治理单测位于 engine）；ACP 44 passed；外部 embedder 完成带工具与 trace 的 turn；`rg` 确认 CLI 不再有 `build_turn`/`GovernedTool`/`TurnJournal` 装配，唯一的裸 `AgentLoop` 使用是离线 replay。
- 偏离/边界：
  1. 删除 CLI 约 900 行治理测试模块，改由 engine 的 7 个行为测试覆盖；E0 覆盖映射已同步到 engine 测试名。测试总数由 1209 降至 1178 是合并重复 CLI 测试所致，而非减少覆盖。
  2. 删除 6 个仅断言已删除私有 helper 的 CLI 测试；对应行为由 engine 的 `the_initial_conversation_follows_the_fixed_order`、acceptance/journal 测试覆盖。
  3. `delegate_tool_is_present_only_on_enabled_fresh_turns` 不再在 CLI 断言 resume 分支；resume 不暴露 `delegate_task` 由 engine 装配与 resume E2E 覆盖，E4 可补 CLI 级断言。（E4 结论：未补，记为后续可选测试增强；见 E4 完成记录偏离项 3。）
  4. `jessica-cli/src/replay.rs` 仍直接构造裸 `AgentLoop`（scripted provider + 录制结果），用于离线确定性回放，不是第二套受治理 turn；E4 再决定迁入 engine 或正式豁免。（E4 结论：已迁入 `jessica-engine::replay`；见 E4 完成记录。）
  5. 未新增 `Cargo.toml` 依赖；engine 公共 API 为增量（端口/配置/类型），未触及已冻结的 `jessica-core`。

Task E4（已完成，2026-11-28）：CLI/ACP 适配器收薄与单一装配

范围与约束：

- `jessica-cli/src/main.rs` 只保留参数解析、配置加载、provider/tools/sandbox/MCP/WASM 构造、信号处理、终端/ACP 输出；turn 生命周期全部委托 engine。
- ACP 与 REPL 共用同一 `Engine` 装配；删除 CLI 内重复的 `build_turn`/`run_attempt`/governance 装配路径；不得保留“第二条受治理路径”。
- 适配器实现端口：`PumpConsole`、`UpdateSink`、`ToolPermissionGate`、`MemoryPort`、`SkillPort`、`SystemPromptSource`、`IdSource`、`ChildToolFactory`；不把终端/协议类型泄漏进 engine 公共 API。
- 不改变 CLI 参数、配置格式、ACP 协议、事件输出与恢复语义。

完成标准：CLI/ACP 端到端行为测试通过；`rg` 证明 `build_turn`、`GovernedTool` 装配与治理链只存在于 engine；CLI 不再直接调用 `AgentLoop`；完整验收命令全绿。

禁止：为了“收薄”改变 CLI/ACP 可观察行为；把 ACP/终端类型放进 engine；删除仍被 phase 9/10 使用的适配器。

E4 完成记录（2026-11-28）：

- 实际耗时：约 10 分钟（2026-11-28 15:52 E3 合并到本地 main 后开始，16:02 +08 完成全量验收）。
- 改动范围：`jessica-engine`（新增 `src/replay.rs`；`src/lib.rs` 增加 `pub mod replay` 并更新 crate 文档）；`jessica-cli`（删除 `src/events.rs`、`src/pump.rs`、`src/replay.rs`；更新 `src/main.rs`、`src/app.rs`、`src/acp.rs`、`src/acp_store.rs`、`src/engine_ports.rs`、`src/trace_replay.rs` 的导入）。未改 `jessica-core`、其他 crate、`Cargo.toml` 或任何第三方依赖。
- 交付：
  - 单一装配：REPL/one-shot 的 `CliRuntime::run_turn_request`/`resume_turn` 与 ACP 均落到同一个 `Engine::run_turn_request`；`engine_ports::build_engine` 仍是 CLI 唯一 engine 装配点，未保留第二条受治理路径。
  - 删除垫片：`jessica-cli/src/events.rs` 与 `jessica-cli/src/pump.rs` 只是 E3b 的 re-export 垫片，现改为直接 `use jessica_engine::{EventRecorder, RecorderError, StoredSession, UpdateSink}`。
  - 离线 replay 归 engine：`jessica-cli/src/replay.rs` 整体迁入 `jessica-engine/src/replay.rs`，公开为 `jessica_engine::replay::{replay_session, replay_resumed_session, render_jsonl, write_jsonl, ReplayError}`；它仍用裸 `AgentLoop` 对录制事件做确定性回放，不执行治理链、不运行真实工具，因此不构成第二受治理路径；engine 的 `#![deny(missing_docs)]` 已对新公开项补齐文档。
  - 依赖边界：`jessica-engine` 仍不依赖 `jessica-cli`、ACP、终端或 WASM/HTTP crate；CLI 的终端输出、TOML、provider/WASM/MCP 构造留在 CLI。
- 行为测试：`jessica-engine` 110 passed（原 104 + 迁入 6 个 replay 测试），`jessica-cli` 二进制单元 328 passed（原 334 − 迁出 6 个 replay 测试），ACP 44 passed，其余集成/E2E 全绿；全 workspace 总数保持 1178。
- 验收结果（2026-11-28 16:02 +08）：
  - `cargo fmt --check`：通过。
  - `cargo clippy --workspace --all-targets -- -D warnings`：通过。
  - `cargo test --workspace`：1178 passed，0 failed，2 ignored。
  - `rg -n "build_turn|GovernedTool|run_attempt|TurnJournal|EventPump|TokenCountingProvider|GovernanceState" --glob '!target'`：只命中 `jessica-engine/src/{api,error,governance,journal,lib,pump,recorder}.rs`；`build_turn` 在 E3b 已删除，全仓无命中。
  - `rg -n "AgentLoop" jessica-cli`：仅 `jessica-cli/tests/trace_verify_e2e.rs:283` 的注释；`jessica-cli/src` 内无命中，CLI 不再直接调用 `AgentLoop`。
  - `rg -n "jessica.cli|agent_client_protocol|crossterm|ratatui|termion" jessica-engine/`：无命中。
  - CLI/ACP 端到端行为测试通过：`acp_stdio` 44、`delegation_e2e` 17、`trace_verify_e2e` 21、`trace_bundle_e2e` 9、`replay_e2e`/`resume_e2e` 等全绿，可观察行为未变。
- 偏离/边界：
  1. E4 完成标准要求 `rg` 证明 `build_turn`/`GovernedTool` 装配只在 engine：`build_turn` 在 E3b 已从 CLI 删除，因此全仓无命中；`GovernedTool`/`TurnJournal`/`EventPump`/`GovernanceState`/`TokenCountingProvider` 只存在于 engine，符合完成标准。
  2. `jessica-cli/src/policy_v2.rs` 保留：它只被 `policy simulate` 子命令用于离线 dry-run 评估，不参与 turn 装配或工具执行，属于 E1 剩余（CLI 保有 `PolicyV2` 具体实现）；删除它会改变 CLI 行为并需迁移约 13 个行为测试，超出 E4 收薄适配器的范围，待 E1 剩余对齐或 E5 前再决定归属。
  3. E3b 偏离项“E4 可补 CLI 级 resume delegation 断言”未在 E4 实施：该断言缺口不影响 E4 完成标准，且 E4 禁止改变可观察行为；记为后续可选测试增强。
  4. 迁移 replay 时只调整聚合可见性（`pub(crate)` → `pub`）并补 doc comment；replay 算法、事件断言与 6 个行为测试未改。CLI 单元测试减少 6 个、engine 增加 6 个是测试移动，不是覆盖减少。
  5. 未新增 `Cargo.toml` 依赖；未触及 `jessica-core` 公共 API。

Task E5（候选，未排期）：冻结 engine 公共 API 与文档

范围与约束：

- 对 engine 公共 API 做最终审查：`Engine::run_turn`/`resume` 是唯一 turn 入口；`TurnRequest` 字段私有且不暴露 `ToolRegistry`；`EngineBuilder` 不暴露预装配 `Harness`/`ToolRegistry`；`TracePolicy` 必须显式；错误用 `thiserror` 结构化；所有公共项有 `///`，crate 保持 `#![deny(missing_docs)]`。
- 增加 `compile_fail` doctest：不能绕过 `Engine` 直接构造受治理 turn，不能把 `TraceIncomplete` 当 `TurnOutcome`；增加 `cargo tree` 依赖检查的文档化命令。
- 更新 `CLAUDE.md`（依赖表、验收命令、engine 冻结说明）、`Arch.md`（新增 engine 层与依赖图）、`README.md`（embedding 示例）与 `PLAN.md` 完成记录；说明版本策略与 semver。
- 冻结后 engine 公共 API 变更需单开 Task；E5 不修改 `jessica-core`。

完成标准：engine 内 example（不依赖 CLI）只通过 engine 跑完一次带工具、trace、审批的 turn；`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿；依赖图与文档一致。

禁止：把未完成的 API 标记为冻结；用 feature 或 `doc(hidden)` 掩盖必须公开的受治理入口；修改 `jessica-core` 来迁就 engine。

Phase 11 开工前需确认的事项（候选阶段不预判结论）：

- E1 新增 crate 与 `CLAUDE.md` 依赖表更新已获用户授权并完成（E1a，2026-11-28）；E1b engine 侧切片已完成（E1b，2026-11-28）；E1c CLI 治理原语接入已完成（E1c，2026-11-28）；E1d descriptor/patch/artifact 对齐已完成（E1d，2026-11-28）。E1 剩余 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属不得在未确认的情况下顺手改变 CLI 行为；其范围与顺序需用户确认。
- E0 已完成（测试放在现有 `#[cfg(test)]` 模块，mutation check 已执行并还原）；以下事项仅针对 E1–E5。
- `EngineConfig` 与 `HarnessConfig` 的拆分边界，尤其是 `ResultManager`/artifact store 的配置来源。
- `TraceStore` 首版是否直接绑定 `jessica-memory` 的 SQLite 实现；若未来需要轻量嵌入，再评估 feature 或 `jessica-engine-sqlite` 拆分。
- engine 公共 API 初版（E1）与冻结（E5）之间的兼容策略；E1–E4 是否允许破坏性调整。
- 子 Agent 治理合同：顶层 `delegate_task` approval 覆盖子 run，逐子工具 policy/approval 另开 Task。
- 并发模型：同一 session 串行、跨 session 并发；保持现有 `PromptBusy` 行为。
- E1 已按 E1a/E1b/E1c/E1d 拆分；剩余 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属是否继续单开 Task（或并入 E2/E3）需用户确认。E3 已按 PLAN 允许项拆为 E3a/E3b 实施：E3a/E3b 均已完成；拆分只影响实施节奏，不影响总验收。

Phase 11 Deferred（不在 E0–E5 内提前实现）：

- Context compression/lineage、prompt caching 与缓存友好的消息布局。
- 确定性并行工具执行与多子 Agent 并发上限。
- Docker/SSH/远程执行后端抽象。
- Gateway/server/TUI/cron/长期运行会话。
- Plugin 生态、skill curator、eval/OTel/Prometheus/trajectory export。
- Checkpoint/rollback、模型路由、credential pool、secret manager/egress 隔离。
- 逐子工具 policy/approval、子 Agent 独立预算与 trace 归属增强。
- 配置/提示摘要写入 trace（需 core schema 审批）。
- `jessica-engine-sqlite` 拆分或轻量 feature；ACP 独立 crate 化。

恢复说明：E0、E1a–E1d、E2、E3a–E3b 与 E4 已完成（E1 仍为部分完成：descriptor 元数据、patch 解析等价性与 engine artifact cleanup 已对齐，CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 仍归 CLI；E3 已完成 engine 侧完整 turn/acceptance/memory/skills/resume/delegation 与 CLI 委托）；E1 剩余对齐与 E5 仍为候选，不占用 Phase 10 的暂缓/恢复条件，Phase 10 仍按原 Deferred 条目处理。E1 剩余对齐与 E5 任何一项开始前，用户需明确指定该 Task 并确认上述相关事项；未确认前不得以“候选已写入 PLAN”为由开始实现。E4 后 CLI/ACP 已只经 `Engine` 执行 turn，CLI 并行 turn 装配已删除；CLI 仍保有 policy/artifact/catalog 具体实现（其中 `policy_v2.rs` 仅用于 `policy simulate` dry-run），因此 engine 是唯一受治理 turn 路径，但 E1 的 catalog/artifact 归属仍未完成。

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
Task 5.4.5 ☑ 约 16 分钟 jessica-cli 新增 `shutdown` 模块与进程级 `ShutdownController`（root `CancellationToken` + 每 turn `child_token()`；提供 `request_shutdown`/`force`/`on_signal`/`is_shutting_down`/`signal`；第一次信号记录种类并取消 root，第二次 force，重复信号幂等且不回退已记录状态）。`SignalListener::spawn` 在返回前同步注册 Unix SIGINT/SIGTERM（非 Unix 回退 `ctrl_c`），第一次信号输出 `[shutdown] ... received; stopping new turns and draining`，第二次输出 `[shutdown] second ... received; exiting immediately` 并在 flush stdout 后 `std::process::exit` 130/143。`app.rs` 移除每 turn 重复创建的 Ctrl+C 任务，`build_turn` 只从 controller 取 child token；`run_turn`/`resume_turn` 在构造后若 token 已取消则零 provider 请求直接返回 `Cancelled`。`main.rs` 在 Turn 模式启动前安装监听；REPL 的 `read_line` 用 `biased select!` 同时等待输入与 `shutdown.cancelled()`，关闭后不再接受输入也不新建 turn。drain 顺序固定为：停止新 turn → 取消当前 turn → 由既有 turn 路径等待 EventPump 持久化终态（沿用 `PUMP_TIMEOUT=30s`）→ `SkillsAssembly::drain()`（受 `skills.drain_timeout_ms` 限制，超时只 warn，不覆盖信号码）→ flush stdout/stderr → 按信号退出。退出码：信号优先，SIGINT=130、SIGTERM=143（即使 turn 同时返回错误）；无信号时 `Completed`/`Cancelled`=0，`BudgetExceeded`/`AcceptanceFailed`/`Error` 及 early error=1。模块文档写明限制：`SessionEnded(Cancelled)` 是终态不可 `--resume`（可恢复关闭需单开 Task）；CommandSensor/CommandAcceptanceCheck 无超时，挂起只能靠第二次信号强制退出（命令超时另开 Task）；不处理 SIGHUP 等其他信号。测试：shutdown 模块 5 个单测（首次取消 active child、第二次 force 且后续 Ignore、关闭后新建 token 已取消、两个 turn token 互不影响、退出码/信号名）；app 2 个（预取消后零 provider 请求、单个 turn 完成不取消进程或下一 turn）；main 2 个（信号码优先于错误结果、无信号时 Cancelled=0/失败=1）；新增 `tests/shutdown_e2e.rs` 4 个 Unix E2E（SIGTERM 中断挂起 turn → 143，事件库恰有 SessionStarted 与单个 SessionEnded(Cancelled)；空闲 REPL SIGINT → 130 且不建 session、不写事件；在途技能 job 在 SIGTERM 后正常 drain 完成 → 143 且 job Done；第二次 SIGTERM 强制退出 → 143、恰 2 次 provider 请求、job 保持 Pending/Processing 可恢复、stderr 含强制退出警告），stdout 均无 panic。偏离/说明：1) `CliRuntime::new` 参数增至 8 个，用 `#[allow(clippy::too_many_arguments)]` 并注释其为唯一装配点；2) 单测直接把 `ShutdownSignal` 注入 `ShutdownController`，未额外抽象 OS 信号通道（`SignalListener` 由 E2E 覆盖）；3) 额外增加技能 worker 正常 drain 的第 4 个 E2E，超出最低要求；4) drain/flush 后显式 `std::process::exit`：REPL 空闲时 `tokio::io::stdin()` 在 blocking pool 上留有 pending read，从 `#[tokio::main]` 返回会等待该 read，stdin 未关闭时进程无法退出；显式 exit 避免此挂起，并使空闲信号路径的退出确定；5) 信号退出码优先于 turn 错误，确保 SIGINT/SIGTERM 不混为 0；6) 未改 jessica-core、未新增第三方 crate。验收：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test -p jessica-cli` 161 passed（115 unit + 13 delegation_e2e + 3 hot_reload_e2e + 5 memory_e2e + 7 reliability_e2e + 1 replay_e2e + 1 resume_e2e + 4 shutdown_e2e + 11 skills_e2e + 1 wasm_tool_e2e）；`cargo test --workspace` 606 passed, 0 failed, 2 ignored。
Task ACP-0 ☑ 约 110 分钟（含 SIGTERM/EOF 竞态定位与修复） 依赖门禁：用户以“请执行 Task ACP-0”批准 `agent-client-protocol` 5.3.0；workspace 声明 `default-features = false, features = ["stdio"]`，未启用 process/unstable_protocol_v2/default。`cargo tree -p agent-client-protocol` 审查新增传递依赖：agent-client-protocol-derive/schema、anyhow、derive_more、serde_with、strum、blocking、uuid（SessionId）及 async-channel/async-task/event-listener/crossbeam-utils 等 stdio/宏依赖；无既有内部 crate 反向依赖。新增 `Mode::Acp` 与 `parse_acp_args`：支持 `jessica acp`、`jessica acp --config <path>` 与 `jessica --config <path> acp`；未知参数、缺失/重复 config 均结构化报错。ACP 分支复用启动时的全局配置加载与 root 解析，安装 `ShutdownController` + `SignalListener`，不进入 REPL/replay/skills 输出路径。`acp.rs` 用 `Agent.v1().name("jessica").on_receive_request(initialize).connect_to(Stdio::new())`：始终返回 protocolVersion=1（客户端传 0/2/999 同样回 1，由客户端决定是否断开）、agentInfo name=jessica/version=crate 版本、loadSession=false、promptCapabilities image/audio/embeddedContext=false、mcpCapabilities http/sse=false、sessionCapabilities={}、authMethods=[]；模块零 stdout 写入且不安装 tracing subscriber。优雅退出：stdin EOF 返回 Ok→exit 0；首次 SIGINT/SIGTERM 触发 `shutdown.cancelled()`，最多等待 250ms 让已生成的响应 flush 后返回，并沿用既有 signal 退出码（SIGTERM=143）；ACP-0 无 session，该值只覆盖单行 initialize 响应，ACP-2 需随 session 取消/排空重新设计；第二次信号强制退出语义由既有 controller 保留。未实现方法经 SDK fallback 返回 -32601（须先完成 initialize；v1 协议在 initialize 前对其他请求返回 -32600，是 SDK 的协议守卫，不视为实现缺陷）。行为测试：4 个单元（initialize 能力/identity、0/2/999 均回 v1）+ 新增 `tests/acp_stdio.rs` 7 个真实进程 E2E（initialize 全字段、非 1 版本、未知方法 -32601 且进程存活、整段 stdout 仅合法 JSON-RPC、stdin EOF 限时 exit 0、SIGTERM exit 143 且 stderr 含信号诊断、`--config` 指向缺失文件 exit 1 且 stdout 为空）。SIGTERM 用例用 `try_wait` 轮询且保持 stdin 打开：tokio `Child::wait` 会先关闭 stdin，制造 EOF 与信号的竞态（曾复现 exit 0）；实现侧 `select!` 使用 `biased`，已记录信号优先于同时到达的 EOF。该用例 8 路并发压测 240 次、整文件重复 15 次均通过。偏离/说明：1) ACP-0 未装配 provider/recorder/memory/skills/wasm/CliRuntime——initialize 不应要求 API key，且本 Task 无 session；完整 runtime 装配留 ACP-2，ACP 分支已复用配置加载/root/shutdown；2) 可执行文件名仍为 `jessica-cli`（`cargo run -p jessica-cli -- acp`），`jessica acp` 指子命令，改名见 Deferred；3) EventPump 未进入 ACP 路径，因此 stdout 纯净无需改 pump，ACP-1 仍按要求增加 Silent 策略；4) 未修改 jessica-core，未启用 v2/process，未实现任何 session 方法。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 617 passed, 0 failed, 2 ignored；`cargo test -p jessica-cli acp` 11 passed（4 unit + 7 E2E）；`cargo test -p jessica-cli` 172 passed（119 unit + 53 integration）。
Task ACP-1 ☑ 约 60 分钟 仅执行上下文重构，未接任何 ACP session 方法、未改现有 CLI 可观察行为。新增 `TurnScope { root, cancel_parent }`：`TurnScope::new` 在 `spawn_blocking` 中 canonicalize root，失败返回结构化 `CliError::CanonicalizeRoot`/`CanonicalizeRootTask`；`CliRuntime.root` 仍为启动 workspace root，仅用于 memory/recorder/skills/wasm 等进程级装配，每个 turn 的文件工具、bash、sensors、acceptance `working_dir`、`resolve_dir` 与 delegation 子工具全部改用 `scope.root()`。`build_turn`/`build_sensors`/`build_child_executor`/`build_tool_registry`/`initial_messages`/`system_prompt` 均显式接收 scope；每 turn 从 `scope.cancel_parent().child_token()` 取取消令牌，父 token 取消可中止在途 turn，两个 scope 互不影响。`jessica-tools` 新增 additive `BashTool::with_current_dir(dir)`（`new()` 仍继承进程 cwd，不存在的目录在执行时按 spawn failure 返回）；父 registry 按 turn root 设置 bash cwd，`NativeChildToolFactory` 的 bash 也使用同一 scope root，read/write 子工具同样基于 scope root。`EventPump` 新增 `PumpOutput::{HumanConsole, Silent}` 与可注入 `PumpConsole`/`UpdateSink`：HumanConsole 默认保留原有 stdout/stderr 渲染与 lag `[warn] skipped N events`；Silent 不写 stdout/stderr，事件仍持久化并转交 sink，broadcast `Lagged` 改为标记当前 session 失败（`has_lagged()` 可观察）而不是 warn-and-continue，`wait_for` 返回 None 使该 attempt 以 Error 终止。`CliRuntime::run_turn` 改为构造启动 root + 空 history + 默认 acceptance 的 `TurnRequest` 并委派 `run_turn_request`；新增 `TurnRequest { scope, history, user_input, update_sink, acceptance_retries }` 与 `TurnOutcome { reason, messages }`，update_sink 存在时自动使用 Silent，acceptance_retries=None 用配置、Some(0) 只尝试一次；REPL/one-shot/resume 入口保持不变。初始消息顺序固定 `system prompt → memory guide → skill guide → history → user`，acceptance 重试复用同一 initial messages；计数只覆盖本 turn 新消息切片（`turn_start = initial.len()`），history 中的工具调用不进入 `structured_counts`、不触发技能阈值，memory 写入统计整个 turn（含 acceptance 尝试），skill 入队统计最终 attempt。新增行为测试：同一 runtime 两个 turn root 的读写隔离与越界拒绝；BashTool 显式 cwd 生效、未设置保持旧行为、缺失 cwd 报 spawn failure；取消一个 scope 不影响另一个 turn、取消父 token 中止在途 turn；Silent 只走 sink 且零渲染、HumanConsole 输出原文不变、Silent lag 使 session 失败而 HumanConsole 保持 warn；history 回放顺序与计数排除、skill 阈值不被 history 触发；TurnOutcome 与事件重建 transcript 一致；sink 收到完整事件；`acceptance_retries=Some(0)` 仅一次 provider 调用；缺失 root 被拒绝。偏离/说明：1) `TurnOutcome.messages` 与 `EventPump::has_lagged` 当前仅测试使用，暂以 `#[allow(dead_code)]` 保留，待 ACP-2 消费；2) `build_tool_registry` 参数增至 8 个，沿用既有 `#[allow(clippy::too_many_arguments)]` 约定；3) Silent 由 `update_sink.is_some()` 隐式选择，避免在 TurnRequest 中重复表达模式；4) memory 的 `tool_call_count` 仍按整个 turn（含 acceptance 重试）统计以保持 5.2.6/5.4 行为，skill 触发按最终 attempt 统计以保持原语义。未修改 jessica-core、未新增第三方 crate、未实现 ACP handler、未改变 `--resume`/replay/skills 语义。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test -p jessica-cli` 184 passed（131 unit + 53 integration）；`cargo test -p jessica-cli acp` 11 passed（4 unit + 7 E2E）；`cargo test --workspace` 632 passed, 0 failed, 2 ignored。
Task ACP-2 ☑ 约 150 分钟（含编译修复、E2E 补测与行为测试调整） 仅实现 `session/new`、`session/prompt`、事件映射与多轮历史；未实现 ACP-3 的 `session/cancel`/并发关闭，未实现 MCP/权限/`session/load`，未修改 jessica-core、未新增第三方 crate。配置：`HarnessConfig` 新增 `[acp]`（`AcpConfig`），`max_sessions=8`、`max_prompt_bytes=1 MiB`、`max_history_messages=200`、`max_history_bytes=2 MiB`；四项均为 0 时在配置加载期返回带字段名的结构化 `ConfigError::Invalid`，未知字段由 `deny_unknown_fields` 拒绝；`acp` 与 budget/policy 等一样视为不可热更新段。会话：新增 `AcpSessionRegistry`/`AcpSession`，session id 为 `acp-{pid}-{seq}`，内部 turn id 为 `{session_id}-t{turn_seq}`；`create` 在 `spawn_blocking` 外使用 `tokio::fs::canonicalize`+metadata 校验绝对路径/存在/目录/位于 canonical 启动 root 内，注册表锁内原子执行 max_sessions 检查；每个 session 保存 canonical cwd、`history: Vec<Message>`、单 active prompt token 与 turn 序列。`session/new` 对非空 `mcpServers`/`additionalDirectories`/`systemPrompt` 返回 `InvalidParams`，超限错误带 `{"reason":"session_limit_reached","maxSessions":n}`，成功返回 `{sessionId}`。prompt 内容：按客户端顺序拼接非空 `text` 与 `resource_link`，后者渲染为单行 `[resource] name=... uri=...`（字段内换行折叠为空格）；image/audio/resource/未知块返回 `InvalidParams`，全空白或超过 `max_prompt_bytes` 同样拒绝，均在获取 session lease 或调用 provider 之前完成。`session/prompt`：查找 session、校验内容、原子检查 busy（同 session 第二次返回 `InvalidParams` + `{"reason":"session_busy"}`）、创建 `shutdown.child_token()`、分配 turn id，然后把 responder 与 worker 移入 `connection.spawn`；handler 只做内存操作，不阻塞 SDK 分发循环，spawn 失败时 guard 释放 busy。worker：懒装配 `CliRuntime`（`OnceCell<Result<_,()>>`，失败缓存且只记通用 warning），构造 `TurnRequest { scope: session root, history, user_input, update_sink, acceptance_retries: Some(0), session_id: Some(turn_id) }`；acceptance 配置即使存在也只跑一次；EventPump 终态后若 `lagged` 则取消 prompt token 并按内部错误返回且不提交 history；所有 update 经无界 mpsc 交给独立 writer task，worker 在响应前发送 oneshot barrier 等待 writer 处理完（发送失败同样内部错误）。事件映射：`ModelDelta` 非空才发 `agent_message_chunk`，本 prompt 所有 chunk 共用 turn id 作为 `messageId`；`ToolCallStarted` 发 `tool_call`（in_progress，name/kind/title/rawInput/locations，title 约 120 字符、单行，rawInput 16 KiB）；`ToolCallFinished` 发 `tool_call_update`（completed/failed，text content + rawOutput 截断 16 KiB）；read_file→read、write_file→edit、bash/delegate_task→execute、其余→other；`PolicyDenied` 不重复发 failed，`SessionStarted`/`SessionEnded`/budget/acceptance/error 不映射，不发 `agent_thought_chunk`。stopReason：Completed→end_turn、Cancelled→cancelled、BudgetExceeded+TokenLimit→max_tokens、其余 budget→max_turn_requests；provider/pump/内部错误统一 `-32603 "prompt failed"`，不泄露 provider 详情。history：成功（含 cancelled/预算终止）提交本次新增 user+assistant 消息；错误与 lag 不提交；提交前 `complete_tool_pairs` 为未闭合 tool call 合成 `tool error: tool execution was interrupted` 且不重排已有结果；`trim_history` 只在 user turn 边界裁剪，两个上限任一超出即丢弃最旧完整 turn，单个超大 turn 整体丢弃、绝不留下孤立 tool result。ACP 代码仅在 session 创建时记录 session id、initialize 记录版本、装配失败记录通用 warning，不把 prompt/工具参数/密钥写入 stderr 或 tracing，ACP 路径不安装 tracing subscriber。行为测试：新增单元测试覆盖 ACP 配置默认值与零值/未知字段拒绝、cwd 四类非法输入、session 上限、同 session busy、不同 session 并发、prompt 文本/资源链接顺序与 image/audio/resource/空/超长拒绝、未闭合 tool call 合成、history 消息/字节裁剪与整 turn 丢弃、事件映射（chunk/messageId/空 delta、read/failed、kind、去重、截断、budget）、顺序历史与 provider 错误不提交、取消保留部分输出、stopReason、initialize；`tests/acp_stdio.rs` 扩展为 11 个真实进程 E2E：原 7 个 ACP-0 用例（未知方法改为真正未注册方法）+ cwd/mcp 校验 + prompt 内容校验且 mock provider 零请求 + 两轮 prompt/tool_call 更新先于响应/stdout 仅 JSON-RPC/第二轮 provider 请求顺序与 tool result 回放 + token 预算 max_tokens 且部分历史保留。偏离/说明：1) provider 装配改为懒加载，避免 initialize/session-new 在无 API key 时失败；同时把 turn/ACP 共用的装配抽为 `assemble_runtime`；2) `TurnOutcome` 增加 `turn_start`/`lagged`，ACP 提交用 `turn_start-1`（该 offset 位于新 user 消息之后）并包含 user 消息；3) ACP-2 的 prompt token 仍是进程 shutdown 的子 token，`session/cancel`、session 生命周期 token、关闭排空属于 ACP-3；4) 取消时依赖 runtime/pump 已产生的部分快照与 `complete_tool_pairs` 补齐，未实现 ACP-3 的 in-flight failed 更新；5) E2E 使用本地 mock OpenAI SSE 服务与真实二进制，脚本化客户端不能替代 ACP-4 的 Paseo 验收。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 665 passed, 0 failed, 2 ignored；`cargo test -p jessica-cli acp` 44 passed（33 unit + 11 E2E）。
Task ACP-3 ☑ 约 15 分钟（自 PR #21 合并后开始；含上下文恢复后的编译修复与补测） 仅实现 `session/cancel`、session 并发与进程/传输关闭；未实现 ACP-4 的限制/隐私/热加载加固，未实现 MCP/权限/`session/load`，未修改 jessica-core、未新增第三方 crate。取消 token 链：进程 `ShutdownController` root → registry `lifetime_parent`（`shutdown.child_token()`）→ 每 session `lifetime: CancellationToken` → 当前 prompt token（`lifetime.child_token()`）→ turn token（`TurnScope` 再取 child）。`AcpSession` 的 active 状态改为 `PromptControl { token, cancel_requested, response_gate }`；`session/cancel` 的 `CancelNotification` handler 只按 sessionId 查找并取消该 session 的 active prompt，未知 session 与无 active prompt 都是幂等 no-op，session 之后仍可接受新 prompt。`PromptGuard` 拆成 `commit(&mut self, messages)` 与 `release(&mut self)`，Drop 仍兜底释放；`cancel_prompt` 先在 response gate 内设置 `cancel_requested` 再 cancel token。worker 在 `execute_prompt` 返回并发送 update barrier 后获取同一 response gate，重新读取 cancel flag；只要客户端发过 cancel，terminal 一律覆盖为 `StopReason::Cancelled`（即使底层已 Completed、BudgetExceeded 或 Error），在 gate 内提交可提交的部分 history、发送 response、release lease 后再释放 gate，避免返回 end_turn 掩盖取消或因提前释放 lease 丢失并发 cancel。in-flight 工具：`AcpUpdateSink` 与 worker 共享 `InFlightTools`，记录 `ToolCallStarted`、移除 `ToolCallFinished`；execution 结束后 `take_pending()`，在 barrier 之前为每个未完成调用发送 `tool_call_update`（status=failed，text/rawOutput=`tool execution was cancelled`），保证 failed 更新先于 prompt response 且客户端不会留下永远 in_progress。history：取消等 Stop 终态提交本次部分消息，`PromptGuard::commit` 内 `complete_tool_pairs` 为未闭合调用合成 `tool error: tool execution was interrupted`；内部错误、pump lag、update writer barrier 失败均不提交。并发：每个 session 独立 lease/token/history，共享 `Arc<CliRuntime>`（provider/recorder/memory/skills）保持既有线程安全结构；EventStore 仍走既有 `spawn_blocking`，本 Task 未新增阻塞调用或 sleep 轮询。关闭：`AcpService` 新增 `PromptTracker`（`closing` + `active` 计数 + watch）与 `transport_failure` token；`begin_shutdown` 停止接受 initialize/session-new/prompt 并 `registry.cancel_all()` 取消 lifetime parent 与所有 session；run 在 SIGTERM/SIGINT、stdin EOF、transport failure 后等待 active prompt worker 最多 5 秒（超时只 `tracing::warn`），再给连接 250ms flush，最后在 runtime 已装配时调用 `CliRuntime::shutdown()`（即 `SkillsAssembly::drain`）；未装配 runtime 时不为了 drain 而启动 provider/memory。stdin EOF 保持 exit 0；SIGTERM 保持 143，第二次信号仍由既有 `SignalListener` 立即强制退出。stdout 写失败：update writer 的 `send_notification` 失败或 prompt response 写入失败都会调用 `fail_transport`，取消全部 session 并唤醒 run，writer 之后只消费 barrier 不再重复写；工具在客户端断开后不能继续发起模型调用。行为测试：单元新增/更新覆盖 cancel 只标记目标 session、idle cancel 幂等、session lifetime 取消可达 prompt token 且不影响另一 session、shutdown 拒绝新 prompt/session 并等待 activity guard、transport failure 取消全部 prompt、in-flight 工具只关闭一次且 only-unfinished、event/tool 映射与 stopReason 既有用例；真实进程 E2E 扩为 15 个：新增“A/B 两 session 并发，A 在 bash 中、B 阻塞于 bash，A 的第二个 prompt 返回 busy，取消 A 后 A 返回 cancelled 且 in-flight tool 收到 failed、无 in_progress、history 保留且 A 可继续 prompt，随后释放 B 的工具让它独立返回 end_turn”、“SIGTERM 时两 session 均有 active tool，进程限时 exit 143 且两个 prompt 都返回 cancelled，同时启用 skills 并确认 skills.db 已装配/排空”、“stdin EOF 时两 session 均 active，进程限时 exit 0 且无额外 provider 调用”、“关闭 stdout 读端后 prompt 不 panic、不挂起、不重复调用 provider”。偏离/说明：1) prompt drain bound 固定 5s、transport flush 固定 250ms，均按 PLAN 的“限定超时、超时只 warn”要求；2) 未完成工具统一使用英文内容 `tool execution was cancelled`（也覆盖内部错误中断的统一收尾），不区分具体错误来源；3) skills drain 只在 runtime 已装配时执行，E2E 通过启用 skills 并检查 skills.db 与限时退出验证装配/关闭路径，未以内部日志断言 worker 的每个 job；4) cancel 与 response 的线性化点是 response gate：gate 之后到达的 cancel 不会改写已排队的 response，gate 之前到达的 cancel 必定得到 cancelled；5) `AcpSession::cancel_prompt` 对“有 active prompt 的重复 cancel”仍返回 true（语义为“存在 active prompt”），但重复取消无额外副作用。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 729 passed, 0 failed, 2 ignored；`cargo test -p jessica-cli acp` 54 passed（39 unit + 15 E2E）。
Task ACP-4 ☑ 约 20 分钟（含 Paseo 0.15.3 手工互通与排障） 仅完成限制、内容/错误策略、隐私与配置加固；未实现 ACP-5/6/7，未修改 jessica-core，未新增第三方 crate。限制：`max_sessions` 在 registry 原子创建时检查，超限返回 `-32602` `session limit reached` 与 `session_limit_reached`/`maxSessions` data；`max_prompt_bytes` 在 prompt 校验时按 UTF-8 字节检查，恰好等于上限接受；`max_history_messages`/`max_history_bytes` 在 commit 时按完整 user turn 裁剪，只丢最旧完整 turn，不拆 tool call/result，单个最新 turn 超限则整 turn 丢弃而非截断；新增合法边界与 provider 收到成对 tool call/result 的行为测试。Notice：仅当 `initialize.clientCapabilities.session.notices` 显式为 `{}` 时发送；未声明时零 notice；只对 `max_tokens`/`max_turn_requests` budget stop 发送 content-free warning notice，普通 stop、PolicyDenied、错误不强行 notice，stopReason/tool_call_update 仍是主信号；schema 与 capability 单元/ E2E 覆盖。错误：method-not-found 保持 SDK `-32601` `Method not found` 且 data 为方法名；非法 params 使用 `-32602` 的稳定消息（session limit、session busy、prompt 空/超限、cwd/unsupported fields）；unknown session 使用 `-32002` `Resource not found` + data.sessionId；provider/internal 失败统一 `-32603` `prompt failed`，不泄露 provider 细节；工具执行失败仍通过 failed `tool_call_update` 表达，不伪装成 JSON-RPC 错误。隐私：ACP 不安装 tracing subscriber、stdout 仅 JSON-RPC、stderr 仅必要诊断；E2E 构造 prompt secret 与 tool secret，断言 provider 实际收到两者而 stderr、JSON-RPC error、stdout 均不含完整内容；断管/写失败路径沿用同一规则且不 panic。热加载：`run` 接收 `--config` 路径，首次 prompt 惰性装配 runtime 后启动现有 `ConfigWatcher`；每个 prompt 从 `CliRuntime::config_snapshot()` 取最新 reloadable 快照；events/memory/skills/wasm/reliability/acp 改动整份拒绝并保留启动值，wasm 仍启动加载；E2E 将 policy allowlist 从 read_file 改为 bash，下一 prompt system prompt 反映新值，再把 `[acp] max_prompt_bytes` 改为 64 并断言整份拒绝、下一 prompt 仍按启动值 8 字节拒绝。文档：README 增加 ACP 启动命令、Zed/Paseo 配置示例、`[acp]` 限制语义、cwd 限制、无 MCP/无权限请求/无 session load、notice 门控、并发/cancel 与已知限制。真实客户端验收（2026-11-15 UTC）：Paseo 0.15.3；临时 home `/tmp/jessica-acp4-paseo/home2`，daemon 从 `/home/ubuntu` 启动（`paseo --home ... daemon start --timeout 30`），provider 配置为 `extends: "acp"`、`command: [target/debug/jessica-cli, acp, --config /tmp/jessica-acp4-paseo/harness.toml]`、env `JESSICA_PROVIDER=openai`/`OPENAI_API_KEY=test-key`/`OPENAI_MODEL=test-model`/`OPENAI_BASE_URL=http://127.0.0.1:46667/v1`、`additionalModels: []`，workspace `/home/ubuntu/jessica-acp4-paseo-workspace`；使用脚本化 OpenAI-compatible mock（首次 text，第二次 bash `sleep 30`），未使用真实模型。阶段观察：initialize 后 `provider ls` 显示 Jessica available，agent inspect 显示 Streaming/Persistence/DynamicModes/McpServers，persisted sessionId `acp-4144475-0` 证明 session/new 使用 workspace cwd 成功；第一轮 `paseo run --provider jessica --cwd ... --no-wait --json "first prompt from paseo"` 后 agent logs 为 `[User] first prompt from paseo` + `paseo first answer`，mock 记录 1 次 text 请求，agent idle；第二轮 `agent send --no-wait "second prompt from paseo"` 后 logs 显示 `[Shell] sleep 30`，`agent attach` 显示 `[Tool: execute] running`，mock 记录第 2 次 tool 请求；`agent stop` 返回 stoppedCount 1，attach 显示 `[Tool: execute] failed`，agent 回到 idle，35 秒后 mock 仍只有 2 次请求（若未 cancel，sleep 30 结束后会继续第 3 次），证明 session/cancel 关闭了 in-flight tool 且未继续模型回合。失败日志与规避：最初 daemon cwd 为 `/home/ubuntu/projects/jessica`、workspace 在 `/tmp`，Paseo provider-snapshot-manager 的 global catalog 探测以 `$HOME` 调 `session/new`，被 Jessica 以 `cwd must be inside the startup workspace root` 拒绝，provider 状态 error；改为 daemon 从 `/home/ubuntu` 启动且 workspace 位于 `$HOME` 下后 catalog 与两轮 prompt 全部成功，无需源码改动。偏离：`[acp]` 限制仍为启动冻结、watcher 随惰性 runtime 在首次 prompt 后启动、notice 只覆盖 budget stop、Paseo global catalog cwd 限制均记入文末 Deferred；mock provider 是脚本化 provider，真实模型未参与，符合 Task 允许。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 683 passed, 0 failed, 2 ignored（50 suites）；`cargo test -p jessica-cli acp` 43 unit + 19 E2E 全通过。
Task ACP-7a ☑ 约 120 分钟（含实现、进程重启 E2E、文档与 Paseo 0.15.3 验收） 仅实现 `session/load` 与 ACP 会话索引；未实现 `session/list`/`delete`/`resume`/`close`（ACP-7b），未实现 ACP-5/6，未修改 jessica-core 或 jessica-memory，未新增第三方 crate。配置：`[acp]` 增加可选 `state_dir`，相对路径按启动 root 解析；`HarnessConfig::validate` 要求存在时 `[events].path` 也存在，空路径由 `AcpConfig::validate` 拒绝；`state_dir` 与 `[acp]` 其余限制一样启动冻结、不参与热加载。新增 `AcpPersistence::open`：创建/canonicalize state dir，并用既有 `EventRecorder` 打开事件库；仅在两者成功后 `initialize.agentCapabilities.loadSession=true`，否则 false 且 `session/load` 返回 `-32601`。索引：每 session 一个 JSON v1 文件，文件名对 session id 做十六进制编码；字段仅 version/sessionId/canonical cwd/createdAt/updatedAt/turn ids，不写 prompt、模型输出、工具参数、env 或密钥；`deny_unknown_fields`，未知字段/版本不符/内容自相矛盾均 fail-closed；写入为同目录临时文件 + `write_all` + `sync_all` + `rename` 原子替换，失败清理临时文件；重复 `create_session` 拒绝覆盖。session/new 成功后写空索引，失败则从 registry 移除并返回 `-32603`。prompt 在 update barrier 成功且本次 turn 可提交时、response 之前追加内部 turn id；索引写失败只 warn，内存 session 继续可用，load 会回退到更旧可恢复 turn。session/turn id 均包含进程 nonce：session id 为 `acp-{pid}-{startup_nanos}-{seq}`，turn id 为 `{session_id}-{pid}-{startup_nanos}-t{turn_seq}`，避免恢复后的 session 从 t0 重新开始并覆盖旧事件 key。`session/load`：拒绝非空 `mcpServers`/`additionalDirectories`/`systemPrompt`（`-32602`）；未知 id `-32002`；cwd 通过既有 absolute/exists/directory/root 校验且必须等于索引 canonical cwd（`-32602`）；重复 load 同进程 `-32602`；索引损坏 `-32603`。历史重建从最新 turn 开始 `EventRecorder::load` + `recover_session`，缺失/损坏/无法恢复时回退到更旧 turn；全部不可恢复返回 `-32603`；过滤 `Role::System` 后执行 `complete_tool_pairs` 与既有 `trim_history`，再以原 id 恢复内存 session。重放：在 load response 之前通过 `session/update` 发送 user/assistant 文本（每消息独立 synthetic messageId）、tool_call（in_progress）和 tool result（completed/failed），孤立 tool result 不发送；写通知失败即关闭 transport 并移除刚恢复的 session。行为测试：新增 `acp_store` 7 个（索引 round-trip、turn 上限/去重、缺失/损坏/版本/未知字段/自相矛盾、重复创建不覆盖、最新 turn 回退与系统消息过滤、全部不可恢复 fail-closed、hex 文件名安全）；`acp_session` 新增 3 个（跨 registry 唯一 id、restore 保留 id/裁剪/续接 turn、restore 拒绝重复/越界 cwd/上限）；`acp_events` 新增 3 个（历史重放顺序/system 过滤/tool 生命周期、失败 tool 与独立 messageId、孤立 tool 跳过与输出截断）；`tests/acp_stdio.rs` 新增 4 个真实进程 E2E（持久化时 loadSession=true 且实际创建 state dir/事件库；含工具 turn 的进程重启后 load 在响应前重放 user/tool/tool result/assistant，并断言第二次 prompt 的 provider 请求包含完整恢复上下文；空 session load 与未知/错误 cwd/重复 load/MCP 拒绝；无持久化 `-32601`），E2E 总数 19→23。Paseo 0.15.3 真实客户端验收（2026-11-16 UTC）：隔离 home `/tmp/jessica-acp7a-paseo/home`、daemon `127.0.0.1:6768`、从 `/home/ubuntu` 启动；provider command 为临时 wrapper `/tmp/jessica-acp7a-paseo/jessica-paseo`，exec `target/debug/jessica-cli acp --config <harness>`，`supportsMcpServers=false`，env 指向本地脚本化 OpenAI-compatible SSE mock（`127.0.0.1:46668`，不嵌入真实密钥；未使用真实模型）；`[events].path` 与 `[acp].state_dir` 使用 workspace `/home/ubuntu/jessica-acp7a-paseo-workspace` 下绝对路径。阶段观察：`provider diagnostic jessica` 报 ACP spawn/initialize/session-new ok、Status Ready；`paseo run --provider jessica --cwd ... --title acp7a-load --wait-timeout 60s` 成功，agent logs 为 `[User] first prompt from paseo` + `first paseo answer`，mock 1 次请求，索引 1 个 turn；`daemon stop` 后 ACP 进程退出，`daemon start` 后 agent 状态 closed；`agent send <id> "second prompt from paseo"` 成功，logs 保留首轮并追加第二轮，mock 第 2 次请求为 4 条消息 `[system, first user, first assistant, Paseo 包裹的 second user]`，索引更新为 2 个 turn，事件库 2 个 turn session。Paseo provider catalog probe 额外创建了若干空索引 session（cwd=`/home/ubuntu`），不影响真实 agent。未改动已部署 `~/.local/bin/jessica-cli` 或默认 `~/.paseo/config.json`。偏离/说明：1) `state_dir` 必须显式配置，不自动从 events path 推导；2) turn id 在原 `{session}-tN` 中额外加入当前进程 nonce，因为恢复 session 的本地 turn sequence 从 0 重新开始；ACP-7a 之前没有持久化 ACP 会话，因此无兼容负担；3) 索引只保留最后 `max_history_messages` 个 turn id，与历史上限一致；4) 索引追加与事件库写入不是跨存储事务、索引无跨进程文件锁，已记入 Deferred；5) 历史重建以最新可恢复 turn 为快照，较旧 turn 仅作 fallback；6) Paseo 验收使用脚本化 provider，符合 Task 允许且避免真实费用/密钥泄露。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 701 passed, 0 failed, 2 ignored；`cargo test -p jessica-cli acp` 57 unit + 23 E2E 全通过。
Task ACP-5 ☑ 约 150 分钟（估算；跨上下文恢复，未精确计时） 仅实现 `session/new.mcpServers` 的最小 stdio MCP 客户端，未实现 HTTP/SSE、权限请求（ACP-6）或 load/resume 的 MCP 恢复。新增 `jessica-cli/src/mcp.rs`：按 ACP `McpServer::Stdio` 启动子进程（绝对 command、args、env overlay、session cwd），执行 `initialize`（protocolVersion `2025-06-18`、clientInfo）+ `notifications/initialized`，在 server 声明 tools capability 时按 cursor 分页 `tools/list`（最多 16 页），并以原始 MCP 名执行 `tools/call`；JSON-RPC 行上限 4 MiB，initialize/discovery 10s 超时，内部 tools/call 安全上限 60s（通常由 `budget.tool_timeout_ms` 先触发）。工具以 `mcp__<server>__<tool>` 命名注册，provider 可见名字中的非 `[A-Za-z0-9_-]` 字符替换为 `_`，保留原始名用于调用；每 session 最多 8 servers/256 tools，拒绝空/重复 server 名、相对 command、sanitize 后空名、重名工具与超大 tool set。MCP 工具作为 `TurnRequest.extra_tools` 注入，每 turn 与 native/WASM 工具一样经 `GovernedTool`、`AllowlistPolicy`、`ToolTimeoutGuard`、`ResultSizeGuard`，不自动加入 allowlist。`AcpSession` 以 `OnceLock<Arc<McpSession>>` 持有连接：`session/close`/`delete`、lifetime 取消、进程 shutdown、传输失败与启动失败均 kill+reap 子进程；`session/cancel` 只取消 prompt，不停止 MCP；child stderr 持续 drain 并丢弃，`StdioServerConfig` 不实现 Debug，env/secret 不进入日志、ACP 错误或持久化。错误映射：非法声明/HTTP/SSE 为 `-32602`；子进程启动后 initialize/discovery 失败为 content-free `-32603` 且 data `{"reason":"mcp_server_startup_failed"}`；MCP `isError` 与协议/传输失败转为 failed `ToolResult`，不 panic；server notification 忽略、server→client request 回 `-32601`。测试：9 个 mcp 单测（sanitize、text/error 渲染、binary placeholder、malformed result、invalid tool、缺省 inputSchema、line 上限、spawn 前配置校验）；`jessica-cli/tests/acp_stdio.rs` 新增 6 个真实进程 E2E（分页发现+namespaced 调用+secret 不出现在 provider/协议/stderr、policy 拒绝且不触达 server、250ms 工具超时并 close reap、init-error/list-error/malformed 启动失败 -32603 且 reap、stdin EOF shutdown 回收子进程、HTTP MCP -32602 后服务存活）；fixture 为 `jessica-cli/tests/fixtures/mcp_stub.py`（normal/init-error/list-error/malformed/no-tools/call-error、pid/call-log/secret/require-secret env）。偏离/说明：1) E2E 放入既有 `acp_stdio.rs` 而非新开 `tests/mcp_stdio.rs`，复用 `AcpProcess`/`MockProvider`/`TempDir`；2) fixture 硬性依赖 `/usr/bin/python3`，缺失时测试显式失败并报告路径，不静默跳过；3) 仅 `session/new` 启动 MCP，`session/load`/`resume` 对非空 `mcpServers` 仍返回 `-32602`；4) 固定上限且无 MCP 专属配置项；5) 未做第三方真实 MCP server 或 Paseo MCP 手工验收（Paseo 配置已标 `supportsMcpServers: true`）；6) 未修改 jessica-core/jessica-memory、未新增第三方 crate。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 727 passed, 0 failed, 2 ignored；`jessica-cli` 196 unit + 37 ACP E2E。
Task ACP-6a ☑ 约 25 分钟 仅完成 ACP-6 的 core/runtime 前置：`jessica_core::tool::Tool` 新增带默认实现的 `execute_with_call(&ToolCall) -> ToolResult`，默认转发 `execute(call.arguments.clone())`，`execute` 保持必需；`jessica-runtime` AgentLoop 改调 `execute_with_call(&call)`，保留 `ToolCallStarted` → 执行 → `ToolCallFinished` 顺序、取消 select、结果 id stamping、unknown-tool 与预算行为。未实现 ACP 权限请求、未改 `GovernedTool`/ACP handler/其他 crate、未新增依赖。测试：core 2 个（默认实现转发 arguments、override 收到原始 id/name/arguments）；runtime 1 个（真实 provider `call.id` 传入 `execute_with_call` 并回填结果）；既有 runtime 工具调用/取消测试全部通过。偏离/说明：按用户决定采用方案 A 并拆出 ACP-6a；ACP-6 权限接入仍未实现，等待后续 Task。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 730 passed, 0 failed, 2 ignored。
Task ACP-6 ☑ 约 90 分钟（跨上下文恢复，未精确计时） 实现 ACP `session/request_permission`，范围按用户确认：敏感固定集 `bash`/`write_file`/`delegate_task`（`read_file`、WASM、MCP 仍仅受 policy）；仅顶层 `delegate_task` 询问，子工具在该次批准下运行；答复超时固定 5 分钟；敏感工具先 `pending`，`allow_once` 后先发同一 `toolCallId` 的 `in_progress` 再执行。`governance` 新增 `ToolPermissionDecision`/`ToolPermissionGate` 与 `GovernedTool::with_permission_gate`；顺序固定为预算触发 → policy → permission → 工具超时启动 → `execute_with_call` → 结果大小；空 `call.id` 不询问客户端直接失败；Reject/Cancelled/TimedOut/TransportFailed 全部 fail-closed、不执行工具；权限等待不计入 `budget.tool_timeout_ms`。`app` 的 `TurnRequest.permission_gate` 只传给 `write_file`/`bash`/`delegate_task`，CLI/resume 传 `None`。`acp_events` 敏感工具 `tool_call` 为 `Pending`（ACP wire 省略默认 status），新增 permission request/in-progress 更新并复用 provider id/title/kind/rawInput/locations。`acp` 新增 `AcpPermissionGate` 与 `AnnouncedToolCalls`：确认 pending `tool_call` 已进入同一 writer FIFO 后才入队请求；仅提供 `allow_once`/`reject_once`；固定 `TOOL_PERMISSION_TIMEOUT=300s`；客户端 `cancelled` 取消整个 prompt，超时/传输失败只失败当前工具；每请求 child token 取消时 writer 丢弃 `SentRequest` 并发 `$/cancel_request`；本地连接终止与 peer JSON-RPC 错误区分；未知 option/outcome 一律 Reject。测试：governance 17（含顺序、策略/预算先于权限、超时隔离、空 id、非 Allow 不执行、policy-only 不询问）；acp_events 14（pending/in_progress、同 id 复用）；acp 28（新增 9 个 gate 单测）；`tests/acp_stdio.rs` 42（新增 5 个真实进程 E2E：allow_once 顺序/选项/同 id/副作用、reject_once 不执行且模型可见原因、客户端 cancelled 结束 turn 且 session 可继续、pending 时 session/cancel 不执行且 session 可继续、delegation 仅父级询问且子 bash 执行；既有 bash 用例改为 pending→in_progress，close/delete/cancel 容忍 `$/cancel_request`）。README 新增 Permission requests 章节并移除“No permission requests”。偏离/说明：固定集与 5 分钟期限无配置项；只支持一次性批准、不持久化；超时只失败工具，只有客户端 cancelled/prompt 取消/close/delete/disconnect 结束 turn；子代理工具不二次询问；未改 jessica-core（ACP-6a 已完成前置）、jessica-memory、Cargo.toml，未新增第三方 crate。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 757 passed, 0 failed, 2 ignored（50 suites）；`cargo test -p jessica-cli --test acp_stdio` 42 passed；`cargo test -p jessica-cli --bin jessica-cli acp::tests` 28 passed；governance 17；acp_events 14。
Task ACP-7b ☑ 约 30 分钟（含实现、8 个 E2E、官方 TypeScript ACP SDK 与 Paseo 0.15.3 验收） 仅实现 `session/list`/`delete`/`resume`/`close` 与索引枚举/删除；未实现 ACP-5/6、MCP、权限请求、archive/fork/status/set_title、ACP v2，未修改 jessica-core/jessica-memory/Cargo.toml，未新增第三方 crate。能力：`loadSession` 与 `sessionCapabilities.list/delete/resume` 仅在 `[events].path` + `[acp].state_dir` 成功打开时声明；`close` 始终声明，因为只释放内存 session。list：只枚举 canonical state_dir 中 stem 为小写十六进制、version=1/非空 id/绝对 cwd/非空 turn id、文件名与 id 一致的普通 `.json`；损坏/版本不符/错名/目录/符号链接跳过并 warn；过滤启动 root，可选 cwd 精确匹配；按 updatedAt 降序、session id 升序；固定页 100；游标 `acp-list-v1:{updatedAtMillis}:{sessionId}`（≤512B，非法 `-32602`）；只返回 sessionId/cwd/UTC RFC 3339 毫秒 updatedAt，无 title/内容/turn id。resume：拒绝非空 mcpServers/additionalDirectories/systemPrompt；同 cwd 校验；未知 `-32002`；历史重建复用 load，但零 replay；live 同 cwd 幂等。close：不要求持久化；live 先 remove、取消 lifetime token、经 per-session watch 最多等 5s；索引保留，close 后仍 list/load/resume；索引存在但未 live 幂等；无持久化/未知 `-32002`。delete：逻辑删除，先 mutation 锁下删索引，再 remove live 并最多等 5s；事件库不删不改；索引不存在但 live 成功；两者皆无 `-32002`；mutation 锁串行化 create/record/delete，晚到 prompt commit 不会重建索引。store 新增 list/exists/delete/read_index；exists 用 symlink_metadata，delete remove_file 幂等；load 对损坏直查 fail-closed，list 跳过；原子写保留。`AcpSession` 新增 watch 活动信号，finish_prompt 广播，wait_idle 先订阅再检查避免丢失唤醒。错误映射：未知 `-32002`、非法 `-32602`、无持久化 `-32601`、内部固定消息 `-32603`，不泄露路径/内容。测试：acp_store 新增 2（list 跳过无关/损坏/版本/错名；delete 只删索引且幂等），当前 9；ACP 服务单测新增能力矩阵/游标/UTC 日期/lifecycle 拒绝；`tests/acp_stdio.rs` 新增 8（list 元数据与损坏跳过；close 释放 live 且保留索引可 reload；close 取消 active 并等停止；resume 零 replay；delete 逻辑删除且事件库仍在；delete active 不重建索引；101 session 分页 100+1 无重复/丢失；无持久化 load/list/delete/resume `-32601` 而 close 成功），E2E 23→31。真实客户端（2026-11-16 UTC）：官方 `@agentclientprotocol/sdk` 2.7.0 临时脚本验证两 live session 的 list 元数据/cwd 过滤/非法游标、close 保留索引、delete 后 events.db 大小不变且 load `-32002`、重启后 list、resume 零 replay 且后续 provider 请求包含恢复上下文、live resume 幂等、再次重启 load 响应前 replay 且不含已删除 session；Paseo 0.15.3 隔离 home `/tmp/jessica-acp7b-paseo/home`、daemon `127.0.0.1:6769`、workspace `/home/ubuntu/jessica-acp7b-paseo-workspace`、临时 wrapper + 本地 mock `127.0.0.1:46669`，provider diagnostic Ready，首轮 run 成功，daemon stop/start 后 agent closed，`agent send` 经 load 保留首轮上下文并更新索引为 2 turns，`paseo delete` 后 ACP 子进程退出但索引仍保留，`paseo import <sessionId>` 恢复两轮历史且未新增 provider 请求；Paseo CLI 无直接 listImportableSessions 命令，list/delete/resume 由官方 SDK 覆盖。偏离/说明：close/delete drain 上限 5s，超时仍确认；list 包含零 turn 空索引（Paseo probe 会创建）；load/resume 与 delete/close 无全局串行化；list 是目录快照、跨页并发会漂移；RFC3339 手写无新依赖；未改部署 `~/.local/bin/jessica-cli`。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 713 passed, 0 failed, 2 ignored（50 suites）；`cargo test -p jessica-cli acp` 61 unit + 31 E2E 全通过。

Task 7.0a ☑ 约 45 分钟（含远端 CI 与三次失败探针验证） — 新增 `.github/workflows/ci.yml`：push(main)/pull_request/workflow_dispatch，同一 workflow+ref 并发取消旧任务，`permissions: contents: read`；rustup 按 `rust-toolchain.toml` 安装 1.99.1 + rustfmt/clippy，缓存 `~/.cargo/registry`/`~/.cargo/git`/`target`（key 含 `Cargo.lock` 与 `rust-toolchain.toml`）；依次执行 fmt/clippy/test --locked/sh -n install.sh；仅使用 GitHub 官方 `actions/checkout@v4` 与 `actions/cache@v4`，无 secrets、不访问真实模型 API。README 增加“Development checks”。本地验收：YAML 解析通过；`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace --locked`（50 suites, 757 passed, 0 failed, 2 ignored）、`sh -n install.sh`、`git diff --check` 全绿。远端验收：PR #29（https://github.com/victorllm/jessica/pull/29，head 649cf001）run 44300376015 全绿，fmt/clippy/test/installer 四步均 success。失败探针：临时分支 `ci/failure-probe` + draft PR #30（未合并，验证后关闭并删除分支）；最终探针 `jessica-cli/tests/aa_ci_failure_probe.rs` 在 run 44303163964 中 `Check formatting` 与 `Run Clippy` success，`Run tests` failure，日志显示 `ci_failure_probe` panic 于该文件第 3 行，确认测试门禁真实生效。偏离/说明：1) 本机 `github.com:443` 不可达（TLS recv error/连接超时），分支与提交改由 GitHub Git Data API 创建，本地提交 1553df1 与远端 649cf001 内容相同（tree 均为 add2dbaf4abe63054287483871dbc85a6789dbda）但 SHA 不同；PAT 需要 Workflows 写权限才能创建 `.github/workflows/ci.yml`。2) 首次探针 0b488c6 的断言未通过 rustfmt，run 44301798032 先在 `Check formatting` 变红，只证明格式门禁；第二次探针 ec44b333 位于 jessica-core，但 `cargo test --workspace` 先运行 jessica-cli，run 44302009658 因既有 shutdown 测试间歇失败而在探针执行前终止；第三次把探针移到 jessica-cli 并排在 shutdown 测试之前，才观察到预期的 `Run tests` 失败。3) run 44302009658 暴露既有测试 `shutdown_e2e::sigint_on_an_idle_repl_exits_130_without_creating_a_turn` 在 CI 中间歇失败（同分支绿色 run 44300376015 通过），已记入 Deferred，未在本次 Task 修改。4) 本 Task 未修改 `jessica-core`（探针文件仅存在于已删除的临时分支）。
Task 7.0b ☑ 约 20 分钟 — 版本号经用户确认采用 0.10.0。版本单一来源：根 `Cargo.toml` 新增 `[workspace.package] version = "0.10.0"`，8 个成员 crate 全部改为 `version.workspace = true`，`Cargo.lock` 中 8 个 package 同步为 0.10.0，无其他版本字面量。二进制命名：`jessica-cli/Cargo.toml` 新增 `[[bin]] name = "jessica" path = "src/main.rs"`，package 名仍为 `jessica-cli`，`-p jessica-cli` 构建命令不变；`target/{debug,release}/jessica` 为唯一构建产物。`--version`/`-V`：在 `main.rs` 的 `parse_args` 中于 provider/config 装配前识别，输出 `jessica {CARGO_PKG_VERSION}` 并退出 0；新增 `jessica-cli/tests/version_e2e.rs` 行为测试，用 `CARGO_BIN_EXE_jessica` 跑真实二进制，断言两种 flag 的 stdout、exit 0、stderr 为空，并把 `JESSICA_PROVIDER` 设为非法值以证明版本输出不依赖 provider 装配。install.sh：安装目标改为 `$BIN_DIR/jessica`；原子创建兼容符号链接 `jessica-cli -> jessica`（临时名 + `mv -f`，可覆盖旧安装的普通文件），cleanup 处理符号链接；Paseo launcher 的 `--version`/`-v` 改为 `exec "$self_dir/jessica" --version`，ACP 启动改为 `exec "$self_dir/jessica" acp ...`，删除 `git describe`/Cargo.toml 解析出的硬编码 `paseo_version` 与 `@JESSICA_VERSION@` 替换；usage、提示与安装报告同步改名。README 更新安装说明、兼容符号链接、Paseo launcher 版本探测（自定义 launcher 须自行应答 `--version`）、全部示例命令与 Zed/Paseo JSON 二进制路径；10 个 E2E 测试的 `CARGO_BIN_EXE_jessica-cli` 改为 `CARGO_BIN_EXE_jessica`。验收：`cargo build --workspace --locked` 通过；`cargo run -p jessica-cli --bin jessica -- --version` 输出 `jessica 0.10.0`、exit 0；临时 HOME 默认 `~/.local/bin` 安装与隔离 `--bin-dir`/`--home` 安装后，`jessica --version`、`jessica-cli --version`、`jessica-paseo --version` 均为 `jessica 0.10.0` 且 exit 0，兼容链接覆盖了预置的普通文件，合并后的 Paseo provider command 指向生成的 launcher；`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace --locked` 51 suites, 758 passed, 0 failed, 2 ignored；`sh -n install.sh` 通过。偏离/说明：1) 采用推荐的兼容策略（保留 `jessica-cli` 符号链接一个发布周期），未全量改名；2) 真实 `paseo provider diagnostic jessica` 未执行：本机 Paseo 0.15.3 的用户 daemon 正在 `0.0.0.0:6767` 运行，用隔离 `--home` 启动诊断 daemon 会争用端口或需停掉现有 daemon，故只直接验证生成的 launcher、其 `--version` 输出与合并后的 config；3) launcher 继续接受 `-v` 并转发到 `jessica --version`，CLI 本身按 Task 只支持 `--version`/`-V`；4) 未修改 `jessica-core` 公共 API，未打 tag/发布 release，未提交 DESIGN.md（留给 Task 7.0c）。
Task 7.0c ☑ 约 30 分钟 — 发布自动化与仓库卫生，未修改 `jessica-core`、未新增第三方 crate、未打 tag/发布 release。Release workflow：新增 `.github/workflows/release.yml`，仅 `push` `v*` tag 触发，`permissions: contents: write`，同一 ref 不取消进行中的发布；ubuntu-24.04 上按 `rust-toolchain.toml` 安装 1.99.1，`cargo build --release --locked -p jessica-cli`，先用 `jessica --version` 校验 tag（`v0.10.0` 必须对应 `jessica 0.10.0`，否则失败），再打包 `jessica-linux-x86_64.tar.gz` 并生成 `jessica-linux-x86_64.tar.gz.sha256`；通过预装 `gh` 创建 release（已存在则 `gh release upload --clobber` 幂等更新），最后从真实 release 下载两个资产、`sha256sum -c` 校验并运行产物 `--version`，仅使用 GitHub 官方 actions 与 `GITHUB_TOKEN`，无额外 secrets。install.sh：新增 `--from-release`，默认仍从 checkout 编译；release 分支通过 `uname -s`/`uname -m` 选择 `linux-x86_64`（其他平台明确报错并提示本地编译），用 curl（https 强制 `--proto '=https' --proto-redir '=https'`）或 wget 下载 tar 与 `.sha256`，校验和必须是 64 位十六进制且与产物一致，否则在创建安装目录前中止；校验通过后仅从 tar 提取 `jessica` 成员（拒绝符号链接），再复用既有临时文件 + `mv -f` 原子安装与 `jessica-cli -> jessica` 兼容链接。release 模式不要求 Cargo.toml/cargo，下载/校验失败均输出清晰错误并提示 `./install.sh` 本地编译；`--paseo`、`--home`、`--bin-dir` 等选项在两种模式下保持一致，`JESSICA_RELEASE_BASE_URL` 可覆盖下载基址（镜像/测试）。README：删除 `DESIGN.md` 引用，改为指向 `CLAUDE.md` 与 `PLAN.md`（用户明确要求 DESIGN.md 继续不跟踪，采用 Task 的“删除引用”分支）；新增 release 安装说明、平台范围、SHA-256 仅校验完整性而非签名、失败回退本地编译与 `JESSICA_RELEASE_BASE_URL`；首句拼写修正为 `deterministic`，验收夹具移至新文件 `examples/typo.md`，`examples/harness.toml` 的 acceptance 改用 `examples/typo.md` 并将名称改为 `fixture-spelling`，README/配置注释中的演示 prompt 同步指向该文件；`jessica-cli/src/config.rs` 的示例配置测试改为断言新名称且 acceptance 参数必须包含 `examples/typo.md`。验收：用 `target/release/jessica` 构造本地 fake release（tar + `.sha256`，本地 HTTP 服务），把 `install.sh` 单独复制到无 checkout 的临时目录，并以无 cargo 的 `HOME`/`PATH` 执行 `--from-release`，安装成功且 `jessica --version`/`jessica-cli --version` 均为 `jessica 0.10.0`，兼容链接指向 `jessica`；另一次带配置的 release 安装创建了 `harness.toml` 与 `acp-state/`；`--from-release --paseo` 同样成功，生成的 launcher `jessica-paseo --version` 输出 `jessica 0.10.0`，Paseo config 合并与 `paseo.env` 模板正常；篡改 tar 后 exit 1 且输出 `SHA-256 mismatch ... refusing to install`、未安装任何 binary；缺失 `.sha256` 与连接失败也分别以清晰错误中止且未安装；本地编译模式仍成功（报告 `source: local build`）。夹具演示：直接执行 acceptance 命令，`examples/typo.md` 初始失败，替换为正确拼写后通过，再恢复为失败状态。tag 校验模拟：`GITHUB_REF_NAME=v0.10.0` 与产物匹配，`v0.9.0` 不匹配即拒绝；release workflow 经 PyYAML 解析验证结构与步骤。`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace --locked`（51 suites, 758 passed, 0 failed, 2 ignored）、`sh -n install.sh`、`git diff --check` 全绿。偏离/说明：1) 按用户“还是不跟踪 Design.md”的指令，未提交 DESIGN.md，采用删除 README 引用方案；因此 `git status` 仍显示 `?? DESIGN.md`，Task 的“git status 干净”验收项按用户指令豁免，其余改动待提交。2) 遵守禁止项，未打 tag、未创建真实 GitHub Release；真实 release 路径用本地 fake release + 模拟 tag 校验验证，workflow 尚未在 GitHub Actions 上实际运行。3) 仅构建 linux-x86_64；Task 中“条件允许时增加 aarch64/macOS”为可选项，本次未增加对应 runner/交叉编译配置。4) 可选 CHANGELOG 未新增，release notes 由 `--generate-notes` 生成。5) SHA-256 与产物同源下载，只证明完整性/未被损坏，不构成签名；README 与 help 均明确说明。6) 未用真实 API key 跑完整模型演示，只直接验证 acceptance 夹具的失败→通过；未运行真实 `paseo provider diagnostic`（本机用户 daemon 占用 6767 的既有约束）。

Task 7.1 ☑ 耗时未精确计时（跨会话执行，含方案确认；未单独记录起止时间） — 按用户批准的四项决策完成 A+B：新增 `jessica-core/src/trace.rs`（`TRACE_SCHEMA_VERSION=1`、`TurnId`、`TraceEvent` 包络、`TracePayload::{Agent,Gap,TurnStarted,TurnEnded}`、`TraceTurnMode`、`TraceGapReason`），`lib.rs` 仅新增 `pub mod trace;`，既有 `AgentEvent` 与冻结契约未改；用户已明确批准该 additive core 变更。`jessica-memory` 新增 `trace_store.rs`：`trace_events`/`trace_heads` 表、`PRAGMA user_version` 0→1 迁移（未来版本 fail-closed）、append-only UPDATE/DELETE trigger、O(1) seq 分配；`append_traced` 在单个 SQLite 事务内写 legacy 与 trace，任一方失败整体回滚；读取校验序号连续、首条 `TurnStarted`、`TurnEnded` 后无条目、head `closed` 与终态一致，不完整轨迹可读且 `closed=false`。`EventStoreError` 增加 `UnsupportedVersion`、`TraceSerialize`、`TraceStore`、`TraceDatabase`、`TraceDecode`、`TraceInvalid`、`TraceSequence`、`TraceLifecycle`。CLI：新增 crate-private `EventJournal` seam（生产 `EventRecorder`、测试注入失败），`record_traced` 使用同一时间戳；新增 `GovernanceState::cancel_for_integrity_failure`；pump 维护 turn/attempt→session 归属，`AcceptanceFailed` 依据 payload attempt 归属被判 attempt 且只写 trace（避免落在 `SessionEnded` 之后破坏 recovery/replay）；metadata/trace/legacy 任一失败即 fail-closed：停止写入、取消 agent、禁止 `TurnEnded`，turn 返回 `CliError::TraceIncomplete`；存在 recorder 时 lag 写 `Gap{BroadcastLagged}`、取消并以非零退出，无 recorder 时保持既有 warn-and-continue；预取消 turn 也写完整空 turn；`--resume` 写 `TurnStarted{Resumed}` 且不重放已恢复批次。测试新增 23 个：core trace 4、memory trace_store 12（无损读取、未终止、生命周期回滚、包络校验、事务回滚、append-only trigger、v0 升级、未来版本拒绝、summary 排序、独立序号、JSON 字节重开、损坏拒绝）、pump 4（acceptance 归属与 legacy 镜像、存储失败停止写入/取消/不关闭、lag gap 阻止关闭、attempt 先于 turn 被拒）、app 3（完整 turn 的闭合 trace 与 legacy 镜像、acceptance 重试的 trace-only 失败与双 session、metadata 冲突 fail-closed 且 provider 零调用）；resume E2E 增加 Resumed 轨迹断言。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 52 suites, 781 passed, 0 failed, 2 ignored。偏离/说明：1) 为拒绝“存储失败后继续写”的不完整轨迹，`RecordingJournal` 测试替身只保留 trace 失败注入，metadata 失败改由 app 层真实 `EventRecorder` + `sessions` 冲突测试覆盖；2) 预取消 turn 通过 `close_empty_turn` 写 `TurnStarted`+`TurnEnded`，保证“正常返回即有闭合 trace”；3) acceptance 失败不入 legacy 是必要偏离（原计划未细化），否则 recovery/replay 会把失败事件放在 attempt 终态之后；4) resume 测试在原 E2E 上扩展，不新增独立测试文件；5) 未提交 DESIGN.md、未新增第三方 crate、未打 tag/发布 release。

Task 7.2 ☑ 耗时未精确计时（跨上下文执行；含方案确认） — 按用户批准的方案把持久化迁到生产者侧。`jessica-core` 新增 additive `events` 模块（`EventSink`/`EventSinkError`、`pub mod events;`），用户已在设计阶段明确批准该 core 变更；其余冻结 API 未改。`jessica-runtime` 的 `AgentLoop` 增加可选 sink：事件先 `emit` 后广播，sink 拒绝即停止且不广播被拒事件或后续终态，`SessionEndReason::Cancelled` + `error="event sink failed"`；新增顺序 sink 与拒绝 sink 两个行为测试。`jessica-harness` 的 `AcceptanceRunner` 在广播 `AcceptanceFailed` 前 `emit`，拒绝时不重试、不广播并返回 `Error`；新增“先于广播”和“拒绝后停止重试”两个测试。CLI 新增 `jessica-cli/src/journal.rs`：turn 级 `TurnJournal` 单写任务、容量 1024、`try_send` 快路径、满队列 `send().await` 背压、FIFO 调 `EventJournal::record_traced`；`begin_turn`/session 元数据/outer session/`finish_turn` 全部 acked，`finish_turn` 只在先前的生产者事件与 `TurnEnded` 提交后成功；首个写失败保留原始原因、停止写入、取消 agent 并经 `watch` 通知 attempt；非停止型 lag 失败仍允许记录生产尾部但禁止关闭；关闭后迟到事件只拒绝、不污染已提交轨迹。`EventPump` 移除全部 trace/legacy/`Gap` 写入与归属状态，仅保留重建、sensor、guard 与 sink；存在 recorder 时 lag 调用 `fail_integrity(..., stop_writes=false)`，不写 `Gap` 且 turn 永不关闭；无 recorder 时行为不变。`GovernanceState` 的 `emit`/`trip`/`charge_tokens` 改为 async 并接入 sink，token 计费在 `Done` 产出前完成，保证预算事件顺序。`app` 在 `build_turn` 装配 journal、sink、pump，attempt 等待 pump 与 journal failure signal 的竞速，outer session 先落盘再广播，所有失败路径 flush 后返回 `CliError::TraceIncomplete`。新增 11 个行为测试：journal 7（成功 turn 的 FIFO/归属/`TurnEnded` 最后、写失败停止且不关闭、满队列背压零丢失、lag 后记录尾部无 `Gap` 且不关闭、非停止失败后满队列仍等待容量、pump lag 无 `Gap`、attempt 归属约束）、runtime 2、harness 2；既有 app trace/acceptance/metadata 测试与 pump 无 recorder lag 测试继续覆盖端到端路径。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 52 suites, 790 passed, 0 failed, 2 ignored。偏离/说明：1) 为通过 clippy 的 `large_enum_variant`，队列中的 `TraceEvent` 使用 `Box`，`prepare_event` 返回值改为私有 `PreparedEvent` 结构；2) lag 属于观察者失败而非存储失败：`writes_stopped=false`，生产尾部继续记录（队列满时也继续等待容量而非丢弃），但 `ensure_writable`/`finish_turn` 仍拒绝关闭，这是“trace 完整但 turn 不可闭合”的设计语义；3) `Gap` 变体保留用于读取兼容，但 CLI 不再产生；4) 未新增 app 层强制 broadcast lag 的独立 E2E，lag 的 pump+journal 集成由 journal 单测覆盖，app 层由既有真实 SQLite trace 测试覆盖生产者路径；5) 未实现 fsync/掉电保证、子 Agent trace、verify/replay parity、retention/GC；6) 未修改 `AgentEvent`/`TraceEvent` schema、未新增迁移、未新增第三方 crate；7) 未提交 DESIGN.md（保持未跟踪）、未打 tag/发布 release。
Task 7.3 ☑ 耗时未精确计时（跨上下文执行；含方案确认） — 按用户批准的事件模型 v2 方案完成 additive core 扩展与全链路接入。`jessica-core`：`SessionStarted.messages`、`RoundStarted`/`RoundFinished`、`PermissionDecided`、`ChildSessionStarted`/`ChildSessionEnded`、`PermissionDecision`；`required_trace_schema_version()` 标记 v2 词汇；`TRACE_SCHEMA_VERSION=2`、`MIN_SUPPORTED_TRACE_SCHEMA_VERSION=1` 与范围校验，包络字段/payload 变体不变。`jessica-runtime`：`AgentLoop` 在模型调用前发 `RoundStarted`、响应后工具前发带 usage 的 `RoundFinished`，resume 从恢复轮次续号；`SessionStarted` 携带 bootstrap messages；recovery 事件优先读取 messages、显式 round 校验/重建/usage 无关，畸形序列 fail-closed；`AgentChildExecutor` 经 sink/广播发父侧 ChildSessionStarted/Ended，start sink 失败不调用模型，end sink 失败转 error result。`jessica-memory`：trace_store 接受 v1 与 v2、拒绝 v1 包络承载 v2 payload、append/read 两侧拒绝同 turn 混版本、拒绝 v3+；`event_type_tag` 补全新词汇。CLI：`GovernedTool` 在执行/拒绝前发 `PermissionDecided`，Allow 的 sink 失败阻止执行并取消；journal 将 permission/child 事实只写 trace 并归属当前 attempt/session；pump 支持显式轮次渲染；`EventRecorder::load` event-first messages、元数据回退、冲突 Integrity；replay 显式 round 边界重建与 v1 启发式，v1 源剥离合成 round/messages 并给 `Done{usage:None}`。测试新增 17 个行为测试：memory trace 兼容 4、runtime recovery 3（messages 权威性与畸形序列）、runtime delegation 3、CLI governance 2、events 3、replay 1、journal 1；既有 agent_loop/recovery/replay/resume/delegation E2E 断言更新为 v2 词汇。偏离：permission/child 生命周期只写 trace 不写 legacy（replay 不重跑权限门/子会话，双写会破坏逐字节 replay）；v1 replay 输出剥离合成 v2 字段；v1 会话被 v2 build resume 后形成的混合 legacy 流暂不支持 replay（见 Deferred）。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 812 passed, 0 failed, 2 ignored（52 suites）。未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 7.4 ☑ 耗时未精确计时（跨上下文执行；含方案确认） — 按用户批准的只读验证方案交付 trace verify CLI 与 trace↔legacy 镜像校验。`jessica-memory`：新增 `EventStore::open_read_only`（`SQLITE_OPEN_READ_ONLY`，不创建缺失文件、不安装/迁移 schema、拒绝未来版本）与结构化 `EventStoreError::TraceSchemaMissing`；未改 SQLite schema 或既有 API 语义。`jessica-cli`：新增 `trace_verify` 模块（纯 verifier/report、文本与稳定 JSON 渲染）与 `Mode::Trace`/`trace verify` 参数解析（`--turn`/`--all` 互斥且必选其一、`--json`、`--allow-open` 仅限 `--all`），`[events].path` 必填并按 workspace root 解析；验证分三层：结构（`trace_turn` 错误归类为 `trace_structure`，`--all` 逐 turn 继续；无 `TurnEnded` 为 open）、v2 语义（envelope session 一致性、attempt/session 归属、round 嵌套/序号/相位、权限 Allow/Deny 解析、child 配对/闭合/计数）、镜像（排除四类 trace-only 事件，其余按 `(session_id,timestamp,payload_json)` 多重集匹配 legacy）；存储错误中止，`--allow-open` 只把 open 视为通过、绝不放宽 invalid，未知 `--turn` 为错误，空库为 0 turn 成功。只读路径不初始化 provider/wasm/memory/skills/runtime，SQLite 经 `spawn_blocking` 调用。测试新增 27 个：memory 4、verifier 13、CLI 参数 3、E2E 7；E2E 用真实 SQLite/真实 CLI，并通过 workspace 已有的 `rusqlite` dev-dependency 直接注入一个无 events 的损坏 head，证明 `--all` 不会因单个坏 trace 中止。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 839 passed, 0 failed, 2 ignored（53 suites）。未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 7.5 ☑ 耗时未精确计时（跨上下文执行；含方案确认） — 按用户批准的单 turn 执行一致性方案交付 `trace verify --replay`。`jessica-cli`：新增 `trace_replay` 模块（可重放子集判定、attempt 分组、事件过滤、调用既有 `replay_session`、按原 trace seq 逐事件比较、mismatch 定位 seq、外层 session 静态校验）；`trace_verify` 集成 `--replay`（仅 `--turn`）、`report_version` 1→2、per-turn `replay` 字段（`null` 或 `{status, attempts, events, reason?}`）、文本 replay 摘要、unsupported/mismatch 的退出语义；`main.rs` 参数解析与 usage；`CliError::TraceReplayUnsupported`。可重放范围仅 closed v2 fresh turn、无 Gap、attempt 1..n 连续、每 attempt 一个 session 且 `SessionStarted` 首/`SessionEnded` 末、终态 Completed/Error 或仅 MaxRounds 的 BudgetExceeded；v1、Resumed、Cancelled、AcceptanceFailed、缺失 start/end、非连续 attempt、非 MaxRounds budget 均显式 unsupported、exit 1。每次 attempt 以 `SessionStarted.messages` + 录制事件驱动真实 `AgentLoop`，排除 trace-only（PermissionDecided、ChildSession*、AcceptanceFailed）与 CLI governance（PolicyDenied、非 MaxRounds budget）；mismatch 生成 `replay_mismatch` problem、turn invalid、exit 1。未改 SQLite schema、`jessica-memory`、既有 `replay --session` 行为或冻结 `jessica-core` API，未新增第三方 crate。测试新增 15 个：`trace_replay` 11、CLI 参数接受/拒绝（并入既有参数测试）、E2E 4（文本/JSON 成功、非 parity invalid、v1 unsupported、batch 拒绝），既有 `report_version` 断言更新为 2。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 854 passed, 0 failed, 2 ignored（53 suites）。偏离/说明：1) 对 open/invalid trace，`--replay` 不运行 parity，只在报告中写 unsupported，并让既有验证失败优先，仍非静默跳过；2) 外层 session 只做静态校验，不重放；3) E2E 使用真实 SQLite/真实 CLI 与手工构造的 trace，不启动 provider 或真实 turn；4) 按方案拒绝 `--all --replay`，批量 parity 留在 Deferred。未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.1 ☑ 约 30 分钟（含只读核对、实现与全量验收；未精确计时） — 修复 main CI run 44431198725 暴露的 shutdown 诊断竞态。根因：`ShutdownController::on_signal` 先记录 signal 并取消 root token，`handle_signal` 之后才写 `eprintln!`；REPL 被取消唤醒后 `main` 读取已记录的 signal 并以 130 退出，可能抢在诊断行到达 stderr 之前。`jessica-cli/src/shutdown.rs`：移除有副作用的 `on_signal`，新增只读 `classify_next() -> SignalAction`（第一条 Cancel、第二条 Force、之后 Ignore）；`handle_signal` 先分类、写诊断并 flush stderr，再 `request_shutdown`（第一条）或 `force` + `process::exit`（第二条）；模块文档明确“先写诊断、后提交状态”及原因。测试：shutdown 单测改为两阶段断言（分类阶段不记录 signal、不取消 child token、不置 forced；提交后才可见）；新增 E2E 压力测试 `every_idle_sigint_writes_its_diagnostic_before_exiting`，用真实 CLI 重复 20 次 idle REPL + SIGINT，每次断言 exit 130、stderr 含 `SIGINT received; stopping new turns and draining`、无 panic；原 idle SIGINT 测试抽取 `run_idle_sigint` helper 复用。未改 SIGINT=130/SIGTERM=143/第二次信号强制退出语义，未新增信号，未改 `jessica-core`、`jessica-memory` 或 SQLite schema，未新增第三方 crate。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 855 passed, 0 failed, 2 ignored（53 suites）；shutdown E2E 5 passed（含 20 轮压力），shutdown 单测 5 passed。偏离/说明：1) 修复前本地连续 20 次串行 + 4 路并发共 40 次未能复现该竞态，故以 CI 失败日志定位根因；修复用结构性顺序保证并新增压力测试防回归，未通过人为延迟制造失败；2) 已提交并合并为 PR #37（merge `321e0ba`），PR CI 与合并后 main CI run 44470072803 全绿；3) 本地 `DESIGN.md` 保持未跟踪。

Task 8.2a ☑ 约 60 分钟（含方案确认、实现、格式化与全量验收；未精确计时） — EventStore 启用 WAL 与 `synchronous=FULL`，并把 schema 安装改为事务化迁移。`jessica-memory/src/lib.rs`：`initialize` 先读 `user_version`，未来版本直接拒绝且不写库；随后设置 `PRAGMA journal_mode=WAL`（文件库必须返回 `wal`，内存库接受 `memory`，否则 `EventStoreError::JournalMode`）与 `PRAGMA synchronous=FULL`；新增 `read_user_version`。`install_schema` 改为按版本循环迁移：每个版本在独立 SQLite transaction 内执行 DDL 并写 `user_version`，失败整体回滚；迁移 1 = 现有 legacy `SCHEMA` + `trace_store::TRACE_SCHEMA`；v1 库仍执行幂等 `IF NOT EXISTS` 安装以保留 schema 修复行为。新增 `apply_migration`、`configure_wal` 与 `EventStoreError::{JournalMode,MigrationUnavailable}`；模块、`open`、`EventStore`、`SCHEMA_VERSION` 文档同步更新。测试：`jessica-memory/tests/durability.rs` 4 个（文件库 WAL 与 clean close 无 sidecar、未来版本不写库即拒绝、失败迁移回滚 DDL 与 `user_version`、v1 缺失 trigger 自动修复）；`lib.rs` 单测 1 个（内存库 `memory` 与 `synchronous=FULL`）；既有 v0→v1 升级测试继续通过。未改 `open_read_only` 的 immutable/pending-WAL 契约、CLI、`jessica-core`、trace 语义、schema 版本或第三方依赖。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 860 passed, 0 failed, 2 ignored（54 suites）。偏离/说明：1) 原计划用不匹配的 `trace_heads` 表触发迁移失败，但 SQLite 允许触发器引用不存在的列；改为预置名为 `sessions` 的 index 使 `CREATE TABLE sessions` 失败，真实触发事务回滚；2) WAL 在迁移事务之前设置，迁移失败时 journal_mode 保持 `wal`，测试已断言；3) 8.2b 的只读 immutable/pending-WAL、SIGKILL/磁盘满测试未实现。未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.2b ☑ 约 45 分钟（含方案确认、实现、格式化与全量验收；未精确计时） — 完成只读审计契约与崩溃/磁盘满测试。`jessica-memory/src/lib.rs`：`open_read_only` 在打开前检查 `-wal` sidecar，非空返回新增 `EventStoreError::PendingWal { path, bytes }`，元数据读取失败返回 `WalInspect`；改用 `file:<percent-encoded path>?mode=ro&immutable=1` 并以 `SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_URI` 打开，`immutable=1` 不建 `-shm`/不加锁，支持 0555 只读目录且不产生任何 sidecar；新增 `immutable_uri` 仅保留 `A-Za-z0-9-._~/` 与 `:`，其余字节（`?`/`#`/`%`/空格/非 ASCII）按 UTF-8 percent-encode，非 UTF-8 路径返回 `NonUtf8Path`；空 `-wal` 允许，未来版本、缺 trace schema、缺文件语义保持不变。测试：新增 `tests/read_only_audit.rs` 5 个（活写者非空 WAL 拒绝并报告真实字节数、clean close 后可审计且不建 sidecar、空 WAL 接受、0555 目录 + 0444 文件可读且目录无 sidecar、含 `?`/`#`/`%`/空格/中文路径可读）；`tests/durability.rs` 新增 SIGKILL 提交恢复测试（`current_exe --exact` 重入 helper 进程，提交 3 个事件并 flush 哨兵后休眠；父进程确认 `-wal` 非空、`Child::kill()` 后重开，断言 3 个事件、seq 1..3 与 payload 顺序，并在 clean close 后只读审计成功；helper 无环境变量时为空操作）；`lib.rs` 单测新增磁盘满回滚（`PRAGMA max_page_count = page_count + 1` 触发真实 `SQLiteFailure(DiskFull)`，断言 `Append` 错误、事件数与 `last_seq` 不变，提高上限后同一 store 可继续追加）。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 868 passed, 0 failed, 2 ignored（55 suites）。偏离/说明：1) 磁盘满用 SQLite `max_page_count` 在页分配层制造真实 `SQLITE_FULL`，未挂载实际满盘文件系统（无需 root/loop 设备）；2) SIGKILL 用同一 integration test 二进制的 helper 重入实现，因此套件多一个无环境变量时立即返回的 helper 测试；3) `immutable=1` 连接上 SQLite 报告 `journal_mode=delete`（实测行为），审计测试不断言 `wal`；4) 未改 CLI/`jessica-core`/schema 版本/trace 语义，未新增第三方 crate，未实现 8.3+。未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.3 ☑ 耗时未精确计时（跨上下文执行；含方案确认、实现、格式化与全量验收） — 交付 trace 哈希链、`trace digest` 与 `verify --expect`。`jessica-memory`：新增 `trace_chain(turn_id, seq, prev_hash, entry_hash)`（`WITHOUT ROWID`、长度 CHECK、append-only UPDATE/DELETE 触发器）与 `trace_chain.rs`（领域分离 + u64 big-endian 长度前缀的 entry/digest 哈希、genesis、hex 编解码、回填与 golden vector 单测）；`SCHEMA_VERSION` 1→2，迁移 2 在单事务内建表并按 `(turn_id, seq)` 回填既有 `trace_events`，失败整体回滚；`repair_schema` 只补建缺失结构、绝不重算或回填；`append_traced` 在同一事务写 trace_events + trace_chain + trace_heads，并对既有 turn 校验最后链节与 head seq 对齐；`trace_turn` 在既有结构/生命周期校验后验证链条数、seq、prev 链接与重算 hash，`StoredTraceTurn::digest()` 暴露 digest；`open_read_only` 要求 v2，v1 返回新增 `TraceSchemaOutdated { found, required }` 且不迁移。`jessica-cli`：新增 `trace_digest` 模块与 `trace digest --turn|--all [--json]`（单 turn 仅输出 digest，all 按 turn_id 升序输出 tab 分隔行，JSON `report_version: 1`/`digest_format`/per-turn `digest/closed/entries`）；`trace verify` 新增 `--expect <64-hex>`（仅 `--turn`、大小写归一），不匹配产生 `digest_mismatch`、无 digest 产生 `digest_unavailable`，`report_version` 2→3 且 per-turn 增加 `digest`，文本打印 digest；参数解析、usage 与 README（命令、schema v2 审计要求、威胁模型）同步。测试新增 23 个：memory 链集成 6、链单测 6、CLI 解析 4、E2E 7（digest 单 turn 与 verify 一致、all 排序/JSON、空库、未知 turn、删链节、expect 匹配/大写/不匹配、结构损坏时 digest_unavailable），既有 E2E `report_version` 更新为 3；`durability`/`trace_store` 未来版本夹具与表清单更新到 v2/trace_chain。未改 `jessica-core`，未新增第三方 crate（复用 sha2），未实现跨 turn 聚合 digest 或 8.4+。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 891 passed, 0 failed, 2 ignored（56 suites）。偏离/说明：1) 哈希覆盖精确存储列而非重新序列化，保证迁移回填与在线追加一致；2) `--all` 仍按 `trace_heads` 枚举，孤儿 `trace_events` 行不可见，已记入 Deferred；3) 未提供跨 turn 聚合摘要/签名，留待 8.4；4) 未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.4 ☑ 耗时未精确计时（跨上下文执行；含方案确认、实现、格式化与全量验收） — 交付可移植证据包 export 与离线 verify。`jessica-memory`：`trace_chain.rs` 新增 `jessica.trace.bundle.v1` 领域与 `bundle_digest`（长度前缀、turn 计数、turn_id 升序、raw 32-byte digest），golden vector 锁定聚合/空聚合；新增 `TraceEvidenceEntry`（私有字段 + 访问器）、`trace_bundle_digest`（排序、拒绝重复 turn、规范 hex 解码）与 `verify_exported_trace`（要求非空/seq 连续/包络有效/payload turn 一致，写入私有内存 EventStore 的 trace_events/chain/heads，再调用 `trace_turn` 复用 8.3 结构、生命周期与链校验，返回重算 digest）；未改 schema、既有 API 或 `jessica-core`。`jessica-cli`：新增 `trace_bundle` 模块与 `trace export --turn|--all --out [--allow-open]`、`trace verify-bundle --bundle [--json] [--allow-open] [--expect]`；export 只读打开并逐 turn 重验，invalid 拒绝、open 仅 `--all --allow-open` 允许、拒绝不写文件，evidence 含精确 trace entry 与 mirror 消费的 legacy 行，同库重复导出字节一致；verify-bundle 不读 `[events]`、不打开/创建/迁移事件库，读前强制 64 MiB 上限，校验 version/format/规范 digest/重复 turn，离线重建并比较逐 turn digest、aggregate 与 `--expect`，`--replay` 显式拒绝；`trace_verify` 暴露复用内部项并新增 `exported_legacy_rows`，参数解析/usage/结构化错误/README 同步。测试新增 19 个：trace_chain 聚合 golden 单测 1、`tests/trace_evidence.rs` 6、CLI 解析 3、`tests/trace_bundle_e2e.rs` 9（无 `[events]` 离线往返、确定性/排序、open 两种路径、invalid 不落盘、trace/legacy/aggregate 篡改、turn digest 与 expect、畸形/超限 bundle、空库、缺失库不创建）。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 910 passed, 0 failed, 2 ignored（58 suites）。偏离/说明：1) 离线校验复用内存 EventStore + `trace_turn`，与在线 verify 同源；2) 聚合只覆盖 turn digest，legacy 通过 mirror 校验绑定；3) 只导出正面证据，invalid trace 与遗漏 turn 不可作为失败/省略证据，除非外部锚定；4) 不支持 `verify-bundle --replay`；5) 不是签名，必须用 `--expect` 锚定库外记录；6) 既有 `trace digest --all` 输出不变；7) 导出使用 `std::fs::write`，未做临时文件 + fsync 原子替换；8) 未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.5 ☑ 耗时未精确计时（跨上下文执行；含方案确认、实现、格式化与全量验收） — 交付 `--all --replay` 与 v1/Resumed 覆盖扩展。`jessica-runtime`：新增 additive `SessionCheckpoint::from_recorded_state(session_id, messages, rounds_completed)`（`last_seq=0`、无终态），供 trace 只记录 checkpoint 状态时重建 resume 起点。`jessica-cli`：`replay` 抽出 fresh/resumed 共用路径，新增 `replay_resumed_session`（真实 `AgentLoop::resume`、总预算 `rounds_completed + 脚本轮数`）与 no-`SessionStarted` 的 resumed script 模式；`trace_replay` 接受 v1/v2，fresh v1 用 `turn_started.messages` bootstrap、resumed 用首 `RoundStarted.round - 1` 推导已完成轮数并禁止 `SessionStarted`，无轮 resumed 与 v1/resumed 多 attempt 显式 unsupported；`trace_verify` 新增逐 turn 加载/验证/重放的批量路径（单 trace 内存有界）、`TraceReplayUnsupportedBatch`、拒绝 `--allow-open --replay`，`report_version` 保持 3；解析器/usage/README 同步。测试新增/更新 18 个：runtime checkpoint 1、replay 2、trace_replay 3、CLI 解析 2、trace_verify E2E 8、真实 resume E2E 1，以及既有 v1/batch 期望更新。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 918 passed, 0 failed, 2 ignored（58 suites）。偏离/说明：1) resumed 原始 `Budget.max_rounds` 未入 trace，以 `rounds_completed + 脚本轮数` 复现；2) `--all --replay` 顺序执行并逐 turn 重开只读库；3) v1/resumed 均仅单 attempt；4) 无模型轮 resumed 与批量外部新增 turn 等已记入 Deferred；5) 未改 `jessica-core`、schema、既有 `replay --session` 或 bundle replay；6) 未提交、未推送，`DESIGN.md` 保持未跟踪。

Task 8.6 ☑ 耗时未精确计时（跨上下文执行；含方案确认、实现、格式化与全量验收） — 交付子 Agent 轨迹证据。`jessica-core`（经批准）：`TRACE_SCHEMA_VERSION` 2→3，新增 `TracePayload::ChildAgent { call_id, event }`，支持 1..=3，v2 reader 拒绝 v3，payload 永远 trace-only。`jessica-runtime`：新增 `ChildEventSink` 与 `with_child_event_sink`，子事件先入 child sink，始终包装 `CountingChildSink`，timeout 父终态使用已观测计数，sink 失败按 Cancelled 中止。`jessica-cli`：journal 实现 child sink，强制父 start → 子 start → 子轨迹 → 子 end（timeout 可缺）→ 父 end，拒绝无括号/重复/越界/parent-only/缺失终态/过期 attempt；verifier 在 turn 含 `ChildAgent` 时严格校验 bootstrap、delegate 链接、轮次与工具状态机、终态和计数（timeout 前缀、工具限额 +1），生命周期-only trace 保持 v2 规则；replay 接受 v1–v3 并忽略子证据；README 记录 v3、replay 不执行子 Agent、digest/bundle 覆盖与隐私留存提示。`jessica-memory`：`payload_kind=child_agent`、`event_type` 存内部 tag，拒绝缺 attempt/session、空 call_id、嵌套 lifecycle 与 legacy mirror，链/离线导出同步。测试新增/更新 27 个（core 1、runtime 3、journal 11、memory 2、verifier 8、replay 1、真实 delegation E2E 1，含 export/verify-bundle 与子轨迹篡改）。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 945 passed, 0 failed, 2 ignored（58 suites）。偏离/说明：1) child sink 拒绝后 transcript 计数可大于已接受证据，journal 失败且不定义部分证据验证；2) 严格校验以含 `ChildAgent` 为开关；3) replay 不执行子 Agent，不验证子 provider/工具可复现性；4) 不支持递归委派；5) 保留/隐私删除留给 8.7；6) 未改 SQLite schema/bundle 格式，未新增第三方 crate，未提交/未推送，`DESIGN.md` 保持未跟踪。

Task 9.0 ☑ 耗时未精确计时（含方案确认、实现、测试与全量验收） — 交付 profile preset、effective config 与默认 trace。`jessica-cli`：新增 `ProfilePreset`（`safe`/`dev`/`ci`/`lab`）、`TraceMode`（`strict`/`off`）与 `ProfileConfig`；`HarnessConfig.profile` 默认 `dev`+`strict`；`from_toml_str` 改用 raw `toml::Table` 合并，显式字段 > preset > 内建默认，preset 只补 `policy.allowed_tools`，不自动启用 memory/skills/reliability；`EventsConfig::default().path = ".jessica/events.db"`，`trace = "off"` 清除隐式路径但拒绝显式 `events.path`；`validate` 强制 strict 需要路径、off 禁止路径、ACP `state_dir` 需要 trace；`profile` 加入不可热更新 section；配置类型派生 `Serialize` 并新增 `to_effective_json`；新增无副作用 `jessica config` 命令与 `parse_args_from`；`EventRecorder::open` 创建缺失父目录并以 `CreateParent` fail-closed。测试覆盖默认值、preset/显式覆盖、safe allowlist、trace off/冲突、非法 preset、effective JSON、真实 CLI config 输出与无副作用、父目录创建、热加载拒绝。README、`.gitignore`（`/.jessica/`）与 PLAN 同步；未改 `install.sh`，因为它已显式生成 `[events].path`。测试净增 10 个（945→955）：配置默认/preset/覆盖/trace off/非法值/effective JSON、`jessica config` 真实 E2E 2 个、EventRecorder 父目录创建与路径阻塞 2 个、`config` 参数解析 1 个；另更新 ACP 依赖默认 trace、热加载不可变 section 与受影响 E2E。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 955 passed, 0 failed, 2 ignored。偏离/说明：1) 计划中的“effective config 和 preset 名写入 trace”未实现，冻结的 core trace payload 无配置承载变体，复用既有 payload 会污染审计语义，已记入 Deferred 待单独批准；2) preset 当前只影响工具 allowlist，memory/skills/reliability 始终 opt-in；3) 默认事件库位于 workspace root 的 `.jessica/events.db`，`install.sh` 仍显式写入 `$JESSICA_HOME/events.db`；4) 未修改 `jessica-core`、未新增第三方 crate、未改 SQLite schema、未提交/未推送，`DESIGN.md` 保持未跟踪。

Task 9.1 ☑ 耗时未精确计时（含方案确认、实现、测试与全量验收） — 交付 Tool Catalog、descriptor、per-tool limits、schema 过滤与 `tools list/inspect`。`jessica-cli`：新增 `tools/{descriptor,catalog,mod}.rs`；descriptor 由宿主按 source 赋值 capabilities/approval/version/policy hints/result format，schema digest 为递归排序键后的 compact JSON `sha256:<64 hex>`；Catalog 对 native、配置 WASM、session MCP extra tools、启用委派的 `delegate_task` 与 `policy.allowed_tools`、`tools.profile` 取交集，未暴露项保留隐藏原因但不注册；重复名、预算超限 fail-closed。新增 `[tools]`（`profile` 默认 `full`、`max_exposed_tools` 64、`max_tool_schema_bytes` 131072、三个原生工具的 `timeout_ms`/`max_args_bytes`）与字段级校验（≥1、per-tool ≤ 全局、bash ≤ 内部 10s）；`[tools]` 每 turn 重建，可热加载。`GovernedTool` 新增 `max_args_bytes`，在预算检查后、policy/permission/执行前按序列化参数长度拒绝。`build_turn` 只注册 exposed entries，per-tool timeout/approval 从 descriptor 装配；委派 `ParentCapabilities` 改用 Catalog 实际暴露的 native 名称。新增无副作用 `jessica tools list [--all]` / `inspect <name>`，稳定排序、隐藏原因、descriptor、digest 与 schema；预算超限时 list 只警告仍可诊断。`jessica-tools` 公开 `DEFAULT_BASH_TIMEOUT`；`jessica-cli` 显式引用 workspace 已有 `sha2`。隐藏工具语义由“进入 schema 后被 policy 拒绝”改为“不进入 schema、调用得到 unknown tool”，同步更新 ACP/WASM E2E、examples 注释与 README。测试净增 28 个（955→983）：descriptor 4、catalog 7、tools 模块 2、配置 4、governance 2、app 6、真实 CLI tools E2E 3。验收：`cargo fmt --check` 通过；`cargo clippy --workspace --all-targets -- -D warnings` 通过；`cargo test --workspace` 983 passed, 0 failed, 2 ignored。偏离/说明：1) profile 默认采用 `full` 而非设计示例的 `coding`，避免既有未配置行为收窄；2) policy hints 按当前实际参数适配为 path/command 字段，`protected_paths` 留空待策略 v2；3) descriptor 的 `max_inline_bytes` 复用 `budget.max_result_bytes`，`max_artifact_bytes`/`max_output_lines` 尚无执行语义，报告为不存在；4) 未修改 `jessica-core`、未新增第三方 crate、未改 SQLite schema、未提交/未推送，`DESIGN.md` 保持未跟踪。

---

Deferred（待办，由用户决定是否单开 Task）

· Phase 11（E0、E1a–E1d、E2、E3a–E3b 与 E4 已完成；E1 剩余 CLI catalog 过滤与 artifact sidecar/`ReadArtifactTool` 归属、E5 候选，未排期）：`jessica-engine` 受治理运行时抽取。2026-11-28 用户确认方向并要求作为候选写入 `PLAN.md`，随后完成 E0 行为基线；E1 获授权后按计划拆分，E1a 已交付 crate/API/端口/配置骨架，E1b 已交付 engine 侧 `PolicyV2`/`ArtifactStore`/`ResultManager`/`GovernanceState`/`GovernedTool` 与最小 `run_turn` 垂直切片，E1c 已交付 `EventSink` 兼容面、`ToolPolicy`/`ToolResultManager` 端口与 CLI 治理适配层，E1d 已交付 descriptor 元数据迁移、CLI/engine patch 解析等价性测试与 engine artifact retention/GC；E2 已交付 journal/pump/EventRecorder 迁入、`TraceStore` 与 `TracePolicy::Required`；E3 按 E3a/E3b 拆分，E3a 交付 engine 侧 `run_turn_request`/`resume_turn`、acceptance、memory/skills 后处理与固定初始消息顺序，E3b 交付 delegation、CLI 委托与 config 拆分；E4 交付 CLI/ACP 单一装配，删除 `events`/`pump`/`replay` 垫片并把离线 replay 迁入 engine。CLI 已依赖 engine 并复用 engine 的 `GovernanceState`/`TokenCountingProvider`/`GovernedTool` 顺序，但仍保有 policy/artifact/catalog 具体实现（`policy_v2.rs` 仅供 `policy simulate` dry-run），E1 未完成。E5 不表示已排期或已开工。详见 Phase 11 章；engine 现为 CLI/ACP 唯一受治理 turn 路径。
· Phase 10 Web Search（2026-11-22 用户决定暂缓）：当前没有 Brave Search API key，10.1a/10.1b 均未开始。暂缓期间不得向 `jessica-tools` 增加 `reqwest`/`tracing` 引用，不得新增 `web_search` 工具、配置、审批项、测试或文档承诺。恢复条件：用户提供可用 Brave API key，或明确选择 SearXNG endpoint/自托管方案，并重新确认后端顺序、审批与父 Agent 隔离边界；恢复后仍按 Phase 10 的两个 Task 拆分验收。
· Task 9.1 的系统提示仍展示完整 `policy.allowed_tools`，而不是 Catalog 的 profile 过滤后暴露集。窄 profile 下模型可能在提示中看到未暴露名称并尝试调用，随后得到 `unknown tool`。若要求提示与 schema 严格一致，需把按 turn 构建的 exposed 集传入 `initial_messages`，并决定 WASM/MCP/委派名称在提示中的呈现方式；不在 9.1 顺手改。
· Task 9.1 的 per-tool `timeout_ms`/`max_args_bytes` 只覆盖 `read_file`、`write_file`、`bash` 三个原生工具；WASM manifest、MCP server 与 `delegate_task` 尚无独立 per-tool 配置，仍分别受 manifest 限额、MCP 固定边界和委派 limits 约束。若需要统一配置面，需单开 Task 扩展 `[tools]` 并定义第三方工具的默认来源。
· Task 9.1 的 descriptor `version` 对 WASM 固定为 `manifest-v1`、对 MCP 固定为协议日期，未读取 manifest 或 server 自报版本；`schema_bytes` 只统计 host 侧 canonical schema，不含 provider 可能追加的包装。若需要版本审计或 provider 级字节预算，需定义来源契约。
· Task 9.1 的 `tools list/inspect` 会编译配置中的 WASM manifest（与 turn 一致），manifest 损坏时命令失败；这与 `jessica config` 完全不触碰 wasm 的行为不同，属于有意取舍，但尚无绕过编译的纯元数据检查模式。

· Task 9.0 未把 effective config 或 preset 名写入 trace。冻结的 `jessica-core` `TracePayload` 没有承载配置快照的变体；用 `TurnStarted.messages`、普通 `AgentEvent`、`Gap` 或 `ChildAgent` 夹带配置会破坏审计语义、replay parity 与 bundle/digest 契约。若需要，须单开 Task 并明确批准 core 公共 API/schema 变更：新增配置 payload 或包络字段、版本号、旧 reader 拒绝规则、verifier/digest/export/bundle 兼容性、敏感配置脱敏与保留策略；不得在 CLI 侧静默编码。

· Task 9.0 的默认 trace 把事件库放在 workspace root 的 `.jessica/events.db`；仓库根运行示例时会把完整会话写入仓库内目录（已加入 `.gitignore`，但不等于隐私保护）。该库、ACP 索引与导出 bundle 仍无保留期、字段脱敏、文件权限收紧或物理删除。若需要生产级默认安全，须单开 Task 定义 0600/0700 权限、umask 行为、保留期/GC、隐私删除与 append-only 审计契约的取舍；不得在 9.x 顺手修改 schema/删除路径。

· Task 9.0 的 `jessica config` 输出未带 schema/version，字段集合由 `Serialize` 派生，适合人工审计而非稳定机器契约；preset 目前只补 `policy.allowed_tools`，`ci`/`lab` 与 `dev` 的工具集相同。若需要 CI 消费该 JSON 或让 preset 覆盖更多默认（预算、超时、memory/skills 安全上限），须单开 Task 定义版本化 schema、兼容策略、preset 变更影响面与“preset 不自动启用副作用子系统”的边界。

· Task 9.0 的默认事件库路径相对启动 workspace root；`install.sh` 生成配置时仍显式写 `$JESSICA_HOME/events.db`，Paseo 启动的 ACP 因此不受默认路径影响。其他调用方若依赖“未配置 `[events]` 即无持久化”，必须显式设置 `profile.trace = "off"`；后续若改变默认路径或迁移到平台数据目录，须单开 Task 处理兼容与数据搬迁。

· Trace Integrity v2（2026-11-17 规划）的包络与存储部分已由 Task 7.1（2026-11-18）交付，生产者侧无损记录由 Task 7.2（2026-11-18）交付：新增 additive `TraceEvent` 包络、`trace_events`/`trace_heads`、事务内 legacy+trace 双写、fail-closed、acceptance 事件归属与 `Resumed` 轨迹，未修改 `AgentEvent`；`EventSink` + `TurnJournal` 保证成功 turn 的全部生产事件与 `TurnEnded` 在返回前提交，pump 不再写 trace，`Gap` 变体保留但 CLI 不再产生。事件模型 v2 已由 Task 7.3（2026-11-19）交付：`SessionStarted.messages`、`RoundStarted`/`RoundFinished`（含 usage）、`PermissionDecided`、`ChildSessionStarted`/`ChildSessionEnded`，`TRACE_SCHEMA_VERSION=2` 且 v1 可读；permission/child 生命周期仅写 trace（不镜像 legacy），子会话内部轨迹当时未持久化，后由 Task 8.6 以 v3 嵌套证据补齐。trace verify CLI 已由 Task 7.4（2026-11-19）交付：`jessica trace verify --turn/--all` 以只读方式打开事件库，校验结构、v2 语义和 trace↔legacy 镜像，支持稳定 JSON 报告与 `--allow-open`；默认不执行 replay。单 turn 执行一致性已由 Task 7.5（2026-11-19）交付：`trace verify --turn ... --replay` 用 trace 自身的 `SessionStarted.messages` 与录制事件重建每个 fresh attempt，驱动真实 `AgentLoop` 并逐事件比较；仅覆盖 closed v2 fresh、Completed/Error/MaxRounds，trace-only 与 CLI governance 事件排除，其他轨迹显式 unsupported。子 Agent 内部轨迹已由 Task 8.6（2026-11-19）交付为 v3 嵌套证据。仍暂缓/未实现：trace 保留/GC 与隐私删除（8.7 候选）、fsync/掉电级持久性、v1→v2 混合 legacy 流 replay（见下方 replay 条目）、子 Agent replay 与子 provider 调用/工具结果可复现性（8.6 只记录并验证嵌套证据，不重放子执行）、跨 turn 聚合摘要之外的签名/TSA；replay/resume 仍只读 legacy 事件流，trace 不是 recovery 的唯一来源。上述各项须单开 Task。
· Task 8.6 的子轨迹严格校验只在 turn 含 `ChildAgent` 时启用；纯生命周期 v3 trace（旧生产者或手工证据）仍按 v2 规则通过。若要强制所有 v3 生产者必须携带子轨迹，需升级版本策略或新增显式模式，不能把缺失证据当作合规。
· Task 8.6 的 `--replay` 仍忽略 `ChildAgent`，不执行子 Agent，因此只证明父事件流可复现，不证明子 provider 请求、子工具结果或子 timeout 可复现。若要验证子执行一致性，需单开 Task 定义子 replay 的脚本、预算与 timeout 语义。
· Task 8.6 不记录子 Agent 的独立预算/超时配置，父 `ChildSessionEnded` 只有 reason 与计数；无法从 trace 审计实际限额值。若需要，须扩展 trace schema 并保持 v1–v3 兼容。
· Phase 7 后续 P2 候选（2026-11-17 规划，暂缓，不属于当前 Phase 7 范围）：执行安全。bash/CommandSensor/CommandAcceptanceCheck/MCP 子进程的进程组终止与超时；文件工具 TOCTOU（openat2/cap-std 等需依赖审批）；Permission v2（allow_always/reject_always、作用域/撤销/审计）；WASI 确定性时钟/随机；密钥脱敏。
· Phase 7 后续 P3 候选（2026-11-17 规划，暂缓，不属于当前 Phase 7 范围）：能力扩展。edit_file/apply_patch/grep/glob/list_dir；Anthropic prompt caching/thinking/image；MCP resume/HTTP-SSE、per-session workspace；memory/skills 显式写入与真实 embedder。
· 可观测性仍按 2026-11-14 的决定暂缓，不因本次 Phase 7 规划重启。
· 说明：以上三块只做优先级归组；与下方既有 Deferred 条目重复之处，以既有条目为准，且各项仍须单开 Task。

· （已由 Task 8.1 于 2026-11-19 修复；main CI 需在提交/PR 后确认全绿）Task 7.0a 记录的 `shutdown_e2e::sigint_on_an_idle_repl_exits_130_without_creating_a_turn` 间歇失败（run 44302009658：退出码 130 正确但 stderr 缺诊断行；run 44431198725 同因）。根因是 `on_signal` 先记录 signal/取消 root、后写 `eprintln!`，`main` 可在诊断写出前退出；现已改为先写并 flush 诊断、后提交状态，并新增 20 轮真实进程压力测试防回归。

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
· 恢复所需的初始 user/system messages 现由 `SessionStarted.messages` 事件承载（Task 7.3），`sessions.messages_json` 作为 v1 兼容回退并在 7.3 继续双写；两者冲突时 fail-closed。若未来要停止双写并删除侧表，需单开 Task 定义旧库迁移、只读回退路径与回滚策略。
· 事件流现已包含显式 round 边界（Task 7.3 的 `RoundStarted`/`RoundFinished`），`recover_session` 优先使用显式轮次；v1 事件流仍按启发式计数，连续两个都不产生文本、只请求工具的模型回合在旧流中不可区分，因此 v1 恢复的 `rounds_completed` 可能低估（resume 可能多获得一次模型调用），不会高估。删除 v1 启发式前必须先定义旧库兼容策略（单开 Task）。
· `--resume` 目前不允许与 prompt 组合；已终结的 session 只能读取记录 reason，不能据此开启新的用户回合。若需要“恢复后继续对话”，应以新 session + 旧 messages 重新开始并单独定义语义（单开 Task）。
· 恢复路径直接调用 `AgentLoop::resume`，不经过 CLI 的 acceptance 重试编排；中断会话恢复完成后的验收检查语义尚未定义，需单开 Task。
· EventPump 只在存在 current session 时持久化事件；AcceptanceRunner 在两个 attempt 之间发出的 AcceptanceFailed 没有归属 session，因此不会写入事件库。若要求完整审计，需要为 turn 级事件建立 outer session 归属或显式 session 关联（单开 Task）。
· 恢复流的事件持久化依赖 EventPump 的 broadcast 接收；通道 lag 或丢事件时，恢复所需的尾部事件也会缺失（与 transcript 风险相同）。若要强一致，需同步持久化 hook 或改事件总线。
· 恢复时 `rounds_completed` 只来自事件流；若原 session 的 max_rounds 很小且末尾批次已用尽预算，resume 会正确发出 BudgetExceeded + SessionEnded；但若 checkpoint 之后用户修改了配置（例如增大 max_rounds），恢复会使用新配置，不保证与原计划一致。
· replay 只重建 AgentLoop 层的事件流：CLI 治理发出的 PolicyDenied、AcceptanceFailed 以及 token/result-size 导致的 BudgetExceeded 不会重新生成；它们对循环的影响已经体现在记录的 ToolResult 与终止原因中。若需要完整 turn 级治理重放，必须重新设计 CLI 装配并解决命令 sensor/acceptance 的外部依赖（单开 Task）。
· 技能任务队列（5.2.2b）当前采用方案 B：job 状态只存 skills.db、不在事件流；resume 的入队幂等依赖 skills.db 唯一索引，事件流无法恢复 job 状态。若未来要求 resume 完全由事件流确定性决定，需单开 Task 迁移到方案 A：在 core 新增 SkillJobEnqueued/SkillJobCompleted 及 payload、实现 rebuild_from_events 物化、resume 改读事件流/物化视图、replay 跳过物化；core 变更必须单独 review。
· `LlmEvent::Done` 的 TokenUsage 已由 Task 7.3 的 `RoundFinished.usage` 记录（provider 未报告时为 `None`）；replay 对显式 usage 原样输出，对 v1 流合成 `Done{usage: None}`。若 Provider 不报告 usage，token 计费仍无法重放或校验；需要在 Provider 契约层强制或补算（单开 Task）。
· replay 要求事件流以 SessionEnded 结尾，只支持 Completed、Error 与 max-rounds BudgetExceeded；Cancelled、AcceptanceFailed 以及中断的流会被拒绝（中断恢复用 --resume）。若需要重放中断会话，应先定义“无终态时如何合成终态”。
· v1/v2 混合 legacy 流 replay 尚不支持：v1 会话被 v2 build `--resume` 后，事件流前半段是无显式 round 的 v1 词汇，后半段从首次 `RoundStarted` 起是 v2 词汇；replay 检测到同一流内 round 词汇切换会拒绝，而不是猜测边界。若需要支持，必须单开 Task 定义混合流的 round 归属、v1 前缀的 usage 缺失输出与逐字节复现规则；不得仅按“首个 RoundStarted”切换而破坏现有纯 v1/v2 流的精确 replay。
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
· 真实 API key 联调仍待进行：Task 7.0c 已把示例 acceptance 夹具移到 `examples/typo.md` 并直接验证“首次失败→修正后通过”，但尚未用真实模型跑完整 governed demo。
· Task 5.2.4 的 `SkillJob` 直接携带 `DistillSource`，而 5.2.2b 的 `skill_jobs.source_json` 保存的是已转义/已预算的内容字段。5.2.6 的 CLI 适配器需要一条从 `source_json` 重建 `DistillSource` 的路径（例如新增 `DistillSource::from_persisted` 或等价构造函数），且不得再次执行 `escape_untrusted` 或预算截断，否则会二次转义并破坏 `to_untrusted_block` 的字节稳定性；该 API 若加入 5.2.3 模块应由 5.2.6 显式实现并测试。
· 子 Agent 轨迹目前不持久化（5.3 的 v1 折中）：父事件流只有 `delegate_task` 的调用与结果，子会话的模型增量、工具调用与终止原因均丢弃，因此子轨迹不可 replay、不可审计、也无法从 checkpoint 恢复。若要保存子会话，需新增子 EventStore、父子 session 关联（含父/子 trace 关联；当前 `child_session_hash` 只由 task + narrowed tools 派生，不含父 session id）以及 replay/resume 的边界规则；5.3.6 候选，必须先定义事件模型与存储 schema，不得把子事件混入父事件流。
· 5.3 v1 只支持单层委派；嵌套委派被 `narrow_tools` 结构性禁止。若未来允许嵌套，需单开 Task 定义深度上限、权限链传递（每层只能收窄）、父 token/工具调用预算切分、循环检测、子会话 EventStore 与失败语义；不得仅删除 `delegate_task` 过滤就放开。
· 5.3 v1 子 Agent 不加载 WASM 工具；`narrow_tools` 与 CLI factory 双重排除。未来若允许子 WASM 实例，需单开 Task 验证每子实例独立 guest 状态/fuel/epoch、模块重复加载的性能与失败隔离、以及不得复用带 Mutex 的 `WasmTool`；当前共享实例会破坏“子 Agent 间不共享可变状态”。
· 5.3 将子工具超时固定为 5000ms，未提供配置项。若未来需要独立配置，需单开 Task 增加 `delegation.child_tool_timeout_ms`，并在配置加载期验证 `child_tool_timeout_ms < delegation.timeout_ms < budget.tool_timeout_ms`，同时保留默认值不变；不得在运行期按模型输入动态调整。
· 5.4.5 的优雅关闭通过取消当前 turn 产生 `SessionEnded(Cancelled)`，该终态在当前恢复语义下不可 `--resume`；第二次信号强制退出会放弃 skill drain。若需要“可恢复关闭”（先持久化 checkpoint 并以非终态结束，后续 `--resume` 继续），需单开 Task 定义 checkpoint 事件、Cancelled 的恢复语义与 drain 中断后的 job 恢复规则，不能在 shutdown 路径顺手修改恢复语义。SIGHUP 等其他信号仍未处理；需要时单开 Task。
· 5.4.5 只处理 SIGINT/SIGTERM，且信号监听仅在 CLI Turn/REPL 路径安装；`skills` 子命令、replay 等短路径不安装监听，进程按默认信号处置退出。若这些路径也需要优雅关闭，需单开 Task 统一入口。
· Task 5.5 可观测性（OTLP 导出、会话回放可视化）暂缓：2026-11-14 用户决定暂时放弃设计/实现。未新增依赖、未修改 jessica-core、未落盘设计文档。若重启，须先 review OTel 依赖方案（官方 SDK vs 手写 OTLP/JSON）、遥测脱敏边界与查看器只读 API，并单开 Task；不得在 5.1–5.4 或收尾工作中顺手实现。
· ACP-5 已补齐 stdio MCP（`session/new` 启动、initialize/tools/list/tools/call、namespaced 工具经 Policy/Guard、close/delete/process shutdown 回收子进程、env 脱敏）。HTTP/SSE MCP 仍返回 `-32602`，`session/load`/`resume` 对非空 `mcpServers` 仍拒绝；若需 HTTP/SSE transport、凭据代理或恢复时重建 MCP 配置，须单开 Task 定义客户端、凭据管理与失败语义，不得在 ACP-5/7 上顺手扩展。
· ACP-5 的 MCP 上限固定（每 session 8 servers/256 tools、4 MiB line、10s initialize/discovery、60s 内部 tools/call 安全上限），没有 `[acp]` 专属配置项；若需可配置，须单开 Task 并校验上限、`budget.tool_timeout_ms` 与工具安全上限的关系。验收仅使用 `/usr/bin/python3` stub fixture，未覆盖真实第三方 MCP server 或 Paseo 手工 MCP 会话；若需兼容性验收，须单开 Task。
· ACP 权限请求已由 ACP-6（2026-11-16）实现：敏感固定集 `bash`/`write_file`/`delegate_task`，一次性 `allow_once`/`reject_once`，客户端 `cancelled`/固定 5 分钟超时/transport 错误全部 fail-closed，真实 provider `toolCallId`（ACP-6a 的 `Tool::execute_with_call`）。剩余限制：只支持一次性批准（无 allow_always/reject_always、无跨 turn 持久化）；`read_file`、WASM 与 MCP 工具不询问；超时只失败当前工具而不结束 turn；`delegate_task` 子工具不单独询问。若需要持久批准、逐子工具授权或可配置超时，须单开 Task 定义批准存储与撤销语义；不得用空/合成 toolCallId。
· ACP-7b 已实现 `session/list`/`delete`/`resume`/`close`。delete 只删除 ACP 索引并释放 live session，事件库保持 append-only，不提供物理删除或空间回收。若需要隐私删除/GC，必须单开 Task 设计 tombstone、事件引用与跨会话恢复语义，不得直接修改或删除 append-only 事件行。
· ACP-7b 的 close/delete 取消 active prompt 后最多等待 5 秒；provider/tool 不响应取消时仍会确认，worker 可能继续运行到自身退出。若需要强制终止/资源回收保证，须单开 Task 定义进程/工具级截止与状态回收。
· ACP-7b 的 `session/list` 会列出所有有效索引，包括尚无 committed turn 的空会话；Paseo catalog probe 会创建此类索引，Paseo import preview 对 load 失败按“可见但无 preview”处理。若需隐藏空会话，必须先定义“可恢复”过滤与 live 空会话的 list 语义，并单开 Task。
· ACP-7b 的 load/resume 与 delete/close 之间没有全局请求串行化；`AcpPersistence.mutation` 只串行化 create/record/delete 对索引的写。同一 session id 上并发互斥生命周期请求的最终状态由调度决定；若需线性一致性，须单开 Task 设计 per-session 状态机或锁。
· ACP-7b 的 `session/list` 是目录扫描快照，不是跨并发写入的原子快照；基于 `(updatedAt, sessionId)` 的 keyset 游标在跨页期间新增/删除时会产生正常漂移。若需要稳定快照，须单开 Task 定义快照标识、生命周期与过期策略。
· ACP-7a 之前创建的会话没有索引，无法恢复；索引也不兼容未来 schema 变更（当前 version=1，遇到不支持的版本 fail-closed 为 `-32603`）。若需要迁移旧会话，必须单开 Task 定义扫描/匹配规则与隐私边界。
· ACP-7a 的索引更新是每 session 的读-改-原子写，没有跨进程文件锁；`[acp].state_dir` 只应供一个 ACP 进程使用。若需要多进程共享，须单开 Task 设计锁/冲突策略。
· ACP-7a 的历史重建只从最新可恢复 turn 开始；若最新事件流损坏，会回退并丢失更晚已提交的 turn。若需要逐 turn 合并恢复所有历史，须单开 Task 处理 turn 边界、重复 user 消息与 tool pair 跨 turn 重组。
· ACP-7a 的索引追加发生在 prompt update barrier 之后、response 之前，但事件库写入与索引写入不是跨存储事务；崩溃可能造成索引与事件流短暂不一致（load 通过回退/失败处理，不静默返回错误对话）。若需要严格原子性，须单开 Task 设计两阶段提交或从事件库推导索引。
· Phase 6 的 ACP session `cwd` 必须位于启动 workspace root 内；memory/recorder/skills/wasm 仍按启动 root 作用域，因此不支持一个进程服务多个 workspace。若需 per-session workspace 服务，须单开 Task。
· Phase 6 的 ACP 模式禁用 acceptance 重试（每 prompt 单次 attempt）；与 REPL/one-shot 的 acceptance 行为不同。启用重试前必须先定义 v1 客户端对 `messageId`/notice 的兼容策略。
· Phase 6 每个 prompt 都把完整历史写入事件库，存储随对话增长呈 O(n²)；后续可改为增量 + 前序 turn 引用，但不得改变 replay 语义而不单开 Task。
· Phase 6 ACP 路径必须把 EventPump broadcast `Lagged` 当内部错误处理；现有 CLI 的“只 warn 并继续”行为不得用于 ACP。若要彻底消除 lag，需另开 Task 改同步持久化/背压设计。
· Phase 6 只支持 ACP stable v1，不支持 v2 alpha；真实客户端互通（至少 initialize → new → 两轮 prompt → cancel）需要人工验收，至少覆盖 Paseo，不能仅凭单元测试或脚本化客户端宣称支持。
· Phase 6 的 `[acp]` 四项限制在进程启动时读取并冻结；热加载修改 `[acp]` 会整份拒绝（stderr 报 `reload rejected: 'acp'`），必须重启 ACP 进程才能改变 session/prompt/history 上限。若需在线调整限制，须先定义已存在 session 的迁移/裁剪语义，单开 Task。
· Phase 6 的 ACP ConfigWatcher 随首次 prompt 的惰性 runtime 装配启动（`assemble_runtime` 成功后才 spawn）；进程启动到首次 prompt 之间的配置文件修改不会触发通知，首次 prompt 使用启动快照，之后需再次修改文件才会被观察。若要求从 `initialize` 起就监听，需把 watcher 与 runtime 装配解耦并定义首次 prompt 取哪一份快照，单开 Task。
· Paseo 0.15.x 的全局 provider catalog 探测会用 `cwd = $HOME` 调用 `session/new`；Jessica 的 cwd 又被限制在 ACP 进程启动 root 内，因此当 Paseo daemon 从项目目录启动且 workspace 在 `$HOME` 之外时，provider 目录刷新会报 `cwd must be inside the startup workspace root`。当前通过从 `$HOME` 启动 daemon（workspace 位于 `$HOME` 下）或 wrapper `cd` 到共同祖先规避，并已记录到 README；若要在任意 daemon cwd 下支持，需要单开 Task 定义可配置的额外允许根或 Paseo 专用 catalog 处理，不能在 ACP-4 放宽 cwd containment。
· 可执行文件名仍为 `jessica-cli`（需要 `cargo run -p jessica-cli -- acp`，安装后命令名为 `jessica-cli`），而 Phase 6 Task 文案写的是 `jessica acp`。改名（如设置 `[[bin]] name = "jessica"`）会影响现有脚本、文档与发布产物，需单开 Task 统一命名并更新全部调用点，不得在 ACP Task 中顺手改动。
· Task 8.3 的 `trace digest --all` 与 `trace verify --all` 仍按 `trace_heads` 枚举 turn；`trace_events` 中可能存在但未被任何 head 索引的孤儿行不会进入审计或 digest，其链缺失也不可见。Task 8.4 的 `trace export --all` 复用同一枚举来源，因此也不会导出孤儿行。若要覆盖全库一致性，须单开 Task 定义孤儿行扫描、归属与修复语义（只读审计不得自动修复），并明确迁移/损坏边界；不得在 8.4+ 顺手改变 `--all` 的枚举来源。
· Task 8.4 的 `trace verify-bundle` 明确拒绝 `--replay`，bundle 本身不携带 provider/工具环境与回放所需的完整外部状态。若要离线重放，须先定义可重放的 bundle 子集、环境声明、结果 schema 与 mismatch 报告，并证明不会执行未受信内容；单开 Task。
· Task 8.4 的 bundle 不是签名或时间戳凭证；能重写整个 bundle 者可重算逐 turn 与聚合 digest。若需证明“由某方在某时生成”，须单开 Task 引入签名（密钥管理、算法与依赖审批）或 RFC 3161 TSA，不得在现有格式中静默扩展。
· Task 8.4 只导出正面证据：invalid trace 不能导出，遗漏的 turn 本身不可证伪，`--expect` 只能锚定聚合而不能证明预期 turn 集合完整。若要支持负证据/省略证明，须先定义预期 turn 清单、服务端日志对照或累计 Merkle 根等语义，单开 Task。
· Task 8.4 的 bundle 含完整 trace payload 与 legacy 行，可能包含提示、模型输出、工具参数和路径等敏感数据；当前没有脱敏、字段裁剪或过期删除机制。隐私删除/保留期须与 append-only 契约一并通过单开 Task 设计，不能静默改写已导出 bundle。
· Task 8.4 的 export 使用一次性 `std::fs::write`，进程在写入中途崩溃或被强杀时可能留下截断文件（不会留下部分验证通过的 bundle，但需要重新导出）。若要求原子发布，须单开 Task 改为同目录临时文件 + fsync + rename，并定义跨文件系统行为。
· Task 8.4 未把 bundle 聚合暴露到数据库侧 `trace digest --all`；若需要 CI 直接对在线库输出聚合而不导出文件，须单开 Task 定义命令面与兼容性（不得改变既有 digest 文本/JSON 契约）。
· Task 8.5 的 v1 replay 只支持单 attempt：version-1 trace 没有逐 attempt 的 bootstrap 字段，多个 attempt 可能从不同会话开始，无法从单个 `turn_started.messages` 重建。若要支持，须先扩展 trace v2 或定义可验证的 attempt 级 bootstrap 记录，不能猜测。
· Task 8.5 的 resumed replay 只支持单 attempt：trace 只有一个 `turn_started` checkpoint 会话与首轮编号，多个 attempt 可能从不同 checkpoint 或不同预算恢复。若需支持，须单开 Task 定义每 attempt 的 checkpoint、rounds_completed 与预算来源。
· Task 8.5 的 resumed replay 要求首个事件是 `RoundStarted`；若 resumed 会话在产生任何模型轮之前结束，trace 无法区分 `rounds_completed` 的具体值。若要支持无轮 resumed trace，须在 trace 中显式记录 checkpoint 轮数或恢复边界，不能从缺失信息推断。
· Task 8.5 的 resumed replay 无法核对原始 `Budget.max_rounds`：trace 未记录该配置。当前以 `rounds_completed + 脚本轮数` 作为总预算，可复现 `Completed`/`Error`，并在 `BudgetExceeded` 时恰好还原消息中的原始总值；但无法证明未耗尽时的原始上限。若需严格核对，须扩展 trace schema 记录预算配置并单独迁移/兼容。
· Task 8.5 的 `--all --replay` 顺序执行并逐 turn 重新打开只读库；枚举后外部新增的 turn 不会进入本次运行，且大库的逐 turn 建连成本为 O(turns)。若要并发、单连接复用或稳定快照，须单开 Task 定义只读隔离、连接生命周期与错误顺序。
· Task 8.5 的批量 unsupported 错误只列出受影响的 turn id 与数量，具体原因在每 turn 的 JSON/text `replay.reason` 中；若需错误消息本身携带逐 turn 原因，须定义稳定的多行错误格式并考虑 CLI 机器消费兼容性。

---

与原始计划的差异说明

这份计划把整体架构拆成 17 个 Task，每个 Task 对应 Claude Code 的一次会话。项目名统一为 Jessica，crate 前缀统一为 jessica-。关键设计是：每个 Task 的验收命令都是可执行的，Claude Code 能自己确认是否完成；“禁止事项”明确列出，因为 Claude Code 倾向于过度发挥（提前实现、顺手重构、把测试写成自证式）；Task 0 的 CLAUDE.md 是跨会话的记忆载体，弥补单次上下文窗口的限制。Phase 2 的四个 Task 是整个计划的价值核心，如果资源有限，Phase 3-5 可以推迟，但 Phase 2 不可压缩。
