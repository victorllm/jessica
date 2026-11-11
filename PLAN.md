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
· Task 5.2 技能沉淀：复杂任务完成后异步提炼，写入技能库，后续通过 Guide 注入。触发条件必须是确定性规则（如工具调用次数 ≥ N），不得用 LLM 判断。
· Task 5.3 子 Agent 委派：delegate_task 工具，子 Agent 工具集只能收窄父级权限；子 Agent 间不共享可变状态。
· Task 5.4 生产化加固：provider 层重试/熔断、速率限制、配置热加载、优雅关闭。
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

---

与原始计划的差异说明

这份计划把整体架构拆成 17 个 Task，每个 Task 对应 Claude Code 的一次会话。项目名统一为 Jessica，crate 前缀统一为 jessica-。关键设计是：每个 Task 的验收命令都是可执行的，Claude Code 能自己确认是否完成；“禁止事项”明确列出，因为 Claude Code 倾向于过度发挥（提前实现、顺手重构、把测试写成自证式）；Task 0 的 CLAUDE.md 是跨会话的记忆载体，弥补单次上下文窗口的限制。Phase 2 的四个 Task 是整个计划的价值核心，如果资源有限，Phase 3-5 可以推迟，但 Phase 2 不可压缩。
