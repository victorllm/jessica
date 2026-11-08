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

Phase 1：裸循环（3 个 Task）

Task 1.1：OpenAI 兼容 Provider

操作：在 jessica-providers 中实现 OpenAiProvider，支持流式 SSE 解析。请求体构造、SSE 行解析、TextDelta/ToolCallRequested 的映射各拆成独立函数并单测。工具调用的流式增量拼接（多个 delta 拼成一个完整 ToolCall）必须有测试覆盖。

验收：

```bash
cargo test -p jessica-providers
# 提供一个 mock SSE 流的单元测试，不依赖真实网络
```

禁止：不要在 Provider 中实现重试/熔断（Phase 5 的事）；不要硬编码 API key，从环境变量读取。

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

· Task 5.1 分层记忆：短期/中期（FTS5）/长期（SQLite-vec 或 Qdrant），MemoryStore 的完整实现，Guide 注入真实记忆。
· Task 5.2 技能沉淀：复杂任务完成后异步提炼，写入技能库，后续通过 Guide 注入。触发条件必须是确定性规则（如工具调用次数 ≥ N），不得用 LLM 判断。
· Task 5.3 子 Agent 委派：delegate_task 工具，子 Agent 工具集只能收窄父级权限；子 Agent 间不共享可变状态。
· Task 5.4 生产化加固：provider 层重试/熔断、速率限制、配置热加载、优雅关闭。
· Task 5.5 可观测性：OTLP 导出、会话回放可视化工具。

每个 Task 的验收都要求：cargo test --workspace 全绿 + cargo clippy --workspace -- -D warnings + 一个端到端场景。

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
Task 1.1 ☐  
Task 1.2 ☐  
Task 1.3 ☐  
Task 2.1 ☐  
Task 2.2 ☐  
Task 2.3 ☐  
Task 2.4 ☐  
Task 3.1 ☐  
Task 3.2 ☐  
Task 3.3 ☐  
Task 4.1 ☐  
Task 4.2 ☐  
Task 4.3 ☐  

---

Deferred（待办，由用户决定是否单开 Task）

· （空）

---

与原始计划的差异说明

这份计划把整体架构拆成 17 个 Task，每个 Task 对应 Claude Code 的一次会话。项目名统一为 Jessica，crate 前缀统一为 jessica-。关键设计是：每个 Task 的验收命令都是可执行的，Claude Code 能自己确认是否完成；“禁止事项”明确列出，因为 Claude Code 倾向于过度发挥（提前实现、顺手重构、把测试写成自证式）；Task 0 的 CLAUDE.md 是跨会话的记忆载体，弥补单次上下文窗口的限制。Phase 2 的四个 Task 是整个计划的价值核心，如果资源有限，Phase 3-5 可以推迟，但 Phase 2 不可压缩。
