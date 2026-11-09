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
Task 1.1 ☑ 约 25 分钟 新增 reqwest（rustls-tls/json/stream）用于流式 HTTP（已在 workspace.dependencies 说明理由）；SSE 解码自行实现（字节级缓冲，处理跨 chunk 的 UTF-8/换行）；请求带 stream_options.include_usage 以填充 TokenUsage；环境变量 OPENAI_API_KEY 必需，OPENAI_BASE_URL/OPENAI_MODEL 可选；未做真实网络调用（无 API key），按计划用 mock SSE 流验收
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

---

Deferred（待办，由用户决定是否单开 Task）

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
