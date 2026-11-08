# CLAUDE.md — Jessica 项目宪法

本文件是跨会话的持久约定。每次会话开始前先阅读并遵守本文件；本文件的约束优先于临时指令，除非用户明确要求变更。

## 项目定位

Jessica 是一个确定性 Rust Agent 运行时，WASM 沙箱扩展，事件溯源持久化。

信条：**不信模型自述，只信可验证轨迹。**

## 会话启动检查

开始任何 Task 前，先确认：

1. 本次会话只执行 PLAN.md 中指定的一个 Task，不越界实现后续 Task。
2. 已了解 Workspace 依赖单向规则与禁止事项。
3. jessica-core 的公共 API 已冻结，变更必须先询问用户并单开 Task。
4. Task 完成后运行验收命令、贴出结果，并更新 PLAN.md 的进度追踪。

## Workspace 布局与依赖单向规则

Workspace 使用 `resolver = "2"`，成员 crate 如下：

| Crate | 角色 | 允许的内部依赖 |
| --- | --- | --- |
| `jessica-core` | 公共类型与 Trait 契约（冻结层） | 无 |
| `jessica-runtime` | Agent 循环、会话恢复 | `jessica-core` |
| `jessica-sandbox` | wasmtime 沙箱与 WASI 能力控制 | `jessica-core` |
| `jessica-harness` | Middleware、Guard、Sensor、Acceptance | `jessica-core` |
| `jessica-providers` | OpenAI 兼容 Provider | `jessica-core` |
| `jessica-memory` | 事件存储与记忆 | `jessica-core` |
| `jessica-tools` | 原生工具与 WASM 工具注册 | `jessica-core`（Task 3.3 起可依赖 `jessica-sandbox`） |
| `jessica-cli` | 可执行入口与装配 | 所有实现层 |

依赖规则（必须遵守）：

- `jessica-core` 不得依赖任何其他内部 crate，也不得引入 wasmtime、sqlx/rusqlite、HTTP 客户端等实现层重依赖。
- `jessica-runtime`、`jessica-sandbox`、`jessica-harness` 只依赖 `jessica-core`。
- `jessica-cli` 依赖所有实现层，用于最终装配。
- 任何内部依赖只能指向更低层，禁止反向依赖或循环依赖。

## 编码纪律

- 所有公共 API（`pub` 类型、函数、Trait、方法、字段）必须有 doc comment（`///`）。
- 错误处理使用 `thiserror` 定义结构化 enum；禁止 `unwrap`/`expect`（测试代码除外）；库代码不得把 panic 当作控制流。
- 异步统一使用 Tokio；禁止在 async 上下文中混入阻塞调用（`std::fs`、`std::thread::sleep`、阻塞 IO 等），确需阻塞时使用 `spawn_blocking`。
- 测试必须是行为测试：构造真实边界条件并断言可观察行为；禁止“调用后断言返回 Ok”这类自证式测试；mock 必须构造真实的边界条件。
- 运行时结构化日志使用 `tracing`，禁止用 `println!` 输出运行时日志（CLI 面向用户的输出除外）。

## 禁止事项

- 不得引入未在 Cargo.toml 中显式批准的第三方 crate；新增依赖必须先修改 workspace 的 Cargo.toml，并在对应 Task 中说明理由。
- 不得提前实现后续 Phase 的 Trait 方法或功能（例如 Phase 0.2 只定义 Trait 骨架、不实现 Guard；Phase 1 不实现重试/熔断）。
- 不得修改已冻结的 jessica-core 公共 API。确需变更时，先停下询问用户，并单独开 Task。
- 不得在 Task 范围外顺手重构；发现的问题记录到 PLAN.md 的 Deferred 列表，由用户决定是否单开 Task。
- 验收命令失败时不得声明任务完成。

## 验收命令速查

每个 Task 完成前必须运行完整验收：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

单 crate 快速检查：

```bash
cargo test -p <crate>
cargo clippy -p <crate> -- -D warnings
```

每个 Task 完成后必须更新 PLAN.md 中的复选框状态，并记录实际耗时和偏离计划的地方。

## Claude Code 执行纪律

1. 一次会话只做一个 Task。完成后主动运行该 Task 的所有验收命令，并把结果贴出来。
2. 验收命令失败时不得声明完成，应修复后重跑，或明确报告阻塞点。
3. 修改 jessica-core 的公共 API 前必须先停下询问，因为这会破坏跨 Phase 契约。
4. 测试必须是行为测试，不得写“调用后断言返回 Ok”这类自证式测试；mock 必须构造真实的边界条件。
5. 不要在 Task 范围外顺手重构，发现的问题记录到 PLAN.md 的 “Deferred” 列表，由用户决定是否单开 Task。
6. 每个 Task 结束前更新 PLAN.md：勾选完成、记录实际耗时和偏离计划的地方。
