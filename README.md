# Jessica

Jessica is a determinstic Rust agent runtime with WASM sandbox extensions and
event-sourced persistence.

> 不信模型自述，只信可验证轨迹。

## Workspace

| Crate | Role |
| --- | --- |
| `jessica-core` | Frozen public types and trait contracts |
| `jessica-runtime` | Agent loop and session execution |
| `jessica-harness` | Guards, policy, sensors, and acceptance orchestration |
| `jessica-providers` | OpenAI-compatible streaming provider |
| `jessica-tools` | Native file and bash tools |
| `jessica-cli` | Governed executable entry point |

See `CLAUDE.md` for project rules and `DESIGN.md` for the architecture.

## Governed CLI demo

> The first sentence deliberately contains a misspelling. The example config
> in `examples/harness.toml` verifies that it is corrected, which makes the
> acceptance-failure retry path observable.

Run the example from the workspace root:

```bash
cargo run -p jessica-cli -- --config examples/harness.toml "修改 README 中的拼写错误并验证"
```

`OPENAI_API_KEY` is required. `OPENAI_BASE_URL` and `OPENAI_MODEL` select an
OpenAI-compatible endpoint and model.

## ACP (Agent Client Protocol)

Jessica serves stable **ACP v1** over newline-delimited JSON-RPC on stdin/stdout.
Start it directly or through an ACP client:

```bash
cargo build -p jessica-cli
./target/debug/jessica-cli acp --config /absolute/path/harness.toml
```

`--config` is optional. Provider credentials come from the process environment:
`JESSICA_PROVIDER` selects `openai` (default) or `anthropic`; the matching
provider reads `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `OPENAI_MODEL` or the
`ANTHROPIC_*` equivalents. `initialize`, `session/new`, `session/prompt`, and
`session/cancel` are always implemented; `session/load` is implemented when
durable ACP state is configured (see below). Stdout carries JSON-RPC only;
diagnostic messages go to stderr, and prompts, tool arguments, environment
values, and secrets are never logged.

### Limits

```toml
[acp]
max_sessions = 8
max_prompt_bytes = 1048576
max_history_messages = 200
max_history_bytes = 2097152
```

All four values must be positive. They are enforced at session creation, prompt
validation, and history commit. The history limits apply to committed prior
turns; the current prompt is separately bounded by `max_prompt_bytes`. History
trimming drops only the oldest complete user turn and never separates a tool
call from its result. Creating a session
past `max_sessions` returns `-32602` with
`{"reason":"session_limit_reached","maxSessions":N}`; an oversized or empty
prompt returns `-32602` with a stable, actionable message.

The `[acp]` section is startup-only. ACP reuses the REPL configuration watcher:
changes to reloadable sections such as `[budget]`, `[policy]`, `[sensor]`,
`[acceptance]`, and `[delegation]` apply to the next prompt, while changes to
`events`, `memory`, `skills`, `wasm`, `reliability`, or `acp` reject the whole
reload and keep the startup value. WASM tools are loaded once at startup.

### Session recovery

Enable `session/load` by configuring both the event store and the ACP state
directory:

```toml
[events]
path = "events.db"

[acp]
state_dir = "acp-state"
```

Relative paths resolve against the startup workspace. `state_dir` requires
`events.path`; startup fails if only one is configured. The state directory
contains one small index per ACP session: the session id, canonical cwd,
timestamps, and committed turn ids. It never stores prompt text, tool
arguments, model output, environment values, or credentials. Conversation
history remains in the append-only event store.

When both open successfully, `initialize` reports
`agentCapabilities.loadSession = true`. `session/load` accepts a persisted
session id and the same cwd recorded at creation, reconstructs the newest
recoverable committed turn, filters internal system/bootstrap messages, applies
the normal history limits, and replays the remaining user, assistant, and tool
history as `session/update` notifications **before** the load response. The
restored session then accepts `session/prompt` with the recovered context.
Unknown session ids return `-32002`; mismatched cwd, already-loaded sessions,
and unsupported request fields return `-32602`.

Recovery covers sessions created after this feature was configured. Older ACP
conversations have no index and are not recoverable. If the index points to a
turn whose event metadata is missing or corrupt, Jessica falls back to the
newest older recoverable turn. When no indexed turn is recoverable, loading
fails with `-32603` rather than returning a silently empty conversation.
`session/list`, `session/delete`, `session/resume`, and `session/close` remain
unimplemented.

### Editor and client configuration

Zed `settings.json` (Linux path: `~/.config/zed/settings.json`):

```json
{
  "agent_servers": {
    "Jessica": {
      "type": "custom",
      "command": "/absolute/path/to/jessica-cli",
      "args": ["acp", "--config", "/absolute/path/harness.toml"],
      "env": {
        "OPENAI_API_KEY": "sk-...",
        "OPENAI_MODEL": "your-model",
        "OPENAI_BASE_URL": "https://api.openai.com/v1"
      }
    }
  }
}
```

Paseo: add this entry under `agents.providers` in `~/.paseo/config.json`
(or the `config.json` inside the directory passed to `paseo --home`):

```json
{
  "agents": {
    "providers": {
      "jessica": {
        "extends": "acp",
        "label": "Jessica",
        "description": "Deterministic Rust agent runtime",
        "command": [
          "/absolute/path/to/jessica-cli",
          "acp",
          "--config",
          "/absolute/path/harness.toml"
        ],
        "env": {
          "OPENAI_API_KEY": "sk-...",
          "OPENAI_MODEL": "your-model",
          "OPENAI_BASE_URL": "https://api.openai.com/v1"
        },
        "supportsMcpServers": false,
        "additionalModels": []
      }
    }
  }
}
```

Set `supportsMcpServers` to `false` because Jessica rejects non-empty
`mcpServers` on both `session/new` and `session/load`. Sessions are resumable
only when the harness file passed with `--config` configures `[events].path`
and `[acp].state_dir`.

Paseo 0.15.x performs a **global** provider catalog probe with
`cwd = $HOME`. Jessica's startup workspace root is the directory in which its
ACP process starts, and every `session/new` cwd must stay inside that root.
Start the Paseo daemon from a directory that contains both `$HOME` and the
project workspace (for example `cd "$HOME"` before `paseo daemon start`, or use
a wrapper that `cd`s to a common ancestor). Otherwise the provider can show
`error` with `cwd must be inside the startup workspace root` during catalog
refresh. A workspace located below `$HOME` works without a wrapper when the
daemon is started from `$HOME`, as in the acceptance run.

The exact settings key and file location vary by client version; consult the
client's ACP documentation if it differs.

### cwd and capability boundaries

- `session/new` requires an absolute `cwd` that exists, resolves to a directory,
  and remains inside the process startup workspace (Jessica's working directory)
  after symlink resolution. Relative, missing, file, and outside-root paths are
  rejected with `-32602`. `session/load` applies the same validation and also
  requires the cwd to equal the persisted canonical cwd.
- Supported prompt content is text and `resource_link`. Image, audio, embedded
  resource blocks, and unknown variants are rejected before any model call.
- **No MCP**: ACP v1 requires stdio MCP support, which this phase does not
  implement. Non-empty `mcpServers` are rejected; MCP is a documented Phase 6
  limitation, not silently ignored.
- **No permission requests**: tools run under Jessica's configured policy
  allowlist. A denied call produces a failed `tool_call_update`; Jessica never
  sends `session/request_permission`.
- **No session lifecycle beyond new/load/prompt/cancel**: `session/list`,
  `delete`, `resume`, and `close` are not implemented. Without `[acp].state_dir`
  no session is persisted and `loadSession` is reported as `false`; with it,
  only `session/load` of sessions created by this version is supported.
- No client `fs/*` or `terminal/*` methods, image/audio/embedded resources,
  session modes/config options/plan/usage updates, or ACP v2.
- Notices are sent only when the client advertises
  `clientCapabilities.session.notices`; otherwise Jessica sends zero notices and
  relies on stop reasons and tool updates. Budget exhaustion still returns
  `max_tokens` / `max_turn_requests` as the primary signal.
- A session can run one prompt at a time; different sessions may run
  concurrently. `session/cancel` affects only the named session and leaves it
  usable. Shutdown and client disconnects cancel and drain active prompts with a
  bounded timeout.

See `PLAN.md` for the complete Phase 6 known-limitations and Deferred list.
