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

## Development checks

CI runs the same checks as the local acceptance commands:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
sh -n install.sh
```

The workflow is defined in `.github/workflows/ci.yml` and uses no secrets.

## Install

Build and install from a checkout:

```bash
./install.sh
```

The installer runs `cargo build --release --locked -p jessica-cli`, installs
the resulting `jessica` binary into `~/.local/bin`, and creates a `jessica-cli`
compatibility symlink for one release cycle. It also prepares `~/.jessica`:

- `harness.toml` configuration, created when absent (re-running keeps it;
  `--force-config` backs it up and regenerates it);
- `acp-state/` for ACP session indexes. The event log `~/.jessica/events.db`
  is created on first run, and `memory.db` / `skills.db` stay there when those
  optional features are enabled.

To register Jessica with a local Paseo installation, run:

```bash
./install.sh --paseo
```

This creates a `~/.local/bin/jessica-paseo` launcher and merges a `jessica`
provider into `~/.paseo/config.json`, keeping a timestamped backup. The
launcher answers Paseo's `--version` probe by running `jessica --version`, runs
`jessica acp` with `~/.jessica/harness.toml`, and reads provider credentials from
`~/.jessica/paseo.env`. That file is created as a commented template with mode
600; fill in one provider:

```bash
# ~/.jessica/paseo.env
JESSICA_PROVIDER=openai
OPENAI_API_KEY=...
OPENAI_MODEL=...
```

Credentials stay in this file instead of `~/.paseo/config.json`. An existing
custom `jessica-paseo` launcher is kept unchanged; the provider entry is
updated to use it. If the launcher does not already pass `--config`, the
installer adds `--config <home>/harness.toml` to the Paseo provider command so
that ACP sessions are persisted and can be resumed. `--paseo-env-file` is
ignored for a kept launcher. A kept custom launcher must still answer Paseo's
`--version` probe itself; the generated launcher delegates to
`jessica --version`. After editing `paseo.env`, run `paseo reload` and verify
with `paseo provider diagnostic jessica`.

Options include `--bin-dir <dir>`, `--home <dir>`, `--paseo-home <dir>`,
`--paseo-env-file <path>`, `--no-config`, and `--force-config`; run
`./install.sh --help` for the full list. Downloading a published release is
not handled by this script yet.

Run it with:

```bash
export PATH="$HOME/.local/bin:$PATH"
export OPENAI_API_KEY=...

jessica --config ~/.jessica/harness.toml "your prompt"
jessica acp --config ~/.jessica/harness.toml
```

## Governed CLI demo

> The first sentence deliberately contains a misspelling. The example config
> in `examples/harness.toml` verifies that it is corrected, which makes the
> acceptance-failure retry path observable.

Run the example from the workspace root:

```bash
cargo run -p jessica-cli --bin jessica -- --config examples/harness.toml "修改 README 中的拼写错误并验证"
```

`OPENAI_API_KEY` is required. `OPENAI_BASE_URL` and `OPENAI_MODEL` select an
OpenAI-compatible endpoint and model.

## ACP (Agent Client Protocol)

Jessica serves stable **ACP v1** over newline-delimited JSON-RPC on stdin/stdout.
Start it directly or through an ACP client:

```bash
cargo build -p jessica-cli
./target/debug/jessica acp --config /absolute/path/harness.toml
```

`--config` is optional. Provider credentials come from the process environment:
`JESSICA_PROVIDER` selects `openai` (default) or `anthropic`; the matching
provider reads `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `OPENAI_MODEL` or the
`ANTHROPIC_*` equivalents. `initialize`, `session/new`, `session/prompt`,
`session/cancel`, and `session/close` are always implemented. `session/load`,
`session/list`, `session/resume`, and `session/delete` are implemented when
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

### Stdio MCP servers

`session/new` accepts MCP servers using the **stdio** transport. Jessica starts
one child process per server, runs the MCP `initialize` handshake, sends
`notifications/initialized`, discovers tools with paginated `tools/list`, and
returns the new session only after discovery succeeds. The command must be an
absolute path; `args` and `env` are passed to the child, and the child's working
directory is the session cwd. ACP-provided environment variables are overlaid
on the inherited process environment.

Each discovered tool is exposed to the model as
`mcp__<server>__<tool>`. Characters that providers commonly reject are replaced
with `_` in that provider-visible name; the original MCP tool name is still
used for `tools/call`. MCP tools are **not** automatically allowed. Add the
namespaced name to the policy allowlist explicitly:

```toml
[policy]
allowed_tools = ["read_file", "mcp__filesystem__read_file"]
```

Policy is reloadable, so a later turn can pick up a changed allowlist. Every
MCP call still passes through the same policy, `budget.tool_timeout_ms`,
`ToolTimeoutGuard`, and `ResultSizeGuard` path as native and WASM tools.
MCP `isError` results become failed `ToolResult`s; protocol and transport
failures also become tool errors rather than panics. Text content is preserved,
while image/audio payloads and other binary data are represented by short
placeholders instead of being copied into the transcript.

Fixed MCP bounds apply per session: at most 8 servers, at most 256 tools, a
4 MiB maximum server line, 10 seconds for initialization and discovery, and an
internal 60-second safety bound for a tool call (normally the configured tool
timeout fires first). There are no MCP-specific configuration keys.

MCP children are session-scoped. `session/close`, `session/delete`, transport
failure, session creation failure, and process shutdown kill and reap every
child. `session/cancel` only cancels the active prompt; it does not stop the
session's MCP servers. Child stderr is drained and discarded, and MCP
environment values and credentials are never logged, persisted, or included in
ACP errors.

HTTP and SSE MCP transports are not supported and return `-32602`. Invalid
server declarations (empty or duplicate names, relative commands, too many
servers, or colliding/oversized tool sets) also return `-32602`; failures after
a process starts, including initialization or discovery errors, return a
content-free `-32603` with `{"reason":"mcp_server_startup_failed"}`. Only
`session/new` starts MCP servers: non-empty `mcpServers` on `session/load` or
`session/resume` remain rejected with `-32602`.

### Session recovery and lifecycle

Enable the persisted session lifecycle by configuring both the event store and
the ACP state directory:

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
`agentCapabilities.loadSession = true` and advertises
`sessionCapabilities.list`, `delete`, `resume`, and `close`. Without
persistence, only `close` is advertised. `session/load` accepts a persisted
session id and the same cwd recorded at creation, reconstructs the newest
recoverable committed turn, filters internal system/bootstrap messages, applies
the normal history limits, and replays the remaining user, assistant, and tool
history as `session/update` notifications **before** the load response. The
restored session then accepts `session/prompt` with the recovered context.
Unknown session ids return `-32002`; mismatched cwd, already-loaded sessions,
and unsupported request fields return `-32602`.

`session/resume` restores the same conversation context and cwd checks as
`session/load` but sends **no replay notifications**. Resuming an already live
session with its recorded cwd is idempotent.

`session/list` returns only `sessionId`, canonical `cwd`, and `updatedAt` for
sessions under Jessica's startup workspace root. It never returns titles or
conversation content. Results are sorted by `updatedAt` descending with the
session id as a tie-break and are paged at 100 entries; pass the opaque
`nextCursor` back unchanged to fetch the next page. An optional `cwd` filters
to exactly that canonical directory. Malformed index files are skipped so one
damaged entry cannot hide every healthy session; a malformed cursor returns
`-32602`.

`session/delete` removes the ACP index and releases a live session, but it does
**not** delete event-store rows. The event log remains append-only, so deletion
is logical from ACP's perspective. A deleted session no longer appears in
`session/list`, cannot be loaded or resumed, and a repeated delete returns
`-32002`.

`session/close` cancels active work, waits a bounded time for it to stop, and
releases the in-memory session. When persistence is configured, the index is
retained, so the session stays listed and can be loaded or resumed later;
closing an indexed session that is not live is an idempotent success. Without
persistence, close releases the live session and a second close returns
`-32002`.

Recovery covers sessions created after this feature was configured. Older ACP
conversations have no index and are not recoverable. If the index points to a
turn whose event metadata is missing or corrupt, Jessica falls back to the
newest older recoverable turn. When no indexed turn is recoverable, loading
fails with `-32603` rather than returning a silently empty conversation.

### Editor and client configuration

Zed `settings.json` (Linux path: `~/.config/zed/settings.json`):

```json
{
  "agent_servers": {
    "Jessica": {
      "type": "custom",
      "command": "/absolute/path/to/jessica",
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

`./install.sh --paseo` performs this integration automatically (see
[Install](#install)). To configure Paseo manually, add this entry under
`agents.providers` in `~/.paseo/config.json` (or the `config.json` inside the
directory passed to `paseo --home`):

```json
{
  "agents": {
    "providers": {
      "jessica": {
        "extends": "acp",
        "label": "Jessica",
        "description": "Deterministic Rust agent runtime",
        "command": [
          "/absolute/path/to/jessica",
          "acp",
          "--config",
          "/absolute/path/harness.toml"
        ],
        "env": {
          "OPENAI_API_KEY": "sk-...",
          "OPENAI_MODEL": "your-model",
          "OPENAI_BASE_URL": "https://api.openai.com/v1"
        },
        "supportsMcpServers": true,
        "additionalModels": []
      }
    }
  }
}
```

`supportsMcpServers` may be `true`: Jessica accepts stdio MCP servers on
`session/new`. A client that offers only HTTP/SSE MCP transports still receives
`-32602`; use stdio servers. MCP servers are rejected on `session/load` and
`session/resume`, so a client that persists MCP configuration should start a new
session rather than resuming one with `mcpServers`. Sessions are recoverable
only when the harness file passed with `--config` configures `[events].path`
and `[acp].state_dir`. With persistence, clients that check `loadSession` use
`session/load`; clients that prefer the non-replaying lifecycle can use
`session/resume`.

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

### Permission requests

Before running a sensitive native tool, Jessica sends ACP
`session/request_permission` and waits for the client. The fixed sensitive set is
`bash`, `write_file`, and `delegate_task`; `read_file`, configured MCP tools, and
WASM tools remain governed only by the policy allowlist.

- Every request offers exactly `allow_once` ("Allow this execution") and
  `reject_once` ("Reject this execution"). Jessica keeps no persistent approval
  state across calls or turns.
- The call is announced as a `tool_call` with its provider `toolCallId` before
  the request is sent. That update carries the default `pending` status, which
  ACP omits on the wire. On `allow_once` Jessica emits a `tool_call_update` with
  status `in_progress` and the same id before execution starts.
- A rejection, a client `cancelled` outcome, cancellation, transport failure, or
  an unanswered request never executes the tool. Rejection and the 5-minute
  answer deadline fail only that call and return an error result to the model; a
  client `cancelled` outcome, `session/cancel`, close, or disconnect ends the
  prompt as `cancelled`.
- The 5-minute answer deadline is fixed and separate from the tool timeout. The
  tool timeout starts only after approval.
- Only the top-level `delegate_task` call asks for permission. The child agent's
  tools run under that single delegation approval and never send their own
  permission requests.

### cwd and capability boundaries

- `session/new` requires an absolute `cwd` that exists, resolves to a directory,
  and remains inside the process startup workspace (Jessica's working directory)
  after symlink resolution. Relative, missing, file, and outside-root paths are
  rejected with `-32602`. `session/load` and `session/resume` apply the same
  validation and require the cwd to equal the persisted canonical cwd.
  `session/list` validates an optional filter cwd the same way.
- Supported prompt content is text and `resource_link`. Image, audio, embedded
  resource blocks, and unknown variants are rejected before any model call.
- **MCP**: stdio servers are supported on `session/new`; their tools are
  namespaced and go through the normal policy and guards. HTTP/SSE transports
  are rejected, and `session/load`/`session/resume` still reject non-empty
  `mcpServers`. See [Stdio MCP servers](#stdio-mcp-servers).
- **Permissions**: `bash`, `write_file`, and `delegate_task` ask through
  `session/request_permission` before execution; see
  [Permission requests](#permission-requests). A denied call produces a failed
  `tool_call_update`.
- **No archive/fork/status/set_title**: the stable v1 `list`, `delete`,
  `resume`, and `close` lifecycle methods are implemented, but archive, fork,
  status, session titles, and other unstable lifecycle extensions are not.
  Without `[acp].state_dir` no session is persisted, `loadSession` is `false`,
  and `list`/`delete`/`resume` return `-32601`; `close` remains available for
  live in-memory sessions. Titles are never fabricated or persisted.
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
