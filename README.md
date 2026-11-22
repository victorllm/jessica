# Jessica

Jessica is a deterministic Rust agent runtime with WASM sandbox extensions and
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

See `CLAUDE.md` for project rules and `PLAN.md` for the delivery plan.

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

Choose one of two install modes.

Build and install from a checkout (requires a Rust toolchain):

```bash
./install.sh
```

Or install the latest prebuilt release (currently Linux x86_64; no Rust
toolchain required):

```bash
./install.sh --from-release
```

Release mode downloads `jessica-linux-x86_64.tar.gz` and its `.sha256` file
from this project's GitHub Releases, verifies the SHA-256 checksum, and only
then installs the binary. The checksum detects corrupted or tampered downloads;
it is not a signature. If downloading or verification fails, the installer
stops with an error and suggests rebuilding from source with `./install.sh`.

Both modes install the `jessica` binary into `~/.local/bin` and create a
`jessica-cli` compatibility symlink for one release cycle. They also prepare
`~/.jessica`:

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

Options include `--from-release`, `--bin-dir <dir>`, `--home <dir>`,
`--paseo-home <dir>`, `--paseo-env-file <path>`, `--no-config`, and
`--force-config`; run `./install.sh --help` for the full list.
`JESSICA_RELEASE_BASE_URL` overrides the release download base URL, for mirrors
or local testing.

Run it with:

```bash
export PATH="$HOME/.local/bin:$PATH"
export OPENAI_API_KEY=...

jessica --config ~/.jessica/harness.toml "your prompt"
jessica acp --config ~/.jessica/harness.toml
```

## Governed CLI demo

> `examples/typo.md` deliberately contains a misspelling. The example config
> in `examples/harness.toml` verifies that it is corrected, which makes the
> acceptance-failure retry path observable.

Run the example from the workspace root:

```bash
cargo run -p jessica-cli --bin jessica -- --config examples/harness.toml "修改 examples/typo.md 中的拼写错误并验证"
```

`OPENAI_API_KEY` is required. `OPENAI_BASE_URL` and `OPENAI_MODEL` select an
OpenAI-compatible endpoint and model.

## Profiles and effective configuration

Jessica resolves configuration in this order: built-in defaults, the selected
`profile.preset`, then explicit fields in the TOML document. Explicit values
always win, including an explicit empty `policy.allowed_tools`.

```toml
[profile]
preset = "dev"   # default; also accepts "safe", "ci", or "lab"
trace = "strict" # default; use "off" to disable persistence
```

Presets only supply safe defaults; they never enable memory, skills, or
reliability. `safe` narrows the default allowlist to `read_file`; `dev`, `ci`,
and `lab` use `bash`, `read_file`, and `write_file`. Side-effecting subsystems
therefore stay opt-in.

Presets and allowlists do not enable the native `bash` tool by themselves. It
also requires `[tools.bash] enabled = true`; without that explicit opt-in bash
stays hidden even when the profile and `policy.allowed_tools` include it.
Configurations written before this gate existed must add the opt-in to keep
their previous tool surface:

```toml
[tools.bash]
enabled = true
```

Trace persistence is strict by default. A relative path resolves against the
startup workspace, and the event store plus any missing parent directories are
created on first run:

```toml
[events]
path = ".jessica/events.db" # the default when omitted
```

Set `trace = "off"` to disable persistence. Combining it with an explicit
`events.path` is rejected instead of silently overriding one side. `--resume`,
`replay`, and ACP session recovery require trace persistence.

`jessica config` prints the fully resolved configuration as JSON without
opening a provider, event store, memory or skills database, or wasm module:

```bash
jessica --config ~/.jessica/harness.toml config
```

## Tool catalog and exposure

Every turn rebuilds a host-side tool catalog. A tool is exposed to the model
only when all of these host decisions agree:

1. its source is available this turn (native, configured WASM, session MCP, or
   enabled delegation);
2. its name is in `policy.allowed_tools`;
3. `tools.profile` permits that source/name pair;
4. for the native `bash` tool, `[tools.bash] enabled = true` is set.

Being in the allowlist is necessary but never sufficient.

```toml
[tools]
profile = "full"          # default; also "readonly", "coding", "ops"
max_exposed_tools = 64
max_tool_schema_bytes = 131072
inline_threshold_bytes = 32768    # results up to this size stay inline
max_inline_bytes = 65536          # hard cap entering the model context
max_artifact_bytes = 16777216     # bytes stored for one artifact
artifact_dir = ".jessica/artifacts"
artifact_retention_days = 7
artifact_max_bytes = 268435456

[tools.read_file]
timeout_ms = 5000         # defaults to budget.tool_timeout_ms
max_args_bytes = 65536    # optional pre-execution argument limit

[tools.bash]
enabled = false           # explicit opt-in; bash stays hidden when false
sandbox = "process"       # only supported value in this build
timeout_ms = 10000        # cannot exceed bash's internal 10 s deadline
```

| Profile | Native tools exposed when allowlisted |
| --- | --- |
| `readonly` | `read_file`, `read_artifact` |
| `coding` | `read_file`, `read_artifact`, `write_file` |
| `ops` | `read_file`, `read_artifact`, `bash` |
| `full` | every allowlisted tool, including WASM, MCP, and `delegate_task` |

`read_artifact` is not in the default allowlist. Add it to
`policy.allowed_tools` if the model should paginate artifacts created by large
tool results; profile filtering is necessary but not sufficient, exactly as for
every other tool. It is read-only, accepts only an artifact digest (never a
filesystem path), and its own results bypass result management so pagination
cannot recursively create more artifacts.

Narrow profiles never expose WASM, MCP, or delegation tools. The default is
`full`; choose a narrower profile to enforce least privilege. `ops` and `full`
still need the `[tools.bash] enabled = true` opt-in before bash is exposed, and
the system prompt does not advertise a disabled bash tool.

Hidden tools are not registered, so they never appear in the provider tool
schema and cannot execute. A model that hallucinates a hidden name receives an
`unknown tool` error result. `jessica tools list --all` and
`jessica tools inspect <name>` explain every hidden reason, including
`disabled; set [tools.bash].enabled = true` for a bash tool that the policy and
profile would otherwise permit. Exceeding either exposure budget fails turn
startup with an actionable error instead of silently dropping tools. Per-tool
`max_args_bytes` is checked before policy, approval, and execution; `timeout_ms`
overrides the global tool timeout for that tool. The whole `[tools]` section,
including `[tools.bash]` and `[tools.supervisor]`, is hot-reloadable and applies
to the next turn.

### Execution supervisor

`bash`, the command sensor, and the acceptance check run under one supervisor:

```toml
[tools.supervisor]
env_allowlist = ["CARGO_HOME", "RUSTUP_HOME"]  # additions to the defaults
command_timeout_ms = 10000    # defaults to min(budget.tool_timeout_ms, 10 s)
max_output_bytes = 65536      # defaults to min(64 KiB, budget.max_result_bytes)
sensor_timeout_ms = 10000     # defaults to command_timeout_ms
acceptance_timeout_ms = 10000 # defaults to command_timeout_ms
```

Every child starts from an empty environment. The defaults `PATH`, `HOME`,
`LANG`, `TERM`, and `TMPDIR` are always present; `env_allowlist` adds names and
cannot remove them. Empty names, wildcards, and credential-bearing names
(`OPENAI_*`, `ANTHROPIC_*`, `JESSICA_*`, `*_KEY`, `*_TOKEN`, `*_SECRET`,
`*_PASSWORD`, SSH/dynamic-linker variables, and similar) are rejected at load
time and filtered again at execution time. Provider keys and Jessica host
credentials therefore never reach a command, sensor, or acceptance process.

The deadline covers spawning, reading both pipes, and waiting for exit. A
timeout fails the tool or check instead of hanging the turn; only the direct
child is killed on drop. This build provides no process-group guarantee, so a
grandchild started by a command can survive a timeout or cancellation, and it
applies no resource limits. Process-group `SIGTERM`→`SIGKILL` cleanup and
rlimits belong to Task 9.3b and are not enabled here; adding `enabled = true`
does not change that boundary.

Policy is not a complete security boundary for bash. Jessica does not parse
command strings, so `policy.allowed_tools` can only permit or block the tool as
a whole. Real isolation depends on the execution sandbox; until process-group
handling and rlimits land, run bash only in trusted workspaces.

Output beyond `max_output_bytes` per stream is truncated with a notice that
records the original byte count. The rendered `bash` result is also capped at
the same value, and any tool result above `inline_threshold_bytes` is stored as
an artifact with a bounded preview, so a large command output cannot trip
`ResultSizeGuard` or turn into `BudgetExceeded`. A non-zero exit still means
“the command ran and failed” and keeps its exit code; only spawn, timeout, read,
and wait failures are supervisor errors.

`[tools.supervisor]` is rebuilt for each turn, so changes apply to the next turn
and never to an already running child. Child-agent commands are aligned with the
same supervisor in a later task; the current delegation path keeps its existing
fixed limits.

```bash
jessica --config ~/.jessica/harness.toml tools list
jessica --config ~/.jessica/harness.toml tools list --all
jessica --config ~/.jessica/harness.toml tools inspect write_file
```

`list` prints exposed tools sorted by name. `--all` adds hidden tools and the
reason each is hidden. `inspect` prints the descriptor, capabilities, effective
limits, approval policy, argument-field hints, and the schema digest. The
schema digest is `sha256:` plus the SHA-256 of the canonical schema JSON with
object keys sorted recursively, so the same configuration always produces the
same output. These commands never open a provider, event store, memory or
skills database, or MCP connection; they also do not create `.jessica/`.

### Result manager and artifacts

Every tool result passes through the host result manager before it reaches the
transcript, trace, sensor, or model. The manager is applied after execution and
before `ResultSizeGuard`; the guard remains the final backstop, but a managed
result never needs to trip it. Only `read_artifact`'s own output bypasses
management, so pagination cannot recursively create artifacts.

- A result at or below `inline_threshold_bytes` is returned unchanged.
- A larger result is stored as a content-addressed artifact. The model receives
  a bounded preview plus the `sha256:` digest, original byte count, `truncated`
  flag, and the next `read_artifact` offset.
- A result larger than `max_artifact_bytes` is truncated at a UTF-8 boundary,
  marked `truncated: true`, and its digest covers the stored bytes.
- `max_inline_bytes` is a hard cap on the text delivered to the model. The
  effective caps are additionally clamped to `budget.max_result_bytes`, so an
  existing configuration with a small result budget keeps working instead of
  producing `BudgetExceeded`.

The transcript and trace record this managed envelope — digest, byte counts,
truncation flag, cursor, and bounded preview — not the full stored artifact.
The complete bytes are only read back through `read_artifact` under its own
bounds.

All `[tools]` numeric artifact limits must be at least 1, and
`inline_threshold_bytes <= max_inline_bytes <= max_artifact_bytes` must hold.
`artifact_dir` must be a relative path inside the workspace; empty paths,
absolute paths, `..`, and paths that resolve outside the workspace through a
symlink are rejected at load time.

Artifacts are stored at `<artifact_dir>/<first two digest hex>/<64 hex digest>`.
Each artifact has a `.json` sidecar recording the version, digest, stored size,
tool name, creation time, original size, and truncation flag. The sidecar never
contains provider credentials, environment variables, tool arguments, or
command text. Writes use a same-directory temporary file, `fsync`, and rename;
directories are created with mode `0700` and files with mode `0600`. Existing
artifacts are content-addressed, so writing the same bytes again reuses the
same path.

`read_artifact` accepts `digest`, optional `offset`, and optional `limit`. It
returns one UTF-8 page with the artifact digest, actual offset, page byte count,
total size, and next offset. Offsets past the end, unknown or malformed digests,
symlinked paths, and missing or inconsistent sidecars produce structured tool
errors. The tool reads only files selected by digest under the configured
artifact directory; it never executes a tool, accepts a filesystem path, or
creates another artifact.

Once per turn, before the first oversized write, the manager runs one bounded
cleanup pass: artifacts older than `artifact_retention_days` are deleted first,
then the oldest remaining artifacts are deleted until the total is at most
`artifact_max_bytes`. Cleanup only considers well-formed artifact files and
sidecars under the configured directory, skips symlinks and corrupt entries,
and logs failures as warnings. A cleanup or write failure never fails the tool
call; the model receives a bounded fallback preview marked as unavailable.

> **Privacy:** artifacts contain raw tool output, are stored unencrypted inside
> the workspace, and may include secrets printed by a command or file. The
> sidecar omits credentials, but the artifact body does not redact anything.
> Keep `artifact_dir` out of version control, choose a short retention period,
> and avoid enabling tools that emit secrets in untrusted workspaces.

## Trace verification and digests

`jessica trace verify` audits recorded turn traces without creating or
migrating the configured event store (default `.jessica/events.db` relative to
the startup workspace). It does not execute a replay unless `--replay` is
requested:

```bash
jessica --config ~/.jessica/harness.toml trace verify --turn <turn_id>
jessica --config ~/.jessica/harness.toml trace verify --turn <turn_id> --replay
jessica --config ~/.jessica/harness.toml trace verify --all --json
jessica --config ~/.jessica/harness.toml trace verify --all --allow-open
jessica --config ~/.jessica/harness.toml trace verify --all --replay
jessica --config ~/.jessica/harness.toml trace verify --turn <turn_id> --expect <digest>
```

It checks structural completeness, v2 round/permission/child-session semantics,
v3 nested child-agent evidence, and that every mirrored agent event still exists
in the legacy event stream with the same session, timestamp, and payload. It also recomputes each turn's
SHA-256 hash chain from the stored column bytes, so an edited, deleted, or
inserted trace row is reported as a structural failure instead of being
verified. Trace schema version 3 is the current producer version; versions 1
through 3 remain readable, while a version newer than the build is rejected.
`--turn` and `--all` are mutually exclusive; `--allow-open` accepts traces
without a `turn_ended` marker but never accepts an invalid trace and is
rejected with `--replay`.

Schema version 3 adds `child_agent` entries. Each entry nests one event emitted
inside a delegated child session under the parent turn's trace chain; the
envelope keeps the parent attempt and the child session, and `call_id` links the
event to the parent `delegate_task` call. Child entries are trace-only and have
no legacy mirror row. When a trace contains child-agent evidence, `verify`
requires the child's `session_started.messages` to equal exactly the fixed
child system prompt plus the task from the preceding, not-yet-finished
`delegate_task` call in the same parent attempt and session. It then enforces
one-based child rounds, model deltas only inside an open round, paired tool
start/finish events, and the absence of parent-only or nested child lifecycle
events. The child `session_ended` reason, rounds, tool calls, and tool errors
must agree with the parent's `child_session_ended`; a missing child terminal is
accepted only for a cancelled (timed-out) child, and only when the reported
counters match the observed prefix. Traces with lifecycle-only child evidence
(for example version-2 traces or older version-3 producers) keep the earlier
relaxed checks.

`--replay` accepts `--turn` or `--all` but cannot be combined with
`--allow-open`. It rebuilds each fresh attempt from the trace's own bootstrap
messages and recorded runtime events, drives the deterministic replay engine,
and requires the produced event sequence to match. Version-1 fresh traces use
`turn_started.messages` as the bootstrap. Resumed traces start the real
`AgentLoop` from the recorded checkpoint conversation; the first
`round_started` supplies the checkpoint's completed-round offset, and replay
uses the same total round budget so budget-exhaustion messages match. It covers
closed fresh v1/v2/v3 traces ending `completed`, `error`, or on the max-rounds
budget, plus closed single-attempt resumed v2/v3 traces with at least one model
round; broader subsets report `unsupported` and exit `1` instead of being
silently skipped. Nested child-agent entries are validated first and then
ignored by replay: it never executes a delegated child or issues a child
provider call, because the parent's recorded `delegate_task` tool result already
reproduces the delegation boundary. `--all --replay` loads, verifies, and replays one turn at a
time so memory stays bounded by the largest single trace. A mismatch invalidates
that turn and the batch continues; an unsupported but otherwise valid trace
stays `ok` with `replay.status: "unsupported"`, and the run exits `1` naming the
affected turns. `--expect` requires `--turn` and a 64-character hexadecimal
digest (case-insensitive); a matching digest leaves the report unchanged, while
a mismatch adds a `digest_mismatch` problem. Exit code `0` means every selected
trace passed; `1` means an invalid trace, an unallowed open trace, an unknown
turn, a replay mismatch, an unreplayable trace, or a storage/configuration
error. `--json` emits a stable report with `report_version: 3`, per-turn
statuses, per-turn `digest`, optional replay results, and machine-readable
problem kinds.

Child evidence is as sensitive as parent evidence: the child bootstrap contains
the delegated task, and the trajectory records child model output, tool
arguments, and tool results. These entries live in the same append-only trace
database as the parent and appear in exported bundles, under the same access
controls and retention. Trace retention and privacy deletion are not
implemented yet, so treat the event database and every exported bundle as
carrying the full child conversation.

Auditing requires event-store schema version 2, which adds the hash chain and
backfills it for existing traces during migration. A version-1 database must be
opened once by a current read-write build before it can be audited; read-only
audit never migrates and reports the required schema version instead.

`jessica trace digest` prints the verified digest for one turn or all turns
without a full verification report:

```bash
jessica --config ~/.jessica/harness.toml trace digest --turn <turn_id>
jessica --config ~/.jessica/harness.toml trace digest --all
jessica --config ~/.jessica/harness.toml trace digest --all --json
```

A single-turn run prints only the digest, so it can be captured directly. An
`--all` run prints one `turn_id<TAB>digest` line per turn in ascending turn-id
order. `--json` emits `report_version: 1`, `digest_format:
"jessica.trace.digest.v1"`, and per-turn `digest`, `closed`, and `entries`.

## Portable evidence bundles

`jessica trace export` writes a self-contained JSON evidence bundle. The
bundle can be verified on a machine that has no event database and no
`[events]` configuration:

```bash
jessica --config ~/.jessica/harness.toml trace export --turn <turn_id> --out evidence.json
jessica --config ~/.jessica/harness.toml trace export --all --out evidence.json
jessica --config ~/.jessica/harness.toml trace export --all --allow-open --out evidence.json
jessica trace verify-bundle --bundle evidence.json
jessica trace verify-bundle --bundle evidence.json --json
jessica trace verify-bundle --bundle evidence.json --allow-open
jessica trace verify-bundle --bundle evidence.json --expect <digest>
```

Export runs against the event store read-only and refuses to write a bundle
for an invalid trace (for example one with a `Gap`, a broken lifecycle, or a
broken hash chain); no partial file is written. Open traces without a
`turn_ended` marker are rejected unless `--all --allow-open` is used;
`--allow-open` is not accepted with `--turn`. The same store exports
byte-identically every time, with turns in ascending turn-id order.

The bundle is `bundle_version: 1`, `bundle_format:
"jessica.trace.bundle.v1"`. Each turn carries its id, `closed` flag, declared
turn digest, trace entries (`seq`, `timestamp`, exact `payload_json`), and the
legacy mirror rows (`session_id`, `timestamp`, `payload_json`) that the mirror
check consumes. Entries are exported verbatim, so nested child-agent evidence is
covered by the per-turn hash chain, `trace digest`, `trace export`, and
`verify-bundle` without a separate bundle format: editing, inserting, or
deleting a child event changes the chain and is detected like any other trace
edit. `aggregate_digest` is the SHA-256 of the
`jessica.trace.bundle.v1` domain, the big-endian turn count, and each turn's id
and raw 32-byte digest in ascending turn-id order, with every field length
prefixed.

`verify-bundle` never reads `[events]`, opens a database, migrates a schema, or
creates sidecars. It reconstructs every chain in a private in-memory store,
re-runs the same structural, semantic, hash-chain, and trace-to-legacy mirror
checks, compares each declared turn digest with the recomputed value, and
compares the declared aggregate with the aggregate recomputed from the
verified chains. `--expect` additionally requires the recomputed aggregate to
equal an externally recorded 64-character hexadecimal digest
(case-insensitive). `--json` emits a stable report with `report_version: 1`,
the declared/computed/expected aggregate, bundle-level problems, and per-turn
statuses. Exit code `0` means the bundle has no problems and every turn is
acceptable; open turns require `--allow-open`. Exit code `1` means a bundle
problem, an invalid or unallowed open turn, a malformed or oversized bundle,
or a digest mismatch. Bundles larger than 64 MiB are rejected before parsing.
Replay is not run from a bundle, so `--replay` is rejected.

A bundle is evidence, not a signature. An attacker able to rewrite the whole
bundle can recompute every chain and the aggregate; compare the aggregate with
a value recorded outside the bundle, for example through `--expect`, to detect
that case. The bundle also records only positive evidence: an exporter that
omits a turn is not proven inconsistent unless the expected aggregate or turn
set is anchored externally.

The hash chain is tamper-evident only relative to an externally kept digest:
an attacker able to rewrite the whole database can recompute the chain. It is
not a signature. Record the digest outside the database, for example in CI
output or a release manifest, and pass it back with `--expect` to anchor the
audit.

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
changes to reloadable sections such as `[budget]`, `[policy]`, `[tools]`,
`[sensor]`, `[acceptance]`, and `[delegation]` apply to the next prompt, while changes to
`profile`, `events`, `memory`, `skills`, `wasm`, `reliability`, or `acp` reject
the whole reload and keep the startup value. WASM tools are loaded once at
startup.

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
used for `tools/call`. MCP tools are **not** automatically exposed. Add the
namespaced name to the policy allowlist explicitly and use the `full` tool
profile:

```toml
[policy]
allowed_tools = ["read_file", "mcp__filesystem__read_file"]

[tools]
profile = "full"
```

Policy is reloadable, so a later turn can pick up a changed allowlist. Every
MCP call still passes through the same policy, `budget.tool_timeout_ms`,
`ToolTimeoutGuard`, result manager, and `ResultSizeGuard` path as native and
WASM tools.
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

Enable the persisted session lifecycle with the ACP state directory. The
default strict trace supplies the event store, so no `[events]` section is
required:

```toml
[acp]
state_dir = "acp-state"

# Optional: override the default .jessica/events.db store.
[events]
path = "events.db"
```

Relative paths resolve against the startup workspace. `state_dir` requires
trace persistence; startup fails when `profile.trace = "off"` disables the
event store. The state directory
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
only when the harness file passed with `--config` configures `[acp].state_dir`
and trace persistence is enabled (the default, or an explicit `[events].path`).
With persistence, clients that check `loadSession` use `session/load`; clients
that prefer the non-replaying lifecycle can use `session/resume`.

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
