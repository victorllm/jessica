# Echo wasm tool example

`echo.wat` is the plugin source; `echo.wasm` is the checked-in compiled module
used by tests and the CLI example. Rebuild it with:

```sh
cargo install wasm-tools   # once
./build.sh
```

The guest exports:

- `memory` — linear memory with two 32 KiB regions;
- `jessica_alloc(len: i32) -> i32` — returns the input region at byte 32768,
  or `-1` when `len` exceeds 32 KiB;
- `jessica_invoke(ptr: i32, len: i32) -> i64` — copies the JSON arguments to
  byte 65536 and returns `(len << 32) | 65536`.

`manifest.json` selects those exports, declares the argument schema, and sets
sandbox limits. It uses manifest version 2: the `annotations` object declares
the guest fully read-only (`read_only: true`, `destructive: false`,
`open_world: false`). The host reports the descriptor version as
`manifest-v2`, keeps the tool policy-only, and still enforces the sandbox
regardless of the declaration: read-only preopens, no write directory, no
network, a fresh instance per call, and the declared fuel/wall/memory/table/
instance limits. A v1 manifest with no `version` or `annotations` still loads
and keeps the conservative read-only sandbox baseline; a v2 manifest that
declares `destructive` or `open_world` is classified more cautiously and asks
for one-time approval when a permission gate is installed.

Relative `module` and `read_only_dirs[].host` paths resolve against this
directory. Results above the configured `tools.inline_threshold_bytes` become
workspace artifacts: the model receives only a bounded preview and SHA-256
digest, and `read_artifact` pages the stored bytes.

Run the CLI example from the repository root:

```sh
cargo run -p jessica-cli -- --config examples/wasm_tool.toml "echo hello"
```

The example allowlist contains only `echo_json`, so native tools are absent
from the provider tool schema and an attempted call returns an `unknown tool`
error result.
