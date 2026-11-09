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
sandbox limits. Relative `module` and `read_only_dirs[].host` paths resolve
against this directory.

Run the CLI example from the repository root:

```sh
cargo run -p jessica-cli -- --config examples/wasm_tool.toml "echo hello"
```

The example allowlist contains only `echo_json`, so a call to a native tool is
denied with a `PolicyDenied` event and an error tool result.
