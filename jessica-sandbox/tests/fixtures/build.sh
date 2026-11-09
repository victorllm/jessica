#!/usr/bin/env sh
# Rebuilds the checked-in .wasm fixture from fixed_value.wat.
# Requires wasm-tools: cargo install wasm-tools
set -eu
cd "$(dirname "$0")"
wasm-tools parse fixed_value.wat -o fixed_value.wasm
