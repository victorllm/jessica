#!/usr/bin/env sh
# Rebuilds echo.wasm from echo.wat.
# Requires wasm-tools: cargo install wasm-tools
set -eu
cd "$(dirname "$0")"
wasm-tools parse echo.wat -o echo.wasm
