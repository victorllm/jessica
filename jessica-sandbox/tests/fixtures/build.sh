#!/usr/bin/env sh
# Rebuilds the checked-in .wasm fixtures from their .wat sources.
# Requires wasm-tools: cargo install wasm-tools
set -eu
cd "$(dirname "$0")"
for source in *.wat; do
    wasm-tools parse "$source" -o "${source%.wat}.wasm"
done
