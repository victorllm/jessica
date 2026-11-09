;; Test fixture: one exported function returns a fixed value, another traps.
;;
;; Rebuild fixed_value.wasm with ./build.sh after editing this file.
(module
  (func (export "answer") (result i32)
    i32.const 42)
  (func (export "trap") (result i32)
    unreachable))
