;; Test fixture: memory and table growth attempts.
;;
;; With trap_on_grow_failure enabled, growth past a configured store limit
;; traps; without a limit these calls return the new size.
;;
;; Rebuild limits.wasm with ./build.sh after editing this file.
(module
  (memory (export "memory") 1)
  (table $values 10 funcref)

  (func (export "grow_memory") (result i32)
    (memory.grow (i32.const 1024)))

  (func (export "grow_table") (result i32)
    (table.grow $values (ref.null func) (i32.const 1000)))

  (func (export "answer") (result i32)
    (i32.const 42)))
