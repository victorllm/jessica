;; Behaviour fixture for the wasm tool buffer ABI.
;;
;; A single module exports a shared allocator plus several invoke functions
;; that succeed, trap, exhaust fuel, or return malformed output. Tests point
;; each manifest's `abi.invoke` at the behaviour they need.
(module
  (memory (export "memory") 2)
  ;; Invalid JSON payload at pointer 0.
  (data (i32.const 0) "not json")
  ;; Invalid UTF-8 payload at pointer 16.
  (data (i32.const 16) "\ff")

  ;; Allocates a fixed 4 KiB input region for ordinary argument sizes.
  (func (export "alloc") (param $len i32) (result i32)
    (if (result i32)
      (i32.le_u (local.get $len) (i32.const 4000))
      (then (i32.const 4096))
      (else (i32.const -1))))

  ;; Always reports allocation failure.
  (func (export "bad_alloc") (param $len i32) (result i32)
    (i32.const -1))

  ;; Copies the input to the output region and returns it.
  (func (export "echo") (param $ptr i32) (param $len i32) (result i64)
    (local $i i32)
    (block $done
      (loop $copy
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (i32.const 8192) (local.get $i))
          (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $copy)))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $len)) (i64.const 32))
      (i64.extend_i32_u (i32.const 8192))))

  ;; Traps with an explicit unreachable instruction.
  (func (export "trap") (param i32) (param i32) (result i64)
    unreachable)

  ;; Loops forever, consuming fuel until the sandbox stops it.
  (func (export "spin") (param i32) (param i32) (result i64)
    (loop $forever (br $forever))
    (i64.const 0))

  ;; Returns the "not json" payload at pointer 0 with length 8.
  (func (export "bad_json") (param i32) (param i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 8)) (i64.const 32))
      (i64.extend_i32_u (i32.const 0))))

  ;; Returns the invalid UTF-8 payload at pointer 16 with length 1.
  (func (export "bad_utf8") (param i32) (param i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 1)) (i64.const 32))
      (i64.extend_i32_u (i32.const 16))))

  ;; Claims a 1000-byte result at pointer 8192.
  (func (export "large") (param i32) (param i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 1000)) (i64.const 32))
      (i64.extend_i32_u (i32.const 8192))))

  ;; Claims 1000 bytes starting near the end of the 128 KiB memory.
  (func (export "out_of_bounds") (param i32) (param i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 1000)) (i64.const 32))
      (i64.extend_i32_u (i32.const 131000))))
)
