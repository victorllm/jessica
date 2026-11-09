;; Example Jessica wasm tool.
;;
;; Implements the manifest buffer ABI:
;;   jessica_alloc(len: i32) -> i32
;;   jessica_invoke(ptr: i32, len: i32) -> i64
;;
;; The guest echoes the JSON arguments back as the tool result. Two fixed
;; 32 KiB regions keep the example small: input at byte 32768, output at byte
;; 65536. The returned i64 packs the output length in the high 32 bits and the
;; output pointer in the low 32 bits.
(module
  (memory (export "memory") 2)

  (func (export "jessica_alloc") (param $len i32) (result i32)
    (if (result i32)
      (i32.le_u (local.get $len) (i32.const 32768))
      (then (i32.const 32768))
      (else (i32.const -1))))

  (func (export "jessica_invoke")
        (param $ptr i32) (param $len i32) (result i64)
    (local $i i32)
    (if (i32.gt_u (local.get $len) (i32.const 32768))
      (then (return (i64.const -1))))
    (block $done
      (loop $copy
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (i32.const 65536) (local.get $i))
          (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $copy)))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $len)) (i64.const 32))
      (i64.extend_i32_u (i32.const 65536))))
)
