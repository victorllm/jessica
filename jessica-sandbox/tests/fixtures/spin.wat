;; Test fixture: a function that never returns on its own.
;;
;; Fuel metering or the wall-clock epoch watchdog must stop it.
;;
;; Rebuild spin.wasm with ./build.sh after editing this file.
(module
  (func (export "spin") (result i32)
    (loop $again
      (br $again))
    (i32.const 0))

  (func (export "answer") (result i32)
    (i32.const 42)))
