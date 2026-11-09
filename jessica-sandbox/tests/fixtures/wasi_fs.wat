;; Test fixture: WASI preview 1 file access attempts.
;;
;; The exports exercise three cases:
;; - read_allowed: open and read `allowed.txt` through preopen fd 3 and check
;;   that the bytes are exactly "SECRET!\n"; returns 0 on success, the WASI
;;   errno on open/read failure, or 4096 when the content does not match.
;; - read_outside: attempt the same read through `../outside.txt`, which must
;;   not escape the preopened directory.
;; - write_new: attempt to create/truncate `new.txt` for writing; a read-only
;;   preopen must deny it.
;; - ok: a plain function used to prove the instance still works after a
;;   denied capability request.
;;
;; Rebuild wasi_fs.wasm with ./build.sh after editing this file.
(module
  (import "wasi_snapshot_preview1" "path_open"
    (func $path_open
      (param i32 i32 i32 i32 i32 i64 i64 i32 i32)
      (result i32)))
  (import "wasi_snapshot_preview1" "fd_read"
    (func $fd_read (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_close"
    (func $fd_close (param i32) (result i32)))

  (memory (export "memory") 1)

  ;; Opened fd at 8, byte count at 12.
  ;; Filenames: "allowed.txt" at 16, "../outside.txt" at 32, "new.txt" at 48.
  ;; The read iovec lives at 80 and its buffer at 128.
  (data (i32.const 16) "allowed.txt")
  (data (i32.const 32) "../outside.txt")
  (data (i32.const 48) "new.txt")

  ;; Reads `path_len` bytes at `path`, verifies the content, and returns:
  ;; 0 on success, a WASI errno on failure, 4096 on a content mismatch.
  (func $read_path (param $path i32) (param $path_len i32) (result i32)
    (local $errno i32)
    (local $fd i32)
    (i32.store (i32.const 8) (i32.const -1))
    (local.set $errno
      (call $path_open
        (i32.const 3)          ;; first preopen
        (i32.const 0)          ;; lookup flags
        (local.get $path)
        (local.get $path_len)
        (i32.const 0)          ;; no oflags
        (i64.const 2)          ;; rights_base = FD_READ
        (i64.const 0)          ;; rights_inheriting
        (i32.const 0)          ;; no fdflags
        (i32.const 8)))        ;; opened fd out
    (if (i32.ne (local.get $errno) (i32.const 0))
      (then (return (local.get $errno))))
    (local.set $fd (i32.load (i32.const 8)))
    (i32.store (i32.const 80) (i32.const 128))
    (i32.store (i32.const 84) (i32.const 8))
    (local.set $errno
      (call $fd_read
        (local.get $fd)
        (i32.const 80)         ;; iovec array
        (i32.const 1)          ;; one iovec
        (i32.const 12)))       ;; bytes read out
    (drop (call $fd_close (local.get $fd)))
    (if (i32.ne (local.get $errno) (i32.const 0))
      (then (return (local.get $errno))))
    ;; "SECRET!\n" as two little-endian i32 loads.
    (if (i32.and
          (i32.eq (i32.load (i32.const 128)) (i32.const 0x52434553))
          (i32.eq (i32.load (i32.const 132)) (i32.const 0x0a215445)))
      (then (return (i32.const 0)))
      (else (return (i32.const 4096))))
    (i32.const 4096))

  (func (export "read_allowed") (result i32)
    (call $read_path (i32.const 16) (i32.const 11)))

  (func (export "read_outside") (result i32)
    (call $read_path (i32.const 32) (i32.const 14)))

  (func (export "write_new") (result i32)
    (local $errno i32)
    (i32.store (i32.const 8) (i32.const -1))
    (local.set $errno
      (call $path_open
        (i32.const 3)
        (i32.const 0)
        (i32.const 48)
        (i32.const 7)
        (i32.const 9)          ;; oflags = CREAT | TRUNC
        (i64.const 64)         ;; rights_base = FD_WRITE
        (i64.const 0)
        (i32.const 0)
        (i32.const 8)))
    (if (i32.ne (local.get $errno) (i32.const 0))
      (then (return (local.get $errno))))
    (drop (call $fd_close (i32.load (i32.const 8))))
    (i32.const 0))

  (func (export "ok") (result i32)
    (i32.const 7)))
