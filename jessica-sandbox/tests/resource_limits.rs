//! Behavior tests for per-store fuel, wall-clock, memory, table, and instance
//! limits.

use std::time::Duration;

use jessica_sandbox::{SandboxError, SandboxPolicy, WasmEngine, WasmModule};

const SPIN_FIXTURE: &[u8] = include_bytes!("fixtures/spin.wasm");
const LIMITS_FIXTURE: &[u8] = include_bytes!("fixtures/limits.wasm");
const FIXED_FIXTURE: &[u8] = include_bytes!("fixtures/fixed_value.wasm");

fn module(bytes: &[u8]) -> WasmModule {
    WasmEngine::new()
        .expect("wasmtime engine must initialize")
        .load_bytes(bytes)
        .expect("fixture must compile")
}

#[test]
fn fuel_exhaustion_is_reported_and_the_host_survives() {
    let mut instance = module(SPIN_FIXTURE)
        .instantiate(
            &SandboxPolicy::default()
                .with_fuel(1_000_000)
                .with_wall_timeout(Duration::from_secs(30)),
        )
        .expect("module must instantiate");

    match instance.call_i32("spin") {
        Err(SandboxError::FuelExhausted { name }) => assert_eq!(name, "spin"),
        other => panic!("expected FuelExhausted, got {other:?}"),
    }
    assert_eq!(
        instance
            .call_i32("answer")
            .expect("a later call must get a fresh fuel budget"),
        42
    );
}

#[test]
fn wall_clock_deadline_interrupts_a_spinning_module() {
    let timeout = Duration::from_millis(100);
    let mut instance = module(SPIN_FIXTURE)
        .instantiate(
            &SandboxPolicy::default()
                .with_fuel(u64::MAX)
                .with_wall_timeout(timeout),
        )
        .expect("module must instantiate");

    match instance.call_i32("spin") {
        Err(SandboxError::DeadlineExceeded {
            name,
            timeout: reported,
        }) => {
            assert_eq!(name, "spin");
            assert_eq!(reported, timeout);
        }
        other => panic!("expected DeadlineExceeded, got {other:?}"),
    }
    assert_eq!(
        instance
            .call_i32("answer")
            .expect("a later call must get a fresh deadline"),
        42
    );
}

#[test]
fn memory_growth_beyond_the_limit_traps() {
    let mut instance = module(LIMITS_FIXTURE)
        .instantiate(&SandboxPolicy::default().with_memory_limit(128 * 1024))
        .expect("module must instantiate");

    match instance.call_i32("grow_memory") {
        Err(SandboxError::Trap { name, .. }) => assert_eq!(name, "grow_memory"),
        other => panic!("expected a trap from the memory limit, got {other:?}"),
    }
    assert_eq!(
        instance
            .call_i32("answer")
            .expect("the instance must remain usable"),
        42
    );
}

#[test]
fn table_growth_beyond_the_limit_traps() {
    let mut instance = module(LIMITS_FIXTURE)
        .instantiate(&SandboxPolicy::default().with_table_limit(100))
        .expect("module must instantiate");

    match instance.call_i32("grow_table") {
        Err(SandboxError::Trap { name, .. }) => assert_eq!(name, "grow_table"),
        other => panic!("expected a trap from the table limit, got {other:?}"),
    }
}

#[test]
fn growth_within_the_limits_succeeds() {
    let mut instance = module(LIMITS_FIXTURE)
        .instantiate(
            &SandboxPolicy::default()
                .with_memory_limit(128 * 1024 * 1024)
                .with_table_limit(10_000),
        )
        .expect("module must instantiate");

    // `memory.grow` and `table.grow` return the previous size; -1 means
    // failure. A successful grow therefore returns the initial sizes here.
    assert_eq!(
        instance
            .call_i32("grow_memory")
            .expect("growth within the memory limit must succeed"),
        1
    );
    assert_eq!(
        instance
            .call_i32("grow_table")
            .expect("growth within the table limit must succeed"),
        10
    );
}

#[test]
fn an_instance_limit_of_zero_rejects_instantiation() {
    match module(FIXED_FIXTURE).instantiate(&SandboxPolicy::default().with_instance_limit(0)) {
        Err(SandboxError::Instantiate(_)) => {}
        other => panic!("expected an Instantiate error, got {other:?}"),
    }
}
