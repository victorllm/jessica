//! End-to-end tests for the side-effect-free `jessica config` command.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

/// Temporary workspace removed recursively.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-config-cli-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary workspace should be created");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn config_prints_preset_resolution_without_creating_runtime_state() {
    let temp = TempDir::new("safe");
    let config = temp.path.join("harness.toml");
    std::fs::write(&config, "[profile]\npreset = \"safe\"\n")
        .expect("configuration should be written");

    let output = run_config(&temp.path, &config);
    assert!(
        output.status.success(),
        "config must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("config output must be JSON");

    assert_eq!(json["profile"]["preset"], "safe");
    assert_eq!(json["profile"]["trace"], "strict");
    assert_eq!(json["events"]["path"], ".jessica/events.db");
    assert_eq!(
        json["policy"]["allowed_tools"],
        serde_json::json!(["read_file"])
    );
    assert_eq!(json["memory"]["enabled"], false);
    assert_eq!(json["skills"]["enabled"], false);
    assert_eq!(json["reliability"]["enabled"], false);
    assert!(
        !temp.path.join(".jessica").exists(),
        "config inspection must not create the event store or its directory"
    );
}

#[test]
fn config_reports_trace_off_and_rejects_a_conflicting_explicit_path() {
    let temp = TempDir::new("trace-off");
    let config = temp.path.join("harness.toml");
    std::fs::write(&config, "[profile]\ntrace = \"off\"\n")
        .expect("configuration should be written");

    let output = run_config(&temp.path, &config);
    assert!(output.status.success());
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("config output must be JSON");
    assert_eq!(json["profile"]["trace"], "off");
    assert!(json["events"]["path"].is_null());

    std::fs::write(
        &config,
        "[profile]\ntrace = \"off\"\n[events]\npath = \"events.db\"\n",
    )
    .expect("conflicting configuration should be written");
    let output = run_config(&temp.path, &config);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "the conflict must be rejected");
    assert!(
        stderr.contains("profile.trace") && stderr.contains("events.path"),
        "the error must identify both sides of the conflict: {stderr}"
    );
}

/// Runs `jessica --config <config> config` inside `workspace`.
fn run_config(workspace: &Path, config: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .arg("--config")
        .arg(config)
        .arg("config")
        .env_remove("JESSICA_PROVIDER")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL")
        .output()
        .expect("jessica config should run")
}
