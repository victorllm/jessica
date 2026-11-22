//! End-to-end tests for `jessica policy simulate`.
//!
//! Simulation loads the same catalog as `jessica tools` and evaluates one
//! synthetic call. It must never construct a provider, event store, memory or
//! skill database, MCP session, or execute the tool.

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
            "jessica-policy-cli-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary workspace should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `jessica --config <config> policy simulate --tool <tool> --args <args>`
/// inside `workspace`.
///
/// `JESSICA_PROVIDER` is deliberately invalid: simulation must never consult
/// provider selection.
fn run_policy(workspace: &Path, config: &Path, tool: &str, args: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .arg("--config")
        .arg(config)
        .arg("policy")
        .arg("simulate")
        .arg("--tool")
        .arg(tool)
        .arg("--args")
        .arg(args)
        .env("JESSICA_PROVIDER", "definitely-not-a-provider")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL")
        .output()
        .expect("jessica policy simulate should run")
}

/// Returns the value of one `key: value` output line.
fn field<'a>(stdout: &'a str, key: &str) -> &'a str {
    let prefix = format!("{key}: ");
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("missing `{key}` line in:\n{stdout}"))
}

/// Asserts that the command created none of the configured runtime stores.
fn assert_no_runtime_state(workspace: &Path) {
    for path in [
        "events.db",
        "memory.db",
        "skills.db",
        ".jessica",
        "artifacts",
    ] {
        assert!(
            !workspace.join(path).exists(),
            "policy simulate must not create `{path}`"
        );
    }
}

#[test]
fn policy_simulate_reports_the_matched_deny_rule_without_runtime_state() {
    let temp = TempDir::new("deny");
    std::fs::create_dir_all(temp.path().join("secrets")).expect("secrets dir");
    std::fs::write(temp.path().join("secrets/token.txt"), "classified").expect("secret file");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[events]\npath = \"events.db\"\n\
         [memory]\nenabled = true\npath = \"memory.db\"\n\
         [skills]\nenabled = true\npath = \"skills.db\"\n\
         [policy]\nallowed_tools = [\"read_file\"]\n\
         [[policy.rules]]\n\
         id = \"deny-secrets\"\n\
         tool = \"read_file\"\n\
         path_glob = \"secrets/**\"\n\
         action = \"deny\"\n\
         reason = \"classified material stays unread\"\n",
    )
    .expect("configuration should be written");

    let output = run_policy(
        temp.path(),
        &config,
        "read_file",
        r#"{"path":"secrets/token.txt"}"#,
    );

    assert!(
        output.status.success(),
        "simulation must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(field(&stdout, "tool"), "read_file");
    assert_eq!(field(&stdout, "source"), "native");
    assert_eq!(field(&stdout, "decision"), "deny");
    assert_eq!(field(&stdout, "rule"), "deny-secrets");
    assert_eq!(field(&stdout, "approval"), "denied");
    assert_eq!(field(&stdout, "reason"), "classified material stays unread");
    assert_no_runtime_state(temp.path());
    assert_eq!(
        std::fs::read_to_string(temp.path().join("secrets/token.txt"))
            .expect("secret file must still exist"),
        "classified",
        "simulation must not touch the evaluated target"
    );
}

#[test]
fn policy_simulate_allows_a_write_rule_without_executing_the_write() {
    let temp = TempDir::new("write");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[events]\npath = \"events.db\"\n\
         [policy]\nallowed_tools = [\"write_file\"]\n\
         [[policy.rules]]\n\
         id = \"allow-generated\"\n\
         tool = \"write_file\"\n\
         path_glob = \"out/**\"\n\
         action = \"allow\"\n\
         reason = \"generated output is permitted\"\n",
    )
    .expect("configuration should be written");

    let output = run_policy(
        temp.path(),
        &config,
        "write_file",
        r#"{"path":"out/created.txt","content":"must not be written"}"#,
    );

    assert!(
        output.status.success(),
        "simulation must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(field(&stdout, "decision"), "allow");
    assert_eq!(field(&stdout, "rule"), "allow-generated");
    assert_eq!(field(&stdout, "approval"), "not_required");
    assert!(
        !temp.path().join("out/created.txt").exists(),
        "simulation must never execute the write"
    );
    assert_no_runtime_state(temp.path());
}

#[test]
fn policy_simulate_rejects_invalid_json_unknown_and_hidden_tools() {
    let temp = TempDir::new("errors");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[events]\npath = \"events.db\"\n[policy]\nallowed_tools = [\"read_file\"]\n",
    )
    .expect("configuration should be written");

    let invalid_json = run_policy(temp.path(), &config, "read_file", "{not json");
    assert!(!invalid_json.status.success());
    let stderr = String::from_utf8_lossy(&invalid_json.stderr);
    assert!(
        stderr.contains("invalid JSON arguments"),
        "invalid JSON must be a structured error: {stderr}"
    );

    let unknown = run_policy(temp.path(), &config, "no_such_tool", "{}");
    assert!(!unknown.status.success());
    let stderr = String::from_utf8_lossy(&unknown.stderr);
    assert!(
        stderr.contains("not present in the configured catalog"),
        "an unknown tool must be rejected explicitly: {stderr}"
    );

    let hidden = run_policy(temp.path(), &config, "write_file", r#"{"path":"x"}"#);
    assert!(!hidden.status.success());
    let stderr = String::from_utf8_lossy(&hidden.stderr);
    assert!(
        stderr.contains("not exposed") && stderr.contains("not in policy.allowed_tools"),
        "a hidden tool must explain its configuration reason: {stderr}"
    );

    let non_object = run_policy(temp.path(), &config, "read_file", "[]");
    assert!(!non_object.status.success());
    let stderr = String::from_utf8_lossy(&non_object.stderr);
    assert!(
        stderr.contains("arguments must be a JSON object"),
        "a non-object argument payload must be rejected: {stderr}"
    );

    assert_no_runtime_state(temp.path());
}

#[cfg(unix)]
#[test]
fn policy_simulate_reports_a_symlink_escape_as_a_structured_error() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new("escape");
    let outside = TempDir::new("escape-outside");
    std::fs::write(outside.path().join("secret.txt"), "outside").expect("outside file");
    symlink(outside.path(), temp.path().join("link")).expect("symlink must be created");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[events]\npath = \"events.db\"\n\
         [policy]\nallowed_tools = [\"read_file\"]\n\
         [[policy.rules]]\n\
         id = \"protect-secrets\"\n\
         tool = \"read_file\"\n\
         path_glob = \"**/secret.txt\"\n\
         action = \"deny\"\n",
    )
    .expect("configuration should be written");

    let output = run_policy(
        temp.path(),
        &config,
        "read_file",
        r#"{"path":"link/secret.txt"}"#,
    );

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("escapes the workspace root"),
        "the symlink escape must be reported structurally: {stderr}"
    );
    assert_no_runtime_state(temp.path());
}

#[test]
fn policy_simulate_matches_exec_program_and_argument_prefixes() {
    let temp = TempDir::new("exec");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[events]\npath = \"events.db\"\n\
         [policy]\nallowed_tools = [\"exec\"]\n\
         [approval]\nmode = \"ask\"\n\
         [[policy.rules]]\n\
         id = \"no-sudo\"\n\
         tool = \"exec\"\n\
         program = \"sudo\"\n\
         action = \"deny\"\n\
         reason = \"sudo is forbidden\"\n\
         [[policy.rules]]\n\
         id = \"ask-push\"\n\
         tool = \"exec\"\n\
         program = \"git\"\n\
         args_prefix = [\"push\"]\n\
         action = \"ask\"\n\
         reason = \"pushing needs approval\"\n",
    )
    .expect("configuration should be written");

    let denied = run_policy(
        temp.path(),
        &config,
        "exec",
        r#"{"program":"sudo","args":["ls"],"cwd":"."}"#,
    );
    assert!(
        denied.status.success(),
        "simulation must succeed: {}",
        String::from_utf8_lossy(&denied.stderr)
    );
    let stdout = String::from_utf8_lossy(&denied.stdout);
    assert_eq!(field(&stdout, "decision"), "deny");
    assert_eq!(field(&stdout, "rule"), "no-sudo");
    assert_eq!(field(&stdout, "reason"), "sudo is forbidden");

    let asked = run_policy(
        temp.path(),
        &config,
        "exec",
        r#"{"program":"git","args":["push","origin"],"cwd":"."}"#,
    );
    assert!(asked.status.success());
    let stdout = String::from_utf8_lossy(&asked.stdout);
    assert_eq!(field(&stdout, "decision"), "ask");
    assert_eq!(field(&stdout, "rule"), "ask-push");
    assert_eq!(field(&stdout, "approval"), "ask");

    let fallback = run_policy(
        temp.path(),
        &config,
        "exec",
        r#"{"program":"git","args":["status"],"cwd":"."}"#,
    );
    assert!(fallback.status.success());
    let stdout = String::from_utf8_lossy(&fallback.stdout);
    assert_eq!(
        field(&stdout, "decision"),
        "ask",
        "an explicit approval mode asks for unmatched sensitive tools"
    );
    assert_eq!(field(&stdout, "rule"), "-");
    assert_eq!(field(&stdout, "approval"), "ask");

    assert_no_runtime_state(temp.path());
}

#[test]
fn policy_simulate_rejects_invalid_rule_configuration_before_evaluating() {
    let temp = TempDir::new("bad-config");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[policy]\nallowed_tools = [\"read_file\"]\n\
         [[policy.rules]]\n\
         id = \"duplicate\"\n\
         tool = \"read_file\"\n\
         action = \"deny\"\n\
         [[policy.rules]]\n\
         id = \"duplicate\"\n\
         tool = \"read_file\"\n\
         action = \"allow\"\n",
    )
    .expect("configuration should be written");

    let output = run_policy(temp.path(), &config, "read_file", r#"{"path":"a"}"#);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("policy.rules[1].id") && stderr.contains("declared more than once"),
        "duplicate ids must fail before evaluation: {stderr}"
    );
    assert_no_runtime_state(temp.path());
}

#[test]
fn editor_paths_are_protected_by_default_and_an_allow_rule_can_override() {
    let temp = TempDir::new("editor-protected");
    std::fs::write(temp.path().join(".env"), "SECRET=value\n").expect(".env should exist");
    let denied_config = temp.path().join("denied.toml");
    std::fs::write(
        &denied_config,
        "[policy]\nallowed_tools = [\"edit_file\", \"apply_patch\"]\n",
    )
    .expect("configuration should be written");

    let edit_args =
        serde_json::json!({"path": ".env", "old": "SECRET", "new": "PUBLIC"}).to_string();
    let edit = run_policy(temp.path(), &denied_config, "edit_file", &edit_args);
    assert!(edit.status.success());
    let stdout = String::from_utf8_lossy(&edit.stdout);
    assert_eq!(field(&stdout, "decision"), "deny");
    assert_eq!(field(&stdout, "rule"), "-");
    assert!(
        field(&stdout, "reason").contains("protected path"),
        "the reason must explain the default protection: {stdout}"
    );

    let patch_args = serde_json::json!({
        "patch": "--- a/.env\n+++ b/.env\n@@ -1 +1 @@\n-SECRET=value\n+PUBLIC=value\n"
    })
    .to_string();
    let patch = run_policy(temp.path(), &denied_config, "apply_patch", &patch_args);
    assert!(patch.status.success());
    let stdout = String::from_utf8_lossy(&patch.stdout);
    assert_eq!(field(&stdout, "decision"), "deny");
    assert_eq!(field(&stdout, "rule"), "-");
    assert!(field(&stdout, "reason").contains("protected path"));

    let allowed_config = temp.path().join("allowed.toml");
    std::fs::write(
        &allowed_config,
        "[policy]\nallowed_tools = [\"edit_file\", \"apply_patch\"]\n\
         [[policy.rules]]\n\
         id = \"allow-env\"\n\
         tool = [\"edit_file\", \"apply_patch\"]\n\
         path_glob = \".env\"\n\
         action = \"allow\"\n\
         reason = \"the operator explicitly allows environment edits\"\n",
    )
    .expect("configuration should be written");

    let edit = run_policy(temp.path(), &allowed_config, "edit_file", &edit_args);
    assert!(edit.status.success());
    let stdout = String::from_utf8_lossy(&edit.stdout);
    assert_eq!(field(&stdout, "decision"), "allow");
    assert_eq!(field(&stdout, "rule"), "allow-env");

    let patch = run_policy(temp.path(), &allowed_config, "apply_patch", &patch_args);
    assert!(patch.status.success());
    let stdout = String::from_utf8_lossy(&patch.stdout);
    assert_eq!(field(&stdout, "decision"), "allow");
    assert_eq!(field(&stdout, "rule"), "allow-env");

    assert_eq!(
        std::fs::read_to_string(temp.path().join(".env")).expect(".env must still exist"),
        "SECRET=value\n",
        "simulation must not modify the protected target"
    );
    assert_no_runtime_state(temp.path());
}
