//! End-to-end tests for the side-effect-free `jessica tools` commands.
//!
//! The commands must work without a provider, event store, memory or skill
//! database, and must not create any runtime state. Output must be sorted and
//! reproducible so two runs over the same configuration can be compared.

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
            "jessica-tools-cli-{label}-{}-{unique}",
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

/// Runs `jessica --config <config> tools <args>` inside `workspace`.
///
/// `JESSICA_PROVIDER` is deliberately invalid: catalog inspection must never
/// consult provider selection.
fn run_tools(workspace: &Path, config: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .arg("--config")
        .arg(config)
        .arg("tools")
        .args(args)
        .env("JESSICA_PROVIDER", "definitely-not-a-provider")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL")
        .output()
        .expect("jessica tools should run")
}

/// Returns the output line describing `name`.
fn tool_line<'a>(stdout: &'a str, name: &str) -> &'a str {
    stdout
        .lines()
        .find(|line| line.split_whitespace().next() == Some(name))
        .unwrap_or_else(|| panic!("missing `{name}` line in:\n{stdout}"))
}

/// Returns the digest column from a `tools list` or `tools list --all` line.
fn digest_in(line: &str) -> &str {
    line.split_whitespace()
        .last()
        .filter(|token| token.starts_with("sha256:"))
        .unwrap_or_else(|| panic!("missing schema digest in: {line}"))
}

#[test]
fn tools_list_is_sorted_reproducible_and_creates_no_runtime_state() {
    let temp = TempDir::new("list");
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("jessica-cli must live inside the workspace")
        .join("examples/plugins/echo/manifest.json");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        format!(
            "[events]\npath = \"events.db\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"echo_json\"]\n\
             [wasm]\nmanifests = [\"{}\"]\n",
            manifest.display()
        ),
    )
    .expect("configuration should be written");

    let first = run_tools(temp.path(), &config, &["list"]);
    assert!(
        first.status.success(),
        "tools list must succeed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let stdout = String::from_utf8_lossy(&first.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        4,
        "expected a header, two exposed tools, and a summary:\n{stdout}"
    );
    assert!(lines[0].starts_with("NAME"));
    assert!(lines[1].starts_with("echo_json"));
    assert!(lines[2].starts_with("read_file"));
    assert!(
        digest_in(lines[1]).starts_with("sha256:"),
        "wasm digest: {stdout}"
    );
    assert!(
        lines[1].contains("wasm"),
        "the source column must identify the wasm plugin: {stdout}"
    );
    assert!(
        lines[2].contains("native"),
        "the source column must identify the native tool: {stdout}"
    );
    assert!(
        lines[3].starts_with("exposed 2/10 tools;"),
        "the summary must count hidden tools: {stdout}"
    );

    let second = run_tools(temp.path(), &config, &["list"]);
    assert!(second.status.success());
    assert_eq!(
        first.stdout, second.stdout,
        "the catalog rendering must be reproducible byte for byte"
    );

    assert!(
        !temp.path().join("events.db").exists() && !temp.path().join(".jessica").exists(),
        "tools list must not create the event store or its directory"
    );
}

#[test]
fn tools_list_all_and_inspect_explain_every_hidden_reason() {
    let temp = TempDir::new("inspect");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[policy]\nallowed_tools = [\"read_file\", \"bash\"]\n\
         [tools]\nprofile = \"readonly\"\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("configuration should be written");

    let exposed = run_tools(temp.path(), &config, &["list"]);
    assert!(exposed.status.success());
    let exposed_stdout = String::from_utf8_lossy(&exposed.stdout);
    assert_eq!(
        exposed_stdout
            .lines()
            .filter(|line| line.starts_with(' '))
            .count(),
        0,
        "the list must contain only table rows: {exposed_stdout}"
    );
    assert!(
        exposed_stdout
            .lines()
            .any(|line| line.split_whitespace().next() == Some("read_file")),
        "the profile-exposed tool must be listed: {exposed_stdout}"
    );
    assert!(
        !exposed_stdout
            .lines()
            .any(|line| line.split_whitespace().next() == Some("bash")),
        "a hidden tool must be omitted from the default list: {exposed_stdout}"
    );

    let all = run_tools(temp.path(), &config, &["list", "--all"]);
    assert!(all.status.success());
    let all_stdout = String::from_utf8_lossy(&all.stdout);
    let read_line = tool_line(&all_stdout, "read_file");
    assert!(
        read_line.contains("yes"),
        "read_file must be exposed: {read_line}"
    );
    let bash_line = tool_line(&all_stdout, "bash");
    assert!(
        bash_line.contains("no") && bash_line.contains("not in tools.profile"),
        "bash must explain its profile exclusion: {bash_line}"
    );
    let write_line = tool_line(&all_stdout, "write_file");
    assert!(
        write_line.contains("not in policy.allowed_tools")
            && write_line.contains("not in tools.profile"),
        "write_file must report both reasons: {write_line}"
    );
    let exec_line = tool_line(&all_stdout, "exec");
    assert!(
        exec_line.contains("not in policy.allowed_tools")
            && exec_line.contains("not in tools.profile"),
        "exec must explain both gates: {exec_line}"
    );

    let inspect = run_tools(temp.path(), &config, &["inspect", "write_file"]);
    assert!(inspect.status.success());
    let inspected = String::from_utf8_lossy(&inspect.stdout);
    assert!(inspected.contains("name: write_file"));
    assert!(inspected.contains("source: native"));
    assert!(inspected.contains("exposed: no"));
    assert!(inspected.contains("hidden: not in policy.allowed_tools, not in tools.profile"));
    assert!(inspected.contains("approval: interactive"));
    assert!(inspected.contains("path_field: path"));
    assert!(
        inspected.contains(&format!("schema_digest: {}", digest_in(write_line))),
        "inspect and list must report the same digest:\n{inspected}"
    );

    let exec_inspect = run_tools(temp.path(), &config, &["inspect", "exec"]);
    assert!(exec_inspect.status.success());
    let exec_inspected = String::from_utf8_lossy(&exec_inspect.stdout);
    assert!(exec_inspected.contains("exposed: no"));
    assert!(exec_inspected.contains("approval: interactive"));
    assert!(exec_inspected.contains("result_format: json"));
    assert!(exec_inspected.contains("executes: yes"));
    assert!(exec_inspected.contains("command_field: program"));
    assert!(
        exec_inspected.contains(&format!("schema_digest: {}", digest_in(exec_line))),
        "exec inspect and list must report the same digest:\n{exec_inspected}"
    );

    let list_dir_inspect = run_tools(temp.path(), &config, &["inspect", "list_dir"]);
    assert!(list_dir_inspect.status.success());
    let list_dir_inspected = String::from_utf8_lossy(&list_dir_inspect.stdout);
    assert!(list_dir_inspected.contains("result_format: text"));
    assert!(list_dir_inspected.contains("path_field: path"));
    assert!(list_dir_inspected.contains("max_output_lines: 500"));

    let unknown = run_tools(temp.path(), &config, &["inspect", "not_a_tool"]);
    assert!(!unknown.status.success(), "an unknown tool must fail");
    let stderr = String::from_utf8_lossy(&unknown.stderr);
    assert!(
        stderr.contains("not_a_tool") && stderr.contains("not present in the configured catalog"),
        "the error must identify the missing tool: {stderr}"
    );
}

#[test]
fn an_over_budget_catalog_is_still_inspectable() {
    let temp = TempDir::new("budget");
    let config = temp.path().join("harness.toml");
    std::fs::write(&config, "[tools]\nmax_exposed_tools = 1\n")
        .expect("configuration should be written");

    let listed = run_tools(temp.path(), &config, &["list"]);
    assert!(
        listed.status.success(),
        "inspection must survive a misconfigured budget"
    );
    let stdout = String::from_utf8_lossy(&listed.stdout);
    let stderr = String::from_utf8_lossy(&listed.stderr);
    assert!(
        stdout.starts_with("NAME") && stdout.contains("exposed 2/9 tools;"),
        "every enabled configured tool must still be shown: {stdout}"
    );
    assert!(
        stderr.contains("warning") && stderr.contains("tools.max_exposed_tools"),
        "the over-budget catalog must warn on stderr: {stderr}"
    );

    let inspected = run_tools(temp.path(), &config, &["inspect", "bash"]);
    assert!(
        inspected.status.success(),
        "inspect must not be blocked by the budget: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
}

#[test]
fn disabled_bash_is_omitted_from_listing_and_explained_on_demand() {
    const HIDDEN_REASON: &str = "disabled; set [tools.bash].enabled = true";

    let temp = TempDir::new("bash-disabled");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[policy]\nallowed_tools = [\"bash\", \"read_file\"]\n",
    )
    .expect("configuration should be written");

    let listed = run_tools(temp.path(), &config, &["list"]);
    assert!(
        listed.status.success(),
        "tools list must succeed: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let stdout = String::from_utf8_lossy(&listed.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.split_whitespace().next() == Some("read_file")),
        "the allowed read_file tool must be listed: {stdout}"
    );
    assert!(
        !stdout
            .lines()
            .any(|line| line.split_whitespace().next() == Some("bash")),
        "allowlisting bash is not enough while it is disabled: {stdout}"
    );
    assert!(
        stdout.contains("exposed 1/9 tools;"),
        "the summary must count the hidden bash tool: {stdout}"
    );

    let all = run_tools(temp.path(), &config, &["list", "--all"]);
    assert!(all.status.success());
    let all_stdout = String::from_utf8_lossy(&all.stdout);
    let bash_line = tool_line(&all_stdout, "bash");
    assert!(
        bash_line.contains("no") && bash_line.contains(HIDDEN_REASON),
        "list --all must explain how to enable bash: {bash_line}"
    );
    assert!(
        !bash_line.contains("not in policy.allowed_tools"),
        "the allowlist did admit bash; only the enable gate hid it: {bash_line}"
    );

    let inspected = run_tools(temp.path(), &config, &["inspect", "bash"]);
    assert!(
        inspected.status.success(),
        "a hidden bash tool must remain inspectable: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let inspected = String::from_utf8_lossy(&inspected.stdout);
    assert!(inspected.contains("exposed: no"));
    assert!(inspected.contains(&format!("hidden: {HIDDEN_REASON}")));

    let enabled_config = temp.path().join("enabled.toml");
    std::fs::write(
        &enabled_config,
        "[policy]\nallowed_tools = [\"bash\", \"read_file\"]\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("enabled configuration should be written");
    let enabled = run_tools(temp.path(), &enabled_config, &["list"]);
    assert!(
        enabled.status.success(),
        "tools list must succeed: {}",
        String::from_utf8_lossy(&enabled.stderr)
    );
    let enabled_stdout = String::from_utf8_lossy(&enabled.stdout);
    assert!(
        enabled_stdout
            .lines()
            .any(|line| line.split_whitespace().next() == Some("bash")),
        "explicitly enabling bash must expose it: {enabled_stdout}"
    );
}
