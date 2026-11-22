//! Behaviour tests for the bash tool.

use std::time::Duration;

use jessica_core::tool::Tool;
use jessica_tools::BashTool;
use serde_json::json;

/// Returns an existing host variable outside the built-in allowlist that is
/// safe to pass through, when the test environment has one.
fn non_default_safe_variable() -> Option<String> {
    std::env::vars_os().find_map(|(name, _)| {
        let name = name.to_str()?.to_owned();
        let is_default = ["PATH", "HOME", "LANG", "TERM", "TMPDIR"]
            .iter()
            .any(|allowed| *allowed == name);
        let upper = name.to_ascii_uppercase();
        let sensitive = [
            "KEY",
            "TOKEN",
            "SECRET",
            "PASSWORD",
            "PASSWD",
            "CREDENTIAL",
            "AUTH",
        ]
        .iter()
        .any(|marker| upper.contains(marker))
            || ["OPENAI_", "ANTHROPIC_", "JESSICA_"]
                .iter()
                .any(|prefix| upper.starts_with(prefix));
        (!is_default && !sensitive && !name.contains('=')).then_some(name)
    })
}

fn tool() -> BashTool {
    BashTool::with_limits(Duration::from_secs(5), 1024)
}

#[tokio::test]
async fn bash_returns_stdout_and_success() {
    let result = tool().execute(json!({"command": "printf hello"})).await;

    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert!(result.content.contains("exit code: 0"));
    assert!(result.content.contains("hello"));
}

#[tokio::test]
async fn bash_reports_nonzero_exit_and_stderr() {
    let result = tool()
        .execute(json!({"command": "printf out; printf err >&2; exit 3"}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("exit code: 3"));
    assert!(result.content.contains("out"));
    assert!(result.content.contains("err"));
}

#[tokio::test]
async fn bash_times_out_and_kills_the_command() {
    let tool = BashTool::with_limits(Duration::from_millis(100), 1024);

    let result = tool.execute(json!({"command": "exec sleep 5"})).await;

    assert!(result.is_error);
    assert!(result.content.contains("timed out"));
}

#[tokio::test]
async fn bash_truncates_output_at_the_configured_limit() {
    let tool = BashTool::with_limits(Duration::from_secs(5), 16);

    let result = tool
        .execute(json!({"command": "printf 'abcdefghijklmnopqrstuvwxyz'"}))
        .await;

    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert!(result.content.contains("abcdefghijklmnop"));
    assert!(!result.content.contains("qrstuvwxyz"));
    assert!(result.content.contains("truncated at 16 bytes"));
}

#[tokio::test]
async fn bash_clears_the_environment_and_passes_only_the_allowlist() {
    let extension = non_default_safe_variable();
    let mut tool = BashTool::with_limits(Duration::from_secs(5), 64 * 1024);
    if let Some(name) = &extension {
        tool = tool.with_env_allowlist([name.clone()]);
    }

    let result = tool.execute(json!({"command": "env"})).await;

    assert!(!result.is_error, "unexpected failure: {}", result.content);
    assert!(
        result.content.contains("PATH="),
        "PATH is a default allowlist entry and must be present: {}",
        result.content
    );
    if let Some(name) = extension {
        assert!(
            result.content.contains(&format!("{name}=")),
            "configured extension `{name}` must be present: {}",
            result.content
        );
    }
    for (name, _) in std::env::vars_os() {
        let Some(name) = name.to_str() else {
            continue;
        };
        let upper = name.to_ascii_uppercase();
        let sensitive = upper.starts_with("OPENAI_")
            || upper.starts_with("ANTHROPIC_")
            || upper.starts_with("JESSICA_")
            || [
                "KEY",
                "TOKEN",
                "SECRET",
                "PASSWORD",
                "PASSWD",
                "CREDENTIAL",
                "AUTH",
            ]
            .iter()
            .any(|marker| upper.contains(marker));
        if sensitive {
            assert!(
                !result
                    .content
                    .lines()
                    .any(|line| line.starts_with(&format!("{name}="))),
                "sensitive variable `{name}` must never be inherited"
            );
        }
    }
}

#[tokio::test]
async fn bash_caps_the_rendered_result_and_reports_the_original_size() {
    let tool = BashTool::with_limits(Duration::from_secs(5), 4096).with_content_limit(128);

    let result = tool
        .execute(json!({
            "command": "i=0; while [ $i -lt 200 ]; do printf 'abcdefghij'; i=$((i+1)); done"
        }))
        .await;

    assert!(!result.is_error, "unexpected failure: {}", result.content);
    assert!(
        result.content.len() <= 128,
        "the rendered result must respect the cap: {} bytes",
        result.content.len()
    );
    assert!(
        result.content.contains("truncated"),
        "truncation must be observable: {}",
        result.content
    );
    assert!(
        result.content.contains("of "),
        "the notice must record the original size: {}",
        result.content
    );
}

#[tokio::test]
async fn bash_rejects_invalid_arguments() {
    let result = tool().execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("invalid bash arguments"));
}

#[test]
fn bash_schema_requires_a_command() {
    let schema = BashTool::new().schema();

    assert_eq!(schema["function"]["name"], "bash");
    assert_eq!(
        schema["function"]["parameters"]["required"],
        json!(["command"])
    );
}
