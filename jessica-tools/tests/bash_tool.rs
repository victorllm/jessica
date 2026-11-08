//! Behaviour tests for the bash tool.

use std::time::Duration;

use jessica_core::tool::Tool;
use jessica_tools::BashTool;
use serde_json::json;

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
