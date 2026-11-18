//! Behavior tests for the `--version` / `-V` command line flags.

use std::process::Command;

/// The package inherits the workspace version, so this is the single source.
fn expected_version() -> String {
    format!("jessica {}\n", env!("CARGO_PKG_VERSION"))
}

#[test]
fn version_flags_print_the_workspace_version_and_exit_zero() {
    for flag in ["--version", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_jessica"))
            .arg(flag)
            // Version reporting must work before any provider is assembled, so
            // deliberately invalid provider settings must not affect it.
            .env("JESSICA_PROVIDER", "not-a-provider")
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .output()
            .expect("jessica must run");

        assert_eq!(output.status.code(), Some(0), "{flag} must exit 0");
        assert_eq!(
            String::from_utf8(output.stdout).expect("stdout must be UTF-8"),
            expected_version(),
            "{flag} must print the workspace version"
        );
        assert!(
            output.stderr.is_empty(),
            "{flag} must not write to stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
