//! Loopback HTTP tests for the classified provider failures added in Task
//! 5.4.1. These tests drive the real reqwest path and verify that status
//! classification is shared by both providers.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use jessica_core::llm::{LlmError, LlmProvider};
use jessica_core::types::Message;
use jessica_providers::{
    AnthropicConfig, AnthropicProvider, ClassifiedProvider, OpenAiConfig, OpenAiProvider,
    ProviderFailure, ProviderFailureKind,
};

/// Serves one HTTP response over loopback and returns the base URL.
fn serve_once(
    status: &'static str,
    body: &'static str,
    extra_headers: &'static str,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("address should be known");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("client should connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout should be set");

        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => request.extend_from_slice(&buffer[..read]),
                Err(error) => panic!("failed to read request: {error}"),
            }
        }

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("response should be written");
        stream.flush().expect("response should flush");
    });
    (format!("http://{address}"), handle)
}

fn openai_provider(base_url: String) -> OpenAiProvider {
    let config =
        OpenAiConfig::new("test-key", base_url, "test-model").expect("config should be valid");
    OpenAiProvider::new(config).expect("provider should be constructed")
}

fn anthropic_provider(base_url: String) -> AnthropicProvider {
    let config =
        AnthropicConfig::new("test-key", base_url, "claude-test").expect("config should be valid");
    AnthropicProvider::new(config).expect("provider should be constructed")
}

async fn openai_failure(status: &'static str, extra_headers: &'static str) -> ProviderFailure {
    let (base_url, server) = serve_once(status, "rejected", extra_headers);
    let failure = match openai_provider(base_url)
        .stream_chat_classified(&[Message::user("hello")], &[])
        .await
    {
        Err(failure) => failure,
        Ok(_) => panic!("expected HTTP {status} to be classified as a failure"),
    };
    server.join().expect("server should finish");
    failure
}

async fn anthropic_failure(status: &'static str, extra_headers: &'static str) -> ProviderFailure {
    let (base_url, server) = serve_once(status, "rejected", extra_headers);
    let failure = match anthropic_provider(base_url)
        .stream_chat_classified(&[Message::user("hello")], &[])
        .await
    {
        Err(failure) => failure,
        Ok(_) => panic!("expected HTTP {status} to be classified as a failure"),
    };
    server.join().expect("server should finish");
    failure
}

#[tokio::test]
async fn openai_http_statuses_are_classified() {
    let cases = [
        ("400 Bad Request", ProviderFailureKind::Client, false),
        ("401 Unauthorized", ProviderFailureKind::Client, false),
        ("403 Forbidden", ProviderFailureKind::Client, false),
        ("404 Not Found", ProviderFailureKind::Client, false),
        ("408 Request Timeout", ProviderFailureKind::Client, true),
        ("409 Conflict", ProviderFailureKind::Client, false),
        (
            "422 Unprocessable Entity",
            ProviderFailureKind::Client,
            false,
        ),
        ("425 Too Early", ProviderFailureKind::Client, true),
        (
            "429 Too Many Requests",
            ProviderFailureKind::RateLimited,
            true,
        ),
        (
            "500 Internal Server Error",
            ProviderFailureKind::Server,
            true,
        ),
        ("502 Bad Gateway", ProviderFailureKind::Server, true),
        ("503 Service Unavailable", ProviderFailureKind::Server, true),
        ("504 Gateway Timeout", ProviderFailureKind::Server, true),
    ];

    for (status, kind, retryable) in cases {
        let failure = openai_failure(status, "").await;
        assert_eq!(failure.kind(), kind, "{status}: {failure:?}");
        assert_eq!(failure.is_retryable(), retryable, "{status}: {failure:?}");
        assert!(failure.message().contains("HTTP"));
    }
}

#[tokio::test]
async fn anthropic_http_statuses_are_classified() {
    let cases = [
        ("400 Bad Request", ProviderFailureKind::Client, false),
        ("401 Unauthorized", ProviderFailureKind::Client, false),
        ("403 Forbidden", ProviderFailureKind::Client, false),
        ("404 Not Found", ProviderFailureKind::Client, false),
        ("408 Request Timeout", ProviderFailureKind::Client, true),
        ("409 Conflict", ProviderFailureKind::Client, false),
        (
            "422 Unprocessable Entity",
            ProviderFailureKind::Client,
            false,
        ),
        ("425 Too Early", ProviderFailureKind::Client, true),
        (
            "429 Too Many Requests",
            ProviderFailureKind::RateLimited,
            true,
        ),
        (
            "500 Internal Server Error",
            ProviderFailureKind::Server,
            true,
        ),
        ("502 Bad Gateway", ProviderFailureKind::Server, true),
        ("503 Service Unavailable", ProviderFailureKind::Server, true),
        ("504 Gateway Timeout", ProviderFailureKind::Server, true),
    ];

    for (status, kind, retryable) in cases {
        let failure = anthropic_failure(status, "").await;
        assert_eq!(failure.kind(), kind, "{status}: {failure:?}");
        assert_eq!(failure.is_retryable(), retryable, "{status}: {failure:?}");
        assert!(failure.message().contains("Anthropic API returned HTTP"));
    }
}

#[tokio::test]
async fn retry_after_is_preserved_for_both_providers() {
    let openai = openai_failure("429 Too Many Requests", "Retry-After: 7\r\n").await;
    assert_eq!(openai.retry_after(), Some(Duration::from_secs(7)));

    let anthropic = anthropic_failure("429 Too Many Requests", "Retry-After: 3\r\n").await;
    assert_eq!(anthropic.retry_after(), Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn missing_retry_after_is_none() {
    let failure = openai_failure("429 Too Many Requests", "").await;
    assert_eq!(failure.retry_after(), None);
}

#[tokio::test]
async fn malformed_retry_after_is_ignored() {
    let openai = openai_failure("429 Too Many Requests", "Retry-After: soon\r\n").await;
    assert_eq!(openai.retry_after(), None);

    let anthropic = anthropic_failure("429 Too Many Requests", "Retry-After: -1\r\n").await;
    assert_eq!(anthropic.retry_after(), None);
}

#[tokio::test]
async fn connection_refused_is_a_transport_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("address should be known");
    drop(listener);

    let config = OpenAiConfig::new("test-key", format!("http://{address}"), "test-model")
        .expect("config should be valid");
    let provider = OpenAiProvider::new(config).expect("provider should be constructed");
    let Err(failure) = provider
        .stream_chat_classified(&[Message::user("hello")], &[])
        .await
    else {
        panic!("connection should fail");
    };

    assert_eq!(failure.kind(), ProviderFailureKind::Transport);
    assert!(failure.is_retryable());
}

#[tokio::test]
async fn legacy_openai_http_error_keeps_its_message() {
    let (base_url, server) = serve_once("500 Internal Server Error", "server exploded", "");
    let provider = openai_provider(base_url);
    let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
        panic!("HTTP 500 should be a setup error");
    };
    server.join().expect("server should finish");

    assert!(matches!(error, LlmError::Request { .. }));
    let message = error.to_string();
    assert!(message.contains("provider returned HTTP 500 Internal Server Error"));
    assert!(message.contains("server exploded"));
}
