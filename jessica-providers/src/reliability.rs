//! Typed provider failures, timeouts, and the reliability wrapper.
//!
//! [`ClassifiedProvider`] is an internal extension of the frozen
//! [`jessica_core::llm::LlmProvider`] contract. It keeps the failure category
//! alongside the failure message so later retry and circuit-breaker layers do
//! not have to parse provider error strings.
//!
//! Task 5.4.1 implements timeouts only. Retry, circuit breaking, and rate
//! limiting are added by later tasks in the same module.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::ToolSchema;
use jessica_core::types::Message;
use thiserror::Error;

/// Maximum accepted timeout in milliseconds.
///
/// Values above this are rejected by [`ReliabilityConfig::validate`] so a
/// configuration typo cannot create an effectively unbounded timer.
pub const MAX_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;

/// Category of a provider failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailureKind {
    /// The connection failed, was reset, or otherwise did not complete.
    Transport,
    /// A request or stream idle timeout elapsed.
    Timeout,
    /// The provider rejected the request with HTTP 429.
    RateLimited,
    /// The provider returned an HTTP 5xx response.
    Server,
    /// The provider returned an HTTP 4xx response that is normally permanent.
    Client,
    /// A response body, SSE payload, or tool-call payload could not be decoded.
    InvalidResponse,
    /// The stream ended before its protocol terminator.
    StreamInterrupted,
}

/// A provider failure with a machine-readable category.
///
/// `retry_after` is populated from the HTTP `Retry-After` header when present.
/// `is_retryable` is true for transport, timeout, rate-limit, server, and
/// interrupted-stream failures; 408 and 425 are represented as retryable
/// [`ProviderFailureKind::Client`] failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ProviderFailure {
    kind: ProviderFailureKind,
    message: String,
    retry_after: Option<Duration>,
    retryable: bool,
}

impl ProviderFailure {
    /// Creates a transport failure.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::Transport, message, None, true)
    }

    /// Creates a timeout failure.
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::Timeout, message, None, true)
    }

    /// Creates a rate-limit failure.
    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::RateLimited, message, None, true)
    }

    /// Creates a server failure.
    pub fn server(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::Server, message, None, true)
    }

    /// Creates a permanently failing client failure.
    pub fn client(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::Client, message, None, false)
    }

    /// Creates a retryable client failure, used for HTTP 408 and 425.
    pub fn client_retryable(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::Client, message, None, true)
    }

    /// Creates an invalid-response failure.
    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::InvalidResponse, message, None, false)
    }

    /// Creates an interrupted-stream failure.
    pub fn stream_interrupted(message: impl Into<String>) -> Self {
        Self::new(ProviderFailureKind::StreamInterrupted, message, None, true)
    }

    /// Attaches a provider-suggested retry delay.
    pub fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }

    /// Returns the failure category.
    pub fn kind(&self) -> ProviderFailureKind {
        self.kind
    }

    /// Returns the human-readable failure message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the provider-suggested retry delay, when present.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Returns whether the failure may be retried before any content is
    /// committed.
    ///
    /// [`ProviderFailureKind::StreamInterrupted`] is marked retryable here;
    /// Task 5.4.2 additionally requires that no [`LlmEvent::TextDelta`] has
    /// been delivered before it retries.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    fn new(
        kind: ProviderFailureKind,
        message: impl Into<String>,
        retry_after: Option<Duration>,
        retryable: bool,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after,
            retryable,
        }
    }
}

/// Provider stream whose failures retain their category.
pub type ClassifiedStream = BoxStream<'static, Result<LlmEvent, ProviderFailure>>;

/// Provider contract that exposes typed failures.
///
/// This trait is implemented by the built-in providers and consumed by
/// [`ReliableProvider`]. It is deliberately separate from the frozen
/// [`jessica_core::llm::LlmProvider`] contract.
#[async_trait]
pub trait ClassifiedProvider: Send + Sync {
    /// Starts a streaming completion whose setup and stream failures are
    /// classified.
    async fn stream_chat_classified(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<ClassifiedStream, ProviderFailure>;
}

/// Timeout configuration for [`ReliableProvider`].
///
/// A zero timeout disables that limit. Later tasks extend this type with retry,
/// circuit-breaker, and rate-limit fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReliabilityConfig {
    /// Maximum time allowed for the provider request setup phase.
    pub request_timeout_ms: u64,
    /// Maximum time allowed between two provider stream events.
    pub stream_idle_timeout_ms: u64,
}

impl ReliabilityConfig {
    /// Validates all timeout values.
    pub fn validate(&self) -> Result<(), ReliabilityConfigError> {
        validate_timeout(
            "request_timeout_ms",
            self.request_timeout_ms,
            MAX_TIMEOUT_MS,
        )?;
        validate_timeout(
            "stream_idle_timeout_ms",
            self.stream_idle_timeout_ms,
            MAX_TIMEOUT_MS,
        )
    }

    /// Returns the request timeout when enabled.
    fn request_timeout(&self) -> Option<Duration> {
        timeout_duration(self.request_timeout_ms)
    }

    /// Returns the stream idle timeout when enabled.
    fn stream_idle_timeout(&self) -> Option<Duration> {
        timeout_duration(self.stream_idle_timeout_ms)
    }
}

/// Invalid reliability configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReliabilityConfigError {
    /// A timeout exceeds the accepted maximum.
    #[error("{field} = {value}ms exceeds the maximum of {max}ms")]
    TimeoutTooLarge {
        /// Configuration field name.
        field: &'static str,
        /// Configured value in milliseconds.
        value: u64,
        /// Maximum accepted value in milliseconds.
        max: u64,
    },
}

/// Wraps a [`ClassifiedProvider`] with request and stream-idle timeouts.
///
/// It implements [`ClassifiedProvider`] so callers can observe typed timeout
/// failures, and [`jessica_core::llm::LlmProvider`] so it can be used wherever
/// the frozen contract is expected.
pub struct ReliableProvider {
    inner: Arc<dyn ClassifiedProvider>,
    config: ReliabilityConfig,
}

impl ReliableProvider {
    /// Creates a reliability wrapper around `inner`.
    ///
    /// Returns an error when the timeout configuration is invalid.
    pub fn new(
        inner: Arc<dyn ClassifiedProvider>,
        config: ReliabilityConfig,
    ) -> Result<Self, ReliabilityConfigError> {
        config.validate()?;
        Ok(Self { inner, config })
    }

    /// Returns the timeout configuration in use.
    pub fn config(&self) -> ReliabilityConfig {
        self.config
    }

    /// Wraps a classified stream with an idle timeout.
    fn with_idle_timeout(&self, stream: ClassifiedStream) -> ClassifiedStream {
        let Some(limit) = self.config.stream_idle_timeout() else {
            return stream;
        };

        struct IdleState {
            stream: ClassifiedStream,
            done: bool,
            limit: Duration,
            timeout_ms: u64,
        }

        let state = IdleState {
            stream,
            done: false,
            limit,
            timeout_ms: self.config.stream_idle_timeout_ms,
        };
        Box::pin(futures::stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            match tokio::time::timeout(state.limit, state.stream.next()).await {
                Ok(Some(item)) => Some((item, state)),
                Ok(None) => None,
                Err(_) => {
                    state.done = true;
                    Some((
                        Err(ProviderFailure::timeout(format!(
                            "provider stream idle for {}ms",
                            state.timeout_ms
                        ))),
                        state,
                    ))
                }
            }
        }))
    }
}

#[async_trait]
impl ClassifiedProvider for ReliableProvider {
    async fn stream_chat_classified(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<ClassifiedStream, ProviderFailure> {
        let request = self.inner.stream_chat_classified(messages, tools);
        let result = match self.config.request_timeout() {
            Some(limit) => match tokio::time::timeout(limit, request).await {
                Ok(result) => result,
                Err(_) => Err(ProviderFailure::timeout(format!(
                    "provider request timed out after {}ms",
                    self.config.request_timeout_ms
                ))),
            },
            None => request.await,
        };

        Ok(self.with_idle_timeout(result?))
    }
}

#[async_trait]
impl LlmProvider for ReliableProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let stream = self
            .stream_chat_classified(messages, tools)
            .await
            .map_err(setup_failure_to_llm_error)?;
        Ok(legacy_stream(stream))
    }
}

/// Maps a classified stream back to the frozen [`LlmEvent`] contract.
fn legacy_stream(stream: ClassifiedStream) -> LlmStream {
    Box::pin(stream.map(|item| match item {
        Ok(event) => event,
        Err(failure) => LlmEvent::Error {
            message: failure.message().to_owned(),
        },
    }))
}

/// Maps a setup failure to the frozen [`LlmError`] contract.
fn setup_failure_to_llm_error(failure: ProviderFailure) -> LlmError {
    let message = failure.message().to_owned();
    match failure.kind() {
        ProviderFailureKind::InvalidResponse => LlmError::InvalidResponse { message },
        _ => LlmError::Request { message },
    }
}

/// Validates one timeout field.
fn validate_timeout(
    field: &'static str,
    value: u64,
    max: u64,
) -> Result<(), ReliabilityConfigError> {
    if value > max {
        return Err(ReliabilityConfigError::TimeoutTooLarge { field, value, max });
    }
    Ok(())
}

/// Converts milliseconds to a duration, treating zero as disabled.
fn timeout_duration(millis: u64) -> Option<Duration> {
    (millis > 0).then(|| Duration::from_millis(millis))
}

/// Classifies an HTTP status code and builds the provider failure.
pub(crate) fn classify_http_status(
    status: u16,
    message: String,
    retry_after: Option<Duration>,
) -> ProviderFailure {
    let failure = match status {
        408 | 425 => ProviderFailure::client_retryable(message),
        429 => ProviderFailure::rate_limited(message),
        500..=599 => ProviderFailure::server(message),
        400..=499 => ProviderFailure::client(message),
        _ => ProviderFailure::client(message),
    };
    failure.with_retry_after(retry_after)
}

/// Parses an integer `Retry-After` header.
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?;
    let seconds = value.to_str().ok()?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

/// Classifies a reqwest transport error.
pub(crate) fn classify_reqwest_error(error: &reqwest::Error) -> ProviderFailure {
    let message = format!("failed to send request: {error}");
    if error.is_timeout() {
        ProviderFailure::timeout(message)
    } else {
        ProviderFailure::transport(message)
    }
}

/// Converts a locally constructed request error into a non-retryable failure.
pub(crate) fn local_failure(error: LlmError) -> ProviderFailure {
    match error {
        LlmError::Request { message } => ProviderFailure::client(message),
        LlmError::Stream { message } | LlmError::InvalidResponse { message } => {
            ProviderFailure::invalid_response(message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use futures::stream;
    use jessica_core::types::TokenUsage;

    #[derive(Clone)]
    enum Behavior {
        Setup(ProviderFailure),
        Stream(Vec<Result<LlmEvent, ProviderFailure>>),
        StreamThenPending,
        PendingStream,
        DelayedSetupAndStream,
        PendingSetup,
    }

    struct MockClassifiedProvider {
        behavior: Mutex<Behavior>,
    }

    impl MockClassifiedProvider {
        fn new(behavior: Behavior) -> Self {
            Self {
                behavior: Mutex::new(behavior),
            }
        }
    }

    #[async_trait]
    impl ClassifiedProvider for MockClassifiedProvider {
        async fn stream_chat_classified(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<ClassifiedStream, ProviderFailure> {
            let behavior = self
                .behavior
                .lock()
                .expect("mock behavior lock should not be poisoned")
                .clone();
            match behavior {
                Behavior::Setup(failure) => Err(failure),
                Behavior::Stream(items) => Ok(Box::pin(stream::iter(items))),
                Behavior::StreamThenPending => Ok(Box::pin(
                    stream::iter(stream_then_pending()).chain(stream::pending()),
                )),
                Behavior::PendingStream => Ok(Box::pin(stream::pending::<
                    Result<LlmEvent, ProviderFailure>,
                >())),
                Behavior::DelayedSetupAndStream => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(Box::pin(stream::once(async {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        Ok::<_, ProviderFailure>(LlmEvent::Done { usage: None })
                    })))
                }
                Behavior::PendingSetup => {
                    std::future::pending::<Result<ClassifiedStream, ProviderFailure>>().await
                }
            }
        }
    }

    fn provider(behavior: Behavior, config: ReliabilityConfig) -> ReliableProvider {
        ReliableProvider::new(Arc::new(MockClassifiedProvider::new(behavior)), config)
            .expect("test configuration should be valid")
    }

    fn stream_then_pending() -> Vec<Result<LlmEvent, ProviderFailure>> {
        vec![Ok(LlmEvent::TextDelta {
            text: "partial".to_owned(),
        })]
    }

    #[test]
    fn failure_kinds_have_the_expected_retryability() {
        let cases = [
            (ProviderFailure::transport("x"), true),
            (ProviderFailure::timeout("x"), true),
            (ProviderFailure::rate_limited("x"), true),
            (ProviderFailure::server("x"), true),
            (ProviderFailure::client("x"), false),
            (ProviderFailure::client_retryable("x"), true),
            (ProviderFailure::invalid_response("x"), false),
            (ProviderFailure::stream_interrupted("x"), true),
        ];
        for (failure, retryable) in cases {
            assert_eq!(failure.is_retryable(), retryable, "{failure:?}");
        }
    }

    #[test]
    fn http_status_classification_covers_retryable_and_permanent_cases() {
        let permanent = [400, 401, 403, 404, 409, 422];
        for status in permanent {
            let failure = classify_http_status(status, format!("HTTP {status}"), None);
            assert_eq!(failure.kind(), ProviderFailureKind::Client);
            assert!(!failure.is_retryable());
        }

        for status in [408, 425] {
            let failure = classify_http_status(status, format!("HTTP {status}"), None);
            assert_eq!(failure.kind(), ProviderFailureKind::Client);
            assert!(failure.is_retryable());
        }

        let failure =
            classify_http_status(429, "slow down".to_owned(), Some(Duration::from_secs(7)));
        assert_eq!(failure.kind(), ProviderFailureKind::RateLimited);
        assert!(failure.is_retryable());
        assert_eq!(failure.retry_after(), Some(Duration::from_secs(7)));

        for status in [500, 502, 503, 504] {
            let failure = classify_http_status(status, format!("HTTP {status}"), None);
            assert_eq!(failure.kind(), ProviderFailureKind::Server);
            assert!(failure.is_retryable());
        }
    }

    #[test]
    fn local_setup_errors_are_non_retryable_and_keep_their_category() {
        let request = local_failure(LlmError::Request {
            message: "bad request".to_owned(),
        });
        assert_eq!(request.kind(), ProviderFailureKind::Client);
        assert!(!request.is_retryable());

        let invalid = local_failure(LlmError::InvalidResponse {
            message: "bad response".to_owned(),
        });
        assert_eq!(invalid.kind(), ProviderFailureKind::InvalidResponse);
        assert!(!invalid.is_retryable());
    }

    #[test]
    fn zero_timeouts_are_disabled() {
        let config = ReliabilityConfig::default();
        assert_eq!(config.request_timeout(), None);
        assert_eq!(config.stream_idle_timeout(), None);
    }

    #[test]
    fn timeouts_above_the_maximum_are_rejected() {
        let config = ReliabilityConfig {
            request_timeout_ms: MAX_TIMEOUT_MS + 1,
            stream_idle_timeout_ms: 0,
        };
        let error = config.validate().expect_err("timeout should be rejected");
        assert!(matches!(
            error,
            ReliabilityConfigError::TimeoutTooLarge { .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn request_timeout_becomes_a_request_error() {
        let provider = provider(
            Behavior::PendingSetup,
            ReliabilityConfig {
                request_timeout_ms: 25,
                stream_idle_timeout_ms: 0,
            },
        );

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("request should time out");
        };
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn request_timeout_is_classified_as_timeout() {
        let provider = provider(
            Behavior::PendingSetup,
            ReliabilityConfig {
                request_timeout_ms: 25,
                stream_idle_timeout_ms: 0,
            },
        );

        let Err(failure) = provider
            .stream_chat_classified(&[Message::user("hello")], &[])
            .await
        else {
            panic!("request should time out");
        };
        assert_eq!(failure.kind(), ProviderFailureKind::Timeout);
        assert!(failure.is_retryable());
    }

    #[tokio::test(start_paused = true)]
    async fn stream_idle_timeout_is_classified_as_timeout() {
        let provider = provider(
            Behavior::StreamThenPending,
            ReliabilityConfig {
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
            },
        );
        let stream = provider
            .stream_chat_classified(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let items = stream.collect::<Vec<_>>().await;

        assert_eq!(
            items.first(),
            Some(&Ok(LlmEvent::TextDelta {
                text: "partial".to_owned()
            }))
        );
        match items.last() {
            Some(Err(failure)) => {
                assert_eq!(failure.kind(), ProviderFailureKind::Timeout);
                assert!(failure.is_retryable());
                assert!(failure.message().contains("idle for 20ms"));
            }
            other => panic!("expected a timeout failure, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stream_idle_timeout_emits_an_error_after_the_first_event() {
        let provider = provider(
            Behavior::StreamThenPending,
            ReliabilityConfig {
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
            },
        );
        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(
            events.first(),
            Some(&LlmEvent::TextDelta {
                text: "partial".to_owned()
            })
        );
        assert!(matches!(
            events.last(),
            Some(LlmEvent::Error { message }) if message.contains("idle for 20ms")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_idle_timeout_before_the_first_event_emits_an_error() {
        let provider = provider(
            Behavior::PendingStream,
            ReliabilityConfig {
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
            },
        );
        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            LlmEvent::Error { message } if message.contains("idle for 20ms")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeouts_allow_delayed_setup_and_stream() {
        let provider = provider(
            Behavior::DelayedSetupAndStream,
            ReliabilityConfig::default(),
        );
        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(events, vec![LlmEvent::Done { usage: None }]);
    }

    #[tokio::test]
    async fn stream_failure_becomes_an_error_event() {
        let provider = provider(
            Behavior::Stream(vec![Err(ProviderFailure::server("boom"))]),
            ReliabilityConfig::default(),
        );
        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![LlmEvent::Error {
                message: "boom".to_owned()
            }]
        );
    }

    #[tokio::test]
    async fn setup_failures_map_to_the_frozen_error_contract() {
        let request_provider = provider(
            Behavior::Setup(ProviderFailure::server("server down")),
            ReliabilityConfig::default(),
        );
        let Err(error) = request_provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
        else {
            panic!("setup should fail");
        };
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("server down"));

        let invalid_provider = provider(
            Behavior::Setup(ProviderFailure::invalid_response("bad body")),
            ReliabilityConfig::default(),
        );
        let Err(error) = invalid_provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
        else {
            panic!("setup should fail");
        };
        assert!(matches!(error, LlmError::InvalidResponse { .. }));
        assert!(error.to_string().contains("bad body"));
    }

    #[tokio::test]
    async fn completed_stream_preserves_done_usage() {
        let provider = provider(
            Behavior::Stream(vec![Ok(LlmEvent::Done {
                usage: Some(TokenUsage::new(3, 4)),
            })]),
            ReliabilityConfig::default(),
        );
        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");

        assert_eq!(
            stream.collect::<Vec<_>>().await,
            vec![LlmEvent::Done {
                usage: Some(TokenUsage::new(3, 4))
            }]
        );
    }
}
