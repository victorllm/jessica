//! Typed provider failures, timeouts, retries, and the reliability wrapper.
//!
//! [`ClassifiedProvider`] is an internal extension of the frozen
//! [`jessica_core::llm::LlmProvider`] contract. It keeps the failure category
//! alongside the failure message so retry, circuit-breaker, and rate-limit
//! layers do not have to parse provider error strings.
//!
//! [`ReliableProvider`] retries only before the first
//! [`jessica_core::llm::LlmEvent::TextDelta`] is delivered. Because a retry
//! issues a complete new provider request, the provider may bill the same
//! prompt more than once; clients never receive duplicated text or execute
//! duplicated tool calls, and no retry happens after the first text delta.
//! Tool calls requested before a failed stream completes are buffered and
//! discarded with that attempt, so only the successful attempt's tool calls
//! reach the caller.
//!
//! A per-wrapper circuit breaker counts failed logical calls, not individual
//! attempts. After `circuit_failure_threshold` retryable logical failures the
//! breaker opens, rejects calls without touching the underlying provider until
//! `circuit_cooldown_ms` elapses, then allows one half-open probe. The state is
//! held only in memory: it is not persisted, not written to the event stream,
//! and resets whenever the process rebuilds the wrapper. An open breaker is a
//! fast failure, not another retry.
//!
//! Rate limiting is added by a later task in the same module.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::Instant;

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

/// Default total attempts per request, including the first.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Default delay after the first failed attempt, in milliseconds.
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 250;
/// Default upper bound for exponential backoff, in milliseconds.
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 4_000;
/// Recommended upper bound for [`ReliabilityConfig::max_attempts`].
///
/// Larger values are accepted, but callers should treat ten attempts as the
/// point where retries stop being useful for an interactive agent.
pub const RECOMMENDED_MAX_ATTEMPTS: u32 = 10;
/// Default number of failed logical calls that opens the circuit.
pub const DEFAULT_CIRCUIT_FAILURE_THRESHOLD: u32 = 5;
/// Default time the circuit stays open before allowing a probe, in
/// milliseconds.
pub const DEFAULT_CIRCUIT_COOLDOWN_MS: u64 = 30_000;

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
    /// [`ReliableProvider`] additionally requires that no
    /// [`LlmEvent::TextDelta`] has been delivered before it retries.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// Adds the total-attempt prefix used by the final error of a retried
    /// request.
    ///
    /// A single attempt keeps the provider's original message unchanged.
    fn after_attempts(mut self, attempts: u32) -> Self {
        if attempts > 1 {
            self.message = format!(
                "provider request failed after {attempts} attempts: {}",
                self.message
            );
        }
        self
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

/// Reliability configuration for [`ReliableProvider`].
///
/// A zero timeout disables that limit. Retries use deterministic, jitter-free
/// exponential backoff. The recommended upper bound for `max_attempts` is
/// [`RECOMMENDED_MAX_ATTEMPTS`]; larger values are accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReliabilityConfig {
    /// Total attempts per request, including the first. Must be at least one.
    pub max_attempts: u32,
    /// Delay after the first failed attempt, in milliseconds.
    ///
    /// Later retries double this value up to `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    /// Upper bound for the exponential backoff, in milliseconds.
    ///
    /// A provider `Retry-After` larger than this stops retrying and returns
    /// the failure instead of waiting.
    pub max_backoff_ms: u64,
    /// Maximum time allowed for the provider request setup phase.
    pub request_timeout_ms: u64,
    /// Maximum time allowed between two provider stream events.
    pub stream_idle_timeout_ms: u64,
    /// Failed logical calls that open the circuit. Must be at least one.
    ///
    /// One logical call may issue several attempts; only its final outcome
    /// counts.
    pub circuit_failure_threshold: u32,
    /// Time the circuit stays open before a probe is allowed, in
    /// milliseconds. Must be positive.
    pub circuit_cooldown_ms: u64,
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff_ms: DEFAULT_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
            request_timeout_ms: 0,
            stream_idle_timeout_ms: 0,
            circuit_failure_threshold: DEFAULT_CIRCUIT_FAILURE_THRESHOLD,
            circuit_cooldown_ms: DEFAULT_CIRCUIT_COOLDOWN_MS,
        }
    }
}

impl ReliabilityConfig {
    /// Validates every retry and timeout value.
    pub fn validate(&self) -> Result<(), ReliabilityConfigError> {
        validate_minimum("max_attempts", u64::from(self.max_attempts), 1)?;
        validate_minimum("initial_backoff_ms", self.initial_backoff_ms, 1)?;
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err(ReliabilityConfigError::MaxBackoffBelowInitial {
                initial_backoff_ms: self.initial_backoff_ms,
                max_backoff_ms: self.max_backoff_ms,
            });
        }
        validate_timeout(
            "request_timeout_ms",
            self.request_timeout_ms,
            MAX_TIMEOUT_MS,
        )?;
        validate_timeout(
            "stream_idle_timeout_ms",
            self.stream_idle_timeout_ms,
            MAX_TIMEOUT_MS,
        )?;
        validate_minimum(
            "circuit_failure_threshold",
            u64::from(self.circuit_failure_threshold),
            1,
        )?;
        validate_minimum("circuit_cooldown_ms", self.circuit_cooldown_ms, 1)
    }

    /// Returns the request timeout when enabled.
    fn request_timeout(&self) -> Option<Duration> {
        timeout_duration(self.request_timeout_ms)
    }

    /// Returns the stream idle timeout when enabled.
    fn stream_idle_timeout(&self) -> Option<Duration> {
        timeout_duration(self.stream_idle_timeout_ms)
    }

    /// Returns the configured maximum backoff as a duration.
    fn max_backoff(&self) -> Duration {
        Duration::from_millis(self.max_backoff_ms)
    }
}

/// Invalid reliability configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReliabilityConfigError {
    /// A retry value is below its accepted minimum.
    #[error("{field} = {value} must be at least {min}")]
    ValueTooSmall {
        /// Configuration field name.
        field: &'static str,
        /// Configured value.
        value: u64,
        /// Minimum accepted value.
        min: u64,
    },
    /// The backoff cap is below the initial delay.
    #[error(
        "max_backoff_ms = {max_backoff_ms} must be at least initial_backoff_ms = {initial_backoff_ms}"
    )]
    MaxBackoffBelowInitial {
        /// Configured initial delay in milliseconds.
        initial_backoff_ms: u64,
        /// Configured maximum delay in milliseconds.
        max_backoff_ms: u64,
    },
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

/// Circuit states for one [`ReliableProvider`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    /// Calls are admitted; `failures` counts consecutive failed logical calls.
    Closed {
        /// Failed logical calls since the last success.
        failures: u32,
    },
    /// Calls fail fast until the cooldown elapses.
    Open {
        /// When the circuit opened.
        opened_at: Instant,
    },
    /// One probe is allowed; other calls fail fast.
    HalfOpen {
        /// Whether the single probe slot is occupied.
        probe_in_flight: bool,
    },
}

/// Why a call was rejected before reaching the provider.
enum CircuitRejection {
    /// The circuit is open for the given remaining cooldown.
    Open {
        /// Time left before a probe is allowed.
        remaining: Duration,
    },
    /// The circuit is half-open and its single probe is already running.
    ProbeInFlight,
}

impl CircuitRejection {
    /// Builds the classified failure returned to the caller.
    fn into_failure(self) -> ProviderFailure {
        let message = match self {
            Self::Open { remaining } => format!(
                "circuit breaker open: retry after {}ms",
                remaining.as_millis()
            ),
            Self::ProbeInFlight => {
                "circuit breaker open: half-open probe already in progress".to_owned()
            }
        };
        ProviderFailure::new(ProviderFailureKind::Server, message, None, false)
    }
}

/// Outcome of one logical provider call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitOutcome {
    /// The call delivered its terminal `Done`.
    Success,
    /// The call failed after retries were exhausted and the failure kind is
    /// retryable.
    RetryableFailure,
    /// The call was cancelled, dropped, or failed for a non-retryable reason.
    Neutral,
}

/// In-memory closed/open/half-open circuit breaker.
///
/// The mutex is never held across an await: [`CircuitBreaker::try_acquire`] and
/// [`CircuitAdmission::settle`] are synchronous.
struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    state: Mutex<CircuitState>,
}

impl CircuitBreaker {
    /// Creates a closed breaker from the reliability configuration.
    fn new(config: ReliabilityConfig) -> Self {
        Self {
            threshold: config.circuit_failure_threshold,
            cooldown: Duration::from_millis(config.circuit_cooldown_ms),
            state: Mutex::new(CircuitState::Closed { failures: 0 }),
        }
    }

    /// Locks the state, recovering from a poisoned mutex.
    fn lock(&self) -> MutexGuard<'_, CircuitState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Admits a call or rejects it without touching the provider.
    fn try_acquire(self: &Arc<Self>) -> Result<CircuitAdmission, CircuitRejection> {
        let mut state = self.lock();
        match &mut *state {
            CircuitState::Closed { .. } => Ok(CircuitAdmission::new(Arc::clone(self), false)),
            CircuitState::Open { opened_at } => {
                let elapsed = Instant::now().saturating_duration_since(*opened_at);
                if elapsed < self.cooldown {
                    return Err(CircuitRejection::Open {
                        remaining: self.cooldown - elapsed,
                    });
                }
                *state = CircuitState::HalfOpen {
                    probe_in_flight: true,
                };
                Ok(CircuitAdmission::new(Arc::clone(self), true))
            }
            CircuitState::HalfOpen { probe_in_flight } => {
                if *probe_in_flight {
                    return Err(CircuitRejection::ProbeInFlight);
                }
                *probe_in_flight = true;
                Ok(CircuitAdmission::new(Arc::clone(self), true))
            }
        }
    }
}

/// Admission ticket for one logical call.
///
/// Dropping an unsettled admission records a neutral outcome, which releases a
/// half-open probe slot without counting a success or a failure.
struct CircuitAdmission {
    breaker: Arc<CircuitBreaker>,
    probe: bool,
    settled: bool,
}

impl CircuitAdmission {
    /// Creates an unsettled admission.
    fn new(breaker: Arc<CircuitBreaker>, probe: bool) -> Self {
        Self {
            breaker,
            probe,
            settled: false,
        }
    }

    /// Records that the logical call completed with `Done`.
    fn record_success(&mut self) {
        self.settle(CircuitOutcome::Success);
    }

    /// Records that the logical call failed retryably after retries.
    fn record_failure(&mut self) {
        self.settle(CircuitOutcome::RetryableFailure);
    }

    /// Records that the logical call ended without a conclusive result.
    fn record_neutral(&mut self) {
        self.settle(CircuitOutcome::Neutral);
    }

    /// Applies one outcome exactly once.
    fn settle(&mut self, outcome: CircuitOutcome) {
        if self.settled {
            return;
        }
        self.settled = true;

        let mut state = self.breaker.lock();
        match outcome {
            CircuitOutcome::Success => {
                let closes = match &*state {
                    CircuitState::Closed { .. } => true,
                    CircuitState::HalfOpen { .. } => self.probe,
                    CircuitState::Open { .. } => false,
                };
                if closes {
                    *state = CircuitState::Closed { failures: 0 };
                }
            }
            CircuitOutcome::RetryableFailure if self.probe => {
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
            }
            CircuitOutcome::RetryableFailure => {
                let mut opens = false;
                if let CircuitState::Closed { failures } = &mut *state {
                    *failures = failures.saturating_add(1);
                    opens = *failures >= self.breaker.threshold;
                }
                if opens {
                    *state = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                }
            }
            CircuitOutcome::Neutral => {
                if self.probe
                    && let CircuitState::HalfOpen { probe_in_flight } = &mut *state
                {
                    *probe_in_flight = false;
                }
            }
        }
    }
}

impl Drop for CircuitAdmission {
    fn drop(&mut self) {
        if !self.settled {
            self.settle(CircuitOutcome::Neutral);
        }
    }
}

/// Records a final stream or setup failure with the breaker.
fn settle_failure(admission: &mut CircuitAdmission, failure: &ProviderFailure) {
    if failure.is_retryable() {
        admission.record_failure();
    } else {
        admission.record_neutral();
    }
}

/// Wraps a [`ClassifiedProvider`] with timeouts, pre-commit retries, and a
/// circuit breaker.
///
/// It implements [`ClassifiedProvider`] so callers can observe typed failures,
/// and [`jessica_core::llm::LlmProvider`] so it can be used wherever the frozen
/// contract is expected. Both paths share the same retry state machine and the
/// same circuit breaker.
///
/// A retry issues a complete new provider request and can therefore cause
/// provider-side duplicate billing. The wrapper never delivers text or tool
/// calls from a failed attempt, and it never retries after the first
/// [`LlmEvent::TextDelta`].
pub struct ReliableProvider {
    inner: Arc<dyn ClassifiedProvider>,
    config: ReliabilityConfig,
    circuit: Arc<CircuitBreaker>,
}

impl ReliableProvider {
    /// Creates a reliability wrapper around `inner`.
    ///
    /// Returns an error when the retry or timeout configuration is invalid.
    pub fn new(
        inner: Arc<dyn ClassifiedProvider>,
        config: ReliabilityConfig,
    ) -> Result<Self, ReliabilityConfigError> {
        config.validate()?;
        Ok(Self {
            inner,
            config,
            circuit: Arc::new(CircuitBreaker::new(config)),
        })
    }

    /// Returns the reliability configuration in use.
    pub fn config(&self) -> ReliabilityConfig {
        self.config
    }
}

#[async_trait]
impl ClassifiedProvider for ReliableProvider {
    async fn stream_chat_classified(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<ClassifiedStream, ProviderFailure> {
        let mut admission = self
            .circuit
            .try_acquire()
            .map_err(|rejection| rejection.into_failure())?;
        let messages: Arc<[Message]> = messages.to_vec().into();
        let tools: Arc<[ToolSchema]> = tools.to_vec().into();
        match open_until(
            Arc::clone(&self.inner),
            self.config,
            Arc::clone(&messages),
            Arc::clone(&tools),
            1,
        )
        .await
        {
            OpenOutcome::Opened { stream, attempt } => Ok(retrying_stream(
                Arc::clone(&self.inner),
                self.config,
                messages,
                tools,
                stream,
                attempt,
                admission,
            )),
            OpenOutcome::Failed(failure) => {
                settle_failure(&mut admission, &failure);
                Err(failure)
            }
        }
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

/// Result of one setup retry loop.
enum OpenOutcome {
    /// A provider stream was opened on the given attempt.
    Opened {
        /// Fresh classified stream.
        stream: ClassifiedStream,
        /// One-based attempt number that succeeded.
        attempt: u32,
    },
    /// The request failed permanently or exhausted its attempts.
    Failed(ProviderFailure),
}

/// Opens a request, retrying retryable setup failures with deterministic
/// backoff until `max_attempts` is reached.
async fn open_until(
    inner: Arc<dyn ClassifiedProvider>,
    config: ReliabilityConfig,
    messages: Arc<[Message]>,
    tools: Arc<[ToolSchema]>,
    start_attempt: u32,
) -> OpenOutcome {
    let mut attempt = start_attempt;

    loop {
        match open_attempt(inner.as_ref(), config, &messages, &tools).await {
            Ok(stream) => return OpenOutcome::Opened { stream, attempt },
            Err(failure) => {
                let retry = failure.is_retryable() && attempt < config.max_attempts;
                let delay = if retry {
                    retry_delay(config, attempt, &failure)
                } else {
                    None
                };
                match delay {
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    None => return OpenOutcome::Failed(failure.after_attempts(attempt)),
                }
            }
        }
    }
}

/// Performs one provider request setup with the configured request timeout and
/// wraps a successful stream with the configured idle timeout.
async fn open_attempt(
    inner: &dyn ClassifiedProvider,
    config: ReliabilityConfig,
    messages: &[Message],
    tools: &[ToolSchema],
) -> Result<ClassifiedStream, ProviderFailure> {
    let request = inner.stream_chat_classified(messages, tools);
    let result = match config.request_timeout() {
        Some(limit) => match tokio::time::timeout(limit, request).await {
            Ok(result) => result,
            Err(_) => Err(ProviderFailure::timeout(format!(
                "provider request timed out after {}ms",
                config.request_timeout_ms
            ))),
        },
        None => request.await,
    };

    Ok(with_idle_timeout(result?, config))
}

/// Wraps a classified stream with the configured idle timeout.
fn with_idle_timeout(stream: ClassifiedStream, config: ReliabilityConfig) -> ClassifiedStream {
    let Some(limit) = config.stream_idle_timeout() else {
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
        timeout_ms: config.stream_idle_timeout_ms,
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

/// Computes the delay before the next attempt after `attempt` failed.
///
/// Returns `None` when the provider's `Retry-After` exceeds `max_backoff_ms`,
/// which means the caller should stop retrying instead of waiting.
fn retry_delay(
    config: ReliabilityConfig,
    attempt: u32,
    failure: &ProviderFailure,
) -> Option<Duration> {
    let backoff = backoff_duration(config, attempt);
    match failure.retry_after() {
        Some(retry_after) if retry_after > config.max_backoff() => None,
        Some(retry_after) => Some(backoff.max(retry_after)),
        None => Some(backoff),
    }
}

/// Deterministic exponential backoff: `initial * 2^(attempt - 1)`, capped.
fn backoff_duration(config: ReliabilityConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(63);
    let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let millis = config
        .initial_backoff_ms
        .saturating_mul(factor)
        .min(config.max_backoff_ms);
    Duration::from_millis(millis)
}

/// Retry-aware stream state shared by successful attempts.
struct RetryStreamState {
    /// Provider used to open a replacement attempt.
    inner: Arc<dyn ClassifiedProvider>,
    /// Retry and timeout configuration.
    config: ReliabilityConfig,
    /// Original request messages, retained for replacement attempts.
    messages: Arc<[Message]>,
    /// Original tool schemas, retained for replacement attempts.
    tools: Arc<[ToolSchema]>,
    /// One-based attempt number currently open.
    attempt: u32,
    /// Current attempt's stream, absent while retrying or after completion.
    stream: Option<ClassifiedStream>,
    /// Tool calls received by the current attempt but not yet safe to deliver.
    tool_calls: Vec<jessica_core::types::ToolCall>,
    /// Whether a text delta has been delivered.
    committed: bool,
    /// Events ready to be yielded before reading the stream again.
    pending: VecDeque<LlmEvent>,
    /// Whether the stream has delivered its terminal event.
    finished: bool,
    /// Circuit admission for this logical call.
    admission: CircuitAdmission,
}

/// Builds a stream that buffers tool calls until `Done` and retries failures
/// only while no text delta has been delivered.
fn retrying_stream(
    inner: Arc<dyn ClassifiedProvider>,
    config: ReliabilityConfig,
    messages: Arc<[Message]>,
    tools: Arc<[ToolSchema]>,
    stream: ClassifiedStream,
    attempt: u32,
    admission: CircuitAdmission,
) -> ClassifiedStream {
    let state = RetryStreamState {
        inner,
        config,
        messages,
        tools,
        attempt,
        stream: Some(stream),
        tool_calls: Vec::new(),
        committed: false,
        pending: VecDeque::new(),
        finished: false,
        admission,
    };

    Box::pin(futures::stream::unfold(state, |mut state| async move {
        loop {
            if state.finished {
                return None;
            }

            if let Some(event) = state.pending.pop_front() {
                if matches!(event, LlmEvent::Done { .. }) {
                    state.finished = true;
                    state.stream = None;
                }
                return Some((Ok(event), state));
            }

            let stream = state.stream.as_mut()?;
            let next = stream.next().await;

            match next {
                Some(Ok(LlmEvent::TextDelta { text })) => {
                    state.committed = true;
                    return Some((Ok(LlmEvent::TextDelta { text }), state));
                }
                Some(Ok(LlmEvent::ToolCallRequested { call })) => {
                    state.tool_calls.push(call);
                }
                Some(Ok(LlmEvent::Done { usage })) => {
                    state.admission.record_success();
                    state.stream = None;
                    for call in state.tool_calls.drain(..) {
                        state
                            .pending
                            .push_back(LlmEvent::ToolCallRequested { call });
                    }
                    state.pending.push_back(LlmEvent::Done { usage });
                }
                Some(Ok(LlmEvent::Error { message })) => {
                    // A classified stream should report failures as `Err`.
                    // An `Error` event is an implementation error, not a
                    // provider failure, so it is terminal and never retried.
                    state.admission.record_neutral();
                    state.finished = true;
                    state.stream = None;
                    state.tool_calls.clear();
                    return Some((Ok(LlmEvent::Error { message }), state));
                }
                Some(Err(failure)) => match recover_stream_failure(&mut state, failure).await {
                    Ok(()) => {}
                    Err(failure) => {
                        settle_failure(&mut state.admission, &failure);
                        return Some((Err(failure), state));
                    }
                },
                None => {
                    let failure =
                        ProviderFailure::stream_interrupted("provider stream ended before Done");
                    match recover_stream_failure(&mut state, failure).await {
                        Ok(()) => {}
                        Err(failure) => {
                            settle_failure(&mut state.admission, &failure);
                            return Some((Err(failure), state));
                        }
                    }
                }
            }
        }
    }))
}

/// Applies the pre-commit retry rule to a stream failure.
///
/// On success the state has a replacement stream and the caller should keep
/// reading. On failure the returned error is already final and prefixed with
/// the attempt count when more than one attempt was made.
async fn recover_stream_failure(
    state: &mut RetryStreamState,
    failure: ProviderFailure,
) -> Result<(), ProviderFailure> {
    let delay = if !state.committed
        && failure.is_retryable()
        && state.attempt < state.config.max_attempts
    {
        retry_delay(state.config, state.attempt, &failure)
    } else {
        None
    };

    let Some(delay) = delay else {
        state.finished = true;
        state.stream = None;
        state.tool_calls.clear();
        return Err(failure.after_attempts(state.attempt));
    };

    state.stream = None;
    state.tool_calls.clear();
    tokio::time::sleep(delay).await;

    match open_until(
        Arc::clone(&state.inner),
        state.config,
        Arc::clone(&state.messages),
        Arc::clone(&state.tools),
        state.attempt + 1,
    )
    .await
    {
        OpenOutcome::Opened { stream, attempt } => {
            state.stream = Some(stream);
            state.attempt = attempt;
            Ok(())
        }
        OpenOutcome::Failed(failure) => {
            state.finished = true;
            Err(failure)
        }
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

/// Validates that one value is at least `min`.
fn validate_minimum(
    field: &'static str,
    value: u64,
    min: u64,
) -> Result<(), ReliabilityConfigError> {
    if value < min {
        return Err(ReliabilityConfigError::ValueTooSmall { field, value, min });
    }
    Ok(())
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
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures::stream;
    use jessica_core::types::{TokenUsage, ToolCall};

    /// One scripted attempt returned by [`MockClassifiedProvider`].
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
        behaviors: Mutex<VecDeque<Behavior>>,
        calls: AtomicU32,
        call_times: Mutex<Vec<tokio::time::Instant>>,
    }

    impl MockClassifiedProvider {
        fn new(behaviors: Vec<Behavior>) -> Self {
            Self {
                behaviors: Mutex::new(behaviors.into()),
                calls: AtomicU32::new(0),
                call_times: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }

        fn call_times(&self) -> Vec<tokio::time::Instant> {
            self.call_times
                .lock()
                .expect("mock call-time lock should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ClassifiedProvider for MockClassifiedProvider {
        async fn stream_chat_classified(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<ClassifiedStream, ProviderFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.call_times
                .lock()
                .expect("mock call-time lock should not be poisoned")
                .push(tokio::time::Instant::now());
            let behavior = self
                .behaviors
                .lock()
                .expect("mock behavior lock should not be poisoned")
                .pop_front();
            match behavior {
                Some(Behavior::Setup(failure)) => Err(failure),
                Some(Behavior::Stream(items)) => Ok(Box::pin(stream::iter(items))),
                Some(Behavior::StreamThenPending) => Ok(Box::pin(
                    stream::iter(stream_then_pending()).chain(stream::pending()),
                )),
                Some(Behavior::PendingStream) => Ok(Box::pin(stream::pending::<
                    Result<LlmEvent, ProviderFailure>,
                >())),
                Some(Behavior::DelayedSetupAndStream) => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(Box::pin(stream::once(async {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        Ok::<_, ProviderFailure>(LlmEvent::Done { usage: None })
                    })))
                }
                Some(Behavior::PendingSetup) => {
                    std::future::pending::<Result<ClassifiedStream, ProviderFailure>>().await
                }
                None => Err(ProviderFailure::client("unexpected extra provider request")),
            }
        }
    }

    fn scripted(
        behaviors: Vec<Behavior>,
        config: ReliabilityConfig,
    ) -> (ReliableProvider, Arc<MockClassifiedProvider>) {
        let mock = Arc::new(MockClassifiedProvider::new(behaviors));
        let provider =
            ReliableProvider::new(Arc::clone(&mock) as Arc<dyn ClassifiedProvider>, config)
                .expect("test configuration should be valid");
        (provider, mock)
    }

    fn provider(behavior: Behavior, config: ReliabilityConfig) -> ReliableProvider {
        scripted(vec![behavior], config).0
    }

    fn retry_config(max_attempts: u32) -> ReliabilityConfig {
        ReliabilityConfig {
            max_attempts,
            ..ReliabilityConfig::default()
        }
    }

    fn stream_then_pending() -> Vec<Result<LlmEvent, ProviderFailure>> {
        vec![Ok(LlmEvent::TextDelta {
            text: "partial".to_owned(),
        })]
    }

    fn tool_call(id: &str) -> ToolCall {
        ToolCall::new(id, "read_file", serde_json::json!({"path": "x"}))
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
            ..ReliabilityConfig::default()
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
                max_attempts: 1,
                request_timeout_ms: 25,
                stream_idle_timeout_ms: 0,
                ..ReliabilityConfig::default()
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
                max_attempts: 1,
                request_timeout_ms: 25,
                stream_idle_timeout_ms: 0,
                ..ReliabilityConfig::default()
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
                max_attempts: 1,
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
                ..ReliabilityConfig::default()
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
                max_attempts: 1,
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
                ..ReliabilityConfig::default()
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
                max_attempts: 1,
                request_timeout_ms: 0,
                stream_idle_timeout_ms: 20,
                ..ReliabilityConfig::default()
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
            ReliabilityConfig {
                max_attempts: 1,
                ..ReliabilityConfig::default()
            },
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
            ReliabilityConfig {
                max_attempts: 1,
                ..ReliabilityConfig::default()
            },
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

    #[test]
    fn default_config_keeps_timeouts_disabled_and_enables_three_attempts() {
        let config = ReliabilityConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_backoff_ms, 250);
        assert_eq!(config.max_backoff_ms, 4_000);
        assert_eq!(config.request_timeout_ms, 0);
        assert_eq!(config.stream_idle_timeout_ms, 0);
        assert_eq!(config.circuit_failure_threshold, 5);
        assert_eq!(config.circuit_cooldown_ms, 30_000);
        assert_eq!(config.request_timeout(), None);
        assert_eq!(config.stream_idle_timeout(), None);
        assert_eq!(RECOMMENDED_MAX_ATTEMPTS, 10);
    }

    #[test]
    fn invalid_retry_configuration_is_rejected_with_the_field_name() {
        let zero_attempts = ReliabilityConfig {
            max_attempts: 0,
            ..ReliabilityConfig::default()
        };
        let error = zero_attempts
            .validate()
            .expect_err("zero attempts must be rejected");
        assert!(matches!(
            error,
            ReliabilityConfigError::ValueTooSmall {
                field: "max_attempts",
                value: 0,
                min: 1
            }
        ));

        let zero_backoff = ReliabilityConfig {
            initial_backoff_ms: 0,
            ..ReliabilityConfig::default()
        };
        let error = zero_backoff
            .validate()
            .expect_err("zero initial backoff must be rejected");
        assert!(error.to_string().contains("initial_backoff_ms"));

        let inverted_backoff = ReliabilityConfig {
            initial_backoff_ms: 500,
            max_backoff_ms: 499,
            ..ReliabilityConfig::default()
        };
        let error = inverted_backoff
            .validate()
            .expect_err("cap below initial delay must be rejected");
        assert!(matches!(
            error,
            ReliabilityConfigError::MaxBackoffBelowInitial { .. }
        ));

        let zero_threshold = ReliabilityConfig {
            circuit_failure_threshold: 0,
            ..ReliabilityConfig::default()
        };
        let error = zero_threshold
            .validate()
            .expect_err("zero circuit threshold must be rejected");
        assert!(matches!(
            error,
            ReliabilityConfigError::ValueTooSmall {
                field: "circuit_failure_threshold",
                value: 0,
                min: 1
            }
        ));

        let zero_cooldown = ReliabilityConfig {
            circuit_cooldown_ms: 0,
            ..ReliabilityConfig::default()
        };
        let error = zero_cooldown
            .validate()
            .expect_err("zero circuit cooldown must be rejected");
        assert!(matches!(
            error,
            ReliabilityConfigError::ValueTooSmall {
                field: "circuit_cooldown_ms",
                value: 0,
                min: 1
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn server_errors_retry_until_success_and_only_final_events_are_delivered() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("HTTP 500 first")),
                Behavior::Setup(ProviderFailure::server("HTTP 500 second")),
                Behavior::Stream(vec![
                    Ok(LlmEvent::TextDelta {
                        text: "recovered".to_owned(),
                    }),
                    Ok(LlmEvent::ToolCallRequested {
                        call: tool_call("call-final"),
                    }),
                    Ok(LlmEvent::Done { usage: None }),
                ]),
            ],
            retry_config(3),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("third attempt should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 3);
        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "recovered".to_owned()
                },
                LlmEvent::ToolCallRequested {
                    call: tool_call("call-final")
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_setup_failures_use_exact_capped_backoff() {
        let mut behaviors = vec![
            Behavior::Setup(ProviderFailure::server("one")),
            Behavior::Setup(ProviderFailure::server("two")),
            Behavior::Setup(ProviderFailure::server("three")),
            Behavior::Setup(ProviderFailure::server("four")),
            Behavior::Setup(ProviderFailure::server("five")),
        ];
        behaviors.push(Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]));
        let (provider, mock) = scripted(
            behaviors,
            ReliabilityConfig {
                max_attempts: 6,
                initial_backoff_ms: 250,
                max_backoff_ms: 1_000,
                ..ReliabilityConfig::default()
            },
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("sixth attempt should succeed");
        assert_eq!(stream.collect::<Vec<_>>().await.len(), 1);
        assert_eq!(mock.calls(), 6);

        let times = mock.call_times();
        let deltas: Vec<u128> = times
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]).as_millis())
            .collect();
        assert_eq!(deltas, vec![250, 500, 1_000, 1_000, 1_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_http_client_status_is_not_retried() {
        for status in [400, 401] {
            let expected = format!("provider returned HTTP {status}");
            let (provider, mock) = scripted(
                vec![
                    Behavior::Setup(ProviderFailure::client(expected.clone())),
                    Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
                ],
                retry_config(3),
            );

            let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
                panic!("{status} must not be retried");
            };

            match error {
                LlmError::Request { message } => assert_eq!(message, expected),
                other => panic!("expected a request error, got {other:?}"),
            }
            assert_eq!(mock.calls(), 1, "{status} must issue exactly one request");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_attempts_report_the_count_and_the_last_message() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("first")),
                Behavior::Setup(ProviderFailure::server("second")),
                Behavior::Setup(ProviderFailure::server("last")),
            ],
            retry_config(3),
        );

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("all attempts should fail");
        };

        match error {
            LlmError::Request { message } => {
                assert_eq!(message, "provider request failed after 3 attempts: last");
            }
            other => panic!("expected a request error, got {other:?}"),
        }
        assert_eq!(mock.calls(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_attempt_never_retries_and_has_no_prefix() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("only")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            retry_config(1),
        );

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the only attempt should fail");
        };

        assert_eq!(error.to_string(), "LLM request failed: only");
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_waits_for_the_larger_of_backoff_and_header() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(
                    ProviderFailure::rate_limited("slow down")
                        .with_retry_after(Some(Duration::from_millis(700))),
                ),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            retry_config(2),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("second attempt should succeed");
        assert_eq!(stream.collect::<Vec<_>>().await.len(), 1);
        assert_eq!(mock.calls(), 2);

        let times = mock.call_times();
        assert_eq!(
            times[1].duration_since(times[0]).as_millis(),
            700,
            "Retry-After must extend the 250ms base backoff"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_above_the_cap_stops_retrying() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(
                    ProviderFailure::rate_limited("slow down")
                        .with_retry_after(Some(Duration::from_millis(1_001))),
                ),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            ReliabilityConfig {
                max_attempts: 3,
                initial_backoff_ms: 250,
                max_backoff_ms: 1_000,
                ..ReliabilityConfig::default()
            },
        );

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("oversized Retry-After must stop retrying");
        };

        match error {
            LlmError::Request { message } => assert_eq!(message, "slow down"),
            other => panic!("expected a request error, got {other:?}"),
        }
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_error_after_text_is_terminal_even_with_attempts_left() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![
                    Ok(LlmEvent::TextDelta {
                        text: "partial".to_owned(),
                    }),
                    Err(ProviderFailure::server("stream boom")),
                ]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            retry_config(3),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 1, "committed text must not be retried");
        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "partial".to_owned()
                },
                LlmEvent::Error {
                    message: "stream boom".to_owned()
                },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn eof_after_text_is_terminal_even_with_attempts_left() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![Ok(LlmEvent::TextDelta {
                    text: "partial".to_owned(),
                })]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            retry_config(3),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 1);
        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "partial".to_owned()
                },
                LlmEvent::Error {
                    message: "provider stream ended before Done".to_owned()
                },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_after_text_is_terminal_even_with_attempts_left() {
        let (provider, mock) = scripted(
            vec![
                Behavior::StreamThenPending,
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            ReliabilityConfig {
                max_attempts: 3,
                stream_idle_timeout_ms: 20,
                ..ReliabilityConfig::default()
            },
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 1);
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0],
            LlmEvent::TextDelta {
                text: "partial".to_owned()
            }
        );
        assert!(matches!(
            &events[1],
            LlmEvent::Error { message } if message.contains("idle for 20ms")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_before_text_retries_the_full_request() {
        let (provider, mock) = scripted(
            vec![
                Behavior::PendingStream,
                Behavior::Stream(vec![
                    Ok(LlmEvent::TextDelta {
                        text: "recovered".to_owned(),
                    }),
                    Ok(LlmEvent::Done { usage: None }),
                ]),
            ],
            ReliabilityConfig {
                max_attempts: 2,
                stream_idle_timeout_ms: 20,
                ..ReliabilityConfig::default()
            },
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 2);
        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "recovered".to_owned()
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tool_calls_before_done_are_buffered_and_failed_attempts_do_not_leak() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![Ok(LlmEvent::ToolCallRequested {
                    call: tool_call("leaked"),
                })]),
                Behavior::Stream(vec![
                    Ok(LlmEvent::ToolCallRequested {
                        call: tool_call("final"),
                    }),
                    Ok(LlmEvent::Done { usage: None }),
                ]),
            ],
            retry_config(2),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 2);
        assert_eq!(
            events,
            vec![
                LlmEvent::ToolCallRequested {
                    call: tool_call("final")
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn classified_error_event_is_terminal_and_not_retried() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![Ok(LlmEvent::Error {
                    message: "classified contract violated".to_owned(),
                })]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            retry_config(3),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let events = stream.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 1);
        assert_eq!(
            events,
            vec![LlmEvent::Error {
                message: "classified contract violated".to_owned()
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_setup_future_during_backoff_prevents_another_request() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("first")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            ReliabilityConfig {
                max_attempts: 2,
                initial_backoff_ms: 10_000,
                max_backoff_ms: 10_000,
                ..ReliabilityConfig::default()
            },
        );

        let result = tokio::time::timeout(
            Duration::from_millis(1),
            provider.stream_chat(&[Message::user("hello")], &[]),
        )
        .await;
        assert!(result.is_err(), "the retry wait should be cancelled");
        assert_eq!(mock.calls(), 1);

        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(
            mock.calls(),
            1,
            "dropping the future must cancel its backoff and request"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_stream_during_backoff_prevents_another_request() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![
                    Ok(LlmEvent::ToolCallRequested {
                        call: tool_call("buffered"),
                    }),
                    Err(ProviderFailure::server("first")),
                ]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            ReliabilityConfig {
                max_attempts: 2,
                initial_backoff_ms: 10_000,
                max_backoff_ms: 10_000,
                ..ReliabilityConfig::default()
            },
        );

        let mut stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup should succeed");
        let next = tokio::time::timeout(Duration::from_millis(1), stream.next()).await;
        assert!(next.is_err(), "the retry wait should be cancelled");
        drop(stream);
        assert_eq!(mock.calls(), 1);

        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(mock.calls(), 1, "dropping the stream must cancel its retry");
    }

    fn circuit_config(threshold: u32, cooldown_ms: u64) -> ReliabilityConfig {
        ReliabilityConfig {
            max_attempts: 1,
            circuit_failure_threshold: threshold,
            circuit_cooldown_ms: cooldown_ms,
            ..ReliabilityConfig::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn circuit_opens_after_threshold_logical_failures_and_rejects_without_calling() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("first")),
                Behavior::Setup(ProviderFailure::server("second")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(2, 30_000),
        );

        for expected in ["first", "second"] {
            let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
                panic!("retryable setup failures must fail the logical call");
            };
            assert_eq!(error.to_string(), format!("LLM request failed: {expected}"));
        }
        assert_eq!(mock.calls(), 2);

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("an open circuit must reject the call");
        };
        match error {
            LlmError::Request { message } => {
                assert!(message.contains("circuit breaker open"), "{message}");
                assert!(message.contains("retry after"), "{message}");
            }
            other => panic!("expected a request error, got {other:?}"),
        }

        let Err(failure) = provider
            .stream_chat_classified(&[Message::user("hello")], &[])
            .await
        else {
            panic!("an open circuit must reject the call");
        };
        assert_eq!(failure.kind(), ProviderFailureKind::Server);
        assert!(
            !failure.is_retryable(),
            "an open circuit must not be retried by a caller"
        );
        let message = failure.message();
        assert!(message.contains("circuit breaker open"), "{message}");
        assert!(message.contains("retry after"), "{message}");
        assert_eq!(
            mock.calls(),
            2,
            "an open circuit must not touch the underlying provider"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_logical_call_with_three_attempts_counts_once() {
        let failures = || {
            vec![
                Behavior::Setup(ProviderFailure::server("attempt 1")),
                Behavior::Setup(ProviderFailure::server("attempt 2")),
                Behavior::Setup(ProviderFailure::server("attempt 3")),
            ]
        };
        let mut behaviors = failures();
        behaviors.extend(failures());
        behaviors.push(Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]));
        let (provider, mock) = scripted(
            behaviors,
            ReliabilityConfig {
                max_attempts: 3,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
                circuit_failure_threshold: 2,
                circuit_cooldown_ms: 30_000,
                ..ReliabilityConfig::default()
            },
        );

        for expected in [
            "provider request failed after 3 attempts: attempt 3",
            "provider request failed after 3 attempts: attempt 3",
        ] {
            let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
                panic!("all attempts must fail");
            };
            assert_eq!(error.to_string(), format!("LLM request failed: {expected}"));
        }
        assert_eq!(mock.calls(), 6, "each logical call issued three attempts");

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the circuit should now be open");
        };
        assert!(error.to_string().contains("circuit breaker open"));
        assert_eq!(
            mock.calls(),
            6,
            "two logical failures opened the circuit after six attempts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_client_failures_do_not_open_the_circuit() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::client("HTTP 401")),
                Behavior::Setup(ProviderFailure::client("HTTP 400")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(1, 30_000),
        );

        for expected in ["HTTP 401", "HTTP 400"] {
            let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
                panic!("client failures must still fail the call");
            };
            assert_eq!(error.to_string(), format!("LLM request failed: {expected}"));
        }

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("a reachable endpoint must still be admitted");
        assert_eq!(stream.collect::<Vec<_>>().await.len(), 1);
        assert_eq!(
            mock.calls(),
            3,
            "non-retryable 4xx failures must never open the circuit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn success_resets_the_failure_count() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("first")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
                Behavior::Setup(ProviderFailure::server("second")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(2, 30_000),
        );

        let Err(_) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the first failure should fail the call");
        };
        let first = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("a success after one failure must be admitted");
        first.collect::<Vec<_>>().await;

        let Err(_) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the post-success failure should fail the call");
        };
        let second = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("success must have reset the failure count");
        second.collect::<Vec<_>>().await;

        assert_eq!(mock.calls(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_terminal_failure_counts_as_one_logical_failure() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Stream(vec![Err(ProviderFailure::server("stream down"))]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(1, 30_000),
        );

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("setup succeeds; the stream itself fails");
        assert_eq!(
            stream.collect::<Vec<_>>().await,
            vec![LlmEvent::Error {
                message: "stream down".to_owned()
            }]
        );
        assert_eq!(mock.calls(), 1);

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("a terminal stream failure must count toward the circuit");
        };
        assert!(error.to_string().contains("circuit breaker open"));
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_calls_do_not_change_the_circuit() {
        let (provider, mock) = scripted(
            vec![
                Behavior::PendingSetup,
                Behavior::PendingStream,
                Behavior::Setup(ProviderFailure::server("counted")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(1, 30_000),
        );

        let result = tokio::time::timeout(
            Duration::from_millis(1),
            provider.stream_chat(&[Message::user("hello")], &[]),
        )
        .await;
        assert!(result.is_err(), "the pending setup should be cancelled");

        let stream = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("the pending stream setup should succeed");
        drop(stream);
        assert_eq!(mock.calls(), 2);

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the next failure should be counted");
        };
        assert_eq!(error.to_string(), "LLM request failed: counted");
        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("one counted failure must open a threshold-one circuit");
        };
        assert!(error.to_string().contains("circuit breaker open"));
        assert_eq!(
            mock.calls(),
            3,
            "dropped calls must neither count nor close the circuit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn half_open_allows_one_probe_and_drop_releases_the_slot() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("down")),
                Behavior::PendingStream,
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(1, 1_000),
        );

        let Err(_) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the first failure should open the circuit");
        };
        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the circuit must reject while open");
        };
        assert!(error.to_string().contains("circuit breaker open"));
        assert_eq!(mock.calls(), 1);

        tokio::time::advance(Duration::from_millis(1_000)).await;
        let probe = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("the half-open probe must be admitted");
        assert_eq!(mock.calls(), 2);

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("a second half-open call must fail fast");
        };
        assert!(
            error
                .to_string()
                .contains("half-open probe already in progress"),
            "{error}"
        );
        assert_eq!(mock.calls(), 2);

        drop(probe);
        let replacement = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("dropping the probe must release its slot");
        assert_eq!(
            replacement.collect::<Vec<_>>().await,
            vec![LlmEvent::Done { usage: None }]
        );
        assert_eq!(mock.calls(), 3);

        let after_success = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("a successful probe must close the circuit");
        after_success.collect::<Vec<_>>().await;
        assert_eq!(mock.calls(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn half_open_probe_failure_reopens_the_circuit() {
        let (provider, mock) = scripted(
            vec![
                Behavior::Setup(ProviderFailure::server("down")),
                Behavior::Setup(ProviderFailure::server("probe failed")),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
                Behavior::Stream(vec![Ok(LlmEvent::Done { usage: None })]),
            ],
            circuit_config(1, 1_000),
        );

        let Err(_) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the first failure should open the circuit");
        };
        tokio::time::advance(Duration::from_millis(1_000)).await;

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("the probe should fail");
        };
        assert_eq!(error.to_string(), "LLM request failed: probe failed");
        assert_eq!(mock.calls(), 2);

        let Err(error) = provider.stream_chat(&[Message::user("hello")], &[]).await else {
            panic!("a failed probe must reopen the circuit");
        };
        assert!(error.to_string().contains("circuit breaker open"));
        assert_eq!(mock.calls(), 2);

        tokio::time::advance(Duration::from_millis(1_000)).await;
        let recovery = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("the next probe should succeed");
        recovery.collect::<Vec<_>>().await;
        assert_eq!(mock.calls(), 3);

        let closed = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("a successful probe must close the circuit");
        closed.collect::<Vec<_>>().await;
        assert_eq!(mock.calls(), 4);
    }
}
