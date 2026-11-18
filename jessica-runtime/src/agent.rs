//! Agent-loop state machine.
//!
//! The loop is intentionally free of Phase 2 governance. Only the structural
//! round limit is enforced here; token, tool-timeout, stuck and result-size
//! limits are added by harness guards later.

use std::sync::Arc;

use futures::StreamExt;
use jessica_core::events::EventSink;
use jessica_core::llm::{LlmEvent, LlmProvider};
use jessica_core::tool::{ToolRegistry, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, BudgetKind, Message, SessionEndReason, SessionId, TokenUsage, ToolCall,
    ToolResult,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::recovery::SessionCheckpoint;

/// Result of a transcript-preserving agent-loop run.
///
/// The counters come from structured loop state, not from event text or from
/// parsing the transcript. `error` carries a provider, stream, or loop error;
/// cancellation and budget exhaustion are represented by `reason` alone.
///
/// On error, cancellation, or tool-call-limit paths the final `messages` may
/// stop at an incomplete tool batch. Callers must consult `reason`,
/// `tool_errors`, and `tool_call_limit_hit` before treating the transcript as a
/// completed conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptRun {
    /// Why the loop stopped.
    pub reason: SessionEndReason,
    /// Messages accumulated through the final completed step.
    pub messages: Vec<Message>,
    /// Model rounds completed during this run.
    pub rounds: u32,
    /// Tool calls requested by the model, including unknown tools.
    pub tool_calls: u32,
    /// Executed tool calls whose [`ToolResult::is_error`] was true.
    pub tool_errors: u32,
    /// First failed tool summary, at most 512 UTF-8 bytes.
    pub first_tool_error: Option<String>,
    /// Provider, stream, or loop termination error.
    pub error: Option<String>,
    /// Whether the loop stopped at its tool-call limit.
    pub tool_call_limit_hit: bool,
}

/// Drives model/tool rounds for a single session.
///
/// The loop owns its dependencies and can be reused for multiple sessions.
/// Every state transition is broadcast as an [`AgentEvent`]; [`AgentLoop::run`]
/// additionally returns the terminal reason.
///
/// `Budget::max_rounds` is the maximum number of model calls. When the final
/// allowed round still requests tools, those tools are executed as in any other
/// round, and the session then ends with [`SessionEndReason::BudgetExceeded`]
/// because the model can no longer see their results. The other budget
/// dimensions are enforced by Phase 2 guards, not by this loop.
pub struct AgentLoop {
    provider: Arc<dyn LlmProvider>,
    tools: Arc<ToolRegistry>,
    budget: Budget,
    events: broadcast::Sender<AgentEvent>,
    sink: Option<Arc<dyn EventSink>>,
}

impl AgentLoop {
    /// Creates an agent loop.
    ///
    /// `events` is the broadcast channel every state transition is sent to.
    /// A send with no subscribers is intentionally ignored so the loop never
    /// depends on an observer being present.
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        tools: Arc<ToolRegistry>,
        budget: Budget,
        events: broadcast::Sender<AgentEvent>,
    ) -> Self {
        Self {
            provider,
            tools,
            budget,
            events,
            sink: None,
        }
    }

    /// Attaches a producer-side event sink.
    ///
    /// When present, every event is offered to `sink` before it is broadcast.
    /// A sink error stops the loop immediately: the rejected event and every
    /// later event are neither recorded nor broadcast. The returned terminal
    /// reason is [`SessionEndReason::Cancelled`] with
    /// [`TranscriptRun::error`] set; the host is expected to inspect its own
    /// sink for the authoritative integrity failure.
    pub fn with_event_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Runs the loop until completion, budget exhaustion, cancellation or error.
    ///
    /// The emitted sequence is:
    ///
    /// 1. [`AgentEvent::SessionStarted`] carrying the initial messages
    /// 2. per round: [`AgentEvent::RoundStarted`], zero or more
    ///    [`AgentEvent::ModelDelta`]s, [`AgentEvent::RoundFinished`] with the
    ///    provider's usage, then for every requested tool an
    ///    [`AgentEvent::ToolCallStarted`] followed by
    ///    [`AgentEvent::ToolCallFinished`]
    /// 3. exactly one terminal sequence: [`AgentEvent::SessionEnded`], preceded
    ///    by [`AgentEvent::BudgetExceeded`] or [`AgentEvent::Error`] on those
    ///    paths.
    ///
    /// A round is one model call. Tool calls requested by that call execute
    /// after `RoundFinished` and before the next `RoundStarted`. A model call
    /// that fails or is cancelled emits no `RoundFinished`.
    ///
    /// Cancellation is checked before each model call, while waiting for the
    /// next model event, and while a tool runs. When it fires the session ends
    /// with [`SessionEndReason::Cancelled`]; a tool that was interrupted emits
    /// no `ToolCallFinished`.
    ///
    /// [`jessica_core::tool::Tool::execute`] only receives the arguments, so a
    /// tool cannot know the originating call id. The loop therefore stamps
    /// every result with the id of the call it answers before emitting or
    /// feeding it back to the model.
    ///
    /// Unknown tools are reported to the model as failed [`ToolResult`]s
    /// instead of terminating the session, allowing the model to correct
    /// itself.
    pub async fn run(
        &self,
        session_id: SessionId,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> SessionEndReason {
        self.run_loop(session_id, messages, 0, true, cancel, None)
            .await
            .reason
    }

    /// Runs the loop and returns the final transcript together with structured
    /// counters.
    ///
    /// `max_tool_calls` bounds the number of tool calls the model may request.
    /// The counter is incremented before each requested call is executed; the
    /// `(limit + 1)`-th request is not executed, sets
    /// [`TranscriptRun::tool_call_limit_hit`], and ends the run with
    /// [`SessionEndReason::Error`]. Remaining calls in that assistant message
    /// are left without tool results. Passing `None` disables the limit.
    ///
    /// The emitted event sequence is the same as [`AgentLoop::run`].
    pub async fn run_with_transcript(
        &self,
        session_id: SessionId,
        messages: Vec<Message>,
        max_tool_calls: Option<u32>,
        cancel: CancellationToken,
    ) -> TranscriptRun {
        self.run_loop(session_id, messages, 0, true, cancel, max_tool_calls)
            .await
    }

    /// Continues an interrupted session from a recovered [`SessionCheckpoint`].
    ///
    /// The checkpoint's messages and round count are used as-is: no model or
    /// tool call is replayed. The loop emits the same events the uninterrupted
    /// run would have emitted after the checkpoint, without a second
    /// [`AgentEvent::SessionStarted`], and it gets exactly the remaining round
    /// budget (`max_rounds - rounds_completed`).
    ///
    /// A checkpoint that already contains a terminal event returns the recorded
    /// [`SessionEndReason`] immediately and emits nothing, because there is no
    /// work left to continue.
    pub async fn resume(
        &self,
        checkpoint: SessionCheckpoint,
        cancel: CancellationToken,
    ) -> SessionEndReason {
        let (session_id, messages, rounds_completed, end_reason) = checkpoint.into_parts();
        if let Some(reason) = end_reason {
            return reason;
        }
        self.run_loop(session_id, messages, rounds_completed, false, cancel, None)
            .await
            .reason
    }

    /// Runs rounds until completion, cancellation, error, or budget exhaustion.
    ///
    /// `rounds_used` suppresses that many model calls from the round budget so
    /// a resumed loop spends no more than the session's original allowance.
    /// `emit_started` is false on resume because the starting event is already
    /// in the recorded stream.
    async fn run_loop(
        &self,
        session_id: SessionId,
        mut messages: Vec<Message>,
        rounds_used: u32,
        emit_started: bool,
        cancel: CancellationToken,
        max_tool_calls: Option<u32>,
    ) -> TranscriptRun {
        let mut state = RunState::default();

        if emit_started
            && !self
                .emit(AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    messages: messages.clone(),
                })
                .await
        {
            return aborted_run(messages, state);
        }

        let tool_schemas = self.tools.schemas();
        let remaining_rounds = self.budget.max_rounds.saturating_sub(rounds_used);

        for _ in 0..remaining_rounds {
            if cancel.is_cancelled() {
                return self
                    .finish(session_id, SessionEndReason::Cancelled, messages, state)
                    .await;
            }

            let round = rounds_used + state.rounds + 1;
            if !self.emit(AgentEvent::RoundStarted { round }).await {
                return aborted_run(messages, state);
            }

            let (content, tool_calls, usage) =
                match self.model_turn(&messages, &tool_schemas, &cancel).await {
                    ModelTurn::Completed {
                        content,
                        tool_calls,
                        usage,
                    } => (content, tool_calls, usage),
                    ModelTurn::Cancelled => {
                        return self
                            .finish(session_id, SessionEndReason::Cancelled, messages, state)
                            .await;
                    }
                    ModelTurn::Failed(message) => {
                        return self.fail_run(session_id, message, messages, state).await;
                    }
                    ModelTurn::SinkFailed => return aborted_run(messages, state),
                };

            state.rounds += 1;
            messages.push(assistant_message(content, &tool_calls));
            if !self.emit(AgentEvent::RoundFinished { round, usage }).await {
                return aborted_run(messages, state);
            }

            if tool_calls.is_empty() {
                return self
                    .finish(session_id, SessionEndReason::Completed, messages, state)
                    .await;
            }

            for call in tool_calls {
                state.tool_calls += 1;
                if let Some(max) = max_tool_calls
                    && state.tool_calls > max
                {
                    state.tool_call_limit_hit = true;
                    let message = format!("maximum tool calls ({max}) exceeded");
                    return self.fail_run(session_id, message, messages, state).await;
                }

                if !self
                    .emit(AgentEvent::ToolCallStarted { call: call.clone() })
                    .await
                {
                    return aborted_run(messages, state);
                }

                let result = match self.tools.get(&call.name) {
                    Some(tool) => {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {
                                return self
                                    .finish(
                                        session_id,
                                        SessionEndReason::Cancelled,
                                        messages,
                                        state,
                                    )
                                    .await;
                            }
                            result = tool.execute_with_call(&call) => result,
                        }
                    }
                    None => {
                        ToolResult::error(call.id.clone(), format!("unknown tool `{}`", call.name))
                    }
                };

                let result = ToolResult {
                    tool_call_id: call.id.clone(),
                    ..result
                };

                if result.is_error {
                    state.tool_errors += 1;
                    if state.first_tool_error.is_none() {
                        state.first_tool_error =
                            Some(summarize_tool_error(&call.name, &result.content));
                    }
                }

                if !self
                    .emit(AgentEvent::ToolCallFinished {
                        result: result.clone(),
                    })
                    .await
                {
                    return aborted_run(messages, state);
                }
                messages.push(tool_message(&call, &result));
            }
        }

        if !self
            .emit(AgentEvent::BudgetExceeded {
                kind: BudgetKind::MaxRounds,
                message: format!("maximum rounds ({}) exceeded", self.budget.max_rounds),
            })
            .await
        {
            return aborted_run(messages, state);
        }
        self.finish(
            session_id,
            SessionEndReason::BudgetExceeded,
            messages,
            state,
        )
        .await
    }

    /// Consumes one model response, emitting deltas as they arrive.
    async fn model_turn(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        cancel: &CancellationToken,
    ) -> ModelTurn {
        let mut stream = tokio::select! {
            biased;
            _ = cancel.cancelled() => return ModelTurn::Cancelled,
            response = self.provider.stream_chat(messages, tools) => match response {
                Ok(stream) => stream,
                Err(error) => return ModelTurn::Failed(error.to_string()),
            },
        };

        let mut content = String::new();
        let mut tool_calls = Vec::new();
        let usage: Option<TokenUsage>;

        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => return ModelTurn::Cancelled,
                event = stream.next() => event,
            };

            match event {
                Some(LlmEvent::TextDelta { text }) => {
                    content.push_str(&text);
                    if !self.emit(AgentEvent::ModelDelta { text }).await {
                        return ModelTurn::SinkFailed;
                    }
                }
                Some(LlmEvent::ToolCallRequested { call }) => tool_calls.push(call),
                Some(LlmEvent::Done { usage: done_usage }) => {
                    usage = done_usage;
                    break;
                }
                Some(LlmEvent::Error { message }) => return ModelTurn::Failed(message),
                None => {
                    return ModelTurn::Failed("LLM stream ended without a Done event".to_owned());
                }
            }
        }

        ModelTurn::Completed {
            content,
            tool_calls,
            usage,
        }
    }

    /// Offers `event` to the sink, then broadcasts it.
    ///
    /// Returns false when the sink rejected the event. The event is not
    /// broadcast in that case, so observers never see an unrecorded event.
    async fn emit(&self, event: AgentEvent) -> bool {
        if let Some(sink) = &self.sink
            && sink.emit(event.clone()).await.is_err()
        {
            return false;
        }
        let _ = self.events.send(event);
        true
    }

    /// Emits an error and the terminal event for that error.
    async fn fail_run(
        &self,
        session_id: SessionId,
        message: String,
        messages: Vec<Message>,
        mut state: RunState,
    ) -> TranscriptRun {
        if !self
            .emit(AgentEvent::Error {
                message: message.clone(),
            })
            .await
        {
            return aborted_run(messages, state);
        }
        state.error = Some(message);
        self.finish(session_id, SessionEndReason::Error, messages, state)
            .await
    }

    /// Emits the terminal event and assembles the transcript result.
    async fn finish(
        &self,
        session_id: SessionId,
        reason: SessionEndReason,
        messages: Vec<Message>,
        state: RunState,
    ) -> TranscriptRun {
        if !self
            .emit(AgentEvent::SessionEnded { session_id, reason })
            .await
        {
            return aborted_run(messages, state);
        }
        TranscriptRun {
            reason,
            messages,
            rounds: state.rounds,
            tool_calls: state.tool_calls,
            tool_errors: state.tool_errors,
            first_tool_error: state.first_tool_error,
            error: state.error,
            tool_call_limit_hit: state.tool_call_limit_hit,
        }
    }
}

/// Builds a transcript result for a run stopped by a sink failure.
///
/// No terminal event is emitted: the sink rejected it, and broadcasting it
/// would create an observable event that is absent from the trace.
fn aborted_run(messages: Vec<Message>, state: RunState) -> TranscriptRun {
    TranscriptRun {
        reason: SessionEndReason::Cancelled,
        messages,
        rounds: state.rounds,
        tool_calls: state.tool_calls,
        tool_errors: state.tool_errors,
        first_tool_error: state.first_tool_error,
        error: Some("event sink failed".to_owned()),
        tool_call_limit_hit: state.tool_call_limit_hit,
    }
}

/// Counters accumulated while a loop runs.
#[derive(Debug, Default)]
struct RunState {
    /// Completed model rounds.
    rounds: u32,
    /// Requested tool calls, including unknown tools and a refused limit-plus-one call.
    tool_calls: u32,
    /// Executed tool calls that returned an error.
    tool_errors: u32,
    /// First failed tool summary.
    first_tool_error: Option<String>,
    /// Structured termination error.
    error: Option<String>,
    /// Whether the tool-call limit stopped the loop.
    tool_call_limit_hit: bool,
}

/// Maximum UTF-8 bytes retained in [`TranscriptRun::first_tool_error`].
const TOOL_ERROR_SUMMARY_BYTES: usize = 512;

/// Summarizes a failed tool call with its name and a bounded error excerpt.
fn summarize_tool_error(name: &str, content: &str) -> String {
    let detail = content.trim();
    let detail = if detail.is_empty() {
        "tool returned an error without details"
    } else {
        detail
    };
    let mut summary = format!("tool `{name}`: {detail}");
    truncate_utf8(&mut summary, TOOL_ERROR_SUMMARY_BYTES);
    summary
}

/// Truncates `text` to at most `max_bytes` on a UTF-8 character boundary.
fn truncate_utf8(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

/// Outcome of one model turn.
enum ModelTurn {
    /// The stream completed with accumulated text and tool calls.
    Completed {
        /// Text accumulated from `TextDelta` events.
        content: String,
        /// Tool calls requested by the model, in arrival order.
        tool_calls: Vec<ToolCall>,
        /// Token usage reported by the closing `Done` event.
        usage: Option<TokenUsage>,
    },
    /// Cancellation interrupted the turn.
    Cancelled,
    /// The provider or its stream failed.
    Failed(String),
    /// The producer-side event sink rejected a model delta.
    SinkFailed,
}

/// Builds the assistant message for a completed model turn.
fn assistant_message(content: String, tool_calls: &[ToolCall]) -> Message {
    if tool_calls.is_empty() {
        Message::assistant(content)
    } else {
        Message::assistant_with_tool_calls(content, tool_calls.to_vec())
    }
}

/// Builds the tool message fed back to the model.
///
/// Tool failures are prefixed so the model can distinguish them from successful
/// output; the OpenAI message format has no structured error flag.
fn tool_message(call: &ToolCall, result: &ToolResult) -> Message {
    let content = if result.is_error {
        format!("tool error: {}", result.content)
    } else {
        result.content.clone()
    };
    Message::tool(call.id.clone(), content)
}
