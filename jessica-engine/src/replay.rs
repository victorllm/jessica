//! Offline deterministic replay of a recorded session.
//!
//! Replay reconstructs the model rounds and tool results from the recorded
//! event stream, then drives a real [`AgentLoop`] with a scripted provider and
//! a tool that returns the recorded results. No model request is issued and no
//! tool is executed again: the inputs are the bytes already stored in the
//! event log.
//!
//! The output is a newline-delimited JSON event stream. Nothing on this path
//! reads the host clock, generates randomness, or touches the network, so two
//! replays of the same database produce byte-identical files.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolRegistry, ToolRegistryError, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, SessionEndReason, SessionId, TokenUsage, ToolCall, ToolResult,
};
use jessica_runtime::{AgentLoop, RecordedEvent, SessionCheckpoint};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::recorder::StoredSession;

/// Number of events buffered while the replay loop produces its stream.
const EVENT_BUFFER: usize = 1024;

/// Replays a stored session and returns the events produced by the loop.
///
/// The returned vector is a fresh [`AgentLoop`] run driven entirely by the
/// recorded model output and tool results. Recording-level bookkeeping events
/// that do not influence the loop (`PolicyDenied`, `AcceptanceFailed`, and a
/// `BudgetExceeded` preceding the terminal event) are not re-emitted; the tool
/// result or terminal reason they explain is already part of the script.
///
/// The loop always emits the version-2 round vocabulary and attaches bootstrap
/// messages to `SessionStarted`. Version-1 recordings contain neither, so those
/// synthesized fields are removed before returning to keep the replay output
/// byte-identical to the stream it reproduces.
pub async fn replay_session(
    session_id: &SessionId,
    stored: &StoredSession,
) -> Result<Vec<AgentEvent>, ReplayError> {
    replay_from(session_id, stored, ReplayStart::Fresh).await
}

/// Replays a resumed session from its recorded checkpoint state.
///
/// Resumed traces contain no `SessionStarted`; they begin directly with the
/// model rounds that followed the checkpoint. `stored.initial_messages` is the
/// checkpoint conversation, and `rounds_completed` is the number of rounds the
/// original session had already spent before resuming. The replay drives
/// [`jessica_runtime::AgentLoop::resume`] instead of a fresh run and gives the
/// loop the same total round budget as the original session.
pub async fn replay_resumed_session(
    session_id: &SessionId,
    stored: &StoredSession,
    rounds_completed: u32,
) -> Result<Vec<AgentEvent>, ReplayError> {
    replay_from(
        session_id,
        stored,
        ReplayStart::Resumed { rounds_completed },
    )
    .await
}

/// How a replay begins relative to the recorded stream.
#[derive(Debug, Clone, Copy)]
enum ReplayStart {
    /// A fresh session that begins with `SessionStarted`.
    Fresh,
    /// A session resumed from a checkpoint recorded outside the trace.
    Resumed {
        /// Rounds the original session completed before resuming.
        rounds_completed: u32,
    },
}

/// Shared implementation for fresh and resumed replays.
async fn replay_from(
    session_id: &SessionId,
    stored: &StoredSession,
    start: ReplayStart,
) -> Result<Vec<AgentEvent>, ReplayError> {
    let resumed_rounds = match start {
        ReplayStart::Fresh => None,
        ReplayStart::Resumed { rounds_completed } => Some(rounds_completed),
    };
    let script = match resumed_rounds {
        Some(_) => Script::build_resumed(session_id, &stored.events)?,
        None => Script::build(session_id, &stored.events)?,
    };
    let Script {
        provider_rounds,
        tool_results,
        end_reason,
        max_rounds,
        explicit_rounds,
        started_with_messages,
    } = script;

    let total_rounds = match resumed_rounds {
        Some(rounds_completed) => rounds_completed
            .checked_add(max_rounds)
            .ok_or(ReplayError::TooManyRounds)?,
        None => max_rounds,
    };

    let results = Arc::new(RecordedResults::new(tool_results));
    let registry = Arc::new(build_registry(&results)?);
    let provider = Arc::new(ScriptedProvider::new(provider_rounds));
    let (sender, receiver) = broadcast::channel(EVENT_BUFFER);
    let agent = AgentLoop::new(
        Arc::clone(&provider) as Arc<dyn LlmProvider>,
        registry,
        Budget::new(u64::MAX, Duration::ZERO, total_rounds),
        sender,
    );

    let collector = tokio::spawn(collect_events(receiver));
    let reason = match resumed_rounds {
        Some(rounds_completed) => {
            let checkpoint = SessionCheckpoint::from_recorded_state(
                session_id.clone(),
                stored.initial_messages.clone(),
                rounds_completed,
            );
            agent.resume(checkpoint, CancellationToken::new()).await
        }
        None => {
            agent
                .run(
                    session_id.clone(),
                    stored.initial_messages.clone(),
                    CancellationToken::new(),
                )
                .await
        }
    };
    drop(agent);
    let events = collector.await.map_err(ReplayError::Task)??;

    if provider.was_exhausted() {
        return Err(ReplayError::ScriptExhausted);
    }
    if let Some(tool) = results.missing() {
        return Err(ReplayError::MissingToolResult { tool });
    }
    let remaining = provider.remaining();
    if remaining != 0 {
        return Err(ReplayError::UnusedScriptRounds { remaining });
    }
    let remaining = results.remaining();
    if remaining != 0 {
        return Err(ReplayError::UnusedToolResults { remaining });
    }
    if reason != end_reason {
        return Err(ReplayError::UnexpectedEndReason {
            expected: end_reason,
            actual: reason,
        });
    }

    Ok(restore_recorded_vocabulary(
        events,
        explicit_rounds,
        started_with_messages,
    ))
}

/// Removes version-2 fields that the recorded stream did not contain.
///
/// A version-1 stream has no round lifecycle and carries no bootstrap messages
/// in `SessionStarted`. The loop under replay always produces both, so they are
/// stripped to keep the output faithful to the source vocabulary.
fn restore_recorded_vocabulary(
    events: Vec<AgentEvent>,
    explicit_rounds: bool,
    started_with_messages: bool,
) -> Vec<AgentEvent> {
    events
        .into_iter()
        .filter_map(|event| match event {
            AgentEvent::RoundStarted { .. } | AgentEvent::RoundFinished { .. }
                if !explicit_rounds =>
            {
                None
            }
            AgentEvent::SessionStarted {
                session_id,
                messages: _,
            } if !started_with_messages => Some(AgentEvent::SessionStarted {
                session_id,
                messages: Vec::new(),
            }),
            event => Some(event),
        })
        .collect()
}

/// Serializes events as JSONL, one compact event object per line.
pub fn render_jsonl(events: &[AgentEvent]) -> Result<Vec<u8>, ReplayError> {
    let mut output = Vec::new();
    for event in events {
        let line = serde_json::to_string(event)?;
        output.extend_from_slice(line.as_bytes());
        output.push(b'\n');
    }
    Ok(output)
}

/// Writes the replayed event stream to `path`, truncating any existing file.
pub async fn write_jsonl(path: PathBuf, events: Vec<AgentEvent>) -> Result<(), ReplayError> {
    tokio::task::spawn_blocking(move || {
        let output = render_jsonl(&events)?;
        std::fs::write(&path, output).map_err(|source| ReplayError::Output { path, source })
    })
    .await
    .map_err(ReplayError::Task)?
}

/// Collects the loop's broadcast events until the sender is dropped.
async fn collect_events(
    mut receiver: broadcast::Receiver<AgentEvent>,
) -> Result<Vec<AgentEvent>, ReplayError> {
    let mut events = Vec::new();
    loop {
        match receiver.recv().await {
            Ok(event) => events.push(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                return Err(ReplayError::EventLag { skipped });
            }
            Err(broadcast::error::RecvError::Closed) => return Ok(events),
        }
    }
}

/// Provider that yields one pre-recorded model response per loop call.
struct ScriptedProvider {
    /// Remaining response streams, in model-call order.
    rounds: Mutex<VecDeque<Vec<LlmEvent>>>,
    /// Whether the loop asked for more responses than were recorded.
    exhausted: AtomicBool,
}

impl ScriptedProvider {
    /// Creates a provider from the scripted model rounds.
    fn new(rounds: Vec<Vec<LlmEvent>>) -> Self {
        Self {
            rounds: Mutex::new(rounds.into()),
            exhausted: AtomicBool::new(false),
        }
    }

    /// Returns the number of unconsumed scripted rounds.
    fn remaining(&self) -> usize {
        self.lock_rounds().len()
    }

    /// Returns whether the loop exhausted the script.
    fn was_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Relaxed)
    }

    /// Locks the round queue, recovering from a poisoned mutex.
    fn lock_rounds(&self) -> std::sync::MutexGuard<'_, VecDeque<Vec<LlmEvent>>> {
        self.rounds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn stream_chat(
        &self,
        _messages: &[jessica_core::types::Message],
        _tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        match self.lock_rounds().pop_front() {
            Some(events) => Ok(Box::pin(futures::stream::iter(events))),
            None => {
                self.exhausted.store(true, Ordering::Relaxed);
                Err(LlmError::Request {
                    message: "the replay script has no further model rounds".to_owned(),
                })
            }
        }
    }
}

/// Recorded tool results shared by every replay tool.
struct RecordedResults {
    /// Per-tool result queues in recorded call order.
    queues: BTreeMap<String, Mutex<VecDeque<ToolResult>>>,
    /// First tool that was called after its queue ran out.
    missing: Mutex<Option<String>>,
}

impl RecordedResults {
    /// Creates shared result queues.
    fn new(results: BTreeMap<String, VecDeque<ToolResult>>) -> Self {
        let queues = results
            .into_iter()
            .map(|(name, results)| (name, Mutex::new(results)))
            .collect();
        Self {
            queues,
            missing: Mutex::new(None),
        }
    }

    /// Returns the tool names in deterministic order.
    fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.queues.keys().map(String::as_str)
    }

    /// Pops the next recorded result for `tool`.
    fn take(&self, tool: &str) -> Option<ToolResult> {
        let queue = self.queues.get(tool)?;
        let mut queue = queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queue.pop_front()
    }

    /// Records that `tool` had no result left.
    fn mark_missing(&self, tool: &str) {
        let mut missing = self
            .missing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if missing.is_none() {
            *missing = Some(tool.to_owned());
        }
    }

    /// Returns the first tool whose queue ran out.
    fn missing(&self) -> Option<String> {
        self.missing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Returns the number of unconsumed results.
    fn remaining(&self) -> usize {
        self.queues
            .values()
            .map(|queue| {
                queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len()
            })
            .sum()
    }
}

/// Tool that returns the next recorded result for its name.
struct ReplayTool {
    /// Tool name as recorded.
    name: String,
    /// Shared recorded results.
    results: Arc<RecordedResults>,
}

#[async_trait]
impl Tool for ReplayTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "parameters": {"type": "object"}
            }
        })
    }

    async fn execute(&self, _args: Value) -> ToolResult {
        match self.results.take(&self.name) {
            Some(result) => result,
            None => {
                self.results.mark_missing(&self.name);
                ToolResult::error(
                    "",
                    format!("no recorded result for replay tool `{}`", self.name),
                )
            }
        }
    }
}

/// Registers one replay tool per recorded tool name.
fn build_registry(results: &Arc<RecordedResults>) -> Result<ToolRegistry, ReplayError> {
    let mut registry = ToolRegistry::new();
    for name in results.names() {
        let tool = ReplayTool {
            name: name.to_owned(),
            results: Arc::clone(results),
        };
        if let Err(source) = registry.register(Arc::new(tool)) {
            return Err(ReplayError::ToolRegistry {
                name: name.to_owned(),
                source,
            });
        }
    }
    Ok(registry)
}

/// Interpreted replay inputs.
struct Script {
    /// One event stream per recorded model call.
    provider_rounds: Vec<Vec<LlmEvent>>,
    /// Recorded tool results grouped by tool name and call order.
    tool_results: BTreeMap<String, VecDeque<ToolResult>>,
    /// Terminal reason the replay must reproduce.
    end_reason: SessionEndReason,
    /// Round budget that makes the loop reproduce `end_reason`.
    max_rounds: u32,
    /// Whether the recorded stream used explicit round events.
    explicit_rounds: bool,
    /// Whether the recorded `SessionStarted` carried bootstrap messages.
    started_with_messages: bool,
}

impl Script {
    /// Parses the recorded stream into replay inputs.
    fn build(session_id: &SessionId, events: &[RecordedEvent]) -> Result<Self, ReplayError> {
        ScriptBuilder::new(session_id).apply(events)
    }

    /// Parses a resumed stream that has no `SessionStarted`.
    fn build_resumed(
        session_id: &SessionId,
        events: &[RecordedEvent],
    ) -> Result<Self, ReplayError> {
        ScriptBuilder::new(session_id)
            .without_session_start()
            .apply(events)
    }
}

/// One model turn being reconstructed.
#[derive(Default)]
struct ReplayRound {
    /// Provider events for this turn, without the closing `Done`.
    events: Vec<LlmEvent>,
    /// Tool calls requested by this turn, in arrival order.
    calls: Vec<ToolCall>,
    /// Recorded results parallel to `calls`.
    results: Vec<Option<ToolResult>>,
    /// Whether the turn ended with a provider error.
    failed: bool,
    /// Usage reported by the explicit `RoundFinished` event, when present.
    ///
    /// Version-1 streams have no usage and leave this as `None`.
    usage: Option<TokenUsage>,
}

impl ReplayRound {
    /// Returns whether the round has no observable events.
    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.calls.is_empty()
    }

    /// Returns the first call still waiting for a result.
    fn pending(&self) -> Option<&str> {
        self.calls
            .iter()
            .zip(&self.results)
            .find(|(_, result)| result.is_none())
            .map(|(call, _)| call.id.as_str())
    }
}

/// Incremental parser for the recorded event stream.
struct ScriptBuilder<'a> {
    /// Session being replayed.
    session_id: &'a SessionId,
    /// Finished rounds.
    rounds: Vec<ReplayRound>,
    /// Round currently being reconstructed.
    current: ReplayRound,
    /// Whether the initial `SessionStarted` was seen.
    started: bool,
    /// Whether the stream must begin with `SessionStarted`.
    requires_session_start: bool,
    /// Terminal reason once observed.
    ended: Option<SessionEndReason>,
    /// Previous sequence number for monotonicity checks.
    previous_seq: Option<u64>,
    /// Highest explicit round started in this stream, if any.
    ///
    /// `None` means the stream predates explicit round events and the original
    /// heuristic round reconstruction is used.
    explicit_round: Option<u32>,
    /// Whether the explicit round is still waiting for `RoundFinished`.
    explicit_open: bool,
    /// Whether the recorded `SessionStarted` carried bootstrap messages.
    started_with_messages: bool,
}

impl<'a> ScriptBuilder<'a> {
    /// Creates an empty builder for `session_id`.
    fn new(session_id: &'a SessionId) -> Self {
        Self {
            session_id,
            rounds: Vec::new(),
            current: ReplayRound::default(),
            started: false,
            requires_session_start: true,
            ended: None,
            previous_seq: None,
            explicit_round: None,
            explicit_open: false,
            started_with_messages: false,
        }
    }

    /// Configures a resumed stream that begins directly with model rounds.
    fn without_session_start(mut self) -> Self {
        self.started = true;
        self.requires_session_start = false;
        self
    }

    /// Parses `events` into a replay script.
    fn apply(mut self, events: &[RecordedEvent]) -> Result<Script, ReplayError> {
        for recorded in events {
            let seq = recorded.seq();
            if let Some(previous) = self.previous_seq
                && seq <= previous
            {
                return Err(ReplayError::NonIncreasingSequence {
                    previous,
                    next: seq,
                });
            }
            self.previous_seq = Some(seq);

            if self.ended.is_some() {
                return Err(ReplayError::EventAfterSessionEnd { seq });
            }
            let event = recorded.event();
            if self.requires_session_start
                && !self.started
                && !matches!(event, AgentEvent::SessionStarted { .. })
            {
                return Err(ReplayError::MissingSessionStart);
            }
            self.on_event(seq, event)?;
        }
        self.finish()
    }

    /// Dispatches one event to its parser step.
    fn on_event(&mut self, seq: u64, event: &AgentEvent) -> Result<(), ReplayError> {
        match event {
            AgentEvent::SessionStarted {
                session_id,
                messages,
            } => self.on_started(seq, session_id, !messages.is_empty()),
            AgentEvent::RoundStarted { round } => self.on_round_started(seq, *round),
            AgentEvent::RoundFinished { round, usage } => {
                self.on_round_finished(seq, *round, *usage)
            }
            AgentEvent::ModelDelta { text } => self.on_delta(seq, text),
            AgentEvent::ToolCallStarted { call } => self.on_call(seq, call),
            AgentEvent::ToolCallFinished { result } => self.on_result(seq, result),
            AgentEvent::Error { message } => self.on_error(seq, message),
            AgentEvent::SessionEnded { session_id, reason } => {
                self.on_end(seq, session_id, *reason)
            }
            AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. }
            | AgentEvent::PolicyDenied { .. }
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. } => Ok(()),
        }
    }

    /// Starts an explicitly numbered model round.
    fn on_round_started(&mut self, seq: u64, round: u32) -> Result<(), ReplayError> {
        if round == 0 {
            return Err(ReplayError::InvalidRoundNumber { seq, round });
        }
        if self.explicit_open {
            return Err(ReplayError::UnexpectedRoundStart { seq, round });
        }
        if let Some(previous) = self.explicit_round {
            let expected = previous.checked_add(1).ok_or(ReplayError::TooManyRounds)?;
            if round != expected {
                return Err(ReplayError::RoundSequence {
                    seq,
                    expected,
                    found: round,
                });
            }
            if let Some(tool_call_id) = self.current.pending() {
                return Err(ReplayError::IncompleteRound {
                    seq,
                    tool_call_id: tool_call_id.to_owned(),
                });
            }
            // The finished round includes tool calls and results that arrive
            // after `RoundFinished`; rotate it now, even when it produced no
            // text or calls, because every explicit round is a model call.
            self.rounds.push(std::mem::take(&mut self.current));
        } else if !self.current.is_empty() {
            return Err(ReplayError::RoundVocabulary {
                seq,
                message: "`round_started` follows events outside an explicit round".to_owned(),
            });
        }
        self.explicit_round = Some(round);
        self.explicit_open = true;
        Ok(())
    }

    /// Closes the explicit round and records its usage.
    fn on_round_finished(
        &mut self,
        seq: u64,
        round: u32,
        usage: Option<TokenUsage>,
    ) -> Result<(), ReplayError> {
        match self.explicit_round {
            Some(started) if started == round => {}
            Some(started) => {
                return Err(ReplayError::RoundSequence {
                    seq,
                    expected: started,
                    found: round,
                });
            }
            None => return Err(ReplayError::UnexpectedRoundFinish { seq, round }),
        }
        if !self.explicit_open {
            return Err(ReplayError::UnexpectedRoundFinish { seq, round });
        }
        self.current.usage = usage;
        self.explicit_open = false;
        Ok(())
    }

    /// Validates the initial session start.
    fn on_started(
        &mut self,
        seq: u64,
        session_id: &SessionId,
        with_messages: bool,
    ) -> Result<(), ReplayError> {
        if session_id != self.session_id {
            return Err(ReplayError::SessionMismatch {
                expected: self.session_id.to_string(),
                found: session_id.to_string(),
            });
        }
        if self.started {
            return Err(ReplayError::UnexpectedSessionStart { seq });
        }
        self.started = true;
        self.started_with_messages = with_messages;
        Ok(())
    }

    /// Appends model text, rotating rounds after a complete tool batch.
    fn on_delta(&mut self, seq: u64, text: &str) -> Result<(), ReplayError> {
        if let Some(round) = self.explicit_round {
            if !self.explicit_open {
                return Err(ReplayError::RoundNotOpen { seq, round });
            }
        } else {
            self.prepare_turn(seq)?;
        }
        self.current.events.push(LlmEvent::TextDelta {
            text: text.to_owned(),
        });
        Ok(())
    }

    /// Records a requested tool call.
    fn on_call(&mut self, seq: u64, call: &ToolCall) -> Result<(), ReplayError> {
        if let Some(round) = self.explicit_round {
            if self.explicit_open {
                // The runtime emits `RoundFinished` before executing tools.
                return Err(ReplayError::RoundNotFinished { seq, round });
            }
        } else {
            self.prepare_turn(seq)?;
        }
        if self
            .current
            .calls
            .iter()
            .any(|pending| pending.id == call.id)
        {
            return Err(ReplayError::DuplicateToolCall {
                seq,
                tool_call_id: call.id.clone(),
            });
        }
        self.current.calls.push(call.clone());
        self.current.results.push(None);
        self.current
            .events
            .push(LlmEvent::ToolCallRequested { call: call.clone() });
        Ok(())
    }

    /// Pairs a recorded result with its announced call.
    fn on_result(&mut self, seq: u64, result: &ToolResult) -> Result<(), ReplayError> {
        if self.current.failed {
            return Err(ReplayError::EventAfterError { seq });
        }
        let index = self
            .current
            .calls
            .iter()
            .position(|call| call.id == result.tool_call_id)
            .ok_or_else(|| ReplayError::UnknownToolResult {
                seq,
                tool_call_id: result.tool_call_id.clone(),
            })?;
        if self.current.results[index].is_some() {
            return Err(ReplayError::DuplicateToolResult {
                seq,
                tool_call_id: result.tool_call_id.clone(),
            });
        }
        self.current.results[index] = Some(result.clone());
        Ok(())
    }

    /// Appends a provider error event that will terminate the model turn.
    fn on_error(&mut self, seq: u64, message: &str) -> Result<(), ReplayError> {
        if let Some(round) = self.explicit_round {
            if !self.explicit_open {
                return Err(ReplayError::RoundNotOpen { seq, round });
            }
        } else {
            self.prepare_turn(seq)?;
        }
        self.current.events.push(LlmEvent::Error {
            message: message.to_owned(),
        });
        self.current.failed = true;
        Ok(())
    }

    /// Validates the terminal event and finalizes the last round.
    fn on_end(
        &mut self,
        seq: u64,
        session_id: &SessionId,
        reason: SessionEndReason,
    ) -> Result<(), ReplayError> {
        if session_id != self.session_id {
            return Err(ReplayError::SessionMismatch {
                expected: self.session_id.to_string(),
                found: session_id.to_string(),
            });
        }
        if let Some(tool_call_id) = self.current.pending() {
            return Err(ReplayError::IncompleteRound {
                seq,
                tool_call_id: tool_call_id.to_owned(),
            });
        }
        if self.explicit_round.is_some() {
            return self.finish_explicit(seq, reason);
        }
        match reason {
            SessionEndReason::Completed => self.finish_completed()?,
            SessionEndReason::Error => self.finish_error()?,
            SessionEndReason::BudgetExceeded => self.finish_budget()?,
            SessionEndReason::Cancelled | SessionEndReason::AcceptanceFailed => {
                return Err(ReplayError::UnsupportedEndReason { reason });
            }
        }
        self.ended = Some(reason);
        Ok(())
    }

    /// Finalizes the last explicit round without v1 heuristic padding.
    fn finish_explicit(&mut self, seq: u64, reason: SessionEndReason) -> Result<(), ReplayError> {
        let Some(round) = self.explicit_round else {
            return Err(ReplayError::RoundVocabulary {
                seq,
                message: "an explicit round ended without a `round_started` event".to_owned(),
            });
        };
        match reason {
            SessionEndReason::Completed => {
                if self.explicit_open {
                    return Err(ReplayError::MissingRoundFinish { seq, round });
                }
                if self.current.failed {
                    return Err(ReplayError::InconsistentEndReason { reason });
                }
            }
            SessionEndReason::BudgetExceeded => {
                if self.explicit_open {
                    return Err(ReplayError::MissingRoundFinish { seq, round });
                }
                if self.current.failed
                    || (!self.current.is_empty() && self.current.calls.is_empty())
                {
                    return Err(ReplayError::InconsistentEndReason { reason });
                }
            }
            SessionEndReason::Error => {
                if !self.current.failed {
                    return Err(ReplayError::InconsistentEndReason { reason });
                }
            }
            SessionEndReason::Cancelled | SessionEndReason::AcceptanceFailed => {
                return Err(ReplayError::UnsupportedEndReason { reason });
            }
        }
        // Every explicit `round_started` is a model call, including one with an
        // empty response, so the terminal round is rotated unconditionally.
        self.rounds.push(std::mem::take(&mut self.current));
        self.ended = Some(reason);
        Ok(())
    }

    /// Finalizes a normally completed session.
    fn finish_completed(&mut self) -> Result<(), ReplayError> {
        if self.current.failed {
            return Err(ReplayError::InconsistentEndReason {
                reason: SessionEndReason::Completed,
            });
        }
        if self.current.is_empty() {
            // The final model response contained neither text nor tool calls;
            // the loop still made the call, so script one empty round.
            self.rounds.push(ReplayRound::default());
            return Ok(());
        }
        let had_calls = !self.current.calls.is_empty();
        self.rotate();
        if had_calls {
            // The recorded stream cannot show the empty model turn that
            // followed the final tool batch, so add it to reach `Completed`.
            self.rounds.push(ReplayRound::default());
        }
        Ok(())
    }

    /// Finalizes a session terminated by a provider error.
    fn finish_error(&mut self) -> Result<(), ReplayError> {
        if !self.current.failed {
            return Err(ReplayError::InconsistentEndReason {
                reason: SessionEndReason::Error,
            });
        }
        self.rotate();
        Ok(())
    }

    /// Finalizes a session that exhausted its round budget.
    fn finish_budget(&mut self) -> Result<(), ReplayError> {
        if self.current.failed || (!self.current.is_empty() && self.current.calls.is_empty()) {
            return Err(ReplayError::InconsistentEndReason {
                reason: SessionEndReason::BudgetExceeded,
            });
        }
        self.rotate();
        Ok(())
    }

    /// Rotates to a new model turn when the previous tool batch is complete.
    fn prepare_turn(&mut self, seq: u64) -> Result<(), ReplayError> {
        if self.current.failed {
            return Err(ReplayError::EventAfterError { seq });
        }
        if !self.current.calls.is_empty() {
            if let Some(tool_call_id) = self.current.pending() {
                return Err(ReplayError::IncompleteRound {
                    seq,
                    tool_call_id: tool_call_id.to_owned(),
                });
            }
            self.rotate();
        }
        Ok(())
    }

    /// Pushes the current round when it contains observable events.
    fn rotate(&mut self) {
        let round = std::mem::take(&mut self.current);
        if !round.is_empty() {
            self.rounds.push(round);
        }
    }

    /// Terminates every round and builds the final script.
    fn finish(self) -> Result<Script, ReplayError> {
        let Some(end_reason) = self.ended else {
            return Err(ReplayError::MissingSessionEnd);
        };
        let explicit_rounds = self.explicit_round.is_some();
        let started_with_messages = self.started_with_messages;
        let mut provider_rounds = Vec::with_capacity(self.rounds.len());
        let mut tool_results: BTreeMap<String, VecDeque<ToolResult>> = BTreeMap::new();

        for mut round in self.rounds {
            if !round.failed {
                // Version-1 recorded streams carry no usage; explicit
                // `RoundFinished` events preserve the provider's value.
                let usage = round.usage;
                round.events.push(LlmEvent::Done { usage });
            }
            for (call, result) in round.calls.iter().zip(round.results.iter()) {
                let Some(result) = result else {
                    return Err(ReplayError::MissingToolResult {
                        tool: call.name.clone(),
                    });
                };
                tool_results
                    .entry(call.name.clone())
                    .or_default()
                    .push_back(result.clone());
            }
            provider_rounds.push(round.events);
        }

        let max_rounds =
            u32::try_from(provider_rounds.len()).map_err(|_| ReplayError::TooManyRounds)?;
        Ok(Script {
            provider_rounds,
            tool_results,
            end_reason,
            max_rounds,
            explicit_rounds,
            started_with_messages,
        })
    }
}

/// Errors raised while reconstructing or running a replay.
#[derive(Debug, Error)]
pub enum ReplayError {
    /// The stream is empty or does not start with `SessionStarted`.
    #[error("the recorded stream does not begin with a session_started event")]
    MissingSessionStart,
    /// An event belongs to a different session.
    #[error("session `{found}` appears in the stream but `{expected}` was requested")]
    SessionMismatch {
        /// Session id requested by the caller.
        expected: String,
        /// Session id found in the event.
        found: String,
    },
    /// Sequence numbers are not strictly increasing.
    #[error("event sequences must strictly increase: {previous} is followed by {next}")]
    NonIncreasingSequence {
        /// Previous sequence number.
        previous: u64,
        /// Sequence number that did not increase.
        next: u64,
    },
    /// A second `SessionStarted` appears.
    #[error("event {seq} starts a session a second time")]
    UnexpectedSessionStart {
        /// Sequence number of the offending event.
        seq: u64,
    },
    /// A round number is zero.
    #[error("event {seq} starts invalid round {round}; rounds are one-based")]
    InvalidRoundNumber {
        /// Sequence number of the offending event.
        seq: u64,
        /// Reported round number.
        round: u32,
    },
    /// Two explicit rounds are not consecutive.
    ///
    /// A resumed session may start at a number above one, but every following
    /// round must increment by exactly one.
    #[error("event {seq} reports round {found}, but the expected round is {expected}")]
    RoundSequence {
        /// Sequence number of the offending event.
        seq: u64,
        /// Expected round number.
        expected: u32,
        /// Reported round number.
        found: u32,
    },
    /// A `RoundStarted` arrived while the previous explicit round was open.
    #[error("event {seq} starts round {round} while another round is still open")]
    UnexpectedRoundStart {
        /// Sequence number of the offending event.
        seq: u64,
        /// Reported round number.
        round: u32,
    },
    /// A `RoundFinished` has no matching open round.
    #[error("event {seq} finishes round {round} without a matching open round")]
    UnexpectedRoundFinish {
        /// Sequence number of the offending event.
        seq: u64,
        /// Reported round number.
        round: u32,
    },
    /// Model output arrived outside an open explicit round.
    #[error("event {seq} belongs to round {round}, which is not open")]
    RoundNotOpen {
        /// Sequence number of the offending event.
        seq: u64,
        /// Round number of the closed round.
        round: u32,
    },
    /// Tool execution started before the model round finished.
    #[error("event {seq} starts a tool before round {round} finished")]
    RoundNotFinished {
        /// Sequence number of the offending event.
        seq: u64,
        /// Still-open round number.
        round: u32,
    },
    /// An explicit round has no `RoundFinished` at the terminal boundary.
    #[error("event {seq} ends round {round} without a `round_finished` event")]
    MissingRoundFinish {
        /// Sequence number of the terminal event.
        seq: u64,
        /// Round that never finished.
        round: u32,
    },
    /// Explicit and legacy round vocabulary are mixed in one stream.
    #[error("event {seq} has inconsistent round vocabulary: {message}")]
    RoundVocabulary {
        /// Sequence number of the offending event.
        seq: u64,
        /// Description of the inconsistency.
        message: String,
    },
    /// An event appears after the terminal event.
    #[error("event {seq} appears after the session ended")]
    EventAfterSessionEnd {
        /// Sequence number of the offending event.
        seq: u64,
    },
    /// An event appears after the model reported an error.
    #[error("event {seq} appears after the model reported an error")]
    EventAfterError {
        /// Sequence number of the offending event.
        seq: u64,
    },
    /// A tool call id was announced twice in one round.
    #[error("event {seq} announces tool call `{tool_call_id}` twice")]
    DuplicateToolCall {
        /// Sequence number of the offending event.
        seq: u64,
        /// Repeated call id.
        tool_call_id: String,
    },
    /// A result arrived without a matching announced call.
    #[error("event {seq} reports a result for unknown tool call `{tool_call_id}`")]
    UnknownToolResult {
        /// Sequence number of the offending event.
        seq: u64,
        /// Unknown call id.
        tool_call_id: String,
    },
    /// A call received a second result.
    #[error("event {seq} reports a second result for tool call `{tool_call_id}`")]
    DuplicateToolResult {
        /// Sequence number of the offending event.
        seq: u64,
        /// Call id with the duplicate result.
        tool_call_id: String,
    },
    /// A tool call has no result at a turn or terminal boundary.
    #[error("event {seq} leaves tool call `{tool_call_id}` without a result")]
    IncompleteRound {
        /// Sequence number at which the gap was detected.
        seq: u64,
        /// Call id waiting for a result.
        tool_call_id: String,
    },
    /// The stream has no terminal event.
    #[error("the recorded stream has no session_ended event")]
    MissingSessionEnd,
    /// The recorded terminal event cannot be produced from its rounds.
    #[error("the recorded rounds cannot produce the terminal reason `{reason:?}`")]
    InconsistentEndReason {
        /// Terminal reason recorded in the stream.
        reason: SessionEndReason,
    },
    /// The terminal reason is outside the replay contract.
    #[error("replay does not support sessions ending with `{reason:?}`")]
    UnsupportedEndReason {
        /// Terminal reason recorded in the stream.
        reason: SessionEndReason,
    },
    /// The loop asked for more model rounds than were recorded.
    #[error("the replay script ran out of model rounds")]
    ScriptExhausted,
    /// Scripted model rounds were left unused.
    #[error("the replay loop did not consume {remaining} scripted model rounds")]
    UnusedScriptRounds {
        /// Number of unconsumed rounds.
        remaining: usize,
    },
    /// Recorded tool results were left unused.
    #[error("the replay loop did not consume {remaining} recorded tool results")]
    UnusedToolResults {
        /// Number of unconsumed results.
        remaining: usize,
    },
    /// A tool was called after its recorded result queue ran out.
    #[error("tool `{tool}` was called without a recorded result")]
    MissingToolResult {
        /// Tool whose queue was empty.
        tool: String,
    },
    /// The loop ended for a different reason than the recording.
    #[error("the replay loop ended with `{actual:?}` but the recording says `{expected:?}`")]
    UnexpectedEndReason {
        /// Reason recorded in the event stream.
        expected: SessionEndReason,
        /// Reason produced by the replay loop.
        actual: SessionEndReason,
    },
    /// The bounded event buffer overflowed.
    #[error("the replay event buffer lagged by {skipped} events")]
    EventLag {
        /// Number of events lost to the lag.
        skipped: u64,
    },
    /// The stream contains more rounds than `u32` can represent.
    #[error("the recorded session has too many model rounds to replay")]
    TooManyRounds,
    /// A replay tool could not be registered.
    #[error("failed to register replay tool `{name}`: {source}")]
    ToolRegistry {
        /// Tool name that could not be registered.
        name: String,
        /// Underlying registry error.
        #[source]
        source: ToolRegistryError,
    },
    /// An event could not be serialized.
    #[error("failed to serialize a replayed event: {0}")]
    Json(#[from] serde_json::Error),
    /// The JSONL file could not be written.
    #[error("failed to write replay output `{path}`: {source}")]
    Output {
        /// Output path.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: io::Error,
    },
    /// The blocking output task failed.
    #[error("replay output task failed: {0}")]
    Task(#[source] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use jessica_core::types::{BudgetKind, Message};

    fn session_id() -> SessionId {
        SessionId::new("replay-test")
    }

    fn stored(events: Vec<AgentEvent>) -> StoredSession {
        let recorded = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| RecordedEvent::new(index as u64 + 1, event))
            .collect();
        StoredSession {
            initial_messages: vec![Message::user("replay this")],
            events: recorded,
        }
    }

    /// Replays twice and requires exact event and byte equality with the input.
    async fn assert_replays_exactly(events: Vec<AgentEvent>) {
        let stored = stored(events.clone());
        let first = replay_session(&session_id(), &stored)
            .await
            .expect("a consistent completed stream must replay");
        assert_eq!(first, events, "replay must reproduce the recorded events");
        let second = replay_session(&session_id(), &stored)
            .await
            .expect("replay must be repeatable");
        assert_eq!(second, first);
        assert_eq!(
            render_jsonl(&first).expect("events should serialize"),
            render_jsonl(&second).expect("events should serialize"),
            "two replays must produce byte-identical JSONL"
        );
    }

    #[tokio::test]
    async fn replay_reproduces_recorded_events_without_running_tools_or_the_model() {
        let call = ToolCall::new("call-1", "read_file", json!({"path": "Cargo.toml"}));
        let usage = TokenUsage::new(11, 7);
        assert_replays_exactly(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: vec![Message::user("replay this")],
            },
            AgentEvent::RoundStarted { round: 1 },
            AgentEvent::ModelDelta {
                text: "reading".to_owned(),
            },
            AgentEvent::RoundFinished {
                round: 1,
                usage: Some(usage),
            },
            AgentEvent::ToolCallStarted { call: call.clone() },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "recorded output"),
            },
            AgentEvent::RoundStarted { round: 2 },
            AgentEvent::ModelDelta {
                text: "done".to_owned(),
            },
            AgentEvent::RoundFinished {
                round: 2,
                usage: None,
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Completed,
            },
        ])
        .await;
    }

    #[tokio::test]
    async fn replay_covers_empty_final_turns_budget_and_error_terminals() {
        // A completed session whose final model response emitted nothing: the
        // explicit final round has no text or calls.
        assert_replays_exactly(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: vec![Message::user("replay this")],
            },
            AgentEvent::RoundStarted { round: 1 },
            AgentEvent::RoundFinished {
                round: 1,
                usage: None,
            },
            AgentEvent::ToolCallStarted {
                call: ToolCall::new("call-1", "echo", json!({})),
            },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "ok"),
            },
            AgentEvent::RoundStarted { round: 2 },
            AgentEvent::RoundFinished {
                round: 2,
                usage: None,
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Completed,
            },
        ])
        .await;

        // Round-budget exhaustion is reproduced by the loop's own max_rounds.
        assert_replays_exactly(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: vec![Message::user("replay this")],
            },
            AgentEvent::RoundStarted { round: 1 },
            AgentEvent::RoundFinished {
                round: 1,
                usage: None,
            },
            AgentEvent::ToolCallStarted {
                call: ToolCall::new("call-1", "echo", json!({})),
            },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "ok"),
            },
            AgentEvent::BudgetExceeded {
                kind: BudgetKind::MaxRounds,
                message: "maximum rounds (1) exceeded".to_owned(),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::BudgetExceeded,
            },
        ])
        .await;

        // Provider errors are scripted back into the model stream. The failed
        // round never emits `RoundFinished`.
        assert_replays_exactly(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: vec![Message::user("replay this")],
            },
            AgentEvent::RoundStarted { round: 1 },
            AgentEvent::ModelDelta {
                text: "partial".to_owned(),
            },
            AgentEvent::Error {
                message: "provider failed".to_owned(),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Error,
            },
        ])
        .await;
    }

    #[tokio::test]
    async fn incomplete_or_unsupported_recordings_are_rejected() {
        let incomplete = stored(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: Vec::new(),
            },
            AgentEvent::ToolCallStarted {
                call: ToolCall::new("call-1", "echo", json!({})),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Completed,
            },
        ]);
        assert!(matches!(
            replay_session(&session_id(), &incomplete).await,
            Err(ReplayError::IncompleteRound { tool_call_id, .. }) if tool_call_id == "call-1"
        ));

        let cancelled = stored(vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: Vec::new(),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Cancelled,
            },
        ]);
        assert!(matches!(
            replay_session(&session_id(), &cancelled).await,
            Err(ReplayError::UnsupportedEndReason {
                reason: SessionEndReason::Cancelled
            })
        ));

        let unterminated = stored(vec![AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        }]);
        assert!(matches!(
            replay_session(&session_id(), &unterminated).await,
            Err(ReplayError::MissingSessionEnd)
        ));
    }

    #[tokio::test]
    async fn version_one_streams_use_the_heuristic_and_replay_without_usage() {
        let call = ToolCall::new("call-1", "read_file", json!({"path": "Cargo.toml"}));
        let events = vec![
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: Vec::new(),
            },
            AgentEvent::ModelDelta {
                text: "reading".to_owned(),
            },
            AgentEvent::ToolCallStarted { call: call.clone() },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "recorded output"),
            },
            AgentEvent::ModelDelta {
                text: "done".to_owned(),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Completed,
            },
        ];

        let stored = stored(events.clone());
        let script = Script::build(&session_id(), &stored.events)
            .expect("a version-1 stream must build the heuristic script");
        assert!(!script.explicit_rounds);
        assert!(!script.started_with_messages);
        assert_eq!(script.provider_rounds.len(), 2);
        for round in &script.provider_rounds {
            assert!(
                matches!(round.last(), Some(LlmEvent::Done { usage: None })),
                "a version-1 stream has no usage: {round:?}"
            );
        }

        assert_replays_exactly(events).await;
    }

    fn resumed_stored(events: Vec<AgentEvent>) -> StoredSession {
        let recorded = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| RecordedEvent::new(index as u64 + 1, event))
            .collect();
        StoredSession {
            initial_messages: vec![
                Message::user("checkpoint question"),
                Message::assistant("checkpoint answer"),
            ],
            events: recorded,
        }
    }

    #[tokio::test]
    async fn resumed_replay_continues_from_the_recorded_round() {
        let events = vec![
            AgentEvent::RoundStarted { round: 3 },
            AgentEvent::ModelDelta {
                text: "continuing".to_owned(),
            },
            AgentEvent::RoundFinished {
                round: 3,
                usage: None,
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::Completed,
            },
        ];
        let stored = resumed_stored(events.clone());

        let replayed = replay_resumed_session(&session_id(), &stored, 2)
            .await
            .expect("a resumed stream must continue from its checkpoint");
        assert_eq!(replayed, events);
        assert!(
            !replayed
                .iter()
                .any(|event| matches!(event, AgentEvent::SessionStarted { .. })),
            "a resumed loop must not invent a session start"
        );
    }

    #[tokio::test]
    async fn resumed_replay_preserves_the_original_round_budget() {
        let call = ToolCall::new("call-1", "echo", json!({}));
        let events = vec![
            AgentEvent::RoundStarted { round: 3 },
            AgentEvent::RoundFinished {
                round: 3,
                usage: None,
            },
            AgentEvent::ToolCallStarted { call: call.clone() },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "ok"),
            },
            AgentEvent::RoundStarted { round: 4 },
            AgentEvent::RoundFinished {
                round: 4,
                usage: None,
            },
            AgentEvent::ToolCallStarted { call: call.clone() },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "ok"),
            },
            AgentEvent::BudgetExceeded {
                kind: BudgetKind::MaxRounds,
                message: "maximum rounds (4) exceeded".to_owned(),
            },
            AgentEvent::SessionEnded {
                session_id: session_id(),
                reason: SessionEndReason::BudgetExceeded,
            },
        ];
        let stored = resumed_stored(events.clone());

        let replayed = replay_resumed_session(&session_id(), &stored, 2)
            .await
            .expect("resumed replay must reproduce budget exhaustion");
        assert_eq!(replayed, events);
    }
}
