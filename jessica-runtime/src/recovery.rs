//! Rebuilding a session checkpoint from a recorded event stream.
//!
//! [`recover_session`] is a pure reducer over [`RecordedEvent`]s. It never
//! calls a model or a tool: every message and every round count comes from
//! events that were already observed and persisted. Continuing an interrupted
//! session therefore cannot re-execute side effects.
//!
//! Only stable checkpoints can be recovered. A slice that ends during a model
//! turn ([`RecoveryError::IncompleteModelTurn`]) or between a tool call and its
//! result ([`RecoveryError::IncompleteRound`]) cannot be continued without
//! replaying the missing work, so recovery rejects it instead of guessing.
//!
//! [`AgentEvent`] has no user-message variant. The caller supplies the
//! session's bootstrap messages (system/user prompts) from the store's session
//! metadata; recovery appends everything the model and tools produced.

use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use thiserror::Error;

/// One event together with its position in the session's recorded stream.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedEvent {
    /// One-based position within the session's event stream.
    seq: u64,
    /// Recorded event.
    event: AgentEvent,
}

impl RecordedEvent {
    /// Creates a recorded event at `seq`.
    pub fn new(seq: u64, event: AgentEvent) -> Self {
        Self { seq, event }
    }

    /// Returns the one-based position within the session's event stream.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the recorded event.
    pub fn event(&self) -> &AgentEvent {
        &self.event
    }
}

/// Conversation and loop state reconstructed through one sequence number.
///
/// The checkpoint contains everything [`crate::AgentLoop`] needs to continue:
/// the session id, the exact messages the next model call must see, the number
/// of model rounds already spent, and the terminal reason when the session had
/// already ended.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionCheckpoint {
    /// Session the stream belongs to.
    session_id: SessionId,
    /// Conversation rebuilt from the bootstrap messages and recorded events.
    messages: Vec<Message>,
    /// Number of model rounds whose output appears in the stream.
    rounds_completed: u32,
    /// Sequence number of the last event included in the checkpoint.
    last_seq: u64,
    /// Terminal reason when the checkpoint includes a `SessionEnded` event.
    end_reason: Option<SessionEndReason>,
}

impl SessionCheckpoint {
    /// Returns the session the checkpoint belongs to.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the conversation rebuilt from the event stream.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Returns how many model rounds the recorded stream already contains.
    ///
    /// A resumed loop subtracts this from `Budget::max_rounds`, so an
    /// interrupted session keeps the same total round budget as an
    /// uninterrupted one.
    ///
    /// The event model has no explicit round boundary: a round is observable
    /// only through its model text and tool calls. Consecutive model turns that
    /// emit tool calls without any text are therefore indistinguishable from a
    /// single turn requesting several tools, and are counted as one round.
    /// Recovery prefers this deterministic undercount over inventing boundaries
    /// that the log does not contain.
    pub fn rounds_completed(&self) -> u32 {
        self.rounds_completed
    }

    /// Returns the sequence number of the last event included.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Returns the terminal reason when the recorded session already ended.
    pub fn end_reason(&self) -> Option<SessionEndReason> {
        self.end_reason
    }

    /// Decomposes the checkpoint into the values needed to continue the loop.
    pub(crate) fn into_parts(self) -> (SessionId, Vec<Message>, u32, Option<SessionEndReason>) {
        (
            self.session_id,
            self.messages,
            self.rounds_completed,
            self.end_reason,
        )
    }
}

/// Rebuilds a session checkpoint by replaying `events` through `through_seq`.
///
/// `initial_messages` are the messages the session started with, as recorded in
/// the store's session metadata. Events are included while
/// `event.seq() <= through_seq`; pass `None` to include the whole stream. The
/// slice must begin with [`AgentEvent::SessionStarted`], sequence numbers must
/// strictly increase, and the session id in `SessionStarted`/`SessionEnded`
/// must match `session_id`.
///
/// The returned checkpoint ends after a complete tool batch or at a terminal
/// event. A slice that stops in the middle of a model turn or tool batch is
/// rejected, because continuing it would require re-running the model or the
/// tool.
pub fn recover_session(
    session_id: &SessionId,
    initial_messages: Vec<Message>,
    events: &[RecordedEvent],
    through_seq: Option<u64>,
) -> Result<SessionCheckpoint, RecoveryError> {
    RecoveryBuilder::new(session_id, initial_messages).apply(events, through_seq)
}

/// One tool call and, once observed, its result.
#[derive(Debug)]
struct PendingCall {
    /// Call announced by `ToolCallStarted`.
    call: ToolCall,
    /// Result announced by `ToolCallFinished`.
    result: Option<ToolResult>,
}

/// Incremental reducer behind [`recover_session`].
struct RecoveryBuilder<'a> {
    /// Session being recovered.
    session_id: &'a SessionId,
    /// Conversation rebuilt so far.
    messages: Vec<Message>,
    /// Model rounds begun so far.
    rounds_completed: u32,
    /// Whether a round is currently accumulating output.
    round_open: bool,
    /// Text accumulated for the open round.
    round_output: String,
    /// Tool calls of the open round.
    pending: Vec<PendingCall>,
    /// Terminal reason observed so far.
    end_reason: Option<SessionEndReason>,
    /// Sequence number of the last event included.
    last_seq: u64,
    /// Whether a `SessionStarted` event has been seen.
    started: bool,
}

impl<'a> RecoveryBuilder<'a> {
    /// Creates a builder seeded with the session's bootstrap messages.
    fn new(session_id: &'a SessionId, messages: Vec<Message>) -> Self {
        Self {
            session_id,
            messages,
            rounds_completed: 0,
            round_open: false,
            round_output: String::new(),
            pending: Vec::new(),
            end_reason: None,
            last_seq: 0,
            started: false,
        }
    }

    /// Applies the stream and produces a checkpoint or an integrity error.
    fn apply(
        mut self,
        events: &[RecordedEvent],
        through_seq: Option<u64>,
    ) -> Result<SessionCheckpoint, RecoveryError> {
        let mut previous_seq = None;
        for recorded in events {
            let seq = recorded.seq();
            if through_seq.is_some_and(|limit| seq > limit) {
                continue;
            }
            if let Some(previous) = previous_seq
                && seq <= previous
            {
                return Err(RecoveryError::NonIncreasingSequence {
                    previous,
                    next: seq,
                });
            }
            previous_seq = Some(seq);

            let event = recorded.event();
            if !self.started && !matches!(event, AgentEvent::SessionStarted { .. }) {
                return Err(RecoveryError::MissingSessionStart);
            }
            self.last_seq = seq;
            self.on_event(seq, event)?;
        }

        if !self.started {
            return Err(RecoveryError::MissingSessionStart);
        }
        self.finish_slice()
    }

    /// Dispatches one event to the matching reducer step.
    fn on_event(&mut self, seq: u64, event: &AgentEvent) -> Result<(), RecoveryError> {
        match event {
            AgentEvent::SessionStarted { session_id } => self.on_session_started(seq, session_id),
            AgentEvent::ModelDelta { text } => {
                self.ensure_running(seq)?;
                self.on_model_delta(seq, text)
            }
            AgentEvent::ToolCallStarted { call } => {
                self.ensure_running(seq)?;
                self.on_tool_started(seq, call)
            }
            AgentEvent::ToolCallFinished { result } => {
                self.ensure_running(seq)?;
                self.on_tool_finished(seq, result)
            }
            AgentEvent::SessionEnded { session_id, reason } => {
                self.on_session_ended(seq, session_id, *reason)
            }
            AgentEvent::PolicyDenied { .. }
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => {
                self.ensure_running(seq)?;
                Ok(())
            }
        }
    }

    /// Validates a repeated or initial `SessionStarted`.
    fn on_session_started(
        &mut self,
        seq: u64,
        session_id: &SessionId,
    ) -> Result<(), RecoveryError> {
        if session_id != self.session_id {
            return Err(RecoveryError::SessionMismatch {
                expected: self.session_id.to_string(),
                found: session_id.to_string(),
            });
        }
        if self.started && self.end_reason.is_none() {
            return Err(RecoveryError::UnexpectedSessionStart { seq });
        }
        self.started = true;
        self.end_reason = None;
        Ok(())
    }

    /// Rejects any event that appears after the session's terminal event.
    fn ensure_running(&self, seq: u64) -> Result<(), RecoveryError> {
        if self.end_reason.is_some() {
            return Err(RecoveryError::EventAfterSessionEnd { seq });
        }
        Ok(())
    }

    /// Appends text to the current round, opening or rotating rounds as needed.
    fn on_model_delta(&mut self, seq: u64, text: &str) -> Result<(), RecoveryError> {
        if !self.round_open {
            self.begin_round();
        } else if !self.pending.is_empty() {
            // A delta after a complete tool batch is the next model turn; a
            // delta while a result is still missing is an inconsistent stream.
            if let Some(missing) = self.pending.iter().find(|pending| pending.result.is_none()) {
                return Err(RecoveryError::IncompleteRound {
                    seq,
                    tool_call_id: missing.call.id.clone(),
                });
            }
            self.flush_round();
            self.begin_round();
        }
        self.round_output.push_str(text);
        Ok(())
    }

    /// Records a tool call, opening a round when it is the first event.
    fn on_tool_started(&mut self, seq: u64, call: &ToolCall) -> Result<(), RecoveryError> {
        if !self.round_open {
            self.begin_round();
        }
        if let Some(missing) = self.pending.iter().find(|pending| pending.result.is_none()) {
            return Err(RecoveryError::IncompleteRound {
                seq,
                tool_call_id: missing.call.id.clone(),
            });
        }
        if self
            .pending
            .iter()
            .any(|pending| pending.call.id == call.id)
        {
            return Err(RecoveryError::DuplicateToolCall {
                seq,
                tool_call_id: call.id.clone(),
            });
        }
        self.pending.push(PendingCall {
            call: call.clone(),
            result: None,
        });
        Ok(())
    }

    /// Pairs a result with its announced call.
    fn on_tool_finished(&mut self, seq: u64, result: &ToolResult) -> Result<(), RecoveryError> {
        let pending = self
            .pending
            .iter_mut()
            .find(|pending| pending.call.id == result.tool_call_id)
            .ok_or_else(|| RecoveryError::UnknownToolResult {
                seq,
                tool_call_id: result.tool_call_id.clone(),
            })?;
        if pending.result.is_some() {
            return Err(RecoveryError::DuplicateToolResult {
                seq,
                tool_call_id: result.tool_call_id.clone(),
            });
        }
        pending.result = Some(result.clone());
        Ok(())
    }

    /// Finalizes the transcript at a terminal event.
    fn on_session_ended(
        &mut self,
        seq: u64,
        session_id: &SessionId,
        reason: SessionEndReason,
    ) -> Result<(), RecoveryError> {
        if session_id != self.session_id {
            return Err(RecoveryError::SessionMismatch {
                expected: self.session_id.to_string(),
                found: session_id.to_string(),
            });
        }
        self.ensure_running(seq)?;

        if reason == SessionEndReason::Completed {
            if self.pending.is_empty() {
                if self.round_open {
                    let output = std::mem::take(&mut self.round_output);
                    self.messages.push(Message::assistant(output));
                }
            } else {
                // The runtime never completes with an outstanding tool call;
                // reject rather than inventing a result.
                self.flush_completed(seq)?;
            }
        } else {
            // Cancellation, budget exhaustion, acceptance failure, and errors
            // can stop between a call and its result. The interrupted call is
            // recorded the same way the CLI transcript records it.
            if !self.pending.is_empty() {
                self.flush_round();
            }
            self.round_output.clear();
        }

        self.round_open = false;
        self.end_reason = Some(reason);
        Ok(())
    }

    /// Starts a model round and accounts for its model call.
    fn begin_round(&mut self) {
        self.rounds_completed = self.rounds_completed.saturating_add(1);
        self.round_output.clear();
        self.round_open = true;
    }

    /// Flushes a batch whose results are all present.
    fn flush_completed(&mut self, seq: u64) -> Result<(), RecoveryError> {
        if let Some(missing) = self.pending.iter().find(|pending| pending.result.is_none()) {
            return Err(RecoveryError::IncompleteRound {
                seq,
                tool_call_id: missing.call.id.clone(),
            });
        }
        self.flush_round();
        Ok(())
    }

    /// Converts the open round into assistant and tool messages.
    ///
    /// Missing results become the same synthetic error the CLI transcript uses,
    /// so an interrupted batch is still representable at a terminal boundary.
    fn flush_round(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let calls = self
            .pending
            .iter()
            .map(|pending| pending.call.clone())
            .collect();
        let output = std::mem::take(&mut self.round_output);
        self.messages
            .push(Message::assistant_with_tool_calls(output, calls));
        for pending in self.pending.drain(..) {
            let content = match pending.result {
                Some(result) if result.is_error => format!("tool error: {}", result.content),
                Some(result) => result.content,
                None => "tool error: tool execution was interrupted".to_owned(),
            };
            self.messages.push(Message::tool(pending.call.id, content));
        }
        self.round_open = false;
    }

    /// Validates that a slice without a terminal event ends at a stable point.
    fn finish_slice(self) -> Result<SessionCheckpoint, RecoveryError> {
        let mut builder = self;
        if !builder.pending.is_empty() {
            if let Some(missing) = builder
                .pending
                .iter()
                .find(|pending| pending.result.is_none())
            {
                return Err(RecoveryError::IncompleteRound {
                    seq: builder.last_seq,
                    tool_call_id: missing.call.id.clone(),
                });
            }
            builder.flush_round();
        } else if builder.round_open {
            return Err(RecoveryError::IncompleteModelTurn {
                seq: builder.last_seq,
            });
        }

        Ok(SessionCheckpoint {
            session_id: builder.session_id.clone(),
            messages: builder.messages,
            rounds_completed: builder.rounds_completed,
            last_seq: builder.last_seq,
            end_reason: builder.end_reason,
        })
    }
}

/// Errors reported when a recorded stream cannot be replayed safely.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoveryError {
    /// The slice is empty or does not start with `SessionStarted`.
    #[error("the recorded stream does not begin with a session_started event")]
    MissingSessionStart,
    /// An event belongs to a different session than the one requested.
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
    /// A second session started before the first one ended.
    #[error("event {seq} starts a session before the previous one ended")]
    UnexpectedSessionStart {
        /// Sequence number of the offending event.
        seq: u64,
    },
    /// An event appears after the terminal event.
    #[error("event {seq} appears after the session ended")]
    EventAfterSessionEnd {
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
    /// A tool call has no result at a checkpoint or round boundary.
    #[error("event {seq} leaves tool call `{tool_call_id}` without a result")]
    IncompleteRound {
        /// Sequence number at which the gap was detected.
        seq: u64,
        /// Call id waiting for a result.
        tool_call_id: String,
    },
    /// The slice ends during a model turn.
    #[error("event {seq} ends in the middle of a model turn")]
    IncompleteModelTurn {
        /// Sequence number of the last included event.
        seq: u64,
    },
}
