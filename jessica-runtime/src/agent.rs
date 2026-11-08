//! Agent-loop state machine.
//!
//! The loop is intentionally free of Phase 2 governance. Only the structural
//! round limit is enforced here; token, tool-timeout, stuck and result-size
//! limits are added by harness guards later.

use std::sync::Arc;

use futures::StreamExt;
use jessica_core::llm::{LlmEvent, LlmProvider};
use jessica_core::tool::{ToolRegistry, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, BudgetKind, Message, SessionEndReason, SessionId, ToolCall, ToolResult,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

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
        }
    }

    /// Runs the loop until completion, budget exhaustion, cancellation or error.
    ///
    /// The emitted sequence is:
    ///
    /// 1. [`AgentEvent::SessionStarted`]
    /// 2. per round: [`AgentEvent::ModelDelta`]s, then for every requested tool
    ///    an [`AgentEvent::ToolCallStarted`] followed by
    ///    [`AgentEvent::ToolCallFinished`]
    /// 3. exactly one terminal sequence: [`AgentEvent::SessionEnded`], preceded
    ///    by [`AgentEvent::BudgetExceeded`] or [`AgentEvent::Error`] on those
    ///    paths.
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
        mut messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> SessionEndReason {
        self.emit(AgentEvent::SessionStarted {
            session_id: session_id.clone(),
        });

        let tool_schemas = self.tools.schemas();

        for _ in 0..self.budget.max_rounds {
            if cancel.is_cancelled() {
                return self.end(session_id, SessionEndReason::Cancelled);
            }

            let (content, tool_calls) =
                match self.model_turn(&messages, &tool_schemas, &cancel).await {
                    ModelTurn::Completed {
                        content,
                        tool_calls,
                    } => (content, tool_calls),
                    ModelTurn::Cancelled => {
                        return self.end(session_id, SessionEndReason::Cancelled);
                    }
                    ModelTurn::Failed(message) => return self.fail(session_id, message),
                };

            messages.push(assistant_message(content, &tool_calls));

            if tool_calls.is_empty() {
                return self.end(session_id, SessionEndReason::Completed);
            }

            for call in tool_calls {
                self.emit(AgentEvent::ToolCallStarted { call: call.clone() });

                let result = match self.tools.get(&call.name) {
                    Some(tool) => {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {
                                return self.end(session_id, SessionEndReason::Cancelled);
                            }
                            result = tool.execute(call.arguments.clone()) => result,
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

                self.emit(AgentEvent::ToolCallFinished {
                    result: result.clone(),
                });
                messages.push(tool_message(&call, &result));
            }
        }

        self.emit(AgentEvent::BudgetExceeded {
            kind: BudgetKind::MaxRounds,
            message: format!("maximum rounds ({}) exceeded", self.budget.max_rounds),
        });
        self.end(session_id, SessionEndReason::BudgetExceeded)
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

        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => return ModelTurn::Cancelled,
                event = stream.next() => event,
            };

            match event {
                Some(LlmEvent::TextDelta { text }) => {
                    content.push_str(&text);
                    self.emit(AgentEvent::ModelDelta { text });
                }
                Some(LlmEvent::ToolCallRequested { call }) => tool_calls.push(call),
                Some(LlmEvent::Done { .. }) => break,
                Some(LlmEvent::Error { message }) => return ModelTurn::Failed(message),
                None => {
                    return ModelTurn::Failed("LLM stream ended without a Done event".to_owned());
                }
            }
        }

        ModelTurn::Completed {
            content,
            tool_calls,
        }
    }

    /// Broadcasts `event`, ignoring the absence of subscribers.
    fn emit(&self, event: AgentEvent) {
        let _ = self.events.send(event);
    }

    /// Emits an error and the terminal event for that error.
    fn fail(&self, session_id: SessionId, message: String) -> SessionEndReason {
        self.emit(AgentEvent::Error { message });
        self.end(session_id, SessionEndReason::Error)
    }

    /// Emits the terminal event and returns its reason.
    fn end(&self, session_id: SessionId, reason: SessionEndReason) -> SessionEndReason {
        self.emit(AgentEvent::SessionEnded { session_id, reason });
        reason
    }
}

/// Outcome of one model turn.
enum ModelTurn {
    /// The stream completed with accumulated text and tool calls.
    Completed {
        /// Text accumulated from `TextDelta` events.
        content: String,
        /// Tool calls requested by the model, in arrival order.
        tool_calls: Vec<ToolCall>,
    },
    /// Cancellation interrupted the turn.
    Cancelled,
    /// The provider or its stream failed.
    Failed(String),
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
