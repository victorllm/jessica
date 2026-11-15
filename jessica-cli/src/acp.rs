//! Stable ACP v1 agent transport over stdio.
//!
//! Task ACP-0 implements only the transport and the `initialize` handshake.
//! Session methods are intentionally absent: the SDK answers every request
//! without a registered handler with JSON-RPC `-32601` (Method not found).
//!
//! Stdout is reserved for newline-delimited JSON-RPC. This module never writes
//! to stdout itself, and the CLI installs no tracing subscriber, so ACP
//! diagnostics cannot corrupt the protocol stream.

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::schema::{
    AgentCapabilities, Implementation, InitializeRequest, InitializeResponse, McpCapabilities,
    PromptCapabilities, SessionCapabilities,
};
use agent_client_protocol::{Agent, Stdio};
use tokio::time::timeout;

use crate::app::CliError;
use crate::shutdown::ShutdownController;

/// Maximum time allowed for the transport to flush after a shutdown signal.
///
/// ACP-0 owns no sessions, so the only work that can remain in flight is an
/// `initialize` response: 250 ms is ample for writing one buffered line. The
/// same bound keeps a signal from hanging on a client that never closes stdin.
/// Session cancellation and draining arrive with ACP-2, which must revisit
/// this bound together with its own shutdown semantics.
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_millis(250);

/// Serves stable ACP v1 over stdin/stdout until EOF or shutdown.
///
/// A clean stdin EOF is a successful termination. On SIGINT or SIGTERM the
/// caller's [`ShutdownController`] is cancelled; this function then returns
/// after at most [`SHUTDOWN_GRACE_PERIOD`], which leaves time for responses
/// that were already generated to flush. ACP-0 has no long-running work, so
/// there is nothing else to drain yet.
pub(crate) async fn run(shutdown: Arc<ShutdownController>) -> Result<(), CliError> {
    let connection = Agent
        .v1()
        .name("jessica")
        .on_receive_request(
            async move |request: InitializeRequest, responder, _connection| {
                responder.respond(initialize_response(&request))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_to(Stdio::new());
    tokio::pin!(connection);

    tokio::select! {
        // A signal that is already recorded takes precedence when the client
        // closes stdin at the same time: the signal determines the exit code.
        biased;
        _ = shutdown.cancelled() => {
            match timeout(SHUTDOWN_GRACE_PERIOD, &mut connection).await {
                Ok(result) => result.map_err(CliError::Acp),
                Err(_) => Ok(()),
            }
        }
        result = &mut connection => result.map_err(CliError::Acp),
    }
}

/// Builds the `initialize` response for one client request.
///
/// Jessica supports only stable ACP v1. A client that requests a different
/// version receives v1, as the v1 schema requires when the requested version
/// is unsupported; the client decides whether to disconnect. All optional
/// capabilities are explicitly disabled so clients never probe unsupported
/// content types, MCP transports, or session lifecycle methods.
fn initialize_response(request: &InitializeRequest) -> InitializeResponse {
    tracing::debug!(
        requested_protocol_version = request.protocol_version.as_u16(),
        supported_protocol_version = ProtocolVersion::V1.as_u16(),
        "acp initialize"
    );

    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(
            AgentCapabilities::new()
                .load_session(false)
                .prompt_capabilities(
                    PromptCapabilities::new()
                        .image(false)
                        .audio(false)
                        .embedded_context(false),
                )
                .mcp_capabilities(McpCapabilities::new().http(false).sse(false))
                .session_capabilities(SessionCapabilities::new()),
        )
        .auth_methods(Vec::new())
        .agent_info(Implementation::new("jessica", env!("CARGO_PKG_VERSION")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_advertises_v1_and_no_optional_capabilities() {
        let response = initialize_response(&InitializeRequest::new(ProtocolVersion::V1));

        assert_eq!(response.protocol_version, ProtocolVersion::V1);
        assert!(!response.agent_capabilities.load_session);
        assert!(!response.agent_capabilities.prompt_capabilities.image);
        assert!(!response.agent_capabilities.prompt_capabilities.audio);
        assert!(
            !response
                .agent_capabilities
                .prompt_capabilities
                .embedded_context
        );
        assert!(!response.agent_capabilities.mcp_capabilities.http);
        assert!(!response.agent_capabilities.mcp_capabilities.sse);
        assert!(response.auth_methods.is_empty());
        let info = response
            .agent_info
            .expect("initialize must identify the agent");
        assert_eq!(info.name, "jessica");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn initialize_with_a_different_version_still_answers_with_v1() {
        for requested in [
            ProtocolVersion::V0,
            ProtocolVersion::from(2_u16),
            ProtocolVersion::from(999_u16),
        ] {
            let response = initialize_response(&InitializeRequest::new(requested));

            assert_eq!(
                response.protocol_version,
                ProtocolVersion::V1,
                "the agent must answer with its latest supported version, not the requested one"
            );
        }
    }
}
