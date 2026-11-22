//! Minimal MCP stdio client used by ACP `session/new`.
//!
//! ACP v1 requires agents to support stdio MCP servers. This module starts one
//! child process per configured server, performs the MCP initialize handshake,
//! discovers tools (including cursor pagination), and exposes each tool as a
//! namespaced [`Tool`]. HTTP and SSE transports are rejected by the ACP layer;
//! only newline-delimited JSON-RPC over child stdin/stdout is implemented.
//!
//! The client is deliberately small and defensive:
//!
//! - A server's tools are registered as `mcp__<server>__<tool>` and still pass
//!   through the normal policy, timeout, and result-size guards.
//! - Established environment variables are inherited; ACP-provided variables
//!   are overlaid. Environment values are never logged or included in errors.
//! - Server stderr is drained but discarded so it cannot corrupt the protocol
//!   stream or block the child.
//! - Every child is killed and reaped when its session closes, its transport
//!   fails, or startup fails. `kill_on_drop` is a final backstop.
//! - Server-controlled lines are bounded, and malformed output closes the
//!   connection rather than being trusted.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::tools::{CatalogCandidate, ToolAnnotations, ToolMetadata, ToolSource};

/// Maximum MCP servers accepted for one ACP session.
pub(crate) const MAX_MCP_SERVERS_PER_SESSION: usize = 8;

/// Maximum tools accepted across all MCP servers in one ACP session.
const MAX_MCP_TOOLS_PER_SESSION: usize = 256;

/// Maximum bytes accepted on one MCP newline-delimited JSON line.
const MAX_MCP_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Bound for MCP initialization and tool discovery.
const MCP_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Internal safety bound for one `tools/call`.
///
/// The governance timeout is normally shorter; this only ensures a pending
/// map entry cannot live forever if a caller bypasses the guard.
const MCP_TOOL_CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum `tools/list` pages followed for one server.
const MAX_TOOL_LIST_PAGES: usize = 16;

/// One stdio MCP server after ACP-independent validation.
///
/// This type intentionally does not implement `Debug`: it carries environment
/// values that must never appear in diagnostics.
#[derive(Clone)]
pub(crate) struct StdioServerConfig {
    /// Human-readable server name used in the tool namespace.
    pub(crate) name: String,
    /// Absolute executable path.
    pub(crate) command: PathBuf,
    /// Command-line arguments.
    pub(crate) args: Vec<String>,
    /// Environment variables overlaid on the inherited environment.
    pub(crate) env: Vec<(String, String)>,
}

/// Content-free MCP failure.
///
/// No variant stores environment values, request arguments, or server-provided
/// message bodies. The ACP layer maps these values to stable JSON-RPC errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum McpError {
    /// The client requested a transport other than stdio.
    #[error("MCP transport `{transport}` is not supported; only stdio is supported")]
    UnsupportedTransport {
        /// Stable transport name (`http`, `sse`, or `unknown`).
        transport: &'static str,
    },
    /// The session requested more servers than the fixed bound.
    #[error("at most {max} MCP servers are supported per session")]
    TooManyServers {
        /// Maximum accepted server count.
        max: usize,
    },
    /// A configured server name was empty after trimming.
    #[error("MCP server name must not be empty")]
    EmptyServerName,
    /// Two configured server names map to the same namespace.
    #[error("MCP server name `{name}` is duplicated")]
    DuplicateServerName {
        /// Duplicated name as supplied by the client.
        name: String,
    },
    /// The configured command was not an absolute path.
    #[error("MCP server `{server}` must use an absolute command path")]
    InvalidCommand {
        /// Server name.
        server: String,
    },
    /// Discovery produced more tools than the fixed bound.
    #[error("MCP server `{server}` exceeds the session tool limit of {max}")]
    TooManyTools {
        /// Server whose page crossed the limit.
        server: String,
        /// Maximum accepted tools per session.
        max: usize,
    },
    /// Two tools mapped to the same provider-visible name.
    #[error("MCP tool name `{name}` collides with another tool")]
    ToolNameCollision {
        /// Colliding provider-visible name.
        name: String,
    },
    /// A discovered tool did not satisfy the minimal MCP shape.
    #[error("MCP server `{server}` returned an invalid tool: {reason}")]
    InvalidTool {
        /// Server that returned the tool.
        server: String,
        /// Stable reason with no server-controlled data.
        reason: &'static str,
    },
    /// The child process could not be started.
    #[error("MCP server `{server}` could not be started: {detail}")]
    Spawn {
        /// Server name.
        server: String,
        /// Operating-system error text (never includes the environment).
        detail: String,
    },
    /// The server violated the newline-delimited JSON-RPC protocol.
    #[error("MCP server `{server}` protocol error: {reason}")]
    Protocol {
        /// Server name.
        server: String,
        /// Stable reason with no server-controlled data.
        reason: &'static str,
    },
    /// A bounded startup or tool request did not complete in time.
    #[error("MCP server `{server}` timed out during {phase}")]
    Timeout {
        /// Server name.
        server: String,
        /// Stable request phase.
        phase: &'static str,
    },
    /// The server returned a JSON-RPC error.
    #[error("MCP server `{server}` returned JSON-RPC error code {code}")]
    Rpc {
        /// Server name.
        server: String,
        /// Server-provided numeric error code.
        code: i64,
    },
    /// The connection closed before a response arrived.
    #[error("MCP server `{server}` connection closed")]
    Closed {
        /// Server name.
        server: String,
    },
}

/// Tools discovered on all stdio servers for one ACP session.
///
/// Dropping the session shuts down every child. The type is shared by the ACP
/// session and prompt workers, so shutdown is idempotent.
pub(crate) struct McpSession {
    /// Live connections in request order.
    connections: Vec<Arc<McpConnection>>,
    /// Namespaced tool handles and their untrusted annotations.
    tools: Vec<Arc<McpTool>>,
}

impl McpSession {
    /// Starts and initializes every configured stdio server.
    ///
    /// All configurations are validated before the first process is spawned.
    /// Any failure after spawning shuts down every already-started child and
    /// returns a content-free error. `cwd` becomes the working directory for
    /// every child.
    pub(crate) async fn start(
        configs: Vec<StdioServerConfig>,
        cwd: &Path,
    ) -> Result<Arc<Self>, McpError> {
        if configs.len() > MAX_MCP_SERVERS_PER_SESSION {
            return Err(McpError::TooManyServers {
                max: MAX_MCP_SERVERS_PER_SESSION,
            });
        }

        let mut namespaces = HashSet::new();
        let mut validated = Vec::with_capacity(configs.len());
        for config in configs {
            let name = config.name.trim().to_owned();
            if name.is_empty() {
                return Err(McpError::EmptyServerName);
            }
            let namespace = sanitize_component(&name);
            if namespace.is_empty() {
                return Err(McpError::EmptyServerName);
            }
            if !namespaces.insert(namespace.clone()) {
                return Err(McpError::DuplicateServerName { name });
            }
            if config.command.as_os_str().is_empty() || !config.command.is_absolute() {
                return Err(McpError::InvalidCommand { server: name });
            }
            validated.push((config, name, namespace));
        }

        let mut connections: Vec<Arc<McpConnection>> = Vec::new();
        let mut tools: Vec<Arc<McpTool>> = Vec::new();
        let mut tool_names = HashSet::new();

        for (config, name, namespace) in validated {
            let connection = match McpConnection::spawn(&config, &name, cwd) {
                Ok(connection) => connection,
                Err(error) => {
                    shutdown_all(&connections);
                    return Err(error);
                }
            };
            connections.push(Arc::clone(&connection));

            let tools_supported = match connection.initialize().await {
                Ok(supported) => supported,
                Err(error) => {
                    shutdown_all(&connections);
                    return Err(error);
                }
            };
            if !tools_supported {
                continue;
            }

            let discovered = match connection.list_tools().await {
                Ok(discovered) => discovered,
                Err(error) => {
                    shutdown_all(&connections);
                    return Err(error);
                }
            };
            for tool in discovered {
                if tools.len() >= MAX_MCP_TOOLS_PER_SESSION {
                    shutdown_all(&connections);
                    return Err(McpError::TooManyTools {
                        server: name,
                        max: MAX_MCP_TOOLS_PER_SESSION,
                    });
                }
                let provider_name = format!("mcp__{namespace}__{}", tool.name);
                if !tool_names.insert(provider_name.clone()) {
                    shutdown_all(&connections);
                    return Err(McpError::ToolNameCollision {
                        name: provider_name,
                    });
                }
                tools.push(Arc::new(McpTool {
                    provider_name,
                    remote_name: tool.name,
                    description: tool.description,
                    schema: tool.schema,
                    annotations: tool.annotations,
                    connection: Arc::clone(&connection),
                }));
            }
        }

        Ok(Arc::new(Self { connections, tools }))
    }

    /// Returns the namespaced tool candidates discovered during startup.
    ///
    /// Each candidate carries the server's advisory annotations. The catalog
    /// still classifies capabilities and approval; annotations are never
    /// trusted as proof that a server-side tool is safe.
    pub(crate) fn tools(&self) -> Vec<CatalogCandidate> {
        self.tools
            .iter()
            .map(|tool| {
                CatalogCandidate::from_tool(Arc::clone(tool) as Arc<dyn Tool>, ToolSource::Mcp)
                    .with_metadata(ToolMetadata {
                        version: None,
                        annotations: tool.annotations,
                    })
            })
            .collect()
    }

    /// Shuts down every child process. Called on close, delete, and shutdown.
    pub(crate) fn shutdown(&self) {
        shutdown_all(&self.connections);
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Kills and reaps every connection.
fn shutdown_all(connections: &[Arc<McpConnection>]) {
    for connection in connections {
        connection.shutdown();
    }
}

/// Replaces characters that providers commonly reject in tool names.
///
/// The original server and tool names are preserved for `tools/call`; only the
/// provider-visible identifier is sanitized. An input that becomes empty after
/// sanitization is rejected by the caller.
fn sanitize_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

/// Advisory MCP `annotations` object.
///
/// Unknown fields are ignored because the MCP schema is additive. Missing
/// fields stay `None`; the catalog applies conservative defaults instead of
/// assuming a safe value.
#[derive(Debug, Default, Deserialize)]
struct AnnotationHints {
    /// `readOnlyHint`.
    #[serde(rename = "readOnlyHint", default)]
    read_only: Option<bool>,
    /// `destructiveHint`.
    #[serde(rename = "destructiveHint", default)]
    destructive: Option<bool>,
    /// `openWorldHint`.
    #[serde(rename = "openWorldHint", default)]
    open_world: Option<bool>,
}

impl AnnotationHints {
    /// Converts server hints into host annotations.
    fn to_annotations(&self) -> ToolAnnotations {
        ToolAnnotations {
            read_only: self.read_only,
            destructive: self.destructive,
            open_world: self.open_world,
        }
    }
}

/// A tool discovered from an MCP server.
struct DiscoveredTool {
    /// Original MCP tool name used for `tools/call`.
    name: String,
    /// Optional model-facing description.
    description: String,
    /// Provider-facing input schema.
    schema: Value,
    /// Advisory annotations supplied by the server.
    annotations: ToolAnnotations,
}

/// One MCP tool exposed through Jessica's [`Tool`] contract.
struct McpTool {
    /// Namespaced provider-visible name.
    provider_name: String,
    /// Original MCP tool name.
    remote_name: String,
    /// Model-facing description.
    description: String,
    /// Input schema.
    schema: Value,
    /// Advisory annotations supplied by the server.
    annotations: ToolAnnotations,
    /// Connection that owns the server process.
    connection: Arc<McpConnection>,
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.provider_name
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": self.provider_name,
                "description": self.description,
                "parameters": self.schema,
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let params = json!({
            "name": self.remote_name,
            "arguments": args,
        });
        let result = self
            .connection
            .request("tools/call", Some(params), Some(MCP_TOOL_CALL_TIMEOUT))
            .await;
        match result {
            Ok(value) => match render_tool_result(&value) {
                Ok(result) => result,
                Err(reason) => ToolResult::error("", format!("MCP tool error: {reason}")),
            },
            Err(error) => ToolResult::error("", error.to_string()),
        }
    }
}

/// Converts an MCP `tools/call` result into a Jessica tool result.
///
/// Text content is preserved verbatim. Binary content and resource identifiers
/// are represented by short placeholders so large base64 payloads never enter
/// the transcript. `isError` maps to [`ToolResult::is_error`].
fn render_tool_result(value: &Value) -> Result<ToolResult, &'static str> {
    let object = value
        .as_object()
        .ok_or("tools/call result is not an object")?;
    let is_error = object
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut parts = Vec::new();
    if let Some(content) = object.get("content") {
        let content = content
            .as_array()
            .ok_or("tools/call content is not an array")?;
        for block in content {
            parts.push(render_content_block(block));
        }
    }
    if parts.is_empty()
        && let Some(structured) = object.get("structuredContent")
    {
        parts.push(
            serde_json::to_string(structured)
                .map_err(|_| "structuredContent is not serializable")?,
        );
    }

    let content = parts.join("\n");
    if is_error {
        Ok(ToolResult::error("", content))
    } else {
        Ok(ToolResult::success("", content))
    }
}

/// Renders one MCP content block without copying binary payloads.
fn render_content_block(block: &Value) -> String {
    let kind = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match kind {
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        "image" | "audio" => {
            let mime = block
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            format!("[{kind}: {mime}]")
        }
        "resource_link" => {
            let name = block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("resource");
            let uri = block.get("uri").and_then(Value::as_str).unwrap_or("");
            format!("[resource link: {name}] {uri}")
        }
        "resource" => {
            let uri = block
                .get("resource")
                .and_then(|resource| resource.get("uri"))
                .and_then(Value::as_str)
                .unwrap_or("");
            format!("[resource] {uri}")
        }
        _ => format!("[unsupported MCP content: {kind}]"),
    }
}

/// One live MCP child process and its request/response state.
struct McpConnection {
    /// Server name used in diagnostics.
    server: String,
    /// Sender accepted by the writer task.
    writer: Mutex<Option<mpsc::UnboundedSender<String>>>,
    /// Pending request responses keyed by client request id.
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpError>>>>,
    /// Child retained so shutdown can kill and reap it.
    child: Mutex<Option<Child>>,
    /// Next JSON-RPC request id.
    next_id: AtomicU64,
    /// Fast closed check before taking locks.
    closed: AtomicBool,
    /// Cancellation token for auxiliary tasks.
    close: CancellationToken,
}

impl McpConnection {
    /// Spawns one child and installs the reader, writer, and stderr tasks.
    fn spawn(config: &StdioServerConfig, name: &str, cwd: &Path) -> Result<Arc<Self>, McpError> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        for (key, value) in &config.env {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(|error| McpError::Spawn {
            server: name.to_owned(),
            detail: error.to_string(),
        })?;
        let stdin = child.stdin.take().ok_or_else(|| McpError::Protocol {
            server: name.to_owned(),
            reason: "child stdin unavailable",
        })?;
        let stdout = child.stdout.take().ok_or_else(|| McpError::Protocol {
            server: name.to_owned(),
            reason: "child stdout unavailable",
        })?;
        let stderr = child.stderr.take().ok_or_else(|| McpError::Protocol {
            server: name.to_owned(),
            reason: "child stderr unavailable",
        })?;

        let connection = Arc::new(Self {
            server: name.to_owned(),
            writer: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            child: Mutex::new(Some(child)),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            close: CancellationToken::new(),
        });

        let (writer, writer_rx) = mpsc::unbounded_channel();
        *connection.lock_writer() = Some(writer);
        tokio::spawn(writer_loop(writer_rx, stdin, Arc::downgrade(&connection)));
        tokio::spawn(reader_loop(
            BufReader::new(stdout),
            Arc::downgrade(&connection),
            name.to_owned(),
        ));
        tokio::spawn(drain_stderr(stderr));
        Ok(connection)
    }

    /// Performs the MCP handshake.
    ///
    /// Returns whether the server advertised the `tools` capability. Servers
    /// without tools are still initialized and kept alive so their lifetime
    /// matches the ACP session.
    async fn initialize(self: &Arc<Self>) -> Result<bool, McpError> {
        let params = json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {
                "name": "jessica",
                "version": env!("CARGO_PKG_VERSION"),
            },
        });
        let result = self
            .request("initialize", Some(params), Some(MCP_STARTUP_TIMEOUT))
            .await?;
        let object = result.as_object().ok_or_else(|| McpError::Protocol {
            server: self.server.clone(),
            reason: "initialize result is not an object",
        })?;
        let version = object
            .get("protocolVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::Protocol {
                server: self.server.clone(),
                reason: "initialize response has no protocol version",
            })?;
        if version.is_empty() {
            return Err(McpError::Protocol {
                server: self.server.clone(),
                reason: "initialize response has an empty protocol version",
            });
        }
        let tools_supported = object
            .get("capabilities")
            .and_then(Value::as_object)
            .and_then(|capabilities| capabilities.get("tools"))
            .and_then(Value::as_object)
            .is_some();
        self.notify("notifications/initialized", None)?;
        Ok(tools_supported)
    }

    /// Follows `tools/list` cursor pages and parses each tool.
    async fn list_tools(self: &Arc<Self>) -> Result<Vec<DiscoveredTool>, McpError> {
        let mut discovered = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_TOOL_LIST_PAGES {
            let params = cursor.as_ref().map(|cursor| json!({ "cursor": cursor }));
            let result = self
                .request("tools/list", params, Some(MCP_STARTUP_TIMEOUT))
                .await?;
            let object = result.as_object().ok_or_else(|| McpError::Protocol {
                server: self.server.clone(),
                reason: "tools/list result is not an object",
            })?;
            let tools = object
                .get("tools")
                .and_then(Value::as_array)
                .ok_or_else(|| McpError::Protocol {
                    server: self.server.clone(),
                    reason: "tools/list result has no tools array",
                })?;
            for tool in tools {
                discovered.push(parse_tool(tool, &self.server)?);
                if discovered.len() > MAX_MCP_TOOLS_PER_SESSION {
                    return Err(McpError::TooManyTools {
                        server: self.server.clone(),
                        max: MAX_MCP_TOOLS_PER_SESSION,
                    });
                }
            }
            match object.get("nextCursor") {
                None | Some(Value::Null) => return Ok(discovered),
                Some(Value::String(next)) if next.is_empty() => return Ok(discovered),
                Some(Value::String(next)) => cursor = Some(next.clone()),
                Some(_) => {
                    return Err(McpError::Protocol {
                        server: self.server.clone(),
                        reason: "tools/list nextCursor is not a string",
                    });
                }
            }
        }
        Err(McpError::Protocol {
            server: self.server.clone(),
            reason: "tools/list exceeded the page limit",
        })
    }

    /// Sends one request and waits for its response.
    ///
    /// A dropped or timed-out caller removes its pending entry and sends a
    /// best-effort `notifications/cancelled` so the server does not keep
    /// working on an unobserved request.
    async fn request(
        self: &Arc<Self>,
        method: &'static str,
        params: Option<Value>,
        limit: Option<Duration>,
    ) -> Result<Value, McpError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(McpError::Closed {
                server: self.server.clone(),
            });
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.lock_pending().insert(id, sender);

        let message = match params {
            Some(params) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }),
            None => json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
            }),
        };
        if let Err(error) = self.send_value(message) {
            self.lock_pending().remove(&id);
            return Err(error);
        }

        let _guard = PendingGuard {
            connection: Arc::clone(self),
            id,
        };
        let response = match limit {
            Some(limit) => match tokio::time::timeout(limit, receiver).await {
                Ok(response) => response,
                Err(_) => {
                    return Err(McpError::Timeout {
                        server: self.server.clone(),
                        phase: method,
                    });
                }
            },
            None => receiver.await,
        };
        response.map_err(|_| McpError::Closed {
            server: self.server.clone(),
        })?
    }

    /// Sends a JSON-RPC notification.
    fn notify(&self, method: &'static str, params: Option<Value>) -> Result<(), McpError> {
        let message = match params {
            Some(params) => json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }),
            None => json!({
                "jsonrpc": "2.0",
                "method": method,
            }),
        };
        self.send_value(message)
    }

    /// Serializes and enqueues one JSON-RPC message.
    fn send_value(&self, value: Value) -> Result<(), McpError> {
        let mut line = serde_json::to_string(&value).map_err(|_| McpError::Protocol {
            server: self.server.clone(),
            reason: "request could not be serialized",
        })?;
        line.push('\n');
        self.send_line(line)
    }

    /// Enqueues one already-framed line for the writer task.
    fn send_line(&self, line: String) -> Result<(), McpError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(McpError::Closed {
                server: self.server.clone(),
            });
        }
        let sender = self.lock_writer().clone();
        let Some(sender) = sender else {
            return Err(McpError::Closed {
                server: self.server.clone(),
            });
        };
        sender.send(line).map_err(|_| McpError::Closed {
            server: self.server.clone(),
        })
    }

    /// Handles one decoded server message.
    fn handle_message(&self, message: Value) {
        if !message.is_object() {
            self.fail(McpError::Protocol {
                server: self.server.clone(),
                reason: "server message is not an object",
            });
            return;
        }
        let Some(id) = message.get("id") else {
            // Server notifications are accepted and ignored.
            return;
        };
        if message.get("method").is_some() {
            // Server-to-client requests are outside this minimal client.
            let _ = self.send_value(json!({
                "jsonrpc": "2.0",
                "id": id.clone(),
                "error": {
                    "code": -32601,
                    "message": "method not found",
                },
            }));
            return;
        }
        let Some(id) = id.as_u64() else {
            return;
        };
        let sender = self.lock_pending().remove(&id);
        let Some(sender) = sender else {
            return;
        };
        if let Some(error) = message.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
            let _ = sender.send(Err(McpError::Rpc {
                server: self.server.clone(),
                code,
            }));
        } else if let Some(result) = message.get("result") {
            let _ = sender.send(Ok(result.clone()));
        } else {
            let _ = sender.send(Err(McpError::Protocol {
                server: self.server.clone(),
                reason: "response has neither result nor error",
            }));
        }
    }

    /// Marks the connection failed, kills the child, and wakes all requests.
    fn fail(&self, error: McpError) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.close.cancel();
        if let Some(sender) = self.lock_writer().take() {
            drop(sender);
        }
        let child = self.lock_child().take();
        if let Some(mut child) = child {
            let _ = child.start_kill();
            spawn_reap(child);
        }
        let pending: Vec<_> = self
            .lock_pending()
            .drain()
            .map(|(_, sender)| sender)
            .collect();
        for sender in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }

    /// Shuts the connection down without treating it as a protocol failure.
    fn shutdown(&self) {
        self.fail(McpError::Closed {
            server: self.server.clone(),
        });
    }

    /// Locks the writer slot, recovering from poisoning.
    fn lock_writer(&self) -> MutexGuard<'_, Option<mpsc::UnboundedSender<String>>> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the pending map, recovering from poisoning.
    fn lock_pending(
        &self,
    ) -> MutexGuard<'_, HashMap<u64, oneshot::Sender<Result<Value, McpError>>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the child slot, recovering from poisoning.
    fn lock_child(&self) -> MutexGuard<'_, Option<Child>> {
        self.child.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for McpConnection {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close.cancel();
        if let Ok(mut writer) = self.writer.lock() {
            writer.take();
        }
        if let Ok(mut child) = self.child.lock()
            && let Some(mut child) = child.take()
        {
            let _ = child.start_kill();
        }
    }
}

/// Removes a pending request and notifies the server when it was abandoned.
struct PendingGuard {
    /// Connection that owns the pending entry.
    connection: Arc<McpConnection>,
    /// Request id to remove.
    id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let removed = self.connection.lock_pending().remove(&self.id).is_some();
        if removed {
            let _ = self.connection.notify(
                "notifications/cancelled",
                Some(json!({
                    "requestId": self.id,
                    "reason": "request cancelled",
                })),
            );
        }
    }
}

/// Writes queued messages to child stdin.
async fn writer_loop(
    mut receiver: mpsc::UnboundedReceiver<String>,
    mut stdin: ChildStdin,
    connection: Weak<McpConnection>,
) {
    while let Some(line) = receiver.recv().await {
        if stdin.write_all(line.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
            if let Some(connection) = connection.upgrade() {
                connection.fail(McpError::Closed {
                    server: connection.server.clone(),
                });
            }
            return;
        }
    }
    let _ = stdin.shutdown().await;
}

/// Reads newline-delimited JSON-RPC messages from child stdout.
async fn reader_loop(
    mut reader: BufReader<ChildStdout>,
    connection: Weak<McpConnection>,
    server: String,
) {
    loop {
        match read_limited_line(&mut reader).await {
            Ok(None) => break,
            Ok(Some(line)) => {
                let Some(connection) = connection.upgrade() else {
                    return;
                };
                match serde_json::from_slice::<Value>(&line) {
                    Ok(message) => connection.handle_message(message),
                    Err(_) => {
                        connection.fail(McpError::Protocol {
                            server: server.clone(),
                            reason: "server output is not valid JSON",
                        });
                        return;
                    }
                }
            }
            Err(LineError::TooLarge) => {
                if let Some(connection) = connection.upgrade() {
                    connection.fail(McpError::Protocol {
                        server,
                        reason: "server output exceeded the line limit",
                    });
                }
                return;
            }
            Err(LineError::Io) => break,
        }
    }
    if let Some(connection) = connection.upgrade()
        && !connection.closed.load(Ordering::SeqCst)
    {
        connection.fail(McpError::Closed { server });
    }
}

/// Discards child stderr so it cannot block and never reaches logs.
async fn drain_stderr(mut stderr: ChildStderr) {
    let mut buffer = [0_u8; 4096];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Error from [`read_limited_line`].
#[derive(Debug)]
enum LineError {
    /// The stream failed.
    Io,
    /// The line exceeded [`MAX_MCP_LINE_BYTES`].
    TooLarge,
}

/// Reads one newline-delimited line with a hard byte bound.
///
/// The returned line excludes the newline and an optional carriage return.
async fn read_limited_line<R>(reader: &mut R) -> Result<Option<Vec<u8>>, LineError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(|_| LineError::Io)?;
        if buffer.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Ok(Some(line));
        }
        if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(newline) > MAX_MCP_LINE_BYTES {
                return Err(LineError::TooLarge);
            }
            line.extend_from_slice(&buffer[..newline]);
            reader.consume(newline + 1);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Some(line));
        }
        if line.len().saturating_add(buffer.len()) > MAX_MCP_LINE_BYTES {
            return Err(LineError::TooLarge);
        }
        let consumed = buffer.len();
        line.extend_from_slice(buffer);
        reader.consume(consumed);
    }
}

/// Parses one `tools/list` entry.
fn parse_tool(value: &Value, server: &str) -> Result<DiscoveredTool, McpError> {
    let object = value.as_object().ok_or_else(|| McpError::InvalidTool {
        server: server.to_owned(),
        reason: "tool is not an object",
    })?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| McpError::InvalidTool {
            server: server.to_owned(),
            reason: "tool has no string name",
        })?;
    if name.is_empty() || sanitize_component(&name).is_empty() {
        return Err(McpError::InvalidTool {
            server: server.to_owned(),
            reason: "tool name is empty or cannot be represented",
        });
    }
    let description = object
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let schema = match object.get("inputSchema") {
        Some(schema) if schema.is_object() => schema.clone(),
        Some(_) => {
            return Err(McpError::InvalidTool {
                server: server.to_owned(),
                reason: "tool inputSchema is not an object",
            });
        }
        None => json!({"type": "object", "properties": {}}),
    };
    // A malformed annotations object is treated like absent hints, not as a
    // discovery failure: the host's conservative baseline is safe, and one
    // bad optional field must not hide an otherwise usable tool.
    let annotations = object
        .get("annotations")
        .and_then(|value| serde_json::from_value::<AnnotationHints>(value.clone()).ok())
        .unwrap_or_default()
        .to_annotations();
    Ok(DiscoveredTool {
        name,
        description,
        schema,
        annotations,
    })
}

/// Reaps a killed child on the current Tokio runtime.
fn spawn_reap(mut child: Child) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = child.wait().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_component_keeps_provider_safe_characters() {
        assert_eq!(sanitize_component("filesystem"), "filesystem");
        assert_eq!(sanitize_component("my-server_2"), "my-server_2");
        assert_eq!(sanitize_component("my server/工具"), "my_server___");
        assert_eq!(sanitize_component("___"), "___");
    }

    #[test]
    fn render_text_and_error_results() {
        let result = render_tool_result(&json!({
            "content": [
                {"type": "text", "text": "first"},
                {"type": "text", "text": "second"},
            ],
        }))
        .expect("valid result");
        assert!(!result.is_error);
        assert_eq!(result.content, "first\nsecond");

        let result = render_tool_result(&json!({
            "content": [{"type": "text", "text": "boom"}],
            "isError": true,
        }))
        .expect("valid error result");
        assert!(result.is_error);
        assert_eq!(result.content, "boom");
    }

    #[test]
    fn binary_content_never_copies_payload_data() {
        let result = render_tool_result(&json!({
            "content": [{
                "type": "image",
                "data": "c2VjcmV0",
                "mimeType": "image/png",
            }],
        }))
        .expect("valid image result");
        assert!(!result.content.contains("c2VjcmV0"));
        assert_eq!(result.content, "[image: image/png]");
    }

    #[test]
    fn malformed_results_are_rejected_without_panicking() {
        assert!(render_tool_result(&json!("text")).is_err());
        assert!(render_tool_result(&json!({"content": "text"})).is_err());
    }

    #[test]
    fn invalid_tools_are_rejected() {
        let error = parse_tool(&json!({"description": "missing name"}), "server")
            .err()
            .expect("missing name");
        assert!(matches!(error, McpError::InvalidTool { .. }));
        let error = parse_tool(&json!({"name": "x", "inputSchema": []}), "server")
            .err()
            .expect("invalid schema");
        assert!(matches!(error, McpError::InvalidTool { .. }));
    }

    #[test]
    fn discovery_defaults_an_omitted_input_schema() {
        let tool = parse_tool(&json!({"name": "ping"}), "server").expect("valid tool");
        assert_eq!(tool.name, "ping");
        assert_eq!(tool.schema, json!({"type": "object", "properties": {}}));
        assert!(
            tool.annotations.is_empty(),
            "an omitted annotations object must stay unclaimed"
        );
    }

    #[test]
    fn discovery_reads_read_only_destructive_and_open_world_hints() {
        let tool = parse_tool(
            &json!({
                "name": "search",
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "openWorldHint": true,
                    "title": "ignored additive field"
                }
            }),
            "server",
        )
        .expect("valid tool");
        assert_eq!(
            tool.annotations,
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: Some(true),
            }
        );

        let malformed = parse_tool(
            &json!({
                "name": "search",
                "annotations": {
                    "readOnlyHint": "yes",
                    "destructiveHint": false
                }
            }),
            "server",
        )
        .expect("a malformed optional annotation must not reject the tool");
        assert!(
            malformed.annotations.is_empty(),
            "a partially malformed annotations object must fall back to none"
        );
    }

    #[tokio::test]
    async fn read_limited_line_enforces_the_byte_bound() {
        let data = vec![b'a'; MAX_MCP_LINE_BYTES + 1];
        let mut reader = BufReader::new(data.as_slice());
        assert!(matches!(
            read_limited_line(&mut reader).await,
            Err(LineError::TooLarge)
        ));

        let mut reader = BufReader::new(b"hello\r\nworld\n".as_slice());
        assert_eq!(
            read_limited_line(&mut reader).await.expect("line"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            read_limited_line(&mut reader).await.expect("line"),
            Some(b"world".to_vec())
        );
        assert_eq!(read_limited_line(&mut reader).await.expect("eof"), None);
    }

    #[tokio::test]
    async fn start_rejects_invalid_configs_before_spawning() {
        let error = McpSession::start(
            vec![StdioServerConfig {
                name: "server".to_owned(),
                command: PathBuf::from("relative-server"),
                args: Vec::new(),
                env: Vec::new(),
            }],
            Path::new("/"),
        )
        .await
        .err()
        .expect("relative command");
        assert!(matches!(error, McpError::InvalidCommand { .. }));

        let mut configs = Vec::new();
        for index in 0..=MAX_MCP_SERVERS_PER_SESSION {
            configs.push(StdioServerConfig {
                name: format!("server-{index}"),
                command: PathBuf::from("/bin/true"),
                args: Vec::new(),
                env: Vec::new(),
            });
        }
        let error = McpSession::start(configs, Path::new("/"))
            .await
            .err()
            .expect("too many servers");
        assert!(matches!(error, McpError::TooManyServers { .. }));
    }
}
