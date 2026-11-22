#!/usr/bin/env python3
"""Deterministic stdio MCP server used by Jessica's ACP end-to-end tests.

The fixture speaks newline-delimited JSON-RPC on stdin/stdout. It deliberately
supports pagination and several failure modes so the client's startup cleanup,
tool invocation, timeout, and secret-handling behavior can be observed through
a real child process.

Environment:
  MCP_STUB_PID_FILE       Write this process's pid here after validating env.
  MCP_STUB_CALL_LOG       Append one tool name per invoked tool.
  MCP_STUB_SECRET         Secret value; written to stderr to prove redaction.
  MCP_STUB_REQUIRE_SECRET Exit unless MCP_STUB_SECRET equals this value.
  MCP_STUB_MODE           normal (default), init-error, list-error,
                          malformed, no-tools, or call-error.
"""

import json
import os
import sys
import time


ECHO_TOOL = {
    "name": "echo",
    "description": "Echo the supplied text.",
    "inputSchema": {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    },
    "annotations": {
        "readOnlyHint": True,
        "destructiveHint": False,
        "openWorldHint": False,
    },
}

FAIL_TOOL = {
    "name": "fail",
    "description": "Return an MCP tool error.",
    "inputSchema": {"type": "object", "properties": {}},
    "annotations": {
        "readOnlyHint": True,
        "destructiveHint": False,
        "openWorldHint": False,
    },
}

SLOW_TOOL = {
    "name": "slow",
    "description": "Sleep before returning.",
    "inputSchema": {
        "type": "object",
        "properties": {"seconds": {"type": "number"}},
    },
    "annotations": {
        "readOnlyHint": True,
        "destructiveHint": False,
        "openWorldHint": False,
    },
}

WIPE_TOOL = {
    "name": "wipe",
    "description": "Simulate a destructive server-side operation.",
    "inputSchema": {
        "type": "object",
        "properties": {"target": {"type": "string"}},
        "required": ["target"],
    },
    "annotations": {
        "readOnlyHint": False,
        "destructiveHint": True,
        "openWorldHint": False,
    },
}

BIG_TOOL = {
    "name": "big",
    "description": "Return a response of the requested size.",
    "inputSchema": {
        "type": "object",
        "properties": {"size": {"type": "integer"}},
        "required": ["size"],
    },
    "annotations": {
        "readOnlyHint": True,
        "destructiveHint": False,
        "openWorldHint": False,
    },
}


def send(message):
    """Writes one compact JSON-RPC message followed by a newline."""
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def result(request_id, value):
    """Sends a successful JSON-RPC response."""
    send({"jsonrpc": "2.0", "id": request_id, "result": value})


def error(request_id, code, message):
    """Sends a JSON-RPC error response."""
    send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
    )


def append_call_log(name):
    """Records a tool invocation without recording arguments."""
    path = os.environ.get("MCP_STUB_CALL_LOG")
    if path:
        with open(path, "a", encoding="utf-8") as log:
            log.write(name + "\n")


def initialize(request_id, mode):
    """Answers initialize according to the requested fixture mode."""
    if mode == "init-error":
        error(request_id, -32000, "stub initialization failed")
        return
    capabilities = {} if mode == "no-tools" else {"tools": {"listChanged": False}}
    result(
        request_id,
        {
            "protocolVersion": "2025-06-18",
            "capabilities": capabilities,
            "serverInfo": {"name": "jessica-mcp-stub", "version": "1.0.0"},
        },
    )


def list_tools(request_id, cursor, mode):
    """Returns two pages for normal mode and a fixed error for list-error."""
    if mode == "list-error":
        error(request_id, -32000, "stub discovery failed")
        return
    if mode == "malformed":
        sys.stdout.write("this is not JSON\n")
        sys.stdout.flush()
        return
    if cursor is None:
        result(request_id, {"tools": [ECHO_TOOL], "nextCursor": "page-2"})
    elif cursor == "page-2":
        result(
            request_id,
            {"tools": [FAIL_TOOL, SLOW_TOOL, WIPE_TOOL, BIG_TOOL]},
        )
    else:
        error(request_id, -32602, "unknown cursor")


def call_tool(request_id, params, mode):
    """Executes one fixture tool."""
    name = params.get("name")
    arguments = params.get("arguments") or {}
    append_call_log(str(name))

    if mode == "call-error" and name == "echo":
        error(request_id, -32000, "stub tool failed")
        return
    if name == "echo":
        text = arguments.get("text", "")
        result(
            request_id,
            {"content": [{"type": "text", "text": text}], "isError": False},
        )
        return
    if name == "fail":
        result(
            request_id,
            {
                "content": [{"type": "text", "text": "stub tool error"}],
                "isError": True,
            },
        )
        return
    if name == "slow":
        seconds = float(arguments.get("seconds", 30))
        time.sleep(seconds)
        result(
            request_id,
            {"content": [{"type": "text", "text": "slept"}], "isError": False},
        )
        return
    if name == "wipe":
        result(
            request_id,
            {
                "content": [
                    {"type": "text", "text": "wiped " + str(arguments.get("target", ""))}
                ],
                "isError": False,
            },
        )
        return
    if name == "big":
        size = max(0, min(int(arguments.get("size", 1024)), 1_000_000))
        result(
            request_id,
            {"content": [{"type": "text", "text": "z" * size}], "isError": False},
        )
        return
    error(request_id, -32602, "unknown tool")


def main():
    secret = os.environ.get("MCP_STUB_SECRET")
    if secret:
        # The client must drain and discard child stderr; this value must never
        # reach ACP stderr, protocol output, logs, or persisted state.
        sys.stderr.write("MCP_STUB_SECRET=" + secret + "\n")
        sys.stderr.flush()

    required = os.environ.get("MCP_STUB_REQUIRE_SECRET")
    if required is not None and secret != required:
        sys.stderr.write("required secret was not provided\n")
        sys.stderr.flush()
        return 2

    pid_file = os.environ.get("MCP_STUB_PID_FILE")
    if pid_file:
        with open(pid_file, "w", encoding="ascii") as handle:
            handle.write(str(os.getpid()) + "\n")
            handle.flush()

    mode = os.environ.get("MCP_STUB_MODE", "normal")
    for line in sys.stdin:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(message, dict):
            continue
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") or {}

        if method == "initialize":
            initialize(request_id, mode)
        elif method == "tools/list":
            list_tools(request_id, params.get("cursor"), mode)
        elif method == "tools/call":
            call_tool(request_id, params, mode)
        elif method in ("notifications/initialized", "notifications/cancelled"):
            # Notifications have no response; cancellation is best effort.
            continue
        elif request_id is not None:
            error(request_id, -32601, "method not found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
