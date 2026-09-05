# MCP server

MonsterMQ exposes MQTT discovery, reads, publishing, and archive queries through
MCP JSON-RPC over HTTP. The implementation is in
[`McpServer.kt`](../broker/src/main/kotlin/extensions/McpServer.kt) and
[`McpHandler.kt`](../broker/src/main/kotlin/extensions/McpHandler.kt).

## Configuration

```yaml
MCP:
  Enabled: true
  Port: 3000

# Needed for non-retained current values and historical queries in this example.
ArchiveGroups:
  - Name: Default
    TopicFilter: ["sensors/#"]
    LastValType: MEMORY
    LastValRetention: "50k"
    ArchiveType: SQLITE
    ArchiveRetention: "7d"
```

An archive group named `Default` is not a startup requirement. It is the default
selection for archive-aware tools when `archiveGroup` is omitted. Select another
name explicitly, or create `Default` with the stores needed by your tools. The
SQLite archive uses the broker's SQLite directory; see [Archiving](archiving.md).

Configure an MCP client that supports HTTP with URL `http://localhost:3000/mcp`.
Client-specific setup differs; do not configure this URL as a local stdio command.
`POST /mcp` accepts JSON-RPC requests and returns JSON responses; notifications
receive HTTP 202. `GET /mcp` opens an SSE connection with endpoint and heartbeat
events. Tool results from POST requests are returned in the HTTP response.

```bash
curl -s http://localhost:3000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"example","version":"1.0"}}}'

curl -s http://localhost:3000/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'

curl -s http://localhost:3000/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## Authentication and access scope

With user management enabled, use a GraphQL or REST login token in
`Authorization: Bearer <token>` on each request. Tokens expire after 24 hours.
The MCP handler also accepts requests without a Bearer token when the broker's
Anonymous account is enabled; disable that account when authentication is required.
With user management disabled, MCP does not require authentication.

```bash
curl -s http://localhost:4000/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"mutation { login(username: \"Admin\", password: \"replace-with-your-password\") { success token message } }"}'
```

The current MCP server authenticates the HTTP request, but does not pass a user
permission context into individual tools. Its data tools and `set-topic-value`
therefore do not enforce per-user MQTT topic ACLs. Limit MCP access to callers
trusted for the exposed data and publishing operations; MQTT ACLs alone do not
isolate MCP callers. See [Security](security.md) for the other broker interfaces.

## Tools

Call `tools/list` for complete parameter schemas. The current tool set is:

| Tool | Required arguments | Optional arguments and behavior |
|---|---|---|
| `list-archive-groups` | None | Lists deployed groups and store capabilities. |
| `find-topics-by-name` | `name` | `ignoreCase` (true), `namespace`, `archiveGroup`, `limit` (10000, capped at 10000). Searches retained and selected last-value stores; use wildcard patterns such as `*temperature*`. |
| `find-topics-by-description` | `description` | `ignoreCase` (true), `namespace`. Searches retained configuration descriptions. The accepted `archiveGroup` argument currently does not change this search. |
| `get-topic-value` | `topics` (array) | `archiveGroup`. Retained values take precedence over the selected group's last-value store. |
| `set-topic-value` | `topic`, `payload` (string) | `qos` (0), `retained` (false). Publishes into the broker as the internal `mcp-server` client. |
| `query-message-archive` | `topic` | `startTime`, `endTime`, `lastSeconds`, `limit`, `archiveGroup`. `lastSeconds` overrides explicit boundaries; the handler's default limit is 1000, although the tool description still says 100. Specify a limit explicitly. |
| `query-message-archive-by-sql` | `sql` | `archiveGroup`. Requires an archive backend supporting SQL; use that backend's dialect and actual table names. |
| `query-message-archive-aggregated` | `topics`, `interval` | Also provide `lastSeconds` or both `startTime` and `endTime`. Additional arguments include `fields`, `functions`, `archiveGroup`; availability depends on the selected archive backend. |

Historical times use ISO 8601 instants, for example `2026-09-01T00:00:00Z`.
Use `list-archive-groups` before choosing an archive backend; a last-value store
alone cannot answer historical queries.

```bash
curl -s http://localhost:3000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get-topic-value","arguments":{"topics":["sensors/temperature"],"archiveGroup":"Default"}}}'
```

## Topic descriptions

Publish retained JSON metadata to `{topic}/<config>` to make descriptions available
to discovery tools. Use the exact key `Description`: description search requests
that key from the retained store. Other keys can provide context but do not add
new server-side search filters.

```bash
mosquitto_pub -h localhost -r -t 'sensors/temperature/<config>' -m '{
  "Description": "Temperature sensor in conference room 1",
  "unit": "°C",
  "location": "Conference Room 1"
}'
```

Add MQTT credentials when user management requires them. The retained backend must
support extended searches for description discovery. A tool response may contain
Markdown tables in MCP text content rather than structured JSON rows.
