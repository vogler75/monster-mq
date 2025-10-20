# MQTT JSON-RPC 2.0 API

Execute GraphQL queries and mutations directly over MQTT using JSON-RPC 2.0 protocol. This is useful for MQTT-native clients that need to query or modify broker state without using HTTP.

## Enabling the API Service

The API Service is enabled by default when GraphQL is enabled. Configure it in your `config.yaml`:

```yaml
GraphQL:
  Enabled: true
  Port: 4000            # GraphQL endpoint
  Path: /graphql        # GraphQL path
  JsonRpcApi:
    Enabled: true       # Enable/disable MQTT JSON-RPC 2.0 API service
```

## Topic Structure

Each node listens on its own API topics organized by service:

- **Request Topic**: `$API/<node-id>/<service>/<realm>/request/<request-id>`
- **Response Topic**: `$API/<node-id>/<service>/<realm>/response/<request-id>`
- **Listen Pattern**: `$API/<node-id>/<service>/+/request/+`

Where:
- `<node-id>`: Your broker's node ID (visible in logs at startup)
- `<service>`: API service name (e.g., "graphql")
- `<realm>`: **Realm/namespace** for ACL-based access control (e.g., "tenant-a", "team-b", "v1", etc.)
  - Allows the same user to make multiple concurrent requests within a realm using different request IDs
  - Different users can be assigned permissions to specific realms via ACL rules
  - Enables multi-tenancy and fine-grained access control
- `<request-id>`: Unique identifier for correlating requests with responses

**Example:**
- Request: `$API/node-1/graphql/tenant-a/request/req-123`
- Response: `$API/node-1/graphql/tenant-a/response/req-123`

## Protocol Format

### Request Message

```json
{
  "jsonrpc": "2.0",
  "method": "<graphql-query-or-mutation>",
  "params": { "variable1": "value1", "variable2": "value2" },
  "id": "request-identifier"
}
```

- **jsonrpc**: Always "2.0"
- **method**: Complete GraphQL query or mutation string
- **params**: Optional variables passed to the GraphQL query
- **id**: Any unique string to correlate the response

### Response Message

**Success Response:**
```json
{
  "jsonrpc": "2.0",
  "result": { "data": { /* graphql result */ } },
  "id": "request-identifier"
}
```

**Error Response:**
```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32603,
    "message": "Error description"
  },
  "id": "request-identifier"
}
```

#### JSON-RPC 2.0 Error Codes

| Code | Meaning |
|------|---------|
| -32700 | Parse error - Invalid JSON in request |
| -32600 | Invalid Request - Malformed JSON-RPC request |
| -32601 | Method not found - GraphQL query/mutation syntax error |
| -32602 | Invalid params - Parameters do not match query variables |
| -32603 | Internal error - Server error or request timeout |

## Usage Examples

### Example 1: Query Current Value (Simple)

**Subscribe to response:**
```
$API/node-1/graphql/v1/response/req-001
```

**Publish request to:**
```
$API/node-1/graphql/v1/request/req-001
```

**Payload:**
```json
{
  "jsonrpc": "2.0",
  "method": "query($topic: String!) { currentValue(topic: $topic) { payload } }",
  "params": { "topic": "input/Gas/Daily" },
  "id": "req-001"
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "result": {
    "data": {
      "currentValue": {
        "payload": "{\"TimeMS\": 1760987934999.612, \"Value\": 31, \"Tags\": {}}"
      }
    }
  }
}
```

---

### Example 2: Query All Brokers

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "query { brokers { nodeId version isLeader } }",
  "id": "req-002"
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "result": {
    "data": {
      "brokers": [
        {
          "nodeId": "node-1",
          "version": "1.6.39",
          "uptime": 3600000,
          "sessions": 42,
          "isLeader": true
        }
      ]
    }
  }
}
```

---

### Example 3: Publish a Message (Mutation)

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "mutation($topic: String!, $payload: String!) { publish(topic: $topic, payload: $payload) { success } }",
  "params": {
    "topic": "output/command",
    "payload": "start"
  },
  "id": "req-003"
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "req-003",
  "result": {
    "data": {
      "publish": {
        "success": true
      }
    }
  }
}
```

---

### Example 4: Search Retained Messages

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "query($pattern: String!) { retainedMessages(pattern: $pattern, limit: 10) { topic payload qosLevel } }",
  "params": { "pattern": "sensors/*" },
  "id": "req-004"
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "req-004",
  "result": {
    "data": {
      "retainedMessages": [
        {
          "topic": "sensors/temperature",
          "payload": "23.5",
          "qosLevel": 1
        },
        {
          "topic": "sensors/humidity",
          "payload": "65",
          "qosLevel": 1
        }
      ]
    }
  }
}
```

---

## Client Implementation

### Using Mosquitto CLI

```bash
# Terminal 1: Subscribe to response topic
mosquitto_sub -h localhost -t '$API/node-1/graphql/v1/response/req-001'

# Terminal 2: Publish request
mosquitto_pub -h localhost -t '$API/node-1/graphql/v1/request/req-001' -m '{
  "jsonrpc": "2.0",
  "method": "query { brokers { nodeId version } }",
  "id": "req-001"
}'
```

### Using Python

```python
import paho.mqtt.client as mqtt
import json
import time

client = mqtt.Client()

def on_message(client, userdata, msg):
    print("Response:", msg.payload.decode())

client.connect("localhost", 1883, 60)
client.subscribe("$API/node-1/graphql/v1/response/req-001")
client.on_message = on_message

# Send request
request = {
    "jsonrpc": "2.0",
    "method": "query { brokers { nodeId version } }",
    "id": "req-001"
}
client.publish("$API/node-1/graphql/v1/request/req-001", json.dumps(request))

client.loop_start()
time.sleep(2)
client.loop_stop()
```

### Using Node.js

```javascript
const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883');

client.subscribe('$API/node-1/graphql/v1/response/req-001', () => {
  const request = {
    jsonrpc: '2.0',
    method: 'query { brokers { nodeId version } }',
    id: 'req-001'
  };

  client.publish(
    '$API/node-1/graphql/v1/request/req-001',
    JSON.stringify(request)
  );
});

client.on('message', (topic, payload) => {
  console.log('Response:', JSON.parse(payload));
  client.end();
});
```

---

## GraphQL Schema Reference

All GraphQL queries and mutations from the [GraphQL API](graphql.md) are available. Use the complete GraphQL query/mutation string as the `method` field.

### Common Queries

- `query { brokers { ... } }` - Get all broker nodes
- `query { sessions(limit: 10) { ... } }` - List MQTT sessions
- `query { currentValue(topic: $topic) { ... } }` - Get last value for a topic
- `query { retainedMessages(pattern: $pattern) { ... } }` - Search retained messages
- `query { searchTopics(pattern: $pattern) { ... } }` - Find topics by pattern

### Common Mutations

- `mutation { publish(topic: $topic, payload: $payload) { ... } }` - Publish MQTT message
- `mutation { session(clientId: $id) { removeSessions } }` - Disconnect client

See [GraphQL API documentation](graphql.md) for the complete schema.

---

## Access Control with Realms

The `<realm>` level enables fine-grained access control using MQTT ACLs. Different users can be restricted to specific realms:

### ACL Configuration Example

```yaml
Users:
  - username: user-tenant-a
    password: "secure-password"
    isAdmin: false

  - username: user-tenant-b
    password: "secure-password"
    isAdmin: false

ACLRules:
  # User for tenant-a can only access tenant-a realm
  - username: user-tenant-a
    permission: "allow"
    action: "subscribe"
    topic: "$API/+/graphql/tenant-a/response/+"

  - username: user-tenant-a
    permission: "allow"
    action: "publish"
    topic: "$API/+/graphql/tenant-a/request/+"

  # User for tenant-b can only access tenant-b realm
  - username: user-tenant-b
    permission: "allow"
    action: "subscribe"
    topic: "$API/+/graphql/tenant-b/response/+"

  - username: user-tenant-b
    permission: "allow"
    action: "publish"
    topic: "$API/+/graphql/tenant-b/request/+"

  # Deny all other API access
  - username: "*"
    permission: "deny"
    action: "subscribe"
    topic: "$API/#"

  - username: "*"
    permission: "deny"
    action: "publish"
    topic: "$API/#"
```

### Benefits of Realm-Based ACLs

- **Multi-Tenancy**: Isolate tenant data by assigning different realms
- **Team Separation**: Different teams access separate realms without interfering
- **Concurrent Requests**: Users can make multiple requests within their realm using different request IDs
- **Fine-Grained Control**: Control API access at the realm level without needing individual request ID rules
- **Scalable**: Add new users/realms without complex ACL modifications

### Example Scenarios

**Scenario 1: Multi-Tenant SaaS**
- User from Tenant A only sees `tenant-a` realm
- User from Tenant B only sees `tenant-b` realm
- Same request ID structure works for both (e.g., `req-001` used independently)

**Scenario 2: Team Access Control**
- Team leads access `team-lead-analytics` realm
- Team members access `team-member-reports` realm
- Both can make concurrent API calls within their realm

**Scenario 3: API Versioning with ACL**
- Beta users access `v2-beta` realm
- Production users access `v2-stable` realm
- Both can use the same request ID independently

---

## Performance & Limits

- **Request Timeout**: 30 seconds per request
- **Max Concurrent Requests**: 100 per broker node
- **QoS**: Responses are published with QoS 0 by default
- **Payload Size**: Limited by MQTT broker max message size (typically 256 KB)

---

## Security Considerations

- **ACL Enforcement**: API requests are subject to ACL rules on the `$API` topics
  - Use realm-based ACLs (the `<realm>` level) to isolate user access
  - Pattern-based rules like `$API/+/graphql/tenant-a/+/+` effectively isolate tenants
- **Authentication**: API requests require valid MQTT credentials if user management is enabled
- **Network**: Use MQTT TLS (MQTTS) for encrypted communication over untrusted networks
- **Realm Isolation**: Ensure ACL denies are in place to prevent unauthorized realm access (see example above)

---

## Troubleshooting

### No Response Received

1. **Verify node ID**: Check broker logs to confirm your node ID (e.g., `node-1`)
2. **Check subscription**: Ensure you're subscribed to the response topic BEFORE publishing the request
3. **Check request format**: Verify JSON-RPC format is correct and GraphQL query is valid
4. **Check logs**: Enable INFO/FINE logging to see API service messages

### GraphQL Errors

If you receive a GraphQL error in the response, the query/mutation syntax is invalid. Copy the `method` string and test it directly in the GraphQL API HTTP endpoint at `http://localhost:4000/graphql`.

### Request Timeout

If requests consistently timeout after 30 seconds:
- Check if GraphQL endpoint is responding to HTTP requests
- Check network connectivity to GraphQL port (default 4000)
- Check broker logs for errors
