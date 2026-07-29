---
name: monstermq-graphql-config
description: >
  Guide for configuring, managing, and mutating MonsterMQ settings, devices, flows, AI agents,
  users, loggers, archive groups, topic schemas, and publishing messages via the GraphQL API.
  Use this skill whenever you need to publish MQTT messages, manage user accounts or ACLs, create
  or update device connectors (OPC UA, PLC4X, WinCC, Kafka, NATS, Redis, Telegram, Neo4j), set up
  loggers, deploy flow engine workflows, or configure AI agents in MonsterMQ via GraphQL.
  Trigger on "publish GraphQL", "create device", "update MQTT client", "configure OPC UA",
  "create flow", "update user", "create agent", "GraphQL mutation", "delete connector",
  or any configuration/mutation action on MonsterMQ.
---

# MonsterMQ GraphQL Configuration & Operations AI Skill

This skill provides instructions, mutation schemas, input types, and code examples for configuring MonsterMQ settings, publishing messages, managing sessions, setting up device connectors, administering users, and configuring automation workflows & AI agents via GraphQL.

---

## 1. Authentication & API Headers

When user management is enabled in MonsterMQ (`Auth.UserStoreType` set), mutations require a valid **JWT token** passed in the `Authorization` header.

### 1. Acquire JWT Token (`login`)
```graphql
mutation Login($username: String!, $password: String!) {
  login(username: $username, password: $password) {
    token
    user {
      username
      isAdmin
    }
  }
}
```

### 2. Include Header in Subsequent Requests
```http
Authorization: Bearer <JWT_TOKEN_STRING>
Content-Type: application/json
```

---

## 2. Message Publishing & Session Operations

### Publish Single Message (`publish`)
Publishes an MQTT message through the broker.

```graphql
mutation PublishMessage($input: PublishInput!) {
  publish(input: $input) {
    success
    topic
    timestamp
    error
  }
}
```

**Variables Example**:
```json
{
  "input": {
    "topic": "factory/line1/temperature",
    "payload": "{\"value\": 23.5, \"unit\": \"C\"}",
    "format": "JSON",
    "qos": 1,
    "retained": true
  }
}
```

### Batch Publish (`publishBatch`)
```graphql
mutation PublishBatch($inputs: [PublishInput!]!) {
  publishBatch(inputs: $inputs) {
    success
    topic
    timestamp
    error
  }
}
```

### Session Control (`session.removeSessions`, `purgeQueuedMessages`)
```graphql
# Remove active/persistent client sessions
mutation RemoveSessions($clientIds: [String!]!) {
  session {
    removeSessions(clientIds: $clientIds) {
      success
      removedCount
      message
    }
  }
}

# Purge offline client message queues
mutation PurgeQueue($clientId: String) {
  purgeQueuedMessages(clientId: $clientId) {
    success
    message
    purgedCount
  }
}
```

---

## 3. User Management & ACL Control (`user`)

Operations for user accounts and topic-level access control lists (ACLs).

```graphql
# Create User Account
mutation CreateUser($input: CreateUserInput!) {
  user {
    createUser(input: $input) {
      success
      message
    }
  }
}

# Update User Permissions
mutation UpdateUser($input: UpdateUserInput!) {
  user {
    updateUser(input: $input) {
      success
      message
    }
  }
}

# Change Password
mutation ChangePassword($input: SetPasswordInput!) {
  user {
    setPassword(input: $input) {
      success
      message
    }
  }
}

# Delete User
mutation DeleteUser($username: String!) {
  user {
    deleteUser(username: $username) {
      success
      message
    }
  }
}

# Create ACL Rule (Topic Access Control)
mutation CreateAclRule($input: CreateAclRuleInput!) {
  user {
    createAclRule(input: $input) {
      success
      message
    }
  }
}
```

---

## 4. Archive Group & Database Connections (`archiveGroup`)

Configure message persistence rules, retained message stores, and database connections.

```graphql
# Create Archive Group
mutation CreateArchiveGroup($input: CreateArchiveGroupInput!) {
  archiveGroup {
    create(input: $input) {
      success
      message
    }
  }
}

# Enable / Disable Archive Group
mutation ToggleArchiveGroup($name: String!, $enable: Boolean!) {
  archiveGroup {
    enable(name: $name) # or disable(name: $name)
    {
      success
      message
    }
  }
}
```

---

## 5. Device Connectors & Industrial Bridges

MonsterMQ uses an extension pattern for device connectors. Each connector has corresponding `create`, `update`, and `delete` mutations.

### OPC UA Client Connector (`createOpcUaClientConfig`)
```graphql
mutation CreateOpcUaClient($input: OpcUaClientConfigInput!) {
  createOpcUaClientConfig(input: $input) {
    name
    endpointUrl
    enabled
    subscriptions {
      nodeId
      mqttTopic
    }
  }
}
```
**Example Input**:
```json
{
  "input": {
    "name": "opcua-plc1",
    "endpointUrl": "opc.tcp://192.168.1.100:4840",
    "securityPolicy": "None",
    "enabled": true,
    "nodeId": "node-1",
    "subscriptions": [
      {
        "nodeId": "ns=2;s=Channel1.Device1.Tag1",
        "mqttTopic": "opcua/plc1/tag1",
        "samplingInterval": 1000
      }
    ]
  }
}
```

### PLC4X Connector (Siemens S7, Modbus, Allen-Bradley, etc.)
```graphql
mutation CreatePlc4xClient($input: Plc4xClientConfigInput!) {
  createPlc4xClientConfig(input: $input) {
    name
    connectionString
    enabled
  }
}
```

### WinCC OA & WinCC Unified Connectors
```graphql
mutation CreateWinCCOaClient($input: WinCCOaClientConfigInput!) {
  createWinCCOaClientConfig(input: $input) {
    name
    enabled
  }
}

mutation CreateWinCCUaClient($input: WinCCUaClientConfigInput!) {
  createWinCCUaClientConfig(input: $input) {
    name
    enabled
  }
}
```

### Kafka, NATS, Redis, Telegram & Neo4j Bridges
- `createKafkaClientConfig` / `createKafkaServerConfig`
- `createNatsClientConfig`
- `createRedisClientConfig`
- `createTelegramClientConfig`
- `createNeo4jClientConfig`

### Database & Time-Series Loggers
- `createJdbcLoggerConfig` (SQL logging)
- `createInfluxDbLoggerConfig` (InfluxDB time-series logging)
- `createTimeBaseLoggerConfig`

---

## 6. Flow Engine Automation Workflows (`flow`)

Create, deploy, and manage automation workflows in JavaScript or Python.

### Create Flow Class (Workflow Template)
```graphql
mutation CreateFlowClass($input: FlowClassInput!) {
  flow {
    createClass(input: $input) {
      name
      namespace
      version
    }
  }
}
```

### Instantiate & Enable Flow Instance
```graphql
mutation DeployFlowInstance($input: FlowInstanceInput!) {
  flow {
    createInstance(input: $input) {
      name
      enabled
      nodeId
    }
  }
}

# Control Flow Execution
mutation ControlFlow($name: String!) {
  flow {
    enableInstance(name: $name) # or disableInstance(name: $name)
    {
      name
      enabled
    }
  }
}
```

---

## 7. AI Agents & GenAI Providers (`agent`, `genAiProvider`)

Configure autonomous AI agents that process MQTT message streams and run tools/MCP servers.

### Create AI Agent
```graphql
mutation CreateAgent($input: AgentInput!) {
  agent {
    create(input: $input) {
      name
      namespace
      nodeId
      enabled
      provider
      model
    }
  }
}
```

### Start / Stop Agent
```graphql
mutation StartAgent($name: String!) {
  agent {
    start(name: $name) {
      name
      enabled
    }
  }
}
```

### Configure GenAI Provider (OpenAI, Anthropic, Gemini, Ollama, etc.)
```graphql
mutation CreateGenAiProvider($input: GenAiProviderInput!) {
  createGenAiProvider(input: $input) {
    name
    providerType
    baseUrl
  }
}
```

---

## 8. Topic Schemas (`createTopicSchema`)

Register JSON Schemas to enforce message structure validation on topic paths.

```graphql
mutation RegisterTopicSchema($input: TopicSchemaInput!) {
  createTopicSchema(input: $input) {
    topicFilter
    description
  }
}
```

---

## 9. Workflow Execution Examples

### Python Complete Configuration & Publishing Workflow
```python
import requests

GRAPHQL_URL = "http://localhost:4000/graphql"

# 1. Login to get JWT Token
login_query = """
mutation Login($user: String!, $pass: String!) {
  login(username: $user, password: $pass) {
    token
  }
}
"""
res = requests.post(GRAPHQL_URL, json={"query": login_query, "variables": {"user": "admin", "pass": "admin123"}})
token = res.json()["data"]["login"]["token"]

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
}

# 2. Publish a Message
pub_query = """
mutation Publish($input: PublishInput!) {
  publish(input: $input) {
    success
    topic
    timestamp
  }
}
"""
pub_vars = {
    "input": {
        "topic": "factory/line1/status",
        "payload": "{\"state\": \"RUNNING\", \"speed\": 120}",
        "qos": 1,
        "retained": True
    }
}
res = requests.post(GRAPHQL_URL, json={"query": pub_query, "variables": pub_vars}, headers=headers)
print("Publish Result:", res.json())
```

### Curl Mutation Example
```bash
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <TOKEN>" \
  -d '{
    "query": "mutation Publish($input: PublishInput!) { publish(input: $input) { success topic timestamp } }",
    "variables": {
      "input": {
        "topic": "cmd/test",
        "payload": "{\"action\":\"RESET\"}",
        "qos": 1
      }
    }
  }'
```
