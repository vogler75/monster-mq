# GraphQL API

MonsterMQ provides a GraphQL API for real-time data access, management, and monitoring. The API enables querying current values, historical data, managing users, and controlling the broker.

## Overview

The GraphQL API provides:
- **Real-time Queries** - Access current topic values and subscriptions
- **Historical Data** - Query archived messages with filtering
- **User Management** - CRUD operations for users and ACLs
- **Broker Control** - Monitor stats, manage connections
- **Subscriptions** - Real-time updates via WebSocket

## Quick Start

### Enable GraphQL

```yaml
# config.yaml
GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql
  PlaygroundEnabled: true  # GraphQL Playground UI
  Authentication: true     # Require authentication
```

### Access GraphQL Playground

Open browser: `http://localhost:4000/graphql`

### Basic Query Examples

```graphql
# Get current value of a topic
query {
  currentValue(topic: "sensors/temperature") {
    topic
    payload
    qos
    timestamp
  }
}

# Get multiple current values
query {
  currentValues(pattern: "sensors/*") {
    topic
    payload
    timestamp
  }
}

# Query archived messages
query {
  archivedMessages(
    topic: "sensors/temperature"
    from: "2024-01-01T00:00:00Z"
    to: "2024-01-02T00:00:00Z"
    limit: 100
  ) {
    topic
    payload
    timestamp
    qos
  }
}
```

## Schema Definition

### Types

```graphql
type Message {
  topic: String!
  payload: String!
  qos: Int!
  timestamp: DateTime!
  retained: Boolean!
  clientId: String
}

type CurrentValue {
  topic: String!
  payload: String!
  qos: Int!
  timestamp: DateTime!
}

type User {
  id: ID!
  username: String!
  roles: [String!]!
  createdAt: DateTime!
  lastLogin: DateTime
  acls: [ACL!]!
}

type ACL {
  id: ID!
  userId: ID!
  topic: String!
  action: ACLAction!
  allow: Boolean!
}

enum ACLAction {
  PUBLISH
  SUBSCRIBE
  ALL
}

type BrokerStats {
  uptime: Int!
  messagesReceived: Int!
  messagesSent: Int!
  clientsConnected: Int!
  subscriptions: Int!
  retainedMessages: Int!
}

type Client {
  clientId: String!
  username: String
  ipAddress: String!
  connectedAt: DateTime!
  lastActivity: DateTime!
  subscriptions: [String!]!
}
```

## Queries

### Current Values

```graphql
# Single topic
query GetCurrentValue {
  currentValue(topic: "sensors/temp1") {
    topic
    payload
    timestamp
  }
}

# Multiple topics with pattern
query GetMultipleValues {
  currentValues(pattern: "sensors/*") {
    topic
    payload
    timestamp
  }
}

# With JSON parsing
query GetParsedValue {
  currentValue(topic: "sensors/data") {
    topic
    payload
    payloadJson  # Parsed JSON object
    timestamp
  }
}
```

### Historical Data

```graphql
# Time range query
query GetHistoricalData {
  archivedMessages(
    topic: "sensors/temperature"
    from: "2024-01-01T00:00:00Z"
    to: "2024-01-01T12:00:00Z"
    limit: 1000
  ) {
    timestamp
    payload
  }
}

# Aggregated data
query GetAggregatedData {
  aggregateMessages(
    topic: "sensors/temperature"
    from: "2024-01-01T00:00:00Z"
    to: "2024-01-02T00:00:00Z"
    interval: "1h"
    function: AVG
  ) {
    timestamp
    value
    count
  }
}

# Pattern matching
query SearchMessages {
  searchMessages(
    topicPattern: "sensors/*"
    payloadPattern: "error"
    from: "2024-01-01T00:00:00Z"
    limit: 100
  ) {
    topic
    payload
    timestamp
  }
}
```

### User Management

```graphql
# List users
query ListUsers {
  users {
    id
    username
    roles
    createdAt
    lastLogin
  }
}

# Get user with ACLs
query GetUser {
  user(id: "123") {
    username
    roles
    acls {
      topic
      action
      allow
    }
  }
}

# Check permissions
query CheckPermission {
  hasPermission(
    username: "john"
    topic: "sensors/temp1"
    action: PUBLISH
  )
}
```

### Broker Statistics

```graphql
# Broker stats
query GetStats {
  brokerStats {
    uptime
    messagesReceived
    messagesSent
    clientsConnected
    subscriptions
    retainedMessages
  }
}

# Connected clients
query GetClients {
  clients {
    clientId
    username
    ipAddress
    connectedAt
    subscriptions
  }
}

# Topic statistics
query TopicStats {
  topicStats(pattern: "sensors/*") {
    topic
    messageCount
    lastMessage
    subscriberCount
  }
}
```

## Mutations

### User Management

```graphql
# Create user
mutation CreateUser {
  createUser(input: {
    username: "john"
    password: "secure123"
    roles: ["user"]
  }) {
    id
    username
    roles
  }
}

# Update user
mutation UpdateUser {
  updateUser(id: "123", input: {
    roles: ["user", "admin"]
  }) {
    id
    roles
  }
}

# Delete user
mutation DeleteUser {
  deleteUser(id: "123")
}

# Change password
mutation ChangePassword {
  changePassword(
    username: "john"
    oldPassword: "secure123"
    newPassword: "newsecure456"
  )
}
```

### ACL Management

```graphql
# Add ACL rule
mutation AddACL {
  addACL(input: {
    userId: "123"
    topic: "sensors/+"
    action: PUBLISH
    allow: true
  }) {
    id
    topic
    action
    allow
  }
}

# Update ACL
mutation UpdateACL {
  updateACL(id: "456", input: {
    allow: false
  }) {
    id
    allow
  }
}

# Delete ACL
mutation DeleteACL {
  deleteACL(id: "456")
}
```

### Message Publishing

```graphql
# Publish message
mutation PublishMessage {
  publish(input: {
    topic: "sensors/temp1"
    payload: "23.5"
    qos: 1
    retained: false
  })
}

# Publish multiple
mutation PublishBatch {
  publishBatch(messages: [
    { topic: "sensors/temp1", payload: "23.5" },
    { topic: "sensors/temp2", payload: "24.1" }
  ])
}
```

### Client Management

```graphql
# Disconnect client
mutation DisconnectClient {
  disconnectClient(clientId: "client123")
}

# Clear retained messages
mutation ClearRetained {
  clearRetainedMessages(topicPattern: "sensors/*")
}
```

## Subscriptions (Real-time)

### WebSocket Subscriptions

```graphql
# Subscribe to topic updates
subscription TopicUpdates {
  messageReceived(topicPattern: "sensors/+") {
    topic
    payload
    timestamp
    clientId
  }
}

# Monitor client connections
subscription ClientEvents {
  clientStatusChanged {
    clientId
    event  # CONNECTED, DISCONNECTED
    timestamp
  }
}

# Real-time stats
subscription StatsUpdate {
  statsUpdate {
    messagesPerSecond
    clientsConnected
    activeSubscriptions
  }
}
```

### WebSocket Client Example

```javascript
// Using Apollo Client
import { ApolloClient, InMemoryCache } from '@apollo/client';
import { WebSocketLink } from '@apollo/client/link/ws';

const wsLink = new WebSocketLink({
  uri: 'ws://localhost:4000/graphql',
  options: {
    reconnect: true,
    connectionParams: {
      authorization: 'Bearer YOUR_TOKEN',
    },
  },
});

const client = new ApolloClient({
  link: wsLink,
  cache: new InMemoryCache(),
});

// Subscribe to messages
const subscription = client.subscribe({
  query: gql`
    subscription OnMessage {
      messageReceived(topicPattern: "sensors/+") {
        topic
        payload
        timestamp
      }
    }
  `,
}).subscribe({
  next: (data) => console.log('Message:', data),
  error: (err) => console.error('Error:', err),
});
```

## Authentication

### Token-based Authentication

```yaml
# Enable authentication
GraphQL:
  Authentication: true
  TokenSecret: "your-secret-key"
  TokenExpiry: 3600  # seconds
```

### Login Mutation

```graphql
mutation Login {
  login(username: "admin", password: "password") {
    token
    expiresIn
    user {
      id
      username
      roles
    }
  }
}
```

### Using Token

```bash
# HTTP header
curl -X POST http://localhost:4000/graphql \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ brokerStats { uptime } }"}'
```

### Role-based Access

```graphql
# Admin only queries
query AdminStats @requireRole(role: "admin") {
  systemMetrics {
    cpuUsage
    memoryUsage
    diskUsage
  }
}
```

## Advanced Features

### Pagination

```graphql
query PaginatedMessages {
  archivedMessages(
    topic: "sensors/temperature"
    first: 20
    after: "cursor123"
  ) {
    edges {
      node {
        payload
        timestamp
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Filtering

```graphql
query FilteredMessages {
  messages(
    where: {
      topic: { startsWith: "sensors/" }
      timestamp: { gte: "2024-01-01T00:00:00Z" }
      payload: { contains: "error" }
    }
    orderBy: TIMESTAMP_DESC
    limit: 100
  ) {
    topic
    payload
    timestamp
  }
}
```

### Aggregations

```graphql
query Aggregations {
  messageStats(
    groupBy: HOUR
    from: "2024-01-01T00:00:00Z"
    to: "2024-01-02T00:00:00Z"
  ) {
    timestamp
    count
    topics
    averageSize
  }
}
```

### Batch Operations

```graphql
query BatchQuery {
  batch {
    temp1: currentValue(topic: "sensors/temp1") {
      payload
    }
    temp2: currentValue(topic: "sensors/temp2") {
      payload
    }
    stats: brokerStats {
      clientsConnected
    }
  }
}
```

## Client Libraries

### JavaScript/TypeScript

```javascript
// Apollo Client
import { ApolloClient, InMemoryCache, gql } from '@apollo/client';

const client = new ApolloClient({
  uri: 'http://localhost:4000/graphql',
  cache: new InMemoryCache(),
  headers: {
    authorization: 'Bearer YOUR_TOKEN',
  },
});

// Query
const result = await client.query({
  query: gql`
    query GetValue {
      currentValue(topic: "sensors/temp1") {
        payload
        timestamp
      }
    }
  `,
});
```

### Python

```python
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url="http://localhost:4000/graphql",
    headers={'Authorization': 'Bearer YOUR_TOKEN'}
)

client = Client(transport=transport, fetch_schema_from_transport=True)

query = gql("""
    query GetValue {
        currentValue(topic: "sensors/temp1") {
            payload
            timestamp
        }
    }
""")

result = client.execute(query)
```

### cURL Examples

```bash
# Simple query
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ currentValue(topic: \"sensors/temp1\") { payload } }"}'

# With variables
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "query": "query GetValue($topic: String!) { currentValue(topic: $topic) { payload } }",
    "variables": {"topic": "sensors/temp1"}
  }'
```

## Performance Optimization

### Query Optimization

```graphql
# Use field selection
query OptimizedQuery {
  messages(limit: 100) {
    payload  # Only request needed fields
    timestamp
    # Don't request unnecessary fields
  }
}

# Use aliases for parallel queries
query ParallelQueries {
  recent: messages(limit: 10, orderBy: TIMESTAMP_DESC) {
    payload
  }
  stats: brokerStats {
    messagesReceived
  }
}
```

### Caching

```yaml
GraphQL:
  Caching:
    Enabled: true
    TTL: 60  # seconds
    MaxSize: 1000  # entries
```

### Rate Limiting

```yaml
GraphQL:
  RateLimit:
    Enabled: true
    RequestsPerMinute: 100
    BurstSize: 20
```

## Monitoring

### Query Logging

```yaml
GraphQL:
  Logging:
    Queries: true
    SlowQueryThreshold: 1000  # ms
    IncludeVariables: false
```

### Metrics

```graphql
query Metrics {
  graphqlMetrics {
    totalQueries
    totalMutations
    totalSubscriptions
    averageQueryTime
    errorRate
    activeSubscriptions
  }
}
```

## Security Best Practices

1. **Always enable authentication** in production
2. **Use HTTPS** for GraphQL endpoint
3. **Implement query depth limiting** to prevent DoS
4. **Set query timeout** for long-running queries
5. **Use field-level authorization** for sensitive data
6. **Implement rate limiting** per user/IP
7. **Validate and sanitize** all inputs
8. **Log and monitor** suspicious queries

## Troubleshooting

### Common Issues

1. **Connection refused:**
   - Verify GraphQL is enabled in config
   - Check port is not blocked
   - Ensure service is running

2. **Authentication errors:**
   - Check token validity
   - Verify token secret matches
   - Ensure user has required permissions

3. **Slow queries:**
   - Add indexes to database
   - Limit query depth
   - Use pagination for large datasets

4. **WebSocket issues:**
   - Check firewall allows WebSocket
   - Verify WebSocket endpoint
   - Check for proxy configuration