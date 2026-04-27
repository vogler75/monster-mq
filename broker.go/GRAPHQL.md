# MonsterMQ Edge — GraphQL API

This file is the authoritative spec of the GraphQL surface implemented by
`broker.go`. The complete SDL lives in
[`internal/graphql/schema/schema.graphqls`](internal/graphql/schema/schema.graphqls);
this document describes **resolver behaviour** for every type and field, so a
re-implementation in another language can produce byte-identical responses.

The API is hosted by the broker on `cfg.GraphQL.Port` (default 8080) at:

- `GET /playground` — embedded GraphQL Playground
- `POST /graphql` and `GET /graphql` — queries / mutations
- `WS /graphql` — subscriptions (`graphql-transport-ws` and `graphql-ws` protocols)
- alias `POST /query` — for compatibility with some Apollo clients
- `GET /health` — returns `ok`

CORS is permissive (`Access-Control-Allow-Origin: *`).

Schema parity with the JVM broker (`../broker/src/main/resources/schema-*.graphqls`)
is mandatory because the dashboard at `../dashboard/` queries this surface
unmodified. Where the JVM has features we don't (OPC UA, Kafka, etc.), we
simply omit those types — the dashboard's pages either return empty arrays
or stay hidden via `Broker.enabledFeatures`.

---

## Scalars and enums

```graphql
scalar Long           # 64-bit signed integer (used for timestamps in ms, byte sizes, …)
scalar JSON           # opaque JSON object/array
enum DataFormat        { JSON BINARY }
enum OrderDirection    { ASC DESC }
enum PayloadFormat     { DEFAULT JSON }
enum MessageStoreType  { NONE MEMORY HAZELCAST POSTGRES CRATEDB MONGODB SQLITE }
enum MessageArchiveType{ NONE POSTGRES CRATEDB MONGODB KAFKA SQLITE }
```

`MessageStoreType` includes values we don't actually deploy (HAZELCAST, CRATEDB);
they exist for schema parity with the JVM broker and will fail at deploy time
with a descriptive error in `connectionStatus.error` if requested.

---

## Payload encoding (critical contract)

`TopicValue`, `RetainedMessage`, `ArchivedMessage` and `TopicUpdate` all share
the same payload-encoding contract:

```graphql
payload: String!       # always present
format:  DataFormat!   # JSON  → payload is the JSON text as-is
                       # BINARY → payload is base64-encoded bytes
```

Algorithm in our resolver (`encodePayload`):

```
if requested == BINARY:
    return (base64(raw), BINARY)
if raw parses as JSON (any value, not just object):
    return (raw_text, JSON)
otherwise:
    return (base64(raw), BINARY)
```

The MQTT v5 metadata fields below are **separate** from `format`:

```graphql
messageExpiryInterval:  Long       # seconds; 0 = no expiry
contentType:            String     # MIME type
responseTopic:          String
payloadFormatIndicator: Boolean    # mqtt v5 byte 0x01 → 'true'
userProperties:         [UserProperty!]
```

`payloadFormatIndicator` is a Boolean here (the MQTT v5 byte stored as 0/1).

---

## Top-level types

### `BrokerMetrics`

Per-node throughput counters. Fields not applicable to broker.go (Kafka, OPC UA,
WinCC, NATS, Redis, Neo4j) are present but always 0.0.

```graphql
type BrokerMetrics {
    messagesIn: Float!
    messagesOut: Float!
    nodeSessionCount: Int!
    clusterSessionCount: Int!     # equals nodeSessionCount in single-node
    queuedMessagesCount: Long!
    subscriptionCount: Int!
    clientNodeMappingSize: Int!   # always 0 (no cluster)
    topicNodeMappingSize: Int!    # always 0
    messageBusIn: Float!          # always 0
    messageBusOut: Float!         # always 0
    mqttClientIn: Float!          # bridge: remote → local
    mqttClientOut: Float!         # bridge: local → remote
    kafkaClientIn: Float!         # always 0
    kafkaClientOut: Float!        # always 0
    opcUaClientIn: Float!         # always 0
    opcUaClientOut: Float!        # always 0
    winCCOaClientIn: Float!       # always 0
    winCCUaClientIn: Float!       # always 0
    natsClientIn: Float!          # always 0
    natsClientOut: Float!         # always 0
    redisClientIn: Float!         # always 0
    redisClientOut: Float!        # always 0
    neo4jClientIn: Float!         # always 0
    timestamp: String!            # ISO 8601, e.g. "2026-04-27T20:21:22Z"
}
```

### `SessionMetrics`, `MqttClientMetrics`, `ArchiveGroupMetrics`

```graphql
type SessionMetrics {
    messagesIn: Float!
    messagesOut: Float!
    connected: Boolean
    lastPing: String
    inFlightMessagesRcv: Int
    inFlightMessagesSnd: Int
    timestamp: String!
}

type MqttClientMetrics {
    messagesIn: Float!
    messagesOut: Float!
    timestamp: String!
}

type ArchiveGroupMetrics {
    messagesOut: Float!
    bufferSize: Int!
    timestamp: String!
}
```

The current implementation returns a single empty/stub snapshot from these
sub-resolvers; only `BrokerMetrics` is fully wired. See README.md → "Metrics".

### `Broker`, `BrokerConfig`, `CurrentUser`

```graphql
type Broker {
    nodeId: String!
    version: String!
    userManagementEnabled: Boolean!
    anonymousEnabled: Boolean!
    isLeader: Boolean!                 # always true (single node)
    isCurrent: Boolean!                # always true (single node)
    enabledFeatures: [String!]!        # ["MqttBroker", "MqttClient"]
    metrics: [BrokerMetrics!]!         # single most-recent snapshot
    metricsHistory(from: String, to: String, lastMinutes: Int): [BrokerMetrics!]!
    sessions(cleanSession: Boolean, connected: Boolean): [Session!]!
}

type BrokerConfig {
    nodeId: String!
    version: String!
    clustered: Boolean!                # always false
    tcpPort, wsPort, tcpsPort, wssPort, natsPort: Int!   # natsPort always 0
    sessionStoreType, retainedStoreType, configStoreType: String!  # "SQLITE" etc.
    userManagementEnabled, anonymousEnabled: Boolean!
    mcpEnabled, prometheusEnabled, i3xEnabled, genAiEnabled: Boolean!  # always false
    mcpPort, prometheusPort, i3xPort: Int!  # always 0
    graphqlEnabled: Boolean!
    graphqlPort: Int!
    metricsEnabled: Boolean!
    genAiProvider, genAiModel: String!  # always ""
    postgresUrl, postgresUser: String!  # password omitted
    crateDbUrl, crateDbUser: String!    # always ""
    mongoDbUrl, mongoDbDatabase: String!
    sqlitePath: String!
    kafkaServers: String!               # always ""
}

type CurrentUser {
    username: String!
    isAdmin: Boolean!
}
```

### `Session`, `MqttSubscription`

```graphql
type Session {
    clientId: String!
    nodeId: String!
    metrics: [SessionMetrics!]!                  # stub
    metricsHistory(...): [SessionMetrics!]!      # stub
    subscriptions: [MqttSubscription!]!          # via SessionStore.GetSubscriptionsForClient
    cleanSession: Boolean!
    sessionExpiryInterval: Long!
    clientAddress: String
    connected: Boolean!
    queuedMessageCount: Long!                    # via QueueStore.Count
    information: String
    protocolVersion: Int                         # 4 or 5
    receiveMaximum: Int
    maximumPacketSize: Long
    topicAliasMaximum: Int
}

type MqttSubscription {
    topicFilter: String!
    qos: Int!
    noLocal: Boolean
    retainHandling: Int       # 0 = always send retained, 1 = if new sub, 2 = never
    retainAsPublished: Boolean
}
```

### `Topic`, `TopicValue`, `RetainedMessage`, `ArchivedMessage`

```graphql
type Topic {
    name: String!
    isLeaf: Boolean!
    value(format: DataFormat = JSON): TopicValue   # delegates to currentValue
}

type TopicValue {
    topic: String!
    payload: String!                         # see "Payload encoding"
    format: DataFormat!
    timestamp: Long!                         # Unix milliseconds
    qos: Int!
    messageExpiryInterval: Long
    contentType: String
    responseTopic: String
    payloadFormatIndicator: Boolean
    userProperties: [UserProperty!]
}

type RetainedMessage  { topic; payload!; format!; timestamp!; qos!; <v5 fields> }
type ArchivedMessage  { topic; payload!; format!; timestamp!; qos!; clientId; <v5 fields> }

type UserProperty { key: String!; value: String! }
```

### `AggregatedResult`

```graphql
type AggregatedResult {
    columns: [String!]!
    rows: [[JSON]!]!     # each row: [bucket_timestamp, value_for_each_function/topic_combo]
}
```

The resolver currently returns `{columns: ["timestamp"], rows: []}`; full
implementation requires backend-specific time-bucketing SQL similar to the
JVM broker's CrateDB-style aggregation.

### `NodeConnectionStatus`, `ArchiveGroupInfo`, `*Result` envelopes

```graphql
type NodeConnectionStatus {
    nodeId: String!
    messageArchive: Boolean
    lastValueStore: Boolean
    error: String
    timestamp: Long!
}

type ArchiveGroupInfo {
    name: String!
    enabled: Boolean!
    deployed: Boolean!                       # true iff in archive.Manager.Snapshot()
    deploymentId: String                     # "<nodeId>:<groupName>" or null
    topicFilter: [String!]!
    retainedOnly: Boolean!
    lastValType: MessageStoreType!
    archiveType: MessageArchiveType!
    payloadFormat: PayloadFormat!
    lastValRetention: String                 # "30d" / "1h" / "60m" / "5s"
    archiveRetention: String
    purgeInterval: String
    createdAt: String                        # currently null; not tracked
    updatedAt: String                        # currently null; not tracked
    connectionStatus: [NodeConnectionStatus!]!
    metrics: [ArchiveGroupMetrics!]!         # stub
    metricsHistory(...): [ArchiveGroupMetrics!]!
}

type ArchiveGroupResult { success: Boolean!; message: String; archiveGroup: ArchiveGroupInfo }
```

**ConnectionStatus rules (per node, single-node here)**:
- `messageArchive` is `null` if `archiveType == NONE`, else `true` if the
  manager has a running Group for this name with a non-nil archive store,
  else `false`.
- `lastValueStore` symmetrically based on `lastValType`.
- `error` carries the deploy error string from `Manager.DeployError(name)`
  if a deploy failed.

### `UserInfo`, `AclRuleInfo`, `LoginResult`, `UserManagementResult`

```graphql
type UserInfo {
    username, createdAt, updatedAt: String
    enabled, canSubscribe, canPublish, isAdmin: Boolean!
    aclRules: [AclRuleInfo!]!                # via UserStore.GetUserAclRules
}

type AclRuleInfo {
    id: String!
    username, topicPattern: String!
    canSubscribe, canPublish: Boolean!
    priority: Int!
    createdAt: String
}

type LoginResult {
    success: Boolean!
    message: String
    token: String                # opaque session token (currently a unique string,
                                 # NOT a JWT — clients persist and resend it)
    username: String
    isAdmin: Boolean
}

type UserManagementResult {
    success: Boolean!
    message: String
    user: UserInfo               # only on user-related ops
    aclRule: AclRuleInfo         # only on ACL-related ops
}
```

### `MqttClient` family

```graphql
type MqttClientAddress {
    mode: String!                    # "SUBSCRIBE" | "PUBLISH"
    remoteTopic, localTopic: String!
    removePath: Boolean!             # see README → MQTT bridge
    qos: Int                         # 0 / 1 / 2
    noLocal, retainAsPublished, payloadFormatIndicator: Boolean
    retainHandling: Int
    messageExpiryInterval: Long
    contentType, responseTopicPattern: String
    userProperties: [UserProperty!]
}

type MqttClientConnectionConfig {
    brokerUrl: String!               # tcp:// | ssl:// | tls:// | ws:// | wss://
    username: String                 # password is never returned
    clientId: String!
    cleanSession: Boolean!
    keepAlive: Int!                  # seconds
    reconnectDelay: Long!            # ms
    connectionTimeout: Long!         # ms
    addresses: [MqttClientAddress!]!
    bufferEnabled: Boolean!
    bufferSize: Int!
    persistBuffer: Boolean!
    deleteOldestMessages: Boolean!
    sslVerifyCertificate: Boolean!
    protocolVersion: Int             # 4 or 5
    sessionExpiryInterval: Long
    receiveMaximum: Int
    maximumPacketSize: Long
    topicAliasMaximum: Int
}

type MqttClient {
    name, namespace, nodeId: String!
    config: MqttClientConnectionConfig!
    enabled: Boolean!
    createdAt, updatedAt: String!     # ISO 8601 (note: NOT Long, unlike message timestamps)
    isOnCurrentNode: Boolean!         # true iff nodeId equals broker NodeID or "*"
    metrics: [MqttClientMetrics!]!    # stub
    metricsHistory(...): [MqttClientMetrics!]!
}

type MqttClientResult {
    success: Boolean!
    client: MqttClient
    errors: [String!]!                # always non-null; empty on success
}
```

### Other result and input types

```graphql
type PublishResult         { success: Boolean!; message: String; topic: String! }
type PurgeResult           { success: Boolean!; message: String; purgedCount: Long! }
type SessionRemovalResult  { success: Boolean!; message: String; removedCount: Int! }

input PublishInput {
    topic: String!
    payload: String                # plain text payload
    payloadBase64: String          # alternative: base64-encoded bytes
    payloadJson: JSON              # alternative: JSON value (Marshal'd to bytes)
    qos: Int                       # default 0
    retain: Boolean                # default false
    format: DataFormat             # advisory; not required
}

input CreateArchiveGroupInput {
    name: String!
    topicFilter: [String!]!
    retainedOnly: Boolean = false
    lastValType: MessageStoreType!
    archiveType: MessageArchiveType!
    payloadFormat: PayloadFormat = DEFAULT
    lastValRetention, archiveRetention, purgeInterval: String
}

input UpdateArchiveGroupInput {  # everything optional except name
    name: String!
    topicFilter: [String!]
    retainedOnly: Boolean
    lastValType: MessageStoreType
    archiveType: MessageArchiveType
    payloadFormat: PayloadFormat
    lastValRetention, archiveRetention, purgeInterval: String
}

input CreateUserInput {
    username: String!
    password: String!                  # bcrypt'd before persistence
    enabled, canSubscribe, canPublish, isAdmin: Boolean
}
input UpdateUserInput { username: String!; enabled, canSubscribe, canPublish, isAdmin: Boolean }
input SetPasswordInput { username, password: String! }
input CreateAclRuleInput {
    username, topicPattern: String!
    canSubscribe, canPublish: Boolean
    priority: Int
}
input UpdateAclRuleInput { id, username, topicPattern: String!; canSubscribe, canPublish: Boolean; priority: Int }

input MqttClientInput {
    name, namespace, nodeId: String!
    enabled: Boolean = true
    config: MqttClientConnectionConfigInput!
}
input MqttClientConnectionConfigInput { (mirror of MqttClientConnectionConfig + password: String) }
input MqttClientAddressInput          { (mirror of MqttClientAddress)        }
input UserPropertyInput               { key, value: String! }
```

### Subscription payloads

```graphql
type TopicUpdate {
    topic: String!
    payload: String!                 # see "Payload encoding"
    format: DataFormat!
    timestamp: Long!
    qos: Int!
    retained: Boolean!
    clientId: String
}

type TopicUpdateBulk {
    updates: [TopicUpdate!]!
    count: Int!
    timestamp: Long!                 # batch emit time (Unix ms)
}

type SystemLogEntry {
    timestamp: String!               # ISO 8601 (NOT Long)
    level: String!                   # "FINE" | "INFO" | "WARNING" | "SEVERE"
    logger: String!
    message: String!
    thread: Long!                    # 0 here (Go runtime IDs are private)
    node: String!
    sourceClass, sourceMethod: String
    parameters: [String!]
    exception: ExceptionInfo
}

type ExceptionInfo {
    class: String!
    message: String
    stackTrace: String!
}
```

`level` strings follow the JVM broker convention: `slog.Error → "SEVERE"`,
`slog.Warn → "WARNING"`, `slog.Info → "INFO"`, anything below → `"FINE"`.

---

## Query

### Identity & broker info

```graphql
currentUser: CurrentUser
```
Returns `{username:"Anonymous", isAdmin:true}` when user management is
disabled; otherwise the same anonymous shape (token-derived current-user
resolution is not yet implemented).

```graphql
brokerConfig: BrokerConfig!
broker(nodeId: String): Broker     # null if nodeId provided and != broker's nodeId
brokers: [Broker!]!                # always single element
```

### Sessions

```graphql
sessions(nodeId: String, cleanSession: Boolean, connected: Boolean): [Session!]!
session(clientId: String!, nodeId: String): Session    # null if not found
```

`nodeId` is accepted for parity but ignored (single-node). `cleanSession` and
`connected` are post-filters applied after iterating `SessionStore`.

### Users

```graphql
users(username: String): [UserInfo!]!
```

Fetches all users from `UserStore.GetAllUsers`; if `username` is provided,
filters to that one user.

### Topic browsing & current values

```graphql
currentValue(topic: String!,         format: DataFormat,  archiveGroup: String): TopicValue
currentValues(topicFilter: String!,  format: DataFormat,  limit: Int, archiveGroup: String): [TopicValue!]!
```

Looks up the named archive group (default `"Default"`) and uses its
`MessageStore` (last-value store). `currentValues` walks topics and applies
MQTT-style match (`+`, `#`).

```graphql
retainedMessage(topic: String!,        format: DataFormat): RetainedMessage
retainedMessages(topicFilter: String!, format: DataFormat, limit: Int): [RetainedMessage!]!
```

Same as above but against the broker-level retained store, not an archive group.

```graphql
searchTopics(pattern: String!, limit: Int, archiveGroup: String): [String!]!
```

Substring search over topic names from the archive group's last-value store.

```graphql
browseTopics(topic: String!, archiveGroup: String): [Topic!]!
```

**Critical contract** (used by the dashboard's topic browser):

- For an exact (no-wildcard) topic: returns `[{name: topic, isLeaf: true}]`
  iff a value exists, otherwise empty.
- For a pattern with `+`: returns distinct topic prefixes truncated at the
  level of the wildcard.
- `isLeaf = true` iff a stored value sits exactly at the queried depth.

Examples (with stored topics `{a, a/b, c/d, sensor/temp, sensor/temp/celsius, sensor/humid}`):

| pattern         | result |
|-----------------|--------|
| `+`             | `[{a, true}, {c, false}, {sensor, false}]` (a is leaf, c & sensor have no value at depth 1) |
| `sensor/+`      | `[{sensor/humid, true}, {sensor/temp, true}]` |
| `sensor/temp/+` | `[{sensor/temp/celsius, true}]` |
| `alarm`         | `[]` if no value, else `[{alarm, true}]` |

### Archived messages

```graphql
archivedMessages(
    topicFilter: String!,
    startTime: String,        # ISO 8601
    endTime: String,
    format: DataFormat,
    limit: Int,
    archiveGroup: String,
    includeTopic: Boolean
): [ArchivedMessage!]!
```

`topicFilter` is converted to SQL `LIKE` (replacing `+`/`#` with `%`) by the
backend's `MessageArchive.GetHistory`. Sorted by time descending.

```graphql
aggregatedMessages(
    topics: [String!]!, interval: Int!, startTime: String!, endTime: String!,
    functions: [String!]!, fields: [String!], archiveGroup: String
): AggregatedResult!
```

Currently returns `{columns:["timestamp"], rows:[]}` — see README.

### Archive groups

```graphql
archiveGroups(
    enabled: Boolean,
    lastValTypeEquals: MessageStoreType,
    lastValTypeNotEquals: MessageStoreType
): [ArchiveGroupInfo!]!
archiveGroup(name: String!): ArchiveGroupInfo
```

Backed by `ArchiveConfigStore.GetAll/Get`. Filters are applied in the
resolver; `lastValTypeNotEquals: NONE` is what the topic-browser uses to find
groups that can be browsed.

### MQTT bridges

```graphql
mqttClients(name: String, node: String): [MqttClient!]!
```

Reads `DeviceConfigStore.GetAll`, filters to type `MQTT_CLIENT` and applies
`name`/`node` filters. Each `config` JSON is decoded into the structured
`MqttClientConnectionConfig` shape.

### System logs (history)

```graphql
systemLogs(
    startTime: String, endTime: String, lastMinutes: Int,
    node: String, level: [String!], logger: String,
    sourceClass: String, sourceMethod: String, message: String,
    limit: Int, orderByTime: OrderDirection
): [SystemLogEntry!]!
```

Returns the in-memory ring buffer (default 1000 entries) with all filters
applied as post-filters. `level` is case-insensitive set membership;
`logger`/`sourceClass`/`sourceMethod`/`message` are substring matches;
`node == "+"` matches any. `orderByTime: DESC` reverses the natural
chronological order; default is ascending.

---

## Mutation

### Top-level

```graphql
login(username: String!, password: String!): LoginResult!
```

Behaviour by mode:

- **User management OFF (or anonymous ON)**: always returns
  `{success:true, message:"Authentication disabled", username:"anonymous", isAdmin:true, token:null}` — matches the JVM broker's response shape exactly.
- **User management ON, anonymous OFF**: validates `(username, password)` via
  `UserStore.ValidateCredentials` (bcrypt). On success returns
  `{success:true, token:"session-<user>-<nano>", username, isAdmin}`. On
  failure: `{success:false, message:"invalid credentials"}`.

```graphql
publish(input: PublishInput!): PublishResult!
publishBatch(inputs: [PublishInput!]!): [PublishResult!]!
```

Decodes the payload (text → bytes, base64 → bytes, JSON → JSON-marshalled
bytes) and calls `mqtt.Server.Publish(topic, payload, retain, qos)` — i.e.
delivers to ALL local subscribers and persists if retained.

```graphql
purgeQueuedMessages(clientId: String!): PurgeResult!
```

`QueueStore.PurgeForClient(ctx, clientId)`.

### Grouped mutation namespaces

```graphql
user:         UserManagementMutations!
session:      SessionMutations!
archiveGroup: ArchiveGroupMutations!
mqttClient:   MqttClientMutations!
```

Each returns an empty struct; the actual mutations are field resolvers on the
struct. Pattern: `mutation { user { createUser(input: ...) { success ... } } }`.

#### `UserManagementMutations`

```graphql
createUser(input: CreateUserInput!): UserManagementResult!
updateUser(input: UpdateUserInput!): UserManagementResult!
deleteUser(username: String!):       UserManagementResult!
setPassword(input: SetPasswordInput!): UserManagementResult!
createAclRule(input: CreateAclRuleInput!): UserManagementResult!
updateAclRule(input: UpdateAclRuleInput!): UserManagementResult!
deleteAclRule(id: String!):          UserManagementResult!
```

All mutations refresh the in-memory auth cache after a successful DB write.
`createUser` runs bcrypt with default cost.

#### `SessionMutations`

```graphql
removeSessions(clientIds: [String!]!): SessionRemovalResult!
```

Calls `SessionStore.DelClient` for each id; counts successes.

#### `ArchiveGroupMutations`

```graphql
create(input: CreateArchiveGroupInput!): ArchiveGroupResult!
update(input: UpdateArchiveGroupInput!): ArchiveGroupResult!
delete(name: String!):  ArchiveGroupResult!
enable(name: String!):  ArchiveGroupResult!
disable(name: String!): ArchiveGroupResult!
```

All five call `archive.Manager.Reload(ctx)` after the DB write so the live
deployment reflects the change (start new, restart changed, stop disabled).

`update` is a partial merge — only fields present in the input override the
existing config.

`create` enforces `archive.ValidateGroupName` (`^[A-Za-z][A-Za-z0-9_]{0,62}$`)
because the name flows into DDL.

#### `MqttClientMutations`

```graphql
create(input: MqttClientInput!):                                           MqttClientResult!
update(name: String!, input: MqttClientInput!):                            MqttClientResult!
delete(name: String!):                                                     Boolean!
start(name: String!):                                                      MqttClientResult!     # toggle(name, true)
stop(name: String!):                                                       MqttClientResult!     # toggle(name, false)
toggle(name: String!, enabled: Boolean!):                                  MqttClientResult!
reassign(name: String!, nodeId: String!):                                  MqttClientResult!
addAddress(deviceName: String!, input: MqttClientAddressInput!):           MqttClientResult!
updateAddress(deviceName: String!, remoteTopic: String!,
              input: MqttClientAddressInput!):                             MqttClientResult!
deleteAddress(deviceName: String!, remoteTopic: String!):                  MqttClientResult!
```

All mutations call `bridge.mqttclient.Manager.Reload(ctx)` after the DB write
so the live bridge connector reflects the change.

`MqttClientResult.errors` is `[]` on success and `[message]` on failure (the
JVM broker uses the same shape).

`addAddress` / `updateAddress` / `deleteAddress` mutate the `addresses` array
embedded in the device's stored JSON config.

---

## Subscription

All three subscriptions use WebSockets (`graphql-transport-ws` or `graphql-ws`
protocols). The dashboard's GraphQL client supports both.

### `topicUpdates` and `topicUpdatesBulk`

```graphql
topicUpdates(topicFilters: [String!]!, format: DataFormat): TopicUpdate!

topicUpdatesBulk(
    topicFilters: [String!]!,
    format: DataFormat,
    timeoutMs: Int,        # default 250 ms
    maxSize: Int           # default 100
): TopicUpdateBulk!
```

Both subscribe to the in-process `pubsub.Bus` which receives every published
message via the storage hook. `topicFilters` use MQTT wildcards (`+`, `#`).
The bulk variant batches updates and emits when either the batch reaches
`maxSize` or the `timeoutMs` window expires.

### `systemLogs`

```graphql
systemLogs(
    node: String = "+",
    level: [String!],
    logger: String,
    thread: Long,
    sourceClass: String,
    sourceMethod: String,
    message: String
): SystemLogEntry!
```

Subscribes to the log `Bus`. Every entry produced by our slog logger AND by
mochi-mqtt (passed via `mqtt.Options.Logger`) flows here. Filters apply the
same way as the `systemLogs` query.

---

## Authorization model

Currently this broker doesn't enforce a per-request token. Anonymous mode
accepts everything; even with user management on, mutations don't currently
require a `Bearer` header. Adding that is straightforward (chi middleware
that calls `UserStore.ValidateCredentials` against a session token map) and
would be the place to enforce admin-only operations.

---

## Differences from the JVM broker

For schema parity completeness (so dashboard pages don't break), our schema
declares some types with values that are static here:

- `Broker.isLeader = true`, `isCurrent = true`, `enabledFeatures = ["MqttBroker", "MqttClient"]`
- `BrokerConfig.clustered = false`, all of `mcp/prometheus/i3x/genAi/kafka` flags are off/empty
- `BrokerMetrics.kafka/opcUa/winCC/nats/redis/neo4j` counters all 0
- Per-store `connectionStatus` reports the local node only

What the dashboard pages we don't try to support send:
- OPC UA (devices/servers/certs) → no resolvers
- Kafka client → no resolvers
- WinCC OA / WinCC Unified clients → no resolvers
- PLC4X, NATS, Redis, Neo4j clients → no resolvers
- Flows, agents, GenAI providers, MCP servers, Sparkplug, JDBC/InfluxDB/TimeBase loggers → no resolvers

Those queries fail validation against our schema (the dashboard handles them
by hiding the corresponding pages once it has read `Broker.enabledFeatures`).
