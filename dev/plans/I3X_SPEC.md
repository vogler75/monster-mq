# i3X v1 API - MQTT Mapping Reference

Source: MonsterMQ broker source under `broker/src/main/kotlin/`, especially
`extensions/I3xServer.kt`, `handlers/ArchiveGroup.kt`,
`stores/IMessageStore.kt`, and `stores/PayloadDecoder.kt`.

This document is the project-specific v1 mapping reference for the MonsterMQ
i3X server. The public API is mounted under `/i3x/v1`.

---

## v1 Response Envelope

All normal endpoint responses use one of these shapes:

```json
{ "success": true, "result": {} }
```

```json
{
  "success": true,
  "results": [
    { "success": true, "elementId": "factory/line1/motor1", "result": {} },
    { "success": false, "elementId": "missing", "error": { "code": 404, "message": "Object not found" } }
  ]
}
```

```json
{ "success": false, "error": { "code": 400, "message": "Validation error" } }
```

The v1 spec says bulk endpoints preserve request order, return one item per
requested ID, and set top-level `success` to `false` if any item failed.

---

## Server Info - `GET /info`

`GET /info` is unauthenticated and lets clients discover the implemented i3X
version and optional capabilities.

```json
{
  "success": true,
  "result": {
    "specVersion": "1.0-beta",
    "serverVersion": "0.1.0",
    "serverName": "monstermq-i3x",
    "capabilities": {
      "query": { "history": true },
      "update": { "current": true, "history": true },
      "subscribe": { "stream": true }
    }
  }
}
```

---

## Address Space Concepts

| i3X concept | v1 meaning | MQTT / MonsterMQ mapping |
| --- | --- | --- |
| Namespace | Logical grouping for ObjectTypes and RelationshipTypes | Broker cluster identity, optionally subdivided by `TopicNamespace` configs (topic-prefix scopes) |
| ObjectType | JSON Schema describing an Object value | `TopicSchemaPolicy` bound to a topic filter, or a synthetic type inferred from the retained payload |
| Object | Instance with readable/writable value | MQTT topic — a retained-value leaf, or a parent level of the topic tree |
| RelationshipType | Bidirectional edge type | Topic hierarchy (`HasParent`/`HasChildren`) and JSON payload composition (`HasComponent`/`ComponentOf`) |

Important v1 field names:

| v0/stale field | v1 field |
| --- | --- |
| `typeId` | `typeElementId` |
| instance-level `namespaceUri` | `metadata.typeNamespaceUri` when `includeMetadata=true` |
| root sentinel `"/"` | `parentId: null` and `GET /objects?root=true` |
| `relationshiptype` | `relationshipType` |

Element IDs are bare MQTT topic strings (levels separated by `/`); they are not
`obj:`, `type:`, or `rel:` prefixed, and they never contain MQTT wildcards
(`+`, `#`).

---

## Namespaces - `GET /namespaces`

A Namespace groups type definitions. In v1, Object instances do not directly
belong to a namespace; their type provenance is exposed on the Object metadata.

The current implementation returns one base namespace for the broker plus one
namespace per configured `TopicNamespace` device:

```json
{
  "success": true,
  "result": [
    { "uri": "mqtt://monstermq.local/", "displayName": "monstermq" },
    { "uri": "mqtt://monstermq.local/sensors", "displayName": "sensors" }
  ]
}
```

MonsterMQ mapping:

- Base URI: `mqtt://<brokerClusterName>/` from the Vert.x cluster / broker
  identity
- Per-namespace URI: `mqtt://<brokerClusterName>/<topicPrefix>` from each
  `DeviceConfig` with `type = "TopicNamespace"`
- The synthetic namespace `http://i3x.dev/base` hosts built-in
  RelationshipTypes and payload-inferred ObjectTypes
- When no `TopicNamespace` is configured, only the base namespace is returned

---

## Object Types

ObjectTypes come from two sources:

1. **Explicit** — one ObjectType per `TopicSchemaPolicy` device config; the
   schema is the policy's `jsonSchema`, the topic filter defines which topics
   are typed by it.
2. **Synthetic** — inferred types for topics with no matching schema policy;
   exposed under `http://i3x.dev/base`.

### `GET /objecttypes`

Returns all ObjectTypes, optionally filtered by `namespaceUri`.

```json
{
  "success": true,
  "result": [
    {
      "elementId": "MotorSchema",
      "displayName": "MotorSchema",
      "namespaceUri": "mqtt://monstermq.local/sensors",
      "sourceTypeId": "MotorSchema",
      "schema": {
        "type": "object",
        "properties": {
          "speed": { "type": "number" },
          "enabled": { "type": "boolean" }
        }
      }
    }
  ]
}
```

Fields:

| Field | Required | MQTT / MonsterMQ mapping |
| --- | --- | --- |
| `elementId` | yes | Schema policy name, or synthetic type name |
| `displayName` | yes | Schema policy name |
| `namespaceUri` | yes | Namespace URI of the owning `TopicNamespace`, or `http://i3x.dev/base` for synthetic types |
| `sourceTypeId` | yes | Schema policy name |
| `version` | no | not currently emitted |
| `schema` | yes | JSON Schema from `TopicSchemaPolicy.jsonSchema`, or an inferred JSON Schema for synthetic types |

Synthetic ObjectTypes cover all the `typeElementId` values that can appear when
no schema policy matches:

- `TopicFolder` — topic level with children but no retained value
- `JsonObject` / `JsonArray` — retained payload decodes to a JSON object /
  array and no schema policy applies
- `Number`, `String`, `Boolean`, `Binary` — primitive payload types

### `POST /objecttypes/query`

Request:

```json
{ "elementIds": ["MotorSchema", "TopicFolder"] }
```

Response is a bulk envelope with ObjectType results or per-item 404 errors.

---

## Relationship Types

### `GET /relationshiptypes`

Returns built-in bidirectional relationship type definitions. Supports optional
`namespaceUri` filtering.

```json
{
  "success": true,
  "result": [
    {
      "elementId": "HasParent",
      "displayName": "HasParent",
      "namespaceUri": "http://i3x.dev/base",
      "relationshipId": "HasParent",
      "reverseOf": "HasChildren"
    },
    {
      "elementId": "HasChildren",
      "displayName": "HasChildren",
      "namespaceUri": "http://i3x.dev/base",
      "relationshipId": "HasChildren",
      "reverseOf": "HasParent"
    },
    {
      "elementId": "HasComponent",
      "displayName": "HasComponent",
      "namespaceUri": "http://i3x.dev/base",
      "relationshipId": "HasComponent",
      "reverseOf": "ComponentOf"
    },
    {
      "elementId": "ComponentOf",
      "displayName": "ComponentOf",
      "namespaceUri": "http://i3x.dev/base",
      "relationshipId": "ComponentOf",
      "reverseOf": "HasComponent"
    }
  ]
}
```

### `POST /relationshiptypes/query`

Request:

```json
{ "elementIds": ["HasParent", "HasChildren"] }
```

Response is a bulk envelope with RelationshipType results or per-item 404
errors.

Relationship semantics:

- `HasParent` / `HasChildren` model the **MQTT topic hierarchy**:
  `factory/line1/motor1` is a child of `factory/line1`.
- `HasComponent` / `ComponentOf` model **JSON payload composition**: when a
  topic's retained payload is a JSON object, each of its fields is exposed as a
  component ObjectInstance.
- Every relationship type must have a registered reverse relationship.

---

## Objects

Objects are address-space instances. The v1 Object shape is:

```json
{
  "elementId": "factory/line1/motor1",
  "displayName": "motor1",
  "typeElementId": "MotorSchema",
  "parentId": "factory/line1",
  "isComposition": true,
  "isExtended": false
}
```

With `includeMetadata=true`, the server adds:

```json
{
  "metadata": {
    "typeNamespaceUri": "mqtt://monstermq.local/sensors",
    "sourceTypeId": "MotorSchema",
    "description": null,
    "relationships": null,
    "extendedAttributes": null,
    "system": null
  }
}
```

### MQTT Object Mapping

An Object is one MQTT topic. The server enumerates Objects from the
retained-message store (`IMessageStore.findMatchingTopics("#")`) and from
archive groups' known topics. Every distinct topic level surfaces as its own
ObjectInstance so clients can read, subscribe, and navigate any sub-path
directly:

| Topic shape | ObjectInstance fields |
| --- | --- |
| Primitive retained payload (leaf) | `isComposition=false`, `typeElementId=<Number \| String \| Boolean \| Binary>` (or the matching schema policy) |
| JSON-object retained payload | `isComposition=true`, `typeElementId=<schemaPolicy \| JsonObject>` |
| JSON-array retained payload | `isComposition=true`, `typeElementId=JsonArray` |
| Intermediate level with children but no retained value | `isComposition=true`, `typeElementId="TopicFolder"` |
| Intermediate level with both a retained value and children | `isComposition=true` with the retained-value type |

Topic-filter ↔ schema matching:

- For each topic, the server picks the most specific `TopicSchemaPolicy` whose
  MQTT topic filter matches (e.g., policy `factory/+/motor/#` matches
  `factory/line1/motor/speed`). That policy's `elementId` becomes the Object's
  `typeElementId`.
- If no policy matches, a synthetic type is assigned based on the decoded
  payload via `PayloadDecoder` (JSON → `JsonObject` / `JsonArray`, UTF-8 text
  → `String`, numeric text → `Number`, `"true"` / `"false"` → `Boolean`,
  binary → `Binary`).

`parentId` is the MQTT topic minus its last level, or `null` for top-level
topics. The root sentinel is `parentId: null`; enumerate roots with
`GET /objects?root=true`.

Element ID convention:

| Entity | Format | Example |
| --- | --- | --- |
| ObjectType | Schema policy name or synthetic name | `MotorSchema`, `JsonObject`, `Number` |
| RelationshipType | relationship name | `HasChildren` |
| Object | MQTT topic | `factory/line1/motor1/speed` |
| Namespace | URI | `mqtt://monstermq.local/sensors` |

### `GET /objects`

Query parameters:

| Parameter | Description |
| --- | --- |
| `typeElementId` | Return objects of a given type |
| `includeMetadata` | Include `metadata` object |
| `root=true` | Return root objects (top-level topic levels), where `parentId === null` |
| `parentId` | Project extension: return direct children of a parent topic |

### `POST /objects/list`

Request:

```json
{
  "elementIds": ["factory/line1/motor1"],
  "includeMetadata": false
}
```

Response is a bulk envelope with Object results or per-item 404 errors.

### `POST /objects/related`

Request:

```json
{
  "elementIds": ["factory/line1"],
  "relationshipType": "HasChildren",
  "includeMetadata": false
}
```

Response:

```json
{
  "success": true,
  "results": [
    {
      "success": true,
      "elementId": "factory/line1",
      "result": [
        {
          "sourceRelationship": "HasChildren",
          "object": {
            "elementId": "factory/line1/motor1",
            "displayName": "motor1",
            "typeElementId": "MotorSchema",
            "parentId": "factory/line1",
            "isComposition": true,
            "isExtended": false
          }
        }
      ]
    }
  ]
}
```

Supported relationship filters are `HasChildren`, `HasComponent`, `HasParent`,
and `ComponentOf`. `HasChildren`/`HasParent` walk the MQTT topic hierarchy;
`HasComponent`/`ComponentOf` walk JSON-object payload fields. Without a filter,
direct children and the direct parent are returned.

---

## Values

v1 value objects use VQT:

```json
{
  "value": 42,
  "quality": "Good",
  "timestamp": "2026-04-24T10:30:00.000Z"
}
```

Quality values:

| Quality | Meaning |
| --- | --- |
| `Good` | Valid value is present |
| `GoodNoData` | Topic exists but no retained value |
| `Bad` | Payload could not be decoded or store is unreachable |
| `Uncertain` | Retained value exists but violates the attached `TopicSchemaPolicy` |

MQTT current-value mapping:

- Value source: retained message from `IMessageStore.get(topic)`
- Timestamp source: `BrokerMessage.time` of the retained record
- Payload decoding: `PayloadDecoder` — tries JSON first (returns `JsonObject` /
  `JsonArray` / number / boolean), then UTF-8 text, then base64 for binary
- Primitive leaf ObjectInstance → scalar VQT with the decoded value
- Composition ObjectInstance (JSON-object leaf, folder, or topic parent) →
  `value: null, quality: "GoodNoData"` when no retained payload exists; when a
  JSON-object payload is present, `value` holds the decoded object and
  `components` is populated with the immediate child ObjectInstances' VQTs
  (keyed by child elementId) when `maxDepth > 1`. Component children come
  from both **JSON fields** of the payload and **sub-topics** of the topic.
  Clients drill further by passing a child's elementId.

### `POST /objects/value`

Request:

```json
{ "elementIds": ["factory/line1/motor1"], "maxDepth": 2 }
```

Response for a JSON-object leaf:

```json
{
  "success": true,
  "results": [
    {
      "success": true,
      "elementId": "factory/line1/motor1",
      "result": {
        "isComposition": true,
        "value": { "speed": 1500, "enabled": true },
        "quality": "Good",
        "timestamp": "2026-04-24T10:30:00.000Z",
        "components": {
          "factory/line1/motor1/state": {
            "isComposition": true, "value": null, "quality": "GoodNoData",
            "timestamp": "2026-04-24T10:30:00.000Z"
          },
          "factory/line1/motor1/speed": {
            "isComposition": false, "value": 1500, "quality": "Good",
            "timestamp": "2026-04-24T10:30:00.000Z"
          }
        }
      }
    }
  ]
}
```

For a primitive leaf the result is just
`{ isComposition: false, value, quality, timestamp }`.

`maxDepth` semantics:

- `1` — no recursion; compositions return no `components`
- `N > 1` — include up to N-1 levels of child `components`
- `0` — unlimited depth

### `PUT /objects/{elementId}/value`

Writes a current value. The current implementation accepts the raw JSON value
as the request body:

```json
42
```

The value is published to the MQTT topic as a retained message. Only leaf
Objects (primitive payload or JSON-object payload with no sub-topic children)
are writable. Intermediate topic levels (folders) are not writable as a whole.

Spec note: the v1 guide describes a VQT request body with `value`, optional
`quality`, and optional `timestamp`. This project currently writes the raw
value and ignores client-supplied quality/timestamp for current writes.

---

## History

MonsterMQ history source: archive groups (`ArchiveGroup` /
`IMessageArchiveExtended.getHistory`). Each topic is recorded by every archive
group whose topic filter matches; supported backends are PostgreSQL, CrateDB,
MongoDB, Kafka, and SQLite (`MessageArchiveType`). Payloads are decoded on
read by `PayloadDecoder`, so history values are returned as parsed JSON, UTF-8
text, or base64 binary (see commit 50b8e21).

### `POST /objects/history`

Request:

```json
{
  "elementIds": ["factory/line1/motor1/speed"],
  "startTime": "2026-04-24T00:00:00.000Z",
  "endTime": "2026-04-24T12:00:00.000Z",
  "maxDepth": 1,
  "maxValues": 1000
}
```

Response:

```json
{
  "success": true,
  "results": [
    {
      "success": true,
      "elementId": "factory/line1/motor1/speed",
      "result": {
        "isComposition": false,
        "values": [
          { "value": 1500, "quality": "Good", "timestamp": "2026-04-24T10:00:00.000Z" }
        ]
      }
    }
  ]
}
```

- `quality` is always `Good` for returned archive records; missing time
  buckets are not emitted.
- When `maxDepth > 1`, composition Objects (JSON-object-typed topics, or topic
  parents with children) are expanded into one bulk result per primitive leaf,
  using synthetic result IDs such as `factory/line1/motor1/state/running`.

### `GET /objects/{elementId}/history`

Project extension for reading one topic's history. Query parameters:
`startTime`, `endTime`, and optional `maxValues`.

### `PUT /objects/{elementId}/history`

Project extension for historical writes. Request body:

```json
{
  "data": [
    { "value": 1500, "timestamp": "2026-04-24T10:00:00.000Z" }
  ]
}
```

Only leaf Objects are writable. The write is forwarded to every archive group
that matches the topic and supports backfill; not all backends accept
out-of-order inserts (Kafka in particular is append-only), and such groups are
skipped with a per-item warning.

---

## Subscriptions

Subscriptions are in-memory per broker node and are lost when the broker
restarts. They monitor MQTT topics via the internal `SessionHandler` message
listener and are bridged to the i3X API by `I3xServer`.

The v1 spec requires `clientId` to scope subscriptions. This implementation
accepts and returns `clientId`, but currently does not require it.

### `POST /subscriptions`

Request:

```json
{ "clientId": "client-a", "displayName": "main-stream" }
```

Response:

```json
{
  "success": true,
  "result": {
    "clientId": "client-a",
    "subscriptionId": "sub-123",
    "displayName": "main-stream"
  }
}
```

### `POST /subscriptions/register`

Request:

```json
{
  "clientId": "client-a",
  "subscriptionId": "sub-123",
  "elementIds": ["factory/line1/motor1/speed"],
  "maxDepth": 1
}
```

Registers topics to monitor. Element IDs are translated directly into MQTT
topic filters on the underlying session; composition Objects expand into
filters for every leaf reached within `maxDepth`. Duplicate registrations
return success.

### `POST /subscriptions/unregister`

Request:

```json
{
  "clientId": "client-a",
  "subscriptionId": "sub-123",
  "elementIds": ["factory/line1/motor1/speed"]
}
```

Stops monitoring the listed IDs for future updates.

### `POST /subscriptions/stream`

Opens a Server-Sent Events stream:

```json
{
  "clientId": "client-a",
  "subscriptionId": "sub-123"
}
```

SSE update payloads are VQT-like updates:

```json
{
  "sequenceNumber": 1,
  "elementId": "factory/line1/motor1/speed",
  "value": 1500,
  "quality": "Good",
  "timestamp": "2026-04-24T10:30:00.000Z"
}
```

### `POST /subscriptions/sync`

Polls queued updates and optionally acknowledges prior updates:

```json
{
  "clientId": "client-a",
  "subscriptionId": "sub-123",
  "lastSequenceNumber": 1
}
```

If `lastSequenceNumber` is provided, the server removes all queued updates with
sequence numbers less than or equal to it before returning the remaining queue.

### `POST /subscriptions/list`

Bulk fetch subscription details:

```json
{
  "clientId": "client-a",
  "subscriptionIds": ["sub-123"]
}
```

### `POST /subscriptions/delete`

Bulk delete subscriptions:

```json
{
  "clientId": "client-a",
  "subscriptionIds": ["sub-123"]
}
```

---

## Implementation Notes

- The server is implemented in Kotlin on Vert.x; see
  `broker/src/main/kotlin/extensions/I3xServer.kt` for the HTTP surface.
- Retained (current) values come from `IMessageStore`; historical values come
  from `IMessageArchiveExtended.getHistory`.
- Payloads are decoded via `PayloadDecoder`: JSON is returned parsed, UTF-8
  text as a string, and true binary as a base64 `payload_base64` field.
- Topic hierarchy is computed purely from the `/` separator; there is no
  stored parent/child graph. `IMessageStore.findMatchingTopics("#")` enumerates
  the retained tree.
- MQTT wildcards (`+`, `#`) are used only internally for topic-filter matching
  and are never returned as element IDs.
- `TopicSchemaPolicy` matching is most-specific-first; ties break by
  registration order.
- Schema governance is partially implemented; see
  `dev/plans/TOPIC_SCHEMA_GOVERNANCE_ISSUE_112.md` for the full plan.

Current v1 alignment gaps to track:

- Bulk envelopes currently use top-level `success: true` even when an item in
  `results` failed.
- Subscription `clientId` is accepted and returned but not required or
  enforced.
- Current-value writes accept a raw JSON value instead of the v1 VQT body.
- Historical writes are backend-gated: append-only archive backends (Kafka)
  silently skip backfill.
- `TopicSchemaPolicy`-driven `Uncertain` quality on schema-violating retained
  payloads is not yet emitted; payloads that violate their schema are
  currently returned as `Good`.
