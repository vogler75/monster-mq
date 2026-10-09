# Edge Broker Topic Schema Governance

This plan describes how to implement the existing MonsterMQ topic schema governance functionality in the Go edge broker. The edge broker should expose the same GraphQL interface shape as the main Kotlin broker where supported, while keeping the implementation lightweight and incrementally extensible.

## Existing Broker Behavior

The main broker implements governance as JSON Schema validation for MQTT publish payloads. The model is split into two concepts:

- `TopicSchemaPolicy`: a reusable JSON Schema definition.
- `TopicNamespace`: a binding from an MQTT topic filter to a schema policy.

The GraphQL schema is defined in `broker/src/main/resources/schema-topic-schema.graphqls`. The runtime implementation lives mainly in:

- `broker/src/main/kotlin/graphql/TopicSchemaQueries.kt`
- `broker/src/main/kotlin/graphql/TopicSchemaMutations.kt`
- `broker/src/main/kotlin/schema/TopicSchemaPolicyCache.kt`
- `broker/src/main/kotlin/schema/JsonSchemaValidator.kt`
- `broker/src/main/kotlin/MqttClient.kt`

Policies and namespaces are stored as `DeviceConfig` entries:

- Policy type: `TopicSchema-Policy`
- Namespace type: `TopicNamespace`
- Policy namespace: `schema`
- Namespace `namespace` field: the governed MQTT topic filter
- Node assignment: `*`

The feature flags are `SchemaPolicy` and `TopicNamespace`. When a feature is disabled, the Kotlin GraphQL resolvers keep the schema available but return empty/null/false results or a structured error result, depending on the declared return type.

## GraphQL Contract To Match

The edge broker should implement these query fields:

- `topicSchemaPolicies: [TopicSchemaPolicy!]!`
- `topicSchemaPolicy(name: String!): TopicSchemaPolicy`
- `topicSchemaValidate(policyName: String!, payload: String!): SchemaPolicyValidationResult!`
- `topicNamespaces: [TopicNamespace!]!`
- `topicNamespace(name: String!): TopicNamespace`
- `topicNamespaceMatch(topic: String!): TopicNamespace`

The edge broker should implement these mutation groups:

- `topicSchemaPolicy.create(input: TopicSchemaPolicyInput!): TopicSchemaPolicyResult!`
- `topicSchemaPolicy.update(name: String!, input: TopicSchemaPolicyInput!): TopicSchemaPolicyResult!`
- `topicSchemaPolicy.delete(name: String!): Boolean!`
- `topicNamespace.create(input: TopicNamespaceInput!): TopicNamespaceResult!`
- `topicNamespace.update(name: String!, input: TopicNamespaceInput!): TopicNamespaceResult!`
- `topicNamespace.delete(name: String!): Boolean!`
- `topicNamespace.toggle(name: String!, enabled: Boolean!): TopicNamespaceResult!`

Keep field names, default values, result shapes, and error category strings compatible with the Kotlin broker:

- `payloadType` defaults to `JSON`.
- `enforcementMode` defaults to `REJECT`.
- Optional fields remain optional: `contentType`, `version`, `description`, `examples`, and `tags`.
- Validation errors use `PARSE_ERROR`, `SCHEMA_ERROR`, or `CONFIG_ERROR`.
- Mutation result errors are returned as `[String!]!`, not thrown GraphQL errors for normal validation failures.

## Runtime Publish Enforcement

The Kotlin broker validates publishes after ACL authorization and before message delivery. The edge broker should use the same order:

1. Reject MQTT publish topic names containing `+` or `#`.
2. Apply existing authentication/ACL publish checks.
3. Match the concrete publish topic against enabled topic namespaces.
4. If no namespace matches, continue publishing normally.
5. If a namespace matches, validate the UTF-8 payload string as JSON against the namespace's compiled JSON Schema.
6. If validation succeeds, continue publishing normally.
7. If validation fails and `enforcementMode` is `REJECT`, drop the publish.
8. For MQTT v5 QoS 1 publishes, return PUBACK with `Payload Format Invalid` where the Go MQTT stack supports this.

The current Kotlin broker only implements `REJECT` behavior even though `enforcementMode` is stored as a string. The edge broker should preserve that behavior first.

## Cache Design

The Kotlin broker builds an immutable in-memory cache:

1. Load all device configs.
2. Compile every `TopicSchema-Policy` JSON Schema.
3. Load enabled `TopicNamespace` configs.
4. Resolve `schemaPolicyName` to a compiled policy.
5. Insert namespace bindings into an MQTT topic tree.
6. Atomically swap the old tree for the new tree.

The Go implementation should mirror this with:

- A `TopicSchemaPolicyCache` service.
- A `sync.RWMutex` or `atomic.Value` holding an immutable cache snapshot.
- A topic filter matcher that supports MQTT `+` and `#` wildcards.
- A compiled validator per policy.
- Counters for `validated`, `rejected`, `parseError`, and `schemaError`.

Recommended Go package layout:

```text
internal/governance/
  model.go
  validator.go
  topic_matcher.go
  cache.go
  graphql.go
  mqtt_hook.go
```

Use a maintained Go JSON Schema library with draft-07 support. Candidate: `github.com/santhosh-tekuri/jsonschema/v5`. Default missing `$schema` to draft-07 to match the Kotlin `JsonSchemaValidator`.

## Persistence

Use the edge broker's existing configuration persistence mechanism if it already has one. If it has no generic device/config store, add a small repository abstraction first:

```go
type GovernanceStore interface {
    ListPolicies(ctx context.Context) ([]TopicSchemaPolicy, error)
    GetPolicy(ctx context.Context, name string) (*TopicSchemaPolicy, error)
    SavePolicy(ctx context.Context, policy TopicSchemaPolicy) error
    DeletePolicy(ctx context.Context, name string) (bool, error)

    ListNamespaces(ctx context.Context) ([]TopicNamespace, error)
    GetNamespace(ctx context.Context, name string) (*TopicNamespace, error)
    SaveNamespace(ctx context.Context, ns TopicNamespace) error
    DeleteNamespace(ctx context.Context, name string) (bool, error)
    ToggleNamespace(ctx context.Context, name string, enabled bool) (*TopicNamespace, error)
}
```

If the edge broker shares `DeviceConfig` concepts with the Kotlin broker, preserve the same serialized type names and config fields so configs can be synced between full broker and edge broker. If the edge broker uses its own storage, still expose the same GraphQL DTOs.

## Input Validation Rules

Match the Kotlin validation first:

- Names must be non-blank.
- Names must match `^[a-zA-Z0-9_-]+$`.
- `jsonSchema` is required for policies.
- `jsonSchema` must compile successfully before saving.
- `topicFilter` is required and non-blank for namespaces.
- Referenced `schemaPolicyName` must exist before creating or updating a namespace.
- Namespace and policy name conflicts should return mutation `errors`, not GraphQL transport errors.

The Kotlin implementation does not deeply validate MQTT topic filter syntax for namespaces. The edge broker may add stricter validation later, but the first compatibility pass should avoid rejecting filters that the main broker currently accepts unless they are impossible for the Go matcher.

## Implementation Phases

### Phase 1: GraphQL Types And Store

- Add the topic schema governance GraphQL types and fields to the edge broker schema.
- Add policy and namespace persistence models.
- Implement query resolvers for list/get/validate/match.
- Implement mutation resolvers for create/update/delete/toggle.
- Preserve result shapes and feature-disabled behavior.

### Phase 2: JSON Schema Validation

- Add a reusable JSON Schema validator wrapper.
- Default missing `$schema` to draft-07.
- Return `PARSE_ERROR` when payload is not a JSON object.
- Return `SCHEMA_ERROR` for schema validation failures.
- Add direct `topicSchemaValidate` tests.

Note: the Kotlin implementation parses payloads as JSON objects via `JSONObject(payload)`. Arrays and primitive JSON values are rejected as parse errors. Match this behavior unless the product explicitly decides to broaden support.

### Phase 3: Runtime Cache

- Implement cache reload from the governance store.
- Compile schemas once per reload.
- Resolve enabled namespaces to compiled validators.
- Atomically swap snapshots.
- Reload after every successful policy or namespace mutation.
- Expose counters internally for metrics/logging.

### Phase 4: MQTT Publish Enforcement

- Hook validation into the publish path after ACL checks and before delivery.
- Match namespace by concrete topic.
- Validate payload only when a namespace matches.
- Reject invalid messages in `REJECT` mode.
- For MQTT v5 QoS 1, map invalid payloads to a PUBACK payload-format error if supported.
- Add logs equivalent to the Kotlin broker: namespace name, schema policy name, topic, and validation detail.

### Phase 5: Compatibility Tests

- Add GraphQL tests for all query and mutation fields.
- Add policy create/update/delete tests.
- Add namespace create/update/delete/toggle tests.
- Add direct validation tests for valid JSON, invalid JSON, and schema violations.
- Add MQTT publish tests for governed and ungoverned topics.
- Add schema compatibility checks against `broker/src/main/resources/schema-topic-schema.graphqls`.

## Edge-Specific Constraints

- Avoid pulling in the full Kotlin `DeviceConfig` architecture unless the edge broker already has an equivalent.
- Avoid distributed invalidation; local reload after local mutation is enough for an edge node unless edge clustering is introduced.
- Keep GraphQL compatibility at the interface level even if some functionality is backed by a simpler local store.
- Feature flags should report `SchemaPolicy` and `TopicNamespace` only when the edge broker actually supports the implemented subset.

## Open Decisions

- Which persistence backend should the edge broker use for governance configs?
- Does the edge broker already have a feature flag mechanism compatible with `Broker.enabledFeatures`?
- Which Go MQTT library is used, and can it send MQTT v5 PUBACK reason code `Payload Format Invalid`?
- Should edge configs be import/export compatible with Kotlin `DeviceConfig` JSON?
- Should namespace match priority exactly mimic the Kotlin `TopicTree` insertion behavior, or should the edge broker define a stricter most-specific-match rule before implementation?
