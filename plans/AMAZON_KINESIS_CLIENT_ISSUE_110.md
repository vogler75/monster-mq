# GitHub Issue Draft: Add Amazon Kinesis client/device connector

## Title

Add Amazon Kinesis client/device connector

## Body

## Summary

Add a first-class `KINESIS-Client` device type to MonsterMQ to bridge Amazon Kinesis Data Streams and the local MQTT broker.

## Goal

Implement a managed client/device integration for Amazon Kinesis following the existing connector pattern used by MQTT, Kafka, and NATS clients.

This should allow MonsterMQ to:
- consume records from a Kinesis stream and republish them into the local MQTT broker
- optionally publish local MQTT messages into a Kinesis stream
- manage the integration as a stored device with cluster-node assignment, GraphQL CRUD, and dashboard support

## Why

MonsterMQ already has a consistent device/connector architecture:
- `DeviceConfig`-based typed devices
- one `Extension` per integration type
- one `Connector` deployment per device
- GraphQL CRUD and metrics surface for managed clients

Amazon Kinesis fits this model well as a cloud stream bridge.

## Proposed Scope

### 1. New device type

Add a new device type constant:
- `KINESIS-Client`

Likely touchpoint:
- `broker/src/main/kotlin/stores/DeviceConfig.kt`

### 2. Device config model

Add a config model under `stores/devices/`, for example:
- `KinesisClientConfig.kt`

Suggested fields:
- `region`
- `streamName`
- `mode` or separate inbound/outbound enable flags
- `consumerType` (`polling` first, enhanced fan-out later if needed)
- `applicationName` or checkpoint namespace
- `accessKeyId`
- `secretAccessKey`
- `sessionToken`
- `useDefaultCredentialsProvider`
- `endpointOverride` for LocalStack/testing
- `payloadFormat` (`DEFAULT`, `JSON`, `TEXT`, `BINARY`)
- `destinationTopicPrefix`
- `topicKeyRegex`
- `topicKeyReplacement`
- `reconnectDelayMs`
- batching settings for outbound publish

Initial implementation should prefer a simple, safe subset.

### 3. Connector implementation

Add:
- `broker/src/main/kotlin/devices/kinesisclient/KinesisClientConnector.kt`
- `broker/src/main/kotlin/devices/kinesisclient/KinesisClientExtension.kt`

Pattern should mirror:
- `devices/kafkaclient/`
- `devices/natsclient/`

#### Inbound

Kinesis -> MQTT
- consume records from the configured Kinesis stream
- decode according to `payloadFormat`
- republish to the local broker via `SessionHandler.publishMessage`

#### Outbound

MQTT -> Kinesis
- subscribe as an internal client to configured MQTT topics
- write records using `PutRecord` / `PutRecords`

### 4. Payload mapping

Support formats similar to the Kafka bridge:
- `DEFAULT`: MonsterMQ/BrokerMessage-compatible envelope
- `JSON`: interoperable JSON envelope with topic, payload, and metadata
- `TEXT`: plain UTF-8 payload
- `BINARY`: raw bytes

Questions to settle:
- whether MQTT topic should be stored in the Kinesis partition key, payload envelope, or both
- how much MQTT metadata should be preserved in the default envelope

Recommendation:
- preserve topic in payload envelope
- optionally use topic as partition key by default

### 5. Delivery semantics

Design for Kinesis realities:
- at-least-once processing
- replay on restart/failure
- duplicate delivery possible
- explicit checkpointing required for inbound consumers

Need a checkpoint strategy:
- DynamoDB-backed KCL style
- or a simpler internal checkpoint store if not using KCL

Initial version should document delivery guarantees clearly.

### 6. GraphQL support

Add GraphQL queries/mutations equivalent to other client devices:
- list clients
- get client by name
- create
- update
- delete
- toggle
- reassign
- expose runtime metrics

Likely touchpoints:
- GraphQL query/mutation classes
- `GraphQLServer.kt`
- schema files under `broker/src/main/resources/`

### 7. Dashboard support

Add dashboard pages/scripts like existing client types:
- clients list page
- detail/edit page
- create/update workflow
- metrics display if implemented

### 8. Metrics

Expose per-client runtime metrics:
- messages in
- messages out
- errors
- dropped records
- last activity timestamp

Integrate with the existing metrics/history approach where practical.

## Implementation Notes

- Reuse the existing `DeviceConfig` + `Extension` + `Connector` lifecycle pattern
- Prefer AWS SDK v2
- Support `endpointOverride` so the connector can be tested against LocalStack
- Keep the first version minimal and production-safe
- Do not add this to broker core; implement it as a managed client/device integration

## Non-Goals for v1

- automatic stream creation/provisioning
- enhanced fan-out consumer support
- complex partition routing strategies
- exactly-once guarantees
- cross-account IAM role assumption unless trivial via the default credential chain

## Acceptance Criteria

- `KINESIS-Client` device type exists and can be stored, enabled, and assigned to a node
- broker deploys one connector per enabled and assigned Kinesis device
- inbound Kinesis -> MQTT works for at least one payload format
- outbound MQTT -> Kinesis works for at least one payload format
- GraphQL CRUD exists for the device
- basic metrics are exposed
- documentation covers config, credentials, payload mapping, and delivery semantics
- a test path exists using LocalStack or equivalent

## Suggested First Milestone

Implement inbound-only Kinesis -> MQTT using JSON payload mapping and LocalStack-based integration tests, then add outbound support in a follow-up.
