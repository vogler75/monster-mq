# MQTT Shared Subscriptions Implementation Plan

Related issue: [#156](https://github.com/vogler75/monster-mq/issues/156)

## Goal

Implement MQTT shared subscriptions in MonsterMQ Core and MonsterMQ Edge with
matching behavior. A publication matching a shared subscription group is
delivered to one eligible group member, while matching ordinary subscriptions
continue to receive their normal deliveries.

The two brokers are separate implementations (Kotlin/Vert.x and Go). Treat this
document as the shared behavioral contract; implement and test the behavior in
each repository rather than introducing a cross-repository runtime dependency.

## Shared behavior contract

- Accept filters in the form `$share/{ShareName}/{TopicFilter}`. Validate the
  group name and underlying topic filter according to MQTT, and reject malformed
  shared filters at subscribe time. Keep the original filter as the persisted
  subscription identity; match publications against the underlying filter.
- Keep shared-group membership distinct from ordinary subscriptions. A client
  with multiple matching subscriptions in the same group is still one candidate
  for that group. A client may independently receive an ordinary delivery and
  one delivery for each matching shared group.
- Select one member per matching group for each publication. Use round-robin
  selection among eligible connected members. If no connected member is
  eligible, select an unexpired persistent session so the existing QoS queue
  path can store the message. Do not select clean offline or expired sessions.
- Apply existing MQTT delivery rules to the selected member: no-local
  constraints, effective QoS, retain handling, subscription identifiers,
  authorization, session expiry, and queued-message expiry. Reject invalid
  shared-subscription options as required by the MQTT version.
- In a Core cluster, make one selection for the cluster-wide group and route the
  message only to the selected member's node. Nodes must not independently
  select a recipient for the same group and publication.
- On Edge, use the same local selection and offline-session behavior. Preserve
  PeerLink's existing `Receive.SharedSubscriptions` policy; do not broaden
  PeerLink delivery semantics as an incidental part of this feature.
- Advertise MQTT 5 Shared Subscription Available (CONNACK property 42) as `0`
  until shared subscriptions are implemented and tested on that broker; then
  advertise `1`.

## Implementation sequence

### 1. Confirm protocol and routing decisions

- Verify the exact MQTT validation and SUBACK behavior for malformed filters
  and incompatible MQTT 5 options against the supported protocol versions.
- Choose and document the cluster-wide Core selection mechanism. It must
  provide a single choice per group/publication across nodes and preserve
  delivery when the selected member is remote. Do not use independent
  per-node round-robin counters.
- Confirm how selected persistent offline members interact with existing
  queue limits and message expiry. Keep this policy consistent in Core and
  Edge.

### 2. Implement MonsterMQ Core

- Parse a shared filter into its group and underlying filter at the
  subscription boundary, validate it, and retain the original filter for
  persistence and unsubscribe matching.
- Extend the subscription index/routing model to separately find ordinary
  recipients and matching shared groups, including the group members and their
  session/node eligibility.
- Add group-aware selection and route each selected recipient through the
  existing direct-delivery or persistent-session queue mechanisms. Preserve
  ordinary subscriber fanout and ensure shared selection occurs only once
  before clustered node forwarding.
- Change property 42 to `1` only after the Core behavior is active.

### 3. Implement MonsterMQ Edge

- Verify the embedded MQTT engine's handling of live shared subscriptions,
  filter validation, and CONNACK property 42. Adapt its protocol hooks or
  delivery integration where the engine does not meet the shared contract.
- Extend Edge's subscription index and offline `QueueHook` so a publication is
  queued to at most one eligible member of each shared group, rather than every
  matching offline member. Keep selection aligned with the live delivery path
  to avoid duplicate or contradictory recipients.
- Preserve persisted subscriptions, reconnect behavior, expiry, queue limits,
  and the configured PeerLink shared-subscription policy.
- Change property 42 to `1` only after Edge's live and queued paths meet the
  contract.

### 4. Tests and documentation

Add equivalent tests in the Core and Edge repositories for:

- Valid and invalid shared filters, including the underlying wildcard filter.
- Shared versus ordinary delivery, multiple groups, overlapping filters, and
  duplicate matching subscriptions from one client.
- Round-robin behavior, unsubscribe, reconnect, and no eligible recipient.
- Persistent offline QoS queueing, clean/expired-session exclusion, queue
  limits, and message expiry.
- MQTT 3.1.1 and MQTT 5 behavior, including the CONNACK property 42 value and
  MQTT 5 subscription options.
- Core cluster routing across nodes and Edge PeerLink behavior under both
  configured shared-subscription policies.

Update the MQTT feature documentation for both brokers after implementation.
Keep the property 42 test aligned with the implementation rollout so it does
not claim support prematurely.

## Acceptance criteria

- Each matching shared group receives exactly one delivery per publication,
  including across Core cluster nodes.
- Ordinary subscriptions retain their existing fanout and do not get replaced
  by shared-group selection.
- Persistent offline delivery uses the existing queue mechanisms without
  duplicate delivery on reconnect.
- Core and Edge pass the equivalent behavioral test cases.
- MQTT 5 property 42 is `1` only on a broker that passes its shared-subscription
  tests.

## Out of scope

- New GraphQL schema or resolver surface.
- New configuration for choosing among selection algorithms; round-robin is
  the planned behavior.
- Changes to PeerLink policy beyond honoring its existing shared-subscription
  setting.
