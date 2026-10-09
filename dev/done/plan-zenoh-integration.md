# Zenoh Federation for MonsterMQ

## Summary

Implement Zenoh as an embedded federation transport between independent MonsterMQ brokers. Each broker owns its local clients, sessions, subscriptions, and queues; Zenoh distributes live MQTT publications between brokers without requiring Hazelcast.

## Implementation Changes

- Add the official Zenoh JVM dependency (`org.eclipse.zenoh:zenoh-java:1.9.0`) and a `MessageBusZenoh` verticle connecting in `client` mode to configured `zenohd` routers.
- Translate configuration properties programmatically using `io.zenoh.Config.fromJson(String)` to open Zenoh sessions.
- Generalize `IMessageBus` by adding a capability property `val isExternal: Boolean` (default `false`), replacing hardcoded Kafka checks (e.g. in `SessionHandler.kt`).
- Ensure `nodeName` is consistently initialized from `NodeName` (falling back to hostname/UUID) at startup in both clustered and local modes, to act as a unique origin broker ID.
- Add `originNodeId: String? = null` to `BrokerMessage` (and update `BrokerMessageCodec.kt` accordingly).
- Implement outbound publication to the message bus in `SessionHandler.kt` for local publications (where `originNodeId` is null or matches the local node).
- Route inbound Zenoh publications: deliver locally but do not re-export. Check origin broker ID and a bounded UUID/TTL cache for loop prevention.
- Publish raw payloads at `<Prefix>/<mqtt-topic>` for native Zenoh interoperability, mapping MQTT topic filters to Zenoh key expressions (`+` -> `*`, `#` -> `**`), with a versioned metadata attachment containing `BrokerMessage` fields.
- Replicate retained publications, including zero-length retained deletion, but do not bootstrap retained history when a broker joins in v1.
- Forward topics matching `Allow` and not matching `Deny` filters using `TopicTree.matches`; default to all topics except `$SYS/#`.
- Keep cross-broker delivery live and at-most-once, without durable replay or cross-broker MQTT acknowledgements.

## Configuration and Public Interfaces

```yaml
Features:
  Zenoh: true

Zenoh:
  Enabled: true
  Mode: client
  Connect:
    - tcp/zenoh-router.example:7447
  Prefix: monstermq/mqtt
  Allow:
    - "#"
  Deny:
    - "$SYS/#"
  Deduplication:
    CacheSize: 100000
    TtlSeconds: 300
```

- Report `Zenoh` through `Broker.enabledFeatures` and validate all Zenoh configuration at startup.
- Reject configurations enabling both the Kafka message bus and Zenoh federation at startup.
- Fail startup if the initial Zenoh session cannot be established, recover after runtime interruptions, and close resources during shutdown.
- Document a production topology with two brokers connecting to redundant Zenoh routers over TCP port 7447.

## Test Plan

- Unit-test configuration, envelope round trips, MQTT 5 metadata, filtering, duplicate expiry, and retained deletion.
- Add a two-broker integration environment and verify bidirectional delivery, wildcards, binary payloads, QoS, MQTT 5 properties, GraphQL publications, filtering, retained state, loop prevention, and native Zenoh interoperability.
- Verify startup failure, runtime disconnect, recovery, and no replay of missed publications.
- Run Kotlin unit tests, Maven packaging, schema checks, and the dedicated Python integration suite.

## Assumptions

- Zenoh federates publications only; sessions, client ownership, subscriptions, queues, shared subscriptions, and ACL configuration remain broker-local.
- Brokers may use separate persistence databases and run without Vert.x/Hazelcast clustering.
- Delivery-time MQTT ACL checks remain active locally; Zenoh security is a deployment responsibility.
- Durable replay, retained bootstrap, global shared subscriptions, and client failover are deferred.
- No commit is created unless explicitly requested.
