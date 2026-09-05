# Clustering

MonsterMQ uses Vert.x and Hazelcast for a shared event bus, node discovery, and
cluster coordination. Configure all nodes to use the same persistent databases
for sessions, queues, and configuration. Local SQLite databases and `MEMORY`
stores do not become shared simply by enabling clustering.

## Start a Node

From the `broker` directory:

```bash
./run.sh -- -cluster -config config.yaml
```

Give each node a unique, stable name:

```yaml
NodeName: node-a
DefaultStoreType: POSTGRES
RetainedStoreType: HAZELCAST
Postgres:
  Url: jdbc:postgresql://shared-db:5432/monster
  User: system
  Pass: manager
```

In cluster mode `NodeName` is used as the Hazelcast instance name and member
attribute. The fallback is the hostname, or a generated name if hostname lookup
fails. Standalone brokers expose the node ID `local`.

## Discovery and Hazelcast Configuration

`HAZELCAST_MEMBERS` accepts comma-separated TCP/IP member addresses and disables
multicast discovery:

```bash
HAZELCAST_MEMBERS=node-a:5701,node-b:5701 ./run.sh -- -cluster
```

`HAZELCAST_CONFIG` loads a Hazelcast **XML** file through `XmlConfigBuilder`:

```bash
HAZELCAST_CONFIG=/etc/monstermq/hazelcast.xml ./run.sh -- -cluster
```

If the file is missing the broker logs a warning and falls back to the default
Hazelcast configuration. The broker overrides the cluster name with `MonsterMQ`
and sets the node identity after loading XML. `HAZELCAST_MEMBERS`, when supplied,
overrides the discovery settings afterward. See the repository's
[Docker Hazelcast example](../docker/hazelcast.xml).

## Shared State and Recovery

- The Vert.x event bus routes publications and internal requests across nodes.
- `HAZELCAST` retained and archive last-value stores use the shared Hazelcast
  instance. They are distributed in-memory stores, not disk persistence.
- Persistent MQTT sessions and queued messages use shared database storage.
  Clients must reconnect to a surviving broker; an existing TCP connection is
  not migrated. Client IDs and session-expiry/clean-start settings determine
  whether the stored session is resumed.
- Device connectors are assigned explicitly through `nodeId`. If their owner
  fails, they remain offline until it returns or an operator reassigns them.
- Use `brokers` and device queries in [GraphQL](graphql.md) to inspect the nodes.

## Feature Flags and Message Buses

Feature flags are configured per node. Assign devices only to nodes supporting
their feature; device mutations check the target node's advertised feature set.
The broker logs a warning when cluster members have different feature sets.

The optional Kafka message bus uses its configured bus topic. It does not replace
Hazelcast node coordination. [Zenoh federation](zenoh.md) connects independent
brokers and has different session and delivery semantics; Kafka bus and Zenoh
cannot be enabled together.

Implementation: [Monster.kt](../broker/src/main/kotlin/Monster.kt), especially
`clusterSetup`, store creation, and feature publication.
