# Clustering

MonsterMQ can join a Vert.x/Hazelcast cluster to share the event bus and coordinate message stores across nodes. The clustering feature is intentionally lightweight: the broker builds a default Hazelcast configuration at runtime and does not read any external Hazelcast YAML. This document explains what works today and the constraints you should plan for.

## Enabling Cluster Mode

Start the broker with the `-cluster` flag. The flag switches Vert.x into clustered mode and creates a Hazelcast cluster manager internally (`broker/src/main/kotlin/Monster.kt:360-438`).

```bash
java -cp "target/classes:target/dependencies/*" \
  at.rocworks.MonsterKt \
  -cluster \
  -config config.yaml
```

### Node Name

Optionally set a node label in your `config.yaml`:

```yaml
NodeName: node-a
```

When present, the name is stored as a Hazelcast member attribute and used by helper methods such as `Monster.getClusterNodeId(...)` (`broker/src/main/kotlin/Monster.kt:389-418`). Without it the broker falls back to the host name or the Hazelcast member UUID.

### Supported Store Types in Cluster Mode

The only component that makes special use of Hazelcast is the message store. If a last-value store is configured with `HAZELCAST`, MonsterMQ wires it against the cluster’s shared Hazelcast instance (`broker/src/main/kotlin/Monster.kt:738-769`). All other store types (PostgreSQL, CrateDB, MongoDB, SQLite, Memory) continue to operate exactly as on a single node.

Archive groups, session stores, retained stores, and user stores do not automatically replicate across nodes unless the chosen backend handles replication on its own.

## What Clustering Currently Provides

- **Shared event bus** – Vert.x uses Hazelcast to propagate internal events (GraphQL mutations, archive notifications, etc.) between nodes.
- **Optional Hazelcast message store** – Choosing `HAZELCAST` for `LastValType` gives you a distributed in-memory last-value cache.
- **Cluster-aware APIs** – Helper methods like `Monster.getClusterNodeIds(...)` and GraphQL fields (`broker/src/main/resources/schema.graphqls:240-356`) expose basic node information so you can build tooling around it.

## Limitations (Know Before You Deploy)

- **No Hazelcast YAML support** – Settings such as multicast, TCP/IP member lists, SSL, or partition groups from the old documentation are not loaded. The broker relies on Hazelcast defaults plus the optional `NodeName` attribute.
- **Manual failover** – Device connectors (OPC UA, archive groups, MQTT session ownership) stay with the node configured in the database. If a node fails you must reassign those resources yourself.
- **Single topic bus** – If you enable the Kafka bus it still uses one Kafka topic across the cluster; there is no automatic sharding by node.
- **External databases drive durability** – PostgreSQL, CrateDB, and MongoDB retain their own clustering semantics; MonsterMQ does not add another replication layer on top.

If you need advanced Hazelcast tuning (custom discovery, TLS, partition control) you must fork the broker and extend `Monster.clusterSetup(...)` to apply your configuration before instantiating `HazelcastClusterManager`.
