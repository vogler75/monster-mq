# Zenoh federation

MonsterMQ can federate live MQTT publications between independent brokers through
[Eclipse Zenoh](https://zenoh.io/). Clients, sessions, subscriptions, queues, and ACLs remain
local to each broker; publications received by one broker are made visible to subscribers on
the others.

## Peer discovery topology

On the same LAN, configure every MonsterMQ broker as a Zenoh peer. The brokers discover one
another using multicast and do not require a central router. Each broker must use a unique
`NodeName` and the same `RemotePrefix`.

```yaml
NodeName: broker-a

Features:
  Zenoh: true

Zenoh:
  Enabled: true
  Mode: peer
  RemotePrefix: monstermq/mqtt
  Allow: ["#"]
  Deny: ["$SYS/#"]
  Deduplication:
    CacheSize: 100000
    TtlSeconds: 300
```

Broker B uses the same configuration with `NodeName: broker-b`. Peer discovery normally requires
both machines to share a multicast-capable network; UDP multicast must not be blocked by the host
firewall, VLAN, or container network.

A starting configuration is available at [`config-zenoh.yaml`](../broker/configs/config-zenoh.yaml).

MQTT topic `factory/line1/temperature` maps to Zenoh key
`monstermq/mqtt/factory/line1/temperature`. The Zenoh payload is the unmodified MQTT payload,
so native Zenoh applications can publish and subscribe directly. MonsterMQ metadata is carried
in a versioned Zenoh attachment.

## Semantics and limits

- Federation forwards live publications with deduplication; it does not durably queue or replay missed publications after an outage.
- Retained updates and retained deletions are replicated while brokers are connected.
- `$SYS/#` is local by default. `Deny` takes precedence over `Allow`.
- Kafka `Bus.Enabled` and `Zenoh.Enabled` are mutually exclusive.
- Zenoh TLS, authentication, and access control are configured in the Zenoh router/network.
- This feature does not provide shared sessions, global shared subscriptions, or client failover.

For routed networks, NAT boundaries, or networks where multicast is unavailable, use explicit
router-backed client mode:

```yaml
Zenoh:
  Enabled: true
  Mode: client
  Connect:
    - tcp/zenoh-router-1.example:7447
    - tcp/zenoh-router-2.example:7447
```

## Current-value queries

The bridge declares Zenoh queryables for its mapped `Allow` expressions. A Zenoh
query reads matching retained messages first, then deployed archive-group
last-value stores, returning at most one value per MQTT topic. Retained values
win when a topic exists in more than one store. This is a current-value lookup,
not an archive-history query or automatic reconnect synchronization.

A single match returns raw payload bytes with the MonsterMQ metadata attachment.
Multiple matches return a JSON array containing `topic`, `payload`, `qos`,
`clientId`, `messageUuid`, and `timestamp` (epoch milliseconds). In that array,
non-UTF-8 payloads become Base64 strings without a separate encoding flag.

`LocalPrefix` selects/removes an MQTT-side prefix; `RemotePrefix` replaces it on
the Zenoh side. Both default to empty strings. The explicit prefix in the example
keeps broker publications under `monstermq/mqtt`.

`Allow`/`Deny` filter live publications. Query processing currently does not apply
`Deny` or per-user MQTT ACL checks to individual returned topics. Configure Zenoh
network access controls for query access; these publication filters are not an
authorization boundary for current-value queries.
