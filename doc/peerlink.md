# PeerLink Inter-Broker Replication

PeerLink (`mmq-peer/1`) is a high-performance inter-broker replication protocol that connects independent MonsterMQ brokers—including the main Kotlin broker and the MonsterMQ Go Edge broker—without relying on external message brokers or distributed consensus clusters.

Replication operates on a peer-to-peer pull model with long-poll batch streaming, CRC-32C verification, retained snapshot synchronization, split-horizon loop protection, and end-to-end TLS 1.3 / mTLS / shared-secret authentication.

---

## 1. Topologies & Architecture

PeerLink can connect brokers in various topologies:

- **Edge-to-Central**: One or more lightweight Go Edge brokers replicate data to/from a central Kotlin broker.
- **Edge-to-Edge**: Two edge brokers replicate locally for high-availability.
- **Central Mesh**: Multiple central Kotlin brokers replicate select topics across sites.
- **Hybrid with Zenoh Federation**: PeerLink and Zenoh can coexist simultaneously on the same broker (see [Zenoh Coexistence](#zenoh-coexistence)).

```
  edge-a  <=== mmq-peer/1 ===>  main (central)
    |                              |
    +--------< mmq-peer/1 >--------+
```

### Core Architecture

- **Capture Tap**: Intercepts locally accepted MQTT publishes, wills, and internal publishes before queuing. Filtered by topic include/exclude rules.
- **In-Memory Ring Log**: High-throughput circular log (default 1024 slots) storing compressed frame records with monotonic epochs and 64-bit offsets.
- **Puller**: Client state machine running on JDK 21 virtual threads, connecting to remote peers, negotiating capabilities via `HELLO`, requesting retained snapshot synchronization (`FILL`), and streaming live records using pipelined long-poll batches.
- **Injector**: Applies pulled replica records directly to the local broker engine with publisher fidelity (retaining original `clientId`, `username`, QoS, timestamp, and MQTT 5 properties). Replicas are gated to prevent echo loops and outbound bridge redispatch.
- **Retained Queue Backpressure**: Retained replicas enqueue into the broker's existing asynchronous retained queue with blocking `put` semantics. If the queue is full, the injector pauses, propagating backpressure upstream to the puller.

---

## 2. Configuration Reference

The `PeerLink` configuration block in `config.yaml` is 100% compatible with the MonsterMQ Go Edge broker specification.

```yaml
NodeId: central-broker              # Top-level identifier (lowercase [a-z0-9._-]{1,64})

PeerLink:
  Enabled: true
  AllowUnauthenticatedPeers: false  # When true, plaintext loopback/private CIDR only

  Listener:
    Address: 0.0.0.0
    Port: 1890
    AllowedNetworks:                # CIDR allow-list for incoming connections
      - "127.0.0.1/32"
      - "10.0.0.0/8"
      - "192.168.0.0/16"
    MaxPreAuthPerIp: 4              # Concurrent unauthenticated handshake slots per IP
    AllowPlaintext: false           # Require TLS on listener

  Tls:
    Enabled: true
    AutoGenerate: true              # Automatically generates self-signed cert/key if missing
    CertPath: certs/peer-{NodeId}.pem
    KeyPath: certs/peer-{NodeId}.key
    TrustStorePath: certs/peer-ca.pem
    TrustStoreType: PEM             # PEM or PKCS12
    ClientAuth: REQUIRED            # NONE, REQUESTED, or REQUIRED (mTLS)
    IdentityFallback: NONE          # NONE, DNS, or CN

  SharedSecrets:                    # Group pre-shared secrets (Base64-encoded)
    - "cGVlcmxpbmstdGVzdC1ncm91cC1zZWNyZXQtdjEtMDEyMw=="

  KeepAliveSeconds: 15

  Log:
    MaxMessages: 1000000            # In-memory log record capacity
    MaxBytes: 134217728             # Maximum log byte size (default: 128 MiB)
    MaxRecordBytes: 524288          # Max individual record bytes (defaults to broker TCP max)
    DrainOnShutdownMs: 5000         # Grace period for consumers to drain during shutdown
    NeverConnectedWarnSec: 600      # Warn if a configured peer never connects

  Capture:
    Wills: true                     # Replicate LWT wills
    Include: ["#"]                  # Captured topic filter patterns
    Exclude:                        # Excluded topic filter patterns
      - "monstermq/hmi/sync/#"
    EchoSuppressMs: 2000            # Recaptured echo window suppression

  Snapshot:
    Mode: FILL                      # Retained sync mode: FILL, NONE, or NEWER
    MaxTopics: 500000               # Retained snapshot topic limit

  Fetch:
    MaxRecords: 2048                # Max records requested per fetch batch
    MaxBytes: 2097152               # Max batch byte size (2 MiB)
    MaxWaitMs: 2000                 # Long-poll timeout
    LingerMs: 5                     # Accumulation delay on source
    Pipeline: 2                     # Max in-flight batch requests
    CrcOnTls: false                 # Check CRC-32C even when TLS is active
    ReconnectMaxMs: 10000           # Jittered backoff ceiling

  Receive:
    Bus: true                       # Deliver replicas to internal bus / GraphQL subscriptions
    BridgeOutbound: false           # Redispatch replicas to MQTT bridges / Kafka / NATS
    Archive: true                   # Store replicas in historical archive DBs
    Queue: false                    # Deliver to offline persistent client queues
    MarkReplicas: true              # Add 'mmq-peer-src' user property
    CatchUpRateFactor: 2.5          # Pacing speedup during log replay
    MaxApplyRate: 50000             # Max replica messages applied per second (0 = unlimited)
    MaxRecordAgeMs: 60000           # Max age before treating message as RetainOnly
    InjectWorkers: 4                # Number of parallel injector threads

  Peers:
    - NodeId: edge-a
      Address: "edge-a.plant.local:1890"
      Serve: true                   # Accept incoming pulls from this peer
      Receive:
        Include: ["plant/edge-a/#"]
      Tls:
        Enabled: true
        PinnedSha256:               # SPKI SHA-256 certificate pins
          - "b6572a5c1146070ae6af4d3cf7a308a2bb0038b20f06081cf726a56c20434f2c"
```

---

## 3. Security and Authentication

PeerLink employs defense-in-depth:

1. **CIDR Network Filtering**: Connections from unauthorized IP networks are dropped immediately before TLS negotiation.
2. **Pre-Auth Slot Limiting**: Defends against handshake flooding by limiting concurrent unauthenticated TLS handshakes per IP (`MaxPreAuthPerIp`, default 4).
3. **Mutual TLS (mTLS)**: Peer identities are authenticated using standard X.509 client certificates with URI SAN format `urn:monstermq:node:<NodeId>`.
4. **SPKI Certificate Pinning**: Peers can be pinned by their public key SHA-256 hash (`PinnedSha256`), eliminating reliance on external Certificate Authorities.
5. **Shared Secret Authentication (RFC 8446 TLS 1.3 Exporter)**:
   - When `SharedSecrets` are configured, TLS 1.3 is strictly enforced.
   - Keying material is exported from the TLS session (`label = "monstermq-peer/1"`, 32 bytes) using BCJSSE on JDK 21 or native JSSE on JDK 25+.
   - A constant-time HMAC-SHA256 challenge-response handshake (`AUTH` frame) verifies the shared secret without ever transmitting it over the wire.
6. **Fail-Closed Validation**:
   - If `AllowUnauthenticatedPeers` is enabled, `AllowedNetworks` must be non-empty and restricted to private/loopback IP blocks.
   - Unrecognized configuration keys under `PeerLink:` abort broker startup immediately.

---

## 4. Retained Snapshot Synchronization

When a puller connects to a source peer:

1. **FILL Mode (Default)**: The puller requests a snapshot of the source's retained store. The source streams all retained messages matching the topic filter. The injector only applies messages for topics that do **not** already exist locally in the retained store or in the pending retained write queue.
2. **NEWER Mode**: Used during manual or operator-triggered resync. Applies snapshot messages if their capture timestamp is newer than the local retained timestamp.
3. **Queue Draining**: Before initiating the snapshot phase, the injector ensures the local asynchronous retained queue is drained to prevent stale overwrites.

---

## 5. Zenoh Coexistence

MonsterMQ supports running both PeerLink and Zenoh federation simultaneously on the same broker node without configuration switches:

- **PeerLink -> Zenoh**: Replicas received from PeerLink are forwarded to the Zenoh bus subject to the existing `Zenoh.Allow` and `Zenoh.Deny` topic filters.
- **Zenoh -> PeerLink**: Messages arriving via Zenoh are captured into the PeerLink log subject to `PeerLink.Capture.Include` and `Exclude`.
- **One-Hop Loop Guard**: Each replica carries the origin source NodeId in `BrokerMessage.peerSource` and `ZenohMessageEnvelope.peerSource`. A message that has crossed a PeerLink hop is never captured into another PeerLink log, preventing multi-hop forwarding loops.
- **UUID Deduplication**: Every replica generates a deterministic UUID from `(sourceNodeId, epoch, offset)`. If a broker receives the same message via both PeerLink and Zenoh, the Zenoh deduplication cache automatically drops the duplicate.

---

## 6. HTTP Status & Management API

PeerLink exposes diagnostic and management endpoints on the PeerLink listener port (default 1890):

### `GET /peerlink/v1/status`
Returns real-time replication health, consumer offsets, and throughput metrics formatted identically to the MonsterMQ Go Edge broker:

```bash
curl http://127.0.0.1:1890/peerlink/v1/status
```

Response JSON example:
```json
{
  "nodeId": "central-broker",
  "version": 1,
  "uptimeSeconds": 3600,
  "log": {
    "epoch": 1,
    "leo": 142050,
    "oldestOffset": 0,
    "committedOffset": 142050,
    "totalRecords": 142050,
    "totalBytes": 18456000,
    "capacityRecords": 1000000,
    "capacityBytes": 134217728
  },
  "sources": [
    {
      "nodeId": "edge-a",
      "state": "STREAMING",
      "address": "edge-a.plant.local:1890",
      "epoch": 1,
      "lastOffset": 45120,
      "committedOffset": 45120,
      "injected": 45120,
      "lagRecords": 0
    }
  ],
  "consumers": [
    {
      "nodeId": "edge-a",
      "connected": true,
      "epoch": 1,
      "ackedOffset": 142050,
      "lagRecords": 0,
      "servedRecords": 142050
    }
  ]
}
```

*Note: For security, plaintext HTTP status requests are accepted only from loopback (`127.0.0.1`). Over TLS, requests require authenticated peer certificates.*

### `POST /peerlink/v1/resync?source=<nodeId>`
Triggers an operator-initiated NEWER retained snapshot resync from the specified source peer.

---

## 7. Operational Guidelines & JVM Tuning

1. **JVM Memory (`-Xmx`)**: Ensure heap memory is sized adequately:
   $$\text{MaxHeap} \ge 2.2 \times \text{Log.MaxBytes} + 150\,\text{MiB}$$
   The default 128 MiB log requires at least 430 MiB of heap headroom.
2. **Message Size Alignment**: The Kotlin broker defaults `TCP.MaxMessageSizeKb` to 512 KB, whereas the Go Edge broker defaults to 1024 KB. Ensure `MaxMessageSizeKb` is aligned across all linked brokers to prevent frame truncation and size drops.
3. **Clustering & Kafka Incompatibility**: PeerLink cannot be used simultaneously with Hazelcast clustering (`-cluster`) or the Kafka message bus. The broker will refuse to start if either is configured.
