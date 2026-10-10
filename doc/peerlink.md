# PeerLink

PeerLink links MonsterMQ brokers so that a message published on one broker is
also delivered by the others, with the same MQTT semantics. It is generic: any
two or more brokers can be linked, and Kotlin MonsterMQ brokers can be linked
with MonsterMQ Edge (Go) brokers (same protocol `mmq-peer/1`, same
configuration keys).

Typical uses:

- **Redundant pair**: two brokers on two hosts, clients may connect to either
  (active-active). PeerLink replaces clustering for this case; it cannot be
  combined with `-cluster`.
- **Mesh**: three or more brokers that all see each other's messages.
- **Edge to central**: an edge broker forwards to a central broker, in one or
  both directions, optionally filtered.

The protocol specification lives in the MonsterMQ Edge repository
(`dev/plans/spec-peerlink-redundancy.md`); this document covers concepts,
configuration and operation.

---

## 1. How it works

```
   clients ──► broker A                          broker B ◄── clients
               │ capture                          ▲ inject
               ▼                                  │
           in-memory log  ◄──── FETCH (long poll) ─┤  puller of B
           (offsets)      ────► BATCH ───────────►┘
```

- **Capture.** Every publish a broker accepts (network clients, wills,
  broker-internal publishers) is appended to an in-memory log with a
  monotonically increasing offset. Nothing is written to disk.
- **Pull.** Each peer that consumes from this broker runs a *puller*: it opens
  one TCP connection (port 1890 by default, optionally TLS), authenticates,
  and fetches batches with long polls. The broker serving its log is the
  *source*, the pulling broker the *consumer*.
- **Inject.** The consumer applies the records to its local engine with the
  original publisher's client id, username, QoS, retain flag, MQTT 5
  properties and publish time, then commits the offset back to the source.
- **Free.** A record is freed once every configured consumer has committed it.
  When the log is full (`Log.MaxBytes` / `Log.MaxMessages`), the oldest
  records are dropped anyway and counted.
- **Resume.** After a reconnect the consumer resumes at its committed offset.
  If the source restarted, its log has a new *epoch*; the consumer notices it
  and counts what was lost.
- **Snapshot.** On first contact (and after a source restart), the consumer can
  fetch the source's retained messages and fill topics that are missing
  locally (`Snapshot.Mode: FILL`).
- **One hop (split horizon).** A broker never captures what it received from a
  peer. PeerLink alone therefore cannot loop, but a chain or ring delivers one
  hop only: **with more than two brokers configure a full mesh**, in which
  every broker lists every other one.

The two directions of a pair are separate, independent links: A pulls from B,
and B pulls from A.

---

## 2. What is forwarded

Every publish a broker accepts: from network clients, wills, and from
broker-internal publishers (GraphQL publish API, bridges and device
connectors, flows). Broker-internal publishes carry the client
id `inline`. Not forwarded:

- topics starting with `$`;
- topics outside `Capture.Include` (default `["#"]`) or inside
  `Capture.Exclude` (default: the HMI sync channel `<HMI.SyncBaseTopic>/#`,
  i.e. `monstermq/hmi/sync/#`; `[]` forwards it too);
- wills fired because the broker itself shuts down (`Capture.Wills: false`
  skips all wills);
- publishes larger than `Log.MaxRecordBytes` (counted as `captureDropped`).

On the receiving side, `Peers[].Receive.Include` / `Exclude` filter what this
broker accepts from that peer. Replicas always reach local subscribers and the
retained store. The other subsystems are controlled by `PeerLink.Receive`:

| Subsystem | Default | Key |
|---|---|---|
| Internal bus (GraphQL subscriptions, flows, Zenoh) | gets replicas | `Bus: true` |
| Archive groups | get replicas | `Archive: true` (set `false` when both brokers archive into one shared database) |
| Outbound bridges (MQTT client, Kafka, NATS, …) | do **not** forward replicas (loop guard) | `BridgeOutbound: false` |
| Offline queues of persistent sessions | get replicas (a session that reconnects on the other broker loses nothing) | `Queue: true` |
| Shared subscription groups | each message once, on the broker it was published on | `SharedSubscriptions: SKIP` (or `DELIVER`) |

Network clients may not use the client ids `inline` or `peerlink:*`.

---

## 3. Getting started

### 3.1 NodeId and peers

Every broker needs a unique `NodeId` (top-level key in `config.yaml`). The
default is the first label of the host name; when the host name is unknown,
set it explicitly. NodeIds are
compared in lower case and may contain `[a-z0-9._-]`, 1 to 64 characters.

A link is configured on **both** sides:

- a peer with an `Address` is a source: this broker pulls from it;
- a peer with `Serve: true` (the default) may pull from this broker.

The `Peers` entry whose `NodeId` equals this broker's own is ignored, so **one
file can serve every host**; only `NodeId` differs.

### 3.2 A pair with TLS and a shared secret

The simplest secure setup. Self-signed certificates are generated on first
start, and the shared secret, bound to the TLS 1.3 session, authenticates the
peers, so the certificates need not be verified.

```yaml
NodeId: broker-a                    # broker-b on the other host; the rest is identical
PeerLink:
  Enabled: true
  Tls:
    Enabled: true
    AutoGenerate: true              # certs/peer-{NodeId}.pem and .key on first start
  SharedSecrets: ["<base64, at least 16 bytes: openssl rand -base64 32>"]   # same on both hosts
  Peers:
    - { NodeId: broker-a, Address: "broker-a.local:1890" }
    - { NodeId: broker-b, Address: "broker-b.local:1890" }
```

### 3.3 mTLS with a peer CA (production, meshes)

Each broker has a certificate with the URI SAN `urn:monstermq:node:<NodeId>`
and the extended key usages serverAuth and clientAuth, issued by a dedicated
peer CA (do not reuse the CA of MQTT client certificates; system roots are
never used).

```yaml
NodeId: broker-a
PeerLink:
  Enabled: true
  Tls:
    Enabled: true
    CertPath: certs/peer-{NodeId}.pem
    KeyPath: certs/peer-{NodeId}.key
    TrustStorePath: certs/peer-ca.pem
    ClientAuth: REQUIRED
  Peers:
    - { NodeId: broker-a, Address: "broker-a.local:1890" }
    - { NodeId: broker-b, Address: "broker-b.local:1890" }
    - { NodeId: broker-c, Address: "broker-c.local:1890" }
```

A minimal peer CA with OpenSSL:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=peer-ca" -keyout peer-ca.key -out peer-ca.pem
for n in broker-a broker-b broker-c; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$n" -keyout peer-$n.key -out peer-$n.csr
  openssl x509 -req -in peer-$n.csr -CA peer-ca.pem -CAkey peer-ca.key -CAcreateserial -days 825 \
    -extfile <(printf "subjectAltName=URI:urn:monstermq:node:$n,DNS:$n.local\nextendedKeyUsage=serverAuth,clientAuth") \
    -out peer-$n.pem
done
```

### 3.4 Pinned self-signed certificates (no CA)

`AutoGenerate: true` and `ClientAuth: REQUIRED` on every broker, and per peer
the SHA-256 of its public key, which each broker logs at startup
(`spkiSha256`):

```yaml
  Tls: { Enabled: true, AutoGenerate: true, ClientAuth: REQUIRED }
  Peers:
    - NodeId: broker-b
      Address: "broker-b.local:1890"
      Tls: { PinnedSha256: ["<spkiSha256 of broker-b>"] }
```

### 3.5 One direction only

Edge forwards to central, central sends nothing back:

```yaml
# on edge broker-a
  Peers:
    - { NodeId: central, Serve: true }                  # central may pull from broker-a
# on central
  Peers:
    - { NodeId: broker-a, Address: "broker-a:1890", Serve: false }   # central pulls, never serves
```

A broker that only pulls does not open the peer port on the network; it binds
`127.0.0.1:<Listener.Port>` for the status endpoint only.

### 3.6 Unauthenticated (trusted networks only)

Plain TCP without authentication is accepted only with
`AllowUnauthenticatedPeers: true` together with a non-empty
`Listener.AllowedNetworks` and `UserManagement.Enabled: false` (replicas are
injected without ACL checks). A WARN is logged on every start.

### 3.7 Rotating secrets and pins

`SharedSecrets` and `PinnedSha256` are lists. To rotate without losing the
link: add the new value on both sides, move it to the first position on both
(the first secret signs, all are accepted), then remove the old one.

---

## 4. Configuration reference

All keys live under `PeerLink`. **Unknown keys fail startup, even while
`Enabled` is false.** Validation of values runs when `Enabled` is true.
`broker/config-default.yaml` has a commented example; `yaml-json-schema.json`
gives editor completion.

### 4.1 General

| Key | Default | Description |
|---|---|---|
| `Enabled` | `false` | Turn PeerLink on. |
| `AllowUnauthenticatedPeers` | `false` | Admit peers without TLS identity or secret. Needs `Listener.AllowedNetworks`; not allowed with `UserManagement.Enabled`. |
| `SharedSecrets` | `[]` | Group secrets, base64, at least 16 decoded bytes. First = current (signs), others still accepted. Need `Tls.Enabled`. With more than two brokers prefer `Peers[].SharedSecrets`: a group secret lets any holder claim any NodeId of the group. |
| `KeepAliveSeconds` | `10` | Link keepalive; a silent link is closed after about three intervals. ≥ 1. |

Top level: `NodeId` (see 3.1).

### 4.2 `Listener`

The peer port. It is bound on `Address` when at least one peer has
`Serve: true`.

| Key | Default | Description |
|---|---|---|
| `Address` | `0.0.0.0` | Bind address. |
| `Port` | `1890` | 1..65535. |
| `AllowedNetworks` | `[]` (all) | CIDR allow-list, checked before TLS. Also applies to the status endpoint, so include `127.0.0.1/32`. |
| `MaxPreAuthPerIp` | `2` | Concurrent connections per IP that are not yet authenticated. ≥ 1. |
| `AllowPlaintext` | `false` | A TLS listener also accepts plaintext sessions (TLS migration only). Needs `AllowUnauthenticatedPeers`. |

### 4.3 `Tls`

This broker's identity and trust. Paths may contain `{NodeId}`.

| Key | Default | Description |
|---|---|---|
| `Enabled` | `false` | TLS on the listener, and the default for outgoing connections (`Peers[].Tls.Enabled` overrides). Needs `CertPath` + `KeyPath`, or `AutoGenerate`. |
| `CertPath` / `KeyPath` | with `AutoGenerate`: `certs/peer-{NodeId}.pem` / `.key` | PEM certificate and unencrypted PEM key. |
| `AutoGenerate` | `false` | Create a self-signed certificate (URI SAN `urn:monstermq:node:<NodeId>`) when the files are missing. |
| `TrustStorePath` | – | Peer CA (PEM, or PKCS12). Empty = verify by pins or secrets only. |
| `TrustStoreType` | `PEM` | `PEM` or `PKCS12` (legacy ciphers only). |
| `TrustStorePassword` | – | For PKCS12. |
| `ClientAuth` | `NONE` | `NONE`, `REQUEST` or `REQUIRED`: whether consumers must present a certificate (mTLS). Needs `Enabled`, and `TrustStorePath` or pins for each serving peer. |
| `IdentityFallback` | `NONE` | `NONE`, `DNS` or `CN`. Only for certificates that carry no `urn:monstermq:node:` URI at all: accept a DNS SAN or the CN equal to the NodeId. |

### 4.4 `Log`

The in-memory log of captured publishes.

| Key | Default | Description |
|---|---|---|
| `MaxMessages` | `2000000` | Record limit. ≥ max(100, `Fetch.MaxRecords`). |
| `MaxBytes` | `268435456` (256 MiB) | Byte limit. ≥ 1 MiB and ≥ 4 × `MaxRecordBytes`. |
| `MaxRecordBytes` | `0` = `TCP.MaxMessageSizeKb` + 64 KiB | Larger publishes are not captured (`captureDropped{size}`). |
| `DrainOnShutdownMs` | `2000` | On shutdown, wait up to this long for connected consumers to catch up. 0 = off. |
| `NeverConnectedWarnSec` | `300` | WARN once per consumer that has not connected after this many seconds. 0 = off. |

### 4.5 `Capture`

What this broker offers to its peers.

| Key | Default | Description |
|---|---|---|
| `Wills` | `true` | Capture wills (never the wills of this broker's own shutdown). |
| `Include` | `["#"]` | Topic filters to capture. |
| `Exclude` | unset = `["<HMI.SyncBaseTopic>/#"]` | Topic filters not to capture. `[]` captures the HMI sync channel too. |
| `EchoSuppressMs` | `0` | Skip a publish that repeats a replica (same topic, payload and retain flag) within this window; guards against external clients that republish what they receive. 0 = off. |

### 4.6 `Snapshot`

| Key | Default | Description |
|---|---|---|
| `Mode` | `FILL` | `FILL`: on first contact and after a source restart, fetch the source's retained messages and set the topics that are absent locally. `OFF`: no snapshot. |
| `MaxTopics` | `1000000` | Upper bound of topics in one snapshot. |

### 4.7 `Fetch`

How this broker pulls from its sources.

| Key | Default | Description |
|---|---|---|
| `MaxRecords` | `4096` | Records per batch. |
| `MaxBytes` | `1048576` (1 MiB) | Bytes per batch (at least one record is always returned). |
| `MaxWaitMs` | `1000` | Long-poll timeout. 10 ≤ value < `KeepAliveSeconds` × 1000. |
| `LingerMs` | `0` | The source waits up to this long to fill a batch (fewer, larger batches). |
| `Pipeline` | `1` | `1` or `2` batches in flight. |
| `CrcOnTls` | `false` | Also send and check batch CRC-32C on TLS links (TLS already protects integrity). |
| `ReconnectMaxMs` | `30000` | Ceiling of the jittered reconnect backoff. |

### 4.8 `Receive`

How replicas are applied on this broker.

| Key | Default | Description |
|---|---|---|
| `Bus` | `true` | Deliver replicas to the internal bus (GraphQL subscriptions, flows, Zenoh). |
| `BridgeOutbound` | `false` | Let outbound bridges (MQTT client, Kafka, NATS, …) forward replicas. Loop risk, see [Loop guards](#8-loop-guards). |
| `Archive` | `true` | Archive groups store replicas. |
| `Queue` | `true` | Offline persistent sessions queue replicas. |
| `SharedSubscriptions` | `SKIP` | `SKIP`: shared subscription groups get a message only on the broker it was published on. `DELIVER`: also replicas. |
| `MarkReplicas` | `false` | Add the user property `mmq-peer-src=<NodeId>` to replicas. |
| `CatchUpRateFactor` | `3` | While catching up (lag > `Fetch.MaxRecords`), apply at most this factor × the source's publish rate (at least 1000/s), so local subscribers are not flooded. 0 = no pacing, else ≥ 1.5. |
| `MaxApplyRate` | `0` | Hard cap of replicas applied per second. 0 = off. |
| `MaxRecordAgeMs` | `0` | Non-retained replicas older than this are dropped (`dropped{stale}`); retained ones only update the store. 0 = off. |
| `MaxFrameBytes` | `16842752` (16 MiB + 64 KiB) | Largest frame accepted from a source. ≥ `Fetch.MaxBytes` + 64 KiB. |
| `InjectWorkers` | `1` | 1..16. |

### 4.9 `Peers[]`

| Key | Default | Description |
|---|---|---|
| `NodeId` | required | The peer's NodeId. Unique (case-insensitive). |
| `Address` | – | `host:port` of the peer's listener. Present = this broker pulls from the peer. |
| `Serve` | `true` | The peer may pull from this broker. A peer needs `Address` or `Serve: true`. |
| `SharedSecrets` | `[]` | Per-peer secrets; replace the group secrets for this peer. |
| `Tls.Enabled` | `PeerLink.Tls.Enabled` | TLS for the outgoing connection to this peer. |
| `Tls.PinnedSha256` | `[]` | SHA-256 of the peer's SubjectPublicKeyInfo or certificate (64 hex digits, colons/spaces allowed). A match replaces chain verification. Needs TLS. |
| `Tls.CertificateIdentity` | `urn:monstermq:node:<NodeId>` | Accepted identity instead (exact URI SAN or DNS SAN). |
| `Tls.ServerName` | – | TLS SNI only. |
| `Tls.RequireClientCert` | `false` | Require a client certificate from this peer (needs `ClientAuth` `REQUEST` or `REQUIRED`). |
| `Tls.InsecureSkipVerify` | `false` | Do not verify the peer's certificate; the pull direction then counts as unauthenticated unless a shared secret is used. |
| `Receive.Include` / `Receive.Exclude` | `["#"]` / `[]` | Topic filters for records accepted from this peer. |
| `Interest` | `INHERIT` | `INHERIT` or `OFF`. `OFF` turns interest routing off with this peer in both directions (dense link). See [Interest routing](#13-interest-routing). |

### 4.10 `Interest`

Interest routing, see [Interest routing](#13-interest-routing).

| Key | Default | Description |
|---|---|---|
| `Enabled` | `false` | Announce this broker's subscriptions to its sources and serve peers only what they subscribe to. At most 64 consuming peers. |
| `Unknown` | `ALL` | What a capable peer gets before its first interest snapshot arrives: `ALL` or `NONE`. |
| `FlushMs` | `5` | Coalescing window for interest changes sent to sources. > 0. |
| `MaxScanPerFetch` | `65536` | Records a source skips at most per `FETCH` before it answers. ≥ 1024. |
| `MaxFiltersPerPeer` | `100000` | A peer announcing more filters is served everything (one WARN, `interestOverLimit`). ≥ 1. |
| `MaxFilterBytes` | `1024` | Longer filters are not announced (`interestRejected`). 1..32768. |

```yaml
PeerLink:
  Interest:
    Enabled: true
    Unknown: ALL          # serve everything until the peer's first snapshot
    FlushMs: 5
    MaxScanPerFetch: 65536
    MaxFiltersPerPeer: 100000
    MaxFilterBytes: 1024
  Peers:
    - NodeId: edge-1
      Address: "edge-1.local:1890"
      Serve: true
    - NodeId: legacy-1      # old broker or no filtering wanted: dense link
      Address: "legacy-1.local:1890"
      Interest: OFF
```

`Interest.Enabled` acts in both directions: this broker announces its own
interest to the peers it pulls from, and filters what it serves to peers that
announce theirs. Both sides need it for filtering in a direction; otherwise
the link stays dense. `Unknown: NONE` saves bandwidth right after a consumer
connects but delays its first records until the snapshot arrives (usually a
few milliseconds); keep `ALL` unless the link is very constrained.

### 4.11 Validation

Startup fails (fail-closed) when, among others:

- a peer has neither `Address` nor `Serve: true`, or two peers share a NodeId;
- no peer other than this broker is configured;
- a direction is not authenticated: a serving peer needs TLS with a client
  certificate (`ClientAuth: REQUIRED`, or `REQUEST` with `RequireClientCert`)
  or a shared secret; a pulled peer needs TLS with `TrustStorePath`, a pin or
  a shared secret (or `InsecureSkipVerify` plus a secret), unless
  `AllowUnauthenticatedPeers` is set;
- secrets or pins are used without TLS in that direction;
- `Interest.Unknown` is not `ALL`/`NONE`, `Peers[].Interest` is not
  `INHERIT`/`OFF`, or `Interest.Enabled` is set with more than 64 serving
  peers;
- a numeric value is out of range (`Fetch.MaxWaitMs` vs. `KeepAliveSeconds`,
  `Log.MaxBytes` vs. `MaxRecordBytes`, `Receive.MaxFrameBytes` vs.
  `Fetch.MaxBytes`, …).

Startup warnings: `AllowUnauthenticatedPeers` set; group secrets with more than
two brokers; `Tls.TrustStorePath` equal to the MQTT TCPS truststore; retained
store `MEMORY` with `Snapshot.Mode: OFF`; no `Peers` entry matches this host.

PeerLink cannot be enabled together with clustering (`-cluster`) or the Kafka
message bus; startup aborts.

---

## 5. Retained messages

- A forwarded retained message is written to the consumer's own retained
  store (memory or database), whatever store the source uses. Retained
  replicas go through the broker's asynchronous retained queue; when it is
  full, the injector waits (backpressure up to the puller). An empty retained payload deletes the topic on the consumer too.
- **Snapshot `FILL`** fills only topics that are absent locally, so it never
  overwrites a newer local value. It runs on first contact and after the
  source restarted.
- **Resync**: `POST /peerlink/v1/resync?source=<NodeId>` (loopback only)
  fetches the source's retained messages again and overwrites local values
  that are more than 1 s older (NEWER).
- Active-active conflicts resolve by arrival order: on each broker the
  retained message applied last wins. Clients publishing the same retained
  topic on both brokers at the same time can leave them with different values
  until the topic is published again.

---

## 6. Delivery guarantees (RPO)

PeerLink is not synchronous replication: a PUBACK means the local broker
accepted the message, not that a peer has it. With the link up on a LAN, the
data at risk is the replication lag.

| Event | Result |
|---|---|
| Connection drop, both brokers running | No loss and no duplicates (resume by offset), while the backlog fits into the source's log |
| Consumer graceful restart | No loss and no duplicates; missing retained values come back through the snapshot |
| Consumer crash | Records applied after the last commit, at most one batch, are delivered again (at least once) |
| Source graceful stop | The source waits up to `Log.DrainOnShutdownMs` for connected consumers. What it could not serve is logged (`shutdownUnserved`, `uncapturedAtShutdown`). |
| Source crash | Records not yet pulled are lost; the consumer counts `sourceResets` and a lower bound in `resetLostLowerBound` |
| Consumer down longer than the log holds | The oldest records are dropped and counted on both sides (`lostTotal`, `gapLostTotal`) |

QoS 2 is not exactly-once across a consumer crash. No loss is silent: every
drop is counted in the status.

---

## 7. Security

1. **Network filter**: `Listener.AllowedNetworks` is checked before TLS.
2. **Pre-auth limit**: `Listener.MaxPreAuthPerIp` limits handshakes per IP.
3. **mTLS**: identity is the URI SAN `urn:monstermq:node:<NodeId>` (or
   `Peers[].Tls.CertificateIdentity`); the trust anchor is a dedicated peer CA.
4. **Pins**: `PinnedSha256` replaces chain verification for that peer.
5. **Shared secrets**: an HMAC challenge-response bound to the TLS 1.3 session
   (exporter), so a secret is never sent and cannot be replayed on another
   connection.
6. **Handshake checks**: a broker refuses itself (`self_connection`), a NodeId
   it does not expect (`unknown_peer`, `wrong_node`), a peer with
   `Serve: false` (`not_allowed`), and a second instance claiming the same
   NodeId (`duplicate_node`).
7. Refusals are logged as ERROR; with authentication configured, the reason is
   not sent to the remote side.

---

## 8. Loop guards

PeerLink never forwards a replica again, but other components can turn a
replica into a new publish, which is then forwarded like any other:

- Never point an MQTT bridge, inbound or outbound, at a peer broker.
- Run every device connector that publishes into the broker (MQTT client
  bridges with inbound subscriptions, OPC UA, WinCC OA/Unified, PLC4X, flows)
  on one broker only, or its output arrives twice.
- Outbound-only bridges may run on every broker with
  `Receive.BridgeOutbound: false` (the default), so each forwards exactly its
  own broker's publishes.
- An archive group that writes into a database shared by several brokers
  belongs to one broker, or set `Receive.Archive: false`.
- External clients that republish what they receive: set
  `Receive.MarkReplicas: true` so they can filter on `mmq-peer-src`, or
  `Capture.EchoSuppressMs`.
- Shared subscription groups need members on every broker.

### Zenoh coexistence

PeerLink and Zenoh federation can run on the same broker:

- Replicas received through PeerLink reach Zenoh subject to `Zenoh.Allow` /
  `Zenoh.Deny`.
- Messages arriving through Zenoh are captured into the PeerLink log subject
  to `Capture.Include` / `Exclude`.
- A message that crossed a PeerLink hop carries its origin (`peerSource`) and
  is never captured into another PeerLink log.
- A message that arrives through both PeerLink and Zenoh is applied once
  (`zenohDupSkipped` in the status).

---

## 9. Sizing

`Log.MaxBytes` (default 256 MiB) bounds the log. A 200-byte record (topic,
client id and payload) takes about 224 bytes, so the default holds about 1.2
million records, about 60 s of outage at 20,000 msg/s. The status reports the
remaining `capacitySeconds` at the current publish rate. Size the log for the
longest consumer outage that must be bridged without loss.

While the log is full the JVM needs heap for about twice `MaxBytes`: size
`-Xmx` to at least 2.2 × `Log.MaxBytes` + 150 MiB (about 710 MiB with the
default 256 MiB log), or lower `Log.MaxBytes`.

Use the same maximum message size on all linked brokers (`TCP.MaxMessageSizeKb`,
default 512 KB here, 1 MiB on Edge). A larger message than the
receiver accepts is dropped there (`dropped{size}`).

---

## 10. Monitoring

### 10.1 Status endpoint

```bash
curl -s http://127.0.0.1:1890/peerlink/v1/status
```

Served on the peer port to loopback clients, and over TLS to mTLS-authenticated
peers. A broker that only pulls binds `127.0.0.1:<Listener.Port>` for it.
Requests with an `Origin` header or a non-loopback `Host` are refused (browser
guard), so use `curl` with `127.0.0.1` or `localhost`.

| Section | Main fields |
|---|---|
| `log` | `epoch`, `lso`, `leo`, `lwm`, `records`, `bytes`, `maxBytes`, `capacitySeconds`, `appended{client,inline,will}`, `evictedUnread`, `evictedBy`, `captureDropped`, `echoSuppressed`, `uncapturedAtShutdown` |
| `admission` | `accepted`, `refusedNetwork`, `refusedBusy`, `refusedPlaintext`, `tlsFailures`, `authFailures{code}` |
| `consumers[]` (peers pulling from this broker) | `nodeId`, `state` (`NEVER_CONNECTED`, `CONNECTED`, `DISCONNECTED`), `remote`, `committed`, `lag`, `lostTotal`, `servedRecords`, `snapshotServed`, `shutdownUnserved`, `oaRetained`, `topicRootMismatch`, `retainedClassMismatch` |
| `consumers[].interest` | `state` (`UNKNOWN`, `LIVE`, `DISCONNECTED`, `OFF`), `mode` (`FILTERED`, `ALL`, `NONE`), `filters`, `filtersPersistent`, `snapshotGeneration`, `lastSnapshotAt`, `instanceId` (hex) |
| `interest` (only with `Interest.Enabled`) | `interestSkipped`, `interestMatched`, `sparseBatches`, `volatileDropped`, `persistentExpired`, `interestBacklogDiscarded`, `interestRejected`, `interestOverLimit`, `deltasReceived`, `local{filters,generation,rejected}` (this broker's announced interest) |
| `sources[]` (peers this broker pulls from) | `interest{active,deltasSent,snapshotsSent}` (only while interest routing is on for the peer), `nodeId`, `address`, `state` (`STOPPED`, `BACKOFF`, `DIALING`, `HANDSHAKE`, `SNAPSHOT`, `STREAMING`), `lagRecords`, `injected`, `retainOnly`, `dupSkipped`, `dropped{malformed,size_source,namespace,filtered,size,expired,stale,will_superseded}`, `gapLostTotal`, `sourceResets`, `resetLostLowerBound`, `retainedDiverged`, `snapshotFilled`, `clockSkewMs`, `rttMs`, `applyDelayMs{p50,p99,p99_9}`, `lastError`, `oaRetained` |

`retainedClassMismatch` is informational: replication works across different
retained store types.

`POST /peerlink/v1/resync?source=<NodeId>` (loopback only) starts a NEWER
retained resync from that source (see [Retained messages](#5-retained-messages)).

`oaRetained` and `topicRootMismatch` are always false on this broker (WinCC OA
specific, Edge only).

The same document is available over GraphQL as `peerLink.status`, together
with the configured peers and the state of their links, for the dashboard's
PeerLink page:

```graphql
{ peerLink { enabled nodeId listen tls peers {
    nodeId address pull serve interest pullState serveState remote lastError source consumer } } }
```

`pull` means this broker dials the peer and receives its messages (`pullState`
is the `sources[]` state), `serve` means the peer dials this broker and
receives this broker's messages (`serveState` is the `consumers[]` state).
`source` and `consumer` are the matching status entries. The query uses the
normal GraphQL authentication instead of the loopback guard.

### 10.2 Log messages

All PeerLink log lines start with `peerlink:`. Connects and decisions are INFO,
data loss and configuration hints WARN, identity and protocol errors ERROR.
Common messages:

| Message | Meaning / action |
|---|---|
| `consumer connected` / `streaming from source` | Link is up (INFO). |
| `configured consumer never connected; it pins the log` | A peer allowed to pull (`Serve`) has not connected within `Log.NeverConnectedWarnSec`. Until it does, records are kept for it until the log limits evict them. Harmless if the peer just starts later; otherwise check its config and network. Logged once. |
| `records lost before resume (source log overflow)` / `consumer resumes after a gap` | The consumer was away longer than the log holds. Run a resync if retained values matter. |
| `source restarted (new epoch)` | The source crashed or restarted; `resetLostLowerBound` estimates the loss. |
| `handshake refused` | Identity, secret or configuration mismatch; `code` and `reason` name it (ERROR, rate-limited). |
| `clock skew` | Clocks differ by more than 1 s; synchronise with NTP. |
| `peer "<id>" interest LIVE` / `interest DISCONNECTED` | A consumer's interest snapshot was applied, or its link broke (volatile filters dropped). |
| `interest not agreed with consumer` | The consumer has no interest routing (old broker or `Interest: OFF`); it is served everything. |
| `interest exceeds MaxFiltersPerPeer` | The consumer announces too many filters and is served everything until it fits again. Raise `Interest.MaxFiltersPerPeer` or reduce subscriptions. |
| `interest filter of client "<id>" not announced` / `invalid interest entries ... ignored` | A filter is invalid or longer than `Interest.MaxFilterBytes`; that subscription gets no replicas from filtering sources. Logged once per client. |
| `persistent interest of peer "<id>" expired` | A persistent session's filters expired while the peer was away; its backlog that only they needed is reclaimed. |

---

## 11. Limitations

- The `$SYS` counters `messages/received` and `packets/received` include the
  replicas a broker applied.
- A snapshot (`FILL`) after a consumer restart can bring back a retained value
  that was deleted on that consumer while the source was unreachable.
- In a mesh, a snapshot also carries retained values the source itself
  received from other peers (harmless with `FILL`).
- A resync snapshot has no tombstones: values the source deleted are not
  removed on the consumer. After an outage longer than the log, run the
  resync, then clear or republish the remaining topics by hand.
- Retained values from snapshots and archive rows of replicas are dated with
  the source's clock. Synchronise the brokers with NTP; a difference above
  1 s is logged (`clockSkewMs`).
- A network client whose CONNECT username is not valid UTF-8 is forwarded
  without its username (`log.usernameStripped`).

---

## 12. Interoperability with MonsterMQ Edge

MonsterMQ Edge (Go) implements the same protocol and the same `PeerLink`
keys, so Kotlin and Go brokers can be linked in any combination. Differences:

- Edge has an additional per-peer key `Peers[].RedundancyPartner` for WinCC OA
  redundant pairs with a WinCC OA retained store. This broker does not know
  it and fails startup on it, so a config file shared with Edge brokers must
  not contain it.
- Edge filters the WinCC OA namespace and announces a WinCC OA system name
  when embedded in WinCC OA; this broker announces none.
- Default maximum message size: 512 KB here, 1 MiB on Edge. Align them.

---

## 13. Interest routing

Without interest routing a source forwards every captured publish to every
peer, and the consumer drops what nobody subscribed to. With
`Interest.Enabled: true` on both brokers, each broker tells its sources which
topic filters it needs, and a source skips the other records for that peer.

What a broker announces:

| Local interest | Class |
|---|---|
| Network client, clean session / clean start | volatile: dropped when the link breaks |
| Network client, persistent session | persistent: kept by the source for the session expiry interval (MQTT 5) or forever (MQTT 3.1.1) |
| GraphQL subscriptions and other internal bus listeners | volatile, only with `Receive.Bus: true` |
| Outbound bridges (MQTT client) | volatile, only with `Receive.BridgeOutbound: true` |
| Archive groups | persistent, only with `Receive.Archive: true` |

Never announced: PeerLink's own clients, `$` filters (no `$share` support on
this broker) and invalid filters (empty, not UTF-8, longer than
`MaxFilterBytes`, bad wildcards; counted in `interestRejected`).

Behaviour:

- Interest routing is negotiated per link (capability bit). A peer without it
  (an older broker, or `Peers[].Interest: OFF` on either side) is served
  everything, as before.
- After the handshake the consumer sends a full interest snapshot, then
  coalesced changes (every `FlushMs`) ahead of its fetches.
- Retained publishes and retained deletions still go to every peer, so the
  retained stores stay in sync; only non-retained records are filtered.
- When the link breaks, the source drops the peer's volatile filters and the
  backlog only they needed; persistent filters keep their records in the log
  until they expire or the log limits evict them.
- A peer with more than `MaxFiltersPerPeer` filters is served everything until
  a later snapshot fits again.

Cost on the source (gate G-IR1; Apple M-series, two consumers, 1k and 10k
filters, half wildcards): a publish nobody needs is skipped before the record
is built, without allocation. Matching a publish against the filters takes
about 40–50 ns, so capturing a publish every peer needs costs roughly 125 ns
instead of 67 ns. End to end over loopback TCP this does not show: with every
publish needed, the link carries 0.97–1.07 times the publishes per second of a
link without interest routing; with 10 % needed, it is about 10 times faster
and carries a tenth of the bytes. Run the benchmarks with
`mvn -o test -Dtest=InterestBenchTest -Dpeerlink.bench=true`.

Together with interest routing, the default of `Receive.Queue` changed to
`true`: a persistent session announces its filters, so a session that moves to
the other broker keeps receiving replicas while it is offline.

