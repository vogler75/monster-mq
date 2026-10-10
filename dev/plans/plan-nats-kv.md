# NATS KV on top of archive-group last-value stores (draft)

Goal: `nats kv get/put/del/watch/keys/ls/info` against MonsterMQ's native NATS port,
with **bucket = archive group name** and the group's `lastValStore` as the backing store.
Status: design sketch only, nothing implemented. Based on reading `NatsClient.kt`,
`handlers/ArchiveGroup.kt`, `stores/IMessageStore.kt`, `data/BrokerMessage.kt`.

## Mapping

| KV concept | MonsterMQ |
|---|---|
| bucket `B` (stream `KV_B`, subjects `$KV.B.>`) | archive group `B` (must be deployed and have a lastVal store != NONE) |
| key `a.b.c` | MQTT topic `a/b/c` (same `.`↔`/` rule the NATS port already uses) |
| key filter `a.*` / `a.>` | `a/+` / `a/#` → `lastValStore.findMatchingMessages()` |
| value | `BrokerMessage.payload` |
| revision / sequence | synthesized from `BrokerMessage.time` (epoch µs). Monotonic per key, not a real stream sequence |
| created timestamp | `BrokerMessage.time` |
| history | always 1 (last value only). Later option: `nats kv history` backed by the group's `archiveStore` |
| put | publish a normal MQTT message on the topic; the archive group stores it like any other publish. MQTT/NATS subscribers see it too |
| delete / purge | `lastValStore.delAll([topic])` (+ optionally publish an empty retained message to clear retained) |
| create / delete bucket | rejected; groups are managed via config/GraphQL/dashboard |

Put on a key that doesn't match the group's `topicFilter` → JetStream error "no stream matches subject".
For `retainedOnly` groups, puts must be published with retain=true (or always retain KV puts; decision needed).

## What the NATS CLI actually sends (nats.go)

1. `kv get B k`: `$JS.API.STREAM.INFO.KV_B` (bind), then either
   `$JS.API.DIRECT.GET.KV_B.$KV.B.k` (if stream config has `allow_direct: true`, answer is a headers message)
   or `$JS.API.STREAM.MSG.GET.KV_B` with `{"last_by_subj":"$KV.B.k"}` (answer is JSON, base64 data).
   Advertising `allow_direct: false` keeps get on plain JSON.
2. `kv put B k v`: publish to `$KV.B.k` with a reply inbox, expects PubAck `{"stream":"KV_B","seq":N}`.
3. `kv del` / `kv purge`: publish to `$KV.B.k` with header `KV-Operation: DEL|PURGE` (needs HPUB).
4. `kv watch` / `kv keys` / `kv ls -v`: ordered push consumer via `$JS.API.CONSUMER.CREATE.KV_B...`
   (`deliver_policy: last_per_subject`, `headers_only` for keys, `idle_heartbeat`). Server then pushes
   messages to the deliver inbox with reply subject `$JS.ACK.KV_B.<consumer>.1.<sseq>.<dseq>.<ts>.<pending>`;
   the client uses `pending` to know the initial snapshot is done. Needs idle heartbeats
   (`NATS/1.0 100 Idle Heartbeat` header messages) or the client recreates the consumer.
5. `kv ls`: `$JS.API.STREAM.NAMES` / `STREAM.LIST` with subject filter `$KV.*.>` → list archive groups.
6. `kv info B`: `$JS.API.STREAM.INFO.KV_B`. Also `$JS.API.INFO` (account info) is probed by some commands.

## Work items

**Protocol prerequisites in `NatsClient.kt`** (useful on their own):
- Advertise `"headers": true` (and `"jetstream": true`) in INFO; parse `HPUB`, emit `HMSG`; no-responders (`503`) status.
- Honor reply-to: today it's parsed and dropped. Map NATS reply-to ↔ MQTT5 `responseTopic`, headers ↔ `userProperties`
  (both already exist on `BrokerMessage`). That also fixes plain core request/reply between NATS/MQTT clients.

**JetStream KV facade** (new class, e.g. `nats/NatsJetStreamKv.kt`, called from `NatsClient` before normal routing):
- Intercept `$JS.API.*` and `$KV.*` publishes per connection; answer directly on that client's socket to its reply inbox
  (no need to route through the MQTT tree).
- Stream INFO/NAMES/LIST/MSG.GET, PubAck for puts, DEL/PURGE, CONSUMER.CREATE/DELETE for ordered consumers:
  snapshot from `findMatchingMessages`, then live updates from an internal subscription on the filter, plus heartbeats.
- ACL: check `canSubscribe` for get/watch and `canPublish` for put/del on the translated MQTT topic.
- Everything else under `$JS.API.` → JetStream error `{"error":{"code":503,"err_code":10039,"description":"jetstream feature not supported"}}`.

**Docs/tests**: extend `doc/nats.md`; integration test with jnats (already a dependency for the bridge) doing put/get/del/watch/keys/ls.

## Open questions
1. Keys: `.`→`/` hierarchy (proposed) vs. raw key as one topic level?
2. KV put: always retained, or retained only for `retainedOnly` groups?
3. Should `del` also clear the retained message, or only the group's last-value entry?
4. Edge broker: same feature there too, or main broker only for now?
