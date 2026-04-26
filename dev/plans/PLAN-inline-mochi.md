# Inline mochi-mqtt-server into broker.go

## Context

We currently depend on `github.com/mochi-mqtt/server/v2 v2.7.9` from the
public module proxy. That release is several commits behind the upstream
`main` branch (e.g. message-expiry improvements, `IsTakenOver` switch to
atomic.Bool, MQTT 5 fixes). A working copy lives at
`/Users/vogler/Workspace/mochi-mqtt-server` — currently at HEAD `5b7f94b`,
~13.4 kLOC of production Go, MIT-licensed.

We want to:

1. Take the upstream code as our own copy so we can apply the latest fixes
   without waiting on a release.
2. Be free to patch the source for our needs (e.g. expose a pre-resend hook
   so the persistent-queue replay can be exact rather than gated on the
   inflight buffer length).
3. Ship a single self-contained binary with no external `mochi-mqtt`
   dependency.

## Scope

**In scope**: copy the production source of mochi-mqtt-server into our tree,
rewrite imports, drop the external module dependency, keep all integration
tests green.

**Out of scope** (separate follow-ups, can be scheduled after this lands):
- Apply local patches (e.g. pre-resend hook, custom listeners).
- Bump versions of mochi's transitive dependencies (`rs/xid`, `gorilla/websocket`,
  `mochi-co/queue`, etc.) — we'll inherit whatever upstream main pins.
- A `scripts/sync-mochi.sh` to re-import future upstream changes.

## Approach

Strategy: **fork the source into `broker.go/internal/mqtt/`** (Strategy C in
the design notes). Reasons:

- We want full ownership — replace directives or `go mod vendor` keep the
  external import path, which discourages local patches and complicates
  the module graph.
- mochi is MIT-licensed; the only obligation is preserving the LICENSE file
  and copyright, which we'll include.
- ~13 kLOC is small enough to maintain. Future upstream syncs are mechanical.
- Go's `internal/` rule actually helps here: nothing outside our module can
  accidentally import this fork's API.

## Layout

```
broker.go/internal/mqtt/                 ← from mochi root
├── LICENSE.md                           ← preserved
├── server.go
├── clients.go
├── hooks.go
├── inflight.go
├── topics.go
├── packets/                             ← from mochi/packets
├── listeners/                           ← from mochi/listeners
├── system/                              ← from mochi/system
├── mempool/                             ← from mochi/mempool
├── hooks/
│   └── auth/                            ← only the auth hook;
│                                          drop hooks/storage/* (we have ours)
└── README-mochi.md                      ← short note: imported from upstream
                                          at HEAD <sha>, MIT, see LICENSE.md
```

Excluded from the import:
- `cmd/`, `examples/`, `config/`, `Dockerfile`, top-level `README*`, `docker/`.
- All `*_test.go` files (mochi's test suite is fine to keep but adds 8 kLOC
  noise; rerun upstream's tests against the fork once on import).
- `hooks/storage/*` (Redis/Pebble/Badger/Bolt) — we use our own SQLite/PG/Mongo
  hooks; carrying these adds dependencies we don't want.

## Step-by-step

1. **Capture the source SHA**
   ```bash
   cd /Users/vogler/Workspace/mochi-mqtt-server
   git rev-parse HEAD > /tmp/mochi-sha   # for the README note
   ```

2. **Copy the source tree**
   ```bash
   DEST=/Users/vogler/Workspace/monster-mq.1/broker.go/internal/mqtt
   mkdir -p $DEST
   cd /Users/vogler/Workspace/mochi-mqtt-server

   # top-level production .go files
   cp server.go clients.go hooks.go inflight.go topics.go LICENSE.md $DEST/

   # subpackages we need
   cp -r packets listeners system mempool $DEST/
   mkdir -p $DEST/hooks
   cp -r hooks/auth $DEST/hooks/
   ```

3. **Rewrite imports** (one sed pass over the copied tree):
   ```bash
   cd $DEST
   find . -name "*.go" -print0 | xargs -0 sed -i '' \
     -e 's|"github.com/mochi-mqtt/server/v2"|"monstermq.io/edge/internal/mqtt"|g' \
     -e 's|"github.com/mochi-mqtt/server/v2/|"monstermq.io/edge/internal/mqtt/|g'
   ```
   Imports of `system`, `packets`, `listeners`, `mempool`, `hooks/auth` all
   resolve under `monstermq.io/edge/internal/mqtt/...`.

4. **Drop the test files** (optional but recommended to keep the diff small):
   ```bash
   find $DEST -name "*_test.go" -delete
   ```

5. **Add the attribution note**
   ```bash
   cat > $DEST/README-mochi.md <<EOF
   This directory is a vendored fork of [mochi-mqtt-server](https://github.com/mochi-mqtt/server)
   (MIT-licensed; see LICENSE.md). Imported from upstream HEAD $(cat /tmp/mochi-sha).

   We took a copy rather than depend on the published module so we can apply
   local patches (see git history of this directory). To re-sync from
   upstream, see scripts/sync-mochi.sh (TODO).
   EOF
   ```

6. **Update our 5 broker files**
   - `broker.go/internal/broker/server.go`
   - `broker.go/internal/broker/hook_auth.go`
   - `broker.go/internal/broker/hook_storage.go`
   - `broker.go/internal/broker/hook_queue.go`
   - `broker.go/internal/broker/tls.go` *(if it imports anything mochi)*

   In each, replace:
   ```
   "github.com/mochi-mqtt/server/v2"          → "monstermq.io/edge/internal/mqtt"
   "github.com/mochi-mqtt/server/v2/hooks/auth" → "monstermq.io/edge/internal/mqtt/hooks/auth"
   "github.com/mochi-mqtt/server/v2/listeners"  → "monstermq.io/edge/internal/mqtt/listeners"
   "github.com/mochi-mqtt/server/v2/packets"    → "monstermq.io/edge/internal/mqtt/packets"
   ```

7. **Detach the external dependency**
   ```bash
   cd /Users/vogler/Workspace/monster-mq.1/broker.go
   go mod edit -droprequire=github.com/mochi-mqtt/server/v2
   go mod tidy
   ```
   `go mod tidy` will pull in any of mochi's transitive deps (rs/xid,
   gorilla/websocket, golang/glog, etc.) that we now import directly. Check
   the resulting `go.mod` diff and confirm the list is reasonable
   (~5–10 modules added).

8. **Build + test**
   ```bash
   go build ./...
   go test ./... -count=1 -timeout 120s
   ./run.sh -b -- -config test/smoke.yaml &
   curl http://localhost:18080/health
   pkill -f bin/monstermq-edge
   ```

9. **Commit in two pieces** (so the diff is reviewable):
   - Commit 1: "Inline mochi-mqtt-server source under internal/mqtt" — pure
     copy + import rewrite, no behavioural change.
   - Commit 2: "Switch broker to inlined mochi" — the 5-file rewrite of our
     own broker package + go.mod cleanup.

## Risks & mitigations

- **Transitive dep churn**: mochi pulls `gorilla/websocket`, `rs/xid`,
  `golang/glog`, `goccy/go-json`. After step 7, these become direct
  requires. Verify the binary still builds with `CGO_ENABLED=0`.
  Mitigation: list `go mod graph | grep mochi` before/after to confirm.

- **License hygiene**: MIT requires preserving copyright notice and
  permission notice. Step 5 keeps `LICENSE.md` next to the code, and the
  `README-mochi.md` records provenance. Add a top-level `NOTICE` if our
  README doesn't already credit mochi-mqtt.

- **Future upstream syncs**: with the source forked, every upstream change
  has to be re-applied manually. Mitigation: write `scripts/sync-mochi.sh`
  in a follow-up that re-runs steps 2–4 with proper conflict reporting.

- **Internal/ visibility**: `internal/mqtt` is only importable by code under
  `monstermq.io/edge/`. Fine for us, but if we ever publish the broker as a
  library, we'd need to move it out of `internal/`.

## Verification (must all pass before commit)

- `go build ./...` clean
- `go vet ./...` clean
- `go test ./... -count=1 -timeout 120s` — all 14 integration tests + the
  SQLite store unit tests still green
- `./run.sh -b -- -config test/smoke.yaml` boots, health probe returns ok,
  GraphQL `{ broker { version enabledFeatures } }` responds
- `mosquitto_pub` / paho client smoke: pub/sub QoS 0/1, retained, persistent
  session offline queue
- Binary size delta: before vs after `ls -la bin/monstermq-edge`
- `go mod graph | grep mochi-mqtt` returns nothing
