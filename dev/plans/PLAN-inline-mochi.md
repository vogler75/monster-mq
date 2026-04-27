# broker.go — switch to our own mochi-mqtt fork

## Context

`broker.go/` currently depends on `github.com/mochi-mqtt/server/v2 v2.7.9`
from the public Go module proxy. That release is several commits behind
upstream `main` (notably the message-expiry fix and the `IsTakenOver`
atomic.Bool change), and we want the freedom to apply local patches —
e.g. a pre-resend hook so our persistent-queue replay can be exact
rather than gated on the inflight-buffer length.

We have a fork at <https://github.com/vogler75/mochi-mqtt-server>. This
plan switches the broker to consume that fork instead of the upstream
release.

## Goals

1. Build broker.go against our fork rather than upstream
   `mochi-mqtt/server v2.7.9`.
2. Keep the option to apply local patches in the fork without breaking
   anyone else's builds.
3. Reproducible: anyone cloning broker.go can build with no extra setup
   (no required `replace` directive pointing at a local checkout).
4. Easy to keep in sync with upstream when we want to.

## Strategy comparison

> **Aside on `/v2`** — Go's "Semantic Import Versioning" rule says any
> module at `v2.0.0` or higher must have `/v2` (or `/v3`, …) **as a
> suffix in its module identifier inside `go.mod`**. It is **not** a
> branch or a directory in the repo. The fork's GitHub URL is plain
> `github.com/vogler75/mochi-mqtt-server` (no `/v2` anywhere); the
> `/v2` only appears in `import` statements because upstream's `go.mod`
> declares `module github.com/mochi-mqtt/server/v2`. Tags are still
> ordinary `vX.Y.Z`.

| Approach | Pro | Con |
|---|---|---|
| **A. `replace` to our fork's GitHub URL** — leave fork's `go.mod` untouched (it still declares `module github.com/mochi-mqtt/server/v2`), and add `replace github.com/mochi-mqtt/server/v2 => github.com/vogler75/mochi-mqtt-server vX.Y.Z` to broker.go's `go.mod`. | No source changes anywhere; fork stays a clean mirror of upstream so cherry-picks are noise-free. | Module identity stays `mochi-mqtt/server/v2` — `go list -m all` shows the replace explicitly, but linters/IDEs may still display the upstream path. |
| **B. Rename module to `github.com/vogler75/mochi-mqtt-server/v2`** — one-line edit of the fork's `go.mod` (the `/v2` here is the SIV suffix; still no `/v2` anywhere in the repo URL), sed-rewrite `github.com/mochi-mqtt/server/v2` → `github.com/vogler75/mochi-mqtt-server/v2` in the fork's own source files, update broker.go's 5 import lines. | Imports clearly say "this is ours". No `replace` directive. | Every upstream cherry-pick needs the same sed pass against incoming files; merges are noisier. |
| **C. Vendor the fork into `broker.go/internal/mqtt/`** | Single self-contained checkout; `internal/` enforces that nothing outside our module imports it. | ~13 kLOC carried in our tree; future upstream syncs are manual diff-and-apply. |

**Pick A.** It's reversible (drop the replace and we're back on upstream),
keeps the fork itself easy to re-base on upstream, and matches how other
mature Go projects manage forks (e.g. Caddy's xcaddy, Argo's k8s
forks).

## Steps

### 1. Prepare the fork

In the fork repo at <https://github.com/vogler75/mochi-mqtt-server>:

1. Confirm the fork's `go.mod` still reads
   `module github.com/mochi-mqtt/server/v2` (it does today; we keep it
   that way for Strategy A).
2. Ensure `main` includes everything we want from upstream (currently
   `5b7f94b "Update server version"` past the released v2.7.9, plus
   the message-expiry and `IsTakenOver` improvements).
3. Tag a release that we can pin to:
   ```bash
   git -C /Users/vogler/Workspace/mochi-mqtt-server tag v2.7.10-monstermq.1
   git -C /Users/vogler/Workspace/mochi-mqtt-server push origin v2.7.10-monstermq.1
   ```
   The `-monstermq.N` suffix makes it obvious this is a local build and
   sorts after upstream's `v2.7.10` if/when upstream publishes one.

### 2. Switch broker.go to consume the fork

In `broker.go/`:

```bash
cd broker.go

# Add the replace pointing at our fork. Note: NO /v2 in the fork URL on
# the right-hand side — the fork's go.mod still declares
# `module github.com/mochi-mqtt/server/v2`, and Go uses that module
# identity, not the URL path.
go mod edit -replace github.com/mochi-mqtt/server/v2=github.com/vogler75/mochi-mqtt-server@v2.7.10-monstermq.1
go mod tidy
```

After `go mod tidy`, expect `go.mod` to contain:

```
require github.com/mochi-mqtt/server/v2 v2.7.9   // pinned upstream version (kept for the require slot)

replace github.com/mochi-mqtt/server/v2 => github.com/vogler75/mochi-mqtt-server v2.7.10-monstermq.1
```

`go.sum` will pin the fork commit's content hash, making the build
reproducible.

### 3. Verify

```bash
go mod download
go build ./...                          # uses the fork
go test ./... -count=1 -timeout 90s     # all 18 integration tests still pass
./run.sh -b -- -config test/smoke.yaml  # smoke
```

Then sanity-check that the fork is in fact what's being compiled:

```bash
go list -m all | grep mochi
# expect:
#   github.com/mochi-mqtt/server/v2 v2.7.9 => github.com/vogler75/mochi-mqtt-server v2.7.10-monstermq.1
```

### 4. Commit

```
broker.go: switch to vogler75/mochi-mqtt-server fork

Adds a replace directive in go.mod so we consume our own fork instead
of upstream mochi-mqtt v2.7.9. Pinned to v2.7.10-monstermq.1, which
includes upstream's message-expiry and IsTakenOver fixes that aren't in
the released v2.7.9 yet, plus headroom to apply our own patches without
waiting for an upstream release.

Build and all 18 integration tests green against the fork.
```

## Maintaining the fork

### Pulling in upstream changes

```bash
cd /Users/vogler/Workspace/mochi-mqtt-server
git fetch upstream                     # if upstream isn't already a remote: git remote add upstream https://github.com/mochi-mqtt/server
git checkout main
git merge upstream/main
git tag v2.7.11-monstermq.1
git push origin main v2.7.11-monstermq.1
```

Then in `broker.go/`:

```bash
go mod edit -replace github.com/mochi-mqtt/server/v2=github.com/vogler75/mochi-mqtt-server@v2.7.11-monstermq.1
go mod tidy
go test ./... -count=1
```

### Applying our own patches

For changes we want only in our fork (e.g. a new pre-resend hook):

1. Branch in the fork: `git checkout -b feature/pre-resend-hook`
2. Edit, commit, push to the fork.
3. Merge into `main` of the fork once tested.
4. Cut a new `vX.Y.Z-monstermq.N+1` tag.
5. `go get` the new tag in broker.go.

When upstream merges our patch (or we don't need it any more), drop the
local commit on the next sync and tag a fresh `-monstermq.N`.

## Risks / open questions

- **Module path collision**: Strategy A relies on `replace` rewriting the
  resolved module. Some tooling (linters, IDEs) may still display the
  upstream import path. This is cosmetic.
- **Fork drift**: with our own tag we're now responsible for keeping the
  fork on a buildable commit. CI on the fork repo (just `go build ./...`
  + `go test ./...`) is recommended.
- **Open-source distribution**: if broker.go is ever released publicly,
  the `replace` directive flows through to consumers. A re-pinned
  upstream version (drop the replace) is the cleanest "we want to be
  back on upstream" knob.

## Verification checklist

- [ ] Fork tagged `v2.7.10-monstermq.1` and pushed to GitHub
- [ ] `go.mod` contains the replace and `go mod tidy` runs clean
- [ ] `go list -m all | grep mochi` confirms the fork is in use
- [ ] `go build ./...` clean
- [ ] `go test ./... -count=1 -timeout 90s` green
- [ ] `./run.sh -b -- -config test/smoke.yaml` boots and serves MQTT + GraphQL
- [ ] Smoke a publish/subscribe round-trip with `mosquitto_pub`/`paho`
