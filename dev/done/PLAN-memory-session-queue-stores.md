# Plan: Memory Session and Queue Store Modes

## Summary

Add `MEMORY` as a valid option for `SessionStoreType`, and add a new
`QueueStoreType` config option with `MEMORY` support. Both memory modes will
reuse the existing SQLite session/subscription and queue implementations, backed
by dedicated SQLite `:memory:` databases so runtime churn does not hit flash.

Defaults stay unchanged: existing configs continue using the persistent backend
selected by `DefaultStoreType`.

## Key Changes

- Config:
  - Allow `SessionStoreType: MEMORY`.
  - Add `QueueStoreType: MEMORY | SQLITE | POSTGRES | MONGODB`.
  - Add `Config.QueueStore()` fallback helper, defaulting to `DefaultStoreType`.
  - Update `config.yaml.example` for flash-light edge mode:
    - `SessionStoreType: MEMORY`
    - `RetainedStoreType: MEMORY`
    - `QueueStoreType: MEMORY`
    - `Metrics.StoreType: MEMORY`
  - Update `yaml-json-schema.json` accordingly.

- SQLite storage wiring:
  - Keep the file-backed SQLite DB for config/users/archive/device stores.
  - If `SessionStoreType == MEMORY`, open a separate SQLite `:memory:` DB and
    build `Sessions` and `Subscriptions` from it.
  - If `QueueStoreType == MEMORY`, open a separate SQLite `:memory:` DB and
    build `Queue` from it.
  - If either is persistent/default, keep current file-backed behavior.
  - Close all opened SQLite handles via `Storage.Close`.

- SQLite open behavior:
  - Add a memory-safe SQLite open path for `:memory:`/memory DSNs that does not
    force WAL.
  - Keep file-backed SQLite using WAL, busy timeout, foreign keys, and current
    behavior.
  - Prefer `synchronous=FULL` for file-backed SQLite durability if we choose to
    fold in the earlier power-loss recommendation; otherwise keep this as a
    separate follow-up.

- Queue semantics:
  - `QueuedMessagesEnabled: true` still controls whether the queue hook is
    active.
  - `QueueStoreType: MEMORY` means queued messages use our queue logic but are
    lost on broker restart.
  - `QueuedMessagesEnabled: false` still bypasses our queue hook and relies on
    mochi's in-process inflight behavior.
  - `SessionStoreType: MEMORY` does not implicitly change the queue store; queue
    memory is explicit via `QueueStoreType`.

## Test Plan

- Config validation:
  - Accept `SessionStoreType: MEMORY`.
  - Accept `QueueStoreType: MEMORY`.
  - Reject invalid `QueueStoreType`.
  - Preserve existing persistent defaults when `QueueStoreType` is omitted.

- SQLite integration:
  - With `SessionStoreType: MEMORY`, connect/subscribe clients and verify
    sessions/subscriptions work during the process.
  - Restart with same file DB and verify memory sessions/subscriptions are gone.
  - With `QueueStoreType: MEMORY` and `QueuedMessagesEnabled: true`, verify
    offline persistent delivery works inside the same process.
  - Restart with same file DB and verify memory queued messages are gone.
  - With persistent queue default, verify existing queue persistence behavior
    still passes.

- Flash-light config scenario:
  - `SessionStoreType: MEMORY`, `RetainedStoreType: MEMORY`,
    `QueueStoreType: MEMORY`, `Metrics.StoreType: MEMORY`.
  - Verify normal publish/subscribe, retained behavior, metrics, and queue
    behavior without persistent runtime metric/session/queue/retained writes.

## Assumptions

- `MEMORY` for sessions means sessions and subscriptions are both volatile,
  because they share `SessionStore`.
- `MEMORY` for queue means queued messages are volatile even when
  `QueuedMessagesEnabled: true`.
- Config, users, archive config, and device config remain persistent on the
  configured backend.
- No GraphQL schema changes are needed.
- Storage-layout parity is preserved for persistent backends; memory modes are
  runtime-only and do not change persistent table/collection shapes.
