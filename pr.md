# Fix: SQLite retained messages not delivered on subscribe

## Problem

SQLite retained value store correctly persists messages to the database but never delivers them to clients on subscribe (both MQTT v3.1.1 and v5.0). This is a regression introduced by commit `b0038d1` ("Refactor SQLite stores to use SQLiteVerticle and event bus").

## Root Cause

Two bugs were found:

### Bug 1: Async/sync mismatch in `findMatchingMessages` and `findMatchingTopics`

After the SQLiteVerticle refactor, these methods use async event bus calls (`sqlClient.executeQuery(sql, params).onComplete { ... }`), but the caller (`MessageHandler.findRetainedMessages`) wraps the call in `vertx.executeBlocking(Callable { ... })` which expects synchronous behavior. The async callback fires *after* the Callable returns, so the counter stays at 0 and no retained messages are delivered.

Using `executeQuerySync` (CompletableFuture.get) was not viable either — it deadlocks because the event bus reply cannot be dispatched while `vertx.executeBlocking` holds the worker thread.

### Bug 2: SQLite JDBC driver type metadata unreliability

The SQLite JDBC driver reports BLOB columns as VARCHAR (type 12) and returns `0` for NULL integer columns via `getInt()`. This caused:
- `payload_blob` to be read as a String instead of byte array
- `message_expiry_interval = NULL` (no expiry) to be read as `0` (expire immediately), silently skipping all retained messages in the expiry check

## Fix

### `SQLiteClient.kt`
- Added `executeQueryDirect()` method — a `@Synchronized` direct read-only JDBC connection that bypasses the event bus entirely
- The read connection uses `PRAGMA query_only = ON` (prevents accidental writes) and `PRAGMA busy_timeout = 5000` (WAL mode supports concurrent readers)
- Path is resolved to absolute at construction time via `File(dbPath).absolutePath` to avoid relative path resolution issues
- Uses `rs.getObject(i)` with Java runtime type dispatch instead of `getColumnType()` — avoids SQLite JDBC type metadata bugs (BLOB-as-VARCHAR, NULL-as-zero)

### `MessageStoreSQLite.kt`
- Changed `findMatchingMessages` from async `executeQuery().onComplete` to synchronous `executeQueryDirect`
- Changed `findMatchingTopics` (2 code paths: exact match and wildcard) from async to synchronous `executeQueryDirect`
- No changes to write paths — all writes remain serialized through the SQLiteVerticle event bus

## Testing

- 6 new regression tests (3 × MQTT v3.1.1 + 3 × MQTT v5.0):
  - Exact topic match retained delivery
  - Single-level wildcard (`+`) retained delivery
  - Multi-level wildcard (`#`) retained delivery
- Full regression suite: **60 passed, 1 skipped** (will delay — known limitation), 0 failures

## Files Changed

| File | Change |
|------|--------|
| `broker/src/main/kotlin/stores/dbs/sqlite/SQLiteClient.kt` | Added `executeQueryDirect()` with direct read-only JDBC connection |
| `broker/src/main/kotlin/stores/dbs/sqlite/MessageStoreSQLite.kt` | Switched `findMatchingMessages` and `findMatchingTopics` to `executeQueryDirect` |
| `tests/mqtt3/test_retained_on_subscribe.py` | New: 3 MQTT v3.1.1 retained delivery regression tests |
| `tests/mqtt5/test_mqtt5_retained_on_subscribe.py` | New: 3 MQTT v5.0 retained delivery regression tests |

## Notes

- `findTopicsByName` and `findTopicsByConfig` in `MessageStoreSQLite` still use `executeQuerySync` — these are called from GraphQL resolvers (not from `vertx.executeBlocking`) so they don't deadlock, but could be migrated to `executeQueryDirect` in a follow-up if needed.
- The `SQLiteVerticle.handleExecuteQuery` has the same type metadata issue (BLOB-as-VARCHAR, NULL-as-zero) but it only affects the async path which was already broken. A follow-up could align its type handling with `executeQueryDirect`.
