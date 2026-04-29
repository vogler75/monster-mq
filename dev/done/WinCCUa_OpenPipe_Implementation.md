# WinCC Unified — OpenPipe Data Access Mode

Implementation reference for the MonsterMQ broker (Kotlin/Vert.X).  
Use this document as a guide when porting to the Go broker.

---

## 1. Feature Overview

Each WinCC Unified client device now has a `dataAccessMode` toggle:

| Value | Transport |
|---|---|
| `GRAPHQL` | Existing HTTP + WebSocket GraphQL path (unchanged) |
| `OPENPIPE` | Local named-pipe IPC to WinCC Unified Runtime |

When `OPENPIPE` is selected the GraphQL fields (endpoint, username, password) are ignored.  
Everything downstream — topic transformation, message formatting, MQTT publish, metrics — is identical between the two modes.

---

## 2. Named Pipe — Platform Details

| OS | Pipe path |
|---|---|
| Windows | `\\.\pipe\HmiRuntime` |
| Linux | `/tmp/HmiRuntime` |

The path can be overridden per-device via the optional `pipePath` config field.

**Access requirements:** the OS user running the broker must be a member of:
- Windows: group `SIMATIC HMI`
- Linux: group `industrial`

The pipe is full-duplex. Reads and writes are independent kernel operations and can happen concurrently.

---

## 3. Protocol — Expert (JSON) Syntax

Always use the **expert syntax** (one-line JSON per message, `\n` terminated). Basic (space-separated) syntax is single-tag only and unsuitable for subscriptions.

### Framing

- One JSON object per line, terminated by `\n` or `\r\n`.
- Responses arrive on the same pipe connection.
- Every request and response carries a `ClientCookie` string used for correlation.

### Key protocol messages

#### BrowseTags (paged)

```json
// Initial request
{"Message":"BrowseTags","Params":{"Filter":"HMI_*","SystemNames":["PV-PC"]},"ClientCookie":"browse-1"}

// Response (one page)
{"Message":"NotifyBrowseTags","Params":{"Tags":[{"Name":"PV-PC::HMI_Tag_1","DisplayName":"...","DataType":3},...]},"ClientCookie":"browse-1"}

// Next page
{"Message":"BrowseTags","Params":"Next","ClientCookie":"browse-1"}

// Browse complete: either an empty Tags array...
{"Message":"NotifyBrowseTags","Params":{"Tags":[]},"ClientCookie":"browse-1"}

// ...or ErrorBrowseTags when the server closes the session after the last partial page:
{"Message":"ErrorBrowseTags","ErrorCode":2165322773,"ErrorDescription":"Your browse request has been expired.","ClientCookie":"browse-1"}
```

**Paging logic:** send `Next` after each non-empty page. The browse is complete when:
- `Tags` array is empty, **or**
- `ErrorBrowseTags` is received for the browse cookie (treat as complete, not an error — use whatever tags were accumulated so far).

#### SubscribeTag — SINGLE mode

One `SubscribeTag` request per tag (N tags = N requests, each with its own cookie):

```json
{"Message":"SubscribeTag","Params":{"Tags":["PV-PC::HMI_Tag_1"]},"ClientCookie":"sub-tag1"}
```

RT fires a notification only when that specific tag changes:

```json
{"Message":"NotifySubscribeTag","Params":{"Tags":[{"Name":"PV-PC::HMI_Tag_1","Value":"42","Quality":"Good","QualityCode":192,"TimeStamp":"2026-04-29T19:59:22Z","ErrorCode":0,"ErrorDescription":"","hasChanged":1}]},"ClientCookie":"sub-tag1"}
```

Publish each tag to `namespace/addressTopic/<transformedTagName>` with the formatted scalar payload.

#### SubscribeTag — BULK mode

One `SubscribeTag` request with all N browsed tags:

```json
{"Message":"SubscribeTag","Params":{"Tags":["PV-PC::HMI_Tag_1","PV-PC::HMI_Tag_2",...]},"ClientCookie":"sub-bulk-1"}
```

RT fires a notification containing **all** subscribed tags on every change (even unchanged ones).  
The `hasChanged` field on each tag entry indicates whether that tag actually changed.

Publish the full `Tags` array JSON to a single MQTT topic `namespace/addressTopic`.

#### SubscribeAlarm

```json
{"Message":"SubscribeAlarm","Params":{"SystemNames":["PV-PC"],"Filter":"...","LanguageId":"en-US"},"ClientCookie":"sub-alarms-1"}
```

First `NotifySubscribeAlarm` returns all currently active alarms; subsequent notifications fire on state changes only.

```json
{"Message":"NotifySubscribeAlarm","ClientCookie":"sub-alarms-1","params":{"Alarms":[{...}]}}
```

Note: the `params` key uses lowercase in some RT builds — handle both.

#### ReadConfig (probe / page-size)

```json
{"Message":"ReadConfig","Params":["DefaultPageSize"],"ClientCookie":"probe-1"}
{"Message":"NotifyReadConfig","Params":{"DefaultPageSize":"1000"},"ClientCookie":"probe-1"}
```

Default page size is 1000. Use this as a connectivity probe before issuing browse/subscribe commands.

---

## 4. Configuration Data Model

### Connection config (`WinCCUaConnectionConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataAccessMode` | string | `"GRAPHQL"` | `"GRAPHQL"` or `"OPENPIPE"` |
| `pipePath` | string? | null | OS default if null |
| `graphqlEndpoint` | string | — | Optional when OPENPIPE |
| `username` | string | — | Optional when OPENPIPE |
| `password` | string | — | Optional when OPENPIPE |

OS default pipe path: `\\.\pipe\HmiRuntime` on Windows, `/tmp/HmiRuntime` on Linux.

### Address config (`WinCCUaAddress`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | enum | — | `TAG_VALUES` or `ACTIVE_ALARMS` |
| `topic` | string | — | MQTT topic prefix |
| `nameFilters` | string[] | — | Tag browse filters, e.g. `["HMI_*"]` |
| `pipeTagMode` | string | `"SINGLE"` | `"SINGLE"` or `"BULK"` (OpenPipe TAG_VALUES only) |
| `systemNames` | string[] | — | For ACTIVE_ALARMS |
| `filterString` | string? | — | For ACTIVE_ALARMS |
| `retained` | bool | false | MQTT retained flag |
| `includeQuality` | bool | false | Include quality in payload |

---

## 5. Connector Lifecycle

```
start()
  └─ openPipe()              // open pipe handle; on success:
       └─ startAsyncRead()   // post first async read (see §6)
       └─ setupSubscriptions()
            ├─ for each TAG_VALUES address:
            │    └─ executeBrowse()        // BrowseTags + paging
            │         └─ subscribeToTags() // SINGLE: N SubscribeTag; BULK: 1 SubscribeTag
            └─ for each ACTIVE_ALARMS address:
                 └─ SubscribeAlarm

// On read/write error or EOF:
onConnectionLost()
  └─ teardownState()         // clear pending browses, subscriptions
  └─ closePipeHandle()
  └─ scheduleReconnection()  // timer → openPipe() → setupSubscriptions()

stop()
  └─ teardownState()
  └─ closePipeHandle()
```

---

## 6. I/O Architecture — Critical for Windows Named Pipes

### The deadlock problem (do NOT use synchronous I/O)

On Windows, named pipes opened with synchronous (blocking) I/O **deadlock** when a read and a write are attempted concurrently on the same handle from two threads. The blocking `ReadFile` holds the handle; the concurrent `WriteFile` parks waiting for it. Neither returns.

This affects Java's `RandomAccessFile` and any Go approach using `os.Open` / `os.File.Read` + `os.File.Write` from separate goroutines on the same handle.

**Node.js `net.createConnection`** works because it uses overlapped (async) I/O internally.

### The fix: overlapped (async) I/O

In the JVM implementation we use `java.nio.channels.AsynchronousFileChannel` which opens the pipe with `FILE_FLAG_OVERLAPPED`. Reads and writes are posted as independent async operations; both can be in-flight simultaneously.

```kotlin
val channel = AsynchronousFileChannel.open(
    Path.of("\\\\.\\pipe\\HmiRuntime"),
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
)
// Position is always 0 for pipes (kernel ignores it in the OVERLAPPED struct)
channel.read(buf, 0L, buf, readCompletionHandler)
channel.write(buf, 0L, null, writeCompletionHandler)
```

### Go equivalent

In Go on Windows, use the **`github.com/microsoft/go-winio`** package which wraps `CreateFile` with `FILE_FLAG_OVERLAPPED` and exposes a `net.Conn`-like interface:

```go
conn, err := winio.DialPipe(`\\.\pipe\HmiRuntime`, nil)
// conn implements net.Conn — use bufio.Scanner for line reads,
// separate goroutines for read and write, no deadlock.
```

On Linux the pipe is a FIFO at `/tmp/HmiRuntime`. Open it with `os.OpenFile(path, os.O_RDWR, 0)` — Linux does not have the Windows overlapped I/O constraint; goroutines do not deadlock on concurrent FIFO reads/writes.

### Read loop (line framing)

Async reads return arbitrary byte chunks. Accumulate into a line buffer and split on `\n`:

```kotlin
private val lineAccumulator = StringBuilder()

private fun processReadBytes(str: String) {
    for (ch in str) {
        if (ch == '\n') {
            val line = lineAccumulator.toString().trimEnd('\r')
            lineAccumulator.clear()
            if (line.isNotEmpty()) handlePipeMessage(line)
        } else {
            lineAccumulator.append(ch)
        }
    }
}
```

In Go this is just `bufio.Scanner` with `ScanLines`.

---

## 7. Browse Paging State Machine

Each in-flight browse is tracked by cookie:

```
state {
    address    WinCCUaAddress
    promise    Promise<List<String>>
    accumulated []string
}

on NotifyBrowseTags(cookie, tags):
    if len(tags) == 0:
        complete(accumulated)          // empty page = done
    else:
        accumulated += tags
        send BrowseTags Next(cookie)

on ErrorBrowseTags(cookie):
    complete(accumulated)              // last partial page already sent; treat as done

on timeout / pipe close:
    fail(promise)
```

---

## 8. Subscription Modes

### SINGLE (pipeTagMode = "SINGLE")

- After browse: send N `SubscribeTag` requests, one per tag name, each with its own unique `ClientCookie`.
- Store `cookie → address` in a map.
- On `NotifySubscribeTag`: the notification contains one tag. Look up address via cookie. Publish to:
  ```
  namespace/addressTopic/<transformedTagName>
  ```
  with the formatted scalar payload (value + timestamp + optional quality).

### BULK (pipeTagMode = "BULK")

- After browse: send one `SubscribeTag` with all tag names, one `ClientCookie`.
- Store `cookie → address`.
- On `NotifySubscribeTag`: publish the entire `Params.Tags` JSON array to:
  ```
  namespace/addressTopic
  ```
  as the raw payload. Consumer receives all tag states in one message.

---

## 9. Concurrent Write Serialization

In the JVM implementation writes go through `AsynchronousFileChannel.write()` which handles overlapped serialization at the OS level. Multiple writes can be posted simultaneously (e.g. during startup when N subscriptions are set up in parallel) — the kernel queues them.

In Go with `go-winio` (which uses overlapped I/O internally), writing from multiple goroutines to the same `net.Conn` concurrently is not safe without a mutex. Use a single writer goroutine fed by a channel:

```go
writeCh := make(chan []byte, 64)

// writer goroutine
go func() {
    w := bufio.NewWriter(conn)
    for msg := range writeCh {
        w.Write(msg)
        w.WriteByte('\n')
        w.Flush()
    }
}()

// send from anywhere
writeCh <- jsonBytes
```

---

## 10. Error Handling & Reconnection

- **Pipe open failure** (pipe not found, wrong group): log, schedule reconnect timer.
- **Read EOF / IOException**: `onConnectionLost` → clear state → close handle → schedule reconnect.
- **Write failure**: `onConnectionLost` (same path).
- **Reconnect**: after delay (default 5 s), re-open pipe and re-run full `setupSubscriptions` (browse + subscribe everything from scratch — the server has no session state from the previous connection).
- **`ErrorBrowseTags`** with accumulated tags: treat as browse complete (not an error).
- **Per-tag `ErrorCode != 0`** in `NotifySubscribeTag`: log warning, skip that tag, continue.

---

## 11. Files Changed (Kotlin broker)

| File | Change |
|---|---|
| `stores/devices/WinCCUaConfig.kt` | Added `dataAccessMode`, `pipePath` to connection config; added `pipeTagMode` to address config; `resolvePipePath()` helper |
| `devices/winccua/WinCCUaExtension.kt` | Deploy `WinCCUaPipeConnector` when `dataAccessMode == OPENPIPE` |
| `devices/winccua/WinCCUaPipeConnector.kt` | New — full OpenPipe connector using `AsynchronousFileChannel` |
| `devices/winccua/WinCCUaPublisher.kt` | New — shared publish helpers extracted from `WinCCUaConnector` |
| `devices/winccua/WinCCUaConnector.kt` | Updated to call `WinCCUaPublisher` instead of inline helpers |
| `graphql/WinCCUaClientConfigMutations.kt` | Round-trip `dataAccessMode`, `pipePath`, `pipeTagMode` |
| `graphql/WinCCUaClientConfigQueries.kt` | Round-trip same fields |
| `resources/schema-mutations.graphqls` | Added fields to `WinCCUaConnectionConfigInput` and `WinCCUaAddressInput` |
| `resources/schema-queries.graphqls` | Added fields to `WinCCUaConnectionConfig` and `WinCCUaAddress` types |
| `dashboard/src/pages/winccua-client-detail.html` | Data Access Mode select, Pipe Path field, OpenPipe Tag Publish Mode select |
| `dashboard/src/js/winccua-client-detail.js` | Mode toggle visibility, load/save new fields |
| `dashboard/src/js/winccua-clients.js` | Fetch and display `dataAccessMode` in client list |

## 12. Test Tools

| File | Purpose |
|---|---|
| `dev/OpenPipeTest.js` | Node.js standalone test: probe → BrowseTags → SubscribeTag → live notifications |
| `dev/OpenPipeTest.java` | Java standalone test (kept for reference; use the Node.js version — Java RandomAccessFile deadlocks) |
| `dev/run-openpipe-test.bat` | Batch wrapper to locate a JDK and run the Java test |
| `dev/WinCC_Unified_OpenPipe_Reference.md` | Full protocol reference |
