# Plan: MonsterMQ HMI MQTT File Synchronization (`mmq hmi sync`)

## 1. Executive Summary

This plan defines the architecture, cross-platform CLI implementation, and wire-level MQTT protocol for synchronizing Human-Machine Interface (HMI) dashboard assets between a local developer workstation and a remote MonsterMQ broker (both **MonsterMQ Edge** in Go and **MonsterMQ Full Broker** in Kotlin).

### The Operational Challenge
- Edge brokers run on constrained devices (Raspberry Pi 4/5) and industrial panels (Siemens WinCC Unified Comfort Panels).
- These environments frequently lack shell access, SSH, FTP, or direct filesystem access due to OT firewall segmentation and hardened panel OS policies.
- AI coding assistants (such as Antigravity) and engineers require local project files to inspect, lint, edit, test, and commit using version control.
- In OT networks, the broker's MQTT listener (ports 1883/8883/1884/8884) is typically the **only** open, authenticated channel.

### The Solution
Instead of an on-demand HTTP proxy (which breaks panel autonomy when the developer laptop disconnects) or an SSH-over-MQTT tunnel (which requires an SSH server and risks broad filesystem access), we implement a **Native MQTT File Synchronization Engine**:
1. **Client Tool (`mmq hmi sync`)**: Integrated into the official cross-platform CLI tool `tools/cli` (`mmq`). Native binaries for **Windows, macOS, and Linux**.
2. **Session Isolation via UUID**: All sync traffic is namespaced under a unique session UUID (`monstermq/hmi/sync/<session-uuid>/...`), fully isolating multiple simultaneous developer sessions.
3. **Directional Channel Architecture (`upstream` / `downstream`)**: Clean 2-topic bidirectional communication model. The client publishes commands upstream and receives responses downstream.
4. **Initial Pull**: Clones existing remote HMI screens down to the local machine via a single-roundtrip MQTT zip export or incremental reconciliation.
5. **Live File Watcher**: Uses `fsnotify` to detect local edits, debounces them, and transmits incremental file updates over MQTT in milliseconds.
6. **Broker-Side Sync Service**: Runs inside the broker, validates paths against traversal attacks (`../`), writes files into `data/hmi/<dashboard>/`, and allows the embedded HTTP server to serve files at local hardware speed 24/7.
7. **Dual-Broker Parity**: Designed with an identical topic and JSON schema contract so both `monster-mq-edge` (Go) and `monster-mq` (Kotlin) implement the exact same server interface.

---

## 2. Multi-Session Topic Architecture: `upstream` and `downstream`

To prevent crosstalk between multiple concurrent developer connections and eliminate topic subscription clutter, the sync protocol uses a **Session UUID** and a **Directional 2-Topic Channel Model**:

```
monstermq/hmi/sync/<session-uuid>/upstream    # Client -> Broker (Commands & Writes)
monstermq/hmi/sync/<session-uuid>/downstream  # Broker -> Client (Responses & Events)
```

```
+-----------------------------------------------------------------------------+
| Client Session A (UUID: a1b2c3d4-...)                                       |
|   Subscribes: monstermq/hmi/sync/a1b2c3d4-.../downstream                    |
|   Publishes:  monstermq/hmi/sync/a1b2c3d4-.../upstream                      |
+------------------------------------+----------------------------------------+
                                     |
                                     | MQTT
                                     v
+-----------------------------------------------------------------------------+
| Broker SyncService                                                          |
|   Subscribes once: monstermq/hmi/sync/+/upstream                            |
|   Parses session UUID from topic, processes request, and replies to:        |
|   monstermq/hmi/sync/<session-uuid>/downstream                              |
+------------------------------------+----------------------------------------+
                                     ^
                                     | MQTT
+------------------------------------+----------------------------------------+
| Client Session B (UUID: e5f6g7h8-...)                                       |
|   Subscribes: monstermq/hmi/sync/e5f6g7h8-.../downstream                    |
|   Publishes:  monstermq/hmi/sync/e5f6g7h8-.../upstream                      |
+-----------------------------------------------------------------------------+
```

### Advantages of the 2-Topic Upstream/Downstream Model
1. **Zero Crosstalk / Strict Isolation**: Each developer session possesses its own independent downstream reply channel. Two engineers syncing different dashboards or files never see each other's traffic.
2. **Minimal Subscriptions**:
   - The client subscribes to exactly **one** topic: `<prefix>/<session-uuid>/downstream`.
   - The broker subscribes to exactly **one** wildcard topic: `<prefix>/+/upstream`.
3. **Correlation & Async Safety**: Every upstream message carries a `reqId`, and every downstream message echoes the matching `reqId`, allowing concurrent, out-of-order pipelining without confusion.

---

## 3. Wire-Level Protocol & JSON Message Envelope

All upstream and downstream messages are JSON envelopes.

### 3.1 Upstream Envelope (Client -> Broker)
```json
{
  "action": "<ping | list | export | read | write | delete | import>",
  "reqId": "<unique-request-id>",
  "dashboard": "<dashboard-name>",
  "...payload fields specific to action..."
}
```

### 3.2 Downstream Envelope (Broker -> Client)
```json
{
  "action": "<echoed-action>",
  "reqId": "<echoed-request-id>",
  "success": true,
  "error": "<error-message-if-failed>",
  "...response fields specific to action..."
}
```

---

## 4. How Initial Download Sync Works

When a developer starts `mmq hmi sync main ./my-hmi --pull`, the initial sync can operate in two modes:

### Mode A: Fast Atomic Bulk Export (Single Roundtrip via Zip) — Recommended Default
Ideal for initial workspace setup or when `--pull` is passed:
1. Client generates session UUID (`sess-9876`) and subscribes to `monstermq/hmi/sync/sess-9876/downstream`.
2. Client sends upstream:
   ```json
   {
     "action": "export",
     "reqId": "1",
     "dashboard": "main"
   }
   ```
3. Broker's `SyncService` receives the request and calls `hmi.Manager.ExportDashboardZip("main")`.
4. Broker packages all files in `data/hmi/main/` into a zip in-memory and returns it downstream:
   ```json
   {
     "action": "export",
     "reqId": "1",
     "success": true,
     "dashboard": "main",
     "zipBase64": "UEsDBBQAAAAIA...",
     "fileCount": 5,
     "sizeBytes": 24500
   }
   ```
5. Client unzips the payload directly into `./my-hmi/`.
6. **Result**: Full initial sync finishes in **< 100 milliseconds** in a single network roundtrip.
7. Client transitions immediately to the file watch loop.

### Mode B: Incremental File Reconciliation (List & Diff)
Used when local files already exist and the developer wants to avoid overwriting newer local files or re-downloading identical files:
1. Client requests file list:
   ```json
   {
     "action": "list",
     "reqId": "2",
     "dashboard": "main"
   }
   ```
2. Broker returns file list with sizes and SHA-256 hashes:
   ```json
   {
     "action": "list",
     "reqId": "2",
     "success": true,
     "dashboard": "main",
     "files": [
       { "path": "index.html", "sizeBytes": 2048, "sha256": "3a7b...", "modTime": 1740000000 },
       { "path": "app.js", "sizeBytes": 4510, "sha256": "8f1c...", "modTime": 1740000000 }
     ]
   }
   ```
3. Client compares local file SHA-256 hashes against remote hashes:
   - Identical hashes: skipped.
   - Missing or outdated local files: requested individually via `action: "read"`:
     ```json
     {
       "action": "read",
       "reqId": "3",
       "dashboard": "main",
       "path": "app.js"
     }
     ```
     Broker replies downstream:
     ```json
     {
       "action": "read",
       "reqId": "3",
       "success": true,
       "path": "app.js",
       "contentBase64": "dmFyIGFwcCA9IC4uLg==",
       "sha256": "8f1c..."
     }
     ```

---

## 5. Live Incremental Syncing (Write & Delete)

Once the initial sync is complete, `mmq` watches the local directory using `fsnotify`.

### 5.1 Local File Write / Modification (`write`)
When an AI assistant (Antigravity) or engineer saves a file:
1. `mmq` debounces for 100ms (to batch rapid IDE saves).
2. Computes SHA-256 hash of local file.
3. Sends upstream:
   ```json
   {
     "action": "write",
     "reqId": "4",
     "dashboard": "main",
     "path": "app.js",
     "contentBase64": "dmFyIG5ld1ZhbHVlID0gNDI7...",
     "sha256": "8f1c..."
   }
   ```
4. Broker validates path containment, verifies SHA-256, and atomically writes the file to disk.
5. Broker responds downstream:
   ```json
   {
     "action": "write",
     "reqId": "4",
     "success": true,
     "path": "app.js",
     "bytesWritten": 4510
   }
   ```
6. Client terminal displays:
   `[10:35:12] [SYNC] Updated app.js (4.5 KB) -> OK (18ms)`

### 5.2 Local File Deletion (`delete`)
When a local file is deleted:
1. Client sends upstream:
   ```json
   {
     "action": "delete",
     "reqId": "5",
     "dashboard": "main",
     "path": "unused.js"
   }
   ```
2. Broker deletes the file via `hmi.Manager.DeleteDashboardFile("main", "unused.js")`.
3. Broker responds downstream:
   ```json
   {
     "action": "delete",
     "reqId": "5",
     "success": true,
     "path": "unused.js"
   }
   ```

---

## 6. Cross-Platform Client Architecture (`tools/cli`)

The client sync program is added directly to `tools/cli` as part of the official `mmq` (MonsterMQ CLI) binary.

### 6.1 OS Support Matrix
| Operating System | Architectures | File Watcher Backend (`fsnotify`) | Path Separator Handling |
|---|---|---|---|
| **Windows** | `amd64` (`mmq-windows-amd64.exe`) | `ReadDirectoryChangesW` | Converts local `\` to `/` before transmission |
| **macOS** | `amd64`, `arm64` (`mmq-darwin-*`) | `FSEvents` / `kqueue` | Native `/` |
| **Linux** | `amd64`, `arm64`, `armv7` (`mmq-linux-*`) | `inotify` | Native `/` |

### 6.2 CLI Command Syntax & Flags
```bash
mmq [global-options] hmi sync <dashboard> [local-dir] [options]
```

#### Example Usages:
```bash
# Watch mode with initial pull from broker (connects to 192.168.1.50:1883)
mmq --host 192.168.1.50 hmi sync main ./my-hmi --pull

# One-shot pull: clone broker's HMI to local directory and exit
mmq --host 192.168.1.50 hmi sync main ./my-hmi --pull-only

# One-shot push: upload all local files to broker and exit
mmq --host 192.168.1.50 hmi sync main ./my-hmi --push-only

# Secure connection using TLS and authentication
mmq --host panel.factory.internal --mqtt-port 8883 --https --user engineer --pass secret hmi sync main ./hmi
```

---

## 7. Security & Directory Sandboxing Guarantee

The broker enforces strict security boundaries:
1. **Lexical Containment**: Every target path is cleaned and verified using `filepath.Rel(dashBase, targetPath)`. Any path attempting directory traversal (`..`, leading `/`, drive letters `C:`) is immediately rejected with an `access denied: outside of dashboard directory` error.
2. **Symlink Containment**: `filepath.EvalSymlinks` verifies that any symlinks inside the dashboard cannot point outside the dashboard directory.
3. **Dashboard Identifier Validation**: Tested against `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.
4. **Atomic File Replacement**: Files written on the broker are written to a temporary file (`.tmp.<uuid>`) and atomically renamed once verified against the SHA-256 hash.
5. **Authentication & ACLs**: If broker UserManagement is active, MQTT clients must supply valid credentials. Publish permissions on `monstermq/hmi/sync/#` are required.
6. **Feature Gating**: Configurable via `HMI.SyncEnabled` (can be disabled in production environments where local syncing should be disallowed).

---

## 8. Dual-Broker Implementation Blueprint

### 8.1 Go Edge Broker (`monster-mq-edge`)
- **Configuration**:
  - `HMI.SyncEnabled: true`
  - `HMI.SyncBaseTopic: "monstermq/hmi/sync"`
- **Service**:
  - `internal/hmi/sync.go`: `SyncService` creates an inline subscription via `server.Subscribe(prefix + "/+/upstream", 0, handler)`.
  - Dispatches requests to an internal queue/goroutine worker to keep the MQTT broker hot path non-blocking.
  - Reuses `hmi.Manager`'s existing traversal-safe methods: `ResolveDashboardPath`, `ListDashboardFiles`, `ExportDashboardZip`, `WriteDashboardFile`, `DeleteDashboardFile`, `UploadDashboardZip`.
  - Lifecycle wired to `broker.Server` (`Start()` and `Close()`).

### 8.2 JVM / Kotlin Full Broker (`monster-mq`)
- **Configuration**:
  - Add `HmiSyncEnabled: Boolean` to `HmiConfig` and `yaml-json-schema.json`.
- **Service**:
  - Add `HmiSyncService` in `broker/src/main/kotlin/tools/` or `handlers/`.
  - Subscribes to `monstermq/hmi/sync/+/upstream` via Vert.x EventBus / internal MQTT client.
  - Interacts with `data/hmi/` storage using the exact same JSON schema and path containment rules.
  - Both brokers behave identically from the perspective of `mmq hmi sync`.
