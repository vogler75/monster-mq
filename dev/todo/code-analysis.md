# MonsterMQ Codebase Analysis

*Generated 2026-06-10 — full-codebase review (287 Kotlin files, ~88k lines) across core broker, stores, GraphQL/extensions, device integrations, security, and engineering practices.*

**TL;DR:** The architecture is solid — clean Vert.x verticle design, well-abstracted stores, a sensible Extension+Connector device pattern, and a strong Python integration test suite. The three biggest problems are: (1) several genuine security holes, including a SCRAM authentication that accepts any password and an unsandboxed script engine; (2) massive code duplication (~10,000+ lines) across the database backends, device extensions, and GraphQL resolvers; and (3) almost no engineering automation — no CI, no linting, ~3.6% unit test coverage.

---

## 🔴 Critical — security (fix before any production claim)

1. **SCRAM-SHA-256 auth is a bypass.** `auth/ScramSha256AuthProvider.kt:194-198` — the client proof verification is stubbed to literally `true // Accept for now`, then logs "Authentication successful". Anyone selecting SCRAM enhanced auth authenticates with any password. Either implement the real RFC 7677 verification or **remove/disable the mechanism** until it's done — a stub that returns success is worse than not offering SCRAM at all.

2. **Flow engine scripts run with full host access.** `flowengine/FlowScriptEngine.kt:32` — GraalVM `Context` is built with `.allowAllAccess(true)` and no CPU/memory/time limits. Anyone who can create a flow (e.g., via GraphQL) can execute arbitrary Java code on the broker and a `while(true)` loop wedges the executor. Use `HostAccess.EXPLICIT` with `@HostAccess.Export` on the API you intend to expose, plus a `ResourceLimits`/statement-limit and an execution timeout.

3. **Secrets committed to the repo.** `broker/server-keystore.jks` and `docker/server-keystore.jks` (TLS private keys), `server-keystore.txt` (documents the store password), and `.env` with `POSTGRES_PASSWORD=manager`. Also `MqttServer.kt:25` hardcodes the keystore password default `"password"`. Purge from git history (BFG), regenerate keys, make the keystore password required config with no default, and ship `.env.example` instead.

4. **Default admin `Admin`/`Admin`, and the credentials are written to the log** (`auth/UserManager.kt:416-435`). Generate a random first-boot password printed once to console only, or force a password change on first login — and never log a password.

5. **Insecure-by-default posture.** `config.yaml` ships `UserManagement.Enabled: false`, and when auth is off, `graphql/AuthenticationResolver.kt:36` reports the caller as `isAdmin: true`. Combined with the open Prometheus/REST/MCP surfaces, a default install is fully open. At minimum, log a loud startup warning and make anonymous mean *least* privilege rather than admin.

## 🟠 High — correctness and robustness

6. **Blocking calls on async paths.** `Thread.sleep(1000)` retry loops in `queue/MessageQueueMemory.kt:54` and `queue/MessageQueueDisk.kt:171`; `MqttClient.kt:509-512` wraps `runBlocking` inside `executeBlocking` (deadlock-prone pattern); device connectors sleep in worker threads. Replace with Vert.x timers / proper future composition.

7. **SQL built by string interpolation.** `stores/dbs/cratedb/MessageStoreCrateDB.kt:534-574` interpolates values (including a `config` variable into `payload_json['${config}']`) and `MessageStorePostgres.kt:471` interpolates constants; `MessageArchiveSQLite.kt:298-311` concatenates dynamic column names. Most inputs today come from config rather than clients, but the pattern is one refactor away from injection — parameterize values and whitelist-validate identifiers everywhere.

8. **No JDBC connection pooling.** `stores/DatabaseConnection.kt:74-76` uses one `DriverManager` connection per store. A single slow query serializes everything behind it, and reconnect storms get ugly. Adopt HikariCP (or Vert.x's reactive pg client for the Postgres path).

9. **Resource/lifecycle leaks.** Unclosed `PreparedStatement`s in `MessageArchiveCrateDB.kt:65` and `MessageArchivePostgres.kt:77` (wrap in `.use {}`); `devices/neo4j/Neo4jConnector.kt` starts a writer thread it never joins in `stop()`; `agents/AgentExecutor.kt` never closes its `mcpClients`; a few timer-cancellation paths missing in Redis/Telegram connectors.

10. **Silent data-loss modes.** Archive batch-insert failures are logged-and-dropped (`MessageArchivePostgres.kt:110`), and CrateDB's error-code dedup (`MessageArchiveCrateDB.kt:104`) suppresses *all* repeats of the same error forever. Add retry/dead-letter handling and time-based log dedup so persistent failure is visible.

11. **API keys serialized in clear.** `agents/GenAiProviderConfig.kt:34` puts `apiKey` straight into JSON (which flows into config storage, GraphQL responses, and logs). Redact on serialization; same for device credentials generally — they're stored plaintext in the device config store.

## 🟡 Medium — architecture and maintainability

12. **Duplication is the dominant maintenance cost.**
    - **Stores:** ~60% overlap across the Postgres/CrateDB/SQLite/MongoDB implementations of each store (~8,000 redundant lines). A template-method base class per store type (common logic + abstract SQL builders) would collapse most of it.
    - **Device extensions:** the 13 `*Extension` classes share ~80-90% identical `initializeDeviceStore`/`deployConnector`/`handleConfigChange` boilerplate (~2,000 lines). Extract `BaseDeviceExtension<T : DeviceConfig>`.
    - **GraphQL:** 24 copy-pasted `deviceToMap()` implementations and identical CRUD callback chains across the `*Queries.kt`/`*Mutations.kt` pairs. AGENTS.md already warns that both copies must be updated in sync — that's the bug factory talking. A generic device-config resolver would eliminate the rule.

13. **God classes.** `SessionHandler.kt` (2,015 lines), `OpcUaConnector.kt` (1,853), `MetricsResolver.kt` (1,725), `Monster.kt` (1,646), `MqttClient.kt` (1,536). `MqttClient.kt` even defines the same `finishClientStartup()` nested function twice (~lines 391 and 568). Split by responsibility (e.g., SessionHandler → subscription mgmt / routing / metrics / queueing).

14. **GraphQL hardening.** Auth is enforced per-field by hand-wired `validateFieldAccess` wrappers in `GraphQLServer.kt:695-930` — a new mutation added without the wrapper silently ships unauthenticated. Move to schema-level instrumentation or an auth directive. Also missing: query depth/complexity limits, payload size limit on `publish`, and bulk-size limit on the REST write endpoint (`RestApiServer.kt:338-402`). Raw exception messages leak to clients (`MutationResolver.kt:349`).

15. **MQTT edge cases.** Will publish isn't awaited before session teardown (lost will on failure); no timeout for stalled QoS 2 flows on idle-but-connected clients; `MqttClient.kt:1310` has a TODO for cleanup when Receive Maximum saturates; in-flight queue size is hardcoded at 10,000 per client (`SessionHandler.kt:1324`, marked `// TODO: configurable`).

## ⚙️ Engineering practice gaps

16. **No CI at all.** `.github/` contains only `FUNDING.yml`. This is the highest-leverage single fix: one workflow running `mvn package` + the pytest suite against a dockerized broker would catch most regressions. Add Docker image publish on tag (release.sh currently has the JAR build commented out).

17. **Unit tests: 10 files vs 277 main files.** The 61-file Python integration suite is genuinely good, but stores (only SQLite tested), all 12 JDBC loggers, all 13 device connectors, handlers, and the message bus have zero unit coverage. Given the store duplication, testing one shared base class after the refactor in #12 gives coverage of all backends cheaply — do those two together.

18. **No static analysis.** Add detekt + ktlint to the Maven build and an `.editorconfig`; detekt would have flagged most of the `printStackTrace()` calls (21 in the device layer alone), empty catch blocks, and god classes automatically. Also bump `maven-surefire/failsafe` from 2.22.2 (2019) to 3.x and add `maven-enforcer-plugin` for dependency convergence. Dependencies themselves are commendably current (Vert.x 5.1.1, Kotlin 2.3, Jackson 2.18).

19. **Repo hygiene:** 9.5MB of built dashboard output committed under `broker/src/main/resources/dashboard/` — generate it in CI instead so it can't drift from `dashboard/src/`.

---

## Suggested order of attack

1. **Now:** disable/fix SCRAM (#1), sandbox the flow engine (#2), purge committed secrets (#3), fix the default-admin logging (#4).
2. **Next:** GitHub Actions CI (#16) — it protects everything that follows.
3. **Then:** connection pooling (#8), the blocking-call and resource-leak fixes (#6, #9), SQL parameterization (#7).
4. **Ongoing:** the three deduplication refactors (#12) paired with unit tests (#17), then chip away at the god classes (#13).

The foundation is genuinely good — the issues are concentrated in security defaults, duplication, and missing automation, all of which are fixable without architectural upheaval.
