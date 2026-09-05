# Database Configuration

MonsterMQ supports several backends for sessions, retained messages, archive groups, metrics, and user data. The broker reads a small set of connection properties from `config.yaml` and builds the schema it needs at runtime. This page documents the options that are actually consumed by the code base.

## Choosing Store Types

At start-up MonsterMQ determines which implementation to use for each logical store (`broker/src/main/kotlin/Monster.kt`). You can either set individual store types or rely on `DefaultStoreType` as a fallback.

```yaml
# Optional global default (used whenever a specific type is not supplied)
DefaultStoreType: POSTGRES

# Explicit overrides
SessionStoreType: POSTGRES
RetainedStoreType: SQLITE
ConfigStoreType: POSTGRES
```

Valid values are `POSTGRES`, `CRATEDB`, `MONGODB`, `SQLITE`, `MEMORY`, `HAZELCAST`, and `NONE` depending on the store. Pick only the types supported by the component you are configuring (e.g. `SessionStoreType` accepts `POSTGRES`, `MONGODB`, `SQLITE`, and `MEMORY`; it does not accept `CRATEDB`).

## Backend Sections

For each backend provide the minimal set of properties below. Connection properties are backend-specific. JDBC URL options can also affect client connection behavior; do not assume arbitrary YAML pool or SSL keys are parsed.

### PostgreSQL

```yaml
Postgres:
  Url: jdbc:postgresql://db-host:5432/monstermq
  User: system
  Pass: manager
  Schema: mqtt_broker    # Optional: defaults to 'public' if not specified
```

#### PostgreSQL Schema Support (Optional)

By default, all PostgreSQL objects are created in the `public` schema. You can optionally specify a custom schema using the `Schema` parameter to:

- **Multi-tenant deployments** - Use different schemas for different tenants in the same database
- **Organization standards** - Follow your organization's database naming and schema conventions
- **Environment isolation** - Separate dev/staging/prod data within the same database instance

**Features:**
- The specified schema is **automatically created** if it doesn't exist
- Schema is applied to all PostgreSQL stores (sessions, messages, archives, metrics, users)
- Uses `SET search_path` on connections managed by these stores
- Omit `Schema` to retain the default schema behavior

**Example - Multi-environment setup:**

```yaml
# Production environment
Postgres:
  Url: jdbc:postgresql://db.example.com:5432/monstermq
  User: system
  Pass: manager
  Schema: prod_mqtt_broker

```

Use a separate configuration for staging:

```yaml
Postgres:
  Url: jdbc:postgresql://db.example.com:5432/monstermq
  User: system
  Pass: manager
  Schema: staging_mqtt_broker
```

PostgreSQL stores manage JDBC connections internally. Message, metrics, and user stores create their tables automatically (`broker/src/main/kotlin/stores/dbs/postgres/MessageStorePostgres.kt`).

### CrateDB

```yaml
CrateDB:
  Url: jdbc:postgresql://crate-host:5432/monstermq
  User: crate
  Pass: ""
```

CrateDB uses the PostgreSQL protocol; the URL format matches the standard JDBC connection string. Table creation is handled by the corresponding store class (`broker/src/main/kotlin/stores/dbs/cratedb/MessageStoreCrateDB.kt`).

### MongoDB

```yaml
MongoDB:
  Url: mongodb://system:manager@mongo-host:27017
  Database: monstermq
  ReadTimeoutMs: 60000  # Cursor read timeout
```

The connection string, database name, and optional read timeout are consumed (`broker/src/main/kotlin/stores/dbs/mongodb/MessageStoreMongoDB.kt`, `UserFactory` for user storage).

### SQLite

```yaml
SQLite:
  Path: "./data"
  EnableWAL: true
```

`Path` names a writable directory, which the broker creates if missing. The broker creates `monstermq.db` (and per-archive files) inside that directory and validates the path on start (`broker/src/main/kotlin/Monster.kt`).

### QuestDB (historical archives)

```yaml
QuestDB:
  Url: "ws::addr=questdb:9000;"
  User: admin
  Pass: replace-with-your-password
```

Select `ArchiveType: QUESTDB` in an archive group. Ingestion uses the QuestDB client
connection string; reads use PGWire on port 8812 for that host. A JDBC URL can be
supplied instead; the broker then derives ingestion on port 9000. The URL parser
is in `MessageArchiveQuestDB.parseQuestDbUrls()`. Credentials for an authenticated
ingestion endpoint must also be included in its connection string as required by
the QuestDB client; the separate `User`/`Pass` fields feed the JDBC connection.
QuestDB is not a session, queue, or last-value store.

### Saved database connections

Archive groups can select a saved database connection through
`databaseConnectionName` instead of the global backend section. Manage these
through the dashboard or the `archiveGroup` GraphQL mutation group (`createDatabaseConnection`,
`updateDatabaseConnection`, `deleteDatabaseConnection`); consult
[`schema-mutations.graphqls`](../broker/src/main/resources/schema-mutations.graphqls)
for supported connection fields. See [Archiving](archiving.md) for group selection.

Kafka integration is a message bus/bridge/server feature, not a current historical
archive backend; see [Kafka](kafka.md).

## Metrics Store

The running broker defaults to `StoreType`, then `DefaultStoreType`, then
`SQLITE`. An explicit persistent metrics store must match this broker store type;
`MEMORY` and `NONE` can be selected independently.

```yaml
Metrics:
  Enabled: true
  CollectionIntervalSeconds: 10
  RetentionHours: 24
  MaxHistoryRows: 3600
  StoreType: MEMORY
```

`MetricsStore.Type` is a legacy alias for `Metrics.StoreType`;
`CollectionInterval` is a legacy alias for `CollectionIntervalSeconds`.

## Recap

- Arbitrary connection-pool or replication keys are not generic broker settings. Configure database-server behavior in the database, and archive retention on the archive group.
- Schema creation, indices, and connection management are handled internally by each store implementation. Adjust database-level performance settings directly on your database server.
- When using SQLite, ensure the broker can create and write its target directory.

## Queue Storage

`QueueStoreType` accepts `POSTGRES`, `MONGODB`, or `SQLITE`. If omitted, it
follows `DefaultStoreType`, then a non-memory `SessionStoreType`, then `SQLITE`.
Set it explicitly when a global default is unsuitable for queues. Current queue
stores use the V2 single-table implementation. A `MEMORY` session store uses an
in-memory SQLite database; it does not make the separate queue store volatile.
