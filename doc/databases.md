# Database Configuration

MonsterMQ supports several backends for sessions, retained messages, archive groups, metrics, and user data. The broker reads a small set of connection properties from `config.yaml` and builds the schema it needs at runtime. This page documents the options that are actually consumed by the code base.

## Choosing Store Types

At start-up MonsterMQ determines which implementation to use for each logical store (`broker/src/main/kotlin/Monster.kt:721-812`). You can either set individual store types or rely on `DefaultStoreType` as a fallback.

```yaml
# Optional global default (used whenever a specific type is not supplied)
DefaultStoreType: POSTGRES

# Explicit overrides
SessionStoreType: POSTGRES
RetainedStoreType: SQLITE
ConfigStoreType: POSTGRES
```

Valid values are `POSTGRES`, `CRATEDB`, `MONGODB`, `SQLITE`, `MEMORY`, `HAZELCAST`, and `NONE` depending on the store. Pick only the types supported by the component you are configuring (e.g. `SessionStoreType` does not accept `MEMORY`).

## Backend Sections

For each backend provide the minimal set of properties below. The broker does not read pooling, shard, or SSL tuning parameters—configure those on the database server itself.

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
- Uses PostgreSQL's native `SET search_path` for clean, efficient schema switching
- **100% backward compatible** - Existing configs without `Schema` parameter work unchanged

**Example - Multi-environment setup:**

```yaml
# Production environment
Postgres:
  Url: jdbc:postgresql://db.example.com:5432/monstermq
  User: system
  Pass: manager
  Schema: prod_mqtt_broker

# Staging environment (same database, different schema)
Postgres:
  Url: jdbc:postgresql://db.example.com:5432/monstermq
  User: system
  Pass: manager
  Schema: staging_mqtt_broker
```

The JDBC URL is passed straight to the Vert.x SQL client. All message/metrics/user stores create their tables automatically (`broker/src/main/kotlin/stores/dbs/postgres/MessageStorePostgres.kt:16-88`).

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
```

Only the connection string and database name are consumed (`broker/src/main/kotlin/stores/dbs/mongodb/MessageStoreMongoDB.kt:17-26`, `UserFactory` for user storage).

### SQLite

```yaml
SQLite:
  Path: "./data"
```

`Path` must point to an existing writable directory. The broker creates `monstermq.db` (and per-archive files) inside that directory and validates the path on start (`broker/src/main/kotlin/Monster.kt:792-812`).

### Kafka (Archive Only)

Kafka is configured separately under the `Kafka` section. See `doc/kafka.md` for details. The database stores never pull additional settings from that section.

## Metrics Store

The metrics collector reuses the same backend configuration and defaults as other stores. When enabled (`Metrics.Enabled = true`) the broker either auto-detects a backend with available configuration or honours `MetricsStore.Type` if present (`broker/src/main/kotlin/stores/factories/MetricsStoreFactory.kt:23-62`).

```yaml
Metrics:
  Enabled: true
  CollectionInterval: 5   # seconds (optional)

# Optional explicit store override
MetricsStore:
  Type: POSTGRES
```

## Recap

- Only the keys listed above are read; properties such as `MaxPoolSize`, `ReplicationFactor`, or retention/partition settings are ignored by the current implementation.
- Schema creation, indices, and connection pooling are handled internally by each Vert.x store class. Adjust database-level performance settings directly on your database server.
- When using SQLite, create the target directory up front and ensure the broker has write permissions.
