# Message Archiving

MonsterMQ provides comprehensive message archiving with configurable retention policies and dynamic management through GraphQL API.

## Overview

Archive groups define how MQTT messages are stored and managed:

- **Current Values (LastVal)** - Latest message per topic
- **Historical Archive** - Time-series message storage
- **Retention Policies** - Automatic cleanup based on age
- **Dynamic Configuration** - Real-time management via GraphQL API

## Archive Group Configuration

### Creating Archive Groups

Use GraphQL API to create and manage archive groups:

```graphql
mutation {
  archiveGroup {
    create(input: {
      name: "ProductionSensors"
      topicFilter: ["sensors/#", "devices/#", "production/#"]
      retainedOnly: false
      lastValType: POSTGRES       # Current values storage
      archiveType: POSTGRES       # Historical messages storage
      lastValRetention: "7d"      # Keep current values for 7 days
      archiveRetention: "30d"     # Keep history for 30 days
      purgeInterval: "1h"         # Clean up every hour
    }) {
      success
      message
      archiveGroup {
        name
        enabled
        deployed
      }
    }
  }
}
```

### Archive Group Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `name` | Unique identifier for the archive group | `"ProductionSensors"` |
| `enabled` | Read current state; change through `enable`/`disable` mutations | `true` |
| `topicFilter` | MQTT topic patterns to archive | `["sensors/#", "devices/+/data"]` |
| `retainedOnly` | Archive only retained messages | `false` |
| `lastValType` | Storage backend for current values | `POSTGRES`, `MEMORY`, `HAZELCAST` |
| `archiveType` | Storage backend for historical data | `POSTGRES`, `CRATEDB`, `QUESTDB`, `MONGODB`, `SQLITE`, `NONE` |
| `lastValRetention` | Age limit for persistent stores; MEMORY uses a count such as `50k` | `"7d"`, `"24h"`, `"30m"` |
| `archiveRetention` | How long to keep historical data | `"30d"`, `"1y"`, `"6M"` |
| `purgeInterval` | How often to run cleanup | `"1h"`, `"6h"`, `"24h"` |
| `payloadFormat` | Message encoding: `DEFAULT` (recommended) or `JSON` | `DEFAULT` |

## Payload Format

Archive groups store raw MQTT payloads. The `payloadFormat` setting controls how data is interpreted when archiving:

- `DEFAULT` (recommended): Internal binary/base64 representation.
- `JSON`: Parse payload as JSON and store structured representation where supported.

If omitted, the system uses `DEFAULT`.

## Storage Backends

### Database Storage

**PostgreSQL**
- Persistent relational storage
- SQL queries and analytics
- ACID compliance and reliability
- Historical queries and aggregations

**CrateDB**
- Optimized for time-series and analytics
- Distributed architecture
- PostgreSQL-compatible wire protocol
- Built-in clustering support

**MongoDB**
- NoSQL document-based storage
- Flexible schema for varying message formats
- Document-oriented message storage
- Built-in sharding and replication

**SQLite**
- Embedded persistence for standalone/edge deployments
- Single-file database
- No network overhead
- Local storage; not shared across cluster nodes

### Memory Storage

**Memory**
- Fastest access for current values
- Volatile storage (lost on restart)
- No persistence across broker restarts
- Ideal for high-frequency current values

**Hazelcast**
- Distributed in-memory storage
- Shared across cluster nodes
- Automatic replication and failover
- Good for cluster-wide current values

### QuestDB and Kafka

`QUESTDB` is supported for historical archives using a configured database
connection. It is not a session, queue, or retained-store type. `KAFKA` and
`MEMORY` are not valid historical archive types. Use the [Kafka client
bridge](kafka.md) to forward MQTT messages to Kafka; the old Kafka archive store
has been removed.

## Retention Policies

### Time Format

Retention periods support flexible time units:

| Unit | Description | Examples |
|------|-------------|----------|
| `s` | Seconds | `30s`, `120s` |
| `m` | Minutes | `15m`, `45m` |
| `h` | Hours | `2h`, `12h`, `24h` |
| `d` | Days | `1d`, `7d`, `30d` |
| `w` | Weeks | `1w`, `2w`, `4w` |
| `M` | Months | `1M`, `3M`, `6M` |
| `y` | Years (365 days) | `1y`, `2y`, `5y` |

Months use 30 days. For `MEMORY` last-value stores, use size-based retention such
as `50k` (50,000 topic entries), or omit it. Time-based retention such as `1h` is
rejected for `MEMORY`. Persistent last-value stores support time-based retention.

### Example Configurations

**High-Volume IoT Sensors**
```graphql
mutation {
  archiveGroup {
    create(input: {
      name: "HighVolumeSensors"
      topicFilter: ["sensors/+/raw"]
      lastValRetention: "50k"     # Bound the MEMORY cache to 50,000 topics
      archiveRetention: "7d"      # History for 1 week
      purgeInterval: "15m"        # Clean up every 15 minutes
      lastValType: MEMORY         # Fast access
      archiveType: POSTGRES       # Reliable storage
    }) { success }
  }
}
```

**Long-Term Analytics**
```graphql
mutation {
  archiveGroup {
    create(input: {
      name: "Analytics"
      topicFilter: ["metrics/#", "events/#"]
      lastValRetention: "30d"     # Current state for 30 days
      archiveRetention: "2y"      # Keep history for 2 years
      purgeInterval: "24h"        # Daily cleanup
      lastValType: POSTGRES
      archiveType: POSTGRES
    }) { success }
  }
}
```

**Development/Testing**
```graphql
mutation {
  archiveGroup {
    create(input: {
      name: "Development"
      topicFilter: ["test/#", "debug/#"]
      lastValRetention: "10k"     # Bound the MEMORY cache to 10,000 topics
      archiveRetention: "6h"      # History for 6 hours
      purgeInterval: "30m"        # Clean up every 30 minutes
      lastValType: MEMORY         # Fast, volatile
      archiveType: SQLITE         # No persistence needed
    }) { success }
  }
}
```

## Archive Group Management

### Enable/Disable Archive Groups

```graphql
# Enable archive group
mutation {
  archiveGroup {
    enable(name: "ProductionSensors") {
      success
      message
      archiveGroup {
        name
        enabled
        deployed
      }
    }
  }
}
```

```graphql
# Disable archive group
mutation {
  archiveGroup {
    disable(name: "ProductionSensors") {
      success
      message
    }
  }
}
```

### Update Archive Groups

```graphql
mutation {
  archiveGroup {
    update(input: {
        name: "ProductionSensors"
        topicFilter: ["sensors/#", "devices/#", "production/#", "alerts/#"]
        archiveRetention: "60d"    # Extend retention to 60 days
        purgeInterval: "2h"        # Change cleanup frequency
      }
    ) {
      success
      message
      archiveGroup {
        name
        topicFilter
        archiveRetention
      }
    }
  }
}
```

### Query Archive Groups

```graphql
# Get all archive groups
query {
  archiveGroups {
    name
    enabled
    deployed
    topicFilter
    lastValType
    archiveType
    lastValRetention
    archiveRetention
    purgeInterval
    createdAt
    updatedAt
  }
}
```

```graphql
# Get specific archive group
query {
  archiveGroup(name: "ProductionSensors") {
    name
    enabled
    deployed
    topicFilter
  }
}
```

### Delete Archive Groups

```graphql
mutation {
  archiveGroup {
    delete(name: "ProductionSensors") {
      success
      message
    }
  }
}
```

Disable the group and wait until `deployed` is false before deleting it. The
current delete resolver removes configuration and explicitly skips storage
cleanup. Existing tables/collections are not dropped automatically.

## Storage Lifecycle Management

Stores create their required tables, collections, and indexes when deployed.
Schema and payload columns differ by backend and payload format; use the store
implementation instead of generic hand-written DDL. Enabling or disabling a
group updates message routing without restarting the broker.

Groups can select a saved database connection through `databaseConnectionName`.
Manage connections with `archiveGroup.createDatabaseConnection`,
`updateDatabaseConnection`, and `deleteDatabaseConnection`. Buffering settings
include `queueType`, `queueSize`, `bulkSize`, `bulkTimeoutMs`, and `queueDiskPath`.
`lastValReadOnly` and `archiveReadOnly` support access to existing data without
normal writes. See the current
[mutation input definitions](../broker/src/main/resources/schema-mutations.graphqls)
for the complete fields.

## Bulk Import Configuration

For initial setup or migration, use the `-archiveConfigs` or `-archiveConfigsMerge` parameters with a JSON file:

- **`-archiveConfigs <file>`** — Full sync: imports all groups from the file and deletes any existing groups not present in the file.
- **`-archiveConfigsMerge <file>`** — Merge: imports/updates groups from the file, keeps existing groups that are not in the file.

These two arguments cannot be used together.

```bash
# Full sync: import archive groups, delete any not in the file
./run.sh -- -archiveConfigs archive-setup.json

# Merge: import/update archive groups, keep existing ones
./run.sh -- -archiveConfigsMerge archive-setup.json
```

**archive-setup.json example:**
```json
[
  {
    "Name": "ProductionSensors",
    "Enabled": true,
    "TopicFilter": ["sensors/#", "devices/#"],
    "RetainedOnly": false,
    "LastValType": "POSTGRES",
    "ArchiveType": "POSTGRES",
    "LastValRetention": "7d",
    "ArchiveRetention": "30d",
    "PurgeInterval": "1h"
  },
  {
    "Name": "DebugData",
    "Enabled": false,
    "TopicFilter": ["debug/+", "test/#"],
    "LastValType": "MEMORY",
    "ArchiveType": "POSTGRES",
    "LastValRetention": "10k",
    "ArchiveRetention": "24h",
    "PurgeInterval": "30m"
  }
]
```

## Export Configuration

Click the download icon button in the Archive Groups table header in the dashboard to export all archive groups as a JSON file.

The exported JSON uses the same format as the import files and can be used directly with `-archiveConfigs` or `-archiveConfigsMerge`.

## Clustering Considerations

### Database Requirements

- Use shared PostgreSQL or MongoDB for cluster session, queue, and configuration stores.
- Archive data can use a different supported backend, including CrateDB or QuestDB.
- SQLite files are local and do not provide shared cluster state.
- Nodes serving the same archive group must reach the same data.

### Distributed Purging

In cluster deployments, purging is coordinated across nodes:

- **Distributed locks** prevent duplicate cleanup operations
- **Only one node** per archive group performs purging
- Lock acquisition is bounded; a node skips a purge when another node holds the lock
- **Automatic coordination** requires no manual intervention

### Example Cluster Configuration

```yaml
# All cluster nodes use same configuration
DefaultStoreType: POSTGRES
SessionStoreType: POSTGRES
RetainedStoreType: HAZELCAST
ConfigStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://shared-db-server:5432/monster
  User: system
  Pass: manager
```

## Best Practices

### Storage Planning

1. **Use appropriate backends:**
   - Memory/Hazelcast for high-frequency current values
   - PostgreSQL/MongoDB for reliable historical storage
   - Kafka client bridge for forwarding publications to Kafka

2. **Set realistic retention:**
   - Balance storage costs with data requirements
   - Consider query patterns and access frequency
   - Plan for compliance and regulatory requirements

3. **Optimize purge intervals:**
   - More frequent purging = smaller batches, more overhead
   - Less frequent purging = larger batches, potential memory usage
   - Typical range: 15 minutes to 24 hours

### Performance Optimization

1. **Database indexes:**
```sql
-- PostgreSQL performance indexes
CREATE INDEX idx_archive_topic_time ON archive_table(topic, time);
CREATE INDEX idx_archive_time ON archive_table(time);
CREATE INDEX idx_lastval_topic ON lastval_table(topic);
```

2. **Topic filter specificity:**
```text
# Good - specific patterns
topicFilter: ["sensors/temperature/#", "devices/+/status"]

# Avoid - overly broad patterns
topicFilter: ["#"]  # Archives ALL messages
```

3. **Retention tuning:**
```text
# High-volume: shorter retention, frequent cleanup
lastValRetention: "1h"
archiveRetention: "7d"
purgeInterval: "15m"

# Low-volume: longer retention, less frequent cleanup
lastValRetention: "30d"
archiveRetention: "1y"
purgeInterval: "24h"
```

## Monitoring and Troubleshooting

### Log Monitoring

Inspect the broker console, container logs, or configured service log capture for
`Purge completed`, `purge lock`, and purge errors. Use `-log FINE` temporarily if
more detail is needed. The default logger writes to the console, not a fixed
`log/monstermq.log` file. See [System logs](graphql-system-logs.md).

### Performance Metrics

Key metrics to monitor:
- Message ingestion rate per archive group
- Purge operation duration and frequency
- Database storage growth
- Query response times

### Common Issues

**1. High Storage Growth**
- Check retention policies are appropriate
- Verify purge operations are running
- Consider more frequent purging

**2. Slow Purge Operations**
- Add database indexes on time columns
- Reduce purge batch size via more frequent intervals
- Optimize database configuration

**3. Cluster Purge Conflicts**
- Check for clock synchronization across nodes
- Verify database connectivity from all nodes
- Monitor distributed lock timeouts

## Related Documentation

- **[Configuration Reference](configuration.md)** - Archive group parameters
- **[Database Setup](databases.md)** - Backend-specific configuration
- **[GraphQL API](graphql.md)** - Management API documentation
- **[Monitoring](monitoring.md)** - Optimization and monitoring