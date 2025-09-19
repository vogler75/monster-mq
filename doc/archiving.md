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
  createArchiveGroup(input: {
    name: "ProductionSensors"
    enabled: true
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
```

### Archive Group Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `name` | Unique identifier for the archive group | `"ProductionSensors"` |
| `enabled` | Whether the archive group is active | `true` |
| `topicFilter` | MQTT topic patterns to archive | `["sensors/#", "devices/+/data"]` |
| `retainedOnly` | Archive only retained messages | `false` |
| `lastValType` | Storage backend for current values | `POSTGRES`, `MEMORY`, `HAZELCAST` |
| `archiveType` | Storage backend for historical data | `POSTGRES`, `MONGODB`, `KAFKA` |
| `lastValRetention` | How long to keep current values | `"7d"`, `"24h"`, `"30m"` |
| `archiveRetention` | How long to keep historical data | `"30d"`, `"1y"`, `"6M"` |
| `purgeInterval` | How often to run cleanup | `"1h"`, `"6h"`, `"24h"` |

## Storage Backends

### Database Storage

**PostgreSQL**
- Best for production environments
- SQL queries and analytics
- ACID compliance and reliability
- Excellent performance for time-series data

**CrateDB**
- Optimized for time-series and analytics
- Distributed architecture
- PostgreSQL-compatible wire protocol
- Built-in clustering support

**MongoDB**
- NoSQL document-based storage
- Flexible schema for varying message formats
- Good performance for high-volume inserts
- Built-in sharding and replication

**SQLite**
- Perfect for development and testing
- Single-file database
- No network overhead
- Cannot be used in cluster mode

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

### Streaming Storage

**Kafka**
- Stream messages to Kafka topics
- Archive group name becomes Kafka topic
- Custom binary serialization format
- Perfect for real-time analytics

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
| `y` | Years | `1y`, `2y`, `5y` |

### Example Configurations

**High-Volume IoT Sensors**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "HighVolumeSensors"
    topicFilter: ["sensors/+/raw"]
    lastValRetention: "1h"      # Current readings for 1 hour
    archiveRetention: "7d"      # History for 1 week
    purgeInterval: "15m"        # Clean up every 15 minutes
    lastValType: MEMORY         # Fast access
    archiveType: POSTGRES       # Reliable storage
  }) { success }
}
```

**Long-Term Analytics**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "Analytics"
    topicFilter: ["metrics/#", "events/#"]
    lastValRetention: "30d"     # Current state for 30 days
    archiveRetention: "2y"      # Keep history for 2 years
    purgeInterval: "24h"        # Daily cleanup
    lastValType: POSTGRES
    archiveType: POSTGRES
  }) { success }
}
```

**Development/Testing**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "Development"
    topicFilter: ["test/#", "debug/#"]
    lastValRetention: "1h"      # Current for 1 hour
    archiveRetention: "6h"      # History for 6 hours
    purgeInterval: "30m"        # Clean up every 30 minutes
    lastValType: MEMORY         # Fast, volatile
    archiveType: MEMORY         # No persistence needed
  }) { success }
}
```

## Archive Group Management

### Enable/Disable Archive Groups

```graphql
# Enable archive group
mutation {
  enableArchiveGroup(name: "ProductionSensors") {
    success
    message
    archiveGroup {
      name
      enabled
      deployed
    }
  }
}

# Disable archive group
mutation {
  disableArchiveGroup(name: "ProductionSensors") {
    success
    message
  }
}
```

### Update Archive Groups

```graphql
mutation {
  updateArchiveGroup(
    name: "ProductionSensors"
    input: {
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
  deleteArchiveGroup(name: "ProductionSensors") {
    success
    message
  }
}
```

**⚠️ Warning:** Deleting an archive group also drops all associated database tables/collections and their data.

## Storage Lifecycle Management

### Automatic Table Creation

When an archive group is enabled, MonsterMQ automatically creates database tables:

**PostgreSQL**
```sql
-- Current values table
CREATE TABLE productionsensorslastval (
    topic VARCHAR PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    payload_b64 VARCHAR,
    qos INTEGER,
    is_retained BOOLEAN,
    client_id VARCHAR
);

-- Historical archive table
CREATE TABLE productionsensorsarchive (
    message_uuid VARCHAR PRIMARY KEY,
    topic VARCHAR NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    payload_b64 VARCHAR,
    qos INTEGER,
    is_retained BOOLEAN,
    client_id VARCHAR
);

-- Create indexes for performance
CREATE INDEX idx_archive_topic_time ON productionsensorsarchive(topic, time);
CREATE INDEX idx_archive_time ON productionsensorsarchive(time);
```

**MongoDB**
```javascript
// Collections are created automatically
db.createCollection("productionsensorslastval");
db.createCollection("productionsensorsarchive");

// Create indexes
db.productionsensorsarchive.createIndex({"topic": 1, "time": 1});
db.productionsensorsarchive.createIndex({"time": 1});
```

### Automatic Storage Cleanup

When an archive group is deleted, all associated storage is automatically cleaned up:

```sql
-- PostgreSQL
DROP TABLE IF EXISTS productionsensorslastval CASCADE;
DROP TABLE IF EXISTS productionsensorsarchive CASCADE;

-- MongoDB
db.productionsensorslastval.drop();
db.productionsensorsarchive.drop();
```

## Real-Time Message Routing

Archive groups automatically start/stop message routing when enabled/disabled:

```kotlin
// When archive group is enabled
archiveHandler.registerArchiveGroup(archiveGroup)
messageHandler.registerArchiveGroup(archiveGroup)

// When archive group is disabled
messageHandler.unregisterArchiveGroup(archiveGroupName)
```

**Benefits:**
- No broker restart required
- Zero downtime for existing connections
- Instant message routing updates
- Automatic cleanup when disabled

## Bulk Import Configuration

For initial setup or migration, use the `-archiveConfig` parameter:

```bash
# Import archive groups from YAML into database
./run.sh -archiveConfig archive-setup.yaml
```

**archive-setup.yaml example:**
```yaml
ArchiveGroups:
  - Name: "ProductionSensors"
    Enabled: true
    TopicFilter: ["sensors/#", "devices/#"]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES
    LastValRetention: "7d"
    ArchiveRetention: "30d"
    PurgeInterval: "1h"

  - Name: "DebugData"
    Enabled: false
    TopicFilter: ["debug/+", "test/#"]
    LastValType: MEMORY
    ArchiveType: POSTGRES
    LastValRetention: "1h"
    ArchiveRetention: "24h"
    PurgeInterval: "30m"
```

## Clustering Considerations

### Database Requirements

- **Clustering requires central database** (PostgreSQL, CrateDB, or MongoDB)
- **SQLite cannot be used** in cluster mode
- **All nodes must connect** to the same database instance

### Distributed Purging

In cluster deployments, purging is coordinated across nodes:

- **Distributed locks** prevent duplicate cleanup operations
- **Only one node** per archive group performs purging
- **Lock timeout** prevents deadlocks (30 seconds)
- **Automatic coordination** requires no manual intervention

### Example Cluster Configuration

```yaml
# All cluster nodes use same configuration
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
   - Kafka for real-time streaming and analytics

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
```graphql
# Good - specific patterns
topicFilter: ["sensors/temperature/#", "devices/+/status"]

# Avoid - overly broad patterns
topicFilter: ["#"]  # Archives ALL messages
```

3. **Retention tuning:**
```graphql
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

Monitor purge operations:
```bash
# Successful operations
tail -f log/monstermq.log | grep "Purge completed"

# Cluster coordination
tail -f log/monstermq.log | grep "purge lock"

# Error conditions
tail -f log/monstermq.log | grep "Failed to.*purge"
```

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
- **[Performance](performance.md)** - Optimization and monitoring