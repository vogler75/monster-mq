# MonsterMQ MQTT Broker

A high-performance MQTT broker built with Kotlin, Vert.x, and Hazelcast, featuring multiple storage backends and advanced message management capabilities.

## Build

> mvn clean package

## Copy dependencies to a directory

> mvn dependency:copy-dependencies

## Generate a keystore 

> keytool -genkeypair -alias monstermq -keyalg RSA -keysize 2048 -validity 365 -keystore server-keystore.jks -storepass password

## Message Retention and Purging

MonsterMQ provides comprehensive message retention capabilities through ArchiveGroups, allowing automatic cleanup of old messages based on configurable retention periods.

### Configuration

Retention is configured per ArchiveGroup in your `config.yaml`:

```yaml
ArchiveGroups:
  - Name: "Default"
    LastValStoreType: "MEMORY"  # or SQLITE, POSTGRES, CRATEDB, MONGODB, HAZELCAST
    MessageArchiveType: "POSTGRES"  # or SQLITE, CRATEDB, MONGODB
    
    # Retention settings
    LastValRetention: "7d"      # Keep last values for 7 days
    ArchiveRetention: "30d"     # Keep archived messages for 30 days
    PurgeInterval: "1h"         # Check for old messages every hour
    
    # Database connections
    LastValStoreUrl: "postgresql://localhost:5432/monstermq"
    MessageArchiveUrl: "postgresql://localhost:5432/monstermq"
```

### Retention Period Format

Retention periods support the following time units:

- `s` - seconds (e.g., `30s`)
- `m` - minutes (e.g., `15m`)
- `h` - hours (e.g., `2h`)  
- `d` - days (e.g., `7d`)
- `w` - weeks (e.g., `2w`)
- `M` - months (e.g., `3M`)
- `y` - years (e.g., `1y`)

Examples:
- `"5m"` - 5 minutes
- `"12h"` - 12 hours
- `"30d"` - 30 days
- `"6M"` - 6 months

### Storage Backend Support

All storage backends support message purging with optimized implementations:

#### Memory Stores
- **Memory**: Direct HashMap operations with progress logging for large datasets
- **Hazelcast**: Distributed predicates for efficient cluster-wide purging

#### Database Stores
- **PostgreSQL**: SQL DELETE operations with proper transaction handling
- **CrateDB**: Optimized for time-series data with batch deletions
- **SQLite**: Async operations via event bus to prevent blocking
- **MongoDB**: Native MongoDB deleteMany() operations with time-based filters

### Purge Process

1. **Scheduled Execution**: Based on `PurgeInterval` setting
2. **Cluster Coordination**: Uses distributed locks to ensure only one node performs purging
3. **LastVal Purging**: Removes outdated current values older than `LastValRetention`
4. **Archive Purging**: Removes historical messages older than `ArchiveRetention`  
5. **Performance Logging**: Tracks deletion counts and execution time
6. **Error Handling**: Graceful handling of database connectivity issues

### Cluster-Aware Purging

In clustered deployments, MonsterMQ automatically coordinates purging operations across all nodes:

#### **Distributed Locking**
- Each ArchiveGroup uses cluster-wide distributed locks (`purge-lock-{ArchiveGroupName}-{StoreType}`)
- Only one node in the cluster can acquire the lock and perform purging
- Lock timeout of 30 seconds prevents deadlocks
- Other nodes skip purging with informational logging

#### **Lock Behavior**
```
[FINE] Acquired purge lock for LastVal store [Default] - starting purge
[FINE] Could not acquire purge lock for Archive store [Default] - skipping purge (likely another cluster node is purging)
```

#### **Benefits**
- **No Duplicate Work**: Prevents multiple nodes from purging the same data
- **Resource Efficiency**: Only one node uses resources for purging operations  
- **Data Safety**: Eliminates race conditions and potential corruption
- **Automatic Failover**: If the purging node fails, another node can acquire the lock

### Monitoring

Purge operations are logged with performance metrics:

```
[FINE] Starting purge for [DefaultLastval] - removing messages older than 2024-08-01T00:00:00Z
[FINE] Purge completed for [DefaultLastval]: deleted 1247 messages in 234ms
```

### Best Practices

1. **Retention Periods**: Balance storage costs with data requirements
2. **Purge Intervals**: More frequent purging reduces batch sizes but increases overhead
3. **Storage Choice**: 
   - Use Memory/Hazelcast for high-performance current values
   - Use PostgreSQL/CrateDB for reliable long-term archiving
   - Use MongoDB for flexible schema and time-series optimization
4. **Monitoring**: Watch purge logs to ensure proper operation and performance