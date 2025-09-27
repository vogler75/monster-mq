# Database Setup

MonsterMQ supports multiple database backends for different use cases and deployment scenarios. This guide covers the configuration and optimization of each supported database.

## Supported Databases

| Database | Best For | Clustering Support | Performance |
|----------|----------|-------------------|-------------|
| **PostgreSQL** | Production deployments, full features | Yes | Excellent |
| **CrateDB** | Time-series data, high-volume IoT | Yes | Very High |
| **MongoDB** | Document-oriented workflows, flexible schema | Yes | High |
| **SQLite** | Development, single-node deployments | No | Good (single-node) |

## PostgreSQL

### Installation

```bash
# Using Docker
docker run -d \
  --name postgres \
  -e POSTGRES_DB=monster \
  -e POSTGRES_USER=system \
  -e POSTGRES_PASSWORD=manager \
  -p 5432:5432 \
  postgres:16-alpine

# Or install locally (Ubuntu/Debian)
sudo apt-get install postgresql postgresql-contrib
sudo -u postgres createdb monster
sudo -u postgres createuser system
```

### Configuration

```yaml
DefaultStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager
  MaxPoolSize: 20  # Connection pool size
  ConnectionTimeout: 30000  # Timeout in milliseconds
```

### Optimization

```sql
-- Optimize for MQTT workloads
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET work_mem = '4MB';

-- Create indexes for better performance
CREATE INDEX idx_messages_topic ON messages(topic);
CREATE INDEX idx_messages_timestamp ON messages(timestamp);
CREATE INDEX idx_sessions_client_id ON sessions(client_id);

-- Enable automatic vacuuming
ALTER SYSTEM SET autovacuum = on;
ALTER SYSTEM SET autovacuum_max_workers = 4;
```

### Backup & Recovery

```bash
# Backup
pg_dump -U system -h localhost monster > monster_backup.sql

# Restore
psql -U system -h localhost monster < monster_backup.sql

# Continuous archiving (WAL)
# In postgresql.conf:
archive_mode = on
archive_command = 'cp %p /backup/wal/%f'
```

## CrateDB

### Installation

```bash
# Using Docker
docker run -d \
  --name cratedb \
  -p 4200:4200 \
  -p 5432:5432 \
  -e CRATE_HEAP_SIZE=1g \
  crate:latest \
  -Cnetwork.host=0.0.0.0

# Access admin UI at http://localhost:4200
```

### Configuration

```yaml
DefaultStoreType: CRATEDB

CrateDB:
  Url: jdbc:postgresql://localhost:5432/monster
  User: crate
  Pass: ""  # CrateDB default has no password
  Schema: doc  # Default schema
  Replicas: "0-1"  # Replication factor
  Shards: 4  # Number of shards
```

### Schema Design

```sql
-- Create optimized tables for MQTT
CREATE TABLE IF NOT EXISTS messages (
    topic TEXT,
    payload TEXT,
    qos INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (topic, timestamp)
) CLUSTERED INTO 4 SHARDS WITH (number_of_replicas = '0-1');

-- Create table for retained messages
CREATE TABLE IF NOT EXISTS retained (
    topic TEXT PRIMARY KEY,
    payload TEXT,
    qos INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE
) WITH (number_of_replicas = '0-1');

-- Optimize for time-series queries
ALTER TABLE messages SET (column_policy = 'dynamic');
```

### Performance Tuning

```sql
-- Optimize for IoT workloads
SET GLOBAL "indices.breaker.query.limit" = '80%';
SET GLOBAL "indices.breaker.request.limit" = '80%';

-- Adjust refresh interval for better ingestion
ALTER TABLE messages SET (refresh_interval = 1000);

-- Enable columnar storage optimization
ALTER TABLE messages SET (codec = 'best_compression');
```

## MongoDB

### Installation

```bash
# Using Docker
docker run -d \
  --name mongodb \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=manager \
  -e MONGO_INITDB_DATABASE=monster \
  -p 27017:27017 \
  mongo:7

# Or install locally
# Ubuntu/Debian
wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | sudo apt-key add -
sudo apt-get install mongodb-org
```

### Configuration

```yaml
DefaultStoreType: MONGODB

MongoDB:
  ConnectionString: mongodb://localhost:27017
  Database: monster
  User: admin
  Pass: manager
  AuthSource: admin  # Authentication database
  MaxPoolSize: 100
  MinPoolSize: 10
```

### Collection Setup

```javascript
// Create collections with indexes
use monster

// Messages collection
db.createCollection("messages", {
  timeseries: {
    timeField: "timestamp",
    metaField: "topic",
    granularity: "seconds"
  }
});

// Sessions collection
db.sessions.createIndex({ clientId: 1 }, { unique: true });
db.sessions.createIndex({ lastSeen: 1 }, { expireAfterSeconds: 86400 });

// Retained messages
db.retained.createIndex({ topic: 1 }, { unique: true });

// Archive collection with TTL
db.archive.createIndex({ timestamp: 1 }, { expireAfterSeconds: 2592000 }); // 30 days
```

### Optimization

```javascript
// Enable sharding for horizontal scaling
sh.enableSharding("monster");
sh.shardCollection("monster.messages", { topic: "hashed" });

// Optimize for write performance
db.adminCommand({
  setParameter: 1,
  wiredTigerConcurrentWriteTransactions: 256
});

// Configure oplog size for replication
use local
db.runCommand({ replSetResizeOplog: 1, size: 10000 });
```

## SQLite

### Installation

SQLite is embedded and requires no installation. MonsterMQ will automatically create the database file.

### Configuration

```yaml
DefaultStoreType: SQLITE

SQLite:
  Path: "monstermq.db"  # Database file path
  CacheSize: 2000  # Page cache size in KB
  JournalMode: WAL  # Write-Ahead Logging for better concurrency
  Synchronous: NORMAL  # Durability vs performance trade-off
```

### Optimization

```sql
-- Enable WAL mode for better concurrency
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- Optimize for performance
PRAGMA cache_size = -64000;  -- 64MB cache
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;  -- 30GB memory-mapped I/O

-- Create indexes
CREATE INDEX idx_messages_topic ON messages(topic);
CREATE INDEX idx_messages_timestamp ON messages(timestamp);
CREATE INDEX idx_sessions_client ON sessions(client_id);

-- Regular maintenance
VACUUM;  -- Reclaim unused space
ANALYZE;  -- Update statistics
```

### Limitations

- **No clustering support** - SQLite is single-node only
- **Limited concurrency** - One writer at a time
- **Size limitations** - Performance degrades with very large databases
- **Best for development** - Not recommended for production clusters

## Migration Between Databases

### Export/Import Tools

```bash
# Export from PostgreSQL
pg_dump -U system -h localhost --data-only --column-inserts monster > export.sql

# Import to CrateDB (adjust SQL dialect)
crash < export.sql

# Export from MongoDB
mongoexport --db monster --collection messages --out messages.json

# Import to MongoDB
mongoimport --db monster --collection messages --file messages.json
```

### Using MonsterMQ Migration Tool

```bash
# Built-in migration (coming soon)
java -cp monster.jar at.rocworks.MigrateTool \
  --source POSTGRES --source-url jdbc:postgresql://localhost:5432/monster \
  --target MONGODB --target-url mongodb://localhost:27017/monster
```

## Performance Comparison

| Metric | PostgreSQL | CrateDB | MongoDB | SQLite |
|--------|------------|---------|---------|--------|
| **Write Throughput** | 50K msg/s | 100K msg/s | 75K msg/s | 10K msg/s |
| **Read Latency** | < 5ms | < 3ms | < 4ms | < 2ms |
| **Storage Efficiency** | Good | Excellent | Good | Fair |
| **Clustering** | Yes | Yes | Yes | No |
| **Time-Series** | Good | Excellent | Good | Fair |

## Monitoring

### PostgreSQL
```sql
-- Connection monitoring
SELECT datname, count(*) FROM pg_stat_activity GROUP BY datname;

-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC;
```

### CrateDB
```sql
-- Cluster health
SELECT name, status FROM sys.cluster;

-- Shard allocation
SELECT table_name, number_of_shards, number_of_replicas
FROM information_schema.tables WHERE table_schema = 'doc';
```

### MongoDB
```javascript
// Database stats
db.stats();

// Collection stats
db.messages.stats();

// Current operations
db.currentOp();
```

## Troubleshooting

### Common Issues

1. **Connection Pool Exhaustion**
   - Increase `MaxPoolSize` in configuration
   - Check for connection leaks in logs

2. **Slow Queries**
   - Add appropriate indexes
   - Run `ANALYZE` (PostgreSQL) or `db.collection.reIndex()` (MongoDB)

3. **Disk Space Issues**
   - Implement retention policies
   - Use `VACUUM` (PostgreSQL/SQLite) or compact (MongoDB)

4. **Replication Lag** (Clustered databases)
   - Check network latency
   - Tune replication settings
   - Consider increasing resources

## Best Practices

1. **Regular Backups** - Implement automated backup schedules
2. **Monitor Performance** - Use database-specific monitoring tools
3. **Index Optimization** - Create indexes for frequently queried fields
4. **Connection Pooling** - Configure appropriate pool sizes
5. **Retention Policies** - Implement data cleanup for old messages
6. **Security** - Use strong passwords and encryption
7. **Resource Planning** - Monitor and plan for growth