# QuestDB Bulk Insert Test

This is a standalone test program to validate QuestDB bulk insert functionality using the same approach as MonsterMQ's PostgreSQLLogger.

## Purpose
This test program isolates the bulk insert logic to help QuestDB developers diagnose any issues with batch inserts via the PostgreSQL wire protocol.

## Requirements
- Java 17 or higher
- Maven (uses system installation)
- QuestDB server

## Setup QuestDB

### Option 1: Using Docker (Recommended)
```bash
docker run -p 9000:9000 -p 8812:8812 questdb/questdb:8.0.3
```

### Option 2: Download and Run QuestDB
1. Download QuestDB from https://questdb.io/get-questdb/
2. Extract and run:
   ```bash
   ./questdb.sh start
   ```

QuestDB will be available at:
- Web console: http://localhost:9000
- PostgreSQL wire protocol: localhost:8812

## Test Table Schema
The test creates and uses this table:
```sql
CREATE TABLE 'monstermq1' ( 
    ts TIMESTAMP,
    info STRING,
    value DOUBLE
) timestamp(ts) PARTITION BY DAY WAL
```

## Configuration

### Command Line Arguments
The program accepts optional command line arguments:
```bash
./run.sh [host] [port] [database] [batches]
```

Examples:
```bash
# Use defaults (localhost:8812/qdb, 10 batches)
./run.sh

# Custom host only
./run.sh 192.168.1.100

# Custom host and port  
./run.sh 192.168.1.100 9999

# Custom host, port, and database
./run.sh 192.168.1.100 9999 mydb

# All custom settings including number of batches
./run.sh linux5 8812 qdb 50
./run.sh localhost 8812 qdb 100
```

### Other Configuration
Edit `src/main/kotlin/QuestDBTest.kt` to modify these constants:
- `BATCH_SIZE = 500` - Number of rows per batch (500 rows × batches = total rows)
- `USERNAME/PASSWORD` - Credentials (default: admin/quest)

The number of batches is now configurable via command line argument (4th parameter).

## Running the Test

1. Start QuestDB (see setup above)
2. Run the test:
   ```bash
   ./run.sh
   ```

The test will:
- Connect to QuestDB using PostgreSQL wire protocol (port 8812)
- Create a test table with sample data structure
- Insert records using bulk operations  
- Measure and report performance metrics

## Troubleshooting

If you get "Connection refused":
1. Ensure QuestDB is running on port 8812
2. Check if QuestDB web console is accessible at http://localhost:9000
3. Try starting QuestDB with Docker: `docker run -p 9000:9000 -p 8812:8812 questdb/questdb:8.0.3`

If you get build errors:
1. Ensure Maven is installed: `mvn --version`
2. Ensure Java 17+ is available: `java --version`

## What the Test Does
1. Connects to QuestDB using PostgreSQL JDBC driver
2. Creates the test table if it doesn't exist
3. Generates test data (timestamps, strings, doubles)
4. Performs bulk inserts in batches of 500 rows
5. Measures and reports performance metrics

## Expected Output
```
QuestDB Bulk Insert Test
========================
Connecting to QuestDB: jdbc:postgresql://localhost:8812/qdb
Connected to QuestDB successfully
Creating table if not exists...
Starting bulk insert test...
Batch size: 500
Number of batches: 10
Total rows: 5000

--- Batch 1/10 ---
Writing bulk of 500 rows to table monstermq1
Successfully wrote 500 rows in 45ms

[... more batches ...]

=== Test Results ===
Total time: 1234ms
Total rows: 5000
Rows per second: 4051.86
```

## Troubleshooting

### Connection Issues
- Ensure QuestDB is running: `docker run -p 9000:9000 -p 8812:8812 questdb/questdb`
- Check QuestDB logs for connection attempts
- Verify PostgreSQL wire protocol is enabled in QuestDB config

### Performance Issues
- Compare results with PostgreSQL using the same test
- Check QuestDB WAL settings and commit frequency
- Monitor QuestDB resource usage during test

### SQL Errors
- Check table creation permissions
- Verify data types are supported
- Review QuestDB compatibility with PostgreSQL JDBC driver

## Files
- `QuestDBTest.kt` - Main test program
- `build.gradle.kts` - Gradle build configuration
- `run.sh` - Convenience runner script
- `README.md` - This documentation

## Modifications from Original
This test program uses the exact same bulk insert pattern as MonsterMQ's `PostgreSQLLogger.kt`:
- Same prepared statement approach
- Same parameter binding logic  
- Same batch execution method
- Same error handling patterns

The only differences are:
- Simplified data model (3 columns vs dynamic schema)
- Hardcoded connection parameters
- Added performance measurement
- Removed Vertx async handling for simplicity