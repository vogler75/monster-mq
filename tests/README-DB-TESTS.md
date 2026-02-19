# Database Backend Testing for MQTT v5 Phase 5

This directory contains scripts and configurations for testing MQTT v5.0 Phase 5 (Message Expiry) implementation across all supported database backends: PostgreSQL, CrateDB, and MongoDB.

## Quick Start

### Windows (PowerShell)

```powershell
# Run all database tests
.\tests\run_all_db_tests.ps1

# Keep databases running after tests
.\tests\run_all_db_tests.ps1 -KeepDatabases

# Use existing database containers
.\tests\run_all_db_tests.ps1 -SkipDockerStart
```

### Linux/macOS (Bash)

```bash
# Run all database tests
./tests/run_all_db_tests.sh

# Keep databases running after tests
KEEP_DATABASES=true ./tests/run_all_db_tests.sh

# Use existing database containers
SKIP_DOCKER_START=true ./tests/run_all_db_tests.sh
```

## What Gets Tested

The test runner executes the following workflow for **each database backend**:

1. **PostgreSQL** - Full relational database with ACID guarantees
2. **CrateDB** - Distributed SQL database for time-series data
3. **MongoDB** - Document-oriented NoSQL database

For each backend, the script:
- Configures the broker with the appropriate database connection
- Starts MonsterMQ broker
- Runs the Phase 5 message expiry test suite:
  - Test 1: Expired messages are NOT delivered
  - Test 2: Valid messages ARE delivered with correct remaining expiry
  - Test 3: Expiry interval updates correctly over time
  - Test 4: Messages without expiry are delivered regardless of time
- Validates message expiry purging and database operations
- Generates a detailed test report

## Files

### Docker Compose

- **`docker-compose-test.yml`** - Orchestrates all test databases
  - PostgreSQL 15 on port 5432
  - CrateDB 5.2 on ports 4200 (admin) and 5433 (postgres protocol)
  - MongoDB 7 on port 27017

### Broker Configurations

- **`broker/configs/test-postgres.yaml`** - PostgreSQL test config
- **`broker/configs/test-cratedb.yaml`** - CrateDB test config
- **`broker/configs/test-mongodb.yaml`** - MongoDB test config

Each configuration:
- Uses test database credentials
- Connects to localhost database containers
- Disables user authentication for testing
- Enables GraphQL API for monitoring

### Test Runners

- **`tests/run_all_db_tests.ps1`** - PowerShell test runner (Windows)
- **`tests/run_all_db_tests.sh`** - Bash test runner (Linux/macOS)

Both scripts provide identical functionality with color-coded output.

## Manual Testing

### Start Databases Only

```bash
# Start all test databases
docker-compose -f docker-compose-test.yml up -d

# Check status
docker-compose -f docker-compose-test.yml ps

# View logs
docker-compose -f docker-compose-test.yml logs -f

# Stop and clean up
docker-compose -f docker-compose-test.yml down -v
```

### Test Individual Database

```powershell
# Windows - PostgreSQL
Copy-Item broker\configs\test-postgres.yaml broker\config.yaml
cd broker
.\run.bat
# In another terminal:
cd tests
python test_mqtt5_phase5_message_expiry.py
```

```bash
# Linux - CrateDB
cp broker/configs/test-cratedb.yaml broker/config.yaml
cd broker
./run.sh
# In another terminal:
cd tests
python3 test_mqtt5_phase5_message_expiry.py
```

## Database Connection Details

### PostgreSQL
- **Host:** localhost:5432
- **Database:** monster_test
- **User:** testuser
- **Password:** testpass
- **Admin UI:** psql -h localhost -U testuser monster_test

### CrateDB
- **Host:** localhost:5433 (PostgreSQL wire protocol)
- **Database:** monster_test
- **User:** crate
- **Password:** (empty)
- **Admin UI:** http://localhost:4200

### MongoDB
- **Host:** localhost:27017
- **Database:** monster_test
- **User:** testuser
- **Password:** testpass
- **Admin CLI:** mongosh --host localhost --username testuser --password testpass --authenticationDatabase admin

## Interpreting Results

### Success Output

```
======================================================================
TEST SUMMARY
======================================================================
PostgreSQL : ✓ PASSED (23.45s)
CrateDB : ✓ PASSED (24.12s)
MongoDB : ✓ PASSED (22.87s)

✓ ALL DATABASE TESTS PASSED!

======================================================================
✓✓✓ PHASE 5 VALIDATION COMPLETE ✓✓✓
======================================================================
ℹ PostgreSQL: VALIDATED ✓
ℹ CrateDB: VALIDATED ✓
ℹ MongoDB: VALIDATED ✓
```

### Failure Output

If a test fails, the output will show:
- Which database backend failed
- Detailed error messages from the test script
- Broker logs (if available)
- Specific test case that failed

## Troubleshooting

### Databases Won't Start

```bash
# Check Docker is running
docker ps

# Check container logs
docker logs monstermq-test-postgres
docker logs monstermq-test-cratedb
docker logs monstermq-test-mongodb

# Force cleanup and retry
docker-compose -f docker-compose-test.yml down -v
docker-compose -f docker-compose-test.yml up -d
```

### Broker Won't Start

1. Check database connectivity
2. Verify config file syntax
3. Ensure no other broker is running on port 1883
4. Check broker logs in `broker/` directory

### Tests Timeout

- Increase sleep delays in test runner scripts
- Check broker logs for connection issues
- Verify MQTT client library (paho-mqtt >= 2.0.0)

### Port Conflicts

If ports 5432, 5433, or 27017 are already in use:
1. Stop conflicting services
2. Or modify ports in `docker-compose-test.yml` and config files

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test All Databases

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install paho-mqtt
      
      - name: Run database tests
        run: ./tests/run_all_db_tests.sh
```

## Performance Notes

- **PostgreSQL**: Best for transactional consistency
- **CrateDB**: Best for time-series and analytics workloads
- **MongoDB**: Best for flexible schema and horizontal scaling

All three backends should pass Phase 5 tests with identical behavior.

## Next Steps

After all tests pass:
- Update [MQTT5_IMPLEMENTATION_PLAN.md](../MQTT5_IMPLEMENTATION_PLAN.md) Phase 5 TODO items
- Mark PostgreSQL, CrateDB, and MongoDB as VALIDATED ✓
- Consider performance benchmarking with large message volumes
- Test message expiry under load conditions

## Support

For issues or questions:
- Check broker logs in `broker/` directory
- Review database container logs
- Consult main documentation in `doc/` directory
