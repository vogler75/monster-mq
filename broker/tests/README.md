# Flow Engine Test Scripts

This directory contains test scripts for the MonsterMQ Flow Engine.

## Prerequisites

- MonsterMQ broker running with GraphQL enabled (port 8080)
- `curl` command-line tool
- `jq` for JSON parsing
- `mosquitto_pub` and `mosquitto_sub` for MQTT testing (optional)

## Scripts

### flow-cleanup.sh

Removes all flow instances and flow classes from the system.

**Usage:**
```bash
./tests/flow-cleanup.sh
```

**What it does:**
1. Queries all flow instances and deletes them
2. Queries all flow classes and deletes them

### flow-test-combine-inputs.sh

Creates a test flow that combines two MQTT topic inputs into a single JSON object.

**Usage:**
```bash
./tests/flow-test-combine-inputs.sh
```

**What it creates:**
- **Flow Class**: `CombineInputsFlow`
  - A function node with two inputs (`input1`, `input2`)
  - JavaScript code that waits for both inputs
  - Combines both values into a JSON object with timestamp
  - Sends combined JSON to one output

- **Flow Instance**: `combine-test-instance`
  - Subscribes to `test/input1` → node input1
  - Subscribes to `test/input2` → node input2
  - Publishes combined result to `test/combined/output`

**Testing the flow:**

1. Run the script to create the flow:
   ```bash
   ./tests/flow-test-combine-inputs.sh
   ```

2. Subscribe to the output topic:
   ```bash
   mosquitto_sub -h localhost -t test/combined/output
   ```

3. Publish to input topics:
   ```bash
   mosquitto_pub -h localhost -t test/input1 -m '{"temperature": 23.5}'
   mosquitto_pub -h localhost -t test/input2 -m '{"humidity": 65}'
   ```

4. You should see the combined output:
   ```json
   {
     "input1": {"temperature": 23.5},
     "input2": {"humidity": 65},
     "timestamp": 1728745623456,
     "combined_at": "2024-10-12T12:13:43.456Z"
   }
   ```

## GraphQL Endpoint

All scripts use the GraphQL endpoint at `http://localhost:8080/graphql`.

If your GraphQL server is running on a different host or port, modify the `GRAPHQL_URL` variable in each script.

## Troubleshooting

### "jq: command not found"

Install jq:
- macOS: `brew install jq`
- Ubuntu: `sudo apt-get install jq`
- Windows: Download from https://stedolan.github.io/jq/

### "Connection refused"

Make sure the MonsterMQ broker is running with GraphQL enabled:

```yaml
# config.yaml
GraphQL:
  Enabled: true
  Port: 8080
  Path: /graphql
```

### Flow instance not processing messages

1. Check that the flow instance is enabled:
   ```bash
   curl -X POST http://localhost:8080/graphql \
     -H "Content-Type: application/json" \
     -d '{"query": "query { flowInstance(name: \"combine-test-instance\") { enabled status { running } } }"}'
   ```

2. Check the broker logs for any errors

3. Make sure you're running on GraalVM JDK (JavaScript support is built-in)
