# Snowflake JDBC Logger Configuration

This guide explains how to configure and use the Snowflake JDBC Logger in MonsterMQ to write MQTT messages to Snowflake using private key authentication.

## Overview

The Snowflake JDBC Logger uses the Snowflake JDBC Thin Driver to connect to Snowflake and write MQTT messages to tables using standard JDBC batch inserts. It supports private key (JWT) authentication for secure, password-free connections.

## Prerequisites

1. **Snowflake Account**: Active Snowflake account with appropriate permissions
2. **Warehouse**: A Snowflake warehouse for query execution
3. **Database & Schema**: Target database and schema created in Snowflake
4. **RSA Key Pair**: Private key for authentication (see generation steps below)
5. **User Configuration**: Snowflake user configured with the public key

## Configuration Fields

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| **JDBC URL** | Full Snowflake JDBC connection URL | `jdbc:snowflake://xy12345.eu-central-1.snowflakecomputing.com` or `jdbc:snowflake://MYORG-MYACCOUNT.snowflakecomputing.com` |
| **Account** | Snowflake account identifier | `MYORG-MYACCOUNT` or `xy12345` |
| **Username** | Snowflake username for authentication | `mqtt_logger_user` |
| **Private Key File** | Path to RSA private key in PKCS#8 (.p8) format | `/etc/snowflake/keys/rsa_key.p8` |
| **Warehouse** | Snowflake warehouse for query execution | `COMPUTE_WH` |
| **Database** | Target database name | `SCADA` or `IOT_DATA` |
| **Schema** | Target schema name | `PUBLIC` or `SENSORS` |

### Optional Fields

| Field | Description | Default | Example |
|-------|-------------|---------|---------|
| **Role** | Snowflake role to use | `ACCOUNTADMIN` | `DATA_ENGINEER` |
| **Topic Name Column** | Column name for storing MQTT topic | (empty) | `mqtt_topic` |
| **Auto Create Table** | Automatically create table if not exists | `true` | `true` or `false` |

## Generating RSA Key Pair

### Step 1: Generate Private Key

```bash
# Generate RSA private key in PKCS#8 format
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

### Step 2: Extract Public Key

```bash
# Extract public key from private key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Step 3: Get Public Key Content

```bash
# Display public key (copy everything EXCEPT the BEGIN/END lines)
cat rsa_key.pub | grep -v "BEGIN PUBLIC KEY" | grep -v "END PUBLIC KEY" | tr -d '\n'
```

### Step 4: Configure Snowflake User

In Snowflake SQL worksheet, assign the public key to your user:

```sql
-- Set the RSA public key for the user
ALTER USER mqtt_logger_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';

-- Verify the key is set
DESC USER mqtt_logger_user;

-- Grant necessary privileges
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ROLE;
GRANT USAGE ON DATABASE SCADA TO ROLE DATA_ROLE;
GRANT USAGE ON SCHEMA SCADA.PUBLIC TO ROLE DATA_ROLE;
GRANT INSERT ON ALL TABLES IN SCHEMA SCADA.PUBLIC TO ROLE DATA_ROLE;
GRANT ROLE DATA_ROLE TO USER mqtt_logger_user;
```

## Example Configurations

### Organization Account Format

```yaml
JDBC URL: jdbc:snowflake://MYORG-MYACCOUNT.snowflakecomputing.com
Account: MYORG-MYACCOUNT
Username: mqtt_logger_user
Private Key File: /etc/snowflake/keys/rsa_key.p8
Warehouse: COMPUTE_WH
Database: SCADA
Schema: PUBLIC
Role: (leave empty, defaults to ACCOUNTADMIN)
```

### Regional Account Format

```yaml
JDBC URL: jdbc:snowflake://xy12345.eu-central-1.snowflakecomputing.com
Account: xy12345
Username: mqtt_logger_user
Private Key File: /home/user/snowflake_rsa.p8
Warehouse: MQTT_WAREHOUSE
Database: IOT_DATA
Schema: SENSORS
Role: DATA_ENGINEER
```

### Account with Port Specified

```yaml
JDBC URL: jdbc:snowflake://xy12345.eu-central-1.snowflakecomputing.com:443
Account: xy12345
Username: mqtt_logger_user
Private Key File: /etc/snowflake/keys/rsa_key.p8
Warehouse: COMPUTE_WH
Database: SCADA
Schema: PUBLIC
```

## Table Creation

The Snowflake logger can automatically create tables based on your JSON schema if the `autoCreateTable` option is enabled (enabled by default). Alternatively, you can pre-create tables manually in Snowflake.

### Automatic Table Creation

When `autoCreateTable` is enabled and a fixed `tableName` is specified (not using `tableNameJsonPath`), the logger will:

1. **Extract field definitions** from the JSON schema `properties`
2. **Create the table** with `CREATE TABLE IF NOT EXISTS`
3. **Add clustering key** on the first timestamp field for better query performance

Field types are mapped from JSON schema to Snowflake types:
- `format: "timestamp"` or `format: "timestampms"` → `TIMESTAMP_NTZ`
- `type: "string"` → `VARCHAR`
- `type: "number"` → `DOUBLE`
- `type: "integer"` → `BIGINT`
- `type: "boolean"` → `BOOLEAN`
- Other types → `VARIANT`

**Note:** All field names are automatically converted to UPPERCASE and quoted to match Snowflake conventions.

### Manual Table Creation

If you prefer to create tables manually, or need custom column types, based on a JSON schema for sensor data:

```sql
CREATE TABLE "SCADA"."PUBLIC"."SENSOR_DATA" (
    "TIMESTAMP" TIMESTAMP_NTZ,
    "SENSOR_ID" VARCHAR,
    "TEMPERATURE" DOUBLE,
    "PRESSURE" DOUBLE,
    "STATUS" VARCHAR
);

-- Optional: Add clustering for better query performance
ALTER TABLE "SCADA"."PUBLIC"."SENSOR_DATA"
CLUSTER BY ("TIMESTAMP");
```

**Note:** Snowflake uses uppercase identifiers by default. The logger automatically converts field names to uppercase and quotes them.

## JSON Schema Examples

### Example 1: Direct Field Mapping (No Transformation)

When incoming JSON field names match your desired column names:

**Incoming Payload:**
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "sensor_id": "temp-01",
  "temperature": 23.5,
  "pressure": 1013.2,
  "status": "OK"
}
```

**JSON Schema:**
```json
{
  "type": "object",
  "properties": {
    "timestamp": {
      "type": "string",
      "format": "timestamp"
    },
    "sensor_id": {
      "type": "string"
    },
    "temperature": {
      "type": "number"
    },
    "pressure": {
      "type": "number"
    },
    "status": {
      "type": "string"
    }
  },
  "required": ["timestamp", "sensor_id"]
}
```

**Result:** Table columns `TIMESTAMP`, `SENSOR_ID`, `TEMPERATURE`, `PRESSURE`, `STATUS`

### Example 2: Field Transformation with Mapping

When incoming JSON field names differ from desired column names:

**Incoming Payload:**
```json
{
  "TimeMS": 1705315800000,
  "SensorID": "temp-01",
  "Value": 23.5,
  "Quality": "Good"
}
```

**JSON Schema with Mapping:**
```json
{
  "type": "object",
  "properties": {
    "ts": {
      "type": "number",
      "format": "timestampms"
    },
    "sensor_id": {
      "type": "string"
    },
    "value": {
      "type": "number"
    },
    "quality": {
      "type": "string"
    }
  },
  "mapping": {
    "ts": "$.TimeMS",
    "sensor_id": "$.SensorID",
    "value": "$.Value",
    "quality": "$.Quality"
  },
  "required": ["sensor_id", "value"]
}
```

**Result:** Table columns `TS`, `SENSOR_ID`, `VALUE`, `QUALITY`

**How it works:**
- `properties` defines target database column names
- `mapping` defines JSONPath expressions to extract from source payload
- When `mapping` exists, JSON Schema validation is skipped
- `required` validates that extracted values are not null

### Example 3: Nested Field Extraction

**Incoming Payload:**
```json
{
  "device": {
    "id": "sensor-01",
    "location": "warehouse-a"
  },
  "reading": {
    "timestamp": 1705315800000,
    "value": 23.5,
    "unit": "celsius"
  }
}
```

**JSON Schema:**
```json
{
  "type": "object",
  "properties": {
    "ts": {
      "type": "number",
      "format": "timestampms"
    },
    "device_id": {
      "type": "string"
    },
    "location": {
      "type": "string"
    },
    "temperature": {
      "type": "number"
    },
    "unit": {
      "type": "string"
    }
  },
  "mapping": {
    "ts": "$.reading.timestamp",
    "device_id": "$.device.id",
    "location": "$.device.location",
    "temperature": "$.reading.value",
    "unit": "$.reading.unit"
  },
  "required": ["device_id", "temperature"]
}
```

**Result:** Table columns `TS`, `DEVICE_ID`, `LOCATION`, `TEMPERATURE`, `UNIT`

### Example 4: Including MQTT Topic Name

To track which MQTT topic each record came from, use the `topicNameColumn` configuration:

**Configuration:**
```yaml
Topic Name Column: mqtt_topic
```

**JSON Schema:**
```json
{
  "type": "object",
  "properties": {
    "ts": {
      "type": "number",
      "format": "timestampms"
    },
    "value": {
      "type": "number"
    }
  },
  "mapping": {
    "ts": "$.TimeMS",
    "value": "$.Value"
  },
  "required": ["value"]
}
```

**Incoming Message:**
- **Topic:** `factory/line1/temperature`
- **Payload:** `{"TimeMS": 1705315800000, "Value": 23.5}`

**Result:**
- Table columns: `TS`, `VALUE`, `MQTT_TOPIC`
- Inserted row: `(1705315800000, 23.5, 'factory/line1/temperature')`

**When auto-create is enabled:**
```sql
CREATE TABLE IF NOT EXISTS "SCADA"."PUBLIC"."SENSOR_DATA" (
    "TS" TIMESTAMP_NTZ,
    "VALUE" DOUBLE,
    "MQTT_TOPIC" VARCHAR
)
```

**Notes:**
- Topic column is added automatically to CREATE TABLE statements
- Topic column is added automatically to INSERT statements
- Leave `topicNameColumn` empty to exclude topic from database
- Topic column type is VARCHAR in Snowflake

## JDBC Connection Properties

When configured, the logger creates a JDBC connection with these properties:

```properties
user=mqtt_logger_user
account=MYORG-MYACCOUNT
role=ACCOUNTADMIN
db=SCADA
schema=PUBLIC
warehouse=COMPUTE_WH
authenticator=snowflake_jwt
privateKey=<base64-encoded-private-key>
ssl=on
```

**Note:** The `account` property is set from the Account field in the configuration.

## Troubleshooting

### Authentication Failed

**Error:** `JWT token is invalid` or `Authentication failed`

**Solutions:**
1. Verify the public key is correctly configured in Snowflake:
   ```sql
   DESC USER mqtt_logger_user;
   ```
2. Ensure the private key file is readable by the MonsterMQ process
3. Verify the private key is in PKCS#8 format (not PKCS#1)

### Table Does Not Exist

**Error:** `Table 'SENSOR_DATA' does not exist`

**Solutions:**
1. Create the table in Snowflake first (see Table Creation section)
2. Verify database and schema names match exactly (case-sensitive in quotes)
3. Check the user has INSERT privileges on the table

### Warehouse Not Found

**Error:** `Warehouse 'COMPUTE_WH' does not exist`

**Solutions:**
1. Create the warehouse in Snowflake:
   ```sql
   CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE='X-SMALL';
   ```
2. Grant usage permission:
   ```sql
   GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ROLE;
   ```

### Connection Timeout

**Error:** `Connection timeout` or `Network unreachable`

**Solutions:**
1. Verify the JDBC URL is correct and accessible
2. Check network connectivity to Snowflake
3. Verify firewall rules allow outbound HTTPS (port 443)
4. Test connection manually by extracting the hostname from JDBC URL:
   ```bash
   # Extract hostname from jdbc:snowflake://hostname
   curl https://MYORG-MYACCOUNT.snowflakecomputing.com
   ```

### Private Key File Not Found

**Error:** `Private key file not found: /path/to/key.p8`

**Solutions:**
1. Verify the file path is absolute (not relative)
2. Check file permissions (must be readable)
3. Verify the file exists:
   ```bash
   ls -la /path/to/key.p8
   ```

## Performance Tuning

### Warehouse Size

Choose appropriate warehouse size based on ingestion rate:

| Ingestion Rate | Warehouse Size | Recommended |
|----------------|----------------|-------------|
| < 1000 msg/s | X-SMALL | Testing/Development |
| 1000-5000 msg/s | SMALL | Light Production |
| 5000-20000 msg/s | MEDIUM | Production |
| > 20000 msg/s | LARGE or X-LARGE | High Volume |

### Batch Size Configuration

Configure bulk write settings for optimal performance:

```yaml
bulkSize: 1000           # Trigger write every 1000 messages
bulkTimeoutMs: 5000      # Or every 5 seconds, whichever comes first
queueSize: 10000         # Buffer up to 10000 messages in memory
```

### Clustering Keys

For time-series data, add clustering on timestamp column:

```sql
ALTER TABLE "SCADA"."PUBLIC"."SENSOR_DATA"
CLUSTER BY ("TIMESTAMP");
```

## Security Best Practices

1. **Private Key Storage**
   - Store private keys in secure locations with restricted permissions
   - Use file permissions: `chmod 600 /path/to/rsa_key.p8`
   - Never commit private keys to version control

2. **User Permissions**
   - Use least-privilege principle
   - Create dedicated role for MQTT logger
   - Grant only INSERT permissions, not DELETE/UPDATE

3. **Network Security**
   - Use SSL for connections (enabled by default)
   - Restrict network access to Snowflake IPs only
   - Use Snowflake network policies if available

4. **Key Rotation**
   - Rotate RSA key pairs periodically
   - Keep backup of old keys during rotation
   - Update Snowflake user configuration with new public key

## Additional Resources

- [Snowflake JDBC Driver Documentation](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc)
- [Snowflake Key Pair Authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- [Snowflake SQL Reference](https://docs.snowflake.com/en/sql-reference)
- [MonsterMQ JDBC Logger Documentation](../README.md)

## Support

For issues specific to:
- **Snowflake connectivity**: Check Snowflake documentation and support
- **MonsterMQ logger**: Create an issue in the MonsterMQ repository
- **JDBC driver**: See [Snowflake JDBC GitHub](https://github.com/snowflakedb/snowflake-jdbc)
