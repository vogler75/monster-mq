# Grafana Integration

MonsterMQ provides a Grafana JSON Data Source API that allows you to visualize MQTT time-series data directly in Grafana dashboards.

## Prerequisites

Install the Grafana JSON Data Source plugin:
- Plugin ID: `simpod-json-datasource`
- Installation: `grafana-cli plugins install simpod-json-datasource`
- Or install via Grafana UI: Configuration → Plugins → Search "JSON"

## Enabling the Server

Configure the Grafana server in your `config.yaml`:

```yaml
Grafana:
  Enabled: true
  Port: 3001
  DefaultArchiveGroup: SCADA  # Used when X-Archive-Group header is not set
```

## Archive Group Selection

The archive group is specified via the `X-Archive-Group` HTTP header. This allows you to configure different Grafana data sources for different archive groups.

If the header is not set, the `DefaultArchiveGroup` from configuration is used.

## Configuring Grafana Data Source

1. Go to **Configuration → Data Sources → Add data source**
2. Search for and select **JSON**
3. Configure the data source:
   - **Name**: MonsterMQ (or any descriptive name)
   - **URL**: `http://localhost:3001/api/grafana`
4. Under **Custom HTTP Headers**, add:
   - **Header**: `X-Archive-Group`
   - **Value**: `SCADA` (or your archive group name)
5. Click **Save & Test**

## Authentication

### No Authentication

If user management is disabled in MonsterMQ, no authentication is required.

### Basic Authentication

When user management is enabled, configure Basic Auth in Grafana:

1. In the data source settings, enable **Basic auth**
2. Enter your MonsterMQ username and password
3. Grafana will send these credentials with each request

### JWT Authentication

Alternatively, use a JWT token:

1. Obtain a JWT token via the GraphQL `login` mutation
2. In Grafana, under **Custom HTTP Headers**, add:
   - **Header**: `Authorization`
   - **Value**: `Bearer <your-jwt-token>`

## Creating Panels

### Adding a Time Series Panel

1. Create a new panel and select **Time series** visualization
2. In the query editor:
   - **Metric**: Select a topic from the dropdown (e.g., `SCADA/Sensor/Temperature`)
   - **JSON Field**: If the payload is JSON, specify the field path (e.g., `Value` or `data.temperature`)
   - **Aggregation**: Select the aggregation function (AVG, MIN, MAX, SUM, COUNT)
   - **Interval**: Select the time bucket size:
     - **Auto**: Grafana calculates based on time range and panel width
     - **None (Raw)**: No aggregation, returns raw data points
     - **1 Minute**, **5 Minutes**, **15 Minutes**, **1 Hour**, **1 Day**: Fixed intervals

### Example: Temperature Sensor

For a topic `SCADA/Original/Meter_Input/WattAct` with JSON payload `{"Value": 123.45}`:

1. **Metric**: `SCADA/Original/Meter_Input/WattAct`
2. **JSON Field**: `Value`
3. **Aggregation**: `AVG`
4. **Interval**: `Auto`

### Raw Data Mode

Select **None (Raw)** for the interval to get unaggregated data points. This is useful for:
- High-resolution data analysis
- Debugging data quality
- Small time ranges where aggregation isn't needed

Note: Raw mode has a limit of 10,000 data points per query.

## API Endpoints

The Grafana server exposes the following endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/grafana/` | GET | Health check |
| `/api/grafana/metrics` | POST | List available topics with payload options |
| `/api/grafana/metric-payload-options` | POST | Dynamic payload options |
| `/api/grafana/search` | POST | List topics (legacy) |
| `/api/grafana/query` | POST | Query time-series data |

## Testing with curl

### Health Check

```bash
curl http://localhost:3001/api/grafana/
```

### List Available Metrics

```bash
curl -X POST http://localhost:3001/api/grafana/metrics \
  -H "Content-Type: application/json" \
  -H "X-Archive-Group: SCADA" \
  -d '{}'
```

### Query Data

```bash
curl -X POST http://localhost:3001/api/grafana/query \
  -H "Content-Type: application/json" \
  -H "X-Archive-Group: SCADA" \
  -d '{
    "range": {
      "from": "2024-01-01T00:00:00Z",
      "to": "2024-01-01T01:00:00Z"
    },
    "intervalMs": 60000,
    "targets": [
      {
        "target": "SCADA/Original/Meter_Input/WattAct",
        "refId": "A",
        "payload": {
          "field": "Value",
          "function": "AVG",
          "interval": "auto"
        }
      }
    ]
  }'
```

### Query Raw Data

```bash
curl -X POST http://localhost:3001/api/grafana/query \
  -H "Content-Type: application/json" \
  -H "X-Archive-Group: SCADA" \
  -d '{
    "range": {
      "from": "2024-01-01T00:00:00Z",
      "to": "2024-01-01T00:10:00Z"
    },
    "targets": [
      {
        "target": "SCADA/Original/Meter_Input/WattAct",
        "refId": "A",
        "payload": {
          "field": "Value",
          "interval": "none"
        }
      }
    ]
  }'
```

## Payload Options

### JSON Field

Path to extract a numeric value from JSON payloads. Supports nested paths using dot notation:
- `Value` - Top-level field
- `data.temperature` - Nested field
- `sensor.readings.value` - Deeply nested field

Leave empty if the payload is a plain numeric value.

### Aggregation Functions

| Function | Description |
|----------|-------------|
| AVG | Average of values in the time bucket |
| MIN | Minimum value in the time bucket |
| MAX | Maximum value in the time bucket |
| SUM | Sum of values in the time bucket |
| COUNT | Number of values in the time bucket |

### Interval Options

| Option | Description |
|--------|-------------|
| Auto | Grafana determines optimal bucket size |
| None | No aggregation, return raw data points |
| 1 Minute | 1-minute time buckets |
| 5 Minutes | 5-minute time buckets |
| 15 Minutes | 15-minute time buckets |
| 1 Hour | 1-hour time buckets |
| 1 Day | 1-day time buckets |

## Multiple Archive Groups

To visualize data from multiple archive groups, create separate Grafana data sources:

1. **MonsterMQ-SCADA**
   - URL: `http://localhost:3001/api/grafana`
   - Custom Header: `X-Archive-Group: SCADA`

2. **MonsterMQ-Production**
   - URL: `http://localhost:3001/api/grafana`
   - Custom Header: `X-Archive-Group: Production`

Then select the appropriate data source when creating panels.

## Troubleshooting

### No Topics in Dropdown

- Verify the archive group has a `lastValStore` configured
- Check that topics have been published to the archive group
- Ensure the `X-Archive-Group` header is set correctly

### Empty Query Results

- Verify the archive group has an `archiveStore` configured
- Check the time range covers when data was published
- Ensure the JSON field path is correct
- Enable debug logging with `-log FINE` to see query details

### Authentication Errors

- Verify credentials are correct
- Check that the user is enabled
- Ensure Basic Auth is properly configured in Grafana

### CORS Errors

The server includes CORS headers for all origins. If you still encounter CORS issues:
- Verify the request includes the correct `Content-Type: application/json` header
- Check that `X-Archive-Group` is in the allowed headers list

## See Also

- [Archiving](archiving.md) - Configure archive groups and stores
- [GraphQL API](graphql.md) - Alternative API for querying data
- [Configuration](configuration.md) - Full configuration reference
