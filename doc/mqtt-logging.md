# MQTT Logging Feature

## Overview

MonsterMQ now includes an advanced MQTT logging feature that captures all Java logging messages from across the broker system and publishes them to MQTT topics. This enables real-time log monitoring, centralized logging, and integration with external monitoring systems.

## Features

- **System-wide Log Capture**: Intercepts all Java logging messages from every component in MonsterMQ
- **Hierarchical Topic Structure**: Organizes logs by cluster node and log level
- **JSON Format**: Rich structured log messages with metadata
- **Configurable Log Level**: Set minimum log level for MQTT publishing
- **Loop Prevention**: Intelligent sender identification to prevent infinite loops
- **Real-time Streaming**: Live log monitoring via MQTT subscriptions

## Topic Structure

All log messages are published to topics following this pattern:

```
$SYS/logs/<node-id>/<level>
```

### Examples
- `$SYS/logs/node-001/info` - INFO level logs from node-001
- `$SYS/logs/node-002/error` - ERROR level logs from node-002
- `$SYS/logs/cluster-main/warning` - WARNING level logs from cluster-main

## Message Format

Each log message is published as JSON with the following structure:

```json
{
  "timestamp": "2025-10-15T09:45:23.123Z",
  "level": "INFO",
  "logger": "at.rocworks.handlers.SessionHandler",
  "message": "Client [mqtt-client-001] connected from 192.168.1.100",
  "thread": 12345,
  "node": "node-001",
  "sourceClass": "at.rocworks.handlers.SessionHandler",
  "sourceMethod": "handleClientConnect",
  "parameters": ["mqtt-client-001", "192.168.1.100"],
  "exception": {
    "class": "java.sql.SQLException",
    "message": "Connection timeout",
    "stackTrace": "java.sql.SQLException: Connection timeout\n\tat ..."
  }
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | String | ISO 8601 timestamp when log was generated |
| `level` | String | Log level (SEVERE, WARNING, INFO, FINE, etc.) |
| `logger` | String | Full name of the logger class |
| `message` | String | The log message text |
| `thread` | Number | Thread ID where log was generated |
| `node` | String | Cluster node identifier |
| `sourceClass` | String | Source class name (if available) |
| `sourceMethod` | String | Source method name (if available) |
| `parameters` | Array | Log message parameters (if any) |
| `exception` | Object | Exception details (if an exception was logged) |

## Configuration

Add the following section to your `config.yaml`:

```yaml
Logging:
  MqttEnabled: true    # Enable MQTT log publishing
  MqttLevel: INFO      # Minimum log level to publish
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MqttEnabled` | Boolean | `false` | Enable/disable MQTT log handler |
| `MqttLevel` | String | `INFO` | Minimum log level: ALL, FINEST, FINER, FINE, INFO, WARNING, SEVERE |

## Usage Examples

### 1. Real-time Log Monitoring

Subscribe to all logs from a specific node:
```bash
mosquitto_sub -h localhost -t '$SYS/logs/node-001/+'
```

Subscribe to error logs from all nodes:
```bash
mosquitto_sub -h localhost -t '$SYS/logs/+/severe'
```

Subscribe to all system logs:
```bash
mosquitto_sub -h localhost -t '$SYS/logs/+/+'
```

### 2. Log Level Filtering

Subscribe only to warnings and errors:
```bash
mosquitto_sub -h localhost -t '$SYS/logs/+/warning' &
mosquitto_sub -h localhost -t '$SYS/logs/+/severe' &
```

### 3. Application Integration

Use any MQTT client library to subscribe to logs programmatically:

```javascript
const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://localhost:1883');

client.subscribe('$SYS/logs/+/+');

client.on('message', (topic, message) => {
  const logEntry = JSON.parse(message.toString());
  console.log(`[${logEntry.level}] ${logEntry.logger}: ${logEntry.message}`);
  
  // Handle exceptions
  if (logEntry.exception) {
    console.error('Exception:', logEntry.exception.message);
  }
});
```

### 4. Log Aggregation

Forward logs to external systems like ELK Stack, Splunk, or Grafana:

```python
import paho.mqtt.client as mqtt
import json
import requests

def on_message(client, userdata, message):
    log_entry = json.loads(message.payload.decode())
    
    # Forward to Elasticsearch
    response = requests.post(
        'http://elasticsearch:9200/monstermq-logs/_doc',
        json=log_entry
    )

client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("$SYS/logs/+/+")
client.loop_forever()
```

## Monitoring Use Cases

### 1. System Health Monitoring
- Monitor ERROR and WARNING levels across all nodes
- Set up alerts for critical exceptions
- Track connection issues and performance problems

### 2. Debug and Troubleshooting
- Enable FINE or FINEST level for detailed debugging
- Monitor specific component logs during issue reproduction
- Analyze exception stack traces in real-time

### 3. Performance Analysis
- Track message processing times and rates
- Monitor database connection issues
- Analyze client connection patterns

### 4. Security Monitoring
- Monitor authentication failures
- Track unauthorized access attempts
- Analyze connection patterns for anomalies

## Performance Considerations

- **QoS 0**: Log messages use QoS 0 to avoid overwhelming the system
- **No Retention**: Log messages are not retained to prevent storage bloat
- **Level Filtering**: Use appropriate log levels to manage message volume
- **Loop Prevention**: Built-in sender identification prevents infinite loops

## Best Practices

1. **Production Environments**: Use INFO or WARNING level to reduce message volume
2. **Development/Debug**: Use FINE or FINEST for detailed troubleshooting
3. **Monitoring Setup**: Set up automated alerting for ERROR/SEVERE messages
4. **Log Rotation**: Implement external log rotation if storing messages long-term
5. **Performance**: Monitor MQTT broker performance when enabling detailed logging

## Integration with External Systems

The MQTT log feature integrates seamlessly with:

- **ELK Stack** (Elasticsearch, Logstash, Kibana)
- **Grafana** with MQTT data source
- **Splunk** with MQTT input
- **Prometheus** with MQTT exporter
- **Custom monitoring solutions** via MQTT clients

## Troubleshooting

### Log Handler Not Working
1. Check configuration: Ensure `MqttEnabled: true` in config.yaml
2. Verify log level: Make sure `MqttLevel` is appropriate
3. Check SessionHandler: Ensure broker is fully started
4. Monitor startup logs: Look for "MQTT log handler installed" message

### Missing Messages
1. Check log level filtering
2. Verify MQTT client subscriptions
3. Monitor for QoS 0 message drops under high load

### Performance Impact
1. Reduce log level (INFO instead of FINE)
2. Monitor broker CPU and memory usage
3. Consider external MQTT broker for high-volume logging

## Security Notes

- Log messages may contain sensitive information from error messages
- Consider network security for MQTT traffic containing logs
- Implement proper authentication for log topic access
- Be aware that connection details may appear in logs