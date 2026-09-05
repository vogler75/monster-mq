# System Logging: Migration from MQTT

The former MQTT log publisher has been removed. `Logging.MqttEnabled` and
`Logging.MqttLevel` are no longer read, and the broker does not publish logs on
`$SYS/syslogs/<node>/<level>`.

Enable system log capture with:

```yaml
Logging:
  Memory:
    Enabled: true
    Entries: 1000
```

Read recent entries through the dashboard log viewer or the GraphQL `systemLogs`
query. For live logs, use the [GraphQL system logs subscription](graphql-system-logs.md).
Log events travel over a dedicated internal event-bus address; they do not require
MQTT subscriptions or an archive group.

Logger levels control which records are emitted. Use `./run.sh -- -log FINE`
for a temporary debug override, or manage logger settings through the dashboard.
The in-memory buffer is bounded and is lost when the process stops.

This filename is retained so existing MQTT-logging links lead to the supported
replacement. See [Monitoring](monitoring.md) for MQTT broker performance topics.
