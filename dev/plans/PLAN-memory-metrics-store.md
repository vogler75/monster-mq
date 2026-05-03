# Plan: Volatile Metrics Store

## Summary

Flash-constrained edge devices should be able to keep live metrics and short
history without writing metric samples to SQLite/Postgres/MongoDB. Add an
independent metrics store selector under `Metrics`, defaulting to the current
persistent behavior.

## Configuration

Add:

```yaml
Metrics:
  Enabled: true
  StoreType: MEMORY
  CollectionIntervalSeconds: 1
  RetentionHours: 168
  MaxHistoryRows: 3600
```

Semantics:

- unset `StoreType`: current behavior, use the configured persistent backend.
- `MEMORY`: store metrics history in bounded RAM only.
- `NONE`: keep live snapshots from the collector, but do not store history.
- `SQLITE`, `POSTGRES`, `MONGODB`: explicit persistent backend; must match the
  broker's configured storage backend in this implementation.

## Implementation

- Add `MetricsConfig.StoreType` and `MetricsConfig.MaxHistoryRows`.
- Add `internal/stores/memory.MetricsStore`, implementing `stores.MetricsStore`.
- During broker startup, replace `storage.Metrics` with the configured metrics
  sink before constructing the metrics collector and GraphQL resolver.
- Guard metrics history resolvers for `nil` metrics stores so `NONE` returns an
  empty history instead of touching persistent storage.
- Update `config.yaml.example` and `yaml-json-schema.json`.

## Tests

- Unit-test memory metrics latest/history/purge/bounds.
- Integration-test `Metrics.StoreType: MEMORY` so metrics history works without
  persistent metrics writes.
- Integration-test `Metrics.StoreType: NONE` so live metrics still work and
  history is empty.
