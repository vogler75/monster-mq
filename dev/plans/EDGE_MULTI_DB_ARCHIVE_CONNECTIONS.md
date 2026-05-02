# Edge Multi-DB Archive Connections

## Summary

Implement the Kotlin broker's named archive database connection feature in the Go edge broker.

Archive groups must be able to select one named Postgres or MongoDB connection. The selected
connection is used by both the last-value store and historical archive store when those stores use
Postgres or MongoDB. Existing YAML `Postgres` and `MongoDB` configuration remains supported as the
fixed read-only built-in default.

## Behavior

- Existing edge configurations keep working without changes.
- If an archive group has no selected connection, it uses the current YAML database configuration.
- YAML database blocks are exposed as one read-only default selection named `Default`.
- `Default` is returned only for a requested database type whose YAML backend is configured:
  - `POSTGRES` returns `Default` only when YAML `Postgres` is configured.
  - `MONGODB` returns `Default` only when YAML `MongoDB` is configured.
  - If the requested type is not configured in YAML, `Default` is not returned for that type.
- The config file must not support custom names for the standard/default archive database
  selection.
- Additional named connections are stored in the edge broker ConfigStore and managed through the
  API/dashboard.
- A selected connection applies to both last-value and archive storage.
- A selected connection is valid only when every Postgres/Mongo-backed store in the archive group
  uses the same database family.
- Reject archive groups that select one named connection while mixing Postgres and MongoDB storage.
- Secrets are write-only: passwords and credentialed URL parts must not be returned by API queries.

## Backend Implementation

Add shared types in the edge broker:

- `DatabaseConnectionType`: `POSTGRES`, `MONGODB`
- `DatabaseConnectionConfig`:
  - `name`
  - `type`
  - `url`
  - `username`
  - `password`
  - `database`
  - `schema`
  - `readOnly`
  - `createdAt`
  - `updatedAt`
- Add `databaseConnectionName` to the archive group model/config struct.

Extend the edge ConfigStore interface with:

- `ListDatabaseConnections(ctx, typeFilter)`
- `GetDatabaseConnection(ctx, name)`
- `SaveDatabaseConnection(ctx, connection)`
- `DeleteDatabaseConnection(ctx, name)`

Persist data in each ConfigStore backend:

- SQL backends: add a `databaseconnections` table.
- MongoDB backend: add a `databaseconnections` collection.
- Archive group persistence: add `database_connection_name` or equivalent JSON/BSON field.
- Migrations should be additive and tolerate existing databases.

Add runtime connection resolution:

- Build the normal database config from YAML first.
- If `databaseConnectionName` is empty, return that config unchanged.
- If it is `Default`, resolve from YAML and mark as read-only.
- Otherwise load the connection from ConfigStore.
- Validate selected connection type against archive group store types.
- Overlay the selected Postgres/MongoDB config before creating the last-value/archive stores.

## API And Dashboard

Expose API operations equivalent to the Kotlin broker GraphQL additions:

- Query/list database connection names, filtered by database type for UI selectors.
- Query/list full database connection metadata, optionally filtered by type.
- Query one database connection by name.
- Create database connection.
- Update database connection.
- Delete database connection.
- Extend archive group create/update/info payloads with `databaseConnectionName`.

API rules:

- The UI-facing list must support `DatabaseConnectionType` filtering.
- The UI-facing list must include `Default` only when that database type exists in YAML.
- Return sanitized URLs.
- Never return passwords.
- On update, missing or empty password keeps the existing secret.
- The built-in `Default` connection selection is read-only and cannot be updated or deleted.
- Deleting a named connection fails while any archive group references it.

Dashboard rules:

- Add a database connection management section.
- Show the YAML default as read-only and named `Default`.
- Allow creating/updating/deleting named Postgres/MongoDB connections.
- Add one `Database Connection` selector to archive group editing.
- Show the selector only when last-value or archive storage uses Postgres/MongoDB.
- Filter options to the required database family.

## Test Plan

Backend tests:

- Existing archive groups without `databaseConnectionName` still use YAML defaults.
- `Default` resolves from YAML and is read-only.
- Named Postgres connection is used by both Postgres last-value and Postgres archive stores.
- Named MongoDB connection is used by both MongoDB last-value and MongoDB archive stores.
- Wrong connection type is rejected.
- Mixed Postgres/MongoDB storage with one selected connection is rejected.
- Deleting an in-use connection is rejected.

Persistence tests:

- Create, read, update, delete a named connection in every supported ConfigStore backend.
- Archive group round-trips `databaseConnectionName`.
- Additive migrations work on databases that already contain archive group records.

API tests:

- Queries include the read-only YAML default as `Default`.
- Passwords are not returned.
- Credentialed URLs are sanitized.
- Updating a connection without a password preserves the previous password.
- The built-in `Default` selection cannot be edited or deleted.

Dashboard/build tests:

- Edge dashboard build passes.
- Archive group selector appears only for Postgres/MongoDB storage.
- Selector options are filtered by database family.
- Creating a connection refreshes the available archive group choices.

## Implementation Notes From Kotlin Broker

The Kotlin broker implementation added:

- `DatabaseConnectionType` and `DatabaseConnectionConfig` next to archive config store types.
- Default methods on `IArchiveConfigStore` for named connection CRUD.
- `databaseConnectionName` on `ArchiveGroup`.
- Connection resolution in `ArchiveHandler` before deploying archive groups.
- GraphQL `databaseConnections` / `databaseConnection` queries and grouped archive mutations.
- Dashboard connection management in Broker Configuration and one archive group selector.

Use the same behavior in Go, but follow the edge broker's package layout, API framework, and
storage abstractions rather than copying Kotlin structure literally.
