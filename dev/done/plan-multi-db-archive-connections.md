# Edge Multi-DB Archive Connections

## Summary

Implement the Kotlin broker's named archive database connection feature in the Go edge broker.

Archive groups must be able to select one named Postgres or MongoDB connection. The selected
connection is used by both the last-value store and historical archive store when those stores use
Postgres or MongoDB. Existing YAML `Postgres` and `MongoDB` configuration remains supported as the
fixed read-only built-in default named `Default` in GraphQL responses.

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
  GraphQL API.
- A selected connection applies to both last-value and archive storage.
- A selected connection is valid only when every Postgres/Mongo-backed store in the archive group
  uses the same database family.
- Archive groups that mix Postgres and MongoDB storage must leave `databaseConnectionName` empty.
  Empty means the runtime uses the YAML `Postgres` block for Postgres stores and the YAML `MongoDB`
  block for MongoDB stores.
- Reject archive groups that select a non-default named connection while mixing Postgres and
  MongoDB storage. Accepting `Default` for mixed storage is optional compatibility, but it should
  resolve exactly like an empty selection and should not persist as a custom default name.
- Secrets are write-only: passwords and credentialed URL parts must not be returned by API queries.
- MongoDB URL credentials must be normalized when a named connection provides `username` and
  `password`: if the URL already contains userinfo, replace that userinfo with the stored
  `username:password` pair. This prevents URLs such as `mongodb://user@host:27017` from reaching
  the MongoDB driver with a username but no password.

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
- If it is `Default`, resolve from YAML and mark as read-only for single-family selections.
- If it is `Default` for mixed Postgres/MongoDB storage, return the YAML-derived config unchanged.
- Otherwise load the connection from ConfigStore.
- Validate selected connection type against archive group store types.
- Overlay the selected Postgres/MongoDB config before creating the last-value/archive stores.
- For MongoDB overlays, pass a URL that contains credentials when username/password are configured,
  replacing incomplete or stale URL userinfo.

## GraphQL API

Expose operations equivalent to the Kotlin broker GraphQL additions:

- Query/list database connection names, filtered by database type.
- Query/list full database connection metadata, optionally filtered by type.
- Query one database connection by name.
- Create database connection.
- Update database connection.
- Delete database connection.
- Extend archive group create/update/info payloads with `databaseConnectionName`.

GraphQL rules:

- The database connection name list must support `DatabaseConnectionType` filtering.
- The database connection name list must include `Default` only when that database type exists in
  YAML.
- Return sanitized URLs.
- Never return passwords.
- On update, missing or empty password keeps the existing secret.
- The built-in `Default` connection selection is read-only and cannot be updated or deleted.
- Deleting a named connection fails while any archive group references it.
- GraphQL validation must allow empty `databaseConnectionName` for mixed Postgres/MongoDB archive
  groups and must reject any non-default named connection for that mixed case.

## Test Plan

Backend tests:

- Existing archive groups without `databaseConnectionName` still use YAML defaults.
- `Default` resolves from YAML and is read-only.
- Named Postgres connection is used by both Postgres last-value and Postgres archive stores.
- Named MongoDB connection is used by both MongoDB last-value and MongoDB archive stores.
- Wrong connection type is rejected.
- Mixed Postgres/MongoDB storage with empty `databaseConnectionName` uses YAML defaults for both
  database families.
- Mixed Postgres/MongoDB storage with a non-default named connection is rejected.
- Deleting an in-use connection is rejected.
- MongoDB named connections with URL userinfo and stored username/password produce a driver URL
  with complete `username:password@host` userinfo.
- MongoDB named connections with incomplete URL userinfo such as `mongodb://user@host:27017` do not
  pass the incomplete userinfo through when stored username/password are present.

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
- Mixed Postgres/MongoDB archive groups accept empty `databaseConnectionName`.
- Mixed Postgres/MongoDB archive groups reject a non-default named connection.

## Implementation Notes From Kotlin Broker

The Kotlin broker implementation added:

- `DatabaseConnectionType` and `DatabaseConnectionConfig` next to archive config store types.
- Default methods on `IArchiveConfigStore` for named connection CRUD.
- `databaseConnectionName` on `ArchiveGroup`.
- Connection resolution in `ArchiveHandler` before deploying archive groups.
- GraphQL `databaseConnections` / `databaseConnection` queries and grouped archive mutations.
- Broker startup no longer fails globally when one enabled archive group fails to resolve or deploy;
  startup skips the failing archive group and continues with the remaining groups.
- Runtime enable/create behavior still reports archive group deployment failures to the caller.
- MongoDB named connection resolution injects stored credentials into the MongoDB URL and replaces
  existing URL userinfo when credentials are configured.

Use the same behavior in Go, but follow the edge broker's package layout, API framework, and
storage abstractions rather than copying Kotlin structure literally.
