# User Management

MonsterMQ stores users and ACL rules in PostgreSQL, SQLite, CrateDB, or MongoDB.
Use this guide for account administration and [ACL rules](acl.md) for topic
permissions. User management is disabled in the starter configuration.

## Enable Authentication

```yaml
DefaultStoreType: SQLITE
SQLite:
  Path: ./sqlite
UserManagement:
  Enabled: true
  StoreType: SQLITE
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60
  DisconnectOnUnauthorized: true
  AclCheckOnSubscription: true
GraphQL:
  Enabled: true
  Port: 4000
```

Database connection settings belong in the top-level `Postgres`, `CrateDB`,
`MongoDB`, or `SQLite` block, not inside `UserManagement`. When its `StoreType` is
omitted, user storage follows `StoreType`, then `DefaultStoreType`, then `SQLITE`.
See [Databases](databases.md).

On initialization the broker ensures the `Admin` and `Anonymous` accounts exist.
The initial administrator credentials are `Admin` / `Admin`; change the password
before exposing the broker. `Anonymous` starts enabled with both global topic
permissions disabled.

## Authenticate to GraphQL

Send requests to `http://localhost:4000/graphql`:

```graphql
mutation Login {
  login(username: "Admin", password: "Admin") {
    success
    token
    isAdmin
  }
}
```

Supply the returned token as `Authorization: Bearer <token>` on subsequent
requests. User and ACL mutations are nested under `user`; the old root-level
`createUser`, `setPassword`, and ACL mutations are no longer available.

## Change a Password

```graphql
mutation ChangePassword {
  user {
    setPassword(input: {username: "Admin", password: "ReplaceWithYourOwnSecret123!"}) {
      success
      message
    }
  }
}
```

The password encoder uses BCrypt with a fixed cost of 12. There is no YAML work
factor setting. Password hashes are not returned by the users query. For a
migration from another broker, provision new passwords through the API; there is
no documented password-hash import endpoint.

## Create and Update Accounts

This account may publish; its ACL rules will restrict the allowed topics:

```graphql
mutation CreateSensor {
  user {
    createUser(input: {
      username: "sensor_001"
      password: "ReplaceWithYourOwnSecret123!"
      enabled: true
      canSubscribe: false
      canPublish: true
      isAdmin: false
    }) {
      success
      message
      user { username enabled canSubscribe canPublish isAdmin }
    }
  }
}
```

Until an account has ACL rules, each enabled global permission allows all topics
for that operation. Create accounts disabled if they must not connect before
rules are installed, then enable them after setup.

```graphql
mutation UpdateSensor {
  user {
    updateUser(input: {username: "sensor_001", enabled: true, canPublish: true}) {
      success
      message
      user { username enabled canPublish }
    }
  }
}
```

`username` belongs inside the update input. It identifies the account and is not
a rename operation. Change passwords separately with `setPassword`.

## Inspect Accounts and Rule IDs

```graphql
query InspectSensor {
  users(username: "sensor_001") {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
    createdAt
    updatedAt
    aclRules { id topicPattern canSubscribe canPublish priority }
  }
}
```

Omit `username` to list all users. This administrative query requires admin access
when authentication is enabled. Use returned ACL IDs for updates and deletion;
they are strings, not numeric sequence IDs.

## Delete an Account

```graphql
mutation DeleteSensor {
  user {
    deleteUser(username: "sensor_001") { success message }
  }
}
```

To temporarily revoke login instead, set `enabled: false` with `updateUser`.

## Anonymous Access

A connection without credentials can use the enabled `Anonymous` account.
Adding ACL rules alone does not enable its global permissions. To grant public
read access, set `canSubscribe: true` and add a subscribe rule for `public/#`.
Keep `canPublish: false` if anonymous publications are not intended. Disable the
account with `user.updateUser(input: {username: "Anonymous", enabled: false})`
when unauthenticated access is unwanted.

## Operations and Troubleshooting

User and ACL data are cached. Changes trigger cache refreshes, and
`CacheRefreshInterval` controls periodic refresh. Inspect the actual account and
rules when diagnosing access failures; do not assume a broad rule overrides a
disabled global permission.

For authentication failures, check the username, password, enabled state, and
user-store connectivity. For authorization failures, follow [ACL resolution](acl.md).
Use the dashboard log viewer or [GraphQL logs](graphql-system-logs.md); a temporary
`./run.sh -- -log FINE` enables more verbose broker logging.

The broker creates its own database tables and indexes. Their definitions vary
by backend; use [UserFactory.kt](../broker/src/main/kotlin/stores/UserFactory.kt)
and the selected store implementation instead of copying generic SQL DDL.
