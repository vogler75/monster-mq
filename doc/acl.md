# Access Control Lists

ACLs restrict which MQTT topics an account may publish to or subscribe to.
Enable and administer accounts as described in [User Management](users.md).
This page is the canonical reference for topic permission behavior.

## Permission Resolution

For an authenticated non-admin account, the current implementation evaluates
permissions as follows:

1. The global `canPublish` or `canSubscribe` flag must be true for the operation.
2. If the account has no ACL rules, that global permission allows every topic.
3. If any ACL rules exist, at least one matching rule must enable the operation.
4. Without a matching allow rule, access is denied.

Admin accounts bypass topic ACL checks. Authentication separately checks whether
an account is enabled. An authenticated user's unmatched topic is not retried
against the `Anonymous` account.

**Rules are allow rules.** A rule with `canPublish: false` is skipped for publish
checks; it is not an explicit deny that overrides another matching allow rule.
Rules are scanned in descending numeric priority, but priority does not turn a
false permission into a deny rule. For isolation, enable only the required global
operations and grant only the desired topic patterns.

Implementation: [AclCache.kt](../broker/src/main/kotlin/auth/AclCache.kt),
`checkPermissionInternal` and `resolvePattern`.

## Patterns and Substitution

| Pattern | Meaning |
|---|---|
| `sensors/+/temperature` | One topic level between `sensors` and `temperature` |
| `sensors/#` | The `sensors` subtree |
| `building/+/sensor/#` | Sensor subtree within one building level |
| `devices/%c/#` | Replace `%c` with the MQTT client ID |
| `users/%u/status` | Replace `%u` with the username |
| `data/%u/%c/telemetry` | Combine username and client ID substitution |

A `%c` rule cannot match if the caller supplies no client ID, as with ordinary
GraphQL data requests. MQTT `+` and `#` are subscription/ACL wildcards and cannot
appear in an actual MQTT publish topic.

## Subscription Check Timing

```yaml
UserManagement:
  Enabled: true
  AclCheckOnSubscription: true
```

| Setting | Subscription admission | Message delivery |
|---|---|---|
| `true` (default) | Check the requested filter against ACLs | No additional per-message ACL check |
| `false` | Check exact topics; allow wildcard filters when global subscription permission permits | Check each concrete message topic |

For example, a user with global subscribe permission and an allow rule for
`sensors/#` cannot subscribe to `#` in the default mode. With the setting false,
it can subscribe to `#` but receives only permitted sensor messages.
`AllowRootWildcardSubscription: false` separately rejects the root `#` filter.

## Manage Rules through GraphQL

Authenticate as an administrator, then call grouped `user` mutations. Run each
operation separately, checking `success` before continuing. In this example,
`sensor_001` must already have `canPublish: true` and `canSubscribe: false`.

```graphql
mutation AllowSensorData {
  user {
    createAclRule(input: {
      username: "sensor_001"
      topicPattern: "sensors/%c/data"
      canPublish: true
      canSubscribe: false
      priority: 10
    }) {
      success
      message
      aclRule { id topicPattern canPublish canSubscribe priority }
    }
  }
}
```

Keep the returned ID, or retrieve it with `users(username: "sensor_001") {
aclRules { id topicPattern } }`.

```graphql
mutation UpdateRule {
  user {
    updateAclRule(input: {
      id: "replace-with-returned-rule-id"
      topicPattern: "sensors/%c/#"
      canPublish: true
      canSubscribe: false
      priority: 20
    }) { success message aclRule { id topicPattern priority } }
  }
}
```

```graphql
mutation DeleteRule {
  user {
    deleteAclRule(id: "replace-with-returned-rule-id") { success message }
  }
}
```

Deleting an account's final ACL rule restores the unrestricted behavior of its
enabled global permissions. Disable the account or global operation first when
removing rules is intended to revoke access.

## Common Patterns

- **Sensor**: global publish true, subscribe false; allow publish to
  `sensors/%c/#`.
- **Dashboard**: global subscribe true, publish false; allow subscribe to
  `sensors/#`.
- **Tenant application**: both global operations true; allow both only within
  `tenant/a/#`. Create a separate account and pattern for another tenant.
- **Public reader**: set the `Anonymous` global subscribe flag true and allow
  subscribe to `public/#`; keep global publish false.

Create disabled accounts, add their intended ACLs, and then enable them to avoid
an interval of unrestricted access during provisioning.

## Troubleshooting

Check the account's enabled state and global permission first, then inspect its
rules, wildcard pattern, substitutions, and the subscription-check mode. A
matching ACL cannot grant an operation whose global flag is false. Conversely,
a false flag on one rule cannot cancel another allow rule.

User and rule updates refresh the cache; periodic refresh is configured with
`UserManagement.CacheRefreshInterval`. Use [system logs](graphql-system-logs.md)
for diagnosis. Database connections are configured at the top level as described
in [Databases](databases.md), and [Security](security.md) covers TLS.
