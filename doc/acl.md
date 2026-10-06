# Access Control Lists

ACLs restrict which MQTT topics an account may publish to or subscribe to.
Enable and administer accounts as described in [User Management](users.md).
This page is the canonical reference for topic permission behavior.

## Permission Resolution

For an authenticated non-admin account, the current implementation evaluates
permissions as follows:

1. The global `canPublish` or `canSubscribe` flag must be true for the operation.
2. If the account has no ACL rules, that global permission allows every topic.
3. If any ACL rules exist, they are scanned in descending numeric priority. On
   equal priority, deny rules are scanned first. The first matching rule that
   decides the operation wins.
4. Without a deciding rule, access is denied.

Admin accounts bypass topic ACL checks. Authentication separately checks whether
an account is enabled. An authenticated user's unmatched topic is not retried
against the `Anonymous` account.

### Allow and Deny Rules

| `canPublish` | `canSubscribe` | Rule type |
|---|---|---|
| true | true | Allows publish and subscribe |
| true | false | Allows publish; skipped for subscribe checks |
| false | true | Allows subscribe; skipped for publish checks |
| false | false | **Denies** publish and subscribe |

A rule that grants only one operation never blocks the other one: a
publish-only rule on `telemetry/#` does not prevent a lower-priority rule from
allowing subscriptions there. A rule with both flags false is a deny rule and
blocks both operations for matching topics unless a higher-priority rule allows
them first.

Example: allow everything except the `secret/#` subtree, but keep
`secret/public/#` readable:

| Priority | Pattern | `canPublish` | `canSubscribe` |
|---|---|---|---|
| 200 | `secret/public/#` | false | true |
| 100 | `secret/#` | false | false |
| 1 | `#` | true | true |

To deny only one operation, put a deny rule below a higher-priority rule that
allows the other operation on the same pattern, for example a read-only
`machine/#` (`canSubscribe: true` at priority 20) above a deny `machine/#` (at
priority 10).

Deny rules also apply to subscription filters: a filter inside a denied subtree
(`secret/#`, `secret/+`) is rejected. A broader filter that only partly overlaps
a denied subtree (`#`, `+/x`) is admitted when an allow rule covers it, and each
delivered message is checked against its concrete topic, so denied topics are
never delivered.

A pattern covers a subscription filter only if it covers every topic the
filter can match: `a/+` covers `a/b` and `a/+`, but not `a/#`. Wildcards at the
first level never cover topics starting with `$` (for example `$SYS/...`);
grant those with a pattern that starts with `$`.

The Go edge broker (`monster-mq-edge`) evaluates the same rules identically,
including `%u`/`%c` substitution and the `Anonymous` user's rules.

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
| `true` (default) | Check the requested filter against ACLs | Check each concrete message topic |
| `false` | Check exact topics; allow wildcard filters when global subscription permission permits | Check each concrete message topic |

Delivery-time checks for non-admin accounts make deny rules effective for broad
wildcard subscriptions, and rule changes apply to existing subscriptions.

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
a rule that grants only one operation cannot cancel another allow rule; only a
rule with both flags false denies.

User and rule updates refresh the cache; periodic refresh is configured with
`UserManagement.CacheRefreshInterval`. Use [system logs](graphql-system-logs.md)
for diagnosis. Database connections are configured at the top level as described
in [Databases](databases.md), and [Security](security.md) covers TLS.
