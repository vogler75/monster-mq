# User Management & ACL System

MonsterMQ includes a comprehensive user authentication and authorization system with fine-grained control over MQTT client access and topic permissions.

## Overview

The user management system provides:
- **Multi-database user storage** - PostgreSQL, SQLite, CrateDB, MongoDB support
- **BCrypt password security** - Industry-standard password hashing
- **Fine-grained ACL rules** - Topic-level permissions with MQTT wildcards
- **Anonymous user support** - Configurable unauthenticated access
- **GraphQL API** - Complete user and ACL management interface

## Quick Start

### Enable User Management

Add to your `config.yaml`:

```yaml
UserManagement:
  Enabled: true
  StoreType: POSTGRES           # POSTGRES, SQLITE, CRATEDB, MONGODB
  PasswordAlgorithm: bcrypt      # Industry-standard hashing
  CacheRefreshInterval: 60       # Seconds between cache updates
  DisconnectOnUnauthorized: true # Disconnect on ACL violations

# Database configuration
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

# GraphQL API for management
GraphQL:
  Enabled: true
  Port: 4000
```

### Default Admin User

⚠️ **Important:** MonsterMQ automatically creates a default admin user on first startup:

- **Username:** `Admin`
- **Password:** `Admin`

**Change this password immediately for security!**

```graphql
mutation {
  updateUserPassword(
    username: "Admin"
    oldPassword: "Admin"
    newPassword: "NewSecurePassword123!"
  ) {
    success
    message
  }
}
```

## Permission Model

### Hierarchy

MonsterMQ uses a hierarchical permission system:

1. **Admin Override** - Admin users bypass all ACL checks
2. **Global Permissions** - User-level `canSubscribe`/`canPublish` settings
3. **ACL Rules** - Topic-specific permissions with wildcards
4. **Anonymous Access** - Configurable anonymous user permissions

### Permission Resolution

When a client attempts to publish or subscribe:

```
Is Admin User? → Allow
     ↓ No
Has Global Permission? → Check ACL Rules
     ↓ No                    ↓
     Deny              Matches ACL Rule? → Allow/Deny based on rule
                            ↓ No
                      Anonymous Allowed? → Allow/Deny
```

## ACL Rules

### MQTT Topic Wildcards

ACL rules support full MQTT wildcard syntax:

| Wildcard | Description | Example | Matches |
|----------|-------------|---------|---------|
| `+` | Single-level wildcard | `sensors/+/temp` | `sensors/room1/temp`, `sensors/room2/temp` |
| `#` | Multi-level wildcard | `sensors/#` | `sensors/temp`, `sensors/room1/temp`, `sensors/room1/device1/temp` |
| Combined | Both wildcards | `+/+/status/#` | `building/floor1/status`, `building/floor1/status/device1` |

### ACL Rule Priority

Rules are evaluated by priority (lower number = higher priority):

```graphql
# Priority 1 - Specific override
topicPattern: "sensors/room1/temp", priority: 1

# Priority 10 - General department access
topicPattern: "sensors/#", priority: 10

# Priority 100 - Fallback rule
topicPattern: "#", priority: 100
```

## GraphQL User Management API

### API Endpoint

- **URL:** `http://localhost:4000/graphql`
- **Playground:** Access the same URL in a browser for interactive GraphQL playground

### User Operations

#### Create User

```graphql
mutation {
  createUser(input: {
    username: "sensor_device"
    password: "secure_password_123"
    canSubscribe: false
    canPublish: true
    isAdmin: false
    enabled: true
  }) {
    success
    message
    user {
      username
      enabled
      canSubscribe
      canPublish
      isAdmin
    }
  }
}
```

#### Update User

```graphql
mutation {
  updateUser(
    username: "sensor_device"
    input: {
      canSubscribe: true
      canPublish: true
      enabled: true
    }
  ) {
    success
    message
    user {
      username
      canSubscribe
      canPublish
    }
  }
}
```

#### Delete User

```graphql
mutation {
  deleteUser(username: "sensor_device") {
    success
    message
  }
}
```

#### Query Users

```graphql
# Get all users
query {
  getAllUsers {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
    createdAt
    lastLogin
  }
}

# Get specific user
query {
  getUser(username: "sensor_device") {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
    aclRules {
      topicPattern
      canPublish
      canSubscribe
      priority
    }
  }
}
```

### ACL Rule Operations

#### Create ACL Rule

```graphql
mutation {
  createAclRule(input: {
    username: "sensor_device"
    topicPattern: "sensors/+/data"
    canPublish: true
    canSubscribe: false
    priority: 1
  }) {
    success
    message
    aclRule {
      id
      username
      topicPattern
      canPublish
      canSubscribe
      priority
    }
  }
}
```

#### Update ACL Rule

```graphql
mutation {
  updateAclRule(
    id: 123
    input: {
      canPublish: true
      canSubscribe: true
      priority: 5
    }
  ) {
    success
    message
    aclRule {
      id
      canPublish
      canSubscribe
      priority
    }
  }
}
```

#### Delete ACL Rule

```graphql
mutation {
  deleteAclRule(id: 123) {
    success
    message
  }
}
```

#### Query ACL Rules

```graphql
# Get user's ACL rules
query {
  getUserAclRules(username: "sensor_device") {
    id
    topicPattern
    canPublish
    canSubscribe
    priority
  }
}

# Get all ACL rules
query {
  getAllAclRules {
    id
    username
    topicPattern
    canPublish
    canSubscribe
    priority
  }
}
```

## Real-World Examples

### IoT Sensor Network

Create a sensor network with publish-only devices and subscribe-only dashboards:

```graphql
# 1. Create sensor device user
mutation {
  createUser(input: {
    username: "sensor_001"
    password: "sensor_secret_key"
    canSubscribe: false
    canPublish: false  # No global permissions
  }) { success }
}

# 2. Allow sensor to publish data
mutation {
  createAclRule(input: {
    username: "sensor_001"
    topicPattern: "sensors/sensor_001/+"
    canPublish: true
    canSubscribe: false
    priority: 1
  }) { success }
}

# 3. Create dashboard user
mutation {
  createUser(input: {
    username: "dashboard"
    password: "dashboard_secret"
    canSubscribe: false
    canPublish: false
  }) { success }
}

# 4. Allow dashboard to subscribe to all sensors
mutation {
  createAclRule(input: {
    username: "dashboard"
    topicPattern: "sensors/#"
    canPublish: false
    canSubscribe: true
    priority: 1
  }) { success }
}

# 5. Allow dashboard to send commands
mutation {
  createAclRule(input: {
    username: "dashboard"
    topicPattern: "commands/+/restart"
    canPublish: true
    canSubscribe: false
    priority: 2
  }) { success }
}
```

### Multi-Tenant System

Isolate tenants with their own namespaces:

```graphql
# Tenant A configuration
mutation CreateTenantA {
  createUser(input: {
    username: "tenant_a_app"
    password: "tenant_a_secret"
  }) { success }
}

mutation TenantAPermissions {
  createAclRule(input: {
    username: "tenant_a_app"
    topicPattern: "tenant/a/#"
    canPublish: true
    canSubscribe: true
    priority: 1
  }) { success }
}

# Tenant B configuration
mutation CreateTenantB {
  createUser(input: {
    username: "tenant_b_app"
    password: "tenant_b_secret"
  }) { success }
}

mutation TenantBPermissions {
  createAclRule(input: {
    username: "tenant_b_app"
    topicPattern: "tenant/b/#"
    canPublish: true
    canSubscribe: true
    priority: 1
  }) { success }
}

# Shared read-only topics
mutation SharedTopics {
  createAclRule(input: {
    username: "tenant_a_app"
    topicPattern: "shared/news/#"
    canPublish: false
    canSubscribe: true
    priority: 10
  }) { success }
}
```

### Department-Based Access

Organize access by department with hierarchical permissions:

```graphql
# Engineering full access
mutation EngineeringAdmin {
  createUser(input: {
    username: "eng_admin"
    password: "eng_admin_pass"
    isAdmin: false
  }) { success }
}

mutation EngineeringAccess {
  createAclRule(input: {
    username: "eng_admin"
    topicPattern: "factory/engineering/#"
    canPublish: true
    canSubscribe: true
    priority: 1
  }) { success }
}

# Production read-only to engineering
mutation ProductionReadEngineering {
  createUser(input: {
    username: "prod_user"
    password: "prod_pass"
  }) { success }
}

mutation ProductionAccess {
  createAclRule(input: {
    username: "prod_user"
    topicPattern: "factory/engineering/status/+"
    canPublish: false
    canSubscribe: true
    priority: 5
  }) { success }
}

# Production full access to their area
mutation ProductionOwnAccess {
  createAclRule(input: {
    username: "prod_user"
    topicPattern: "factory/production/#"
    canPublish: true
    canSubscribe: true
    priority: 1
  }) { success }
}
```

## Anonymous Access

Configure anonymous user access for unauthenticated clients:

```yaml
# In config.yaml
UserManagement:
  Enabled: true
  AnonymousEnabled: true
  AnonymousCanSubscribe: true
  AnonymousCanPublish: false
```

Or create ACL rules for anonymous user:

```graphql
mutation {
  createAclRule(input: {
    username: "_anonymous"  # Special username
    topicPattern: "public/#"
    canPublish: false
    canSubscribe: true
    priority: 100
  }) { success }
}
```

## Security Features

### Password Security

- **BCrypt Hashing** - Industry-standard with configurable work factor
- **No Plain Text** - Passwords never stored in plain text
- **Salt Per Password** - Unique salt for each password

### Performance Optimization

- **In-Memory Caching** - ACL rules cached for fast lookup
- **Tree Structure** - Optimized topic pattern matching
- **Automatic Refresh** - Cache updates based on `CacheRefreshInterval`

### Audit and Monitoring

Track user activity:

```graphql
query {
  getUser(username: "sensor_001") {
    lastLogin
    lastActivity
    connectionCount
    messagesPublished
    messagesReceived
  }
}
```

## Database Schema

### Users Table

```sql
CREATE TABLE users (
    username VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT true,
    can_subscribe BOOLEAN DEFAULT true,
    can_publish BOOLEAN DEFAULT true,
    is_admin BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP,
    last_activity TIMESTAMP
);
```

### ACL Rules Table

```sql
CREATE TABLE acl_rules (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) REFERENCES users(username) ON DELETE CASCADE,
    topic_pattern VARCHAR(255) NOT NULL,
    can_publish BOOLEAN DEFAULT false,
    can_subscribe BOOLEAN DEFAULT false,
    priority INTEGER DEFAULT 50,
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_username (username),
    INDEX idx_priority (priority)
);
```

## Best Practices

### Security Guidelines

1. **Change Default Admin Password** - Immediately after installation
2. **Use Strong Passwords** - Enforce minimum complexity
3. **Principle of Least Privilege** - Grant minimal required permissions
4. **Regular Audits** - Review user permissions periodically
5. **Disable Unused Accounts** - Set `enabled: false` instead of deleting

### ACL Design Patterns

1. **Namespace Isolation**
```
tenant/a/#  - Tenant A full access
tenant/b/#  - Tenant B full access
shared/#    - Shared read-only area
```

2. **Device-Specific Topics**
```
devices/{deviceId}/status  - Device publishes
devices/{deviceId}/command - Device subscribes
```

3. **Hierarchical Departments**
```
company/engineering/#     - Engineering department
company/production/#      - Production department
company/management/reports/# - Management reports
```

### Performance Tips

1. **Optimize ACL Rules** - Use specific patterns over broad wildcards
2. **Set Appropriate Cache Interval** - Balance freshness vs. performance
3. **Index Database** - Ensure proper indexes on username and topic columns
4. **Monitor Cache Hit Rate** - Adjust cache settings based on usage

## Troubleshooting

### Common Issues

**Authentication Failed**
- Check username and password
- Verify user is enabled
- Check database connectivity

**Authorization Denied**
- Verify ACL rules with GraphQL query
- Check rule priority order
- Ensure wildcards are correct

**Performance Issues**
- Increase cache refresh interval
- Add database indexes
- Optimize ACL rule patterns

### Debug Logging

Enable debug logging for user management:

```bash
./run.sh -log FINE
```

Monitor authentication and authorization:
```bash
tail -f log/monstermq.log | grep "UserManager"
tail -f log/monstermq.log | grep "ACL"
```

## Migration from Other Systems

### From Mosquitto

Convert Mosquitto password file:
```bash
# Export from Mosquitto
mosquitto_passwd -U passwordfile

# Import via GraphQL API
# Script to parse and create users
```

### From Other MQTT Brokers

Most MQTT brokers can export user lists. Use the GraphQL API to bulk import:

```graphql
mutation BulkImport {
  user1: createUser(input: {username: "user1", password: "pass1"}) { success }
  user2: createUser(input: {username: "user2", password: "pass2"}) { success }
  # ... more users
}
```

## Related Documentation

- **[Configuration Reference](configuration.md)** - User management configuration
- **[GraphQL API](graphql.md)** - Complete API documentation
- **[Security](security.md)** - Security best practices
- **[Database Setup](databases.md)** - Database-specific configuration