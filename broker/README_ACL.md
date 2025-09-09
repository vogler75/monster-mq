# MonsterMQ Access Control List (ACL) Documentation

This document describes the comprehensive user management and Access Control List (ACL) system implemented in MonsterMQ.

## Overview

MonsterMQ provides a robust user authentication and authorization system that controls:
- **Authentication**: Who can connect to the broker
- **Authorization**: What topics users can subscribe to and publish on
- **Administration**: User and ACL rule management

## Features

- 🔐 **BCrypt Password Hashing**: Secure password storage
- 🗄️ **Multi-Database Support**: PostgreSQL, SQLite, CrateDB, MongoDB
- ⚡ **High-Performance Caching**: In-memory ACL cache with topic tree optimization
- 🌐 **GraphQL API**: Complete CRUD operations for users and ACL rules
- 🎯 **MQTT Topic Wildcards**: Full support for `+` and `#` wildcards in ACL rules
- 🔄 **Real-time Updates**: Automatic cache refresh on changes
- 👤 **Anonymous Access**: Configurable anonymous user support

## Configuration

Enable user management in your `config.yaml`:

```yaml
UserManagement:
  Enabled: true
  AuthStoreType: SQLITE           # SQLITE, POSTGRES, CRATEDB, MONGODB
  PasswordAlgorithm: bcrypt       # Currently only bcrypt supported
  CacheRefreshInterval: 60        # Seconds between automatic cache refreshes
  DisconnectOnUnauthorized: true  # Disconnect clients on unauthorized actions
```

### Database Configuration Examples

#### SQLite
```yaml
UserManagement:
  Enabled: true
  AuthStoreType: SQLITE
  Path: "./users.db"
```

#### PostgreSQL
```yaml
UserManagement:
  Enabled: true
  AuthStoreType: POSTGRES
  Url: "jdbc:postgresql://localhost:5432/monstermq"
  Username: "mqttuser"
  Password: "mqttpass"
```

#### MongoDB
```yaml
UserManagement:
  Enabled: true
  AuthStoreType: MONGODB
  Url: "mongodb://localhost:27017"
  Database: "monstermq"
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ACL Rules Table
```sql
CREATE TABLE usersacl (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(255) REFERENCES users(username) ON DELETE CASCADE,
    topic_pattern VARCHAR(1024) NOT NULL,
    can_subscribe BOOLEAN DEFAULT false,
    can_publish BOOLEAN DEFAULT false,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## User Management

### User Properties
- **username**: Unique identifier for the user
- **password**: Stored as BCrypt hash
- **enabled**: Whether the user can authenticate
- **canSubscribe**: Global subscribe permission
- **canPublish**: Global publish permission  
- **isAdmin**: Admin users bypass all ACL checks
- **createdAt/updatedAt**: Timestamps

### Anonymous User
The system automatically creates an "Anonymous" user for unauthenticated connections:
- Username: `Anonymous`
- No password (empty hash)
- Can subscribe and publish by default
- Not an admin

## ACL Rules

ACL rules provide fine-grained topic-level access control.

### Rule Properties
- **id**: Unique identifier (UUID)
- **username**: User this rule applies to
- **topicPattern**: MQTT topic pattern with wildcards
- **canSubscribe**: Allow subscription to matching topics
- **canPublish**: Allow publishing to matching topics
- **priority**: Higher values take precedence (0 = lowest)

### Topic Pattern Matching

ACL rules support full MQTT wildcard syntax:

#### Single-level Wildcard (`+`)
- `sensor/+/temperature` matches:
  - `sensor/room1/temperature`
  - `sensor/room2/temperature`
- Does NOT match:
  - `sensor/room1/humidity`
  - `sensor/room1/sub/temperature`

#### Multi-level Wildcard (`#`)
- `sensor/#` matches:
  - `sensor/room1/temperature`
  - `sensor/room1/humidity`
  - `sensor/room1/sub/temperature`
  - `sensor/anything/deep/nested`

#### Combined Wildcards
- `building/+/sensor/#` matches:
  - `building/floor1/sensor/temperature`
  - `building/floor2/sensor/humidity/current`

### Permission Resolution

The ACL system uses a hierarchical permission model:

1. **Admin Override**: Admin users (`isAdmin: true`) bypass all ACL checks
2. **Global Permissions**: User-level `canSubscribe`/`canPublish` settings
3. **ACL Rules**: Specific topic pattern rules (by priority)
4. **Default Behavior**: 
   - If global permission is granted AND no ACL rules exist → Allow any topic
   - If ACL rules exist → Must have explicit permission for the topic

#### Permission Algorithm
```kotlin
fun hasPermission(user: User, topic: String, operation: Permission): Boolean {
    // 1. Admin users can do anything
    if (user.isAdmin) return true
    
    // 2. Check if user is enabled
    if (!user.enabled) return false
    
    // 3. Check global permissions
    val globalPermission = when(operation) {
        SUBSCRIBE -> user.canSubscribe
        PUBLISH -> user.canPublish
    }
    
    // 4. Find matching ACL rules (by priority)
    val matchingRules = findMatchingAclRules(user.username, topic)
    
    // 5. If no ACL rules and global permission granted → Allow
    if (matchingRules.isEmpty()) return globalPermission
    
    // 6. Check ACL rules (highest priority first)
    for (rule in matchingRules.sortedByDescending { it.priority }) {
        if (topicMatches(rule.topicPattern, topic)) {
            return when(operation) {
                SUBSCRIBE -> rule.canSubscribe
                PUBLISH -> rule.canPublish
            }
        }
    }
    
    // 7. No matching rule found → Deny
    return false
}
```

## GraphQL API

MonsterMQ provides a comprehensive GraphQL API for user and ACL management.

### Endpoint
- **URL**: `http://localhost:8080/graphql`
- **GraphQL Playground**: Available in browser for testing

### User Queries

#### Get All Users
```graphql
query {
  getAllUsers {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
    createdAt
    updatedAt
  }
}
```

#### Get Specific User
```graphql
query {
  getUser(username: "alice") {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
    createdAt
  }
}
```

### User Mutations

#### Create User
```graphql
mutation {
  createUser(input: {
    username: "alice"
    password: "secure_password_123"
    enabled: true
    canSubscribe: true
    canPublish: true
    isAdmin: false
  }) {
    success
    message
    user {
      username
      enabled
    }
  }
}
```

#### Update User
```graphql
mutation {
  updateUser(input: {
    username: "alice"
    enabled: true
    canSubscribe: true
    canPublish: false
  }) {
    success
    message
    user {
      username
      canPublish
    }
  }
}
```

#### Set Password
```graphql
mutation {
  setPassword(input: {
    username: "alice"
    password: "new_secure_password"
  }) {
    success
    message
  }
}
```

#### Delete User
```graphql
mutation {
  deleteUser(username: "alice") {
    success
    message
  }
}
```

### ACL Rule Queries

#### Get All ACL Rules
```graphql
query {
  getAllAclRules {
    id
    username
    topicPattern
    canSubscribe
    canPublish
    priority
    createdAt
  }
}
```

#### Get User's ACL Rules
```graphql
query {
  getUserAclRules(username: "alice") {
    id
    topicPattern
    canSubscribe
    canPublish
    priority
  }
}
```

### ACL Rule Mutations

#### Create ACL Rule
```graphql
mutation {
  createAclRule(input: {
    username: "alice"
    topicPattern: "sensor/+/temperature"
    canSubscribe: true
    canPublish: false
    priority: 1
  }) {
    success
    message
  }
}
```

#### Update ACL Rule
```graphql
mutation {
  updateAclRule(input: {
    id: "rule-uuid-here"
    topicPattern: "sensor/#"
    canSubscribe: true
    canPublish: true
    priority: 2
  }) {
    success
    message
    aclRule {
      topicPattern
      canPublish
    }
  }
}
```

#### Delete ACL Rule
```graphql
mutation {
  deleteAclRule(id: "rule-uuid-here") {
    success
    message
  }
}
```

## Command Line Tools

### Password Hash Generator
Generate BCrypt hashes for manual user creation:

```bash
cd broker
mvn exec:java -Dexec.mainClass="at.rocworks.tools.GeneratePasswordHashKt" -Dexec.args="mypassword"
```

Output:
```
Password: mypassword
BCrypt Hash: $2a$12$ABC123...
Time taken: 156ms
Hash verification: SUCCESS
```

## Example Use Cases

### Basic Setup: Single User
```graphql
# Create a user with full access
mutation {
  createUser(input: {
    username: "mqttuser"
    password: "mqttpass123"
    canSubscribe: true
    canPublish: true
  }) {
    success
    message
  }
}
```

### IoT Sensor Network
```graphql
# Create sensor user (publish only)
mutation {
  createUser(input: {
    username: "sensor_device"
    password: "device_secret"
    canSubscribe: false
    canPublish: false  # Will use ACL rules
  }) {
    success
  }
}

# Allow publishing to sensor topics
mutation {
  createAclRule(input: {
    username: "sensor_device"
    topicPattern: "sensors/+/data"
    canSubscribe: false
    canPublish: true
    priority: 1
  }) {
    success
  }
}

# Create dashboard user (subscribe only)
mutation {
  createUser(input: {
    username: "dashboard"
    password: "dash_secret"
    canSubscribe: false
    canPublish: false
  }) {
    success
  }
}

# Allow subscribing to all sensor data
mutation {
  createAclRule(input: {
    username: "dashboard"
    topicPattern: "sensors/#"
    canSubscribe: true
    canPublish: false
    priority: 1
  }) {
    success
  }
}
```

### Multi-tenant System
```graphql
# Tenant A user - restricted to their namespace
mutation {
  createUser(input: {
    username: "tenant_a_user"
    password: "tenant_a_secret"
    canSubscribe: false
    canPublish: false
  }) {
    success
  }
}

# Tenant A permissions
mutation {
  createAclRule(input: {
    username: "tenant_a_user"
    topicPattern: "tenant_a/#"
    canSubscribe: true
    canPublish: true
    priority: 1
  }) {
    success
  }
}

# Admin user with override
mutation {
  createUser(input: {
    username: "admin"
    password: "admin_secret"
    isAdmin: true
  }) {
    success
  }
}
```

## Performance Considerations

### ACL Cache
- **In-memory storage**: Rules cached in memory for fast lookup
- **Topic tree structure**: Optimized for wildcard matching
- **Automatic refresh**: Cache updates automatically on rule changes
- **Manual refresh**: Available via `UserManager.refreshCache()`

### Database Optimization
- **Indexes**: Automatic index creation on username, topic patterns, priority
- **Connection pooling**: Configurable database connections
- **Bulk operations**: Efficient batch updates where possible

### Best Practices
1. **Use specific patterns**: `sensor/room1/temp` is faster than `sensor/+/temp`
2. **Limit rule count**: Keep ACL rules focused and minimal
3. **Set appropriate priorities**: Use priority levels to avoid rule conflicts
4. **Regular cleanup**: Remove unused users and rules
5. **Monitor cache**: Check cache statistics for performance tuning

## Security Considerations

### Password Security
- **BCrypt hashing**: Industry-standard password hashing
- **Salt generation**: Automatic salt generation per password
- **Cost factor**: Configurable work factor (default: 12)

### Access Control
- **Principle of least privilege**: Grant minimal necessary permissions
- **Regular audits**: Review user permissions periodically
- **Admin separation**: Limit admin users to essential personnel
- **Anonymous control**: Disable anonymous access if not needed

### Network Security
- **TLS encryption**: Use SSL/TLS for MQTT connections
- **Network segmentation**: Isolate MQTT traffic
- **Firewall rules**: Restrict access to GraphQL endpoints
- **Authentication timeouts**: Configure appropriate session timeouts

## Troubleshooting

### Common Issues

#### User Cannot Connect
```
Client Authentication failed for user [username]
```
**Solutions:**
1. Verify user exists and is enabled
2. Check password is correct
3. Ensure user management is enabled
4. Check database connectivity

#### Permission Denied
```
Client [clientId] Subscribe denied for topic [topic]
```
**Solutions:**
1. Check user has global subscribe permission OR matching ACL rule
2. Verify topic pattern matching is correct
3. Check ACL rule priority conflicts
4. Ensure cache is refreshed

#### GraphQL Errors
```
User management is not enabled
```
**Solutions:**
1. Set `UserManagement.Enabled: true` in config
2. Configure correct database settings
3. Restart broker after configuration changes

### Debug Logging
Enable detailed logging:
```yaml
# In logging configuration
Logging:
  Level: FINE  # or FINER, FINEST for more detail
```

### Cache Statistics
Monitor ACL cache performance:
```kotlin
// Via UserManager
val stats = userManager.getCacheStats()
// Returns: user count, rule count, cache hits, etc.
```

## Migration Guide

### From No Authentication
1. Enable user management in config
2. Create admin user
3. Create Anonymous user with desired permissions
4. Test connections
5. Gradually add specific users and rules

### Database Migration
When switching database types:
1. Export existing users/rules via GraphQL
2. Update configuration
3. Restart broker (creates new tables)
4. Import users/rules via GraphQL

## API Reference

### GraphQL Schema Types
```graphql
type UserInfo {
    username: String!
    enabled: Boolean!
    canSubscribe: Boolean!
    canPublish: Boolean!
    isAdmin: Boolean!
    createdAt: String
    updatedAt: String
}

type AclRuleInfo {
    id: String!
    username: String!
    topicPattern: String!
    canSubscribe: Boolean!
    canPublish: Boolean!
    priority: Int!
    createdAt: String
}

type UserManagementResult {
    success: Boolean!
    message: String
    user: UserInfo
    aclRule: AclRuleInfo
}
```

Complete schema available at: `/graphql` endpoint

---

For more information, see the main [README.md](README.md) and [CLAUDE.md](CLAUDE.md) files.