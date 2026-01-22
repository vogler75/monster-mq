# Feature: Implement SASL Authentication Support

## Summary

Implement SASL (Simple Authentication and Security Layer) support for MonsterMQ to provide standardized, extensible authentication mechanisms beyond the current basic username/password authentication.

## Background

### Current State
MonsterMQ currently supports:
- Basic username/password authentication via MQTT CONNECT packet
- BCrypt password hashing (12 rounds)
- Database-backed user management (PostgreSQL, SQLite, CrateDB, MongoDB)
- TLS/SSL transport security (TCPS, WSS)
- ACL-based authorization

### Why SASL?
SASL provides:
- **Standardized authentication framework** - Industry-standard RFC 4422
- **Mechanism negotiation** - Client and broker can negotiate the best available mechanism
- **Extensibility** - Easy to add new authentication methods
- **Security features** - Challenge-response protocols prevent password transmission
- **Enterprise integration** - LDAP, Kerberos, OAuth2 compatibility

## Proposed SASL Mechanisms

### Phase 1: Core SASL Mechanisms
| Mechanism | Priority | Description |
|-----------|----------|-------------|
| **SASL-PLAIN** | High | Simple username/password over TLS (RFC 4616) |
| **SASL-SCRAM-SHA-256** | High | Salted Challenge Response (RFC 7677) |
| **SASL-SCRAM-SHA-512** | Medium | Stronger variant of SCRAM |

### Phase 2: Enterprise Mechanisms
| Mechanism | Priority | Description |
|-----------|----------|-------------|
| **SASL-EXTERNAL** | Medium | Client certificate authentication (mTLS) |
| **SASL-OAUTHBEARER** | Medium | OAuth 2.0 token authentication (RFC 7628) |
| **SASL-GSSAPI** | Low | Kerberos authentication |

## Implementation Plan

### 1. SASL Framework Architecture

#### 1.1 Create SASL Abstraction Layer
**New files to create:**
- `broker/src/main/kotlin/auth/sasl/SaslMechanism.kt` - Base interface for all mechanisms
- `broker/src/main/kotlin/auth/sasl/SaslServer.kt` - SASL server context manager
- `broker/src/main/kotlin/auth/sasl/SaslResult.kt` - Authentication result types

```kotlin
// Proposed interface structure
interface SaslMechanism {
    val name: String
    fun createServer(): SaslServerContext
}

interface SaslServerContext {
    fun evaluateResponse(response: ByteArray): SaslStepResult
    fun isComplete(): Boolean
    fun getAuthenticatedUser(): String?
}

sealed class SaslStepResult {
    data class Challenge(val data: ByteArray) : SaslStepResult()
    data class Success(val username: String) : SaslStepResult()
    data class Failure(val reason: String) : SaslStepResult()
}
```

#### 1.2 Mechanism Registry
- `broker/src/main/kotlin/auth/sasl/SaslMechanismRegistry.kt`
- Dynamic registration of available mechanisms
- Mechanism priority ordering
- Configuration-based enabling/disabling

### 2. SASL-PLAIN Implementation

**File:** `broker/src/main/kotlin/auth/sasl/mechanisms/PlainMechanism.kt`

Tasks:
- [ ] Implement RFC 4616 PLAIN mechanism
- [ ] Parse `\0authzid\0authcid\0password` format
- [ ] Integrate with existing `UserManager.authenticate()`
- [ ] Require TLS for PLAIN (security requirement)
- [ ] Add configuration option to allow/disallow over non-TLS (default: disallow)

### 3. SASL-SCRAM Implementation

**Files:**
- `broker/src/main/kotlin/auth/sasl/mechanisms/ScramMechanism.kt`
- `broker/src/main/kotlin/auth/sasl/mechanisms/ScramUtils.kt`

Tasks:
- [ ] Implement SCRAM-SHA-256 (RFC 7677)
- [ ] Implement SCRAM-SHA-512 variant
- [ ] Client-first message parsing
- [ ] Server-first message generation with salt and iteration count
- [ ] Client-final message verification
- [ ] Server-final message with signature
- [ ] Channel binding support (SCRAM-SHA-256-PLUS) - optional

**Database Changes:**
- [ ] Add `salt` column to users table
- [ ] Add `iteration_count` column to users table
- [ ] Add `stored_key` and `server_key` columns for SCRAM
- [ ] Migration script for existing users

### 4. MQTT Protocol Integration

#### 4.1 MQTT 5 Enhanced Authentication
**File:** `broker/src/main/kotlin/MqttClient.kt`

MQTT 5 provides native SASL support via:
- AUTH packet (0xF0)
- Authentication Method property
- Authentication Data property

Tasks:
- [ ] Implement AUTH packet handling
- [ ] Support multi-step authentication flow
- [ ] Re-authentication support during session

#### 4.2 MQTT 3.1.1 Compatibility
For MQTT 3.1.1 clients (current support level):
- [ ] Use username field for mechanism selection (e.g., `SCRAM-SHA-256:username`)
- [ ] Use password field for initial client response
- [ ] Alternative: Custom topic-based authentication handshake

### 5. Configuration Updates

**File:** `broker/yaml-json-schema.json`

```yaml
Authentication:
  Enabled: true
  Mechanisms:
    - name: SCRAM-SHA-256
      enabled: true
      iterations: 4096
    - name: SCRAM-SHA-512
      enabled: true
      iterations: 4096
    - name: PLAIN
      enabled: true
      requireTls: true
  DefaultMechanism: SCRAM-SHA-256
  AllowAnonymous: false
```

Tasks:
- [ ] Update JSON schema with SASL configuration options
- [ ] Add mechanism-specific settings
- [ ] Backward compatibility with existing `UserManagement` config
- [ ] Migration path documentation

### 6. User Store Updates

**Files to modify:**
- `broker/src/main/kotlin/stores/IUserStore.kt`
- `broker/src/main/kotlin/stores/postgres/UserStorePostgres.kt`
- `broker/src/main/kotlin/stores/sqlite/UserStoreSqlite.kt`
- `broker/src/main/kotlin/stores/cratedb/UserStoreCrateDb.kt`
- `broker/src/main/kotlin/stores/mongodb/UserStoreMongoDB.kt`

Tasks:
- [ ] Add SCRAM credential fields to User model
- [ ] Implement credential derivation on user creation/password change
- [ ] Add `getSaslCredentials(username, mechanism)` method
- [ ] Database migration scripts for all supported databases

### 7. Testing

#### Unit Tests
- [ ] SASL-PLAIN parsing and authentication
- [ ] SCRAM message generation and verification
- [ ] Mechanism negotiation
- [ ] Invalid credential handling
- [ ] Timing attack resistance

#### Integration Tests
- [ ] End-to-end SASL-PLAIN over TLS
- [ ] End-to-end SCRAM-SHA-256 authentication
- [ ] Mechanism fallback scenarios
- [ ] Re-authentication flow (MQTT 5)
- [ ] Compatibility with existing clients

#### Security Tests
- [ ] PLAIN rejected without TLS
- [ ] Brute force protection
- [ ] Credential timing attack resistance
- [ ] Channel binding verification

### 8. Documentation

- [ ] Update README with SASL configuration
- [ ] Add SASL authentication guide
- [ ] Document migration from basic auth to SASL
- [ ] Client library compatibility matrix
- [ ] Security best practices

## Phase 2: Enterprise Mechanisms (Future)

### SASL-EXTERNAL (mTLS)
- Client certificate authentication
- Certificate DN to username mapping
- Certificate revocation checking

### SASL-OAUTHBEARER
- OAuth 2.0 / OpenID Connect integration
- Token validation
- Refresh token handling
- Multiple identity provider support

### SASL-GSSAPI (Kerberos)
- Enterprise Active Directory integration
- Service principal configuration
- Keytab management

## Dependencies

### Required Libraries
```xml
<!-- SCRAM implementation -->
<dependency>
    <groupId>com.ongres.scram</groupId>
    <artifactId>scram-client</artifactId>
    <version>2.1</version>
</dependency>

<!-- Or implement using standard crypto -->
<dependency>
    <groupId>org.bouncycastle</groupId>
    <artifactId>bcprov-jdk18on</artifactId>
    <version>1.78</version>
</dependency>
```

## Security Considerations

1. **PLAIN over TLS only** - Never allow PLAIN mechanism without TLS
2. **Timing attacks** - Use constant-time comparison for credentials
3. **Iteration count** - Minimum 4096 for SCRAM, recommend 10000+
4. **Salt storage** - Unique per-user, minimum 16 bytes
5. **Channel binding** - Implement for SCRAM-*-PLUS variants when possible
6. **Credential storage** - Never store plaintext passwords, only derived keys for SCRAM

## Breaking Changes

- Configuration format changes (backward compatible with deprecation warnings)
- User database schema changes (migration required)
- MQTT 5 requirement for full SASL support (MQTT 3.1.1 limited support)

## Related

- RFC 4422 - Simple Authentication and Security Layer (SASL)
- RFC 4616 - The PLAIN Simple Authentication and Security Layer Mechanism
- RFC 7677 - SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
- RFC 7628 - A Set of SASL Mechanisms for OAuth
- MQTT 5.0 Specification - Section 4.12 Enhanced Authentication

## Acceptance Criteria

- [ ] SASL-PLAIN authentication works over TLS
- [ ] SASL-SCRAM-SHA-256 authentication works
- [ ] Existing basic auth continues to work (backward compatibility)
- [ ] Configuration is documented and validated
- [ ] All database backends support SASL credentials
- [ ] Unit and integration tests pass
- [ ] No security vulnerabilities in implementation
