# Security

MonsterMQ provides comprehensive security features including TLS/SSL encryption, authentication, authorization, and certificate management. This guide covers all security aspects of MonsterMQ deployment.

## Overview

Security features include:
- **TLS/SSL Encryption** - Secure communication channels
- **Authentication** - Username/password and certificate-based
- **Authorization** - ACL-based access control
- **Certificate Management** - X.509 certificate handling
- **Password Security** - BCrypt hashing
- **Token Authentication** - JWT for API access
- **OPC UA Security** - Industrial protocol security

## TLS/SSL Configuration

### Basic TLS Setup

```yaml
# Enable TLS on MQTT port
TCPS: 8883
```

**Note:** MonsterMQ currently uses a hardcoded SSL configuration:
- Keystore file: `server-keystore.jks`
- Keystore password: `password`
- Location: Must be in the working directory where MonsterMQ is started

To enable SSL, place your `server-keystore.jks` file in the MonsterMQ working directory with the password `password`.

### Certificate Requirements

MonsterMQ requires a Java KeyStore (JKS) file for SSL/TLS functionality. The current implementation has these limitations:

- **Fixed filename**: `server-keystore.jks`
- **Fixed password**: `password`
- **Fixed location**: Working directory
- **No advanced TLS configuration**: Protocol versions and cipher suites use Java/Vert.x defaults

### Certificate Generation

#### Self-Signed Certificates (Development)

```bash
# Generate server keystore (must use these exact names/passwords for MonsterMQ)
keytool -genkeypair \
  -alias server \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore server-keystore.jks \
  -storepass password \
  -dname "CN=localhost, OU=Development, O=MonsterMQ, L=Vienna, C=AT"

# Export server certificate
keytool -exportcert \
  -alias server \
  -keystore server.jks \
  -storepass changeit \
  -file server.crt

# Create truststore and import server certificate
keytool -importcert \
  -alias server \
  -file server.crt \
  -keystore truststore.jks \
  -storepass changeit \
  -noprompt
```

#### Production Certificates (Let's Encrypt)

```bash
# Generate Let's Encrypt certificate
certbot certonly --standalone -d mqtt.example.com

# Convert to PKCS12
openssl pkcs12 -export \
  -in /etc/letsencrypt/live/mqtt.example.com/fullchain.pem \
  -inkey /etc/letsencrypt/live/mqtt.example.com/privkey.pem \
  -out server.p12 \
  -name server \
  -password pass:changeit

# Import into Java keystore
keytool -importkeystore \
  -srckeystore server.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore server.jks \
  -deststoretype JKS \
  -deststorepass changeit
```

### Client Certificate Authentication

Client certificate authentication is not currently configurable in MonsterMQ. The SSL implementation only supports server certificates for encrypted communication.

## Authentication

### Username/Password Authentication

```yaml
Authentication:
  Enabled: true
  Type: INTERNAL  # Use internal user database

  # Password requirements
  PasswordPolicy:
    MinLength: 8
    RequireUppercase: true
    RequireLowercase: true
    RequireDigits: true
    RequireSpecialChars: true

  # Account lockout
  Lockout:
    Enabled: true
    MaxAttempts: 5
    LockoutDuration: 300  # seconds
```

### External Authentication

#### LDAP/Active Directory

```yaml
Authentication:
  Type: LDAP

  LDAP:
    Url: "ldap://ldap.example.com:389"
    BaseDN: "dc=example,dc=com"
    UserSearchBase: "ou=users"
    UserSearchFilter: "(uid={0})"
    GroupSearchBase: "ou=groups"
    GroupSearchFilter: "(member={0})"

    # Bind credentials
    BindDN: "cn=admin,dc=example,dc=com"
    BindPassword: "${LDAP_BIND_PASSWORD}"

    # SSL/TLS
    UseSSL: true
    TrustStore: "/app/security/ldap-truststore.jks"
```

#### OAuth 2.0 / JWT

```yaml
Authentication:
  Type: JWT

  JWT:
    # JWT validation
    Secret: "${JWT_SECRET}"  # For HMAC
    PublicKey: "/app/security/jwt-public.pem"  # For RSA
    Algorithm: RS256

    # Token configuration
    Issuer: "https://auth.example.com"
    Audience: "monstermq"
    ClockSkew: 60  # seconds

    # Claims mapping
    UsernameClaim: "sub"
    RolesClaim: "roles"
    PermissionsClaim: "permissions"
```

### Multi-Factor Authentication (MFA)

```yaml
Authentication:
  MFA:
    Enabled: true
    Type: TOTP  # Time-based One-Time Password

    TOTP:
      Issuer: "MonsterMQ"
      Period: 30  # seconds
      Digits: 6
      Algorithm: SHA256
```

## Authorization (ACL)

### ACL Configuration

```yaml
Authorization:
  Enabled: true
  DefaultPolicy: DENY  # Deny by default

  # Anonymous access
  Anonymous:
    Enabled: false
    Permissions:
      - topic: "public/+"
        actions: [SUBSCRIBE]
```

### ACL Rules

```yaml
# User-specific ACLs
Users:
  - Username: sensor-device
    Permissions:
      - topic: "sensors/+/data"
        actions: [PUBLISH]
      - topic: "commands/+/request"
        actions: [SUBSCRIBE]

  - Username: admin
    Permissions:
      - topic: "#"  # All topics
        actions: [PUBLISH, SUBSCRIBE]

# Group-based ACLs
Groups:
  - Name: operators
    Permissions:
      - topic: "production/+"
        actions: [SUBSCRIBE]
      - topic: "alerts/+"
        actions: [PUBLISH, SUBSCRIBE]
```

### Dynamic ACL Management

```graphql
# Add ACL rule via GraphQL
mutation AddACL {
  addACL(
    username: "device-001"
    topic: "devices/001/+"
    actions: [PUBLISH, SUBSCRIBE]
  ) {
    success
  }
}

# Remove ACL rule
mutation RemoveACL {
  removeACL(
    username: "device-001"
    topic: "devices/001/+"
  ) {
    success
  }
}
```

## Security Headers

### HTTP Security Headers (GraphQL/Dashboard)

```yaml
HTTP:
  Security:
    Headers:
      X-Frame-Options: DENY
      X-Content-Type-Options: nosniff
      X-XSS-Protection: "1; mode=block"
      Content-Security-Policy: "default-src 'self'"
      Strict-Transport-Security: "max-age=31536000; includeSubDomains"
```

## Rate Limiting

### Connection Rate Limiting

```yaml
RateLimiting:
  Enabled: true

  # Per-IP limits
  ConnectionsPerIP:
    Limit: 10
    Window: 60  # seconds

  # Global limits
  MaxConnectionsPerSecond: 100

  # Message rate limiting
  MessagesPerClient:
    Limit: 1000
    Window: 60  # seconds

  # Subscription limits
  MaxSubscriptionsPerClient: 100
```

## Audit Logging

### Audit Configuration

```yaml
Audit:
  Enabled: true
  LogLevel: INFO

  # What to audit
  Events:
    - LOGIN_SUCCESS
    - LOGIN_FAILURE
    - PUBLISH
    - SUBSCRIBE
    - ACL_VIOLATION
    - CERTIFICATE_ERROR
    - ADMIN_ACTION

  # Output configuration
  Output:
    Type: FILE  # FILE, SYSLOG, DATABASE
    Path: "/var/log/monstermq/audit.log"
    Format: JSON
    Rotation:
      Size: 100MB
      Keep: 30
```

### Audit Log Format

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "event": "LOGIN_SUCCESS",
  "username": "user123",
  "clientId": "client-001",
  "ipAddress": "192.168.1.100",
  "details": {
    "authMethod": "password",
    "protocol": "MQTT",
    "tlsVersion": "TLSv1.3"
  }
}
```

## Network Security

### Firewall Configuration

```bash
# Allow MQTT ports
ufw allow 1883/tcp comment 'MQTT'
ufw allow 8883/tcp comment 'MQTT TLS'
ufw allow 9000/tcp comment 'WebSocket'
ufw allow 9001/tcp comment 'WebSocket TLS'

# Allow cluster communication
ufw allow from 10.0.0.0/24 to any port 5701 comment 'Hazelcast'

# Allow monitoring
ufw allow from 10.0.0.0/24 to any port 4000 comment 'GraphQL'
```

### IP Whitelisting

```yaml
Network:
  IPFilter:
    Enabled: true
    DefaultPolicy: DENY

    Whitelist:
      - 192.168.1.0/24
      - 10.0.0.0/8

    Blacklist:
      - 192.168.1.100  # Specific IP

    # Per-protocol filtering
    MQTT:
      Whitelist: ["192.168.1.0/24"]
    GraphQL:
      Whitelist: ["10.0.0.0/24"]
```

## Data Encryption

### Encryption at Rest

```yaml
Storage:
  Encryption:
    Enabled: true
    Algorithm: AES-256-GCM

    # Key management
    KeyStore:
      Type: FILE  # FILE, HSM, KMS
      Path: "/app/security/data-keys.jks"
      Password: "${KEYSTORE_PASSWORD}"

    # Encrypt specific data
    EncryptPayloads: true
    EncryptRetained: true
    EncryptArchive: true
```

### Field-Level Encryption

```yaml
Encryption:
  Fields:
    - Field: payload
      Topics: ["sensitive/+"]
      Algorithm: AES-256-GCM

    - Field: clientId
      Algorithm: SHA256  # Hash instead of encrypt
```

## Security Best Practices

### 1. Certificate Management

```bash
# Regular certificate rotation
0 0 1 * * /app/scripts/rotate-certificates.sh

# Certificate expiry monitoring
0 8 * * * /app/scripts/check-certificate-expiry.sh
```

### 2. Password Management

```yaml
# Enforce strong passwords
PasswordPolicy:
  MinLength: 12
  MaxLength: 128
  RequireUppercase: true
  RequireLowercase: true
  RequireDigits: true
  RequireSpecialChars: true
  DisallowCommon: true  # Check against common passwords
  DisallowUsername: true  # Password can't contain username
  HistoryCount: 5  # Can't reuse last 5 passwords
```

### 3. Secure Defaults

```yaml
# Production configuration template
Security:
  # Disable insecure protocols
  DisableInsecure: true

  # Require authentication
  Authentication:
    Required: true
    Anonymous: false

  # Enable all security features
  TLS:
    Required: true
    MinVersion: TLSv1.2

  # Strict ACLs
  Authorization:
    DefaultPolicy: DENY
```

## Compliance

### GDPR Compliance

```yaml
Privacy:
  GDPR:
    Enabled: true

    # Data retention
    Retention:
      DefaultDays: 90
      MinimumDays: 30

    # Right to erasure
    Erasure:
      Enabled: true
      API: true

    # Data portability
    Export:
      Enabled: true
      Format: JSON
```

### PCI DSS Compliance

```yaml
PCI:
  # Encryption requirements
  Encryption:
    MinKeyLength: 2048
    Protocols: [TLSv1.2, TLSv1.3]

  # Access logging
  Audit:
    LogAllAccess: true
    RetentionDays: 365

  # Network segmentation
  NetworkSegmentation:
    Enabled: true
    CardholderDataTopics: ["payments/+"]
```

## Security Monitoring

### Intrusion Detection

```yaml
IDS:
  Enabled: true

  Rules:
    - Name: "Brute Force Detection"
      Pattern: "LOGIN_FAILURE"
      Threshold: 5
      Window: 300  # seconds
      Action: BLOCK_IP

    - Name: "Unusual Topic Access"
      Pattern: "SUBSCRIBE system/#"
      Action: ALERT
```

### Security Metrics

```yaml
Metrics:
  Security:
    - failed_authentications
    - certificate_errors
    - acl_violations
    - suspicious_patterns
    - encryption_operations
```

## Incident Response

### Automated Response

```yaml
IncidentResponse:
  AutoResponse:
    - Trigger: BRUTE_FORCE
      Actions:
        - BLOCK_IP
        - ALERT_ADMIN
        - LOG_INCIDENT

    - Trigger: CERTIFICATE_EXPIRED
      Actions:
        - REJECT_CONNECTION
        - NOTIFY_CLIENT
        - LOG_ERROR
```

### Security Hardening Checklist

- [ ] Enable TLS/SSL for all connections
- [ ] Require strong passwords
- [ ] Implement rate limiting
- [ ] Enable audit logging
- [ ] Configure firewall rules
- [ ] Set up intrusion detection
- [ ] Regular security updates
- [ ] Certificate rotation schedule
- [ ] Backup encryption keys
- [ ] Test disaster recovery
- [ ] Security training for operators
- [ ] Regular security audits

## Troubleshooting

### Common Security Issues

1. **Certificate Validation Errors**
   ```bash
   # Check certificate
   openssl s_client -connect localhost:8883 -showcerts

   # Verify certificate chain
   openssl verify -CAfile ca.crt server.crt
   ```

2. **Authentication Failures**
   ```bash
   # Check audit logs
   tail -f /var/log/monstermq/audit.log | grep LOGIN_FAILURE

   # Test authentication
   mosquitto_pub -h localhost -p 8883 --cafile ca.crt \
     -u testuser -P testpass -t test -m "test"
   ```

3. **ACL Violations**
   ```graphql
   # Check user permissions
   query {
     userPermissions(username: "user123") {
       topic
       actions
     }
   }
   ```