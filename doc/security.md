# Security

MonsterMQ provides comprehensive security features including TLS/SSL encryption, authentication, authorization, and certificate management. This guide covers all security aspects of MonsterMQ deployment.

## Overview

Security features include:
- **TLS/SSL Encryption** - Secure communication channels
- **Authentication** - Username/password and certificate-based
- **Authorization** - ACL-based access control
- **Certificate Management** - X.509 certificate handling
- **Password Security** - BCrypt hashing
- **OPC UA Security** - Industrial protocol security

## TLS/SSL Configuration

### Basic TLS Setup

```yaml
# Enable TLS on MQTT port
TCPS: 8883

# Enable TLS on the Secure WebSocket port
WSS: 8884

# Optional: Configure keystore (defaults shown)
SSL:
  KeyStorePath: server-keystore.jks
  KeyStorePassword: password
  KeyStoreType: JKS   # JKS (default) | PKCS12 / PFX / P12 | PEM
```

By default, MonsterMQ looks for `server-keystore.jks` in the working directory with password `password`. You can customize this using the `SSL` configuration section.

### Certificate Requirements

`SSL.KeyStoreType` can be `JKS` (default), `PKCS12`/`PFX`/`P12`, or `PEM`. For `PEM`, point `SSL.KeyStorePath` at the certificate (or fullchain) file and `SSL.KeyPath` at the private key file - useful if you're using certs straight off certbot/Let's Encrypt without converting them first.

- **Default filename**: `server-keystore.jks` (configurable via `SSL.KeyStorePath`)
- **Default password**: `password` (configurable via `SSL.KeyStorePassword`)
- **Default location**: Working directory (can specify absolute or relative path)
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

### Different Certificates for TCPS and WSS

`WSS` uses the same certificate as `TCPS` unless you override it. This is handy if, say, your WSS listener needs a normal domain cert for browser clients but MQTTS should use a cert from your own internal CA:

```yaml
SSL:
  KeyStorePath: mqtts-server.p12   # used by TCPS, and by WSS unless overridden below
  KeyStorePassword: password
  KeyStoreType: PKCS12
  WSS:
    KeyStorePath: domain-cert.p12  # only used by WSS
    KeyStorePassword: password
    KeyStoreType: PKCS12
```

Leave out whichever fields you don't need to change in the `WSS` block - they fall back to the top-level `SSL` value.

### Client Certificate Authentication (Mutual TLS)

MQTTS can also require clients to present a certificate, verified against a trust store with your CA cert. `WSS` doesn't support this - it stays server-auth only.

```yaml
SSL:
  KeyStorePath: mqtts-server.jks
  KeyStorePassword: password
  ClientAuth: REQUIRED              # NONE (default), REQUEST, or REQUIRED
  TrustStorePath: ca-truststore.jks  # or a CA .crt with TrustStoreType: PEM
  TrustStorePassword: password
  TrustStoreType: JKS
```

`NONE` is the default and behaves exactly like before. `REQUEST` asks for a client cert but still connects if none is given. `REQUIRED` fails the handshake unless the client presents a cert that verifies against `TrustStorePath`.

### Using the Certificate as the Login (No Password Needed)

With `ClientAuth: REQUEST` or `REQUIRED`, you can also skip password authentication entirely for clients that present a certificate - their certificate's Common Name becomes their identity instead.

```yaml
UserManagement:
  Enabled: true
SSL:
  ClientAuth: REQUEST                # cert optional, so non-cert clients can still use a password
  TrustStorePath: ca.crt
  TrustStoreType: PEM
  UseIdentityAsUsername: true
```

A client presenting a certificate signed by the CA in `TrustStorePath` connects immediately, authenticated as the certificate's CN, no username or password required. A client with no certificate falls back to the normal username/password check. Both kinds of clients can connect to the same port at once.

The certificate is already verified against the CA during the TLS handshake before this ever kicks in, so no separate user record needs to exist for that CN - the CA signature is treated as sufficient proof of identity. Per-topic permissions still work the same way they do for any other user.

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

> External identity providers and token-based authentication flows are not supported in the current broker build.

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
