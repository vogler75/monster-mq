# Security

MonsterMQ provides comprehensive security features including TLS/SSL encryption, authentication, authorization, and certificate management. This guide describes the security controls implemented in this broker.

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
# Generate a development JKS keystore matching the example SSL settings
keytool -genkeypair \
  -alias server \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -ext "SAN=dns:localhost,ip:127.0.0.1" \
  -keystore server-keystore.jks \
  -storetype JKS \
  -storepass password \
  -dname "CN=localhost, OU=Development, O=MonsterMQ, L=Vienna, C=AT"

# Export server certificate
keytool -exportcert \
  -alias server \
  -keystore server-keystore.jks \
  -storepass password \
  -rfc \
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

Configure `SSL.KeyStorePath: server.jks`, `SSL.KeyStorePassword: changeit`, and
`SSL.KeyStoreType: JKS` to use the converted keystore, or configure the PEM files
directly. Restart the broker after replacing listener certificates.

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

With `ClientAuth: REQUEST` or `REQUIRED`, you can skip password authentication for clients that present a certificate - the certificate's Common Name becomes their username instead.

```yaml
UserManagement:
  Enabled: true
SSL:
  ClientAuth: REQUEST                # cert optional, so non-cert clients can still use a password
  TrustStorePath: ca.crt
  TrustStoreType: PEM
  UseIdentityAsUsername: true
  AutoCreateUser: false              # true to create accounts on first connect
```

A client presenting a certificate signed by the CA in `TrustStorePath` is authenticated as the certificate's Common Name, with no username or password required. A client with no certificate falls back to the normal username and password check, so both kinds of clients can use the same port.

The certificate only replaces the password. The user account still decides what the client may do:

- If an account exists for that Common Name, it is used, so its `canPublish`, `canSubscribe` and ACL rules apply exactly as they would for a password user.
- If the account is disabled, the connection is rejected. This is how you cut off a decommissioned or compromised device.
- If no account exists, the connection is rejected unless `AutoCreateUser` is enabled.

With `AutoCreateUser: true`, a certificate Common Name that has no account yet gets one created on first connect, with default non-admin permissions. This suits device fleets where every device already carries a certificate from your own CA and you do not want to create accounts by hand. The account is created with a random password that is never used, so the connecting device does not know a password for that account. An administrator can later set a password through the user API. Set it to `false` when you would rather create every account up front and reject anything unknown.

## Authentication and Authorization

```yaml
UserManagement:
  Enabled: true
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60
  DisconnectOnUnauthorized: true
  AclCheckOnSubscription: true
```

Use the [user API](users.md) to change the initial `Admin` password, manage
accounts, and disable `Anonymous` if unauthenticated connections are unwanted.
[ACL rules](acl.md) restrict topics after the account's global operation
permission is enabled. Admin accounts bypass topic ACL checks.

HTTP APIs support JWTs obtained with the GraphQL or REST login endpoint. See
[GraphQL](graphql.md), [REST](rest-api.md), and [MCP](mcp.md) for their respective
transport and authentication requirements. MQTT uses credentials in CONNECT or
the configured certificate identity mechanism.

## Listener Exposure and Rate Limits

The broker reads these per-MQTT-client message-rate settings:

```yaml
MaxPublishRate: 1000
MaxSubscribeRate: 1000
```

A value of 0 disables the corresponding limit. These are message throughput
limits, not per-IP connection limits or account lockout controls. See
[Configuration](configuration.md#rate-limiting).

Set `TCP: 0` and `WS: 0` if only encrypted MQTT listeners should be available.
The MQTT `SSL` settings do not enable HTTPS for GraphQL, REST, or the dashboard;
provide HTTPS at a reverse proxy. Configure network access restrictions and
HTTP security headers in the firewall/proxy used for the deployment.

## Logging and Storage

Enable `Logging.Memory.Enabled` for the dashboard and [GraphQL system logs](graphql-system-logs.md).
This is a bounded runtime log buffer, not a persistent security audit trail.
Arrange persistent collection separately if needed. Archive retention settings
control message deletion schedules; they do not encrypt stored data.

The broker does not parse the old documentation's `Authentication`,
`Authorization`, `Users`, `Groups`, `HTTP.Security`, `RateLimiting`, `Audit`,
`Network.IPFilter`, `Storage.Encryption`, `Encryption`, `PasswordPolicy`,
`Security`, `Privacy`, `PCI`, `IDS`, or `IncidentResponse` blocks. They do not
enable lockout, encryption at rest, intrusion detection, certificate rotation,
or compliance controls. Do not rely on those obsolete examples.

## Troubleshooting

Inspect a TLS handshake and certificate chain:

```bash
openssl s_client -connect mqtt.example.com:8883 -servername mqtt.example.com -showcerts
openssl verify -CAfile ca.crt server.crt
```

For mTLS, verify that `TrustStorePath` is configured as well as `ClientAuth`.
Without a trust-store path, the broker warns and does not check client
certificates. For identity login, check the certificate Common Name, the account's
enabled state, and `UseIdentityAsUsername`.

For access failures, inspect the account and its rules:

```graphql
query InspectAccess {
  users(username: "sensor_001") {
    username enabled canSubscribe canPublish isAdmin
    aclRules { topicPattern canSubscribe canPublish priority }
  }
}
```

Implementation references: [MqttServer.kt](../broker/src/main/kotlin/MqttServer.kt),
[Monster.kt](../broker/src/main/kotlin/Monster.kt), and the
[authentication code](../broker/src/main/kotlin/auth/).

## Interface-specific enforcement limits

Authentication and topic ACL enforcement are not uniform across every extension.
The current [GraphQL WebSocket route](graphql.md#subscriptions),
[MCP tools](mcp.md#authentication-and-access-scope), and
[Zenoh current-value queries](zenoh.md#current-value-queries) have the specific
limits documented in those guides. The [OPC UA server](opcua-server.md#security-block-security)
does not wire a MonsterMQ username identity validator. Restrict access to those
interfaces according to their actual enforcement, rather than assuming the MQTT
client authorization path applies to every protocol.
