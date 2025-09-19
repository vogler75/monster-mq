# OPC UA Integration

MonsterMQ provides comprehensive OPC UA client integration with certificate-based security, dynamic device management, and unified message archiving.

## Features

- **Cluster-Aware Device Management** - Distribute OPC UA devices across cluster nodes
- **Certificate Security** - X.509 certificates with auto-generation
- **Flexible Addressing** - NodeId and BrowsePath subscriptions
- **Hot Configuration** - Real-time device/address management without restart
- **Unified Archiving** - OPC UA data uses same storage as MQTT messages

## Device Configuration

### Creating OPC UA Devices

Use GraphQL API to create and manage OPC UA devices:

```graphql
mutation {
  createOpcUaDevice(input: {
    name: "plc01"
    namespace: "opcua/factory/plc01"
    nodeId: "node1"
    enabled: true
    config: {
      endpointUrl: "opc.tcp://192.168.1.100:4840"
      securityPolicy: "Basic256Sha256"
      username: "admin"
      password: "password"
      subscriptionSamplingInterval: 1000.0
      connectionTimeout: 10000
      requestTimeout: 5000
      addresses: [
        {
          address: "NodeId://ns=2;i=1001"
          topic: "temperature"
          publishMode: "SEPARATE"
        },
        {
          address: "BrowsePath://Objects/Factory/Line1/#"
          topic: "production"
          publishMode: "SEPARATE"
          removePath: true
        }
      ]
      certificateConfig: {
        securityDir: "security"
        applicationName: "MonsterMQ@factory"
        applicationUri: "urn:MonsterMQ:Factory:Client"
        organization: "Factory Corp"
        createSelfSigned: true
        keystorePassword: "factory123"
      }
    }
  }) {
    success
    message
    device {
      name
      enabled
      connectionStatus
    }
  }
}
```

### Device Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `name` | Unique device identifier | `"plc01"` |
| `namespace` | MQTT topic namespace for this device | `"opcua/factory/plc01"` |
| `nodeId` | Cluster node assignment | `"node1"` |
| `enabled` | Device activation status | `true` |
| `endpointUrl` | OPC UA server endpoint | `"opc.tcp://192.168.1.100:4840"` |
| `securityPolicy` | Security level | `"None"`, `"Basic256Sha256"`, `"AES128_SHA256_RSAOAEP"` |
| `username`/`password` | Authentication credentials | Optional |
| `subscriptionSamplingInterval` | Default sampling rate (ms) | `1000.0` |

## Address Configuration

### NodeId Addresses

Direct node subscriptions using NodeId:

```graphql
{
  address: "NodeId://ns=2;i=1001"
  topic: "temperature"
  publishMode: "SEPARATE"
}
```

**Result:** Messages published to `opcua/factory/plc01/temperature`

### BrowsePath Addresses

Browse-based subscriptions with wildcards:

```graphql
{
  address: "BrowsePath://Objects/Factory/Line1/#"
  topic: "production"
  publishMode: "SEPARATE"
  removePath: true
}
```

**Result:** For node `Objects/Factory/Line1/Station1/Temperature`:
- With `removePath: true` → `opcua/factory/plc01/production/Station1/Temperature`
- With `removePath: false` → `opcua/factory/plc01/production/Objects/Factory/Line1/Station1/Temperature`

### Publish Modes

**SEPARATE Mode** (default)
- Each OPC UA node gets its own MQTT topic
- Structured topic hierarchy
- Easy to subscribe to specific values

**SINGLE Mode**
- All values from this address published to one topic
- JSON payload with multiple values
- Useful for grouped data

```graphql
{
  address: "BrowsePath://Objects/Factory/#"
  topic: "all_data"
  publishMode: "SINGLE"
}
```

## Certificate Security

### Certificate Storage Architecture

MonsterMQ uses a device-specific certificate storage system to support multiple OPC UA connections with different security requirements:

```
security/                                     # Root security directory
├── monstermq-opcua-client-device1.pfx      # OPC UA client certificate for device1
├── monstermq-opcua-client-device2.pfx      # OPC UA client certificate for device2
├── trusted-device1/                        # Trust store (only created if validation enabled)
│   └── trusted/certs/                      # Trusted server certificates
└── trusted-device2/                        # Trust store (only created if validation enabled)
    └── trusted/certs/                      # Trusted server certificates
```

**Key Points:**
- Each device gets its own certificate file (`monstermq-opcua-client-{device-name}.pfx`)
- Trust directories (`trusted-{device-name}/`) are **only created when certificate validation is enabled**
- With `validateServerCertificate: false`, no trust directories are created at all
- Special characters in device names are replaced with underscores
- No certificate conflicts between multiple OPC UA connections

**Directory Creation Logic:**
- **Always created**: Client certificate file (`.pfx`) when using security policies other than "None"
- **Conditionally created**: Trust directories only when `validateServerCertificate: true`
- **Never created**: Trust directories when `validateServerCertificate: false` or `securityPolicy: "None"`

### Auto-Generated Certificates

MonsterMQ automatically generates self-signed certificates when:
- `createSelfSigned: true` is configured
- No existing certificate is found

```kotlin
// Certificate format: PKCS12 keystore with X.509 certificate
// Key algorithm: RSA 2048-bit
// Certificate alias: "client-ai"
```

### Certificate Configuration

```graphql
certificateConfig: {
  securityDir: "security"                    # Certificate directory
  applicationName: "MonsterMQ@factory"       # Application identifier
  applicationUri: "urn:MonsterMQ:Client"     # Unique application URI
  organization: "Factory Corp"               # Certificate organization
  organizationalUnit: "Production"           # Department/unit
  localityName: "Vienna"                     # City
  countryCode: "AT"                          # Country code (2 letters)
  createSelfSigned: true                     # Auto-generate if missing
  keystorePassword: "secure123"              # Keystore password
  validateServerCertificate: true            # Validate server certificates
  autoAcceptServerCertificates: false        # Auto-accept new server certs
}
```

### Certificate Validation Options

**validateServerCertificate** (default: true)
- `true`: Validates server certificates against trust store
- `false`: Disables certificate validation (insecure, not for production)

**autoAcceptServerCertificates** (default: false)
- `true`: Automatically accepts and saves new server certificates
- `false`: Only accepts certificates already in trust store
- Note: If trust store is empty with `validateServerCertificate: true`, the first certificate is auto-accepted

### Certificate Properties

Generated certificates include:
- **RSA 2048-bit key pair**
- **All local IP addresses** (for network connectivity)
- **All hostnames** (localhost, FQDN, etc.)
- **Common Name** from applicationName
- **Subject Alternative Names** for flexibility
- **Validity period**: 365 days from creation

### Using Existing Certificates

#### Option 1: Replace Auto-Generated Certificate

If you have an existing certificate from a Certificate Authority (CA):

```bash
# 1. Convert your certificate and key to PKCS12 format (if needed)
openssl pkcs12 -export \
  -in your-certificate.crt \
  -inkey your-private-key.key \
  -out monstermq-opcua-client-yourdevice.pfx \
  -name "client-ai" \
  -password pass:yourpassword

# 2. Place in security directory with correct naming
cp monstermq-opcua-client-yourdevice.pfx /path/to/monstermq/security/

# 3. Update device configuration
# Set createSelfSigned: false
# Set keystorePassword to match your certificate password
```

#### Option 2: Import Existing Certificate into Keystore

```bash
# Import existing certificate with private key
keytool -importkeystore \
  -srckeystore your-certificate.p12 \
  -srcstoretype PKCS12 \
  -destkeystore security/monstermq-opcua-client-yourdevice.pfx \
  -deststoretype PKCS12 \
  -deststorepass yourpassword \
  -destalias "client-ai"
```

#### Option 3: Use Certificate with Different Alias

```bash
# List aliases in your certificate
keytool -list -keystore your-certificate.pfx -storetype PKCS12

# If alias is different from "client-ai", rename it
keytool -changealias \
  -alias "old-alias" \
  -destalias "client-ai" \
  -keystore your-certificate.pfx \
  -storetype PKCS12
```

### Manual Certificate Management

```bash
# View certificate information
keytool -list -v \
  -keystore security/monstermq-opcua-client-device1.pfx \
  -storetype PKCS12 \
  -storepass yourpassword

# Extract certificate for OPC UA server trust
keytool -export \
  -alias client-ai \
  -keystore security/monstermq-opcua-client-device1.pfx \
  -storetype PKCS12 \
  -storepass yourpassword \
  -file device1-client.cer

# Import server certificate to trust store
keytool -import \
  -alias opcua-server \
  -file server-certificate.cer \
  -keystore security/trusted-device1/trusted/certs/server.crt \
  -storetype PEM \
  -noprompt
```

### Server Certificate Trust Management

#### Auto-Accept Mode (Development)

For development or initial setup:

```graphql
certificateConfig: {
  validateServerCertificate: true
  autoAcceptServerCertificates: true  # Auto-accept and save
}
```

Server certificates are automatically saved to: `security/trusted-{device}/trusted/certs/`

#### Manual Trust (Production)

For production environments:

```bash
# 1. Get server certificate
openssl s_client -connect opcua-server:4840 \
  -showcerts </dev/null 2>/dev/null | \
  openssl x509 -outform PEM > server.crt

# 2. Place in device trust store
cp server.crt security/trusted-device1/trusted/certs/

# 3. Configure strict validation
certificateConfig: {
  validateServerCertificate: true
  autoAcceptServerCertificates: false  # Only trust existing certs
}
```

### Certificate Troubleshooting

**Issue: "trustAnchors parameter must be non-empty"**
- **Cause**: Trust store is empty and `autoAcceptServerCertificates: false`
- **Solution**: Either set `autoAcceptServerCertificates: true` or manually add server certificate to trust store

**Issue: Certificate file not found**
- **Cause**: `createSelfSigned: false` but certificate doesn't exist
- **Solution**: Either set `createSelfSigned: true` or provide existing certificate

**Issue: Bad certificate password**
- **Cause**: Wrong password in configuration
- **Solution**: Verify `keystorePassword` matches certificate password

**Issue: Server rejects client certificate**
- **Cause**: Client certificate not trusted by OPC UA server
- **Solution**: Export client certificate and import into server's trusted certificates

```bash
# Export MonsterMQ client certificate
keytool -export \
  -alias client-ai \
  -keystore security/monstermq-opcua-client-device1.pfx \
  -storetype PKCS12 \
  -file monstermq-opcua-client.cer

# On OPC UA server, import to trusted certificates
# Location varies by server (Kepware, Siemens, etc.)
```

## Security Policies

### None (No Security)
```graphql
securityPolicy: "None"
```
- No encryption or authentication
- Fastest performance
- Use only on secure networks

### Basic256Sha256
```graphql
securityPolicy: "Basic256Sha256"
```
- AES-256 encryption
- SHA-256 signatures
- Good balance of security and performance

### AES128_SHA256_RSAOAEP
```graphql
securityPolicy: "AES128_SHA256_RSAOAEP"
```
- AES-128 encryption with OAEP padding
- SHA-256 signatures
- Modern security profile

### AES256_SHA256_RSAPSS
```graphql
securityPolicy: "AES256_SHA256_RSAPSS"
```
- AES-256 encryption
- SHA-256 signatures with PSS padding
- Highest security level

## Device Management

### Enable/Disable Devices

```graphql
# Enable device
mutation {
  enableOpcUaDevice(name: "plc01") {
    success
    message
    device {
      name
      enabled
      connectionStatus
    }
  }
}

# Disable device
mutation {
  disableOpcUaDevice(name: "plc01") {
    success
    message
  }
}
```

### Update Device Configuration

```graphql
mutation {
  updateOpcUaDevice(
    name: "plc01"
    input: {
      config: {
        subscriptionSamplingInterval: 500.0  # Faster sampling
        addresses: [
          {
            address: "NodeId://ns=2;i=1001"
            topic: "temperature"
            publishMode: "SEPARATE"
          },
          {
            address: "NodeId://ns=2;i=1002"
            topic: "pressure"
            publishMode: "SEPARATE"
          }
        ]
      }
    }
  ) {
    success
    message
  }
}
```

### Query Devices

```graphql
# Get all devices
query {
  opcUaDevices {
    name
    namespace
    nodeId
    enabled
    connectionStatus
    config {
      endpointUrl
      securityPolicy
      addresses {
        address
        topic
        publishMode
      }
    }
  }
}

# Get specific device
query {
  opcUaDevice(name: "plc01") {
    name
    enabled
    connectionStatus
    lastConnected
  }
}
```

### Delete Devices

```graphql
mutation {
  deleteOpcUaDevice(name: "plc01") {
    success
    message
  }
}
```

## Cluster Management

### Node Assignment

Devices are assigned to specific cluster nodes:

```graphql
mutation {
  createOpcUaDevice(input: {
    name: "plc01"
    nodeId: "node1"        # Assigned to cluster node "node1"
    # ... other configuration
  }) { success }
}
```

### Load Balancing

Distribute devices across cluster nodes:

```bash
# Node 1 handles PLC devices
devices: plc01, plc02, plc03

# Node 2 handles sensor networks
devices: sensors01, sensors02

# Node 3 handles production line
devices: line01, line02
```

### Failover Handling

When a cluster node fails:
1. **Device connections are lost** on the failed node
2. **Other nodes continue** handling their assigned devices
3. **Manual reassignment** required for failed node devices
4. **Future enhancement** will add automatic failover

## Message Flow

### Data Flow Architecture

```
OPC UA Server → MonsterMQ OPC UA Client → MQTT Topics → Archive Groups
```

### Topic Mapping

**NodeId Example:**
```
OPC UA: ns=2;i=1001 (Temperature)
MQTT Topic: opcua/factory/plc01/temperature
```

**BrowsePath Example:**
```
OPC UA: Objects/Factory/Line1/Station1/Temperature
MQTT Topic: opcua/factory/plc01/production/Station1/Temperature
```

### Message Format

OPC UA values are published as MQTT messages:

```json
{
  "value": 23.5,
  "quality": "Good",
  "timestamp": "2024-01-15T10:30:00.123Z",
  "sourceTimestamp": "2024-01-15T10:29:59.890Z"
}
```

## Monitoring and Diagnostics

### Connection Status

Monitor device connections:

```graphql
query {
  opcUaDevices {
    name
    connectionStatus    # "CONNECTED", "DISCONNECTED", "CONNECTING", "ERROR"
    lastConnected      # ISO timestamp
    lastError          # Error message if any
  }
}
```

### Log Monitoring

```bash
# Monitor OPC UA connections
tail -f log/monstermq.log | grep "OPC UA"

# Certificate generation
tail -f log/monstermq.log | grep "certificate"

# Connection errors
tail -f log/monstermq.log | grep "Failed to connect"
```

### Health Checks

```bash
# Check if certificates exist
ls -la security/monstermq-opcua-client-device1.pfx

# Test OPC UA endpoint connectivity
telnet 192.168.1.100 4840

# Monitor MQTT topics
mosquitto_sub -h localhost -p 1883 -t "opcua/#"
```

## Integration with MQTT Features

### Archive Groups

OPC UA data integrates with MonsterMQ's archiving system:

```graphql
# Create archive group for OPC UA data
mutation {
  createArchiveGroup(input: {
    name: "OpcUaProduction"
    enabled: true
    topicFilter: ["opcua/factory/#"]
    lastValType: POSTGRES
    archiveType: POSTGRES
    lastValRetention: "7d"
    archiveRetention: "30d"
  }) { success }
}
```

### User Management

Apply ACL rules to OPC UA topics:

```graphql
# Restrict access to OPC UA data
mutation {
  createAclRule(input: {
    username: "operator"
    topicPattern: "opcua/factory/+/temperature"
    canSubscribe: true
    canPublish: false
  }) { success }
}
```

### GraphQL Queries

Query OPC UA data like any MQTT data:

```graphql
query {
  currentValue(topic: "opcua/factory/plc01/temperature") {
    payload
    timestamp
  }
}
```

## Best Practices

### Security

1. **Use appropriate security policies:**
   - Development: `"None"` for simplicity
   - Production: `"Basic256Sha256"` or higher

2. **Certificate management:**
   - Use unique applicationUri per deployment
   - Backup certificate files
   - Monitor certificate expiration

3. **Network security:**
   - Isolate OPC UA networks
   - Use VPN for remote connections
   - Monitor connection attempts

### Performance

1. **Sampling intervals:**
   - Match OPC UA server capabilities
   - Consider network bandwidth
   - Balance latency vs. load

2. **Address configuration:**
   - Use specific NodeId addresses when possible
   - Limit BrowsePath wildcards scope
   - Group related addresses

3. **Cluster distribution:**
   - Balance devices across nodes
   - Consider geographic proximity
   - Monitor node resource usage

### Reliability

1. **Connection monitoring:**
   - Set appropriate timeouts
   - Monitor connection status
   - Plan for network interruptions

2. **Error handling:**
   - Monitor logs for connection issues
   - Implement retry strategies
   - Handle certificate errors

3. **Backup strategies:**
   - Backup device configurations
   - Store certificates securely
   - Document network topology

## Example Configurations

### Simple Temperature Monitoring

```graphql
mutation {
  createOpcUaDevice(input: {
    name: "temp_sensor"
    namespace: "opcua/sensors"
    nodeId: "node1"
    enabled: true
    config: {
      endpointUrl: "opc.tcp://sensor.local:4840"
      securityPolicy: "None"
      addresses: [
        {
          address: "NodeId://ns=2;i=1001"
          topic: "temperature"
          publishMode: "SEPARATE"
        }
      ]
    }
  }) { success }
}
```

### Production Line Monitoring

```graphql
mutation {
  createOpcUaDevice(input: {
    name: "production_line1"
    namespace: "opcua/production/line1"
    nodeId: "node1"
    enabled: true
    config: {
      endpointUrl: "opc.tcp://plc.factory.local:4840"
      securityPolicy: "Basic256Sha256"
      username: "monitor"
      password: "monitor123"
      subscriptionSamplingInterval: 500.0
      addresses: [
        {
          address: "BrowsePath://Objects/ProductionLine/Stations/#"
          topic: "stations"
          publishMode: "SEPARATE"
          removePath: true
        },
        {
          address: "BrowsePath://Objects/ProductionLine/Alarms/#"
          topic: "alarms"
          publishMode: "SEPARATE"
          removePath: true
        }
      ]
      certificateConfig: {
        applicationName: "MonsterMQ@ProductionLine1"
        applicationUri: "urn:MonsterMQ:Production:Line1"
        organization: "Factory Corp"
        createSelfSigned: true
      }
    }
  }) { success }
}
```

## Troubleshooting

### Common Issues

**1. Certificate Errors**
```bash
# Check certificate exists
ls -la security/monstermq-opcua-client-device1.pfx

# Verify certificate details
keytool -list -v -keystore security/monstermq-opcua-client-device1.pfx -storetype PKCS12

# Trust client certificate on OPC UA server
```

**2. Connection Timeouts**
```bash
# Test network connectivity
telnet opcua-server 4840

# Check firewall rules
sudo iptables -L | grep 4840

# Verify endpoint URL
```

**3. Authentication Failures**
```bash
# Verify username/password
# Check OPC UA server user configuration
# Ensure proper security policy
```

**4. Subscription Issues**
```bash
# Verify NodeId exists on server
# Check BrowsePath syntax
# Monitor sampling intervals
```

## Related Documentation

- **[Configuration Reference](configuration.md)** - OPC UA configuration parameters
- **[Security](security.md)** - Certificate and security best practices
- **[Clustering](clustering.md)** - Multi-node device management
- **[GraphQL API](graphql.md)** - Device management API