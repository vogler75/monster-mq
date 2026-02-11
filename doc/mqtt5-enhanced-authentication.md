# MQTT v5.0 Enhanced Authentication

## Overview

MonsterMQ implements MQTT v5.0 enhanced authentication as specified in the MQTT v5.0 specification section 3.15 and section 3.1.2.11. Enhanced authentication extends basic username/password authentication to support challenge-response mechanisms like SCRAM-SHA-256.

## Current Status

**Phase 6: Enhanced Authentication - COMPLETED (February 2, 2026)**

✅ **Implemented:**
- Authentication Method parsing (CONNECT property 21)
- Authentication Data parsing (CONNECT property 22)
- Enhanced authentication provider interface
- SCRAM-SHA-256 implementation (RFC 7677)
- Pluggable authentication architecture
- Configuration support

⏳ **Pending Vert.x MQTT API:**
- AUTH packet handling (send/receive)
- Full challenge-response flow
- Re-authentication support

## Architecture

### Enhanced Authentication Provider Interface

```kotlin
interface EnhancedAuthProvider {
    fun getAuthMethod(): String
    fun startAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    fun continueAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    fun cleanupAuth(clientId: String)
}

data class AuthResult(
    val status: AuthStatus,
    val responseData: ByteArray? = null,
    val reasonString: String? = null,
    val username: String? = null
)

enum class AuthStatus {
    SUCCESS,   // Authentication completed
    CONTINUE,  // Send AUTH packet to client
    FAILED     // Reject connection
}
```

### SCRAM-SHA-256 Provider

MonsterMQ includes a full implementation of SCRAM-SHA-256 (Salted Challenge Response Authentication Mechanism) as defined in RFC 7677 and RFC 5802.

**Features:**
- Mutual authentication (both client and server verify each other)
- HMAC-SHA-256 with 4096 iterations (RFC recommended minimum)
- Cryptographically secure nonce and salt generation
- Session management for multi-step authentication
- Protection against eavesdropping and replay attacks

**Authentication Flow:**

```
Client                           Server
  |                                |
  |  CONNECT                       |
  |  - Auth Method: SCRAM-SHA-256  |
  |  - Auth Data: client-first     |
  |------------------------------->|
  |                                | Parse properties
  |                                | startAuth()
  |                                | Generate nonce, salt
  |                                |
  |  CONNACK or AUTH               |
  |  - Auth Data: server-first     |
  |<-------------------------------|
  |                                |
Client computes proof             |
  |                                |
  |  AUTH                          |
  |  - Auth Data: client-final     |
  |------------------------------->|
  |                                | continueAuth()
  |                                | Verify proof
  |                                | Generate signature
  |                                |
  |  AUTH                          |
  |  - Auth Data: server-final     |
  |<-------------------------------|
  |                                |
Client verifies signature         |
  |                                |
  |  Connection established        |
  |<==============================>|
```

## Configuration

Add enhanced authentication configuration to `config.yaml`:

```yaml
UserManagement:
  Enabled: true
  PasswordAlgorithm: bcrypt
  
  # MQTT v5.0 Enhanced Authentication
  EnhancedAuth:
    Enabled: true
    Methods:
      - SCRAM-SHA-256
```

## Usage

### Client Example (Python with paho-mqtt)

```python
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import base64

# Create MQTT v5 client
client = mqtt.Client(
    client_id="enhanced-auth-client",
    protocol=mqtt.MQTTv5
)

# Set credentials for fallback
client.username_pw_set("username", "password")

# Create CONNECT properties with authentication method
connect_properties = Properties(PacketTypes.CONNECT)
connect_properties.AuthenticationMethod = "SCRAM-SHA-256"

# Build client-first-message
client_nonce = base64.b64encode(b"random_nonce").decode('ascii')
client_first = f"n,,n=username,r={client_nonce}"
connect_properties.AuthenticationData = client_first.encode('utf-8')

# Connect with enhanced auth
client.connect(
    host="localhost",
    port=1883,
    properties=connect_properties
)
```

## Implementation Details

### Property Parsing

The broker parses authentication properties from the CONNECT packet:

```kotlin
when (p.propertyId()) {
    21 -> mqtt5AuthMethod = p.value() as? String  // Authentication Method
    22 -> mqtt5AuthData = (p.value() as? ByteArray)  // Authentication Data
}
```

### Current Limitation

Vert.x MQTT 5.0.7 does not provide APIs for sending/receiving AUTH packets during the authentication flow. The implementation currently:

1. ✅ Parses authentication method and data from CONNECT
2. ✅ Logs the enhanced authentication attempt
3. ⚠️ Falls back to basic username/password authentication
4. ⏳ Awaits Vert.x MQTT API enhancement for AUTH packet

When AUTH packet support is added to Vert.x MQTT, the full challenge-response flow will activate automatically.

### Fallback Behavior

When enhanced authentication is requested but not fully supported:

```
[INFO] Client [test-client] MQTT 5.0 enhanced authentication: method=SCRAM-SHA-256
[WARN] Enhanced authentication (SCRAM-SHA-256) not yet supported - AUTH packet handling requires Vert.x MQTT API enhancement
[INFO] Falling back to basic username/password authentication
[INFO] Client [test-client] Authentication successful for user [testuser]
```

## Testing

Run Phase 6 test script:

```bash
python tests/test_mqtt5_phase6_enhanced_auth.py
```

**Test Coverage:**
1. ✅ Authentication method and data parsing from CONNECT
2. ✅ Enhanced auth detection and logging
3. ✅ Graceful fallback to basic authentication
4. ✅ Normal MQTT v5 connection without enhanced auth

## Security Considerations

### SCRAM-SHA-256 Benefits

- **Password not transmitted:** Only hashed proofs are sent
- **Mutual authentication:** Client verifies server, server verifies client
- **Replay protection:** Nonces prevent replay attacks
- **Salted passwords:** 4096 iterations with random salt
- **Channel binding:** Optional support for TLS channel binding

### Recommendations

1. **Use TLS:** Enhanced authentication should be used over TLS (port 8883)
2. **Strong passwords:** Minimum 12 characters with complexity
3. **Monitor attempts:** Log and alert on failed authentication
4. **Regular rotation:** Rotate passwords and salts periodically

## Password Storage

For full SCRAM-SHA-256 support, passwords should be stored in SCRAM format:

```
StoredKey = SHA256(ClientKey)
where ClientKey = HMAC(SaltedPassword, "Client Key")
where SaltedPassword = Hi(password, salt, iterations)
```

Current implementation uses bcrypt-hashed passwords and performs simplified verification for compatibility. When AUTH packet support is available, password storage can be migrated to SCRAM format.

## Future Enhancements

When Vert.x MQTT adds AUTH packet support:

1. **Full challenge-response flow:**
   - Multi-round authentication with AUTH packets
   - Server-initiated re-authentication
   - Enhanced error reporting with reason codes

2. **Additional authentication methods:**
   - SCRAM-SHA-512 (RFC 7677)
   - OAuth 2.0 / JWT authentication
   - Custom authentication providers

3. **Password format migration:**
   - SCRAM-specific password storage
   - Backward compatibility mode
   - Migration tools for existing users

## References

- [MQTT v5.0 Specification - Section 3.15 (AUTH Packet)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217)
- [MQTT v5.0 Specification - Section 3.1.2.11 (Authentication)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901056)
- [RFC 7677 - SCRAM-SHA-256](https://tools.ietf.org/html/rfc7677)
- [RFC 5802 - SCRAM Mechanism](https://tools.ietf.org/html/rfc5802)
- [Vert.x MQTT GitHub](https://github.com/vert-x3/vertx-mqtt)

## Support

For issues or questions about enhanced authentication:

1. Check broker logs for authentication attempts
2. Verify configuration in config.yaml
3. Test with provided Python test script
4. Monitor Vert.x MQTT releases for AUTH packet support
