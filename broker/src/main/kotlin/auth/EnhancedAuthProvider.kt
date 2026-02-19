package at.rocworks.auth

import io.vertx.core.Future

/**
 * Enhanced Authentication Provider Interface for MQTT v5.0
 * 
 * Implements enhanced authentication as described in MQTT v5.0 specification section 4.12.
 * Enhanced authentication extends the authentication methods beyond basic username/password
 * to support challenge/response mechanisms like SCRAM-SHA-256.
 * 
 * Note: Full AUTH packet support requires Vert.x MQTT enhancements.
 * Current implementation provides the foundation for enhanced authentication
 * and will fully integrate when Vert.x adds AUTH packet API support.
 */
interface EnhancedAuthProvider {
    
    /**
     * Get the authentication method name (e.g., "SCRAM-SHA-256")
     * This corresponds to MQTT v5 property 21 (Authentication Method)
     */
    fun getAuthMethod(): String
    
    /**
     * Start enhanced authentication flow with client's initial authentication data
     * 
     * @param clientId The MQTT client identifier
     * @param authData Initial authentication data from client (MQTT v5 property 22)
     * @return Future with AuthResult containing continue/success status and optional response data
     */
    fun startAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    
    /**
     * Continue authentication flow with client's response data
     * 
     * @param clientId The MQTT client identifier
     * @param authData Client's authentication response data
     * @return Future with AuthResult containing continue/success/fail status
     */
    fun continueAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    
    /**
     * Clean up authentication state for a client (e.g., on disconnect)
     * 
     * @param clientId The MQTT client identifier
     */
    fun cleanupAuth(clientId: String)
}

/**
 * Result of enhanced authentication step
 */
data class AuthResult(
    /**
     * Authentication status
     */
    val status: AuthStatus,
    
    /**
     * Optional authentication data to send back to client
     * This will be sent in AUTH packet (property 22 - Authentication Data)
     */
    val responseData: ByteArray? = null,
    
    /**
     * Optional reason string for failures
     */
    val reasonString: String? = null,
    
    /**
     * Authenticated username (only set on SUCCESS)
     */
    val username: String? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AuthResult

        if (status != other.status) return false
        if (responseData != null) {
            if (other.responseData == null) return false
            if (!responseData.contentEquals(other.responseData)) return false
        } else if (other.responseData != null) return false
        if (reasonString != other.reasonString) return false
        if (username != other.username) return false

        return true
    }

    override fun hashCode(): Int {
        var result = status.hashCode()
        result = 31 * result + (responseData?.contentHashCode() ?: 0)
        result = 31 * result + (reasonString?.hashCode() ?: 0)
        result = 31 * result + (username?.hashCode() ?: 0)
        return result
    }
}

/**
 * Authentication flow status
 */
enum class AuthStatus {
    /**
     * Authentication completed successfully - client is authenticated
     */
    SUCCESS,
    
    /**
     * Authentication requires another round - send AUTH packet to client
     */
    CONTINUE,
    
    /**
     * Authentication failed - reject connection
     */
    FAILED
}
