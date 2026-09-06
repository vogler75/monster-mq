package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import graphql.schema.DataFetchingEnvironment
import io.vertx.ext.web.RoutingContext
import java.util.logging.Logger

/**
 * GraphQL Authorization Context
 * Handles JWT token validation and user context for GraphQL operations
 */
class GraphQLAuthContext(
    private val userManager: UserManager
) {
    companion object {
        private val logger: Logger = Utils.getLogger(GraphQLAuthContext::class.java)
        private const val CONTEXT_KEY = "authContext"
        
        // Operations that don't require authentication (always public)
        private val PUBLIC_OPERATIONS = setOf("login")

        // Mutations that require authentication (write-protected).
        // publish / publishBatch are NOT in this set — their ACL is enforced by the resolver itself.
        private val WRITE_PROTECTED_MUTATIONS = setOf(
            "purgeQueuedMessages",
            "user",
            "session",
            "archiveGroup",
            "importDevices",
            "opcUaDevice",
            "opcUaServer",
            "mqttClient",
            "kafkaClient",
            "natsClient",
            "redisClient",
            "telegramClient",
            "winCCOaDevice",
            "winCCUaDevice",
            "plc4xDevice",
            "neo4jClient",
            "jdbcLogger",
            "influxdbLogger",
            "timebaseLogger",
            "sparkplugBDecoder",
            "flow",
            "topicSchemaPolicy",
            "topicNamespace",
            "agent",
            "mcpServer",
            "genAiProvider"
        )
    }

    /**
     * Extract and validate authentication context from request
     */
    fun extractAuthContext(routingContext: RoutingContext): AuthContext? {
        val authHeader = routingContext.request().getHeader("Authorization")
        return extractAuthContextFromHeader(authHeader)
    }

    fun extractAuthContextFromHeader(authHeader: String?): AuthContext? {
        val token = JwtService.extractTokenFromHeader(authHeader)
        return extractAuthContextFromToken(token)
    }

    fun extractAuthContextFromToken(token: String?): AuthContext? {
        if (token == null) {
            return null
        }
        
        val username = JwtService.extractUsername(token)
        
        if (username == null || JwtService.isTokenExpired(token)) {
            return null
        }

        val isAdmin = if (userManager.isUserManagementEnabled()) {
            val user = userManager.getUser(username)
            if (user == null || !user.enabled) {
                return null
            }
            user.isAdmin
        } else {
            JwtService.extractIsAdmin(token)
        }
        
        return AuthContext(
            username = username,
            isAdmin = isAdmin,
            token = token
        )
    }

    /**
     * Check if operation requires authentication
     */
    fun requiresAuthentication(operationName: String?): Boolean {
        return operationName != null && !PUBLIC_OPERATIONS.contains(operationName)
    }

    /**
     * Check if user has admin privileges
     */
    fun requiresAdmin(fieldName: String): Boolean {
        return when (fieldName) {
            // User Management operations require admin
            "createUser", "updateUser", "deleteUser", "setPassword",
            "getAllUsers", "getUser",
            // ACL Management operations require admin
            "createAclRule", "updateAclRule", "deleteAclRule",
            "getAllAclRules", "getUserAclRules",
            // Destructive / device-import operations require admin
            "purgeQueuedMessages", "importDevices",
            // Configuration-changing mutation groups require admin.
            // Checked at the parent mutation field (e.g. `mutation { agent { create(...) } }`)
            // before any child resolver runs.
            "user", "session", "archiveGroup",
            "opcUaDevice", "opcUaServer",
            "mqttClient", "kafkaClient", "natsClient",
            "redisClient", "telegramClient",
            "winCCOaDevice", "winCCUaDevice", "plc4xDevice", "neo4jClient",
            "jdbcLogger", "influxdbLogger", "timebaseLogger",
            "sparkplugBDecoder", "flow",
            "topicSchemaPolicy", "topicNamespace",
            "agent", "mcpServer", "genAiProvider" -> true
            else -> false
        }
    }

    /**
     * Check if user can access a specific topic for subscription
     */
    fun canSubscribeToTopic(authContext: AuthContext?, topic: String): Boolean {
        if (!userManager.isUserManagementEnabled()) {
            logger.fine { "User management disabled, allowing subscribe to $topic" }
            return true // No user management, allow everything
        }
        
        if (authContext == null) {
            // No authentication - check Anonymous user permissions
            val result = userManager.canSubscribe("Anonymous", topic)
            logger.fine("Anonymous user subscribe check for topic $topic: $result")
            return result
        }

        val isAdmin = if (userManager.isUserManagementEnabled()) userManager.isAdmin(authContext.username) else authContext.isAdmin
        if (isAdmin) {
            logger.fine("Admin user ${authContext.username} allowed to subscribe to $topic")
            return true // Admin can access everything
        }

        val result = userManager.canSubscribe(authContext.username, topic)
        logger.fine("User ${authContext.username} subscribe check for topic $topic: $result")
        return result
    }

    /**
     * Check if user can publish to a specific topic
     */
    fun canPublishToTopic(authContext: AuthContext?, topic: String): Boolean {
        if (!userManager.isUserManagementEnabled()) {
            logger.fine("User management disabled, allowing publish to $topic")
            return true // No user management, allow everything
        }

        if (authContext == null) {
            // No authentication - check Anonymous user permissions
            val result = userManager.canPublish("Anonymous", topic)
            logger.fine("Anonymous user publish check for topic $topic: $result")
            return result
        }

        val isAdmin = if (userManager.isUserManagementEnabled()) userManager.isAdmin(authContext.username) else authContext.isAdmin
        if (isAdmin) {
            logger.fine("Admin user ${authContext.username} allowed to publish to $topic")
            return true // Admin can access everything
        }

        val result = userManager.canPublish(authContext.username, topic)
        logger.fine("User ${authContext.username} publish check for topic $topic: $result")
        return result
    }

    /**
     * Check if user has global subscribe permission (for searchTopics)
     */
    fun hasGlobalSubscribePermission(authContext: AuthContext?): Boolean {
        if (!userManager.isUserManagementEnabled()) {
            return true // No user management, allow everything
        }
        
        if (authContext == null) {
            // Check Anonymous user global permissions
            val anonymousUser = userManager.getUser("Anonymous")
            return anonymousUser?.canSubscribe == true
        }
        
        val isAdmin = if (userManager.isUserManagementEnabled()) userManager.isAdmin(authContext.username) else authContext.isAdmin
        if (isAdmin) {
            return true // Admin can access everything
        }
        
        val user = userManager.getUser(authContext.username)
        return user?.canSubscribe == true
    }

    /**
     * Validate authorization for a GraphQL field
     */
    fun validateFieldAccess(env: DataFetchingEnvironment): AuthorizationResult {
        return validateFieldAccess(env.field.name)
    }

    fun validateFieldAccess(fieldName: String): AuthorizationResult {
        // Get auth context from thread-local service
        val authContext: AuthContext? = AuthContextService.getAuthContext()
        
        // Check if user management is enabled
        if (!userManager.isUserManagementEnabled()) {
            return AuthorizationResult.allowed() // No user management, allow everything
        }

        // Always allow public operations (login)
        if (fieldName in PUBLIC_OPERATIONS) {
            return AuthorizationResult.allowed()
        }

        // publish / publishBatch: delegate ACL check entirely to the resolver
        if (fieldName == "publish" || fieldName == "publishBatch") {
            return AuthorizationResult.allowed()
        }

        // Check if user account is disabled or deleted
        if (authContext != null) {
            val user = userManager.getUser(authContext.username)
            if (user == null || !user.enabled) {
                return AuthorizationResult.denied("Authentication required")
            }
        }

        // Write-protected mutations require a valid auth context
        if (fieldName in WRITE_PROTECTED_MUTATIONS) {
            if (authContext == null) {
                return AuthorizationResult.denied("Login required")
            }
        }

        // Admin-only fields require an authenticated admin
        if (requiresAdmin(fieldName)) {
            if (authContext == null) {
                return AuthorizationResult.denied("Authentication required")
            }
            if (!userManager.isAdmin(authContext.username)) {
                return AuthorizationResult.denied("Admin privileges required")
            }
        }

        // Everything else (queries, subscriptions, non-write mutations) is allowed
        return AuthorizationResult.allowed()
    }
}

/**
 * Result of authorization check
 */
data class AuthorizationResult(
    val allowed: Boolean,
    val errorMessage: String? = null
) {
    companion object {
        fun allowed() = AuthorizationResult(true)
        fun denied(message: String) = AuthorizationResult(false, message)
    }
}