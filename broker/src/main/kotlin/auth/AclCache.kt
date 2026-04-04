package auth

import at.rocworks.data.TopicTree
import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.IUserStore
import io.vertx.core.Future
import java.util.concurrent.ConcurrentHashMap

class AclCache {
    private val logger = Utils.getLogger(this::class.java)
    

    // User data cached in memory
    private val users = ConcurrentHashMap<String, User>()
    
    // ACL rules organized by username for fast lookup
    private val userAcls = ConcurrentHashMap<String, List<AclRule>>()
    
    // Topic trees for efficient wildcard matching
    private val subscribeTopicTree = TopicTree<String, AclRule>()
    private val publishTopicTree = TopicTree<String, AclRule>()
    
    // Permission result cache for frequently checked combinations
    private val permissionCache = ConcurrentHashMap<String, Boolean>()
    private val maxCacheSize = 10000
    
    /**
     * Load all users and ACL rules from the store into memory
     */
    fun loadFromStore(store: IUserStore): Future<Void> {
        val startTime = System.currentTimeMillis()
        logger.fine { "Loading users and ACL rules into memory cache..." }

        return store.loadAllUsersAndAcls().compose { (allUsers, allAcls) ->
            try {
                // Clear existing data
                users.clear()
                userAcls.clear()
                permissionCache.clear()

                // Load users
                allUsers.forEach { user ->
                    users[user.username] = user
                }

                // Group ACL rules by username and build topic trees
                val groupedAcls = allAcls.groupBy { it.username }
                groupedAcls.forEach { (username, rules) ->
                    userAcls[username] = rules.sortedByDescending { it.priority }

                    // Add rules to topic trees for efficient matching
                    rules.forEach { rule ->
                        if (rule.canSubscribe) {
                            subscribeTopicTree.add(rule.topicPattern, username, rule)
                        }
                        if (rule.canPublish) {
                            publishTopicTree.add(rule.topicPattern, username, rule)
                        }
                    }
                }

                val endTime = System.currentTimeMillis()
                val duration = endTime - startTime
                logger.fine { "Loaded ${allUsers.size} users and ${allAcls.size} ACL rules in ${duration}ms" }
                Future.succeededFuture<Void>()
            } catch (e: Exception) {
                logger.severe("Failed to load users and ACL rules: ${e.message}")
                Future.failedFuture<Void>(e)
            }
        }.recover { throwable ->
            logger.severe("Failed to load users and ACL rules: ${throwable.message}")
            Future.succeededFuture<Void>()
        }
    }
    
    /**
     * Get a user by username
     */
    fun getUser(username: String): User? {
        return users[username]
    }
    
    /**
     * Check if user exists and is enabled
     */
    fun isUserValid(username: String): Boolean {
        return users[username]?.enabled == true
    }
    
    /**
     * Check if user is an admin (bypasses ACL checks)
     */
    fun isUserAdmin(username: String): Boolean {
        return users[username]?.isAdmin == true
    }
    
    /**
     * Check if user has general subscribe permission
     */
    fun canUserSubscribe(username: String): Boolean {
        return users[username]?.canSubscribe == true
    }
    
    /**
     * Check if user has general publish permission
     */
    fun canUserPublish(username: String): Boolean {
        return users[username]?.canPublish == true
    }
    
    /**
     * Check if user can subscribe to a specific topic
     * @param clientId optional MQTT client ID for %c substitution in ACL patterns
     */
    fun checkSubscribePermission(username: String, topicFilter: String, clientId: String? = null): Boolean {
        val cacheKey = "SUB:$username:${clientId ?: ""}:$topicFilter"
        
        // Check cache first
        permissionCache[cacheKey]?.let { return it }
        
        val result = checkPermissionInternal(username, topicFilter, true, clientId)
        
        // Cache the result if cache isn't full
        if (permissionCache.size < maxCacheSize) {
            permissionCache[cacheKey] = result
        }
        
        return result
    }
    
    /**
     * Check if user can publish to a specific topic
     * @param clientId optional MQTT client ID for %c substitution in ACL patterns
     */
    fun checkPublishPermission(username: String, topic: String, clientId: String? = null): Boolean {
        val cacheKey = "PUB:$username:${clientId ?: ""}:$topic"
        
        // Check cache first
        permissionCache[cacheKey]?.let { return it }
        
        val result = checkPermissionInternal(username, topic, false, clientId)
        
        // Cache the result if cache isn't full
        if (permissionCache.size < maxCacheSize) {
            permissionCache[cacheKey] = result
        }
        
        return result
    }
    
    /**
     * Internal permission checking logic.
     * Supports %c (client ID) and %u (username) placeholders in ACL topic patterns,
     * similar to Mosquitto's ACL substitution.
     */
    private fun checkPermissionInternal(username: String, topic: String, isSubscribe: Boolean, clientId: String? = null): Boolean {
        val user = users[username] ?: return false
        
        // Admin users bypass ACL checks
        if (user.isAdmin) return true
        
        // Check general user permissions first
        val hasGeneralPermission = if (isSubscribe) user.canSubscribe else user.canPublish
        if (!hasGeneralPermission) return false
        
        // Get user's ACL rules
        val rules = userAcls[username]
        
        // If no ACL rules exist for this user, allow access based on general permissions
        if (rules == null || rules.isEmpty()) {
            logger.finest { "No ACL rules for user=$username, allowing based on general permissions: ${if (isSubscribe) "subscribe" else "publish"}=$hasGeneralPermission" }
            return hasGeneralPermission
        }
        
        // Check rules in priority order (highest priority first)
        for (rule in rules) {
            val ruleApplies = if (isSubscribe) rule.canSubscribe else rule.canPublish
            if (!ruleApplies) continue
            
            val resolvedPattern = resolvePattern(rule.topicPattern, username, clientId)
            if (resolvedPattern != null && topicMatches(resolvedPattern, topic)) {
                logger.finest { "ACL rule match: user=$username, topic=$topic, pattern=${rule.topicPattern}, resolved=$resolvedPattern, allow=${if (isSubscribe) "subscribe" else "publish"}" }
                return true
            }
        }
        
        // No matching allow rule found among existing rules
        logger.finest { "No ACL rule match: user=$username, topic=$topic, operation=${if (isSubscribe) "subscribe" else "publish"}" }
        return false
    }
    
    /**
     * Resolve %c and %u placeholders in an ACL topic pattern.
     * - %u is replaced with the username
     * - %c is replaced with the client ID (if available)
     * Returns null if the pattern contains %c but no client ID is available (no match possible).
     */
    private fun resolvePattern(pattern: String, username: String, clientId: String?): String? {
        if (!pattern.contains('%')) return pattern
        var resolved = pattern.replace("%u", username)
        if (resolved.contains("%c")) {
            if (clientId == null) return null
            resolved = resolved.replace("%c", clientId)
        }
        return resolved
    }
    
    /**
     * Check if a topic matches an MQTT topic pattern (supports + and # wildcards)
     */
    private fun topicMatches(pattern: String, topic: String): Boolean {
        return TopicTree.matches(pattern, topic)
    }
    
    /**
     * Recursive topic matching implementation
     */
    // Unified matching now via TopicTree.matches
    
    /**
     * Clear the permission cache (useful after ACL updates)
     */
    fun clearPermissionCache() {
        permissionCache.clear()
        logger.fine { "Permission cache cleared" }
    }
    
    /**
     * Get cache statistics
     */
    fun getCacheStats(): Map<String, Any> {
        return mapOf(
            "users" to users.size,
            "userAcls" to userAcls.size,
            "permissionCacheSize" to permissionCache.size,
            "maxCacheSize" to maxCacheSize
        )
    }
    
    /**
     * Get all usernames (for admin purposes)
     */
    fun getAllUsernames(): Set<String> {
        return users.keys.toSet()
    }
}