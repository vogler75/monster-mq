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
                load(allUsers, allAcls)

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
     * Replace the cached users and ACL rules.
     */
    fun load(allUsers: List<User>, allAcls: List<AclRule>) {
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
            // Highest priority first; on equal priority the deny rule is checked first
            userAcls[username] = rules.sortedWith(
                compareByDescending<AclRule> { it.priority }.thenByDescending { isDenyRule(it) }
            )

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
        if (!user.enabled) return false
        
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
        
        // Check rules in priority order (highest priority first, deny first on
        // equal priority); the first matching rule that decides wins.
        // - canSubscribe=false and canPublish=false: deny rule for both operations
        // - otherwise: allow rule for the operations set to true; it is skipped
        //   for the other operation
        // A wildcard subscription that only partly overlaps a deny rule is
        // admitted; publishMessage re-checks the concrete topic on delivery.
        for (rule in rules) {
            val deny = isDenyRule(rule)
            val grants = if (isSubscribe) rule.canSubscribe else rule.canPublish
            if (!deny && !grants) continue
            
            val resolvedPattern = resolvePattern(rule.topicPattern, username, clientId)
            if (resolvedPattern != null && aclMatches(resolvedPattern, topic)) {
                logger.finest { "ACL rule match: user=$username, topic=$topic, pattern=${rule.topicPattern}, resolved=$resolvedPattern, ${if (deny) "deny" else "allow"}=${if (isSubscribe) "subscribe" else "publish"}" }
                return !deny
            }
        }
        
        // No matching rule found among existing rules
        logger.finest { "No ACL rule match: user=$username, topic=$topic, operation=${if (isSubscribe) "subscribe" else "publish"}" }
        return false
    }
    
    private fun isDenyRule(rule: AclRule): Boolean = !rule.canSubscribe && !rule.canPublish

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
     * Check if the ACL pattern covers topic, which is either a concrete topic or a
     * subscription filter (then every topic the filter can match must be covered).
     * Wildcards at the first level do not cover topics starting with '$' (MQTT 4.7.2).
     * Same as topicMatches in the Go edge broker (internal/auth/auth.go).
     */
    private fun aclMatches(pattern: String, topic: String): Boolean {
        val pp = pattern.split('/')
        val tt = topic.split('/')
        if (tt[0].startsWith('$') && !pp[0].startsWith('$') && (pp[0] == "+" || pp[0] == "#")) return false
        for (i in pp.indices) {
            val p = pp[i]
            if (p == "#") return true
            if (i >= tt.size) return false
            if (p == "+") {
                // A single-level wildcard does not cover a multi-level filter
                if (tt[i] == "#") return false
                continue
            }
            if (p != tt[i]) return false
        }
        return pp.size == tt.size
    }
    
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