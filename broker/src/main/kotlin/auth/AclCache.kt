package at.rocworks.auth

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.TopicTree
import at.rocworks.data.User
import at.rocworks.stores.IUserManagementStore
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class AclCache {
    private val logger = Utils.getLogger(this::class.java)
    
    init {
        logger.level = Const.DEBUG_LEVEL
    }

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
    suspend fun loadFromStore(store: IUserManagementStore) {
        val startTime = System.currentTimeMillis()
        logger.info("Loading users and ACL rules into memory cache...")
        
        try {
            val (allUsers, allAcls) = store.loadAllUsersAndAcls()
            
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
            logger.info("Loaded ${allUsers.size} users and ${allAcls.size} ACL rules in ${duration}ms")
        } catch (e: Exception) {
            logger.severe("Failed to load users and ACL rules: ${e.message}")
            throw e
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
     */
    fun checkSubscribePermission(username: String, topicFilter: String): Boolean {
        val cacheKey = "SUB:$username:$topicFilter"
        
        // Check cache first
        permissionCache[cacheKey]?.let { return it }
        
        val result = checkPermissionInternal(username, topicFilter, true)
        
        // Cache the result if cache isn't full
        if (permissionCache.size < maxCacheSize) {
            permissionCache[cacheKey] = result
        }
        
        return result
    }
    
    /**
     * Check if user can publish to a specific topic
     */
    fun checkPublishPermission(username: String, topic: String): Boolean {
        val cacheKey = "PUB:$username:$topic"
        
        // Check cache first
        permissionCache[cacheKey]?.let { return it }
        
        val result = checkPermissionInternal(username, topic, false)
        
        // Cache the result if cache isn't full
        if (permissionCache.size < maxCacheSize) {
            permissionCache[cacheKey] = result
        }
        
        return result
    }
    
    /**
     * Internal permission checking logic
     */
    private fun checkPermissionInternal(username: String, topic: String, isSubscribe: Boolean): Boolean {
        val user = users[username] ?: return false
        
        // Admin users bypass ACL checks
        if (user.isAdmin) return true
        
        // Check general user permissions first
        val hasGeneralPermission = if (isSubscribe) user.canSubscribe else user.canPublish
        if (!hasGeneralPermission) return false
        
        // Get user's ACL rules
        val rules = userAcls[username] ?: return false
        
        // Check rules in priority order (highest priority first)
        for (rule in rules) {
            val ruleApplies = if (isSubscribe) rule.canSubscribe else rule.canPublish
            if (!ruleApplies) continue
            
            if (topicMatches(rule.topicPattern, topic)) {
                logger.finest { "ACL rule match: user=$username, topic=$topic, pattern=${rule.topicPattern}, allow=${if (isSubscribe) "subscribe" else "publish"}" }
                return true
            }
        }
        
        // No matching allow rule found
        logger.finest { "No ACL rule match: user=$username, topic=$topic, operation=${if (isSubscribe) "subscribe" else "publish"}" }
        return false
    }
    
    /**
     * Check if a topic matches an MQTT topic pattern (supports + and # wildcards)
     */
    private fun topicMatches(pattern: String, topic: String): Boolean {
        val patternLevels = Utils.getTopicLevels(pattern)
        val topicLevels = Utils.getTopicLevels(topic)
        
        return topicMatchesRecursive(patternLevels, topicLevels, 0, 0)
    }
    
    /**
     * Recursive topic matching implementation
     */
    private fun topicMatchesRecursive(
        pattern: List<String>, 
        topic: List<String>, 
        pIndex: Int, 
        tIndex: Int
    ): Boolean {
        // If we've consumed all pattern levels
        if (pIndex >= pattern.size) {
            return tIndex >= topic.size // Pattern matches if topic is also consumed
        }
        
        // If we've consumed all topic levels but pattern has more
        if (tIndex >= topic.size) {
            // Only match if remaining pattern is just "#"
            return pIndex == pattern.size - 1 && pattern[pIndex] == "#"
        }
        
        val currentPattern = pattern[pIndex]
        val currentTopic = topic[tIndex]
        
        return when (currentPattern) {
            "#" -> true // Multi-level wildcard matches everything remaining
            "+" -> {
                // Single-level wildcard, continue with next level
                topicMatchesRecursive(pattern, topic, pIndex + 1, tIndex + 1)
            }
            else -> {
                // Exact match required
                if (currentPattern == currentTopic) {
                    topicMatchesRecursive(pattern, topic, pIndex + 1, tIndex + 1)
                } else {
                    false
                }
            }
        }
    }
    
    /**
     * Clear the permission cache (useful after ACL updates)
     */
    fun clearPermissionCache() {
        permissionCache.clear()
        logger.fine("Permission cache cleared")
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