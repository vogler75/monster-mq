package at.rocworks.auth

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.AuthStoreType
import at.rocworks.stores.IUserManagement
import at.rocworks.stores.UserManagementFactory
import auth.AclCache
import auth.PasswordEncoder
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class UserManager(
    private val config: JsonObject
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)
    
    private var userStore: IUserManagement? = null
    private val aclCache = AclCache()
    private var scheduler: ScheduledExecutorService? = null
    
    // Configuration properties
    private val isEnabled: Boolean
    private val authStoreType: AuthStoreType
    private val passwordAlgorithm: String
    private val cacheRefreshInterval: Long
    private val disconnectOnUnauthorized: Boolean
    
    init {
        logger.level = Const.DEBUG_LEVEL
        
        val userMgmtConfig = config.getJsonObject("UserManagement") ?: JsonObject()
        isEnabled = userMgmtConfig.getBoolean("Enabled", false)
        authStoreType = AuthStoreType.valueOf(userMgmtConfig.getString("AuthStoreType", "SQLITE"))
        passwordAlgorithm = userMgmtConfig.getString("PasswordAlgorithm", "bcrypt")
        cacheRefreshInterval = userMgmtConfig.getLong("CacheRefreshInterval", 60)
        disconnectOnUnauthorized = userMgmtConfig.getBoolean("DisconnectOnUnauthorized", true)
        
        logger.info("User management enabled: $isEnabled")
        if (isEnabled) {
            logger.info("Auth store type: $authStoreType")
            logger.info("Password algorithm: $passwordAlgorithm")
            logger.info("Cache refresh interval: ${cacheRefreshInterval}s")
            logger.info("Disconnect on unauthorized: $disconnectOnUnauthorized")
        }
    }
    
    override fun start(startPromise: Promise<Void>) {
        if (!isEnabled) {
            logger.info("User management is disabled")
            startPromise.complete()
            return
        }
        
        try {
            // Create user store
            userStore = UserManagementFactory.create(authStoreType, config)
            
            // Initialize store and load cache
            vertx.executeBlocking(java.util.concurrent.Callable<Void> {
                kotlinx.coroutines.runBlocking {
                    userStore!!.init()
                    ensureAnonymousUser()
                    ensureDefaultAdminUser()
                    aclCache.loadFromStore(userStore!!)
                }
                null
            }).onComplete { result ->
                if (result.succeeded()) {
                    // Start periodic cache refresh
                    startCacheRefresh()
                    logger.info("User management initialized successfully")
                    startPromise.complete()
                } else {
                    logger.severe("Failed to initialize user management: ${result.cause()?.message}")
                    startPromise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error starting user management: ${e.message}")
            startPromise.fail(e)
        }
    }
    
    override fun stop(stopPromise: Promise<Void>) {
        try {
            scheduler?.shutdown()
            vertx.executeBlocking(java.util.concurrent.Callable<Void> {
                kotlinx.coroutines.runBlocking {
                    userStore?.close()
                }
                null
            }).onComplete {
                stopPromise.complete()
            }
        } catch (e: Exception) {
            logger.warning("Error stopping user management: ${e.message}")
            stopPromise.complete()
        }
    }
    
    /**
     * Check if user management is enabled
     */
    fun isUserManagementEnabled(): Boolean = isEnabled
    
    /**
     * Authenticate user with username and password
     */
    suspend fun authenticate(username: String, password: String): User? {
        if (!isEnabled) return null
        
        return try {
            userStore?.validateCredentials(username, password)
        } catch (e: Exception) {
            logger.warning("Authentication error for user [$username]: ${e.message}")
            null
        }
    }
    
    /**
     * Check if user can subscribe to a topic
     */
    fun canSubscribe(username: String, topicFilter: String): Boolean {
        if (!isEnabled) return true // If disabled, allow all
        
        return aclCache.checkSubscribePermission(username, topicFilter)
    }
    
    /**
     * Check if user can publish to a topic
     */
    fun canPublish(username: String, topic: String): Boolean {
        if (!isEnabled) return true // If disabled, allow all
        
        return aclCache.checkPublishPermission(username, topic)
    }
    
    /**
     * Check if user is admin
     */
    fun isAdmin(username: String): Boolean {
        if (!isEnabled) return false
        
        return aclCache.isUserAdmin(username)
    }
    
    /**
     * Get user by username (for admin purposes)
     */
    fun getUser(username: String): User? {
        if (!isEnabled) return null
        
        return aclCache.getUser(username)
    }
    
    /**
     * Create a new user (admin operation)
     */
    suspend fun createUser(username: String, password: String, enabled: Boolean = true, 
                          canSubscribe: Boolean = true, canPublish: Boolean = true, 
                          isAdmin: Boolean = false): Boolean {
        if (!isEnabled) return false
        
        val passwordHash = PasswordEncoder.hash(password)
        val user = User(
            username = username,
            passwordHash = passwordHash,
            enabled = enabled,
            canSubscribe = canSubscribe,
            canPublish = canPublish,
            isAdmin = isAdmin
        )
        
        return try {
            val result = userStore?.createUser(user) ?: false
            if (result) {
                // Refresh cache to include new user
                refreshCache()
            }
            result
        } catch (e: Exception) {
            logger.warning("Error creating user [$username]: ${e.message}")
            false
        }
    }
    
    /**
     * Create ACL rule for user (admin operation)
     */
    suspend fun createAclRule(username: String, topicPattern: String, canSubscribe: Boolean = false, 
                             canPublish: Boolean = false, priority: Int = 0): Boolean {
        if (!isEnabled) return false
        
        val rule = AclRule(
            id = "", // Will be generated by store
            username = username,
            topicPattern = topicPattern,
            canSubscribe = canSubscribe,
            canPublish = canPublish,
            priority = priority
        )
        
        return try {
            val result = userStore?.createAclRule(rule) ?: false
            if (result) {
                // Refresh cache to include new rule
                refreshCache()
            }
            result
        } catch (e: Exception) {
            logger.warning("Error creating ACL rule for user [$username]: ${e.message}")
            false
        }
    }
    
    /**
     * Get configuration property: disconnect on unauthorized
     */
    fun shouldDisconnectOnUnauthorized(): Boolean = disconnectOnUnauthorized
    
    /**
     * Manually refresh the ACL cache
     */
    suspend fun refreshCache() {
        if (!isEnabled) return
        
        try {
            userStore?.let { store ->
                aclCache.loadFromStore(store)
                aclCache.clearPermissionCache()
                logger.info("ACL cache refreshed manually")
            }
        } catch (e: Exception) {
            logger.severe("Error refreshing ACL cache: ${e.message}")
        }
    }
    
    /**
     * Get cache statistics
     */
    fun getCacheStats(): Map<String, Any> {
        return if (isEnabled) {
            aclCache.getCacheStats()
        } else {
            mapOf("enabled" to false)
        }
    }

    /**
     * Get all users
     */
    suspend fun getAllUsers(): List<User> {
        return if (isEnabled) {
            userStore?.getAllUsers() ?: emptyList()
        } else {
            emptyList()
        }
    }

    /**
     * Update user
     */
    suspend fun updateUser(user: User): Boolean {
        return if (isEnabled) {
            val result = userStore?.updateUser(user) ?: false
            if (result) {
                refreshCache()
            }
            result
        } else {
            false
        }
    }

    /**
     * Delete user
     */
    suspend fun deleteUser(username: String): Boolean {
        return if (isEnabled) {
            val result = userStore?.deleteUser(username) ?: false
            if (result) {
                refreshCache()
            }
            result
        } else {
            false
        }
    }

    /**
     * Set user password
     */
    suspend fun setUserPassword(username: String, newPassword: String): Boolean {
        return if (isEnabled) {
            val user = userStore?.getUser(username)
            if (user != null) {
                val hashedPassword = PasswordEncoder.hash(newPassword)
                val updatedUser = user.copy(passwordHash = hashedPassword)
                updateUser(updatedUser)
            } else {
                false
            }
        } else {
            false
        }
    }

    /**
     * Get ACL rule by ID
     */
    suspend fun getAclRule(id: String): AclRule? {
        return if (isEnabled) {
            userStore?.getAclRule(id)
        } else {
            null
        }
    }

    /**
     * Get all ACL rules
     */
    suspend fun getAllAclRules(): List<AclRule> {
        return if (isEnabled) {
            userStore?.getAllAclRules() ?: emptyList()
        } else {
            emptyList()
        }
    }

    /**
     * Get ACL rules for a specific user
     */
    suspend fun getUserAclRules(username: String): List<AclRule> {
        return if (isEnabled) {
            userStore?.getUserAclRules(username) ?: emptyList()
        } else {
            emptyList()
        }
    }

    /**
     * Update ACL rule
     */
    suspend fun updateAclRule(rule: AclRule): Boolean {
        return if (isEnabled) {
            val result = userStore?.updateAclRule(rule) ?: false
            if (result) {
                refreshCache()
            }
            result
        } else {
            false
        }
    }

    /**
     * Delete ACL rule
     */
    suspend fun deleteAclRule(id: String): Boolean {
        return if (isEnabled) {
            val result = userStore?.deleteAclRule(id) ?: false
            if (result) {
                refreshCache()
            }
            result
        } else {
            false
        }
    }

    /**
     * Get user store for direct access (used by GraphQL resolver)
     */
    fun getUserStore(): IUserManagement? = userStore
    
    /**
     * Ensure Anonymous user exists in the system
     */
    private suspend fun ensureAnonymousUser() {
        try {
            val existingUser = userStore?.getUser(Const.ANONYMOUS_USER)
            if (existingUser == null) {
                // Create Anonymous user with default permissions (can subscribe but not publish)
                val anonymousUser = User(
                    username = Const.ANONYMOUS_USER,
                    passwordHash = "", // No password for anonymous user
                    enabled = true,
                    canSubscribe = false,
                    canPublish = false, 
                    isAdmin = false
                )
                
                val created = userStore?.createUser(anonymousUser) ?: false
                if (created) {
                    logger.info("Created Anonymous user for unauthenticated access")
                } else {
                    logger.warning("Failed to create Anonymous user")
                }
            } else {
                logger.fine("Anonymous user already exists")
            }
        } catch (e: Exception) {
            logger.warning("Error ensuring Anonymous user exists: ${e.message}")
        }
    }
    
    /**
     * Ensure default admin user exists in the system
     */
    private suspend fun ensureDefaultAdminUser() {
        try {
            val adminUsername = "Admin"
            val existingAdmin = userStore?.getUser(adminUsername)
            if (existingAdmin == null) {
                // Create default admin user
                val adminUser = User(
                    username = adminUsername,
                    passwordHash = PasswordEncoder.hash("Admin"),
                    enabled = true,
                    canSubscribe = true,
                    canPublish = true,
                    isAdmin = true
                )

                val created = userStore?.createUser(adminUser) ?: false
                if (created) {
                    logger.info("Created default admin user 'Admin' with password 'Admin' - PLEASE CHANGE PASSWORD!")
                } else {
                    logger.warning("Failed to create default admin user")
                }
            } else {
                logger.fine("Default admin user already exists")
            }
        } catch (e: Exception) {
            logger.warning("Error ensuring default admin user exists: ${e.message}")
        }
    }
    
    /**
     * Start periodic cache refresh
     */
    private fun startCacheRefresh() {
        if (cacheRefreshInterval > 0) {
            scheduler = Executors.newSingleThreadScheduledExecutor { r ->
                Thread(r, "UserManager-CacheRefresh")
            }
            
            scheduler!!.scheduleWithFixedDelay({
                try {
                    vertx.executeBlocking(java.util.concurrent.Callable<Void> {
                        userStore?.let { store ->
                            kotlinx.coroutines.runBlocking {
                                aclCache.loadFromStore(store)
                                aclCache.clearPermissionCache()
                            }
                        }
                        null
                    })
                } catch (e: Exception) {
                    logger.warning("Error scheduling cache refresh: ${e.message}")
                }
            }, cacheRefreshInterval, cacheRefreshInterval, TimeUnit.SECONDS)
            
            logger.info("Started periodic cache refresh every ${cacheRefreshInterval}s")
        }
    }
}