package at.rocworks.auth

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.StoreType
import at.rocworks.stores.IUserStore
import at.rocworks.stores.UserFactory
import at.rocworks.Monster
import auth.AclCache
import auth.PasswordEncoder
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class UserManager(
    private val config: JsonObject
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)
    
    private var userStore: IUserStore? = null
    private val aclCache = AclCache()
    private var scheduler: ScheduledExecutorService? = null
    
    // Configuration properties
    private val isEnabled: Boolean
    private val storeType: StoreType
    private val passwordAlgorithm: String
    private val cacheRefreshInterval: Long
    private val disconnectOnUnauthorized: Boolean
    
    init {
        logger.level = Const.DEBUG_LEVEL
        
        val userMgmtConfig = config.getJsonObject("UserManagement") ?: JsonObject()
        isEnabled = userMgmtConfig.getBoolean("Enabled", false)
        storeType = StoreType.valueOf(Monster.getStoreType(config))
        passwordAlgorithm = userMgmtConfig.getString("PasswordAlgorithm", "bcrypt")
        cacheRefreshInterval = userMgmtConfig.getLong("CacheRefreshInterval", 60)
        disconnectOnUnauthorized = userMgmtConfig.getBoolean("DisconnectOnUnauthorized", true)
        
        logger.info("User management enabled: $isEnabled")
        if (isEnabled) {
            logger.info("Auth store type: $storeType")
            logger.info("Password algorithm: $passwordAlgorithm")
            logger.info("Cache refresh interval: ${cacheRefreshInterval}s")
            logger.info("Disconnect on unauthorized: $disconnectOnUnauthorized")
        }
    }
    
    override fun start(startPromise: Promise<Void>) {
        try {
            // Always create user store, even when user management is disabled
            // This allows admins to configure users before enabling user management
            userStore = UserFactory.create(storeType, config, vertx)

            // Initialize store and load cache
            userStore!!.init().compose { initResult ->
                if (initResult) {
                    if (isEnabled) {
                        ensureAnonymousUser()
                    } else {
                        Future.succeededFuture<Void>()
                    }
                } else {
                    Future.failedFuture<Void>("Store initialization failed")
                }
            }.compose { _ ->
                if (isEnabled) {
                    ensureDefaultAdminUser()
                } else {
                    Future.succeededFuture<Void>()
                }
            }.compose { _ ->
                if (isEnabled) {
                    aclCache.loadFromStore(userStore!!)
                } else {
                    Future.succeededFuture<Void>()
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    // Start periodic cache refresh only if enabled
                    if (isEnabled) {
                        startCacheRefresh()
                    }
                    logger.info("User store initialized successfully")
                    startPromise.complete()
                } else {
                    logger.severe("Failed to initialize user store: ${result.cause()?.message}")
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
            userStore?.close()?.onComplete {
                stopPromise.complete()
            } ?: stopPromise.complete()
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
    fun authenticate(username: String, password: String): Future<User?> {
        return try {
            userStore?.validateCredentials(username, password) ?: Future.succeededFuture(null)
        } catch (e: Exception) {
            logger.warning("Authentication error for user [$username]: ${e.message}")
            Future.succeededFuture(null)
        }
    }
    
    /**
     * Check if user can subscribe to a topic
     */
    fun canSubscribe(username: String, topicFilter: String): Boolean {
        return aclCache.checkSubscribePermission(username, topicFilter)
    }
    
    /**
     * Check if user can publish to a topic
     */
    fun canPublish(username: String, topic: String): Boolean {
        return aclCache.checkPublishPermission(username, topic)
    }
    
    /**
     * Check if user is admin
     */
    fun isAdmin(username: String): Boolean {
        return aclCache.isUserAdmin(username)
    }
    
    /**
     * Get user by username (for admin purposes)
     */
    fun getUser(username: String): User? {
        return aclCache.getUser(username)
    }
    
    /**
     * Create a new user (admin operation)
     */
    fun createUser(username: String, password: String, enabled: Boolean = true,
                          canSubscribe: Boolean = true, canPublish: Boolean = true,
                          isAdmin: Boolean = false): Future<Boolean> {
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
            val userStoreFuture = userStore?.createUser(user) ?: Future.succeededFuture(false)
            userStoreFuture.compose { result ->
                if (result) {
                    // Refresh cache to include new user
                    refreshCache().map { result }
                } else {
                    Future.succeededFuture(result)
                }
            }
        } catch (e: Exception) {
            logger.warning("Error creating user [$username]: ${e.message}")
            Future.succeededFuture(false)
        }
    }
    
    /**
     * Create ACL rule for user (admin operation)
     */
    fun createAclRule(username: String, topicPattern: String, canSubscribe: Boolean = false,
                             canPublish: Boolean = false, priority: Int = 0): Future<Boolean> {
        val rule = AclRule(
            id = "", // Will be generated by store
            username = username,
            topicPattern = topicPattern,
            canSubscribe = canSubscribe,
            canPublish = canPublish,
            priority = priority
        )

        return try {
            val userStoreFuture = userStore?.createAclRule(rule) ?: Future.succeededFuture(false)
            userStoreFuture.compose { result ->
                if (result) {
                    // Refresh cache to include new rule
                    refreshCache().map { result }
                } else {
                    Future.succeededFuture(result)
                }
            }
        } catch (e: Exception) {
            logger.warning("Error creating ACL rule for user [$username]: ${e.message}")
            Future.succeededFuture(false)
        }
    }
    
    /**
     * Get configuration property: disconnect on unauthorized
     */
    fun shouldDisconnectOnUnauthorized(): Boolean = disconnectOnUnauthorized
    
    /**
     * Manually refresh the ACL cache
     */
    fun refreshCache(): Future<Void> {
        return try {
            userStore?.let { store ->
                aclCache.loadFromStore(store).compose {
                    aclCache.clearPermissionCache()
                    logger.info("ACL cache refreshed manually")
                    Future.succeededFuture<Void>()
                }
            } ?: Future.succeededFuture()
        } catch (e: Exception) {
            logger.severe("Error refreshing ACL cache: ${e.message}")
            Future.succeededFuture()
        }
    }
    
    /**
     * Get cache statistics
     */
    fun getCacheStats(): Map<String, Any> {
        return aclCache.getCacheStats()
    }

    /**
     * Get all users
     */
    fun getAllUsers(): Future<List<User>> {
        return userStore?.getAllUsers() ?: Future.succeededFuture(emptyList())
    }

    /**
     * Update user
     */
    fun updateUser(user: User): Future<Boolean> {
        val userStoreFuture = userStore?.updateUser(user) ?: Future.succeededFuture(false)
        return userStoreFuture.compose { result ->
            if (result) {
                refreshCache().map { result }
            } else {
                Future.succeededFuture(result)
            }
        }
    }

    /**
     * Delete user
     */
    fun deleteUser(username: String): Future<Boolean> {
        val userStoreFuture = userStore?.deleteUser(username) ?: Future.succeededFuture(false)
        return userStoreFuture.compose { result ->
            if (result) {
                refreshCache().map { result }
            } else {
                Future.succeededFuture(result)
            }
        }
    }

    /**
     * Set user password
     */
    fun setUserPassword(username: String, newPassword: String): Future<Boolean> {
        val userFuture = userStore?.getUser(username) ?: Future.succeededFuture(null)
        return userFuture.compose { user ->
            if (user != null) {
                val hashedPassword = PasswordEncoder.hash(newPassword)
                val updatedUser = user.copy(passwordHash = hashedPassword)
                updateUser(updatedUser)
            } else {
                Future.succeededFuture(false)
            }
        }
    }

    /**
     * Get ACL rule by ID
     */
    fun getAclRule(id: String): Future<AclRule?> {
        return userStore?.getAclRule(id) ?: Future.succeededFuture(null)
    }

    /**
     * Get all ACL rules
     */
    fun getAllAclRules(): Future<List<AclRule>> {
        return userStore?.getAllAclRules() ?: Future.succeededFuture(emptyList())
    }

    /**
     * Get ACL rules for a specific user
     */
    fun getUserAclRules(username: String): Future<List<AclRule>> {
        return userStore?.getUserAclRules(username) ?: Future.succeededFuture(emptyList())
    }

    /**
     * Update ACL rule
     */
    fun updateAclRule(rule: AclRule): Future<Boolean> {
        val userStoreFuture = userStore?.updateAclRule(rule) ?: Future.succeededFuture(false)
        return userStoreFuture.compose { result ->
            if (result) {
                refreshCache().map { result }
            } else {
                Future.succeededFuture(result)
            }
        }
    }

    /**
     * Delete ACL rule
     */
    fun deleteAclRule(id: String): Future<Boolean> {
        val userStoreFuture = userStore?.deleteAclRule(id) ?: Future.succeededFuture(false)
        return userStoreFuture.compose { result ->
            if (result) {
                refreshCache().map { result }
            } else {
                Future.succeededFuture(result)
            }
        }
    }

    /**
     * Get user store for direct access (used by GraphQL resolver)
     */
    fun getUserStore(): IUserStore? = userStore
    
    /**
     * Ensure Anonymous user exists in the system
     */
    private fun ensureAnonymousUser(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val userFuture = userStore?.getUser(Const.ANONYMOUS_USER) ?: Future.succeededFuture(null)
            userFuture.onComplete { userResult ->
                if (userResult.succeeded()) {
                    val existingUser = userResult.result()
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

                        val createFuture = userStore?.createUser(anonymousUser) ?: Future.succeededFuture(false)
                        createFuture.onComplete { createResult ->
                            if (createResult.succeeded() && createResult.result()) {
                                logger.info("Created Anonymous user for unauthenticated access")
                            } else {
                                logger.warning("Failed to create Anonymous user")
                            }
                            promise.complete()
                        }
                    } else {
                        logger.fine { "Anonymous user already exists" }
                        promise.complete()
                    }
                } else {
                    logger.warning("Error checking for Anonymous user: ${userResult.cause()?.message}")
                    promise.complete()
                }
            }
        } catch (e: Exception) {
            logger.warning("Error ensuring Anonymous user exists: ${e.message}")
            promise.complete()
        }

        return promise.future()
    }
    
    /**
     * Ensure default admin user exists in the system
     */
    private fun ensureDefaultAdminUser(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val adminUsername = "Admin"
            val userFuture = userStore?.getUser(adminUsername) ?: Future.succeededFuture(null)
            userFuture.onComplete { userResult ->
                if (userResult.succeeded()) {
                    val existingAdmin = userResult.result()
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

                        val createFuture = userStore?.createUser(adminUser) ?: Future.succeededFuture(false)
                        createFuture.onComplete { createResult ->
                            if (createResult.succeeded() && createResult.result()) {
                                logger.info("Created default admin user 'Admin' with password 'Admin' - PLEASE CHANGE PASSWORD!")
                            } else {
                                logger.warning("Failed to create default admin user")
                            }
                            promise.complete()
                        }
                    } else {
                        logger.fine { "Default admin user already exists" }
                        promise.complete()
                    }
                } else {
                    logger.warning("Error checking for default admin user: ${userResult.cause()?.message}")
                    promise.complete()
                }
            }
        } catch (e: Exception) {
            logger.warning("Error ensuring default admin user exists: ${e.message}")
            promise.complete()
        }

        return promise.future()
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
                    userStore?.let { store ->
                        aclCache.loadFromStore(store).onComplete { result ->
                            if (result.succeeded()) {
                                aclCache.clearPermissionCache()
                                logger.fine { "Periodic cache refresh completed" }
                            } else {
                                logger.warning("Error in periodic cache refresh: ${result.cause()?.message}")
                            }
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error scheduling cache refresh: ${e.message}")
                }
            }, cacheRefreshInterval, cacheRefreshInterval, TimeUnit.SECONDS)
            
            logger.info("Started periodic cache refresh every ${cacheRefreshInterval}s")
        }
    }
}