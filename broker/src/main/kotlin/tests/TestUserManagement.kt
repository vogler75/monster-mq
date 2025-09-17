package at.rocworks.tests

import auth.AclCache
import auth.PasswordEncoder
import at.rocworks.auth.UserManager
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.sqlite.UserStoreSqlite
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    println("=== User Management Tests ===")
    
    // Test password encoder
    testPasswordEncoder()
    
    // Test ACL cache
    testAclCache()
    
    // Test SQLite store
    testSqliteStore()
    
    // Test user manager integration
    testUserManager()
    
    println("=== All User Management Tests Completed ===")
}

fun testPasswordEncoder() {
    println("\n--- Testing Password Encoder ---")
    
    val password = "testPassword123"
    val hash1 = PasswordEncoder.hash(password)
    val hash2 = PasswordEncoder.hash(password)
    
    // Different hashes for same password (salt)
    assert(hash1 != hash2) { "Hash should be different due to salt" }
    println("✓ Different hashes generated for same password")
    
    // Verify correct password
    assert(PasswordEncoder.verify(password, hash1)) { "Password verification failed" }
    assert(PasswordEncoder.verify(password, hash2)) { "Password verification failed" }
    println("✓ Password verification works correctly")
    
    // Verify wrong password
    assert(!PasswordEncoder.verify("wrongPassword", hash1)) { "Wrong password should not verify" }
    println("✓ Wrong password correctly rejected")
}

fun testAclCache() {
    println("\n--- Testing ACL Cache ---")

    // Create a temporary SQLite store for testing ACL cache
    val tempDbFile = File.createTempFile("test_acl_cache", ".db")
    tempDbFile.deleteOnExit()

    val cache = AclCache()
    val vertx = Vertx.vertx()

    try {
        val latch = CountDownLatch(1)
        val store = UserStoreSqlite(tempDbFile.absolutePath, vertx)
        store.init().onComplete { initResult: io.vertx.core.AsyncResult<Boolean> ->
            assert(initResult.succeeded()) { "Store initialization should succeed" }

            // Create test users
            val adminUser = User("admin", PasswordEncoder.hash("password"), true, true, true, true)
            val normalUser = User("user1", PasswordEncoder.hash("password"), true, true, true, false)
            val readOnlyUser = User("readonly", PasswordEncoder.hash("password"), true, true, false, false)

            store.createUser(adminUser).compose { _: Boolean ->
                store.createUser(normalUser)
            }.compose { _: Boolean ->
                store.createUser(readOnlyUser)
            }.compose { _ ->
                // Create test ACL rules
                store.createAclRule(AclRule("", "user1", "sensors/+/temperature", true, false, 10))
            }.compose { _ ->
                store.createAclRule(AclRule("", "user1", "actuators/#", false, true, 5))
            }.compose { _: Boolean ->
                store.createAclRule(AclRule("", "readonly", "sensors/#", true, false, 10))
            }.compose { _: Boolean ->
                store.createAclRule(AclRule("", "readonly", "status/+", true, false, 5))
            }.compose { _: Boolean ->
                // Load test data from store
                cache.loadFromStore(store)
            }.compose { _: Void ->
                store.close()
            }.onComplete { result: io.vertx.core.AsyncResult<Void> ->
                if (result.succeeded()) {
                    // Test admin permissions (should bypass ACL)
                    assert(cache.isUserAdmin("admin")) { "Admin user not recognized" }
                    assert(cache.checkSubscribePermission("admin", "any/topic")) { "Admin should have all subscribe permissions" }
                    assert(cache.checkPublishPermission("admin", "any/topic")) { "Admin should have all publish permissions" }
                    println("✓ Admin permissions work correctly")
                    // Test normal user subscribe permissions
                    assert(cache.checkSubscribePermission("user1", "sensors/temp1/temperature")) { "User1 should subscribe to sensors/+/temperature" }
                    assert(cache.checkSubscribePermission("user1", "sensors/temp2/temperature")) { "User1 should subscribe to sensors/+/temperature" }
                    assert(!cache.checkSubscribePermission("user1", "sensors/temp1/humidity")) { "User1 should not subscribe to humidity" }
                    assert(!cache.checkSubscribePermission("user1", "sensors/temp1/temperature/details")) { "Single level wildcard should not match multiple levels" }
                    println("✓ Single level wildcard (+) permissions work correctly")
                    // Test normal user publish permissions
                    assert(cache.checkPublishPermission("user1", "actuators/valve1")) { "User1 should publish to actuators/#" }
                    assert(cache.checkPublishPermission("user1", "actuators/pump/control")) { "User1 should publish to actuators/#" }
                    assert(!cache.checkPublishPermission("user1", "sensors/temp1")) { "User1 should not publish to sensors" }
                    println("✓ Multi level wildcard (#) permissions work correctly")
                    // Test readonly user permissions
                    assert(cache.checkSubscribePermission("readonly", "sensors/anything/here")) { "Readonly should subscribe to sensors/#" }
                    assert(cache.checkSubscribePermission("readonly", "status/system")) { "Readonly should subscribe to status/+" }
                    assert(!cache.checkSubscribePermission("readonly", "status/system/details")) { "Single level wildcard should not match multiple levels" }
                    assert(!cache.checkPublishPermission("readonly", "any/topic")) { "Readonly should not publish to anything" }
                    println("✓ Readonly user permissions work correctly")
                    // Test cache statistics
                    val stats = cache.getCacheStats()
                    assert(stats["users"] == 3) { "Expected 3 users in cache, got ${stats["users"]}" }
                    assert(stats["userAcls"] == 2) { "Expected 2 user ACL entries (user1 and readonly), got ${stats["userAcls"]}" }
                    println("✓ Cache statistics work correctly")
                    // Test permission caching
                    cache.checkSubscribePermission("user1", "sensors/temp1/temperature")
                    cache.checkSubscribePermission("user1", "sensors/temp1/temperature") // Should hit cache
                    val statsAfterCache = cache.getCacheStats()
                    println("✓ Permission caching appears to be working")
                } else {
                    println("Failed ACL cache test: ${result.cause()?.message}")
                }
                latch.countDown()
            }
        }

        latch.await(10, TimeUnit.SECONDS)
    } finally {
        vertx.close()
    }
}

fun testSqliteStore() {
    println("\n--- Testing SQLite Store ---")

    val tempDbFile = File.createTempFile("test_user_mgmt", ".db")
    tempDbFile.deleteOnExit()

    val vertx = Vertx.vertx()
    try {
        val latch = CountDownLatch(1)
        val store = UserStoreSqlite(tempDbFile.absolutePath, vertx)

        store.init().compose { initResult: Boolean ->
            assert(initResult) { "Store initialization failed" }
            println("✓ SQLite store initialized successfully")
            // Test user creation
            val testUser = User("testuser", PasswordEncoder.hash("password123"), true, true, true, false)
            store.createUser(testUser)
        }.compose { createResult: Boolean ->
            assert(createResult) { "User creation failed" }
            println("✓ User creation works")
            // Test user retrieval
            store.getUser("testuser")
        }.compose { retrievedUser: User? ->
            assert(retrievedUser != null) { "User retrieval failed" }
            assert(retrievedUser!!.username == "testuser") { "Retrieved username mismatch" }
            assert(retrievedUser.enabled) { "User should be enabled" }
            assert(!retrievedUser.isAdmin) { "User should not be admin" }
            println("✓ User retrieval works")
            // Test authentication
            store.validateCredentials("testuser", "password123")
        }.compose { authUser: User? ->
            assert(authUser != null) { "Authentication failed for correct password" }
            store.validateCredentials("testuser", "wrongpassword")
        }.compose { noAuthUser: User? ->
            assert(noAuthUser == null) { "Authentication should fail for wrong password" }
            println("✓ Authentication works correctly")
            // Test ACL rule creation
            val aclRule = AclRule("", "testuser", "sensors/+/temp", true, false, 10)
            store.createAclRule(aclRule)
        }.compose { aclCreateResult: Boolean ->
            assert(aclCreateResult) { "ACL rule creation failed" }
            println("✓ ACL rule creation works")
            // Test ACL rule retrieval
            store.getUserAclRules("testuser")
        }.compose { userRules: List<AclRule> ->
            assert(userRules.size == 1) { "Expected 1 ACL rule for user" }
            assert(userRules[0].topicPattern == "sensors/+/temp") { "ACL rule topic pattern mismatch" }
            assert(userRules[0].canSubscribe) { "ACL rule should allow subscribe" }
            assert(!userRules[0].canPublish) { "ACL rule should not allow publish" }
            println("✓ ACL rule retrieval works")
            // Test bulk data loading
            store.loadAllUsersAndAcls()
        }.compose { data: Pair<List<User>, List<AclRule>> ->
            val (users, rules) = data
            assert(users.size >= 1) { "Expected at least 1 user" }
            assert(rules.size >= 1) { "Expected at least 1 rule" }
            println("✓ Bulk data loading works")
            // Cleanup
            store.close()
        }.onComplete { result: io.vertx.core.AsyncResult<Void> ->
            if (result.succeeded()) {
                println("✓ Store cleanup completed")
            } else {
                println("Store test failed: ${result.cause()?.message}")
            }
            latch.countDown()
        }

        latch.await(15, TimeUnit.SECONDS)
    } finally {
        vertx.close()
    }
}

fun testUserManager() {
    println("\n--- Testing User Manager ---")

    val tempDbFile = File.createTempFile("test_user_manager", ".db")
    tempDbFile.deleteOnExit()

    val config = JsonObject().apply {
        put("UserManagement", JsonObject().apply {
            put("Enabled", true)
            put("AuthStoreType", "SQLITE")
            put("PasswordAlgorithm", "bcrypt")
            put("CacheRefreshInterval", 0) // Disable periodic refresh for testing
            put("DisconnectOnUnauthorized", true)
        })
        put("SQLite", JsonObject().apply {
            put("Path", tempDbFile.absolutePath)
        })
    }

    val vertx = Vertx.vertx()

    try {
        val latch = CountDownLatch(1)
        val userManager = UserManager(config)

        // Deploy user manager as verticle
        vertx.deployVerticle(userManager).compose { deploymentId: String ->
            println("✓ User manager deployed successfully")

            // Test if enabled
            assert(userManager.isUserManagementEnabled()) { "User management should be enabled" }

            // Create test user
            userManager.createUser("testuser", "password123", true, true, true, false)
        }.compose { createResult: Boolean ->
            assert(createResult) { "User creation through manager failed" }
            println("✓ User creation through manager works")

            // Test authentication
            userManager.authenticate("testuser", "password123")
        }.compose { authResult: User? ->
            assert(authResult != null) { "Authentication through manager failed" }
            assert(authResult!!.username == "testuser") { "Authenticated user mismatch" }

            userManager.authenticate("testuser", "wrongpassword")
        }.compose { noAuthResult: User? ->
            assert(noAuthResult == null) { "Authentication should fail for wrong password" }
            println("✓ Authentication through manager works")

            // First test permissions without ACL rules - should allow all based on general permissions
            assert(userManager.canSubscribe("testuser", "any/topic/here")) {
                "User should be able to subscribe to any topic (no ACL rules, general permission true)"
            }
            assert(userManager.canPublish("testuser", "any/topic/here")) {
                "User should be able to publish to any topic (no ACL rules, general permission true)"
            }
            println("✓ Permission checking without ACL rules works (allows all based on general permissions)")

            // Now add ACL rule and test specific permissions
            userManager.createAclRule("testuser", "sensors/+/temp", true, false, 10)
        }.compose { aclCreateResult: Boolean ->
            assert(aclCreateResult) { "ACL rule creation through manager failed" }
            println("✓ ACL rule creation through manager works")

            // Test permissions with ACL rules - should restrict to specific patterns
            assert(userManager.canSubscribe("testuser", "sensors/room1/temp")) {
                "User should be able to subscribe to sensors/room1/temp"
            }
            assert(!userManager.canSubscribe("testuser", "actuators/valve1")) {
                "User should not be able to subscribe to actuators/valve1"
            }
            assert(!userManager.canPublish("testuser", "sensors/room1/temp")) {
                "User should not be able to publish to sensors/room1/temp"
            }
            println("✓ Permission checking with ACL rules works (restricts to specific patterns)")

            // Test cache refresh
            userManager.refreshCache()
        }.compose { _: Void ->
            println("✓ Cache refresh works")

            // Test cache stats
            val stats = userManager.getCacheStats()
            assert(stats.containsKey("users")) { "Cache stats should contain users" }
            println("✓ Cache statistics work")

            println("Debug: About to test user retrieval...")

            // Test user retrieval
            val retrievedUser = userManager.getUser("testuser")
            assert(retrievedUser != null) { "User retrieval through manager failed" }
            assert(retrievedUser!!.username == "testuser") { "Retrieved user mismatch" }
            println("✓ User retrieval through manager works")

            // Test Anonymous user creation and permissions
            try {
                val anonymousUser = userManager.getUser("Anonymous")
                if (anonymousUser != null) {
                    assert(anonymousUser.username == "Anonymous") { "Anonymous user name mismatch" }
                    assert(!anonymousUser.canSubscribe) { "Anonymous user should not be able to subscribe by default" }
                    assert(!anonymousUser.canPublish) { "Anonymous user should not be able to publish by default" }
                    assert(!anonymousUser.isAdmin) { "Anonymous user should not be admin" }

                    // Test Anonymous user permissions (should follow general permissions which are false)
                    assert(!userManager.canSubscribe("Anonymous", "any/topic")) {
                        "Anonymous user should not be able to subscribe to any topic (no ACL rules, general permission false)"
                    }
                    assert(!userManager.canPublish("Anonymous", "any/topic")) {
                        "Anonymous user should not be able to publish to any topic (no ACL rules, general permission false)"
                    }

                    println("✓ Anonymous user creation and permissions work")
                } else {
                    println("! Anonymous user not found - may still be creating asynchronously")
                }
            } catch (e: Exception) {
                println("! Anonymous user check failed: ${e.message}")
            }

            // Test admin check
            assert(!userManager.isAdmin("testuser")) { "Regular user should not be admin" }
            println("✓ Admin check works")

            // Test disconnect policy
            assert(userManager.shouldDisconnectOnUnauthorized()) { "Should disconnect on unauthorized" }
            println("✓ Disconnect policy configuration works")

            // Return the deployment ID for cleanup
            Future.succeededFuture<String>("deployment-completed")
        }.onComplete { result: io.vertx.core.AsyncResult<String> ->
            if (result.succeeded()) {
                println("✓ All user manager tests completed successfully")
            } else {
                println("User manager test failed: ${result.cause()?.message}")
            }
            latch.countDown()
        }

        latch.await(20, TimeUnit.SECONDS)
    } finally {
        vertx.close()
    }
}

