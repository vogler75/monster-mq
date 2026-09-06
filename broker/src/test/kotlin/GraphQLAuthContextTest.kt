package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.data.User
import at.rocworks.extensions.graphql.AuthContextService
import at.rocworks.extensions.graphql.GraphQLAuthContext
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class GraphQLAuthContextTest {

    private lateinit var vertx: Vertx
    private lateinit var tempDir: File

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        tempDir = Files.createTempDirectory("test_graphql_auth_").toFile()
        val future = CompletableFuture<String>()
        vertx.deployVerticle(SQLiteVerticle()).onComplete { ar ->
            if (ar.succeeded()) future.complete(ar.result())
            else future.completeExceptionally(ar.cause())
        }
        future.get(10, TimeUnit.SECONDS)
    }

    @After
    fun tearDown() {
        AuthContextService.clearAuthContext()
        val future = CompletableFuture<Void>()
        vertx.close().onComplete { future.complete(null) }
        future.get(10, TimeUnit.SECONDS)
        tempDir.deleteRecursively()
    }

    private fun createUserManager(enabled: Boolean = true): UserManager {
        val config = JsonObject().apply {
            put("UserManagement", JsonObject().apply {
                put("Enabled", enabled)
                put("StoreType", "SQLITE")
                put("PasswordAlgorithm", "bcrypt")
                put("CacheRefreshInterval", 0)
                put("DisconnectOnUnauthorized", true)
            })
            put("SQLite", JsonObject().apply {
                put("Path", tempDir.absolutePath)
            })
        }
        val userManager = UserManager(config)
        val future = CompletableFuture<String>()
        vertx.deployVerticle(userManager).onComplete { ar ->
            if (ar.succeeded()) future.complete(ar.result())
            else future.completeExceptionally(ar.cause())
        }
        future.get(15, TimeUnit.SECONDS)
        return userManager
    }

    @Test
    fun testAdminTokenAllowsAdminOperationsWhenRoleActive() {
        val userManager = createUserManager(enabled = true)
        val authContext = GraphQLAuthContext(userManager)

        // Create admin user
        val createFuture = CompletableFuture<Boolean>()
        userManager.createUser("admin1", "pass123", enabled = true, canSubscribe = true, canPublish = true, isAdmin = true)
            .onComplete { createFuture.complete(it.result() ?: false) }
        assertTrue(createFuture.get(10, TimeUnit.SECONDS))

        // Generate token with isAdmin = true
        val token = JwtService.generateToken("admin1", isAdmin = true)

        val ctx = authContext.extractAuthContextFromToken(token)
        assertNotNull("AuthContext should not be null", ctx)
        assertEquals("admin1", ctx!!.username)
        assertTrue("isAdmin should be true", ctx.isAdmin)

        AuthContextService.setAuthContext(ctx)
        val result = authContext.validateFieldAccess("user")
        assertTrue("Admin mutation 'user' should be allowed for active admin", result.allowed)
        assertTrue(authContext.canSubscribeToTopic(ctx, "any/restricted/topic"))
        assertTrue(authContext.canPublishToTopic(ctx, "any/restricted/topic"))
    }

    @Test
    fun testDemotedAdminLosesAdminPrivilegesImmediately() {
        val userManager = createUserManager(enabled = true)
        val authContext = GraphQLAuthContext(userManager)

        // Create admin user
        val createFuture = CompletableFuture<Boolean>()
        userManager.createUser("admin_to_demote", "pass123", enabled = true, canSubscribe = true, canPublish = true, isAdmin = true)
            .onComplete { createFuture.complete(it.result() ?: false) }
        assertTrue(createFuture.get(10, TimeUnit.SECONDS))

        // Issue token while user is admin
        val token = JwtService.generateToken("admin_to_demote", isAdmin = true)
        assertTrue("JWT claim should be admin", JwtService.extractIsAdmin(token))

        // Demote user in UserManager
        val user = userManager.getUser("admin_to_demote")!!
        val demoteFuture = CompletableFuture<Boolean>()
        userManager.updateUser(user.copy(isAdmin = false)).onComplete { demoteFuture.complete(it.result() ?: false) }
        assertTrue(demoteFuture.get(10, TimeUnit.SECONDS))

        // Even though JWT token says isAdmin = true, extractAuthContext must re-read role and return isAdmin = false
        val ctx = authContext.extractAuthContextFromToken(token)
        assertNotNull("AuthContext should exist for active non-admin user", ctx)
        assertEquals("admin_to_demote", ctx!!.username)
        assertFalse("isAdmin must be revoked/false after demotion", ctx.isAdmin)

        AuthContextService.setAuthContext(ctx)
        val adminOpResult = authContext.validateFieldAccess("user")
        assertFalse("Admin operation should be denied for demoted user", adminOpResult.allowed)
        assertEquals("Admin privileges required", adminOpResult.errorMessage)

        // Also test if a stale AuthContext with isAdmin = true was somehow present, validateFieldAccess still checks userManager
        val staleCtx = at.rocworks.extensions.graphql.AuthContext("admin_to_demote", isAdmin = true, token = token)
        AuthContextService.setAuthContext(staleCtx)
        val staleResult = authContext.validateFieldAccess("user")
        assertFalse("Even with stale AuthContext.isAdmin=true, field access must re-check userManager", staleResult.allowed)
        assertEquals("Admin privileges required", staleResult.errorMessage)
    }

    @Test
    fun testDisabledUserAuthenticationIsRevoked() {
        val userManager = createUserManager(enabled = true)
        val authContext = GraphQLAuthContext(userManager)

        // Create user
        val createFuture = CompletableFuture<Boolean>()
        userManager.createUser("user_to_disable", "pass123", enabled = true, canSubscribe = true, canPublish = true, isAdmin = true)
            .onComplete { createFuture.complete(it.result() ?: false) }
        assertTrue(createFuture.get(10, TimeUnit.SECONDS))

        val token = JwtService.generateToken("user_to_disable", isAdmin = true)

        // Disable user
        val user = userManager.getUser("user_to_disable")!!
        val disableFuture = CompletableFuture<Boolean>()
        userManager.updateUser(user.copy(enabled = false)).onComplete { disableFuture.complete(it.result() ?: false) }
        assertTrue(disableFuture.get(10, TimeUnit.SECONDS))

        // Token should now be rejected as unauthenticated
        val ctx = authContext.extractAuthContextFromToken(token)
        assertNull("Disabled user token must produce null AuthContext", ctx)

        // If a stale AuthContext for a disabled user was in thread local, validateFieldAccess must deny it
        val staleCtx = at.rocworks.extensions.graphql.AuthContext("user_to_disable", isAdmin = true, token = token)
        AuthContextService.setAuthContext(staleCtx)
        val result = authContext.validateFieldAccess("session")
        assertFalse("Disabled user access must be denied", result.allowed)
        assertEquals("Authentication required", result.errorMessage)
    }

    @Test
    fun testDeletedUserAuthenticationIsRevoked() {
        val userManager = createUserManager(enabled = true)
        val authContext = GraphQLAuthContext(userManager)

        // Create user
        val createFuture = CompletableFuture<Boolean>()
        userManager.createUser("user_to_delete", "pass123", enabled = true, canSubscribe = true, canPublish = true, isAdmin = true)
            .onComplete { createFuture.complete(it.result() ?: false) }
        assertTrue(createFuture.get(10, TimeUnit.SECONDS))

        val token = JwtService.generateToken("user_to_delete", isAdmin = true)

        // Delete user
        val deleteFuture = CompletableFuture<Boolean>()
        userManager.deleteUser("user_to_delete").onComplete { deleteFuture.complete(it.result() ?: false) }
        assertTrue(deleteFuture.get(10, TimeUnit.SECONDS))

        // Token should now be rejected
        val ctx = authContext.extractAuthContextFromToken(token)
        assertNull("Deleted user token must produce null AuthContext", ctx)

        val staleCtx = at.rocworks.extensions.graphql.AuthContext("user_to_delete", isAdmin = true, token = token)
        AuthContextService.setAuthContext(staleCtx)
        val result = authContext.validateFieldAccess("user")
        assertFalse("Deleted user access must be denied", result.allowed)
        assertEquals("Authentication required", result.errorMessage)
    }

    @Test
    fun testPromotedUserGainsAdminPrivilegesImmediately() {
        val userManager = createUserManager(enabled = true)
        val authContext = GraphQLAuthContext(userManager)

        // Create standard non-admin user
        val createFuture = CompletableFuture<Boolean>()
        userManager.createUser("user_to_promote", "pass123", enabled = true, canSubscribe = true, canPublish = true, isAdmin = false)
            .onComplete { createFuture.complete(it.result() ?: false) }
        assertTrue(createFuture.get(10, TimeUnit.SECONDS))

        // Token issued with isAdmin = false
        val token = JwtService.generateToken("user_to_promote", isAdmin = false)
        assertFalse(JwtService.extractIsAdmin(token))

        // Promote user
        val user = userManager.getUser("user_to_promote")!!
        val promoteFuture = CompletableFuture<Boolean>()
        userManager.updateUser(user.copy(isAdmin = true)).onComplete { promoteFuture.complete(it.result() ?: false) }
        assertTrue(promoteFuture.get(10, TimeUnit.SECONDS))

        // extractAuthContextFromToken should reflect promoted status
        val ctx = authContext.extractAuthContextFromToken(token)
        assertNotNull(ctx)
        assertTrue("Promoted user should immediately have isAdmin = true", ctx!!.isAdmin)

        AuthContextService.setAuthContext(ctx)
        val result = authContext.validateFieldAccess("user")
        assertTrue("Promoted user should have access to admin fields", result.allowed)
    }

    @Test
    fun testUserManagementDisabledAllowsAllOperations() {
        val userManager = createUserManager(enabled = false)
        val authContext = GraphQLAuthContext(userManager)

        val token = JwtService.generateToken("any_user", isAdmin = false)
        val ctx = authContext.extractAuthContextFromToken(token)
        assertNotNull(ctx)

        AuthContextService.setAuthContext(ctx)
        val result = authContext.validateFieldAccess("user")
        assertTrue("When user management is disabled, all operations should be allowed", result.allowed)
    }
}
