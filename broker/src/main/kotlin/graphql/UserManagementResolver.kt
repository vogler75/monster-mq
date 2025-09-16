package at.rocworks.extensions.graphql

import at.rocworks.auth.UserManager
import at.rocworks.data.User
import at.rocworks.data.AclRule
import graphql.schema.DataFetcher
import graphql.GraphQLException
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking

class UserManagementResolver(
    private val vertx: Vertx,
    private val userManager: UserManager,
    private val authContext: GraphQLAuthContext
) {
    companion object {
        private val logger: Logger = Logger.getLogger(UserManagementResolver::class.java.name)
    }

    // Helper function to convert User to UserInfo
    private fun User.toUserInfo(aclRules: List<AclRuleInfo> = emptyList()) = UserInfo(
        username = this.username,
        enabled = this.enabled,
        canSubscribe = this.canSubscribe,
        canPublish = this.canPublish,
        isAdmin = this.isAdmin,
        createdAt = this.createdAt?.toString(),
        updatedAt = this.updatedAt?.toString(),
        aclRules = aclRules
    )

    // Helper function to convert AclRule to AclRuleInfo
    private fun AclRule.toAclRuleInfo() = AclRuleInfo(
        id = this.id,
        username = this.username,
        topicPattern = this.topicPattern,
        canSubscribe = this.canSubscribe,
        canPublish = this.canPublish,
        priority = this.priority,
        createdAt = this.createdAt?.toString()
    )

    // Helper function to check authorization
    private fun checkAuthorization(env: graphql.schema.DataFetchingEnvironment, future: CompletableFuture<*>): Boolean {
        val authResult = authContext.validateFieldAccess(env)
        if (!authResult.allowed) {
            future.completeExceptionally(GraphQLException(authResult.errorMessage ?: "Unauthorized"))
            return false
        }
        return true
    }

    // Query Resolvers
    fun users(): DataFetcher<CompletableFuture<List<UserInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<UserInfo>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val username = env.getArgument<String?>("username")

            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }

                vertx.executeBlocking<List<UserInfo>> {
                    runBlocking {
                        try {
                            if (username != null) {
                                // Get specific user with ACL rules
                                val user = userManager.getUser(username)
                                if (user != null) {
                                    val aclRules = userManager.getUserAclRules(username).map { it.toAclRuleInfo() }
                                    listOf(user.toUserInfo(aclRules))
                                } else {
                                    emptyList()
                                }
                            } else {
                                // Get all users with their ACL rules
                                val users = userManager.getAllUsers()
                                users.map { user ->
                                    val aclRules = userManager.getUserAclRules(user.username).map { it.toAclRuleInfo() }
                                    user.toUserInfo(aclRules)
                                }
                            }
                        } catch (e: Exception) {
                            logger.severe("Error getting users: ${e.message}")
                            emptyList()
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error getting users: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error getting users: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    fun getAllUsers(): DataFetcher<CompletableFuture<List<UserInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<UserInfo>>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<List<UserInfo>> {
                    runBlocking {
                        try {
                            val users = userManager.getAllUsers()
                            users.map { user -> user.toUserInfo() }
                        } catch (e: Exception) {
                            logger.severe("Error getting all users: ${e.message}")
                            emptyList()
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error getting all users: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error getting all users: ${e.message}")
                future.completeExceptionally(e)
            }
            
            future
        }
    }

    fun getUser(): DataFetcher<CompletableFuture<UserInfo?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserInfo?>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            val username = env.getArgument<String>("username")
            
            try {
                if (!userManager.isUserManagementEnabled() || username == null) {
                    future.complete(null)
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserInfo?> {
                    try {
                        val user = userManager.getUser(username)
                        user?.toUserInfo()
                    } catch (e: Exception) {
                        logger.severe("Error getting user $username: ${e.message}")
                        null
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error getting user $username: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error getting user $username: ${e.message}")
                future.completeExceptionally(e)
            }
            
            future
        }
    }

    fun getAllAclRules(): DataFetcher<CompletableFuture<List<AclRuleInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<AclRuleInfo>>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<List<AclRuleInfo>> {
                    runBlocking {
                        try {
                            val rules = userManager.getAllAclRules()
                            rules.map { rule -> rule.toAclRuleInfo() }
                        } catch (e: Exception) {
                            logger.severe("Error getting all ACL rules: ${e.message}")
                            emptyList()
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error getting all ACL rules: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error getting all ACL rules: ${e.message}")
                future.completeExceptionally(e)
            }
            
            future
        }
    }

    fun getUserAclRules(): DataFetcher<CompletableFuture<List<AclRuleInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<AclRuleInfo>>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            val username = env.getArgument<String>("username")
            
            try {
                if (!userManager.isUserManagementEnabled() || username == null) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<List<AclRuleInfo>> {
                    runBlocking {
                        try {
                            val rules = userManager.getUserAclRules(username)
                            rules.map { rule -> rule.toAclRuleInfo() }
                        } catch (e: Exception) {
                            logger.severe("Error getting ACL rules for user $username: ${e.message}")
                            emptyList()
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error getting ACL rules for user $username: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error getting ACL rules for user $username: ${e.message}")
                future.completeExceptionally(e)
            }
            
            future
        }
    }

    // Mutation Resolvers
    fun createUser(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                val input = env.getArgument<Map<String, Any>>("input") 
                    ?: return@DataFetcher future.apply { 
                        complete(UserManagementResult(success = false, message = "Input is required")) 
                    }
                
                val createUserInput = CreateUserInput(
                    username = input["username"] as? String ?: "",
                    password = input["password"] as? String ?: "",
                    enabled = input["enabled"] as? Boolean ?: true,
                    canSubscribe = input["canSubscribe"] as? Boolean ?: true,
                    canPublish = input["canPublish"] as? Boolean ?: true,
                    isAdmin = input["isAdmin"] as? Boolean ?: false
                )
                
                if (createUserInput.username.isEmpty() || createUserInput.password.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "Username and password are required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            // Check if user already exists
                            val existingUser = userManager.getUser(createUserInput.username)
                            if (existingUser != null) {
                                UserManagementResult(success = false, message = "User already exists")
                            } else {
                                userManager.createUser(
                                    username = createUserInput.username,
                                    password = createUserInput.password,
                                    enabled = createUserInput.enabled,
                                    canSubscribe = createUserInput.canSubscribe,
                                    canPublish = createUserInput.canPublish,
                                    isAdmin = createUserInput.isAdmin
                                )
                                val newUser = userManager.getUser(createUserInput.username)
                                UserManagementResult(
                                    success = true,
                                    message = "User created successfully",
                                    user = newUser?.toUserInfo()
                                )
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error creating user: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error creating user: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error creating user: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating user: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error creating user: ${e.message}"))
            }
            
            future
        }
    }

    fun updateUser(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try{
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                val input = env.getArgument<Map<String, Any>>("input") 
                    ?: return@DataFetcher future.apply { 
                        complete(UserManagementResult(success = false, message = "Input is required")) 
                    }
                
                val updateUserInput = UpdateUserInput(
                    username = input["username"] as? String ?: "",
                    enabled = input["enabled"] as? Boolean,
                    canSubscribe = input["canSubscribe"] as? Boolean,
                    canPublish = input["canPublish"] as? Boolean,
                    isAdmin = input["isAdmin"] as? Boolean
                )
                
                if (updateUserInput.username.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "Username is required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            val existingUser = userManager.getUser(updateUserInput.username)
                            if (existingUser == null) {
                                UserManagementResult(success = false, message = "User not found")
                            } else {
                                val updatedUser = existingUser.copy(
                                    enabled = updateUserInput.enabled ?: existingUser.enabled,
                                    canSubscribe = updateUserInput.canSubscribe ?: existingUser.canSubscribe,
                                    canPublish = updateUserInput.canPublish ?: existingUser.canPublish,
                                    isAdmin = updateUserInput.isAdmin ?: existingUser.isAdmin
                                )
                                val success = userManager.updateUser(updatedUser)
                                if (success) {
                                    val refreshedUser = userManager.getUser(updateUserInput.username)
                                    UserManagementResult(
                                        success = true,
                                        message = "User updated successfully",
                                        user = refreshedUser?.toUserInfo()
                                    )
                                } else {
                                    UserManagementResult(success = false, message = "Failed to update user")
                                }
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error updating user: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error updating user: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error updating user: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating user: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error updating user: ${e.message}"))
            }
            
            future
        }
    }

    fun deleteUser(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            val username = env.getArgument<String>("username")
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                if (username == null || username.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "Username is required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            val existingUser = userManager.getUser(username)
                            if (existingUser == null) {
                                UserManagementResult(success = false, message = "User not found")
                            } else {
                                val success = userManager.deleteUser(username)
                                if (success) {
                                    UserManagementResult(
                                        success = true,
                                        message = "User deleted successfully"
                                    )
                                } else {
                                    UserManagementResult(success = false, message = "Failed to delete user")
                                }
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error deleting user: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error deleting user $username: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error deleting user: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting user $username: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error deleting user: ${e.message}"))
            }
            
            future
        }
    }

    fun setPassword(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                val input = env.getArgument<Map<String, Any>>("input") 
                    ?: return@DataFetcher future.apply { 
                        complete(UserManagementResult(success = false, message = "Input is required")) 
                    }
                
                val setPasswordInput = SetPasswordInput(
                    username = input["username"] as? String ?: "",
                    password = input["password"] as? String ?: ""
                )
                
                if (setPasswordInput.username.isEmpty() || setPasswordInput.password.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "Username and password are required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            val existingUser = userManager.getUser(setPasswordInput.username)
                            if (existingUser == null) {
                                UserManagementResult(success = false, message = "User not found")
                            } else {
                                val success = userManager.setUserPassword(setPasswordInput.username, setPasswordInput.password)
                                if (success) {
                                    val refreshedUser = userManager.getUser(setPasswordInput.username)
                                    UserManagementResult(
                                        success = true,
                                        message = "Password updated successfully",
                                        user = refreshedUser?.toUserInfo()
                                    )
                                } else {
                                    UserManagementResult(success = false, message = "Failed to update password")
                                }
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error setting password: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error setting password: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error setting password: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error setting password: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error setting password: ${e.message}"))
            }
            
            future
        }
    }

    fun createAclRule(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                val input = env.getArgument<Map<String, Any>>("input") 
                    ?: return@DataFetcher future.apply { 
                        complete(UserManagementResult(success = false, message = "Input is required")) 
                    }
                
                val createAclRuleInput = CreateAclRuleInput(
                    username = input["username"] as? String ?: "",
                    topicPattern = input["topicPattern"] as? String ?: "",
                    canSubscribe = input["canSubscribe"] as? Boolean ?: false,
                    canPublish = input["canPublish"] as? Boolean ?: false,
                    priority = input["priority"] as? Int ?: 0
                )
                
                if (createAclRuleInput.username.isEmpty() || createAclRuleInput.topicPattern.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "Username and topic pattern are required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            userManager.createAclRule(
                                username = createAclRuleInput.username,
                                topicPattern = createAclRuleInput.topicPattern,
                                canSubscribe = createAclRuleInput.canSubscribe,
                                canPublish = createAclRuleInput.canPublish,
                                priority = createAclRuleInput.priority
                            )
                            UserManagementResult(
                                success = true,
                                message = "ACL rule created successfully"
                            )
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error creating ACL rule: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error creating ACL rule: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error creating ACL rule: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating ACL rule: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error creating ACL rule: ${e.message}"))
            }
            
            future
        }
    }

    fun updateAclRule(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                val input = env.getArgument<Map<String, Any>>("input") 
                    ?: return@DataFetcher future.apply { 
                        complete(UserManagementResult(success = false, message = "Input is required")) 
                    }
                
                val updateAclRuleInput = UpdateAclRuleInput(
                    id = input["id"] as? String ?: "",
                    username = input["username"] as? String,
                    topicPattern = input["topicPattern"] as? String,
                    canSubscribe = input["canSubscribe"] as? Boolean,
                    canPublish = input["canPublish"] as? Boolean,
                    priority = input["priority"] as? Int
                )
                
                if (updateAclRuleInput.id.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "ACL rule ID is required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            val existingRule = userManager.getAclRule(updateAclRuleInput.id)
                            if (existingRule == null) {
                                UserManagementResult(success = false, message = "ACL rule not found")
                            } else {
                                val updatedRule = existingRule.copy(
                                    username = updateAclRuleInput.username ?: existingRule.username,
                                    topicPattern = updateAclRuleInput.topicPattern ?: existingRule.topicPattern,
                                    canSubscribe = updateAclRuleInput.canSubscribe ?: existingRule.canSubscribe,
                                    canPublish = updateAclRuleInput.canPublish ?: existingRule.canPublish,
                                    priority = updateAclRuleInput.priority ?: existingRule.priority
                                )
                                val success = userManager.updateAclRule(updatedRule)
                                if (success) {
                                    val refreshedRule = userManager.getAclRule(updateAclRuleInput.id)
                                    UserManagementResult(
                                        success = true,
                                        message = "ACL rule updated successfully",
                                        aclRule = refreshedRule?.toAclRuleInfo()
                                    )
                                } else {
                                    UserManagementResult(success = false, message = "Failed to update ACL rule")
                                }
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error updating ACL rule: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error updating ACL rule: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error updating ACL rule: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating ACL rule: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error updating ACL rule: ${e.message}"))
            }
            
            future
        }
    }

    fun deleteAclRule(): DataFetcher<CompletableFuture<UserManagementResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<UserManagementResult>()
            
            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future
            
            val id = env.getArgument<String>("id")
            
            try {
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "User management is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                if (id == null || id.isEmpty()) {
                    future.complete(UserManagementResult(
                        success = false,
                        message = "ACL rule ID is required"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<UserManagementResult> {
                    runBlocking {
                        try {
                            val existingRule = userManager.getAclRule(id)
                            if (existingRule == null) {
                                UserManagementResult(success = false, message = "ACL rule not found")
                            } else {
                                val success = userManager.deleteAclRule(id)
                                if (success) {
                                    UserManagementResult(
                                        success = true,
                                        message = "ACL rule deleted successfully"
                                    )
                                } else {
                                    UserManagementResult(success = false, message = "Failed to delete ACL rule")
                                }
                            }
                        } catch (e: Exception) {
                            UserManagementResult(success = false, message = "Error deleting ACL rule: ${e.message}")
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error deleting ACL rule $id: ${result.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error deleting ACL rule: ${result.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting ACL rule $id: ${e.message}")
                future.complete(UserManagementResult(success = false, message = "Error deleting ACL rule: ${e.message}"))
            }
            
            future
        }
    }
}