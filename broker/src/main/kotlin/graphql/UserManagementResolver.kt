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

                if (username != null) {
                    // Get specific user with ACL rules
                    val user = userManager.getUser(username)
                    if (user != null) {
                        userManager.getUserAclRules(username).onComplete { aclResult ->
                            if (aclResult.succeeded()) {
                                val aclRules = aclResult.result().map { it.toAclRuleInfo() }
                                future.complete(listOf(user.toUserInfo(aclRules)))
                            } else {
                                logger.severe("Error getting ACL rules for user $username: ${aclResult.cause()?.message}")
                                future.complete(listOf(user.toUserInfo()))
                            }
                        }
                    } else {
                        future.complete(emptyList())
                    }
                } else {
                    // Get all users with their ACL rules
                    userManager.getAllUsers().onComplete { usersResult ->
                        if (usersResult.succeeded()) {
                            val users = usersResult.result()
                            if (users.isEmpty()) {
                                future.complete(emptyList())
                                return@onComplete
                            }

                            // Collect all user info with ACL rules
                            val userInfoList = mutableListOf<UserInfo>()
                            var completed = 0

                            users.forEach { user ->
                                userManager.getUserAclRules(user.username).onComplete { aclResult ->
                                    val aclRules = if (aclResult.succeeded()) {
                                        aclResult.result().map { it.toAclRuleInfo() }
                                    } else {
                                        logger.warning("Failed to get ACL rules for user ${user.username}: ${aclResult.cause()?.message}")
                                        emptyList()
                                    }

                                    synchronized(userInfoList) {
                                        userInfoList.add(user.toUserInfo(aclRules))
                                        completed++
                                        if (completed == users.size) {
                                            future.complete(userInfoList.toList())
                                        }
                                    }
                                }
                            }
                        } else {
                            logger.severe("Error getting all users: ${usersResult.cause()?.message}")
                            future.completeExceptionally(usersResult.cause())
                        }
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
                
                userManager.getAllUsers().onComplete { usersResult ->
                    if (usersResult.succeeded()) {
                        val users = usersResult.result()
                        val userInfoList = users.map { user -> user.toUserInfo() }
                        future.complete(userInfoList)
                    } else {
                        logger.severe("Error getting all users: ${usersResult.cause()?.message}")
                        future.completeExceptionally(usersResult.cause())
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
                
                try {
                    val user = userManager.getUser(username)
                    future.complete(user?.toUserInfo())
                } catch (e: Exception) {
                    logger.severe("Error getting user $username: ${e.message}")
                    future.completeExceptionally(e)
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
                
                userManager.getAllAclRules().onComplete { rulesResult ->
                    if (rulesResult.succeeded()) {
                        val rules = rulesResult.result()
                        val ruleInfoList = rules.map { rule -> rule.toAclRuleInfo() }
                        future.complete(ruleInfoList)
                    } else {
                        logger.severe("Error getting all ACL rules: ${rulesResult.cause()?.message}")
                        future.completeExceptionally(rulesResult.cause())
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
                
                userManager.getUserAclRules(username).onComplete { rulesResult ->
                    if (rulesResult.succeeded()) {
                        val rules = rulesResult.result()
                        val ruleInfoList = rules.map { rule -> rule.toAclRuleInfo() }
                        future.complete(ruleInfoList)
                    } else {
                        logger.severe("Error getting ACL rules for user $username: ${rulesResult.cause()?.message}")
                        future.completeExceptionally(rulesResult.cause())
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
                
                try {
                    // Check if user already exists
                    val existingUser = userManager.getUser(createUserInput.username)
                    if (existingUser != null) {
                        future.complete(UserManagementResult(success = false, message = "User already exists"))
                        return@DataFetcher future
                    }

                    userManager.createUser(
                        username = createUserInput.username,
                        password = createUserInput.password,
                        enabled = createUserInput.enabled,
                        canSubscribe = createUserInput.canSubscribe,
                        canPublish = createUserInput.canPublish,
                        isAdmin = createUserInput.isAdmin
                    ).onComplete { createResult ->
                        if (createResult.succeeded() && createResult.result()) {
                            try {
                                val newUser = userManager.getUser(createUserInput.username)
                                future.complete(UserManagementResult(
                                    success = true,
                                    message = "User created successfully",
                                    user = newUser?.toUserInfo()
                                ))
                            } catch (e: Exception) {
                                logger.severe("Error retrieving created user: ${e.message}")
                                future.complete(UserManagementResult(
                                    success = false,
                                    message = "User created but error retrieving details: ${e.message}"
                                ))
                            }
                        } else {
                            val errorMsg = if (createResult.failed()) {
                                "Error creating user: ${createResult.cause()?.message}"
                            } else {
                                "Failed to create user"
                            }
                            logger.severe(errorMsg)
                            future.complete(UserManagementResult(success = false, message = errorMsg))
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Error creating user: ${e.message}")
                    future.complete(UserManagementResult(success = false, message = "Error creating user: ${e.message}"))
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
                
                try {
                    val existingUser = userManager.getUser(updateUserInput.username)
                    if (existingUser == null) {
                        future.complete(UserManagementResult(success = false, message = "User not found"))
                        return@DataFetcher future
                    }

                    val updatedUser = existingUser.copy(
                        enabled = updateUserInput.enabled ?: existingUser.enabled,
                        canSubscribe = updateUserInput.canSubscribe ?: existingUser.canSubscribe,
                        canPublish = updateUserInput.canPublish ?: existingUser.canPublish,
                        isAdmin = updateUserInput.isAdmin ?: existingUser.isAdmin
                    )

                    userManager.updateUser(updatedUser).onComplete { updateResult ->
                        if (updateResult.succeeded() && updateResult.result()) {
                            try {
                                val refreshedUser = userManager.getUser(updateUserInput.username)
                                future.complete(UserManagementResult(
                                    success = true,
                                    message = "User updated successfully",
                                    user = refreshedUser?.toUserInfo()
                                ))
                            } catch (e: Exception) {
                                logger.severe("Error retrieving updated user: ${e.message}")
                                future.complete(UserManagementResult(
                                    success = false,
                                    message = "User updated but error retrieving details: ${e.message}"
                                ))
                            }
                        } else {
                            val errorMsg = if (updateResult.failed()) {
                                "Error updating user: ${updateResult.cause()?.message}"
                            } else {
                                "Failed to update user"
                            }
                            logger.severe(errorMsg)
                            future.complete(UserManagementResult(success = false, message = errorMsg))
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Error updating user: ${e.message}")
                    future.complete(UserManagementResult(success = false, message = "Error updating user: ${e.message}"))
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
                
                try {
                    val existingUser = userManager.getUser(username)
                    if (existingUser == null) {
                        future.complete(UserManagementResult(success = false, message = "User not found"))
                        return@DataFetcher future
                    }

                    userManager.deleteUser(username).onComplete { deleteResult ->
                        if (deleteResult.succeeded() && deleteResult.result()) {
                            future.complete(UserManagementResult(
                                success = true,
                                message = "User deleted successfully"
                            ))
                        } else {
                            val errorMsg = if (deleteResult.failed()) {
                                "Error deleting user: ${deleteResult.cause()?.message}"
                            } else {
                                "Failed to delete user"
                            }
                            logger.severe("Error deleting user $username: $errorMsg")
                            future.complete(UserManagementResult(success = false, message = errorMsg))
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Error deleting user $username: ${e.message}")
                    future.complete(UserManagementResult(success = false, message = "Error deleting user: ${e.message}"))
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
                
                try {
                    val existingUser = userManager.getUser(setPasswordInput.username)
                    if (existingUser == null) {
                        future.complete(UserManagementResult(success = false, message = "User not found"))
                        return@DataFetcher future
                    }

                    userManager.setUserPassword(setPasswordInput.username, setPasswordInput.password).onComplete { passwordResult ->
                        if (passwordResult.succeeded() && passwordResult.result()) {
                            try {
                                val refreshedUser = userManager.getUser(setPasswordInput.username)
                                future.complete(UserManagementResult(
                                    success = true,
                                    message = "Password updated successfully",
                                    user = refreshedUser?.toUserInfo()
                                ))
                            } catch (e: Exception) {
                                logger.severe("Error retrieving user after password update: ${e.message}")
                                future.complete(UserManagementResult(
                                    success = false,
                                    message = "Password updated but error retrieving details: ${e.message}"
                                ))
                            }
                        } else {
                            val errorMsg = if (passwordResult.failed()) {
                                "Error setting password: ${passwordResult.cause()?.message}"
                            } else {
                                "Failed to update password"
                            }
                            logger.severe(errorMsg)
                            future.complete(UserManagementResult(success = false, message = errorMsg))
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Error setting password: ${e.message}")
                    future.complete(UserManagementResult(success = false, message = "Error setting password: ${e.message}"))
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
                
                userManager.createAclRule(
                    username = createAclRuleInput.username,
                    topicPattern = createAclRuleInput.topicPattern,
                    canSubscribe = createAclRuleInput.canSubscribe,
                    canPublish = createAclRuleInput.canPublish,
                    priority = createAclRuleInput.priority
                ).onComplete { createResult ->
                    if (createResult.succeeded() && createResult.result()) {
                        future.complete(UserManagementResult(
                            success = true,
                            message = "ACL rule created successfully"
                        ))
                    } else {
                        val errorMsg = if (createResult.failed()) {
                            "Error creating ACL rule: ${createResult.cause()?.message}"
                        } else {
                            "Failed to create ACL rule"
                        }
                        logger.severe(errorMsg)
                        future.complete(UserManagementResult(success = false, message = errorMsg))
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
                
                userManager.getAclRule(updateAclRuleInput.id).onComplete { getRuleResult ->
                    if (getRuleResult.succeeded()) {
                        val existingRule = getRuleResult.result()
                        if (existingRule == null) {
                            future.complete(UserManagementResult(success = false, message = "ACL rule not found"))
                            return@onComplete
                        }

                        val updatedRule = existingRule.copy(
                            username = updateAclRuleInput.username ?: existingRule.username,
                            topicPattern = updateAclRuleInput.topicPattern ?: existingRule.topicPattern,
                            canSubscribe = updateAclRuleInput.canSubscribe ?: existingRule.canSubscribe,
                            canPublish = updateAclRuleInput.canPublish ?: existingRule.canPublish,
                            priority = updateAclRuleInput.priority ?: existingRule.priority
                        )

                        userManager.updateAclRule(updatedRule).onComplete { updateResult ->
                            if (updateResult.succeeded() && updateResult.result()) {
                                userManager.getAclRule(updateAclRuleInput.id).onComplete { refreshResult ->
                                    if (refreshResult.succeeded()) {
                                        val refreshedRule = refreshResult.result()
                                        future.complete(UserManagementResult(
                                            success = true,
                                            message = "ACL rule updated successfully",
                                            aclRule = refreshedRule?.toAclRuleInfo()
                                        ))
                                    } else {
                                        logger.warning("ACL rule updated but failed to retrieve: ${refreshResult.cause()?.message}")
                                        future.complete(UserManagementResult(
                                            success = true,
                                            message = "ACL rule updated successfully"
                                        ))
                                    }
                                }
                            } else {
                                val errorMsg = if (updateResult.failed()) {
                                    "Error updating ACL rule: ${updateResult.cause()?.message}"
                                } else {
                                    "Failed to update ACL rule"
                                }
                                logger.severe(errorMsg)
                                future.complete(UserManagementResult(success = false, message = errorMsg))
                            }
                        }
                    } else {
                        logger.severe("Error getting ACL rule: ${getRuleResult.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error getting ACL rule: ${getRuleResult.cause()?.message}"
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
                
                userManager.getAclRule(id).onComplete { getRuleResult ->
                    if (getRuleResult.succeeded()) {
                        val existingRule = getRuleResult.result()
                        if (existingRule == null) {
                            future.complete(UserManagementResult(success = false, message = "ACL rule not found"))
                            return@onComplete
                        }

                        userManager.deleteAclRule(id).onComplete { deleteResult ->
                            if (deleteResult.succeeded() && deleteResult.result()) {
                                future.complete(UserManagementResult(
                                    success = true,
                                    message = "ACL rule deleted successfully"
                                ))
                            } else {
                                val errorMsg = if (deleteResult.failed()) {
                                    "Error deleting ACL rule: ${deleteResult.cause()?.message}"
                                } else {
                                    "Failed to delete ACL rule"
                                }
                                logger.severe("Error deleting ACL rule $id: $errorMsg")
                                future.complete(UserManagementResult(success = false, message = errorMsg))
                            }
                        }
                    } else {
                        logger.severe("Error getting ACL rule $id: ${getRuleResult.cause()?.message}")
                        future.complete(UserManagementResult(
                            success = false,
                            message = "Error getting ACL rule: ${getRuleResult.cause()?.message}"
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