package at.rocworks.extensions.graphql

import at.rocworks.auth.UserManager
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking

class AuthenticationResolver(
    private val vertx: Vertx,
    private val userManager: UserManager
) {
    companion object {
        private val logger: Logger = Logger.getLogger(AuthenticationResolver::class.java.name)
    }

    /**
     * Login mutation - authenticates user and returns JWT token
     * This is the only GraphQL operation that doesn't require authentication
     */
    fun login(): DataFetcher<CompletableFuture<LoginResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<LoginResult>()
            
            try {
                // Check if user management is enabled first
                if (!userManager.isUserManagementEnabled()) {
                    // Authentication is disabled, return success with null token
                    future.complete(LoginResult(
                        success = true,
                        token = null,
                        message = "Authentication disabled",
                        username = "anonymous",
                        isAdmin = true
                    ))
                    return@DataFetcher future
                }

                val username = env.getArgument<String>("username") ?: ""
                val password = env.getArgument<String>("password") ?: ""

                if (username.isEmpty() || password.isEmpty()) {
                    future.complete(LoginResult(
                        success = false,
                        message = "Username and password are required"
                    ))
                    return@DataFetcher future
                }
                
                // Now authenticate() returns Future<User?>, so we handle it directly
                userManager.authenticate(username, password).onComplete { authResult ->
                    try {
                        if (authResult.succeeded()) {
                            val user = authResult.result()

                            if (user != null && user.enabled) {
                                // Generate JWT token
                                val token = JwtService.generateToken(user.username, user.isAdmin)

                                future.complete(LoginResult(
                                    success = true,
                                    token = token,
                                    message = "Login successful",
                                    username = user.username,
                                    isAdmin = user.isAdmin
                                ))
                            } else {
                                future.complete(LoginResult(
                                    success = false,
                                    message = "Invalid username or password"
                                ))
                            }
                        } else {
                            logger.severe("Error during login: ${authResult.cause()?.message}")
                            future.complete(LoginResult(
                                success = false,
                                message = "Authentication failed"
                            ))
                        }
                    } catch (e: Exception) {
                        logger.severe("Error during login: ${e.message}")
                        future.complete(LoginResult(
                            success = false,
                            message = "Authentication failed"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error in login resolver: ${e.message}")
                future.complete(LoginResult(
                    success = false,
                    message = "Authentication failed"
                ))
            }
            
            future
        }
    }
}