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
                val username = env.getArgument<String>("username") ?: ""
                val password = env.getArgument<String>("password") ?: ""

                if (username.isEmpty() || password.isEmpty()) {
                    future.complete(LoginResult(
                        success = false,
                        message = "Username and password are required"
                    ))
                    return@DataFetcher future
                }
                
                // Check if user management is enabled
                if (!userManager.isUserManagementEnabled()) {
                    future.complete(LoginResult(
                        success = false,
                        message = "Authentication is not enabled"
                    ))
                    return@DataFetcher future
                }
                
                vertx.executeBlocking<LoginResult> {
                    runBlocking {
                        try {
                            // Authenticate user
                            val user = userManager.authenticate(username, password)
                            
                            if (user != null && user.enabled) {
                                // Generate JWT token
                                val token = JwtService.generateToken(user.username, user.isAdmin)
                                
                                LoginResult(
                                    success = true,
                                    token = token,
                                    message = "Login successful",
                                    username = user.username,
                                    isAdmin = user.isAdmin
                                )
                            } else {
                                LoginResult(
                                    success = false,
                                    message = "Invalid username or password"
                                )
                            }
                        } catch (e: Exception) {
                            logger.severe("Error during login: ${e.message}")
                            LoginResult(
                                success = false,
                                message = "Authentication failed"
                            )
                        }
                    }
                }.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.severe("Error processing login: ${result.cause()?.message}")
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