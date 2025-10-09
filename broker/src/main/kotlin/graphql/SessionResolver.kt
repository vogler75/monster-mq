package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.ISessionStoreAsync
import graphql.schema.DataFetcher
import graphql.GraphQLException
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class SessionResolver(
    private val vertx: Vertx,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: SessionHandler,
    private val authContext: GraphQLAuthContext
) {
    companion object {
        private val logger: Logger = Utils.getLogger(SessionResolver::class.java)
    }

    fun removeSessions(): DataFetcher<CompletableFuture<SessionRemovalResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<SessionRemovalResult>()

            // Check authorization - requires admin privileges
            val authResult = authContext.validateFieldAccess(env)
            if (!authResult.allowed) {
                future.complete(
                    SessionRemovalResult(
                        success = false,
                        message = authResult.errorMessage ?: "Admin privileges required",
                        removedCount = 0,
                        results = emptyList()
                    )
                )
                return@DataFetcher future
            }

            val clientIds = env.getArgument<List<String>>("clientIds") ?: emptyList()

            if (clientIds.isEmpty()) {
                future.complete(
                    SessionRemovalResult(
                        success = true,
                        message = "No client IDs provided",
                        removedCount = 0,
                        results = emptyList()
                    )
                )
                return@DataFetcher future
            }

            logger.info("Removing ${clientIds.size} session(s): ${clientIds.joinToString(", ")}")

            val results = mutableListOf<SessionRemovalDetail>()
            var successCount = 0
            var processedCount = 0

            // Process each clientId
            clientIds.forEach { clientId ->
                try {
                    // 1. Check if session exists in store
                    sessionStore.isPresent(clientId).onComplete { isPresentResult ->
                        if (isPresentResult.succeeded() && isPresentResult.result()) {
                            // Session exists - disconnect it via bus command (cluster-aware)
                            sessionHandler.disconnectClient(clientId, "Removed via GraphQL API")

                            // Remove session from database (for persistent sessions)
                            sessionStore.delClient(clientId) { subscription ->
                                logger.finest("Deleted subscription for removed session: $subscription")
                            }.onComplete { delResult ->
                                processedCount++

                                if (delResult.succeeded()) {
                                    successCount++
                                    results.add(
                                        SessionRemovalDetail(
                                            clientId = clientId,
                                            success = true,
                                            error = null,
                                            nodeId = null // Node ID is not tracked after deletion
                                        )
                                    )
                                    logger.info("Successfully removed session: $clientId")
                                } else {
                                    results.add(
                                        SessionRemovalDetail(
                                            clientId = clientId,
                                            success = false,
                                            error = "Failed to remove session from database: ${delResult.cause()?.message}",
                                            nodeId = null
                                        )
                                    )
                                    logger.warning("Failed to remove session $clientId from database: ${delResult.cause()?.message}")
                                }

                                // Check if all sessions have been processed
                                if (processedCount == clientIds.size) {
                                    future.complete(
                                        SessionRemovalResult(
                                            success = successCount == clientIds.size,
                                            message = if (successCount == clientIds.size) {
                                                "Successfully removed all $successCount session(s)"
                                            } else {
                                                "Removed $successCount out of ${clientIds.size} session(s)"
                                            },
                                            removedCount = successCount,
                                            results = results
                                        )
                                    )
                                }
                            }
                        } else {
                            // Session not found
                            processedCount++
                            results.add(
                                SessionRemovalDetail(
                                    clientId = clientId,
                                    success = false,
                                    error = "Session not found",
                                    nodeId = null
                                )
                            )
                            logger.warning("Session not found: $clientId")

                            // Check if all sessions have been processed
                            if (processedCount == clientIds.size) {
                                future.complete(
                                    SessionRemovalResult(
                                        success = successCount == clientIds.size,
                                        message = if (successCount == clientIds.size) {
                                            "Successfully removed all $successCount session(s)"
                                        } else {
                                            "Removed $successCount out of ${clientIds.size} session(s)"
                                        },
                                        removedCount = successCount,
                                        results = results
                                    )
                                )
                            }
                        }
                    }.onFailure { error ->
                        processedCount++
                        results.add(
                            SessionRemovalDetail(
                                clientId = clientId,
                                success = false,
                                error = "Error checking session existence: ${error.message}",
                                nodeId = null
                            )
                        )
                        logger.severe("Error checking if session $clientId exists: ${error.message}")

                        // Check if all sessions have been processed
                        if (processedCount == clientIds.size) {
                            future.complete(
                                SessionRemovalResult(
                                    success = successCount == clientIds.size,
                                    message = if (successCount == clientIds.size) {
                                        "Successfully removed all $successCount session(s)"
                                    } else {
                                        "Removed $successCount out of ${clientIds.size} session(s)"
                                    },
                                    removedCount = successCount,
                                    results = results
                                )
                            )
                        }
                    }
                } catch (e: Exception) {
                    processedCount++
                    results.add(
                        SessionRemovalDetail(
                            clientId = clientId,
                            success = false,
                            error = "Exception during removal: ${e.message}",
                            nodeId = null
                        )
                    )
                    logger.severe("Exception removing session $clientId: ${e.message}")

                    // Check if all sessions have been processed
                    if (processedCount == clientIds.size) {
                        future.complete(
                            SessionRemovalResult(
                                success = successCount == clientIds.size,
                                message = if (successCount == clientIds.size) {
                                    "Successfully removed all $successCount session(s)"
                                } else {
                                    "Removed $successCount out of ${clientIds.size} session(s)"
                                },
                                removedCount = successCount,
                                results = results
                            )
                        )
                    }
                }
            }

            future
        }
    }
}

// Data classes for session removal results
data class SessionRemovalResult(
    val success: Boolean,
    val message: String?,
    val removedCount: Int,
    val results: List<SessionRemovalDetail>
)

data class SessionRemovalDetail(
    val clientId: String,
    val success: Boolean,
    val error: String?,
    val nodeId: String?
)
