package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.IMessageStoreExtended

import io.vertx.core.*
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import java.util.UUID

/**
 * Streamable HTTP server for MCP (Message Control Protocol) handling.
 */
class McpServer(
    private val host: String,
    private val port: Int,
    private val retainedStore: IMessageStore,
    private val messageStore: IMessageStore?,
    private val messageArchive: IMessageArchive?
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    class Connection(
        val connectionId: String,
        val response: HttpServerResponse
    )

    private val connections: MutableMap<String, Connection> = mutableMapOf()

    // Companion object for constants
    companion object {
        private const val MCP_PATH = "/mcp"
    }

    // Check if archive group is extended
    private val isArchiveGroupExtended = messageArchive != null &&
        retainedStore is IMessageStoreExtended &&
        (messageStore != null && messageStore is IMessageStoreExtended)

    private var mcpHandler: McpHandler? = null

    override fun start(startPromise: Promise<Void>) {
        logger.info("Archive group extended: $isArchiveGroupExtended")

        mcpHandler = McpHandler(vertx, retainedStore, messageStore, messageArchive) // McpHandler is initialized

        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        // Post handler for MCP requests
        router.post(MCP_PATH).handler { ctx: RoutingContext ->
            logger.info("Post request received for MCP at path $MCP_PATH")
            val body = ctx.body().asJsonObject()
            mcpHandler!!.handleRequest(body)
                .onComplete { ar ->
                    if (ar.succeeded()) {
                        val result: JsonObject? = ar.result()
                        if (result != null) {
                            ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(result.encode())
                        } else {
                            // For notifications that return null
                            ctx.response()
                                .setStatusCode(204)
                                .end()
                        }
                    } else {
                        ctx.response()
                            .setStatusCode(500)
                            .putHeader("Content-Type", "application/json")
                            .end(
                                JsonObject()
                                    .put("error", ar.cause()?.message ?: "Unknown error")
                                    .encode()
                            )
                    }
                }
        }

        // Get handler for MCP
        /*
        router.get(MCP_PATH).handler { ctx: RoutingContext ->
            logger.warning("GET request received for $MCP_PATH. This endpoint does not support GET requests for SSE in this configuration.")
            ctx.response()
                .setStatusCode(405) // 405 Method Not Allowed
                .putHeader("Content-Type", "application/json")
                .end(
                    JsonObject()
                        .put("error", "GET method on $MCP_PATH is not acceptable. Server-Sent Events (SSE) are not supported or enabled for this path.")
                        .encode()
                )
        }
        */
        // Get handler for MCP
        router.get("/mcp").handler { ctx: RoutingContext ->
            logger.info("Get request received for MCP: " + ctx.request().headers())
            val response = ctx.response()
            response.putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .putHeader("Connection", "keep-alive")
                .putHeader("Access-Control-Allow-Origin", "*")
                .setChunked(true)

            val connectionId = UUID.randomUUID().toString()
            val connection = Connection(connectionId, response)
            connections.put(connectionId, connection)

            // Send initial connection event
            sendMessage(
                connection, "connected", JsonObject().put("connectionId", connectionId)
            )

            // Send heartbeat every 30 seconds
            val timerId = vertx.setPeriodic(10000, Handler { id: Long? ->
                if (connections.containsKey(connectionId)) {
                    sendMessage(
                        connection, "heartbeat", JsonObject()
                            .put("timestamp", System.currentTimeMillis())
                    )
                }
            })

            // Handle connection close
            response.closeHandler { _ ->
                vertx.cancelTimer(timerId)
                connections.remove(connectionId)
                logger.info("Connection closed: $connectionId")
            }

            // Handle client disconnect
            response.exceptionHandler { throwable: Throwable? ->
                vertx.cancelTimer(timerId)
                connections.remove(connectionId)
                logger.severe("Connection error: " + connectionId + " - " + throwable!!.message)
            }
        }


        // HTTP server setup
        val options = HttpServerOptions()
            .setPort(port)
            .setHost(host)

        vertx.createHttpServer(options)
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                logger.info("MCP Server started on port ${server.actualPort()}")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("MCP Server failed to start $error")
                startPromise.fail(error)
            }
    }

    private fun sendMessage(
        connection: Connection,
        string: String,
        put: JsonObject
    ) {
        val message = JsonObject()
            .put("event", string)
            .put("data", put)

        connection.response.write("data: ${message.encode()}\n\n")
        logger.info("Sent message to connection ${connection.connectionId}: $message")
    }
}


