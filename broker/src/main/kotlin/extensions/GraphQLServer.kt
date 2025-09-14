package at.rocworks.extensions

import at.rocworks.Monster
import at.rocworks.auth.UserManager
import at.rocworks.bus.IMessageBus
import at.rocworks.data.MqttMessage
import at.rocworks.extensions.graphql.*
import at.rocworks.stores.*
import graphql.GraphQL
import graphql.scalars.ExtendedScalars
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import graphql.schema.idl.TypeDefinitionRegistry
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.graphql.GraphQLHandler
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler
import java.util.logging.Logger

class GraphQLServer(
    private val vertx: Vertx,
    private val config: JsonObject,
    private val messageBus: IMessageBus,
    private val messageHandler: at.rocworks.handlers.MessageHandler,
    private val retainedStore: IMessageStore?,
    private val archiveGroups: Map<String, ArchiveGroup>,
    private val userManager: UserManager,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: at.rocworks.handlers.SessionHandler,
    private val metricsStore: at.rocworks.stores.IMetricsStore?
) {
    companion object {
        private val logger: Logger = Logger.getLogger(GraphQLServer::class.java.name)
    }

    private val port = config.getInteger("Port", 8080)
    private val corsEnabled = config.getBoolean("CorsEnabled", true)
    private val path = config.getString("Path", "/graphql")
    private val authContext = GraphQLAuthContext(userManager)

    fun start() {
        logger.info("Starting GraphQL server on port $port")

        val schema = loadSchema()
        val graphQL = createGraphQL(schema)

        val router = Router.router(vertx)

        // Add CORS handler if enabled
        if (corsEnabled) {
            router.route().handler(
                CorsHandler.create()
                    .addOrigin("*")
                    .allowedHeader("*")
                    .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                    .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                    .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
            )
        }

        // Add body handler for POST requests
        router.route().handler(BodyHandler.create())

        // Create GraphQL handler
        val graphQLHandler = GraphQLHandler.create(
            graphQL,
            GraphQLHandlerOptions()
                .setRequestBatchingEnabled(true)
        )

        // Create WebSocket handler for subscriptions
        val wsHandler = GraphQLWSHandler.create(graphQL)

        // Setup routes with auth injection middleware and GraphQL handler
        router.route(path).handler { ctx ->
            try {
                // Check if request has a query
                val body = ctx.body()?.asJsonObject()
                if (body == null || (!body.containsKey("query") && !body.containsKey("variables"))) {
                    ctx.response()
                        .setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end(JsonObject().put("error", "Query is missing").encode())
                    return@handler
                }

                // Extract auth context and set it in thread-local for resolvers
                val authCtx = authContext.extractAuthContext(ctx)
                AuthContextService.setAuthContext(authCtx)
                ctx.next()
            } catch (e: Exception) {
                logger.severe("Error setting auth context: ${e.message}")
                ctx.fail(500, e)
            }
        }.handler(graphQLHandler).handler { ctx ->
            // Clear auth context after GraphQL execution to prevent memory leaks
            try {
                AuthContextService.clearAuthContext()
            } catch (e: Exception) {
                logger.warning("Error clearing auth context: ${e.message}")
            }
            // Continue with response
        }
        router.route("${path}ws").handler(wsHandler)

        // Health check endpoint
        router.get("/health").handler { ctx ->
            ctx.response()
                .putHeader("content-type", "application/json")
                .end(JsonObject().put("status", "healthy").encode())
        }

        // Create HTTP server
        val options = HttpServerOptions()
            .setPort(port)
            .setHost("0.0.0.0")

        vertx.createHttpServer(options)
            .requestHandler(router)
            .listen()
            .onSuccess {
                logger.info("GraphQL server started successfully on port $port")
                logger.info("GraphQL endpoint: http://localhost:$port$path")
                logger.info("GraphQL WebSocket endpoint: ws://localhost:$port${path}ws")
            }
            .onFailure { err ->
                logger.severe("Failed to start GraphQL server: ${err.message}")
            }
    }

    private fun loadSchema(): String {
        return this::class.java.classLoader.getResourceAsStream("schema.graphqls")
            ?.bufferedReader()
            ?.use { it.readText() }
            ?: throw RuntimeException("Failed to load GraphQL schema")
    }

    private fun createGraphQL(schemaDefinition: String): GraphQL {
        val typeRegistry = SchemaParser().parse(schemaDefinition)
        val runtimeWiring = buildRuntimeWiring()
        val graphQLSchema = SchemaGenerator().makeExecutableSchema(typeRegistry, runtimeWiring)

        return GraphQL.newGraphQL(graphQLSchema).build()
    }
    
    

    private fun buildRuntimeWiring(): RuntimeWiring {
        val queryResolver = QueryResolver(vertx, retainedStore, archiveGroups, authContext)
        val metricsResolver = MetricsResolver(vertx, sessionStore, sessionHandler, metricsStore)
        val mutationResolver = MutationResolver(vertx, messageBus, messageHandler, sessionStore, sessionHandler, authContext)
        val subscriptionResolver = SubscriptionResolver(vertx, messageBus)
        val userManagementResolver = UserManagementResolver(vertx, userManager, authContext)
        val authenticationResolver = AuthenticationResolver(vertx, userManager)

        return RuntimeWiring.newRuntimeWiring()
            // Register scalar types
            .scalar(ExtendedScalars.GraphQLLong)
            .scalar(ExtendedScalars.Json)
            // Register custom enum type resolver
            .type("DataFormat") { builder ->
                builder.enumValues { name -> DataFormat.valueOf(name) }
            }
            // Register query resolvers
            .type("Query") { builder ->
                builder
                    .dataFetcher("currentValue", queryResolver.currentValue())
                    .dataFetcher("currentValues", queryResolver.currentValues())
                    .dataFetcher("retainedMessage", queryResolver.retainedMessage())
                    .dataFetcher("retainedMessages", queryResolver.retainedMessages())
                    .dataFetcher("archivedMessages", queryResolver.archivedMessages())
                    .dataFetcher("searchTopics", queryResolver.searchTopics())
                    // Metrics queries
                    .dataFetcher("broker", metricsResolver.broker())
                    .dataFetcher("brokers", metricsResolver.brokers())
                    .dataFetcher("sessions", metricsResolver.sessions())
                    .dataFetcher("session", metricsResolver.session())
                    // User management queries
                    .dataFetcher("users", userManagementResolver.users())
            }
            // Register mutation resolvers
            .type("Mutation") { builder ->
                builder
                    // Authentication (no token required)
                    .dataFetcher("login", authenticationResolver.login())
                    // Publishing (requires token + ACL check)
                    .dataFetcher("publish", mutationResolver.publish())
                    .dataFetcher("publishBatch", mutationResolver.publishBatch())
                    // User management mutations (requires admin token)
                    .dataFetcher("createUser", userManagementResolver.createUser())
                    .dataFetcher("updateUser", userManagementResolver.updateUser())
                    .dataFetcher("deleteUser", userManagementResolver.deleteUser())
                    .dataFetcher("setPassword", userManagementResolver.setPassword())
                    .dataFetcher("createAclRule", userManagementResolver.createAclRule())
                    .dataFetcher("updateAclRule", userManagementResolver.updateAclRule())
                    .dataFetcher("deleteAclRule", userManagementResolver.deleteAclRule())
                    // Queued messages management (requires admin token)
                    .dataFetcher("purgeQueuedMessages", mutationResolver.purgeQueuedMessages())
            }
            // Register subscription resolvers
            .type("Subscription") { builder ->
                builder
                    .dataFetcher("topicUpdates", subscriptionResolver.topicUpdates())
                    .dataFetcher("multiTopicUpdates", subscriptionResolver.multiTopicUpdates())
            }
            // Register field resolvers for types
            .type("Broker") { builder ->
                builder
                    .dataFetcher("sessions", metricsResolver.brokerSessions())
                    .dataFetcher("metrics", metricsResolver.brokerMetrics())
            }
            .type("Session") { builder ->
                builder
                    .dataFetcher("queuedMessageCount", metricsResolver.sessionQueuedMessageCount())
                    .dataFetcher("metrics", metricsResolver.sessionMetrics())
            }
            .build()
    }
}