package at.rocworks.extensions.graphql

import at.rocworks.auth.UserManager
import at.rocworks.bus.IMessageBus
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.IMetricsStore
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.graphql.DeviceConfigMutations
import at.rocworks.graphql.DeviceConfigQueries
import at.rocworks.stores.DeviceConfigStoreFactory
import graphql.GraphQL
import graphql.scalars.ExtendedScalars
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.handler.graphql.GraphQLHandler
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler
import java.util.logging.Logger

class GraphQLServer(
    private val vertx: Vertx,
    private val config: JsonObject,
    private val messageBus: IMessageBus,
    private val messageHandler: MessageHandler,
    private val retainedStore: IMessageStore?,
    private val archiveGroups: Map<String, ArchiveGroup>,
    private val userManager: UserManager,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: SessionHandler,
    private val metricsStore: IMetricsStore?,
    private val archiveHandler: ArchiveHandler?
) {
    companion object {
        private val logger: Logger = Logger.getLogger(GraphQLServer::class.java.name)
    }

    private val graphQLConfig = config.getJsonObject("GraphQL", JsonObject())
    private val port = graphQLConfig.getInteger("Port", 8080)
    private val path = graphQLConfig.getString("Path", "/graphql")
    private val authContext = GraphQLAuthContext(userManager)

    fun start() {
        logger.info("Starting GraphQL server on port $port")

        val schema = loadSchema()
        val graphQL = createGraphQL(schema)

        val router = Router.router(vertx)

        // Smart CORS handler that works with both browsers and API clients
        router.route().handler { ctx ->
            val origin = ctx.request().getHeader("Origin")

            // If there's an Origin header (browser request), add CORS headers
            if (origin != null) {
                ctx.response()
                    .putHeader("Access-Control-Allow-Origin", origin)  // Echo back the origin
                    .putHeader("Access-Control-Allow-Credentials", "true")
                    .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                    .putHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                    .putHeader("Access-Control-Max-Age", "3600")

                // Handle preflight OPTIONS request
                if (ctx.request().method() == HttpMethod.OPTIONS) {
                    ctx.response().setStatusCode(204).end()
                    return@handler
                }
            }

            ctx.next()
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
                // Skip validation for OPTIONS requests (CORS preflight)
                if (ctx.request().method() == HttpMethod.OPTIONS) {
                    ctx.next()
                    return@handler
                }

                // Check if request has a query (only for POST requests)
                if (ctx.request().method() == HttpMethod.POST) {
                    val body = ctx.body()?.asJsonObject()
                    if (body == null || (!body.containsKey("query") && !body.containsKey("variables"))) {
                        ctx.response()
                            .setStatusCode(400)
                            .putHeader("content-type", "application/json")
                            .end(JsonObject().put("error", "Query is missing").encode())
                        return@handler
                    }
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

        // Serve dashboard static files
        router.route("/*").handler(
            StaticHandler.create("dashboard")
                .setIndexPage("pages/login.html")  // Default to login page
                .setCachingEnabled(false)  // Disable caching for development
        )

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
        val queryResolver = QueryResolver(vertx, retainedStore, archiveHandler, authContext)
        val metricsResolver = MetricsResolver(vertx, sessionStore, sessionHandler, metricsStore)
        val mutationResolver = MutationResolver(vertx, messageBus, messageHandler, sessionStore, sessionHandler, authContext)
        val subscriptionResolver = SubscriptionResolver(vertx, messageBus)
        val userManagementResolver = UserManagementResolver(vertx, userManager, authContext)
        val authenticationResolver = AuthenticationResolver(vertx, userManager)
        val archiveGroupResolver = archiveHandler?.let { ArchiveGroupResolver(vertx, it, authContext) }

        // Initialize OPC UA resolvers
        val configStoreType = config.getString("ConfigStoreType")

        val deviceStore = try {
            val store = DeviceConfigStoreFactory.create(configStoreType, config, vertx)
            store?.initialize()?.onComplete { result ->
                if (result.failed()) {
                    logger.warning("Failed to initialize OPC UA device store: ${result.cause()?.message}")
                } else {
                    logger.info("OPC UA device store initialized successfully")
                }
            }
            store
        } catch (e: NotImplementedError) {
            logger.warning("OPC UA device store not implemented for $configStoreType, OPC UA features will be disabled")
            null
        } catch (e: Exception) {
            logger.warning("Failed to create OPC UA device store: ${e.message}")
            null
        }

        val opcUaQueries = deviceStore?.let { DeviceConfigQueries(vertx, it) }
        val opcUaMutations = deviceStore?.let { DeviceConfigMutations(vertx, it) }

        return RuntimeWiring.newRuntimeWiring()
            // Register scalar types
            .scalar(ExtendedScalars.GraphQLLong)
            .scalar(ExtendedScalars.Json)
            // Register custom enum type resolvers
            .type("DataFormat") { builder ->
                builder.enumValues { name -> DataFormat.valueOf(name) }
            }
            .type("MessageStoreType") { builder ->
                builder.enumValues { name -> name }
            }
            .type("MessageArchiveType") { builder ->
                builder.enumValues { name -> name }
            }
            .type("SecurityPolicy") { builder ->
                builder.enumValues { name -> name }
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
                    // ArchiveGroup queries
                    .apply {
                        archiveGroupResolver?.let { resolver ->
                            dataFetcher("archiveGroups", resolver.archiveGroups())
                            dataFetcher("archiveGroup", resolver.archiveGroup())
                        }
                    }
                    // OPC UA queries
                    .apply {
                        opcUaQueries?.let { resolver ->
                            dataFetcher("opcUaDevices", resolver.opcUaDevices())
                            dataFetcher("opcUaDevice", resolver.opcUaDevice())
                            dataFetcher("opcUaDevicesByNode", resolver.opcUaDevicesByNode())
                            dataFetcher("clusterNodes", resolver.clusterNodes())
                        }
                    }
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
                    // ArchiveGroup mutations
                    .apply {
                        archiveGroupResolver?.let { resolver ->
                            dataFetcher("createArchiveGroup", resolver.createArchiveGroup())
                            dataFetcher("updateArchiveGroup", resolver.updateArchiveGroup())
                            dataFetcher("deleteArchiveGroup", resolver.deleteArchiveGroup())
                            dataFetcher("enableArchiveGroup", resolver.enableArchiveGroup())
                            dataFetcher("disableArchiveGroup", resolver.disableArchiveGroup())
                        }
                    }
                    // OPC UA mutations
                    .apply {
                        opcUaMutations?.let { resolver ->
                            dataFetcher("addOpcUaDevice", resolver.addOpcUaDevice())
                            dataFetcher("updateOpcUaDevice", resolver.updateOpcUaDevice())
                            dataFetcher("deleteOpcUaDevice", resolver.deleteOpcUaDevice())
                            dataFetcher("toggleOpcUaDevice", resolver.toggleOpcUaDevice())
                            dataFetcher("reassignOpcUaDevice", resolver.reassignOpcUaDevice())
                            dataFetcher("addOpcUaAddress", resolver.addOpcUaAddress())
                            dataFetcher("deleteOpcUaAddress", resolver.deleteOpcUaAddress())
                        }
                    }
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
                    .dataFetcher("metricsHistory", metricsResolver.brokerMetricsHistory())
            }
            .type("Session") { builder ->
                builder
                    .dataFetcher("queuedMessageCount", metricsResolver.sessionQueuedMessageCount())
                    .dataFetcher("metrics", metricsResolver.sessionMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.sessionMetricsHistory())
            }
            .type("ArchiveGroupInfo") { builder ->
                builder.apply {
                    archiveGroupResolver?.let { resolver ->
                        dataFetcher("connectionStatus", resolver.connectionStatus())
                    }
                }
            }
            .build()
    }
}