package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.bus.IMessageBus
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.IMetricsStore
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.graphql.OpcUaClientConfigMutations
import at.rocworks.graphql.OpcUaClientConfigQueries
import at.rocworks.graphql.OpcUaServerQueries
import at.rocworks.graphql.OpcUaServerMutations
import at.rocworks.graphql.OpcUaServerInfo
import at.rocworks.graphql.OpcUaServerCertificateInfo
import at.rocworks.graphql.MqttClientConfigQueries
import at.rocworks.graphql.MqttClientConfigMutations
import at.rocworks.graphql.KafkaClientConfigQueries
import at.rocworks.graphql.KafkaClientConfigMutations
import at.rocworks.graphql.WinCCOaClientConfigQueries
import at.rocworks.graphql.WinCCOaClientConfigMutations
import at.rocworks.graphql.WinCCUaClientConfigQueries
import at.rocworks.graphql.WinCCUaClientConfigMutations
import at.rocworks.graphql.Plc4xClientConfigQueries
import at.rocworks.graphql.Plc4xClientConfigMutations
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.Monster
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
        private val logger: Logger = Utils.getLogger(GraphQLServer::class.java)
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

        logger.info("Creating HTTP server with options: port=$port, host=0.0.0.0")
        try {
            vertx.createHttpServer(options)
                .requestHandler(router)
                .listen(port, "0.0.0.0")  // Explicitly specify port and host
                .onSuccess {
                    logger.info("GraphQL server started successfully on port $port")
                    logger.info("GraphQL endpoint: http://localhost:$port$path")
                    logger.info("GraphQL WebSocket endpoint: ws://localhost:$port${path}ws")
                    logger.info("Dashboard available at: http://localhost:$port/dashboard")
                }
                .onFailure { err ->
                    logger.severe("Failed to start GraphQL server: ${err.message}")
                    err.printStackTrace()
                }
        } catch (e: Exception) {
            logger.severe("Exception creating HTTP server: ${e.message}")
            e.printStackTrace()
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
        val sessionResolver = SessionResolver(vertx, sessionStore, sessionHandler, authContext)

        // Initialize OPC UA resolvers
        val configStoreType = Monster.getConfigStoreType(config)

        val deviceStore = try {
            val store = if (configStoreType != "NONE") {
                DeviceConfigStoreFactory.create(configStoreType, config, vertx)
            } else {
                null
            }
            // Initialize device store asynchronously but don't block on it
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

        val opcUaQueries = deviceStore?.let { OpcUaClientConfigQueries(vertx, it) }
        val opcUaMutations = deviceStore?.let { OpcUaClientConfigMutations(vertx, it) }

        // Initialize OPC UA Server resolvers
        val opcUaServerQueries = deviceStore?.let { OpcUaServerQueries(vertx, it) }
        val opcUaServerMutations = deviceStore?.let { OpcUaServerMutations(vertx, it) }

        // Initialize MQTT Client resolvers
        val mqttClientQueries = deviceStore?.let { MqttClientConfigQueries(vertx, it) }
        val mqttClientMutations = deviceStore?.let { MqttClientConfigMutations(vertx, it) }

        // Initialize Kafka Client resolvers
        val kafkaClientQueries = deviceStore?.let { KafkaClientConfigQueries(vertx, it) }
        val kafkaClientMutations = deviceStore?.let { KafkaClientConfigMutations(vertx, it) }

        // Initialize WinCC OA Client resolvers
        val winCCOaClientQueries = deviceStore?.let { WinCCOaClientConfigQueries(vertx, it) }
        val winCCOaClientMutations = deviceStore?.let { WinCCOaClientConfigMutations(vertx, it) }

        // Initialize WinCC Unified Client resolvers
        val winCCUaClientQueries = deviceStore?.let { WinCCUaClientConfigQueries(vertx, it) }
        val winCCUaClientMutations = deviceStore?.let { WinCCUaClientConfigMutations(vertx, it) }

        // Initialize PLC4X Client resolvers
        val plc4xClientQueries = deviceStore?.let { Plc4xClientConfigQueries(vertx, it) }
        val plc4xClientMutations = deviceStore?.let { Plc4xClientConfigMutations(vertx, it) }

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
                    .dataFetcher("browseTopics", queryResolver.browseTopics())
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
                    // OPC UA Client queries
                    .apply {
                        opcUaQueries?.let { resolver ->
                            dataFetcher("opcUaDevices", resolver.opcUaDevices())
                            dataFetcher("opcUaDevice", resolver.opcUaDevice())
                            dataFetcher("opcUaDevicesByNode", resolver.opcUaDevicesByNode())
                            dataFetcher("clusterNodes", resolver.clusterNodes())
                        }
                    }
                    // OPC UA Server queries
                    .apply {
                        opcUaServerQueries?.let { resolver ->
                            dataFetcher("opcUaServers", resolver.opcUaServers())
                            dataFetcher("opcUaServer", resolver.opcUaServer())
                            dataFetcher("opcUaServersByNode", resolver.opcUaServersByNode())
                            dataFetcher("opcUaServerCertificates", resolver.opcUaServerCertificates())
                        }
                    }
                    // MQTT Client queries
                    .apply {
                        mqttClientQueries?.let { resolver ->
                            dataFetcher("mqttClients", resolver.mqttClients())
                            dataFetcher("mqttClient", resolver.mqttClient())
                            dataFetcher("mqttClientsByNode", resolver.mqttClientsByNode())
                        }
                    }
                    // Kafka Client queries
                    .apply {
                        kafkaClientQueries?.let { resolver ->
                            dataFetcher("kafkaClients", resolver.kafkaClients())
                            dataFetcher("kafkaClient", resolver.kafkaClient())
                            dataFetcher("kafkaClientsByNode", resolver.kafkaClientsByNode())
                        }
                    }
                    // WinCC OA Client queries
                    .apply {
                        winCCOaClientQueries?.let { resolver ->
                            dataFetcher("winCCOaClients", resolver.winCCOaClients())
                            dataFetcher("winCCOaClient", resolver.winCCOaClient())
                            dataFetcher("winCCOaClientsByNode", resolver.winCCOaClientsByNode())
                        }
                    }
                    // WinCC Unified Client queries
                    .apply {
                        winCCUaClientQueries?.let { resolver ->
                            dataFetcher("winCCUaClients", resolver.winCCUaClients())
                            dataFetcher("winCCUaClient", resolver.winCCUaClient())
                            dataFetcher("winCCUaClientsByNode", resolver.winCCUaClientsByNode())
                        }
                    }
                    // PLC4X Client queries
                    .apply {
                        plc4xClientQueries?.let { resolver ->
                            dataFetcher("plc4xClients", resolver.plc4xClients())
                            dataFetcher("plc4xClient", resolver.plc4xClient())
                            dataFetcher("plc4xClientsByNode", resolver.plc4xClientsByNode())
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
                    // User management mutations - grouped under user
                    .dataFetcher("user") { _ -> emptyMap<String, Any>() }
                    // Queued messages management (requires admin token)
                    .dataFetcher("purgeQueuedMessages", mutationResolver.purgeQueuedMessages())
                    // Session management mutations - grouped under session
                    .dataFetcher("session") { _ -> emptyMap<String, Any>() }
                    // Archive Group mutations - grouped under archiveGroup
                    .apply {
                        archiveGroupResolver?.let { _ ->
                            // Return an empty object - actual resolvers are on ArchiveGroupMutations type
                            dataFetcher("archiveGroup") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // OPC UA Device mutations - grouped under opcUaDevice
                    .apply {
                        opcUaMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on OpcUaDeviceMutations type
                            dataFetcher("opcUaDevice") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // OPC UA Server mutations - grouped under opcUaServer
                    .apply {
                        opcUaServerMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on OpcUaServerMutations type
                            dataFetcher("opcUaServer") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // MQTT Client mutations - grouped under mqttClient
                    .apply {
                        mqttClientMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on MqttClientMutations type
                            dataFetcher("mqttClient") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // Kafka Client mutations - grouped under kafkaClient
                    .apply {
                        kafkaClientMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on KafkaClientMutations type
                            dataFetcher("kafkaClient") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // WinCC OA Client mutations - grouped under winCCOaDevice
                    .apply {
                        winCCOaClientMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on WinCCOaDeviceMutations type
                            dataFetcher("winCCOaDevice") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // WinCC Unified Client mutations - grouped under winCCUaDevice
                    .apply {
                        winCCUaClientMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on WinCCUaDeviceMutations type
                            dataFetcher("winCCUaDevice") { _ -> emptyMap<String, Any>() }
                        }
                    }
                    // PLC4X Client mutations - grouped under plc4xDevice
                    .apply {
                        plc4xClientMutations?.let { _ ->
                            // Return an empty object - actual resolvers are on Plc4xDeviceMutations type
                            dataFetcher("plc4xDevice") { _ -> emptyMap<String, Any>() }
                        }
                    }
            }
            // Register MQTT Client Mutations type
            .type("MqttClientMutations") { builder ->
                builder.apply {
                    mqttClientMutations?.let { resolver ->
                        dataFetcher("create", resolver.createMqttClient())
                        dataFetcher("update", resolver.updateMqttClient())
                        dataFetcher("delete", resolver.deleteMqttClient())
                        dataFetcher("start", resolver.startMqttClient())
                        dataFetcher("stop", resolver.stopMqttClient())
                        dataFetcher("toggle", resolver.toggleMqttClient())
                        dataFetcher("reassign", resolver.reassignMqttClient())
                        dataFetcher("addAddress", resolver.addMqttClientAddress())
                        dataFetcher("deleteAddress", resolver.deleteMqttClientAddress())
                    }
                }
            }
            // Register Kafka Client Mutations type
            .type("KafkaClientMutations") { builder ->
                builder.apply {
                    kafkaClientMutations?.let { resolver ->
                        dataFetcher("create", resolver.createKafkaClient())
                        dataFetcher("update", resolver.updateKafkaClient())
                        dataFetcher("delete", resolver.deleteKafkaClient())
                        dataFetcher("start", resolver.startKafkaClient())
                        dataFetcher("stop", resolver.stopKafkaClient())
                        dataFetcher("toggle", resolver.toggleKafkaClient())
                        dataFetcher("reassign", resolver.reassignKafkaClient())
                    }
                }
            }
            // Register WinCC OA Device Mutations type
            .type("WinCCOaDeviceMutations") { builder ->
                builder.apply {
                    winCCOaClientMutations?.let { resolver ->
                        dataFetcher("create", resolver.createWinCCOaClient())
                        dataFetcher("update", resolver.updateWinCCOaClient())
                        dataFetcher("delete", resolver.deleteWinCCOaClient())
                        dataFetcher("start", resolver.startWinCCOaClient())
                        dataFetcher("stop", resolver.stopWinCCOaClient())
                        dataFetcher("toggle", resolver.toggleWinCCOaClient())
                        dataFetcher("reassign", resolver.reassignWinCCOaClient())
                        dataFetcher("addAddress", resolver.addWinCCOaClientAddress())
                        dataFetcher("deleteAddress", resolver.deleteWinCCOaClientAddress())
                    }
                }
            }
            // Register WinCC Unified Device Mutations type
            .type("WinCCUaDeviceMutations") { builder ->
                builder.apply {
                    winCCUaClientMutations?.let { resolver ->
                        dataFetcher("create", resolver.createWinCCUaClient())
                        dataFetcher("update", resolver.updateWinCCUaClient())
                        dataFetcher("delete", resolver.deleteWinCCUaClient())
                        dataFetcher("start", resolver.startWinCCUaClient())
                        dataFetcher("stop", resolver.stopWinCCUaClient())
                        dataFetcher("toggle", resolver.toggleWinCCUaClient())
                        dataFetcher("reassign", resolver.reassignWinCCUaClient())
                        dataFetcher("addAddress", resolver.addWinCCUaClientAddress())
                        dataFetcher("deleteAddress", resolver.deleteWinCCUaClientAddress())
                    }
                }
            }
            // Register OPC UA Device Mutations type
            .type("OpcUaDeviceMutations") { builder ->
                builder.apply {
                    opcUaMutations?.let { resolver ->
                        dataFetcher("add", resolver.addOpcUaDevice())
                        dataFetcher("update", resolver.updateOpcUaDevice())
                        dataFetcher("delete", resolver.deleteOpcUaDevice())
                        dataFetcher("toggle", resolver.toggleOpcUaDevice())
                        dataFetcher("reassign", resolver.reassignOpcUaDevice())
                        dataFetcher("addAddress", resolver.addOpcUaAddress())
                        dataFetcher("deleteAddress", resolver.deleteOpcUaAddress())
                    }
                }
            }
            // Register OPC UA Server Mutations type
            .type("OpcUaServerMutations") { builder ->
                builder.apply {
                    opcUaServerMutations?.let { resolver ->
                        dataFetcher("create", resolver.createOpcUaServer())
                        dataFetcher("start", resolver.startOpcUaServer())
                        dataFetcher("stop", resolver.stopOpcUaServer())
                        dataFetcher("delete", resolver.deleteOpcUaServer())
                        dataFetcher("addAddress", resolver.addOpcUaServerAddress())
                        dataFetcher("removeAddress", resolver.removeOpcUaServerAddress())
                        dataFetcher("trustCertificates", resolver.trustOpcUaServerCertificates())
                        dataFetcher("deleteCertificates", resolver.deleteOpcUaServerCertificates())
                    }
                }
            }
            // Register PLC4X Device Mutations type
            .type("Plc4xDeviceMutations") { builder ->
                builder.apply {
                    plc4xClientMutations?.let { resolver ->
                        dataFetcher("create", resolver.create())
                        dataFetcher("update", resolver.update())
                        dataFetcher("delete", resolver.delete())
                        dataFetcher("start", resolver.start())
                        dataFetcher("stop", resolver.stop())
                        dataFetcher("toggle", resolver.toggle())
                        dataFetcher("reassign", resolver.reassign())
                        dataFetcher("addAddress", resolver.addAddress())
                        dataFetcher("deleteAddress", resolver.deleteAddress())
                    }
                }
            }
            // Register Archive Group Mutations type
            .type("ArchiveGroupMutations") { builder ->
                builder.apply {
                    archiveGroupResolver?.let { resolver ->
                        dataFetcher("create", resolver.createArchiveGroup())
                        dataFetcher("update", resolver.updateArchiveGroup())
                        dataFetcher("delete", resolver.deleteArchiveGroup())
                        dataFetcher("enable", resolver.enableArchiveGroup())
                        dataFetcher("disable", resolver.disableArchiveGroup())
                    }
                }
            }
            // Register User Management Mutations type
            .type("UserManagementMutations") { builder ->
                builder.apply {
                    dataFetcher("createUser", userManagementResolver.createUser())
                    dataFetcher("updateUser", userManagementResolver.updateUser())
                    dataFetcher("deleteUser", userManagementResolver.deleteUser())
                    dataFetcher("setPassword", userManagementResolver.setPassword())
                    dataFetcher("createAclRule", userManagementResolver.createAclRule())
                    dataFetcher("updateAclRule", userManagementResolver.updateAclRule())
                    dataFetcher("deleteAclRule", userManagementResolver.deleteAclRule())
                }
            }
            // Register Session Management Mutations type
            .type("SessionMutations") { builder ->
                builder.apply {
                    dataFetcher("removeSessions", sessionResolver.removeSessions())
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
            .type("MqttClient") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.mqttClientMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.mqttClientMetricsHistory())
            }
            .type("KafkaClient") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.kafkaClientMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.kafkaClientMetricsHistory())
            }
            .type("WinCCOaClient") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.winCCOaClientMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.winCCOaClientMetricsHistory())
            }
            .type("WinCCUaClient") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.winCCUaClientMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.winCCUaClientMetricsHistory())
            }
            .type("Plc4xClient") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.plc4xClientMetrics())
                    .dataFetcher("metricsHistory", metricsResolver.plc4xClientMetricsHistory())
            }
            .type("ArchiveGroupInfo") { builder ->
                builder.apply {
                    archiveGroupResolver?.let { resolver ->
                        dataFetcher("connectionStatus", resolver.connectionStatus())
                    }
                    dataFetcher("metrics", metricsResolver.archiveGroupMetricsField())
                    dataFetcher("metricsHistory", metricsResolver.archiveGroupMetricsHistoryField())
                }
            }
            .type("Topic") { builder ->
                builder
                    .dataFetcher("value", queryResolver.topicValue())
            }
            .type("OpcUaServer") { builder ->
                builder.apply {
                    opcUaServerQueries?.let { resolver ->
                        dataFetcher("trustedCertificates") { env ->
                            val server = env.getSource<OpcUaServerInfo>()
                            if (server != null) {
                                resolver.opcUaServerCertificates().get(
                                    graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment(env)
                                        .arguments(mapOf(
                                            "serverName" to server.name,
                                            "trusted" to true
                                        ))
                                        .build()
                                )
                            } else {
                                java.util.concurrent.CompletableFuture.completedFuture(emptyList<OpcUaServerCertificateInfo>())
                            }
                        }
                        dataFetcher("untrustedCertificates") { env ->
                            val server = env.getSource<OpcUaServerInfo>()
                            if (server != null) {
                                resolver.opcUaServerCertificates().get(
                                    graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment(env)
                                        .arguments(mapOf(
                                            "serverName" to server.name,
                                            "trusted" to false
                                        ))
                                        .build()
                                )
                            } else {
                                java.util.concurrent.CompletableFuture.completedFuture(emptyList<OpcUaServerCertificateInfo>())
                            }
                        }
                    }
                }
            }
            .type("OpcUaDevice") { builder ->
                builder
                    .dataFetcher("metrics", metricsResolver.opcUaDeviceMetricsField())
                    .dataFetcher("metricsHistory", metricsResolver.opcUaDeviceMetricsHistoryField())
            }
            .build()
    }
}