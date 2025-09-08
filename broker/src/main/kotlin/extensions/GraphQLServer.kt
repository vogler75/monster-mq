package at.rocworks.extensions

import at.rocworks.Monster
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
    private val retainedStore: IMessageStore?,
    private val archiveGroups: Map<String, ArchiveGroup>
) {
    companion object {
        private val logger: Logger = Logger.getLogger(GraphQLServer::class.java.name)
    }

    private val port = config.getInteger("Port", 8080)
    private val corsEnabled = config.getBoolean("CorsEnabled", true)
    private val path = config.getString("Path", "/graphql")

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
            GraphQLHandlerOptions().setRequestBatchingEnabled(true)
        )

        // Create WebSocket handler for subscriptions
        val wsHandler = GraphQLWSHandler.create(graphQL)

        // Setup routes
        router.route(path).handler(graphQLHandler)
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
        val queryResolver = QueryResolver(vertx, retainedStore, archiveGroups)
        val mutationResolver = MutationResolver(vertx, messageBus)
        val subscriptionResolver = SubscriptionResolver(vertx, messageBus)

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
            }
            // Register mutation resolvers
            .type("Mutation") { builder ->
                builder
                    .dataFetcher("publish", mutationResolver.publish())
                    .dataFetcher("publishBatch", mutationResolver.publishBatch())
            }
            // Register subscription resolvers
            .type("Subscription") { builder ->
                builder
                    .dataFetcher("topicUpdates", subscriptionResolver.topicUpdates())
                    .dataFetcher("multiTopicUpdates", subscriptionResolver.multiTopicUpdates())
            }
            .build()
    }
}