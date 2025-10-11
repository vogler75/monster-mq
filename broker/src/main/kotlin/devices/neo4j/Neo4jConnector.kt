package at.rocworks.devices.neo4j

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.Neo4jClientConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * Neo4j Connector for writing MQTT topic trees to Neo4j graph database
 */
class Neo4jConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: Neo4jClientConfig

    private var driver: Driver? = null
    private var session: Session? = null
    private var namespaceRootId: Value = Values.NULL

    // Path writer thread for hierarchical structure creation
    private val writePathStop = AtomicBoolean(false)
    private val writePathIds = mutableSetOf<Value>()
    private val writePathQueue: LinkedBlockingQueue<Pair<Value, BrokerMessage>>

    // Metrics
    private val messagesIn = AtomicLong(0)
    private val messagesWritten = AtomicLong(0)
    private val errors = AtomicLong(0)
    private var lastResetTime = System.currentTimeMillis()

    // Batch accumulation
    private val messageBatch = mutableListOf<BrokerMessage>()
    private var lastBatchWrite = System.currentTimeMillis()

    init {
        writePathQueue = LinkedBlockingQueue(10000) // Will be updated from config
    }

    private val mqttWriteValuesQuery = """
        UNWIND ${"$"}records AS record
        MERGE (n:MqttValue {
          System : record.System,
          NodeId : record.NodeId
        })
        SET n += {
          Name: record.Name,
          Value : record.Value,
          ServerTime : record.ServerTime,
          Topic : record.Topic
        }
        RETURN ID(n)
    """.trimIndent()

    override fun start(startPromise: Promise<Void>) {
        try {
            val cfgJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(cfgJson)
            cfg = Neo4jClientConfig.fromJson(device.config)

            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                throw IllegalArgumentException("Neo4j config errors: ${validationErrors.joinToString(", ")}")
            }

            logger.info("Starting Neo4jConnector for device ${device.name} url=${cfg.url}")

            // Initialize queue size from config
            while (writePathQueue.size > 0) writePathQueue.clear()

            // Connect to Neo4j
            connectToNeo4j()
                .onSuccess {
                    // Subscribe to MQTT topics
                    subscribeToTopics()

                    // Start periodic batch writer
                    startBatchWriter()

                    // Setup metrics endpoint
                    setupMetricsEndpoint()

                    logger.info("Neo4jConnector started successfully for ${device.name}")
                    startPromise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to connect to Neo4j: ${error.message}")
                    startPromise.fail(error)
                }

        } catch (e: Exception) {
            logger.severe("Failed to start Neo4jConnector: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("Stopping Neo4jConnector for device: ${device.name}")

            // Stop path writer thread
            writePathStop.set(true)

            // Flush any remaining batch
            flushBatch()

            // Close Neo4j session and driver
            session?.close()
            session = null
            driver?.close()
            driver = null

            // Unsubscribe from MQTT topics
            unsubscribeFromTopics()

            logger.info("Neo4jConnector stopped for ${device.name}")
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping Neo4jConnector: ${e.message}")
            stopPromise.complete()
        }
    }

    private fun connectToNeo4j(): io.vertx.core.Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            logger.info("Connecting to Neo4j at ${cfg.url}")
            driver = GraphDatabase.driver(cfg.url, AuthTokens.basic(cfg.username, cfg.password))
            session = driver!!.session()

            // Create indexes
            createSchema()

            // Start path writer thread
            thread(start = true, block = ::writeMqttNodesThread)

            promise.complete()
        } catch (e: Exception) {
            logger.severe("Error connecting to Neo4j: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun createSchema() {
        try {
            logger.info("Creating Neo4j schema...")

            // Create indexes in write transactions
            session?.executeWrite { tx ->
                tx.run("""
                    CREATE INDEX IF NOT EXISTS
                    FOR (n:MqttNode)
                    ON (n.System, n.Path)
                """.trimIndent())
                logger.fine("MqttNode index created")
            }

            session?.executeWrite { tx ->
                tx.run("""
                    CREATE INDEX IF NOT EXISTS
                    FOR (n:MqttValue)
                    ON (n.System, n.NodeId)
                """.trimIndent())
                logger.fine("MqttValue index created")
            }

            // Create namespace root node
            createNamespaceRootNode()

            logger.info("Neo4j schema created successfully")
        } catch (e: Exception) {
            logger.severe("Error creating Neo4j schema: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun createNamespaceRootNode() {
        try {
            logger.info("Creating namespace root node for: ${device.namespace}")

            // Execute in a write transaction to ensure it's committed
            session?.executeWrite { tx ->
                val result = tx.run(
                    """
                    MERGE (n:MqttNode { System: ${'$'}System, Path: ${'$'}Path, Name: ${'$'}Name, IsNamespaceRoot: true })
                    RETURN ID(n)
                    """.trimIndent(),
                    Values.parameters(
                        "System", device.namespace,
                        "Path", device.namespace,
                        "Name", device.namespace
                    )
                )

                namespaceRootId = result.single()[0]

                logger.info("Namespace root node created/found with ID: ${namespaceRootId.asLong()} for namespace: ${device.namespace}")
            }

            if (namespaceRootId.isNull) {
                logger.warning("Failed to create namespace root node")
            }
        } catch (e: Exception) {
            logger.severe("Error creating namespace root node: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun subscribeToTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("SessionHandler not available, cannot subscribe to topics")
            return
        }

        cfg.topicFilters.forEach { topicFilter ->
            logger.info("Subscribing to MQTT topic filter: $topicFilter")
            sessionHandler.subscribeInternal(
                clientId = "neo4j-${device.name}",
                topicFilter = topicFilter,
                qos = 0
            ) { message ->
                handleIncomingMessage(message)
            }
        }
    }

    private fun unsubscribeFromTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            return
        }

        cfg.topicFilters.forEach { topicFilter ->
            logger.info("Unsubscribing from MQTT topic filter: $topicFilter")
            sessionHandler.unsubscribeInternal("neo4j-${device.name}", topicFilter)
        }
    }

    private fun handleIncomingMessage(message: BrokerMessage) {
        try {
            logger.finest("Received message: ${message.topicName}")
            messagesIn.incrementAndGet()

            synchronized(messageBatch) {
                messageBatch.add(message)

                // Flush if batch is full
                if (messageBatch.size >= cfg.batchSize) {
                    flushBatch()
                }
            }
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.warning("Error handling incoming message: ${e.message}")
        }
    }

    private fun startBatchWriter() {
        // Periodic batch flush (every 1 second)
        vertx.setPeriodic(1000) {
            synchronized(messageBatch) {
                if (messageBatch.isNotEmpty() && System.currentTimeMillis() - lastBatchWrite > 1000) {
                    flushBatch()
                }
            }
        }
    }

    private fun flushBatch() {
        synchronized(messageBatch) {
            if (messageBatch.isEmpty()) {
                return
            }

            val batch = messageBatch.toList()
            messageBatch.clear()
            lastBatchWrite = System.currentTimeMillis()

            // Write to Neo4j in a blocking operation
            vertx.executeBlocking<Void> {
                try {
                    writeMqttValues(batch)
                    null
                } catch (e: Exception) {
                    logger.warning("Error writing batch to Neo4j: ${e.message}")
                    errors.incrementAndGet()
                    throw e
                }
            }
        }
    }

    private fun writeMqttValues(messages: List<BrokerMessage>) {
        val records = messages.map { message ->
            val name = message.topicName.substringAfterLast('/')
            val payloadStr = try {
                String(message.payload, Charsets.UTF_8)
            } catch (e: Exception) {
                message.payload.toString()
            }

            mapOf(
                "Name" to name,
                "System" to device.namespace,
                "NodeId" to message.topicName,
                "Value" to payloadStr,
                "ServerTime" to message.time.toString(),
                "Topic" to message.topicName
            )
        }

        session?.executeWrite { tx ->
            val result = tx.run(mqttWriteValuesQuery, Values.parameters("records", records))
            val results = result.list()
            val isNewValue = result.consume().counters().nodesCreated() > 0

            if (isNewValue) {
                results.zip(messages).forEach { (resultRecord, message) ->
                    val id = resultRecord[0]
                    synchronized(writePathIds) {
                        if (!writePathIds.contains(id)) {
                            writePathIds.add(id)
                            writePathQueue.offer(Pair(id, message))
                        }
                    }
                }
            }

            messagesWritten.addAndGet(messages.size.toLong())
        }
    }

    private fun writeMqttNodesThread() {
        logger.info("Path writer thread started with queue size [${writePathQueue.remainingCapacity()}]")
        val pathSession = driver?.session()
        writePathStop.set(false)

        while (!writePathStop.get()) {
            try {
                val data = writePathQueue.poll(10, TimeUnit.MILLISECONDS)
                data?.let { (value, message) ->
                    pathSession?.executeWrite { tx ->
                        synchronized(writePathIds) {
                            writePathIds.remove(value)
                        }
                        writeMqttValuePath(tx, value, message)
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error in path writer thread: ${e.message}")
                errors.incrementAndGet()
            }
        }

        pathSession?.close()
        logger.info("Path writer thread stopped")
    }

    private fun writeMqttValuePath(
        tx: TransactionContext,
        mqttValueId: Value,
        message: BrokerMessage
    ) {
        val topicParts = message.topicName.split("/").filter { it.isNotEmpty() }
        if (topicParts.isEmpty()) return

        val connectQuery =
            "MATCH (n1) WHERE ID(n1) = \$parentId \n" +
            "MATCH (n2) WHERE ID(n2) = \$folderId \n" +
            "MERGE (n1)-[:HAS]->(n2)"

        // Start with namespace root as parent
        var parentId: Value = namespaceRootId
        var currentPath = ""

        // Create nodes for each path component
        topicParts.dropLast(1).forEach { name ->
            currentPath = if (currentPath.isEmpty()) name else "$currentPath/$name"

            val folderId = tx.run(
                "MERGE (n:MqttNode { System: \$System, Path: \$Path, Name: \$Name }) RETURN ID(n)",
                Values.parameters("System", device.namespace, "Path", currentPath, "Name", name)
            ).single()[0]

            // Connect to parent (namespace root or previous node)
            if (!parentId.isNull) {
                tx.run(connectQuery, Values.parameters("parentId", parentId, "folderId", folderId))
            }

            parentId = folderId
        }

        // Connect the last path node to the value node
        if (!parentId.isNull) {
            tx.run(connectQuery, Values.parameters("parentId", parentId, "folderId", mqttValueId))
        }
    }

    private fun setupMetricsEndpoint() {
        val address = EventBusAddresses.Neo4jBridge.connectorMetrics(device.name)
        vertx.eventBus().consumer<JsonObject>(address) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedSec = (now - lastResetTime) / 1000.0
                val inRate = if (elapsedSec > 0) messagesIn.get() / elapsedSec else 0.0
                val writeRate = if (elapsedSec > 0) messagesWritten.get() / elapsedSec else 0.0

                val response = JsonObject()
                    .put("device", device.name)
                    .put("messagesIn", messagesIn.getAndSet(0))
                    .put("messagesWritten", messagesWritten.getAndSet(0))
                    .put("errors", errors.get())
                    .put("pathQueueSize", writePathQueue.size)
                    .put("messagesInRate", inRate)
                    .put("messagesWrittenRate", writeRate)

                msg.reply(response)
                lastResetTime = now
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }
}
