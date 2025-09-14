package at.rocworks.stores.mongodb

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import org.bson.conversions.Bson
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import org.bson.Document
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

class MetricsStoreMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private val collectionName = "metrics"
    private lateinit var vertx: Vertx

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>

    fun start(vertx: Vertx, startPromise: Promise<Void>) {
        try {
            // Configure MongoDB client with optimized settings
            val clientSettings = MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(connectionString))
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(5000, TimeUnit.MILLISECONDS))
                .build()

            mongoClient = MongoClients.create(clientSettings)
            database = mongoClient.getDatabase(databaseName)
            collection = database.getCollection(collectionName)

            // Create indexes for efficient querying
            vertx.executeBlocking<Void>(Callable {
                try {
                    // Compound index for queries by type and identifier with timestamp
                    val typeIdentifierTimestampIndex = Indexes.compoundIndex(
                        Indexes.ascending("metric_type"),
                        Indexes.ascending("identifier"),
                        Indexes.descending("timestamp")
                    )

                    // Timestamp index for time-based queries
                    val timestampIndex = Indexes.ascending("timestamp")

                    collection.createIndexes(listOf(
                        IndexModel(typeIdentifierTimestampIndex),
                        IndexModel(timestampIndex)
                    ))

                    logger.info("MongoDB metrics store initialized successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes: ${e.message}")
                    throw e
                }
                null
            }).onComplete { result ->
                if (result.succeeded()) {
                    startPromise.complete()
                } else {
                    startPromise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to initialize MongoDB metrics store: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun startStore(vertx: Vertx): Future<Void> {
        this.vertx = vertx
        val promise = Promise.promise<Void>()
        start(vertx, promise)
        return promise.future()
    }

    override fun stopStore(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            if (::mongoClient.isInitialized) {
                mongoClient.close()
            }
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getName(): String = name

    override fun getType(): MetricsStoreType = MetricsStoreType.MONGODB

    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val metricsDoc = Document()
                    .append("messagesIn", metrics.messagesIn)
                    .append("messagesOut", metrics.messagesOut)
                    .append("nodeSessionCount", metrics.nodeSessionCount)
                    .append("clusterSessionCount", metrics.clusterSessionCount)
                    .append("queuedMessagesCount", metrics.queuedMessagesCount)
                    .append("topicIndexSize", metrics.topicIndexSize)
                    .append("clientNodeMappingSize", metrics.clientNodeMappingSize)
                    .append("topicNodeMappingSize", metrics.topicNodeMappingSize)
                    .append("messageBusIn", metrics.messageBusIn)
                    .append("messageBusOut", metrics.messageBusOut)

                val document = Document()
                    .append("timestamp", Date.from(timestamp))
                    .append("metric_type", "broker")
                    .append("identifier", nodeId)
                    .append("metrics", metricsDoc)

                // Use upsert to replace existing document with same timestamp, type, and identifier
                val filter = Filters.and(
                    Filters.eq("timestamp", Date.from(timestamp)),
                    Filters.eq("metric_type", "broker"),
                    Filters.eq("identifier", nodeId)
                )

                collection.replaceOne(
                    filter,
                    document,
                    ReplaceOptions().upsert(true)
                )
            } catch (e: Exception) {
                logger.warning("Error storing broker metrics: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val metricsDoc = Document()
                    .append("messagesIn", metrics.messagesIn)
                    .append("messagesOut", metrics.messagesOut)

                val document = Document()
                    .append("timestamp", Date.from(timestamp))
                    .append("metric_type", "session")
                    .append("identifier", clientId)
                    .append("metrics", metricsDoc)

                // Use upsert to replace existing document with same timestamp, type, and identifier
                val filter = Filters.and(
                    Filters.eq("timestamp", Date.from(timestamp)),
                    Filters.eq("metric_type", "session"),
                    Filters.eq("identifier", clientId)
                )

                collection.replaceOne(
                    filter,
                    document,
                    ReplaceOptions().upsert(true)
                )
            } catch (e: Exception) {
                logger.warning("Error storing session metrics for client $clientId: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun getBrokerMetrics(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<BrokerMetrics> {
        return vertx.executeBlocking<BrokerMetrics>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)

            if (fromTime == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", "broker"),
                Filters.eq("identifier", nodeId),
                Filters.gte("timestamp", Date.from(fromTime))
            ))

            if (toTime != null) {
                filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            }

            val filter = Filters.and(filterList)

            val document = collection
                .find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(1)
                .firstOrNull()

            if (document != null) {
                val metricsDoc = document.get("metrics", Document::class.java)
                BrokerMetrics(
                    messagesIn = metricsDoc.getLong("messagesIn") ?: 0L,
                    messagesOut = metricsDoc.getLong("messagesOut") ?: 0L,
                    nodeSessionCount = metricsDoc.getInteger("nodeSessionCount") ?: 0,
                    clusterSessionCount = metricsDoc.getInteger("clusterSessionCount") ?: 0,
                    queuedMessagesCount = metricsDoc.getLong("queuedMessagesCount") ?: 0L,
                    topicIndexSize = metricsDoc.getInteger("topicIndexSize") ?: 0,
                    clientNodeMappingSize = metricsDoc.getInteger("clientNodeMappingSize") ?: 0,
                    topicNodeMappingSize = metricsDoc.getInteger("topicNodeMappingSize") ?: 0,
                    messageBusIn = metricsDoc.getLong("messageBusIn") ?: 0L,
                    messageBusOut = metricsDoc.getLong("messageBusOut") ?: 0L
                )
            } else {
                // No historical data found, return zero metrics
                BrokerMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            }
        })
    }

    override fun getSessionMetrics(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<SessionMetrics> {
        return vertx.executeBlocking<SessionMetrics>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)

            if (fromTime == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", "session"),
                Filters.eq("identifier", clientId),
                Filters.gte("timestamp", Date.from(fromTime))
            ))

            if (toTime != null) {
                filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            }

            val filter = Filters.and(filterList)

            val document = collection
                .find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(1)
                .firstOrNull()

            if (document != null) {
                val metricsDoc = document.get("metrics", Document::class.java)
                SessionMetrics(
                    messagesIn = metricsDoc.getLong("messagesIn") ?: 0L,
                    messagesOut = metricsDoc.getLong("messagesOut") ?: 0L
                )
            } else {
                // No historical data found, return zero metrics
                SessionMetrics(0, 0)
            }
        })
    }

    override fun getBrokerMetricsHistory(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, BrokerMetrics>>> {
        return vertx.executeBlocking<List<Pair<Instant, BrokerMetrics>>>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)

            if (fromTime == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", "broker"),
                Filters.eq("identifier", nodeId),
                Filters.gte("timestamp", Date.from(fromTime))
            ))

            if (toTime != null) {
                filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            }

            val filter = Filters.and(filterList)

            val documents = collection
                .find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(limit)
                .toList()

            documents.map { document ->
                val timestamp = document.getDate("timestamp").toInstant()
                val metricsDoc = document.get("metrics", Document::class.java)
                val metrics = BrokerMetrics(
                    messagesIn = metricsDoc.getLong("messagesIn") ?: 0L,
                    messagesOut = metricsDoc.getLong("messagesOut") ?: 0L,
                    nodeSessionCount = metricsDoc.getInteger("nodeSessionCount") ?: 0,
                    clusterSessionCount = metricsDoc.getInteger("clusterSessionCount") ?: 0,
                    queuedMessagesCount = metricsDoc.getLong("queuedMessagesCount") ?: 0L,
                    topicIndexSize = metricsDoc.getInteger("topicIndexSize") ?: 0,
                    clientNodeMappingSize = metricsDoc.getInteger("clientNodeMappingSize") ?: 0,
                    topicNodeMappingSize = metricsDoc.getInteger("topicNodeMappingSize") ?: 0,
                    messageBusIn = metricsDoc.getLong("messageBusIn") ?: 0L,
                    messageBusOut = metricsDoc.getLong("messageBusOut") ?: 0L
                )
                timestamp to metrics
            }
        })
    }

    override fun getSessionMetricsHistory(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, SessionMetrics>>> {
        return vertx.executeBlocking<List<Pair<Instant, SessionMetrics>>>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)

            if (fromTime == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", "session"),
                Filters.eq("identifier", clientId),
                Filters.gte("timestamp", Date.from(fromTime))
            ))

            if (toTime != null) {
                filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            }

            val filter = Filters.and(filterList)

            val documents = collection
                .find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(limit)
                .toList()

            documents.map { document ->
                val timestamp = document.getDate("timestamp").toInstant()
                val metricsDoc = document.get("metrics", Document::class.java)
                val metrics = SessionMetrics(
                    messagesIn = metricsDoc.getLong("messagesIn") ?: 0L,
                    messagesOut = metricsDoc.getLong("messagesOut") ?: 0L
                )
                timestamp to metrics
            }
        })
    }

    override fun purgeOldMetrics(olderThan: Instant): Future<Long> {
        return vertx.executeBlocking<Long>(Callable {
            try {
                val filter = Filters.lt("timestamp", Date.from(olderThan))
                val result = collection.deleteMany(filter)
                result.deletedCount
            } catch (e: Exception) {
                logger.warning("Error purging old metrics: ${e.message}")
                throw e
            }
        })
    }

    private fun calculateTimeRange(from: Instant?, to: Instant?, lastMinutes: Int?): Pair<Instant?, Instant?> {
        return when {
            lastMinutes != null -> {
                val toTime = Instant.now()
                val fromTime = toTime.minus(lastMinutes.toLong(), ChronoUnit.MINUTES)
                Pair(fromTime, toTime)
            }
            from != null -> {
                Pair(from, to)
            }
            else -> Pair(null, null)
        }
    }
}