package at.rocworks.stores.memory

import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.KafkaClientMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import at.rocworks.extensions.graphql.OpcUaDeviceMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.TimestampConverter
import at.rocworks.extensions.graphql.WinCCOaClientMetrics
import at.rocworks.extensions.graphql.WinCCUaClientMetrics
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricKind
import at.rocworks.stores.MetricsStoreType
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.temporal.ChronoUnit

open class MetricsStoreMemory(
    private val name: String,
    private val maxHistoryRows: Int = 3600,
    private val storeType: MetricsStoreType = MetricsStoreType.MEMORY,
    private val storeHistory: Boolean = true
) : IMetricsStoreAsync {
    private data class Key(val kind: MetricKind, val identifier: String)
    private data class Sample(val timestamp: Instant, val metrics: JsonObject)

    private val samples = mutableMapOf<Key, MutableList<Sample>>()
    private val maxRows = maxHistoryRows.coerceAtLeast(1)

    override fun startStore(vertx: Vertx): Future<Void> = Future.succeededFuture()

    override fun stopStore(): Future<Void> = Future.succeededFuture()

    override fun getName(): String = name

    override fun getType(): MetricsStoreType = storeType

    override fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: JsonObject): Future<Void> {
        if (!storeHistory) return Future.succeededFuture()

        synchronized(samples) {
            val key = Key(kind, identifier)
            val list = samples.getOrPut(key) { mutableListOf() }
            val existingIndex = list.indexOfFirst { it.timestamp == timestamp }
            val sample = Sample(timestamp, metricsJson.copy())
            if (existingIndex >= 0) {
                list[existingIndex] = sample
            } else {
                list.add(sample)
            }
            list.sortByDescending { it.timestamp }
            if (list.size > maxRows) {
                list.subList(maxRows, list.size).clear()
            }
        }

        return Future.succeededFuture()
    }

    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void> =
        storeMetrics(MetricKind.BROKER, timestamp, nodeId, brokerMetricsToJson(metrics))

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void> =
        storeMetrics(MetricKind.SESSION, timestamp, clientId, sessionMetricsToJson(metrics))

    override fun storeMqttClientMetrics(timestamp: Instant, clientName: String, metrics: MqttClientMetrics): Future<Void> =
        storeMetrics(MetricKind.MQTTCLIENT, timestamp, clientName, mqttClientMetricsToJson(metrics))

    override fun storeWinCCOaClientMetrics(timestamp: Instant, clientName: String, metrics: WinCCOaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.WINCCOACLIENT, timestamp, clientName, winCCOaClientMetricsToJson(metrics))

    override fun storeKafkaClientMetrics(timestamp: Instant, clientName: String, metrics: KafkaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.KAFKACLIENT, timestamp, clientName, kafkaClientMetricsToJson(metrics))

    override fun storeWinCCUaClientMetrics(timestamp: Instant, clientName: String, metrics: WinCCUaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.WINCCUACLIENT, timestamp, clientName, winCCUaClientMetricsToJson(metrics))

    override fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<JsonObject> {
        val history = getHistory(kind, identifier, from, to, lastMinutes, 1)
        return Future.succeededFuture(history.firstOrNull()?.second?.copy() ?: JsonObject())
    }

    override fun getMetricsHistory(
        kind: MetricKind,
        identifier: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, JsonObject>>> =
        Future.succeededFuture(getHistory(kind, identifier, from, to, lastMinutes, limit).map { it.first to it.second.copy() })

    override fun getBrokerMetrics(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<BrokerMetrics> =
        getLatestMetrics(MetricKind.BROKER, nodeId, from, to, lastMinutes).map { jsonToBrokerMetrics(it) }

    override fun getSessionMetrics(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<SessionMetrics> =
        getLatestMetrics(MetricKind.SESSION, clientId, from, to, lastMinutes).map { jsonToSessionMetrics(it) }

    override fun getMqttClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<MqttClientMetrics> =
        getLatestMetrics(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes).map { jsonToMqttClientMetrics(it) }

    override fun getKafkaClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<KafkaClientMetrics> =
        getLatestMetrics(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes).map { jsonToKafkaClientMetrics(it) }

    override fun getOpcUaDeviceMetrics(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<OpcUaDeviceMetrics> =
        getLatestMetrics(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes).map { jsonToOpcUaDeviceMetrics(it) }

    override fun getBrokerMetricsList(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<BrokerMetrics>> =
        getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getSessionMetricsList(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<SessionMetrics>> =
        getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getMqttClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<MqttClientMetrics>> =
        getMqttClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getKafkaClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<KafkaClientMetrics>> =
        getKafkaClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getOpcUaDeviceMetricsList(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<OpcUaDeviceMetrics>> =
        getOpcUaDeviceMetricsHistory(deviceName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getWinCCOaClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<WinCCOaClientMetrics>> =
        getMetricsHistory(MetricKind.WINCCOACLIENT, clientName, from, to, lastMinutes, Int.MAX_VALUE)
            .map { list -> list.map { jsonToWinCCOaClientMetrics(it.second) } }

    override fun getWinCCUaClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<WinCCUaClientMetrics>> =
        getMetricsHistory(MetricKind.WINCCUACLIENT, clientName, from, to, lastMinutes, Int.MAX_VALUE)
            .map { list -> list.map { jsonToWinCCUaClientMetrics(it.second) } }

    override fun getBrokerMetricsHistory(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, BrokerMetrics>>> =
        getMetricsHistory(MetricKind.BROKER, nodeId, from, to, lastMinutes, limit).map { list ->
            list.map { it.first to jsonToBrokerMetrics(it.second) }
        }

    override fun getSessionMetricsHistory(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, SessionMetrics>>> =
        getMetricsHistory(MetricKind.SESSION, clientId, from, to, lastMinutes, limit).map { list ->
            list.map { it.first to jsonToSessionMetrics(it.second) }
        }

    override fun getMqttClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, MqttClientMetrics>>> =
        getMetricsHistory(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes, limit).map { list ->
            list.map { it.first to jsonToMqttClientMetrics(it.second) }
        }

    override fun getKafkaClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, KafkaClientMetrics>>> =
        getMetricsHistory(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes, limit).map { list ->
            list.map { it.first to jsonToKafkaClientMetrics(it.second) }
        }

    override fun getOpcUaDeviceMetricsHistory(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, OpcUaDeviceMetrics>>> =
        getMetricsHistory(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes, limit).map { list ->
            list.map { it.first to jsonToOpcUaDeviceMetrics(it.second) }
        }

    override fun purgeOldMetrics(olderThan: Instant): Future<Long> {
        if (!storeHistory) return Future.succeededFuture(0L)

        val removed = synchronized(samples) {
            var count = 0L
            samples.values.forEach { list ->
                val before = list.size
                list.removeAll { it.timestamp < olderThan }
                count += (before - list.size).toLong()
            }
            samples.entries.removeIf { it.value.isEmpty() }
            count
        }

        return Future.succeededFuture(removed)
    }

    private fun getHistory(
        kind: MetricKind,
        identifier: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): List<Pair<Instant, JsonObject>> {
        if (!storeHistory) return emptyList()

        val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)
        val effectiveLimit = limit.coerceAtLeast(0)
        if (effectiveLimit == 0) return emptyList()

        return synchronized(samples) {
            samples[Key(kind, identifier)].orEmpty()
                .asSequence()
                .filter { fromTimestamp == null || !it.timestamp.isBefore(fromTimestamp) }
                .filter { toTimestamp == null || !it.timestamp.isAfter(toTimestamp) }
                .take(effectiveLimit)
                .map { it.timestamp to it.metrics }
                .toList()
        }
    }

    private fun calculateTimeRange(from: Instant?, to: Instant?, lastMinutes: Int?): Pair<Instant?, Instant?> = when {
        lastMinutes != null -> {
            val toTime = Instant.now()
            val fromTime = toTime.minus(lastMinutes.toLong(), ChronoUnit.MINUTES)
            fromTime to toTime
        }
        from != null -> from to to
        else -> null to null
    }

    private fun brokerMetricsToJson(m: BrokerMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("nodeSessionCount", m.nodeSessionCount)
        .put("clusterSessionCount", m.clusterSessionCount)
        .put("queuedMessagesCount", m.queuedMessagesCount)
        .put("subscriptionCount", m.subscriptionCount)
        .put("clientNodeMappingSize", m.clientNodeMappingSize)
        .put("topicNodeMappingSize", m.topicNodeMappingSize)
        .put("messageBusIn", m.messageBusIn)
        .put("messageBusOut", m.messageBusOut)
        .put("mqttClientIn", m.mqttClientIn)
        .put("mqttClientOut", m.mqttClientOut)
        .put("opcUaClientIn", m.opcUaClientIn)
        .put("opcUaClientOut", m.opcUaClientOut)
        .put("kafkaClientIn", m.kafkaClientIn)
        .put("kafkaClientOut", m.kafkaClientOut)
        .put("winCCOaClientIn", m.winCCOaClientIn)
        .put("winCCUaClientIn", m.winCCUaClientIn)
        .put("natsClientIn", m.natsClientIn)
        .put("natsClientOut", m.natsClientOut)
        .put("redisClientIn", m.redisClientIn)
        .put("redisClientOut", m.redisClientOut)
        .put("neo4jClientIn", m.neo4jClientIn)
        .put("timestamp", m.timestamp)

    private fun sessionMetricsToJson(m: SessionMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun mqttClientMetricsToJson(m: MqttClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun kafkaClientMetricsToJson(m: KafkaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun winCCOaClientMetricsToJson(m: WinCCOaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("connected", m.connected)
        .put("timestamp", m.timestamp)

    private fun winCCUaClientMetricsToJson(m: WinCCUaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("connected", m.connected)
        .put("timestamp", m.timestamp)

    private fun jsonToBrokerMetrics(j: JsonObject) = if (j.isEmpty) BrokerMetrics(
        messagesIn = 0.0,
        messagesOut = 0.0,
        nodeSessionCount = 0,
        clusterSessionCount = 0,
        queuedMessagesCount = 0,
        subscriptionCount = 0,
        clientNodeMappingSize = 0,
        topicNodeMappingSize = 0,
        messageBusIn = 0.0,
        messageBusOut = 0.0,
        mqttClientIn = 0.0,
        mqttClientOut = 0.0,
        opcUaClientIn = 0.0,
        opcUaClientOut = 0.0,
        kafkaClientIn = 0.0,
        kafkaClientOut = 0.0,
        winCCOaClientIn = 0.0,
        winCCUaClientIn = 0.0,
        natsClientIn = 0.0,
        natsClientOut = 0.0,
        redisClientIn = 0.0,
        redisClientOut = 0.0,
        neo4jClientIn = 0.0,
        timestamp = TimestampConverter.currentTimeIsoString()
    ) else BrokerMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        nodeSessionCount = j.getInteger("nodeSessionCount", 0),
        clusterSessionCount = j.getInteger("clusterSessionCount", 0),
        queuedMessagesCount = j.getLong("queuedMessagesCount", 0),
        subscriptionCount = j.getInteger("subscriptionCount", 0),
        clientNodeMappingSize = j.getInteger("clientNodeMappingSize", 0),
        topicNodeMappingSize = j.getInteger("topicNodeMappingSize", 0),
        messageBusIn = j.getDouble("messageBusIn", 0.0),
        messageBusOut = j.getDouble("messageBusOut", 0.0),
        mqttClientIn = j.getDouble("mqttClientIn", 0.0),
        mqttClientOut = j.getDouble("mqttClientOut", 0.0),
        opcUaClientIn = j.getDouble("opcUaClientIn", 0.0),
        opcUaClientOut = j.getDouble("opcUaClientOut", 0.0),
        kafkaClientIn = j.getDouble("kafkaClientIn", 0.0),
        kafkaClientOut = j.getDouble("kafkaClientOut", 0.0),
        winCCOaClientIn = j.getDouble("winCCOaClientIn", 0.0),
        winCCUaClientIn = j.getDouble("winCCUaClientIn", 0.0),
        natsClientIn = j.getDouble("natsClientIn", 0.0),
        natsClientOut = j.getDouble("natsClientOut", 0.0),
        redisClientIn = j.getDouble("redisClientIn", 0.0),
        redisClientOut = j.getDouble("redisClientOut", 0.0),
        neo4jClientIn = j.getDouble("neo4jClientIn", 0.0),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToSessionMetrics(j: JsonObject) = if (j.isEmpty) SessionMetrics(
        0.0,
        0.0,
        TimestampConverter.currentTimeIsoString()
    ) else SessionMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToMqttClientMetrics(j: JsonObject) = if (j.isEmpty) MqttClientMetrics(
        0.0,
        0.0,
        TimestampConverter.currentTimeIsoString()
    ) else MqttClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToKafkaClientMetrics(j: JsonObject) = if (j.isEmpty) KafkaClientMetrics(
        0.0,
        0.0,
        TimestampConverter.currentTimeIsoString()
    ) else KafkaClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToWinCCOaClientMetrics(j: JsonObject) = if (j.isEmpty) WinCCOaClientMetrics(
        0.0,
        false,
        TimestampConverter.currentTimeIsoString()
    ) else WinCCOaClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        connected = j.getBoolean("connected", false),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToWinCCUaClientMetrics(j: JsonObject) = if (j.isEmpty) WinCCUaClientMetrics(
        0.0,
        false,
        TimestampConverter.currentTimeIsoString()
    ) else WinCCUaClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        connected = j.getBoolean("connected", false),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToOpcUaDeviceMetrics(j: JsonObject) = if (j.isEmpty) OpcUaDeviceMetrics(
        0.0,
        0.0,
        TimestampConverter.currentTimeIsoString()
    ) else OpcUaDeviceMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
    )
}

class MetricsStoreNone(name: String) : MetricsStoreMemory(
    name = name,
    maxHistoryRows = 1,
    storeType = MetricsStoreType.NONE,
    storeHistory = false
)
