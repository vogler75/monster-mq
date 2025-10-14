package at.rocworks.stores

import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import io.vertx.core.Future
import java.time.Instant

enum class MetricsStoreType {
    POSTGRES,
    CRATEDB,
    MONGODB,
    SQLITE
}

enum class MetricKind {
    BROKER, SESSION, MQTTCLIENT, KAFKACLIENT, WINCCOACLIENT, WINCCUACLIENT, OPCUADEVICE, ARCHIVEGROUP, NEO4JCLIENT, JDBCLOGGER;

    fun toDbString(): String = when (this) {
        BROKER -> "broker"
        SESSION -> "session"
        MQTTCLIENT -> "mqtt"
        KAFKACLIENT -> "kafka"
        WINCCOACLIENT -> "winccoa"
        WINCCUACLIENT -> "winccua"
        OPCUADEVICE -> "opcua"
        ARCHIVEGROUP -> "archive"
        NEO4JCLIENT -> "neo4j"
        JDBCLOGGER -> "jdbclogger"
    }
}

interface IMetricsStore {
    fun getName(): String
    fun getType(): MetricsStoreType

    fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void>
    fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void>
    fun storeMqttClientMetrics(timestamp: Instant, clientName: String, metrics: MqttClientMetrics): Future<Void>
    fun storeWinCCOaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.WinCCOaClientMetrics): Future<Void>

    // Generic JSON based API (metric_type + identifier + metrics JSON)
    fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: io.vertx.core.json.JsonObject): Future<Void>
    fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<io.vertx.core.json.JsonObject>
    fun getMetricsHistory(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int = 100): Future<List<Pair<Instant, io.vertx.core.json.JsonObject>>>

    fun getBrokerMetrics(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<BrokerMetrics>

    fun getBrokerMetricsList(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<BrokerMetrics>>

    fun getSessionMetrics(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<SessionMetrics>

    fun getSessionMetricsList(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<SessionMetrics>>

    fun getMqttClientMetrics(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<MqttClientMetrics>

    fun getMqttClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<MqttClientMetrics>>

    fun getBrokerMetricsHistory(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int = 100
    ): Future<List<Pair<Instant, BrokerMetrics>>>

    fun getSessionMetricsHistory(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int = 100
    ): Future<List<Pair<Instant, SessionMetrics>>>

    fun getMqttClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int = 100
    ): Future<List<Pair<Instant, MqttClientMetrics>>>

    // Kafka Client Metrics
    fun storeKafkaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.KafkaClientMetrics): Future<Void>
    fun getKafkaClientMetrics(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<at.rocworks.extensions.graphql.KafkaClientMetrics>
    fun getKafkaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.KafkaClientMetrics>>
    fun getKafkaClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int = 100
    ): Future<List<Pair<Instant, at.rocworks.extensions.graphql.KafkaClientMetrics>>>

    // WinCC OA Client Metrics
    fun getWinCCOaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.WinCCOaClientMetrics>>

    // WinCC Unified (UA) Client Metrics
    fun storeWinCCUaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.WinCCUaClientMetrics): Future<Void>
    fun getWinCCUaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.WinCCUaClientMetrics>>

    // OPC UA Device Metrics
    fun getOpcUaDeviceMetrics(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<at.rocworks.extensions.graphql.OpcUaDeviceMetrics>

    fun getOpcUaDeviceMetricsList(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.OpcUaDeviceMetrics>>

    fun getOpcUaDeviceMetricsHistory(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int = 100
    ): Future<List<Pair<Instant, at.rocworks.extensions.graphql.OpcUaDeviceMetrics>>>

    fun purgeOldMetrics(olderThan: Instant): Future<Long>
}

interface IMetricsStoreAsync : IMetricsStore {
    fun startStore(vertx: io.vertx.core.Vertx): Future<Void>
    fun stopStore(): Future<Void>
}
