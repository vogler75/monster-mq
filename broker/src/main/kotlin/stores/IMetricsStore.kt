package at.rocworks.stores

import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import io.vertx.core.Future
import java.time.Instant

enum class MetricsStoreType {
    POSTGRES,
    CRATEDB,
    MONGODB,
    SQLITE
}

enum class MetricKind { BROKER, SESSION }

interface IMetricsStore {
    fun getName(): String
    fun getType(): MetricsStoreType

    fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void>
    fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void>

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

    fun purgeOldMetrics(olderThan: Instant): Future<Long>
}

interface IMetricsStoreAsync : IMetricsStore {
    fun startStore(vertx: io.vertx.core.Vertx): Future<Void>
    fun stopStore(): Future<Void>
}