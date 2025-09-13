package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.MqttClient
import at.rocworks.Utils
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.logging.Logger

class MetricsHandler : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(MetricsHandler::class.java.name)

        // EventBus address for metrics requests
        const val METRICS_ADDRESS_PREFIX = "monstermq.node.metrics"

        fun getMetricsAddress(nodeId: String): String {
            return "$METRICS_ADDRESS_PREFIX.$nodeId"
        }
    }

    override fun start(startPromise: Promise<Void>) {
        val nodeId = Monster.getClusterNodeId(vertx)
        val metricsAddress = getMetricsAddress(nodeId)

        logger.info("Starting MetricsHandler for node $nodeId on address $metricsAddress")

        // Register handler for metrics requests for this node
        vertx.eventBus().consumer<JsonObject>(metricsAddress) { message ->
            try {
                val metrics = collectNodeMetrics()
                message.reply(metrics)
            } catch (e: Exception) {
                logger.severe("Error collecting metrics: ${e.message}")
                message.fail(500, e.message)
            }
        }

        startPromise.complete()
    }

    private fun collectNodeMetrics(): JsonObject {
        val sessionRegistry = MqttClient.getSessionRegistry()

        var messagesIn = 0L
        var messagesOut = 0L
        var nodeSessionCount = 0

        // Aggregate metrics from all sessions on this node
        sessionRegistry.values.forEach { session ->
            messagesIn += session.messagesIn.get()
            messagesOut += session.messagesOut.get()
            nodeSessionCount++
        }

        return JsonObject()
            .put("messagesIn", messagesIn)
            .put("messagesOut", messagesOut)
            .put("nodeSessionCount", nodeSessionCount)
            .put("nodeId", Monster.getClusterNodeId(vertx))
    }
}