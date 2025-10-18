package at.rocworks.logging

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import io.vertx.core.json.JsonObject
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger

/**
 * MQTT Log Handler - Publishes Java log records to the event bus
 *
 * This handler intercepts all Java logging messages and publishes them to the event bus
 * where the SyslogVerticle processes them for MQTT publishing and in-memory storage.
 *
 * Features:
 * - Publishes logs from all loggers in the system
 * - Organizes logs by cluster node ID and log level
 * - JSON format with timestamp, logger name, message, and exception details
 * - Configurable minimum log level
 * - Automatic node identification
 */
class MqttLogHandler : Handler() {

    private val logger: Logger = Utils.getLogger(MqttLogHandler::class.java)
    private var nodeId: String = "unknown"
    private var initialized = false

    companion object {
        const val LOG_TOPIC_PREFIX = "${Const.SYS_TOPIC_NAME}/${Const.LOG_TOPIC_NAME}"
        private var instance: MqttLogHandler? = null

        /**
         * Initialize and install the MQTT log handler
         */
        fun install(): MqttLogHandler {
            if (instance == null) {
                instance = MqttLogHandler()

                // Add to root logger to capture all logging
                val rootLogger = Logger.getLogger("")
                rootLogger.addHandler(instance!!)

                // Set level to capture all logs (individual loggers control their own levels)
                instance!!.level = Level.ALL
            }
            return instance!!
        }

        /**
         * Remove the MQTT log handler
         */
        fun uninstall() {
            instance?.let { handler ->
                val rootLogger = Logger.getLogger("")
                rootLogger.removeHandler(handler)
                handler.close()
                instance = null
            }
        }
    }

    init {
        // Initialize with a permissive formatter that we'll override
        formatter = java.util.logging.SimpleFormatter()
    }

    override fun publish(record: LogRecord) {
        try {
            // Skip our own log messages to prevent infinite loops
            if (record.loggerName == MqttLogHandler::class.java.name) {
                return
            }
            
            // Check if this record's level meets the handler's threshold
            // This is normally done by the framework, but we make it explicit for clarity
            if (record.level.intValue() < this.level.intValue()) {
                return
            }
            
            // Initialize node ID if not done yet
            if (!initialized) {
                initializeNodeId()
            }

            // Get Vertx instance for event bus publishing
            val vertx =
                Monster.getVertx() ?: // Vertx not available yet, skip silently (no error logging to prevent loops)
                return

            // Create MQTT topic based on node and log level
            val levelName = record.level.name.lowercase()
            val topic = "$LOG_TOPIC_PREFIX/$nodeId/$levelName"

            // Create JSON payload with log information
            val logData = JsonObject().apply {
                put("timestamp", record.instant.toString())
                put("level", record.level.name)
                put("logger", record.loggerName)
                put("message", record.message)
                put("thread", record.longThreadID)
                put("node", nodeId)
                
                // Add source location if available
                if (record.sourceClassName != null) {
                    put("sourceClass", record.sourceClassName)
                }
                if (record.sourceMethodName != null) {
                    put("sourceMethod", record.sourceMethodName)
                }
                
                // Add exception details if present
                record.thrown?.let { throwable ->
                    val exceptionInfo = JsonObject().apply {
                        put("class", throwable.javaClass.name)
                        put("message", throwable.message)
                        put("stackTrace", getStackTraceString(throwable))
                    }
                    put("exception", exceptionInfo)
                }
                
                // Add parameters if present
                record.parameters?.let { params ->
                    val paramArray = params.map { param ->
                        when (param) {
                            null -> null
                            is String -> param
                            is Number -> param
                            is Boolean -> param
                            else -> param.toString()
                        }
                    }
                    put("parameters", paramArray)
                }
            }

            // Publish log entry to the node-specific syslog event bus
            // SyslogVerticle will handle MQTT publishing and in-memory storage
            val nodeSpecificAddress = EventBusAddresses.Syslog.logsForNode(nodeId)
            vertx.eventBus().publish(nodeSpecificAddress, logData)

        } catch (e: Exception) {
            // Silently ignore all errors to prevent logging loops
            // Do NOT use System.err.println or any logging here
        }
    }

    override fun flush() {
        // Nothing to flush for MQTT publishing
    }

    override fun close() {
        // Clean up if needed
    }

    /**
     * Initialize the node ID from the cluster
     */
    private fun initializeNodeId() {
        try {
            // Try to get node ID from Monster singleton
            val sessionHandler = Monster.getSessionHandler()
            nodeId = if (sessionHandler != null) {
                // SessionHandler extends AbstractVerticle, so it has access to vertx
                Monster.getClusterNodeId(sessionHandler.vertx)
            } else {
                "unknown"
            }
            initialized = true
        } catch (e: Exception) {
            nodeId = "unknown"
            initialized = false
        }
    }

    /**
     * Convert exception to stack trace string
     */
    private fun getStackTraceString(throwable: Throwable): String {
        return try {
            val sw = java.io.StringWriter()
            val pw = java.io.PrintWriter(sw)
            throwable.printStackTrace(pw)
            sw.toString()
        } catch (e: Exception) {
            "Unable to get stack trace: ${e.message}"
        }
    }
}