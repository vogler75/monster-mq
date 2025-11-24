package at.rocworks.extensions.oa4j

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

/**
 * Manager for WinCC OA for Java (oa4j) integration.
 * Handles initialization and lifecycle management of oa4j components.
 *
 * Currently runs as a separate program alongside MonsterMQ broker.
 * Future integration phases will add broker startup and component integration.
 */
class Oa4jManager(
    private val vertx: Vertx,
    private val config: JsonObject
) {
    private val logger = LoggerFactory.getLogger(Oa4jManager::class.java)
    private var isInitialized = false

    /**
     * Initialize the oa4j manager.
     * Loads configuration from config.yaml under "oa4j" section.
     */
    fun init() {
        try {
            val oa4jConfig = config.getJsonObject("oa4j") ?: JsonObject()

            logger.info("Initializing oa4j manager")
            logger.debug("oa4j configuration: {}", oa4jConfig.encodePrettily())

            // Currently oa4j runs as a separate process
            // In future phases, this will initialize oa4j components and integrate with broker

            isInitialized = true
            logger.info("oa4j manager initialized successfully")
        } catch (e: Exception) {
            logger.error("Failed to initialize oa4j manager: {}", e.message, e)
            throw e
        }
    }

    /**
     * Start the oa4j manager.
     * Currently a no-op as oa4j runs separately.
     */
    fun start() {
        if (!isInitialized) {
            throw IllegalStateException("oa4j manager must be initialized before starting")
        }
        logger.info("oa4j manager started (running as separate program)")
    }

    /**
     * Stop the oa4j manager.
     * Cleanup and graceful shutdown of oa4j components.
     */
    fun stop() {
        if (isInitialized) {
            logger.info("Stopping oa4j manager")
            // Future: implement oa4j component shutdown
            isInitialized = false
        }
    }

    fun isRunning(): Boolean = isInitialized

    companion object {
        fun create(vertx: Vertx, config: JsonObject): Oa4jManager {
            return Oa4jManager(vertx, config)
        }
    }
}
