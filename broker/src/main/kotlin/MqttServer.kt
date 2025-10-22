package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.net.JksOptions

import io.vertx.mqtt.MqttServer
import io.vertx.mqtt.MqttServerOptions

class MqttServer(
    private val port: Int,
    private val ssl: Boolean,
    private val useWebSocket: Boolean,
    private val maxMessageSize: Int,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val options = MqttServerOptions().let { it ->
        it.isSsl = ssl
        it.keyCertOptions = JksOptions()
            .setPath("server-keystore.jks")
            .setPassword("password")
        it.isUseWebSocket = this.useWebSocket
        it.maxMessageSize = this.maxMessageSize

        // CRITICAL FIX: Prevent TCP packet coalescing under high load
        // This fixes "Illegal QOS Level" and "invalid topic name" errors
        it.isTcpNoDelay = true  // Disable Nagle's algorithm - send packets immediately, don't coalesce
        it.receiveBufferSize = 512 * 1024  // 512KB receive buffer for burst traffic
        it.sendBufferSize = 512 * 1024     // 512KB send buffer

        // Set reasonable idle timeout
        it.idleTimeout = 60  // seconds

        it
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)

        mqttServer.exceptionHandler {
            logger.severe("MQTT Server error: ${it.message} [${Utils.getCurrentFunctionName()}]")
            //it.printStackTrace()
        }

        mqttServer.endpointHandler { endpoint ->
            MqttClient.deployEndpoint(vertx, endpoint, sessionHandler, userManager)
        }

        mqttServer.listen(port)
            .onSuccess { server ->
                logger.info("MQTT Server is listening on port [${server.actualPort()}] SSL [$ssl] WS [$useWebSocket] [${deploymentID()}] [${Utils.getCurrentFunctionName()}]")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Error starting MQTT Server: ${error.message} [${Utils.getCurrentFunctionName()}]")
                startPromise.fail(error)
            }
    }
}
