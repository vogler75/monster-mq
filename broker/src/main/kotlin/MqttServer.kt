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
    private val tcpNoDelay: Boolean = true,
    private val receiveBufferSize: Int = 512 * 1024,
    private val sendBufferSize: Int = 512 * 1024,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager,
    private val keyStorePath: String = "server-keystore.jks",
    private val keyStorePassword: String = "password"
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val options = MqttServerOptions().let { it ->
        it.isSsl = ssl
        it.keyCertOptions = JksOptions()
            .setPath(keyStorePath)
            .setPassword(keyStorePassword)
        it.isUseWebSocket = this.useWebSocket
        it.maxMessageSize = this.maxMessageSize
        it.isTcpNoDelay = this.tcpNoDelay      // Disable Nagle's algorithm - send packets immediately, don't coalesce
        it.receiveBufferSize = this.receiveBufferSize  // Receive buffer for burst traffic
        it.sendBufferSize = this.sendBufferSize         // Send buffer for burst traffic

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
                logger.info("MQTT Server is listening on port [${server.actualPort()}] [${if (useWebSocket) "WS " else "TCP"}][${if (ssl) "TLS" else "   "}]")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Error starting MQTT Server: ${error.message} [${Utils.getCurrentFunctionName()}]")
                startPromise.fail(error)
            }
    }
}
