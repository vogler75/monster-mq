package at.rocworks

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
    private val sessionHandler: SessionHandler
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val options = MqttServerOptions().let { it ->
        it.isSsl = ssl
        it.keyStoreOptions = JksOptions()
            .setPath("server-keystore.jks")
            .setPassword("password")
        it.isUseWebSocket = this.useWebSocket
        it.maxMessageSize = this.maxMessageSize
        it
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)

        mqttServer.exceptionHandler {
            it.printStackTrace()
        }

        mqttServer.endpointHandler { endpoint ->
            MqttClient.deployEndpoint(vertx, endpoint, sessionHandler)
        }

        mqttServer.listen(port) { ar ->
            if (ar.succeeded()) {
                logger.info("MQTT Server is listening on port [${ar.result().actualPort()}] SSL [$ssl] WS [$useWebSocket] [${deploymentID()}] [${Utils.getCurrentFunctionName()}]")
                startPromise.complete()
            } else {
                logger.severe("Error starting MQTT Server: ${ar.cause().message} [${Utils.getCurrentFunctionName()}]")
                startPromise.fail(ar.cause())
            }
        }
    }
}
