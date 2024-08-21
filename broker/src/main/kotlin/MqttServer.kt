package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.net.JksOptions

import io.vertx.mqtt.MqttServer
import io.vertx.mqtt.MqttServerOptions

import java.util.logging.Logger

class MqttServer(
    private val port: Int,
    private val ssl: Boolean,
    private val ws: Boolean,
    private val distributor: Distributor
) : AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val options = MqttServerOptions().apply {
        isSsl = ssl
        keyStoreOptions = JksOptions()
            .setPath("server-keystore.jks")
            .setPassword("password")
        isUseWebSocket = ws
    }

    override fun start(startPromise: Promise<Void>) {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)

        mqttServer.exceptionHandler {
            it.printStackTrace()
        }

        mqttServer.endpointHandler { endpoint ->
            logger.info("MQTT Client on server [${deploymentID()}]")
            MqttClient.deployEndpoint(vertx, endpoint, distributor)
        }

        mqttServer.listen(port) { ar ->
            if (ar.succeeded()) {
                logger.info("MQTT Server is listening on port [${ar.result().actualPort()}] SSL [$ssl] WS [$ws] [${deploymentID()}]")
                startPromise.complete()
            } else {
                logger.severe("Error starting MQTT Server: ${ar.cause().message}")
                startPromise.fail(ar.cause())
            }
        }
    }
}
