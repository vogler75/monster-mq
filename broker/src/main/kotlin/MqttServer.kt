package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.net.JksOptions

import io.vertx.mqtt.MqttServer
import io.vertx.mqtt.MqttServerOptions

import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.logging.LogManager
import java.util.logging.Logger

class MqttServer(
    private val port: Int,
    private val ssl: Boolean,
    private val distributor: Distributor
) : AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val options = MqttServerOptions().apply {
        isSsl = ssl
        keyStoreOptions = JksOptions()
            .setPath("server-keystore.jks")
            .setPassword("password")
    }

    override fun start(startPromise: Promise<Void>) {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)

        mqttServer.exceptionHandler {
            it.printStackTrace()
        }

        mqttServer.endpointHandler { endpoint ->
            MqttClient.deployEndpoint(vertx, endpoint, distributor)
        }

        mqttServer.listen(port) { ar ->
            if (ar.succeeded()) {
                logger.info("MQTT Server is listening on port ${ar.result().actualPort()}")
                startPromise.complete()
            } else {
                logger.severe("Error starting MQTT Server: ${ar.cause().message}")
                startPromise.fail(ar.cause())
            }
        }
    }
}
