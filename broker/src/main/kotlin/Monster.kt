package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.net.JksOptions
import io.vertx.mqtt.MqttServer
import io.vertx.mqtt.MqttServerOptions

class Monster(private val port: Int, ssl: Boolean) : AbstractVerticle() {



    // Configure MQTT server options to use TLS
    private val options = MqttServerOptions().apply {
        isSsl = ssl
        keyStoreOptions = JksOptions()
            .setPath("server-keystore.jks")
            .setPassword("password")
    }

    override fun start() {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)
        mqttServer.exceptionHandler {
            it.printStackTrace()
        }
        mqttServer.endpointHandler { endpoint -> MonsterClient.endpointHandler(vertx, endpoint) }
        mqttServer.listen(port) { ar ->
            if (ar.succeeded()) {
                println("MQTT server is listening on port ${ar.result().actualPort()}")
            } else {
                println("Error starting MQTT server: ${ar.cause().message}")
            }
        }
    }
}
