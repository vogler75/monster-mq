package at.rocworks

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage

class MonsterClient: AbstractVerticle() {
    private var endpoint: MqttEndpoint? = null
    private val subscriptions = mutableMapOf<String, MessageConsumer<Buffer>>()
    @Volatile private var pauseClient: Boolean = false
    private val messageQueue = mutableListOf<Pair<String, Buffer>>()

    companion object {
        private val clients: HashMap<String, MonsterClient> = hashMapOf()

        fun endpointHandler(vertx: Vertx, endpoint: MqttEndpoint) {
            val client : MonsterClient? = clients[endpoint.clientIdentifier()]
            if (client != null) {
                println("Existing client ${endpoint.clientIdentifier()}")
                if (endpoint.isCleanSession) client.cleanSession()
                client.startEndpoint(endpoint)
            } else {
                println("New client ${endpoint.clientIdentifier()}")
                val client = MonsterClient()
                vertx.deployVerticle(client)
                clients[endpoint.clientIdentifier()] = client
                client.startEndpoint(endpoint)
            }
            vertx.setTimer(100) { println("Verticles: " + vertx.deploymentIDs()) }
        }

        fun removeEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            println("Remove client ${endpoint.clientIdentifier()}")
            val client : MonsterClient? = clients[endpoint.clientIdentifier()]
            if (client != null) {
                clients.remove(endpoint.clientIdentifier())
                vertx.undeploy(client.deploymentID())
            }
            vertx.setTimer(100) { println("Verticles: " + vertx.deploymentIDs()) }
        }
    }

    override fun start() {
        super.start()
    }

    override fun stop() {
        super.stop()
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        println("MQTT client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession}")
        this.endpoint = endpoint
        endpoint.subscribeHandler(::subscribeHandler)
        endpoint.unsubscribeHandler(::unsubscribeHandler)
        endpoint.publishHandler(::publishHandler)
        endpoint.publishReleaseHandler(::publishReleaseHandler)
        endpoint.disconnectHandler { closeHandler() }
        endpoint.closeHandler { closeHandler() }
        endpoint.accept(endpoint.isCleanSession)
        if (this.pauseClient) {
            println("Send queue ${messageQueue.size}")
            messageQueue.forEach {
                endpoint.publish(it.first, it.second, MqttQoS.AT_LEAST_ONCE, false, false)
            }
            messageQueue.clear()
            this.pauseClient = false
        }
    }

    private fun stopEndpoint() {
        endpoint?.let { endpoint ->
            println("Stop client ${endpoint.clientIdentifier()} ")
            if (endpoint.isCleanSession) {
                println("Undeploy client ${endpoint.clientIdentifier()}")
                cleanSession()
                removeEndpoint(vertx, endpoint)
            } else {
                println("Pause client ${endpoint.clientIdentifier()}")
                pauseClient = true
            }
        }
    }

    fun cleanSession() {
        subscriptions.forEach { it.value.unregister() }
        subscriptions.clear()
        messageQueue.clear()
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        subscribe.topicSubscriptions().forEach {
            println("Subscription for ${it.topicName()} with QoS ${it.qualityOfService()}")
        }

        // Acknowledge the subscriptions
        endpoint?.subscribeAcknowledge(subscribe.messageId(), subscribe.topicSubscriptions().map { it.qualityOfService() })

        subscribe.topicSubscriptions().forEach { subscription ->
            val address = subscription.topicName()
            if (!subscriptions.contains(address)) {
                val consumer = vertx.eventBus().consumer<Buffer>(address) { message ->
                    val payload = message.body()
                    if (!this.pauseClient) {
                        this.endpoint?.publish(address, payload, MqttQoS.AT_LEAST_ONCE, false, false)
                        println("Delivered message from event bus to ${endpoint?.clientIdentifier()} topic $address: $payload")
                    } else {
                        println("Message from event bus must be queued, client ${endpoint?.clientIdentifier()} paused.")
                        messageQueue.add(address to payload)
                    }
                }
                subscriptions[address] = consumer
            }
        }
    }

    private fun unsubscribeHandler(unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { address ->
            subscriptions[address]?.unregister()
            subscriptions.remove(address)
        }
    }

    private fun publishHandler(message: MqttPublishMessage) {
        println("Received message on topic ${message.topicName()} with payload ${message.payload()} and QoS ${message.qosLevel()}")

        // Publish the message to the Vert.x event bus
        val address: String = message.topicName()
        val payload: Buffer = message.payload()
        vertx.eventBus().publish(address, payload)

        endpoint?.apply {
            // Handle QoS levels
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                publishAcknowledge(message.messageId())
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                publishReceived(message.messageId())
            }
        }
    }

    private fun publishReleaseHandler(messageId: Int) {
        endpoint?.publishComplete(messageId)
    }

    private fun closeHandler() {
        println("Close received ${endpoint?.clientIdentifier()}.")
        stopEndpoint()
    }
}