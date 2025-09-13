package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.MqttClient
import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.data.*
import at.rocworks.stores.ISessionStoreAsync
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.mqtt.MqttWill
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

open class SessionHandler(
    private val sessionStore: ISessionStoreAsync,
    private val messageBus: IMessageBus,
    private val messageHandler: MessageHandler,
    private val enqueueMessages: Boolean
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val topicIndex = TopicTree<String, Int>() // Topic index with client and QoS
    private val clientStatus = ConcurrentHashMap<String, ClientStatus>() // ClientId + Status

    // Metrics tracking
    private val clientMetrics = ConcurrentHashMap<String, SessionMetrics>() // ClientId -> Metrics
    private val clientDetails = ConcurrentHashMap<String, ClientDetails>() // ClientId -> Session details

    private val subAddQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val subDelQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable

    private val msgAddQueue: ArrayBlockingQueue<Pair<MqttMessage, List<String>>> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val msgDelQueue: ArrayBlockingQueue<Pair<String, String>> = ArrayBlockingQueue(10_000) // TODO: configurable
    private var waitForFlush: Promise<Void>? = null

    private val subscriptionAddAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE+"/A"
    private val subscriptionDelAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE+"/D"

    private val clientStatusAddress = Const.GLOBAL_CLIENT_TABLE_NAMESPACE+"/C"

    private val sparkplugHandler = Monster.getSparkplugExtension()

    private val inFlightMessages = HashMap<String, ArrayBlockingQueue<MqttMessage>>()

    private fun commandAddress() = "${Const.GLOBAL_EVENT_NAMESPACE}/${deploymentID()}/C"
    private fun metricsAddress() = "monstermq.node.metrics.${Monster.getClusterNodeId(vertx)}"
    //private fun messageAddress() = "${Const.GLOBAL_EVENT_NAMESPACE}/${deploymentID()}/M"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
    }

    enum class ClientStatus {
        UNKNOWN, CREATED, ONLINE, PAUSED, DELETE
    }

    data class SessionMetrics(
        val messagesIn: AtomicLong = AtomicLong(0),
        val messagesOut: AtomicLong = AtomicLong(0)
    )

    data class ClientDetails(
        val nodeId: String,
        val clientAddress: String?,
        val cleanSession: Boolean,
        val sessionExpiryInterval: Int?
    )

    override fun start(startPromise: Promise<Void>) {
        logger.info("Start session handler...")

        vertx.eventBus().consumer<MqttSubscription>(subscriptionAddAddress) {
            topicIndex.add(it.body().topicName, it.body().clientId, it.body().qos.value())
        }

        vertx.eventBus().consumer<MqttSubscription>(subscriptionDelAddress) {
            topicIndex.del(it.body().topicName, it.body().clientId)
        }

        vertx.eventBus().consumer<JsonObject>(clientStatusAddress) { message ->
            logger.fine("Client status [${message.body()}]")
            val clientId = message.body().getString("ClientId", "")
            val status = ClientStatus.valueOf(message.body().getString("Status", ""))
            val deliveryOptions = DeliveryOptions(JsonObject().put("NodeId", Monster.getClusterNodeId(vertx)))

            fun flushFinished() {
                logger.fine("Flushed messages finished")
                message.reply(true, deliveryOptions)
            }

            when (status) {
                ClientStatus.CREATED -> {
                    clientStatus[clientId] = ClientStatus.CREATED
                    waitForFlush?.let { promise ->
                        logger.fine("Existing wait for flush...")
                        promise.future().onComplete { flushFinished() }
                    } ?: Promise.promise<Void>().let { promise ->
                        logger.fine("New wait for flush...")
                        waitForFlush = promise
                        promise.future().onComplete { flushFinished() }
                    }
                }
                ClientStatus.ONLINE -> {
                    inFlightMessages[clientId]?.let { messages ->
                        logger.fine("Publishing [${messages.count()}] in-flight messages to client [${clientId}] [${Utils.getCurrentFunctionName()}]")
                        messages.forEach { message -> sendMessageToClient(clientId, message)}
                    }
                    inFlightMessages.remove(clientId)
                    clientStatus[clientId] = ClientStatus.ONLINE
                    message.reply(true, deliveryOptions)
                }
                ClientStatus.PAUSED -> {
                    clientStatus[clientId] = ClientStatus.PAUSED
                    message.reply(true, deliveryOptions)
                }
                ClientStatus.DELETE -> {
                    clientStatus.remove(clientId)
                    message.reply(true, deliveryOptions)
                }
                else -> {
                    logger.warning("Unknown client status [${message.body()}] [${Utils.getCurrentFunctionName()}]")
                    message.reply(false, deliveryOptions)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(commandAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received request [${payload}] [${Utils.getCurrentFunctionName()}]" }
                when (payload.getString(COMMAND_KEY)) {
                    COMMAND_SUBSCRIBE -> subscribeCommand(message)
                    COMMAND_UNSUBSCRIBE -> unsubscribeCommand(message)
                    else -> logger.warning("Unknown command [${payload}] [${Utils.getCurrentFunctionName()}]")
                }
            }
        }

        // Metrics query handler
        vertx.eventBus().consumer<JsonObject>(metricsAddress()) { message ->
            val metrics = JsonObject()
            var totalMessagesIn = 0L
            var totalMessagesOut = 0L

            clientMetrics.values.forEach { sessionMetrics ->
                totalMessagesIn += sessionMetrics.messagesIn.get()
                totalMessagesOut += sessionMetrics.messagesOut.get()
            }

            metrics.put("messagesIn", totalMessagesIn)
                   .put("messagesOut", totalMessagesOut)
                   .put("nodeSessionCount", clientMetrics.size)

            message.reply(metrics)
        }

        queueWorkerThread("SubAddQueue", subAddQueue, 1000, sessionStore::addSubscriptions)
        queueWorkerThread("SubDelQueue", subDelQueue, 1000, sessionStore::delSubscriptions)

        queueWorkerThread("MsgAddQueue", msgAddQueue, 1000, sessionStore::enqueueMessages)
        queueWorkerThread("MsgDelQueue", msgDelQueue, 1000, sessionStore::removeMessages)

        logger.info("Subscribing to message bus [${Utils.getCurrentFunctionName()}]")
        val f0 = messageBus.subscribeToMessageBus(::consumeMessageFromBus)

        logger.info("Indexing subscription table [${Utils.getCurrentFunctionName()}]")
        val f1 = sessionStore.iterateSubscriptions(topicIndex::add)

        logger.info("Indexing offline clients [${Utils.getCurrentFunctionName()}]")
        val f2 = sessionStore.iterateOfflineClients { clientId ->
            clientStatus[clientId] = ClientStatus.PAUSED
        }

        Future.all(f0, f1, f2).onComplete {
            if (it.succeeded()) {
                logger.info("Session handler ready [${Utils.getCurrentFunctionName()}]")
                startPromise.complete()
            } else {
                startPromise.fail(it.cause())
            }
        }
    }

    /*
    protected open fun publishMessageToBus(message: MqttMessage) {
        vertx.eventBus().publish(messageAddress(), message)
    }

    protected open fun subscribeToMessageBus(): Future<Void> {
        vertx.eventBus().consumer<MqttMessage>(messageAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}] [${Utils.getCurrentFunctionName()}]" }
                consumeMessageFromBus(payload)
            }
        }
        return Future.succeededFuture()
    }
     */

    private fun <T> queueWorkerThread(
        name: String,
        queue: ArrayBlockingQueue<T>,
        blockSize: Int,
        execute: (block: List<T>)->Future<Void>
    ) /*= thread(start = true)*/ {
        logger.fine("Start [$name] loop")
        val block = arrayListOf<T>()
        var lastCheckTime = System.currentTimeMillis()

        fun flushed() {
            if (name == "MsgAddQueue" && waitForFlush != null) {
                waitForFlush?.complete()
                waitForFlush = null
            }
        }

        fun loop() {
            vertx.executeBlocking(Callable {
                queue.poll(100, TimeUnit.MILLISECONDS)?.let { item ->
                    block.add(item)
                    while (queue.poll()?.let(block::add) != null && block.size < blockSize) {
                        // nothing to do here
                    }
                }
            }).onComplete {
                if (block.isNotEmpty()) {
                    execute(block).onComplete {
                        block.clear()
                        flushed()
                        vertx.runOnContext { loop() }
                    }
                } else {
                    flushed()
                    vertx.runOnContext { loop() }
                }

                val currentTime = System.currentTimeMillis()
                if (currentTime - lastCheckTime >= 1000 && queue.size > 1000) { // TODO: configurable
                    logger.warning("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
                    lastCheckTime = currentTime
                }
            }
        }
        loop()
    }

    fun getClientStatus(clientId: String): ClientStatus = clientStatus[clientId] ?: ClientStatus.UNKNOWN

    // Metrics tracking methods
    fun incrementMessagesIn(clientId: String) {
        clientMetrics[clientId]?.messagesIn?.incrementAndGet()
    }

    fun incrementMessagesOut(clientId: String) {
        clientMetrics[clientId]?.messagesOut?.incrementAndGet()
    }

    fun getClientMetrics(clientId: String): SessionMetrics? = clientMetrics[clientId]

    fun getAllClientMetrics(): Map<String, SessionMetrics> = clientMetrics.toMap()

    fun getClientDetails(clientId: String): ClientDetails? = clientDetails[clientId]

    fun getSessionCount(): Int = clientMetrics.size

    fun setClient(clientId: String, cleanSession: Boolean, information: JsonObject): Future<Void> {
        logger.fine("Set client [$clientId] clean session [$cleanSession] information [$information]")

        // Initialize metrics and client details
        clientMetrics[clientId] = SessionMetrics()
        clientDetails[clientId] = ClientDetails(
            nodeId = Monster.getClusterNodeId(vertx),
            clientAddress = information.getString("clientAddress"),
            cleanSession = cleanSession,
            sessionExpiryInterval = information.getInteger("sessionExpiryInterval")
        )

        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.CREATED)
        val f1 = sessionStore.setClient(clientId, Monster.getClusterNodeId(vertx), cleanSession, true, information)
        return if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            Future.all<Any>(listOf(f1)+fx as List<Future<*>>).mapEmpty()

        } else {
            val f2 = vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            Future.all(f1, f2).mapEmpty()
        }
    }

    fun onlineClient(clientId: String): Future<Void> {
        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.ONLINE)
        return if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            Future.all<Any>(fx as List<Future<*>>).mapEmpty()
        } else {
            vertx.eventBus().request<Boolean>(clientStatusAddress, payload).mapEmpty()
        }
    }

    fun pauseClient(clientId: String): Future<Void> {
        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.PAUSED)
        val f1 = sessionStore.setConnected(clientId, false)
        if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            return Future.all<Any>(listOf(f1)+fx as List<Future<*>>).mapEmpty()
        } else {
            val f2 = vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            return Future.all(f1, f2).mapEmpty()
        }
    }

    fun delClient(clientId: String): Future<Void> {
        // Clean up metrics and client details
        clientMetrics.remove(clientId)
        clientDetails.remove(clientId)

        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.DELETE)
        vertx.eventBus().publish(clientStatusAddress, payload)
        return sessionStore.delClient(clientId) { subscription ->
            logger.finest { "Delete subscription [$subscription]" }
            vertx.eventBus().publish(subscriptionDelAddress, subscription)
        }
    }

    fun setLastWill(clientId: String, will: MqttWill): Future<Void> {
        if (will.isWillFlag) {
            val message = MqttMessage(clientId, will)
            return sessionStore.setLastWill(clientId, message)
        } else {
            return sessionStore.setLastWill(clientId, null)
        }
    }

    fun isPresent(clientId: String): Future<Boolean> = sessionStore.isPresent(clientId)

    private fun enqueueMessage(message: MqttMessage, clientIds: List<String>) {
        if (enqueueMessages) msgAddQueue.add(Pair(message, clientIds))
    }

    fun dequeueMessages(clientId: String, callback: (MqttMessage)->Boolean) = sessionStore.dequeueMessages(clientId, callback)

    fun removeMessage(clientId: String, messageUuid: String) {
        msgDelQueue.add(Pair(clientId, messageUuid))
    }

    private fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionAddAddress, subscription)
        try {
            subAddQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    private fun delSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionDelAddress, subscription)
        try {
            subDelQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    private fun findClients(topicName: String): Set<Pair<String, Int>> {
        val result = topicIndex.findDataOfTopicName(topicName).toSet()
        logger.finest { "Found [${result.size}] clients [${result.joinToString(",")}] [${Utils.getCurrentFunctionName()}]" }
        return result
    }

    fun purgeSessions() = sessionStore.purgeSessions()

    fun purgeQueuedMessages() = sessionStore.purgeQueuedMessages()

    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit)
    = sessionStore.iterateNodeClients(nodeId, callback)


    private fun sendMessageToClient(clientId: String, message: MqttMessage): Future<Boolean> {
        if (message.qosLevel == 0) {
            vertx.eventBus().send(MqttClient.getMessagesAddress(clientId), message)
            return Future.succeededFuture(true)
        } else {
            val promise = Promise.promise<Boolean>()
            vertx.eventBus().request<Boolean>(MqttClient.getMessagesAddress(clientId), message)
                .onComplete { ar ->
                    promise.complete(!ar.failed() && ar.result().body())
                }
            return promise.future()
        }
    }

    private fun addInFlightMessage(clientId: String, message: MqttMessage) {
        logger.fine { "Adding in-flight message to queue [${message.topicName}] [${message.getPayloadAsJson()}]}" }
        inFlightMessages[clientId]?.let { queue ->
            if (queue.remainingCapacity() == 0) {
                logger.warning("In-flight messages queue full [${Utils.getCurrentFunctionName()}]")
            } else {
                queue.put(message)
            }
        } ?: run {
            ArrayBlockingQueue<MqttMessage>(10_000).let { // TODO: configurable
                inFlightMessages[clientId] = it
                it.put(message)
            }
        }
    }

    private fun flushInFlightToQueue(clientId: String) {
        inFlightMessages[clientId]?.let { queue ->
            queue.forEach { enqueueMessage(it, listOf(clientId)) }
            queue.clear()
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MqttClient, topicName: String, qos: MqttQoS) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
            .put(Const.QOS_KEY, qos.value())
        vertx.eventBus().request<Boolean>(commandAddress(), request)
            .onFailure { error ->
                logger.severe("Subscribe request failed [${error}] [${Utils.getCurrentFunctionName()}]")
            }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        val qos = MqttQoS.valueOf(command.body().getInteger(Const.QOS_KEY))

        messageHandler.findRetainedMessages(topicName, 0) { message -> // TODO: max must be configurable
            logger.finest { "Publish retained message [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
            sendMessageToClient(clientId, message)
        }.onComplete {
            logger.finest { "Retained messages published [${it.result()}] [${Utils.getCurrentFunctionName()}]" }
            addSubscription(MqttSubscription(clientId, topicName, qos))
            command.reply(true)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
        vertx.eventBus().request<Boolean>(commandAddress(), request)
            .onFailure { error ->
                logger.severe("Unsubscribe request failed [${error}] [${Utils.getCurrentFunctionName()}]")
            }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        delSubscription(MqttSubscription(clientId, topicName, MqttQoS.FAILURE /* not needed */))
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        messageBus.publishMessageToBus(message)
        messageHandler.saveMessage(message)
        sparkplugHandler?.metricExpansion(message) { spbMessage ->
            logger.finest { "Publishing Sparkplug message [${spbMessage.topicName}] [${Utils.getCurrentFunctionName()}]" }
            messageBus.publishMessageToBus(spbMessage)
            messageHandler.saveMessage(spbMessage)
        }
    }

    private fun messageWithQos(message: MqttMessage, qos: Int): MqttMessage {
        return if (qos < message.qosLevel) {
            message.cloneWithNewQoS(qos)
        } else {
            message
        }
    }

    private fun consumeMessageFromBus(message: MqttMessage) {
        findClients(message.topicName).groupBy { (a, subscriptionQos) ->
            if (subscriptionQos < message.qosLevel) subscriptionQos else message.qosLevel // Potentially downgrade QoS
        }.forEach { (qos, clients) ->
            val m = messageWithQos(message, qos) // Potentially downgrade QoS
            when (qos) {
                0 -> clients.forEach { (clientId, _) ->
                    sendMessageToClient(clientId, m)
                    incrementMessagesOut(clientId)
                }
                1, 2 -> {
                    val (online, others) = clients.partition { (clientId, _) ->
                        clientStatus[clientId] == ClientStatus.ONLINE
                    }
                    logger.finest { "Online [${online.size}] Other [${others.size}] [${Utils.getCurrentFunctionName()}]" }
                    online.forEach { (clientId, _) ->
                        sendMessageToClient(clientId, m).onComplete {
                            if (it.failed() || !it.result()) {
                                logger.warning("Message sent to online client failed [${clientId}]")
                                enqueueMessage(m, listOf(clientId))
                            } else {
                                incrementMessagesOut(clientId)
                            }
                        }
                    }
                    if (others.isNotEmpty()) {
                        val (created, offline) = others.partition { (clientId, _) ->
                            clientStatus[clientId] == ClientStatus.CREATED
                        }
                        logger.finest { "Created [${created.size}] Offline [${offline.size}] [${Utils.getCurrentFunctionName()}]" }
                        created.forEach { (clientId, _) ->
                            addInFlightMessage(clientId, m)
                        }
                        if (offline.isNotEmpty()) {
                            enqueueMessage(m, offline.map { it.first })
                        }
                    }
                }
            }
        }
    }
}