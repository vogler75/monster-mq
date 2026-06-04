package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IKafkaQueueStore
import at.rocworks.devices.kafkaserver.KafkaServerConfig
import at.rocworks.devices.kafkaserver.KafkaStreamMapping
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class KafkaServerConfigQueries(
    private val vertx: Vertx,
    private val deviceConfigStore: IDeviceConfigStore,
    private val configJson: JsonObject
) {
    companion object {
        private val logger: Logger = Utils.getLogger(KafkaServerConfigQueries::class.java)
        
        // Cache of "$storeType:$tableNameSuffix" -> Future<IKafkaQueueStore>
        private val activeStores = ConcurrentHashMap<String, Future<IKafkaQueueStore>>()
    }

    private val currentNodeId = Monster.getClusterNodeId(vertx)

    fun kafkaServers(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }

            deviceConfigStore.getAllDevices().onComplete { ar ->
                if (ar.succeeded()) {
                    try {
                        val devices = ar.result().filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER }
                        if (devices.isEmpty()) {
                            future.complete(emptyList())
                            return@onComplete
                        }

                        val futures = devices.map { deviceToMap(it) }
                        CompletableFuture.allOf(*futures.toTypedArray()).whenComplete { _, err ->
                            if (err != null) {
                                future.completeExceptionally(err)
                            } else {
                                future.complete(futures.map { it.join() })
                            }
                        }
                    } catch (e: Exception) {
                        logger.severe("Failed to process Kafka servers: ${e.message}")
                        future.completeExceptionally(e)
                    }
                } else {
                    logger.severe("Failed to load Kafka servers: ${ar.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun kafkaServer(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(null) }
            }

            val name = env.getArgument<String>("name")
            if (name == null) {
                future.complete(null)
                return@DataFetcher future
            }

            deviceConfigStore.getDevice(name).onComplete { ar ->
                if (ar.succeeded()) {
                    val device = ar.result()
                    if (device != null && device.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER) {
                        deviceToMap(device).whenComplete { result, err ->
                            if (err != null) {
                                future.completeExceptionally(err)
                            } else {
                                future.complete(result)
                            }
                        }
                    } else {
                        future.complete(null)
                    }
                } else {
                    logger.severe("Failed to load Kafka server '$name': ${ar.cause()?.message}")
                    future.complete(null)
                }
            }
            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): CompletableFuture<Map<String, Any?>> {
        val future = CompletableFuture<Map<String, Any?>>()
        val config = try {
            KafkaServerConfig.fromJsonObject(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse KafkaServerConfig for ${device.name}: ${e.message}")
            KafkaServerConfig()
        }

        val streamsList = config.streams.map { mapping ->
            mapOf(
                "streamName" to mapping.streamName,
                "topicFilter" to mapping.topicFilter,
                "retentionHours" to mapping.retentionHours,
                "storeType" to mapping.storeType,
                "allowWrite" to mapping.allowWrite
            )
        }

        // Check if server is running on the node
        val statusAddress = "kafkaserver.connectors.list.$currentNodeId"

        vertx.eventBus().request<JsonObject>(statusAddress, JsonObject()).onComplete { replyAr ->
            var active = false
            var starting = false
            if (replyAr.succeeded()) {
                val body = replyAr.result().body()
                val list = body.getJsonArray("servers")
                if (list != null && list.contains(device.name)) {
                    active = true
                }
                val startingList = body.getJsonArray("starting")
                if (startingList != null && startingList.contains(device.name)) {
                    starting = true
                }
            }
            
            val statusVal = if (device.enabled) {
                if (active) "RUNNING"
                else if (starting) "STARTING"
                else "ERROR"
            } else "STOPPED"
            
            future.complete(mapOf(
                "name" to device.name,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "enabled" to device.enabled,
                "host" to config.host,
                "port" to config.port,
                "storeType" to config.storeType,
                "streams" to streamsList,
                "createdAt" to device.createdAt.toString(),
                "updatedAt" to device.updatedAt.toString(),
                "isOnCurrentNode" to (device.nodeId == "*" || device.nodeId == currentNodeId),
                "status" to statusVal
            ))
        }

        return future
    }

    private fun calculateLag(store: IKafkaQueueStore, groupId: String, topics: List<String>): Future<Long> {
        val futures = topics.map { topic ->
            val offsetFuture = store.getOffset(groupId, topic, 0)
            val latestFuture = store.getLatestOffset(topic)
            Future.all(offsetFuture, latestFuture).map { _ ->
                val committed = offsetFuture.result() ?: 0L
                val latest = latestFuture.result() ?: 0L
                (latest - committed).coerceAtLeast(0L)
            }
        }
        return Future.all(futures).map { _ ->
            futures.sumOf { it.result() ?: 0L }
        }
    }

    fun kafkaConsumerGroups(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }
            
            getQueueStores().onComplete { storesAr ->
                if (storesAr.failed()) {
                    future.complete(emptyList())
                    return@onComplete
                }
                
                val stores = storesAr.result()
                if (stores.isEmpty()) {
                    future.complete(emptyList())
                    return@onComplete
                }
                
                val groupFutures = stores.map { it.getConsumerGroups() }
                Future.all(groupFutures).onComplete { allAr ->
                    if (allAr.succeeded()) {
                        try {
                            val allGroups = mutableMapOf<String, Pair<at.rocworks.stores.KafkaConsumerGroup, IKafkaQueueStore>>()
                            allAr.result().list<List<at.rocworks.stores.KafkaConsumerGroup>>().forEachIndexed { index, list ->
                                val store = stores[index]
                                list.forEach { group ->
                                    val existing = allGroups[group.groupId]
                                    if (existing == null) {
                                        allGroups[group.groupId] = Pair(group, store)
                                    } else {
                                        val mergedTopics = (existing.first.topics + group.topics).distinct()
                                        val maxTime = maxOf(existing.first.lastCommitTime, group.lastCommitTime)
                                        allGroups[group.groupId] = Pair(at.rocworks.stores.KafkaConsumerGroup(group.groupId, mergedTopics, maxTime), store)
                                    }
                                }
                            }
                            
                            val lagFutures = allGroups.values.map { (group, store) ->
                                calculateLag(store, group.groupId, group.topics).map { lagVal ->
                                    mapOf(
                                        "groupId" to group.groupId,
                                        "topics" to group.topics,
                                        "lastCommitTime" to Instant.ofEpochMilli(group.lastCommitTime).toString(),
                                        "lag" to lagVal
                                    )
                                }
                            }
                            
                            Future.all(lagFutures).onComplete { lagAr ->
                                if (lagAr.succeeded()) {
                                    future.complete(lagFutures.map { it.result() })
                                } else {
                                    logger.severe("Failed to calculate lag for consumer groups: ${lagAr.cause()?.message}")
                                    val resultList = allGroups.values.map { (group, _) ->
                                        mapOf(
                                            "groupId" to group.groupId,
                                            "topics" to group.topics,
                                            "lastCommitTime" to Instant.ofEpochMilli(group.lastCommitTime).toString(),
                                            "lag" to 0L
                                        )
                                    }
                                    future.complete(resultList)
                                }
                            }
                        } catch (e: Exception) {
                            logger.severe("Failed to process consumer groups: ${e.message}")
                            future.complete(emptyList())
                        }
                    } else {
                        logger.severe("Failed to retrieve consumer groups: ${allAr.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            }
            
            future
        }
    }

    private fun getQueueStores(): Future<List<IKafkaQueueStore>> {
        val promise = io.vertx.core.Promise.promise<List<IKafkaQueueStore>>()
        
        deviceConfigStore.getAllDevices().onComplete { ar ->
            if (ar.failed()) {
                promise.complete(emptyList())
                return@onComplete
            }
            
            try {
                // Collect dynamic streams
                val kafkaDevices = ar.result().filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER }
                val streamsToInitialize = mutableListOf<Pair<String?, String>>() // storeType, streamName
                
                kafkaDevices.forEach { device ->
                    val serverConfig = try {
                        KafkaServerConfig.fromJsonObject(device.config)
                    } catch (e: Exception) {
                        null
                    }
                    if (serverConfig != null) {
                        val serverStoreType = serverConfig.storeType
                        serverConfig.streams.forEach { stream ->
                            val streamStoreType = stream.storeType ?: serverStoreType
                            streamsToInitialize.add(Pair(streamStoreType, stream.streamName))
                        }
                    }
                }
                
                // Collect static streams from configJson
                val kafkaConfig = configJson.getJsonObject("KafkaServer", JsonObject())
                val staticStreams = kafkaConfig.getJsonArray("Streams") ?: kafkaConfig.getJsonArray("streams") ?: io.vertx.core.json.JsonArray()
                val staticServerStoreType = kafkaConfig.getString("storeType") ?: kafkaConfig.getString("StoreType")
                staticStreams.forEach { stream ->
                    val streamObj = stream as JsonObject
                    val streamName = streamObj.getString("StreamName") ?: streamObj.getString("streamName") ?: streamObj.getString("TopicFilter") ?: streamObj.getString("topicFilter")
                    if (streamName != null) {
                        val streamStoreType = streamObj.getString("StoreType") ?: streamObj.getString("storeType") ?: staticServerStoreType
                        streamsToInitialize.add(Pair(streamStoreType, streamName))
                    }
                }
                
                // Deduplicate by storeType + tableNameSuffix
                val uniqueStreams = streamsToInitialize.map { (storeType, streamName) ->
                    val tableNameSuffix = sanitizeTableNameSuffix(streamName)
                    Triple(storeType, tableNameSuffix, streamName)
                }.distinctBy { Pair(it.first, it.second) }
                
                if (uniqueStreams.isEmpty()) {
                    promise.complete(emptyList())
                    return@onComplete
                }
                
                val futures = uniqueStreams.map { (storeType, tableNameSuffix, streamName) ->
                    val cacheKey = "${storeType ?: "DEFAULT"}:$tableNameSuffix"
                    activeStores.computeIfAbsent(cacheKey) { _ ->
                        // Build custom config for the factory
                        val customConfig = configJson.copy()
                        val customKafkaServer = customConfig.getJsonObject("KafkaServer", JsonObject())
                        if (storeType != null) {
                            customKafkaServer.put("storeType", storeType)
                        } else {
                            customKafkaServer.remove("storeType")
                        }
                        customConfig.put("KafkaServer", customKafkaServer)
                        
                        at.rocworks.stores.KafkaQueueStoreFactory.create(vertx, customConfig, tableNameSuffix)
                    }
                }
                
                Future.all(futures).onComplete { allRes ->
                    if (allRes.succeeded()) {
                        val stores = allRes.result().list<IKafkaQueueStore>()
                        promise.complete(stores)
                    } else {
                        logger.severe("Failed to initialize some Kafka queue stores: ${allRes.cause()?.message}")
                        // Return the ones that succeeded if any
                        val succeededStores = mutableListOf<IKafkaQueueStore>()
                        futures.forEach { fut ->
                            if (fut.succeeded()) {
                                succeededStores.add(fut.result())
                            }
                        }
                        promise.complete(succeededStores)
                    }
                }
                
            } catch (e: Exception) {
                logger.severe("Error collecting queue stores: ${e.message}")
                promise.complete(emptyList())
            }
        }
        
        return promise.future()
    }
    
    fun kafkaMessages(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }

            val serverName = env.getArgument<String>("serverName")
            val topic = env.getArgument<String>("topic")
            val startOffsetArg = env.getArgument<Number>("startOffset")?.toLong()
            val limit = env.getArgument<Number>("limit")?.toInt() ?: 100

            if (serverName == null || topic == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            deviceConfigStore.getDevice(serverName).onComplete { ar ->
                val device = ar.result()
                if (ar.failed() || device == null) {
                    future.complete(emptyList())
                    return@onComplete
                }

                val serverConfig = try {
                    KafkaServerConfig.fromJsonObject(device.config)
                } catch (e: Exception) {
                    null
                }

                if (serverConfig == null) {
                    future.complete(emptyList())
                    return@onComplete
                }

                val matchingStream = serverConfig.streams.find { it.streamName == topic }
                val storeType = matchingStream?.storeType ?: serverConfig.storeType
                val tableNameSuffix = sanitizeTableNameSuffix(topic)
                val cacheKey = "${storeType ?: "DEFAULT"}:$tableNameSuffix"

                val storeFuture = activeStores.computeIfAbsent(cacheKey) { _ ->
                    val customConfig = configJson.copy()
                    val customKafkaServer = customConfig.getJsonObject("KafkaServer", JsonObject())
                    if (storeType != null) {
                        customKafkaServer.put("storeType", storeType)
                    } else {
                        customKafkaServer.remove("storeType")
                    }
                    customConfig.put("KafkaServer", customKafkaServer)

                    at.rocworks.stores.KafkaQueueStoreFactory.create(vertx, customConfig, tableNameSuffix)
                }

                storeFuture.onComplete { storeAr ->
                    if (storeAr.failed()) {
                        future.complete(emptyList())
                        return@onComplete
                    }
                    val store = storeAr.result()
                    
                    store.getEarliestOffset(topic).compose { earliest ->
                        store.getLatestOffset(topic).compose { latest ->
                            val startOffset = if (startOffsetArg != null && startOffsetArg >= 0) {
                                startOffsetArg
                            } else {
                                (latest - limit + 1).coerceAtLeast(earliest)
                            }
                            store.fetch(topic, startOffset, limit)
                        }
                    }.onComplete { fetchAr ->
                        if (fetchAr.failed()) {
                            future.complete(emptyList())
                        } else {
                            val result = fetchAr.result().map { (offset, msg) ->
                                mapOf(
                                    "offset" to offset,
                                    "topic" to msg.topicName,
                                    "partition" to 0,
                                    "timestamp" to msg.time.toString(),
                                    "key" to msg.topicName,
                                    "value" to msg.getPayloadAsString(),
                                    "size" to msg.payload.size
                                )
                            }
                            future.complete(result)
                        }
                    }
                }
            }
            future
        }
    }

    fun kafkaTopicOffsets(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(null) }
            }

            val serverName = env.getArgument<String>("serverName")
            val topic = env.getArgument<String>("topic")
            if (serverName == null || topic == null) {
                future.complete(null)
                return@DataFetcher future
            }

            deviceConfigStore.getDevice(serverName).onComplete { ar ->
                val device = ar.result()
                if (ar.failed() || device == null) {
                    future.complete(null)
                    return@onComplete
                }

                val serverConfig = try {
                    KafkaServerConfig.fromJsonObject(device.config)
                } catch (e: Exception) {
                    null
                }

                if (serverConfig == null) {
                    future.complete(null)
                    return@onComplete
                }

                val matchingStream = serverConfig.streams.find { it.streamName == topic }
                val storeType = matchingStream?.storeType ?: serverConfig.storeType
                val tableNameSuffix = sanitizeTableNameSuffix(topic)
                val cacheKey = "${storeType ?: "DEFAULT"}:$tableNameSuffix"

                val storeFuture = activeStores.computeIfAbsent(cacheKey) { _ ->
                    val customConfig = configJson.copy()
                    val customKafkaServer = customConfig.getJsonObject("KafkaServer", JsonObject())
                    if (storeType != null) {
                        customKafkaServer.put("storeType", storeType)
                    } else {
                        customKafkaServer.remove("storeType")
                    }
                    customConfig.put("KafkaServer", customKafkaServer)

                    at.rocworks.stores.KafkaQueueStoreFactory.create(vertx, customConfig, tableNameSuffix)
                }

                storeFuture.onComplete { storeAr ->
                    if (storeAr.failed()) {
                        future.complete(null)
                        return@onComplete
                    }
                    val store = storeAr.result()
                    store.getEarliestOffset(topic).compose { earliest ->
                        store.getLatestOffset(topic).map { latest ->
                            mapOf(
                                "topic" to topic,
                                "earliestOffset" to earliest,
                                "latestOffset" to latest
                            )
                        }
                    }.onComplete { resAr ->
                        if (resAr.succeeded()) {
                            future.complete(resAr.result())
                        } else {
                            future.complete(null)
                        }
                    }
                }
            }
            future
        }
    }

    private fun sanitizeTableNameSuffix(name: String): String {
        val sanitized = name.lowercase()
            .replace(Regex("[^a-z0-9]"), "_")
            .replace(Regex("_+"), "_")
            .trim('_')
        return if (sanitized.isEmpty()) "default" else sanitized
    }
}
