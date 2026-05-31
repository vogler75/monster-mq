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
                Future.all<List<at.rocworks.stores.KafkaConsumerGroup>>(groupFutures).onComplete { allAr ->
                    if (allAr.succeeded()) {
                        try {
                            val allGroups = mutableMapOf<String, at.rocworks.stores.KafkaConsumerGroup>()
                            allAr.result().list<List<at.rocworks.stores.KafkaConsumerGroup>>().forEach { list ->
                                list.forEach { group ->
                                    val existing = allGroups[group.groupId]
                                    if (existing == null) {
                                        allGroups[group.groupId] = group
                                    } else {
                                        val mergedTopics = (existing.topics + group.topics).distinct()
                                        val maxTime = maxOf(existing.lastCommitTime, group.lastCommitTime)
                                        allGroups[group.groupId] = at.rocworks.stores.KafkaConsumerGroup(group.groupId, mergedTopics, maxTime)
                                    }
                                }
                            }
                            
                            val resultList = allGroups.values.map { group ->
                                mapOf(
                                    "groupId" to group.groupId,
                                    "topics" to group.topics,
                                    "lastCommitTime" to Instant.ofEpochMilli(group.lastCommitTime).toString()
                                )
                            }
                            future.complete(resultList)
                        } catch (e: Exception) {
                            logger.severe("Failed to merge consumer groups: ${e.message}")
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
                
                Future.all<IKafkaQueueStore>(futures).onComplete { allRes ->
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
    
    private fun sanitizeTableNameSuffix(name: String): String {
        val sanitized = name.lowercase()
            .replace(Regex("[^a-z0-9]"), "_")
            .replace(Regex("_+"), "_")
            .trim('_')
        return if (sanitized.isEmpty()) "default" else sanitized
    }
}
