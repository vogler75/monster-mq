package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IKafkaQueueStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.net.NetServerOptions
import io.vertx.core.net.NetSocket
import java.nio.charset.StandardCharsets
import java.util.zip.CRC32

import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Future
import at.rocworks.devices.kafkaserver.KafkaServerConfig
import at.rocworks.devices.kafkaserver.KafkaStreamMapping
import at.rocworks.data.TopicTree

class KafkaProtocolServer(
    private val configJson: JsonObject,
    private val kafkaStore: IKafkaQueueStore,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager,
    private val deviceConfigStore: IDeviceConfigStore? = null
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val kafkaConfig = configJson.getJsonObject("KafkaServer", JsonObject())
    private val host = kafkaConfig.getString("host", "0.0.0.0")
    private val port = kafkaConfig.getInteger("port", 9092)
    private val rawAdvertisedHost = kafkaConfig.getString("advertisedHost") ?: kafkaConfig.getString("advertised_host") ?: host
    private val advertisedPort = kafkaConfig.getInteger("advertisedPort") ?: kafkaConfig.getInteger("advertised_port") ?: port

    private val advertisedHost: String = if (rawAdvertisedHost == "0.0.0.0") {
        resolveLocalIp()
    } else {
        rawAdvertisedHost
    }

    private fun resolveLocalIp(): String {
        return try {
            val clusterManager = Monster.getClusterManager()
            if (clusterManager != null) {
                val hz = clusterManager.hazelcastInstance
                val hostString = hz.cluster.localMember.socketAddress.hostString
                if (!hostString.isNullOrEmpty() && hostString != "0.0.0.0") {
                    return hostString
                }
            }
            val localAddress = java.net.InetAddress.getLocalHost().hostAddress
            if (localAddress == "0.0.0.0" || localAddress == "127.0.0.1") {
                "localhost"
            } else {
                localAddress
            }
        } catch (e: Exception) {
            "localhost"
        }
    }

    private val configuredTopics = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val activeStreams = java.util.concurrent.CopyOnWriteArrayList<KafkaStreamConfig>()

    override fun start(startPromise: Promise<Void>) {
        // Load static and dynamic configured topics/streams
        refreshConfiguredTopics().onComplete { ar ->
            if (ar.failed()) {
                logger.warning("Failed to perform initial Kafka configured topics load: ${ar.cause()?.message}")
            }
            
            val serverOptions = NetServerOptions()
                .setHost(host)
                .setPort(port)
                .setTcpNoDelay(true)

            val server = vertx.createNetServer(serverOptions)
            server.connectHandler { socket ->
                KafkaConnection(socket).start()
            }

            server.listen().onComplete { result ->
                if (result.succeeded()) {
                    logger.info("Apache Kafka protocol server listening on $host:$port")
                    
                    // Trigger dynamic topics refresh on Event Bus config changes
                    vertx.eventBus().consumer<JsonObject>("kafkaserver.device.config.changed") { _ ->
                        refreshConfiguredTopics()
                    }
                    
                    startPromise.complete()
                } else {
                    logger.severe("Failed to start Kafka protocol server on $host:$port: ${result.cause()?.message}")
                    startPromise.fail(result.cause())
                }
            }
        }
    }

    private fun refreshConfiguredTopics(): Future<Void> {
        val newStreams = mutableListOf<KafkaStreamConfig>()
        
        // 1. Load static streams from config.yaml
        val streamsConfig = kafkaConfig.getJsonArray("streams", io.vertx.core.json.JsonArray())
        streamsConfig.forEach { stream ->
            val streamObj = stream as JsonObject
            val topicFilter = streamObj.getString("topicFilter")
            val streamName = streamObj.getString("streamName") ?: topicFilter
            val allowWrite = streamObj.getBoolean("allowWrite", true)
            if (!streamName.isNullOrBlank() && !topicFilter.isNullOrBlank()) {
                newStreams.add(KafkaStreamConfig(streamName, topicFilter, allowWrite))
            }
        }

        if (deviceConfigStore == null) {
            updateActiveStreams(newStreams)
            return Future.succeededFuture()
        }

        val currentNodeId = Monster.getClusterNodeId(vertx)
        return deviceConfigStore.getEnabledDevicesByNode(currentNodeId).map { list ->
            // 2. Dynamic streams from KafkaServer devices
            list.filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER }.forEach { dev ->
                try {
                    val serverConfig = KafkaServerConfig.fromJsonObject(dev.config)
                    serverConfig.streams.forEach { mapping ->
                        if (mapping.streamName.isNotBlank() && mapping.topicFilter.isNotBlank()) {
                            newStreams.add(KafkaStreamConfig(mapping.streamName, mapping.topicFilter, mapping.allowWrite))
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Failed to parse KafkaServerConfig streams: ${e.message}")
                }
            }

            // 3. Standalone streams from Kafka-Stream devices
            list.filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_STREAM }.forEach { dev ->
                val streamName = dev.name
                val topicFilter = dev.namespace
                val allowWrite = dev.config.getBoolean("allowWrite") ?: dev.config.getBoolean("AllowWrite") ?: true
                if (streamName.isNotBlank() && topicFilter.isNotBlank()) {
                    newStreams.add(KafkaStreamConfig(streamName, topicFilter, allowWrite))
                }
            }

            updateActiveStreams(newStreams)
            null
        }
    }

    private fun updateActiveStreams(newStreams: List<KafkaStreamConfig>) {
        activeStreams.clear()
        activeStreams.addAll(newStreams)
        
        configuredTopics.clear()
        newStreams.forEach { stream ->
            configuredTopics.add(stream.streamName)
        }
        
        logger.info("Updated active Kafka streams: ${activeStreams.map { "${it.streamName} -> ${it.topicFilter} (allowWrite=${it.allowWrite})" }}")
    }

    private fun isWriteAllowed(topic: String, msgTopic: String): Boolean {
        // Find all configured streams for this Kafka topic
        val matchingStreams = activeStreams.filter { it.streamName == topic }
        
        // "Otherwise it can publish to any topic."
        if (matchingStreams.isEmpty()) {
            return true
        }
        
        // Check if there is at least one matching stream config that allows write and matches the filter
        return matchingStreams.any { stream ->
            stream.allowWrite && at.rocworks.data.TopicTree.matches(stream.topicFilter, msgTopic)
        }
    }

    private inner class KafkaConnection(private val socket: NetSocket) {
        private var closed = false
        private var inputBuffer = Buffer.buffer()
        private var authenticated = !userManager.isUserManagementEnabled()
        private var username: String? = null
        private var saslHandshakeReceived = false
        private var authenticating = false

        fun start() {
            socket.handler { buf ->
                inputBuffer.appendBuffer(buf)
                processInput()
            }

            socket.closeHandler {
                cleanup()
            }

            socket.exceptionHandler { err ->
                logger.fine("Kafka client socket error: ${err.message}")
                close()
            }
        }

        private fun processInput() {
            while (inputBuffer.length() >= 4 && !closed && !authenticating) {
                val size = inputBuffer.getInt(0)
                if (size <= 0 || size > 10 * 1024 * 1024) { // Max 10MB sanity check
                    close()
                    break
                }
                if (inputBuffer.length() < size + 4) {
                    // Packet not fully received yet
                    break
                }

                // Slice the complete packet
                val packet = inputBuffer.slice(4, size + 4)
                
                // Advance inputBuffer to the remaining portion safely by copying remaining bytes
                val remainingLen = inputBuffer.length() - (size + 4)
                inputBuffer = if (remainingLen > 0) {
                    Buffer.buffer(inputBuffer.getBytes(size + 4, inputBuffer.length()))
                } else {
                    Buffer.buffer()
                }

                try {
                    if (saslHandshakeReceived && !authenticated) {
                        // Check if standard SaslAuthenticate request (apiKey == 36) or raw token payload
                        val tempReader = KafkaBufferReader(packet)
                        val tempApiKey = if (packet.length() >= 2) tempReader.readShort().toInt() else -1
                        if (tempApiKey == 36) {
                            handleRequest(packet)
                        } else {
                            handleRawSaslPlainToken(packet)
                        }
                    } else {
                        handleRequest(packet)
                    }
                } catch (e: Exception) {
                    logger.severe("Error handling Kafka request packet: ${e.message}")
                    e.printStackTrace()
                    close()
                }
            }
        }

        private fun handleRequest(packet: Buffer) {
            val reader = KafkaBufferReader(packet)
            val apiKey = reader.readShort()
            val apiVersion = reader.readShort()
            val correlationId = reader.readInt()
            val clientId = reader.readString()

            logger.finest { "Received Kafka Request: apiKey=$apiKey, apiVersion=$apiVersion, correlationId=$correlationId, clientId=$clientId" }

            if (userManager.isUserManagementEnabled() && !authenticated) {
                val apiKeyInt = apiKey.toInt()
                if (apiKeyInt != 18 && apiKeyInt != 17 && apiKeyInt != 36) {
                    logger.warning("Unauthenticated Kafka request rejected (apiKey=$apiKey, clientId=$clientId). Closing connection.")
                    close()
                    return
                }
            }

            val response = KafkaBufferWriter()
            response.writeInt(correlationId) // First field in response is always correlationId

            when (apiKey.toInt()) {
                0 -> handleProduce(apiVersion, reader, response, clientId)
                18 -> handleApiVersions(apiVersion, response)
                3 -> handleMetadata(apiVersion, reader, response)
                10 -> handleFindCoordinator(apiVersion, reader, response)
                2 -> handleListOffsets(apiVersion, reader, response)
                8 -> handleOffsetCommit(apiVersion, reader, response)
                9 -> handleOffsetFetch(apiVersion, reader, response)
                1 -> handleFetch(apiVersion, reader, response)
                11 -> handleJoinGroup(apiVersion, reader, response)
                12 -> handleHeartbeat(apiVersion, reader, response)
                13 -> handleLeaveGroup(apiVersion, reader, response)
                14 -> handleSyncGroup(apiVersion, reader, response)
                17 -> handleSaslHandshake(apiVersion, reader, response)
                36 -> handleSaslAuthenticate(apiVersion, reader, response)
                else -> {
                    logger.warning("Unsupported Kafka API Key: $apiKey")
                    close()
                }
            }
        }

        private fun handleApiVersions(apiVersion: Short, response: KafkaBufferWriter) {
            response.writeShort(0) // No error (0)

            // API Keys Array
            response.writeInt(14) // We support 14 APIs

            // Produce
            response.writeShort(0)  // key
            response.writeShort(0)  // min version
            response.writeShort(7)  // max version

            // Fetch
            response.writeShort(1)  // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // ListOffsets
            response.writeShort(2)  // key
            response.writeShort(0)  // min version
            response.writeShort(3)  // max version

            // Metadata
            response.writeShort(3)  // key
            response.writeShort(0)  // min version
            response.writeShort(1)  // max version (Support V0/V1)

            // OffsetCommit
            response.writeShort(8)  // key
            response.writeShort(0)  // min version
            response.writeShort(7)  // max version

            // OffsetFetch
            response.writeShort(9)  // key
            response.writeShort(0)  // min version
            response.writeShort(1)  // max version

            // FindCoordinator
            response.writeShort(10) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // JoinGroup
            response.writeShort(11) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // Heartbeat
            response.writeShort(12) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // LeaveGroup
            response.writeShort(13) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // SyncGroup
            response.writeShort(14) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // SaslHandshake
            response.writeShort(17) // key
            response.writeShort(0)  // min version
            response.writeShort(1)  // max version

            // ApiVersions
            response.writeShort(18) // key
            response.writeShort(0)  // min version
            response.writeShort(0)  // max version

            // SaslAuthenticate
            response.writeShort(36) // key
            response.writeShort(0)  // min version
            response.writeShort(1)  // max version

            if (apiVersion.toInt() >= 1) {
                response.writeInt(0) // Throttle time ms
            }

            writeResponse(response)
        }

        private fun handleMetadata(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            // Read requested topics
            val topicCount = reader.readInt()
            val requestedTopics = mutableListOf<String>()
            for (i in 0 until topicCount) {
                requestedTopics.add(reader.readString() ?: "")
            }

            // Brokers list
            response.writeInt(1) // 1 Broker
            response.writeInt(0) // Broker ID: 0
            response.writeString(advertisedHost)
            response.writeInt(advertisedPort)
            if (apiVersion.toInt() >= 1) {
                response.writeString(null) // Rack (null)
            }

            if (apiVersion.toInt() >= 1) {
                response.writeInt(0) // Controller ID
            }

            // If requestedTopics is empty, return all configured topic streams
            val topicsToReturn = if (requestedTopics.isEmpty()) {
                configuredTopics.toList()
            } else {
                requestedTopics.filter { it in configuredTopics }
            }

            response.writeInt(topicsToReturn.size)
            topicsToReturn.forEach { topic ->
                response.writeShort(0) // No error
                response.writeString(topic)
                if (apiVersion.toInt() >= 1) {
                    response.writeByte(0) // Is internal: false
                }

                // Partitions list (always exactly Partition 0)
                response.writeInt(1)
                response.writeShort(0) // Partition error code: 0
                response.writeInt(0)  // Partition ID: 0
                response.writeInt(0)  // Leader ID: 0

                // Replicas
                response.writeInt(1)
                response.writeInt(0)

                // ISR
                response.writeInt(1)
                response.writeInt(0)
            }

            writeResponse(response)
        }

        private fun handleFindCoordinator(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString()
            if (apiVersion.toInt() >= 1) {
                val coordinatorType = reader.readByte()
            }
            logger.fine { "Kafka client requesting coordinator for group: $groupId (version $apiVersion)" }

            if (apiVersion.toInt() >= 1) {
                response.writeInt(0) // throttleTimeMs
            }
            response.writeShort(0) // No error

            // Coordinator Broker
            response.writeInt(0) // Broker ID: 0
            response.writeString(advertisedHost)
            response.writeInt(advertisedPort)

            writeResponse(response)
        }

        private fun handleJoinGroup(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString()
            val sessionTimeout = reader.readInt()
            val memberId = reader.readString() ?: ""
            val protocolType = reader.readString()
            val protocolCount = reader.readInt()

            var selectedProtocol = "range"
            var clientMetadata: ByteArray? = null

            for (i in 0 until protocolCount) {
                val protocolName = reader.readString() ?: ""
                val protocolMetadata = reader.readBytes()
                if (i == 0) {
                    selectedProtocol = protocolName
                    clientMetadata = protocolMetadata
                }
            }

            logger.fine { "JoinGroup request from client '$memberId' in group '$groupId' with protocol '$selectedProtocol'" }

            // Reply JoinGroupResponse V0
            response.writeShort(0) // error_code: 0 (No error)
            response.writeInt(1) // generation_id: 1
            response.writeString(selectedProtocol) // group_protocol

            val assignedMemberId = if (memberId.isEmpty()) "member_" + java.util.UUID.randomUUID().toString() else memberId
            response.writeString(assignedMemberId) // leader_id
            response.writeString(assignedMemberId) // member_id

            // Members array
            response.writeInt(1) // exactly 1 member (the current caller)
            response.writeString(assignedMemberId) // member_id
            response.writeBytes(clientMetadata) // member_metadata

            writeResponse(response)
        }

        private fun handleSyncGroup(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString()
            val generationId = reader.readInt()
            val memberId = reader.readString()
            val assignmentCount = reader.readInt()

            var myAssignment: ByteArray? = null

            for (i in 0 until assignmentCount) {
                val assignedMemberId = reader.readString()
                val memberAssignment = reader.readBytes()
                if (assignedMemberId == memberId) {
                    myAssignment = memberAssignment
                }
            }

            logger.fine { "SyncGroup request from client '$memberId' in group '$groupId'" }

            // Reply SyncGroupResponse V0
            response.writeShort(0) // error_code: 0 (No error)
            if (myAssignment == null) {
                response.writeBytes(ByteArray(0))
            } else {
                response.writeBytes(myAssignment)
            }

            writeResponse(response)
        }

        private fun handleHeartbeat(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString()
            val generationId = reader.readInt()
            val memberId = reader.readString()

            logger.finest { "Heartbeat request from client '$memberId' in group '$groupId'" }

            // Reply HeartbeatResponse V0
            response.writeShort(0) // error_code: 0 (No error)
            writeResponse(response)
        }

        private fun handleLeaveGroup(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString()
            val memberId = reader.readString()

            logger.fine { "LeaveGroup request from client '$memberId' in group '$groupId'" }

            // Reply LeaveGroupResponse V0
            response.writeShort(0) // error_code: 0 (No error)
            writeResponse(response)
        }

        private fun handleListOffsets(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val replicaId = reader.readInt()
            
            var isolationLevel: Byte = 0
            if (apiVersion.toInt() >= 2) {
                isolationLevel = reader.readByte()
            }
            
            val topicCount = reader.readInt()

            val futures = mutableListOf<FutureOffsetResult>()

            for (i in 0 until topicCount) {
                val topic = reader.readString() ?: ""
                val partitionCount = reader.readInt()
                for (j in 0 until partitionCount) {
                    val partition = reader.readInt()
                    val timestamp = reader.readLong()
                    
                    // maxNumOffsets is ONLY present in version 0
                    if (apiVersion.toInt() == 0) {
                        val maxNumOffsets = reader.readInt()
                    }

                    val promise = Promise.promise<Pair<String, Long>>()
                    futures.add(FutureOffsetResult(topic, partition, promise.future()))

                    if (timestamp == -1L) {
                        // Requesting high watermark (latest offset)
                        kafkaStore.getLatestOffset(topic).onComplete { ar ->
                            if (ar.succeeded()) {
                                promise.complete(Pair(topic, ar.result() + 1)) // Next offset is max + 1
                            } else {
                                promise.complete(Pair(topic, 0L))
                            }
                        }
                    } else {
                        // Requesting low watermark (earliest offset)
                        kafkaStore.getEarliestOffset(topic).onComplete { ar ->
                            if (ar.succeeded()) {
                                promise.complete(Pair(topic, ar.result()))
                            } else {
                                promise.complete(Pair(topic, 0L))
                            }
                        }
                    }
                }
            }

            // Wait for all database offsets to resolve
            io.vertx.core.Future.all(futures.map { it.future }).onComplete { ar ->
                if (apiVersion.toInt() >= 2) {
                    response.writeInt(0) // throttleTimeMs = 0
                }

                response.writeInt(topicCount)
                futures.forEach { result ->
                    response.writeString(result.topic)
                    response.writeInt(1) // 1 partition

                    response.writeInt(result.partition)
                    response.writeShort(0) // No error

                    val offsetVal = if (ar.succeeded()) {
                        val matchingResult = futures.find { it.topic == result.topic && it.partition == result.partition }
                        matchingResult?.future?.result()?.second ?: 0L
                    } else 0L

                    if (apiVersion.toInt() == 0) {
                        // Offsets array (V0)
                        response.writeInt(1)
                        response.writeLong(offsetVal)
                    } else {
                        // Timestamp and offset (V1+)
                        response.writeLong(-1L) // Timestamp
                        response.writeLong(offsetVal)
                    }
                }
                writeResponse(response)
            }
        }


        private fun handleOffsetCommit(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString() ?: "default-group"

            var generationId = -1
            var memberId = ""
            if (apiVersion.toInt() >= 1) {
                generationId = reader.readInt()
                memberId = reader.readString() ?: ""
            }

            var groupInstanceId: String? = null
            if (apiVersion.toInt() >= 7) {
                groupInstanceId = reader.readString()
            }

            var retentionTimeMs: Long = -1L
            if (apiVersion.toInt() in 2..4) {
                retentionTimeMs = reader.readLong()
            }

            val topicCount = reader.readInt()

            val commitFutures = mutableListOf<io.vertx.core.Future<Void>>()
            val topics = mutableListOf<Pair<String, List<Int>>>()

            for (i in 0 until topicCount) {
                val topic = reader.readString() ?: ""
                val partitionCount = reader.readInt()
                val partitions = mutableListOf<Int>()
                for (j in 0 until partitionCount) {
                    val partition = reader.readInt()
                    val offset = reader.readLong()

                    if (apiVersion.toInt() >= 6) {
                        val committedLeaderEpoch = reader.readInt()
                    }

                    val metadata = reader.readString()

                    partitions.add(partition)
                    commitFutures.add(kafkaStore.commitOffset(groupId, topic, partition, offset))
                }
                topics.add(Pair(topic, partitions))
            }

            io.vertx.core.Future.all(commitFutures).onComplete { ar ->
                if (apiVersion.toInt() >= 3) {
                    response.writeInt(0) // throttleTimeMs = 0
                }
                response.writeInt(topics.size)
                for (topicPair in topics) {
                    val topic = topicPair.first
                    val partitions = topicPair.second

                    response.writeString(topic)
                    response.writeInt(partitions.size)
                    for (partition in partitions) {
                        response.writeInt(partition)
                        if (ar.succeeded()) {
                            response.writeShort(0) // Success error code
                        } else {
                            response.writeShort(1) // Unknown error code
                        }
                    }
                }
                writeResponse(response)
            }
        }

        private fun handleOffsetFetch(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val groupId = reader.readString() ?: "default-group"
            val topicCount = reader.readInt()

            val fetchFutures = mutableListOf<io.vertx.core.Future<Pair<Int, Long?>>>()
            val topics = mutableListOf<Pair<String, List<Int>>>()

            for (i in 0 until topicCount) {
                val topic = reader.readString() ?: ""
                val partitionCount = reader.readInt()
                val partitions = mutableListOf<Int>()
                for (j in 0 until partitionCount) {
                    val partition = reader.readInt()
                    partitions.add(partition)

                    val promise = Promise.promise<Pair<Int, Long?>>()
                    kafkaStore.getOffset(groupId, topic, partition).onComplete { ar ->
                        if (ar.succeeded()) {
                            promise.complete(Pair(partition, ar.result()))
                        } else {
                            promise.complete(Pair(partition, null))
                        }
                    }
                    fetchFutures.add(promise.future())
                }
                topics.add(Pair(topic, partitions))
            }

            io.vertx.core.Future.all(fetchFutures).onComplete { ar ->
                response.writeInt(topics.size)
                var futureIndex = 0
                for (topicPair in topics) {
                    val topic = topicPair.first
                    val partitions = topicPair.second

                    response.writeString(topic)
                    response.writeInt(partitions.size)

                    for (partition in partitions) {
                        response.writeInt(partition)

                        val offset = if (ar.succeeded()) {
                            fetchFutures[futureIndex].result().second ?: -1L
                        } else -1L
                        futureIndex++

                        response.writeLong(offset)
                        response.writeString("") // Metadata (empty)
                        response.writeShort(0) // No error
                    }
                }
                writeResponse(response)
            }
        }

        private fun handleProduce(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter, clientId: String?) {
            var transactionalId: String? = null
            if (apiVersion.toInt() >= 3) {
                transactionalId = reader.readString()
            }
            val acks = reader.readShort()
            val timeout = reader.readInt()
            val topicCount = reader.readInt()

            val messagesToEnqueue = mutableListOf<BrokerMessage>()
            val responseTopics = mutableListOf<Pair<String, List<Pair<Int, Short>>>>()

            for (i in 0 until topicCount) {
                val topic = reader.readString() ?: ""
                val partitionCount = reader.readInt()
                val partitionResponses = mutableListOf<Pair<Int, Short>>()

                for (j in 0 until partitionCount) {
                    val partition = reader.readInt()
                    val messageSetSize = reader.readInt()
                    var partitionError: Short = 0
                    val partitionMessages = mutableListOf<BrokerMessage>()

                    if (messageSetSize > 0) {
                        val messageSetBytes = reader.readRawBytes(messageSetSize)
                        val magic = if (messageSetBytes.size >= 17) messageSetBytes[16] else 0.toByte()

                        if (magic == 2.toByte()) {
                            // Parse RecordBatch (Magic 2)
                            val batchReader = KafkaBufferReader(Buffer.buffer(messageSetBytes))
                            while (batchReader.hasRemaining(61) && partitionError == 0.toShort()) {
                                val baseOffset = batchReader.readLong()
                                val batchLength = batchReader.readInt()
                                val partitionLeaderEpoch = batchReader.readInt()
                                val magicVal = batchReader.readByte()
                                val crc = batchReader.readInt()
                                val attributes = batchReader.readShort()
                                val lastOffsetDelta = batchReader.readInt()
                                val baseTimestamp = batchReader.readLong()
                                val maxTimestamp = batchReader.readLong()
                                val producerId = batchReader.readLong()
                                val producerEpoch = batchReader.readShort()
                                val baseSequence = batchReader.readInt()
                                val recordsCount = batchReader.readInt()

                                val compression = attributes.toInt() and 0x07
                                if (compression == 1) { // GZIP
                                    val compressedLen = batchLength - 49
                                    if (compressedLen > 0 && batchReader.hasRemaining(compressedLen)) {
                                        val compressedBytes = batchReader.readRawBytes(compressedLen)
                                        val decompressedBytes = try {
                                            decompressGzip(compressedBytes)
                                        } catch (e: Exception) {
                                            logger.severe("Failed to decompress GZIP record batch: ${e.message}")
                                            partitionError = 1 // UnknownServerException
                                            null
                                        }
                                        
                                        if (decompressedBytes != null) {
                                            val recordReader = KafkaBufferReader(Buffer.buffer(decompressedBytes))
                                            for (r in 0 until recordsCount) {
                                                if (recordReader.hasRemaining(1) && partitionError == 0.toShort()) {
                                                    val recordLen = recordReader.readVarint()
                                                    if (recordLen > 0 && recordReader.hasRemaining(recordLen)) {
                                                        val recordBytes = recordReader.readRawBytes(recordLen)
                                                        val innerRecordReader = KafkaBufferReader(Buffer.buffer(recordBytes))
                                                        
                                                        val recordAttributes = innerRecordReader.readByte()
                                                        val timestampDelta = innerRecordReader.readVarlong()
                                                        val offsetDelta = innerRecordReader.readVarint()
                                                        
                                                        var keyBytes: ByteArray? = null
                                                        val keyLen = innerRecordReader.readVarint()
                                                        if (keyLen > 0 && innerRecordReader.hasRemaining(keyLen)) {
                                                            keyBytes = innerRecordReader.readRawBytes(keyLen)
                                                        }
                                                        
                                                        val valueLen = innerRecordReader.readVarint()
                                                        if (valueLen > 0 && innerRecordReader.hasRemaining(valueLen)) {
                                                            val valueBytes = innerRecordReader.readRawBytes(valueLen)
                                                            val msgTopic = if (keyBytes != null) String(keyBytes, StandardCharsets.UTF_8) else topic
                                                            val authenticatedUser = username ?: "Anonymous"
                                                            val canPublish = !userManager.isUserManagementEnabled() || userManager.canPublish(authenticatedUser, msgTopic)
                                                            if (isWriteAllowed(topic, msgTopic) && canPublish) {
                                                                val brokerMsg = BrokerMessage(
                                                                    messageUuid = Utils.getUuid(),
                                                                    messageId = 0,
                                                                    topicName = msgTopic,
                                                                    payload = valueBytes,
                                                                    qosLevel = 1,
                                                                    isRetain = false,
                                                                    isDup = false,
                                                                    isQueued = false,
                                                                    clientId = clientId ?: "kafka-producer"
                                                                )
                                                                partitionMessages.add(brokerMsg)
                                                            } else {
                                                                logger.warning("Kafka producer write to topic '$topic' with key '$msgTopic' rejected: Not allowed by stream configuration or ACL permissions.")
                                                                partitionError = 29 // TopicAuthorizationFailedException
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if (compression == 0) { // Uncompressed
                                    for (r in 0 until recordsCount) {
                                        if (batchReader.hasRemaining(1) && partitionError == 0.toShort()) {
                                            val recordLen = batchReader.readVarint()
                                            if (recordLen > 0 && batchReader.hasRemaining(recordLen)) {
                                                val recordBytes = batchReader.readRawBytes(recordLen)
                                                val recordReader = KafkaBufferReader(Buffer.buffer(recordBytes))
                                                
                                                val recordAttributes = recordReader.readByte()
                                                val timestampDelta = recordReader.readVarlong()
                                                val offsetDelta = recordReader.readVarint()
                                                
                                                var keyBytes: ByteArray? = null
                                                val keyLen = recordReader.readVarint()
                                                if (keyLen > 0 && recordReader.hasRemaining(keyLen)) {
                                                    keyBytes = recordReader.readRawBytes(keyLen)
                                                }
                                                
                                                val valueLen = recordReader.readVarint()
                                                if (valueLen > 0 && recordReader.hasRemaining(valueLen)) {
                                                    val valueBytes = recordReader.readRawBytes(valueLen)
                                                    val msgTopic = if (keyBytes != null) String(keyBytes, StandardCharsets.UTF_8) else topic
                                                    val authenticatedUser = username ?: "Anonymous"
                                                    val canPublish = !userManager.isUserManagementEnabled() || userManager.canPublish(authenticatedUser, msgTopic)
                                                    if (isWriteAllowed(topic, msgTopic) && canPublish) {
                                                        val brokerMsg = BrokerMessage(
                                                            messageUuid = Utils.getUuid(),
                                                            messageId = 0,
                                                            topicName = msgTopic,
                                                            payload = valueBytes,
                                                            qosLevel = 1,
                                                            isRetain = false,
                                                            isDup = false,
                                                            isQueued = false,
                                                            clientId = clientId ?: "kafka-producer"
                                                        )
                                                        partitionMessages.add(brokerMsg)
                                                    } else {
                                                        logger.warning("Kafka producer write to topic '$topic' with key '$msgTopic' rejected: Not allowed by stream configuration or ACL permissions.")
                                                        partitionError = 29 // TopicAuthorizationFailedException
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    logger.warning("Unsupported Kafka compression codec in RecordBatch: $compression")
                                    partitionError = 13 // UnsupportedForMessageFormatOnBrokerException
                                }
                            }
                        } else {
                            // Parse Legacy MessageSet (Magic 0/1) recursively
                            parseLegacyMessageSet(topic, messageSetBytes, partitionMessages, clientId) { err ->
                                partitionError = err
                            }
                        }
                    }
                    if (partitionError != 0.toShort()) {
                        partitionResponses.add(Pair(partition, partitionError))
                    } else {
                        messagesToEnqueue.addAll(partitionMessages)
                        partitionResponses.add(Pair(partition, 0.toShort()))
                    }
                }
                responseTopics.add(Pair(topic, partitionResponses))
            }

            // Enqueue all extracted messages to kafkaStore
            if (messagesToEnqueue.isNotEmpty()) {
                kafkaStore.enqueue(messagesToEnqueue).onComplete { ar ->
                    if (ar.failed()) {
                        logger.warning("Failed to enqueue produced Kafka messages: ${ar.cause()?.message}")
                    } else {
                        // Also publish to MQTT sessionHandler so they are distributed to MQTT subscribers
                        messagesToEnqueue.forEach { msg ->
                            sessionHandler.publishMessage(msg)
                        }
                    }
                    
                    // Reply to Produce Request
                    buildProduceResponse(apiVersion, response, responseTopics)
                    writeResponse(response)
                }
            } else {
                buildProduceResponse(apiVersion, response, responseTopics)
                writeResponse(response)
            }
        }

        private fun decompressGzip(compressed: ByteArray): ByteArray {
            return java.io.ByteArrayInputStream(compressed).use { bais ->
                java.util.zip.GZIPInputStream(bais).use { gzis ->
                    gzis.readAllBytes()
                }
            }
        }

        private fun parseLegacyMessageSet(
            topic: String,
            messageSetBytes: ByteArray,
            partitionMessages: MutableList<BrokerMessage>,
            clientId: String?,
            onError: (Short) -> Unit
        ) {
            val messageSetReader = KafkaBufferReader(Buffer.buffer(messageSetBytes))
            var partitionError: Short = 0
            
            while (messageSetReader.hasRemaining(12) && partitionError == 0.toShort()) { // offset (8) + size (4)
                val offset = messageSetReader.readLong()
                val msgSize = messageSetReader.readInt()
                if (msgSize > 0 && messageSetReader.hasRemaining(msgSize)) {
                    val msgBytes = messageSetReader.readRawBytes(msgSize)
                    val msgReader = KafkaBufferReader(Buffer.buffer(msgBytes))
                    val crc = msgReader.readInt()
                    val magicVal = msgReader.readByte()
                    val attributes = msgReader.readByte()
                    if (magicVal == 1.toByte()) {
                        val timestamp = msgReader.readLong()
                    }
                    
                    val compression = attributes.toInt() and 0x03
                    if (compression == 1) { // GZIP
                        val keyBytes = msgReader.readBytes()
                        val valueBytes = msgReader.readBytes()
                        if (valueBytes != null) {
                            val decompressedBytes = try {
                                decompressGzip(valueBytes)
                            } catch (e: Exception) {
                                logger.severe("Failed to decompress nested GZIP MessageSet: ${e.message}")
                                partitionError = 1
                                null
                            }
                            if (decompressedBytes != null) {
                                parseLegacyMessageSet(topic, decompressedBytes, partitionMessages, clientId, onError)
                            }
                        }
                    } else if (compression == 0) {
                        val keyBytes = msgReader.readBytes()
                        val valueBytes = msgReader.readBytes()
                        if (valueBytes != null) {
                            val msgTopic = if (keyBytes != null) String(keyBytes, StandardCharsets.UTF_8) else topic
                            val authenticatedUser = username ?: "Anonymous"
                            val canPublish = !userManager.isUserManagementEnabled() || userManager.canPublish(authenticatedUser, msgTopic)
                            if (isWriteAllowed(topic, msgTopic) && canPublish) {
                                val brokerMsg = BrokerMessage(
                                    messageUuid = Utils.getUuid(),
                                    messageId = 0,
                                    topicName = msgTopic,
                                    payload = valueBytes,
                                    qosLevel = 1,
                                    isRetain = false,
                                    isDup = false,
                                    isQueued = false,
                                    clientId = clientId ?: "kafka-producer"
                                )
                                partitionMessages.add(brokerMsg)
                            } else {
                                logger.warning("Kafka producer write to topic '$topic' with key '$msgTopic' rejected: Not allowed by stream configuration or ACL permissions.")
                                partitionError = 29 // TopicAuthorizationFailedException
                            }
                        }
                    } else {
                        logger.warning("Unsupported legacy compression codec: $compression")
                        partitionError = 13 // UnsupportedForMessageFormatOnBrokerException
                    }
                }
            }
            if (partitionError != 0.toShort()) {
                onError(partitionError)
            }
        }

        private fun buildProduceResponse(apiVersion: Short, response: KafkaBufferWriter, topics: List<Pair<String, List<Pair<Int, Short>>>>) {
            response.writeInt(topics.size)
            topics.forEach { topicPair ->
                val topic = topicPair.first
                val partitionResponses = topicPair.second
                
                response.writeString(topic)
                response.writeInt(partitionResponses.size)
                partitionResponses.forEach { (partition, errorCode) ->
                    response.writeInt(partition)
                    response.writeShort(errorCode)
                    response.writeLong(0L) // baseOffset
                    if (apiVersion.toInt() >= 2) {
                        response.writeLong(-1L) // logAppendTimeMs
                    }
                    if (apiVersion.toInt() >= 5) {
                        response.writeLong(0L) // logStartOffset
                    }
                }
            }
            if (apiVersion.toInt() >= 1) {
                response.writeInt(0) // throttleTimeMs
            }
        }

        private fun handleFetch(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val replicaId = reader.readInt()
            val maxWaitMs = reader.readInt()
            val minBytes = reader.readInt()
            
            var maxBytes = 0
            if (apiVersion.toInt() >= 3) {
                maxBytes = reader.readInt()
            }
            
            var isolationLevel: Byte = 0
            if (apiVersion.toInt() >= 4) {
                isolationLevel = reader.readByte()
            }
            
            var sessionId = 0
            var epoch = 0
            if (apiVersion.toInt() >= 7) {
                sessionId = reader.readInt()
                epoch = reader.readInt()
            }
            
            val topicCount = reader.readInt()

            val fetchFutures = mutableListOf<io.vertx.core.Future<FetchPartitionResult>>()

            for (i in 0 until topicCount) {
                val topic = reader.readString() ?: ""
                val partitionCount = reader.readInt()
                for (j in 0 until partitionCount) {
                    val partition = reader.readInt()
                    val fetchOffset = reader.readLong()
                    
                    if (apiVersion.toInt() >= 9) {
                        val currentLeaderEpoch = reader.readInt()
                    }
                    
                    val partitionMaxBytes = reader.readInt()

                    val promise = Promise.promise<FetchPartitionResult>()
                    val authenticatedUser = username ?: "Anonymous"
                    val canSubscribe = !userManager.isUserManagementEnabled() || userManager.canSubscribe(authenticatedUser, topic)

                    if (canSubscribe) {
                        val watermarkFuture = kafkaStore.getLatestOffset(topic)
                        val recordsFuture = kafkaStore.fetch(topic, fetchOffset, 100)

                        io.vertx.core.Future.all(watermarkFuture, recordsFuture).onComplete { ar ->
                            if (ar.succeeded()) {
                                val maxOffset = watermarkFuture.result()
                                val records = recordsFuture.result()
                                promise.complete(FetchPartitionResult(topic, partition, maxOffset, records))
                            } else {
                                promise.complete(FetchPartitionResult(topic, partition, 0L, emptyList()))
                            }
                        }
                    } else {
                        logger.warning("Kafka consumer read from topic '$topic' rejected: Not authorized by ACL.")
                        promise.complete(FetchPartitionResult(topic, partition, 0L, emptyList()))
                    }

                    fetchFutures.add(promise.future())
                }
            }

            io.vertx.core.Future.all(fetchFutures).onComplete { ar ->
                // Write throttleTimeMs if apiVersion >= 1
                if (apiVersion.toInt() >= 1) {
                    response.writeInt(0) // throttleTimeMs = 0
                }
                
                // Write sessionId/errorCode if apiVersion >= 7
                if (apiVersion.toInt() >= 7) {
                    response.writeShort(0) // errorCode = 0
                    response.writeInt(0) // sessionId = 0
                }

                response.writeInt(topicCount)
                fetchFutures.forEach { f ->
                    val result = f.result()
                    response.writeString(result.topic)
                    response.writeInt(1) // 1 partition

                    response.writeInt(result.partition)
                    response.writeShort(0) // No error
                    response.writeLong(result.highWatermark) // High watermark offset

                    // Serialize MessageSet V0 records
                    val messageSetWriter = KafkaBufferWriter()
                    result.records.forEach { (offset, message) ->
                        // Offset: INT64
                        messageSetWriter.writeLong(offset)

                        // Message Size: INT32
                        val payloadBytes = message.payload
                        val payloadSize = payloadBytes.size
                        val keyBytes = message.topicName.toByteArray(StandardCharsets.UTF_8)
                        val keyLen = keyBytes.size
                        val msgSize = 4 + 1 + 1 + (4 + keyLen) + (4 + payloadSize) // crc (4) + magic (1) + attr (1) + keyLen (4) + key + valLen (4) + payload
                        messageSetWriter.writeInt(msgSize)

                        // Calculate CRC32 of: Magic + Attributes + KeyLength + Key + ValueLength + Payload
                        val crcCalculator = CRC32()
                        val magic = 0.toByte()
                        val attr = 0.toByte()
                        
                        val crcBuffer = KafkaBufferWriter()
                        crcBuffer.writeByte(magic)
                        crcBuffer.writeByte(attr)
                        crcBuffer.writeInt(keyLen)
                        crcBuffer.writeRawBytes(keyBytes)
                        crcBuffer.writeInt(payloadSize)
                        crcBuffer.writeRawBytes(payloadBytes)
                        
                        crcCalculator.update(crcBuffer.toBuffer().bytes)
                        val crcVal = crcCalculator.value.toInt()

                        // Write message payload
                        messageSetWriter.writeInt(crcVal)
                        messageSetWriter.writeByte(magic)
                        messageSetWriter.writeByte(attr)
                        messageSetWriter.writeInt(keyLen)
                        messageSetWriter.writeRawBytes(keyBytes)
                        messageSetWriter.writeInt(payloadSize)
                        messageSetWriter.writeRawBytes(payloadBytes)
                    }

                    val serializedBytes = messageSetWriter.toBuffer()
                    response.writeInt(serializedBytes.length()) // Record set size
                    response.writeRawBytes(serializedBytes.bytes) // MessageSet payload
                }
                writeResponse(response)
            }
        }


        private fun writeResponse(payload: KafkaBufferWriter) {
            val responseBytes = payload.toBuffer()
            val completeResponse = Buffer.buffer()
            completeResponse.appendInt(responseBytes.length()) // 4-byte size prefix
            completeResponse.appendBuffer(responseBytes)

            socket.write(completeResponse)
        }

        private fun close() {
            if (!closed) {
                closed = true
                socket.close()
            }
        }

        private fun cleanup() {
            close()
        }

        private fun handleSaslHandshake(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val mechanism = reader.readString()
            logger.fine { "SaslHandshake: mechanism=$mechanism, version=$apiVersion" }
            saslHandshakeReceived = true
            authenticated = false // Reset authenticated since they initiated a SASL handshake

            response.writeShort(0) // No error (0)
            response.writeInt(1) // Number of mechanisms
            response.writeString("PLAIN")

            writeResponse(response)
        }

        private fun handleSaslAuthenticate(apiVersion: Short, reader: KafkaBufferReader, response: KafkaBufferWriter) {
            val saslAuthBytes = reader.readBytes()
            if (saslAuthBytes == null) {
                logger.warning("SaslAuthenticate: Empty auth token received")
                response.writeShort(58) // SASLAuthenticationFailed (58)
                response.writeString("Empty auth token")
                response.writeBytes(ByteArray(0))
                writeResponse(response)
                return
            }

            val credentials = parsePlainToken(saslAuthBytes)
            if (credentials == null) {
                logger.warning("SaslAuthenticate: Failed to parse plain token")
                response.writeShort(58) // SASLAuthenticationFailed (58)
                response.writeString("Failed to parse plain token")
                response.writeBytes(ByteArray(0))
                writeResponse(response)
                return
            }

            val (usernameVal, password) = credentials

            if (!userManager.isUserManagementEnabled()) {
                logger.info("Kafka SASL PLAIN client authenticated successfully (user: $usernameVal, user-management disabled)")
                username = usernameVal
                authenticated = true
                response.writeShort(0) // Success
                response.writeString(null) // No error message
                response.writeBytes(ByteArray(0))
                writeResponse(response)
                processInput() // Resume processing
                return
            }

            authenticating = true

            userManager.authenticate(usernameVal, password).onComplete { ar ->
                authenticating = false
                if (closed) return@onComplete

                if (ar.succeeded() && ar.result() != null) {
                    logger.info("Kafka SASL PLAIN client authenticated successfully as user: $usernameVal")
                    username = usernameVal
                    authenticated = true
                    response.writeShort(0) // Success
                    response.writeString(null) // No error message
                    response.writeBytes(ByteArray(0))
                } else {
                    logger.warning("Kafka SASL PLAIN authentication failed for user: $usernameVal")
                    response.writeShort(58) // SASLAuthenticationFailed
                    response.writeString("Authentication failed")
                    response.writeBytes(ByteArray(0))
                }
                writeResponse(response)

                if (authenticated) {
                    processInput() // Resume processing
                } else {
                    close() // Close connection on auth failure
                }
            }
        }

        private fun handleRawSaslPlainToken(packet: Buffer) {
            val credentials = parsePlainToken(packet.bytes)
            if (credentials == null) {
                logger.warning("Raw SASL PLAIN: Failed to parse plain token")
                close()
                return
            }

            val (usernameVal, password) = credentials

            if (!userManager.isUserManagementEnabled()) {
                logger.info("Kafka raw SASL PLAIN client authenticated successfully (user: $usernameVal, user-management disabled)")
                username = usernameVal
                authenticated = true
                
                // Reply with 4-byte length prefix of 0 (success)
                val successBuffer = Buffer.buffer()
                successBuffer.appendInt(0)
                socket.write(successBuffer)
                processInput() // Resume processing
                return
            }

            authenticating = true

            userManager.authenticate(usernameVal, password).onComplete { ar ->
                authenticating = false
                if (closed) return@onComplete

                if (ar.succeeded() && ar.result() != null) {
                    logger.info("Kafka raw SASL PLAIN client authenticated successfully as user: $usernameVal")
                    username = usernameVal
                    authenticated = true
                    
                    // Reply with 4-byte length prefix of 0 (success)
                    val successBuffer = Buffer.buffer()
                    successBuffer.appendInt(0)
                    socket.write(successBuffer)
                } else {
                    logger.warning("Kafka raw SASL PLAIN authentication failed for user: $usernameVal")
                    close()
                }

                if (authenticated) {
                    processInput() // Resume processing
                }
            }
        }

        private fun parsePlainToken(bytes: ByteArray): Pair<String, String>? {
            try {
                var i = 0
                // Skip authId (find first null byte)
                while (i < bytes.size && bytes[i] != 0.toByte()) {
                    i++
                }
                if (i >= bytes.size) return null
                i++ // skip the null byte
                
                // Read username
                val usernameStart = i
                while (i < bytes.size && bytes[i] != 0.toByte()) {
                    i++
                }
                if (i >= bytes.size) return null
                val usernameVal = String(bytes, usernameStart, i - usernameStart, StandardCharsets.UTF_8)
                i++ // skip the null byte
                
                // Read password
                val passwordStart = i
                val password = String(bytes, passwordStart, bytes.size - passwordStart, StandardCharsets.UTF_8)
                return Pair(usernameVal, password)
            } catch (e: Exception) {
                logger.warning("Error parsing SASL PLAIN token: ${e.message}")
                return null
            }
        }
    }

    // Helper classes for binary parsing and serialization
    private class KafkaBufferReader(private val buffer: Buffer) {
        private var position = 0

        fun readByte(): Byte {
            val valByte = buffer.getByte(position)
            position += 1
            return valByte
        }

        fun readShort(): Short {
            val valShort = buffer.getShort(position)
            position += 2
            return valShort
        }

        fun readInt(): Int {
            val valInt = buffer.getInt(position)
            position += 4
            return valInt
        }

        fun readLong(): Long {
            val valLong = buffer.getLong(position)
            position += 8
            return valLong
        }

        fun readString(): String? {
            val length = readShort()
            if (length.toInt() == -1) return null
            val stringBytes = buffer.getBytes(position, position + length)
            position += length
            return String(stringBytes, StandardCharsets.UTF_8)
        }

        fun readBytes(): ByteArray? {
            val length = readInt()
            if (length == -1) return null
            val bytes = buffer.getBytes(position, position + length)
            position += length
            return bytes
        }

        fun readRawBytes(length: Int): ByteArray {
            val bytes = buffer.getBytes(position, position + length)
            position += length
            return bytes
        }

        fun hasRemaining(size: Int = 1): Boolean {
            return position + size <= buffer.length()
        }

        fun readVarint(): Int {
            var value = 0
            var i = 0
            while (true) {
                val b = readByte().toInt()
                value = value or ((b and 0x7F) shl i)
                if ((b and 0x80) == 0) {
                    break
                }
                i += 7
            }
            return (value ushr 1) xor -(value and 1)
        }

        fun readVarlong(): Long {
            var value = 0L
            var i = 0
            while (true) {
                val b = readByte().toLong()
                value = value or ((b and 0x7FL) shl i)
                if ((b and 0x80L) == 0L) {
                    break
                }
                i += 7
            }
            return (value ushr 1) xor -(value and 1L)
        }
    }

    private class KafkaBufferWriter {
        private val buffer = Buffer.buffer()

        fun writeByte(value: Byte) {
            buffer.appendByte(value)
        }

        fun writeShort(value: Short) {
            buffer.appendShort(value)
        }

        fun writeInt(value: Int) {
            buffer.appendInt(value)
        }

        fun writeLong(value: Long) {
            buffer.appendLong(value)
        }

        fun writeString(value: String?) {
            if (value == null) {
                writeShort(-1)
            } else {
                val bytes = value.toByteArray(StandardCharsets.UTF_8)
                writeShort(bytes.size.toShort())
                buffer.appendBytes(bytes)
            }
        }

        fun writeBytes(value: ByteArray?) {
            if (value == null) {
                writeInt(-1)
            } else {
                writeInt(value.size)
                buffer.appendBytes(value)
            }
        }

        fun writeRawBytes(bytes: ByteArray) {
            buffer.appendBytes(bytes)
        }

        fun toBuffer(): Buffer {
            return buffer
        }
    }
}

private class FutureOffsetResult(
    val topic: String,
    val partition: Int,
    val future: io.vertx.core.Future<Pair<String, Long>>
)

private class FetchPartitionResult(
    val topic: String,
    val partition: Int,
    val highWatermark: Long,
    val records: List<Pair<Long, BrokerMessage>>
)

data class KafkaStreamConfig(
    val streamName: String,
    val topicFilter: String,
    val allowWrite: Boolean
)

