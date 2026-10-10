package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.stores.MessageStoreType
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.UUID
import java.util.concurrent.Callable

/**
 * NATS JetStream Key-Value facade for one native NATS connection.
 *
 * Buckets are archive groups with a last-value store: bucket `B` is the stream `KV_B` with subjects `$KV.B.>`,
 * key `a.b.c` is the MQTT topic `a/b/c`. Only the JetStream API subset the KV clients (`nats kv ...`, nats.go,
 * nats.py) use is answered: stream info/names/list, last-message get, KV put/delete/purge and ordered push
 * consumers for watch/keys. Revisions are synthesized from the message time (epoch milliseconds), history is 1.
 * A KV put is a retained MQTT publish; a delete or purge is an empty retained publish.
 */
class NatsJetStreamKv(private val client: NatsClient) {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private const val API_PREFIX = "\$JS.API."
        private const val KV_PREFIX = "\$KV."
        private const val STREAM_PREFIX = "KV_"
        private const val COUNT_LIMIT = 1_000_000
        private val BUCKET_NAME = Regex("^[a-zA-Z0-9_-]+$")

        fun isJetStreamSubject(subject: String): Boolean =
            subject.startsWith("\$JS.") || subject.startsWith(KV_PREFIX)

        fun revision(message: BrokerMessage): Long = message.time.toEpochMilli()

        private fun nanos(time: Instant): Long = time.epochSecond * 1_000_000_000L + time.nano

        private fun iso(time: Instant): String = DateTimeFormatter.ISO_INSTANT.format(time)
    }

    private inner class Consumer(
        val name: String,
        val bucket: String,
        val group: ArchiveGroup,
        val deliverSubject: String,
        val filters: List<String>,
        val headersOnly: Boolean,
        val config: JsonObject,
        val created: Instant
    ) {
        var deliveredSeq = 0L
        var lastStreamSeq = 0L
        var ready = false
        var bound = false
        var heartbeatMs = 0L
        var timerId: Long? = null
        var loaded = false
        var snapshot: List<BrokerMessage>? = null
        val waitingInfoReplies = mutableListOf<String>()
        val pendingLive = mutableListOf<BrokerMessage>()

        fun matches(topic: String): Boolean =
            filters.any { client.mqttTopicMatchesFilter(topic, it) } &&
                (group.topicFilter.isEmpty() || group.filterTree.isTopicNameMatching(topic))
    }

    private val consumers = mutableMapOf<String, Consumer>()

    // --- Entry points from NatsClient ---

    fun handlePublish(subject: String, replyTo: String?, headers: Map<String, String>, payload: ByteArray) {
        when {
            subject.startsWith(KV_PREFIX) -> handlePut(subject, replyTo, headers, payload)
            subject.startsWith(API_PREFIX) -> {
                if (replyTo == null) return // API requests without a reply subject cannot be answered
                handleApi(subject.removePrefix(API_PREFIX), replyTo, payload)
            }
            else -> logger.finer { "Ignoring JetStream subject [$subject]" } // e.g. \$JS.ACK and flow control replies
        }
    }

    fun onBrokerMessage(message: BrokerMessage) {
        if (consumers.isEmpty()) return
        for (consumer in consumers.values) {
            if (!consumer.matches(message.topicName)) continue
            if (consumer.ready) deliver(consumer, message, 0) else consumer.pendingLive.add(message)
        }
    }

    /** Called after a SUB: start consumers that were waiting for interest on their deliver subject. */
    fun onSubscribe() {
        consumers.values.filter { !it.ready }.forEach { startDelivery(it) }
    }

    /** Called after an UNSUB: drop consumers whose deliver subject is no longer subscribed. */
    fun onUnsubscribe() {
        consumers.values.filter { it.bound && client.sidsForSubject(it.deliverSubject).isEmpty() }.forEach { removeConsumer(it) }
    }

    fun close() {
        consumers.values.toList().forEach { removeConsumer(it) }
    }

    // --- Buckets ---

    private fun buckets(): Map<String, ArchiveGroup> =
        (Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()).filter { (name, group) ->
            BUCKET_NAME.matches(name) && group.lastValStore != null && group.getLastValType() != MessageStoreType.NONE
        }

    private fun bucketOfStream(stream: String): String? =
        if (stream.startsWith(STREAM_PREFIX)) stream.removePrefix(STREAM_PREFIX) else null

    private fun keyToTopic(key: String): String? {
        if (key.isEmpty() || key.startsWith('.') || key.endsWith('.') || key.contains("..")) return null
        if (key.contains('*') || key.contains('>')) return null
        return client.natsSubjectToMqttTopic(key)
    }

    private fun kvSubject(bucket: String, topic: String): String =
        "$KV_PREFIX$bucket.${client.mqttTopicToNatsSubject(topic)}"

    // --- Replies ---

    private fun reply(replyTo: String, json: JsonObject) {
        client.deliverLocal(replyTo, null, null, json.encode().toByteArray(Charsets.UTF_8))
    }

    private fun error(type: String?, code: Int, errCode: Int, description: String): JsonObject {
        val json = JsonObject()
        if (type != null) json.put("type", type)
        return json.put("error", JsonObject().put("code", code).put("err_code", errCode).put("description", description))
    }

    private fun streamNotFound(type: String) = error(type, 404, 10059, "stream not found")

    private fun <T> blocking(block: () -> T, done: (T?, Throwable?) -> Unit) {
        client.vertxInstance.executeBlocking(Callable { block() }).onComplete { ar ->
            if (ar.succeeded()) done(ar.result(), null) else done(null, ar.cause())
        }
    }

    private fun parseBody(payload: ByteArray): JsonObject =
        if (payload.isEmpty()) JsonObject() else try { JsonObject(String(payload, Charsets.UTF_8)) } catch (e: Exception) { JsonObject() }

    // --- JetStream API ---

    private fun handleApi(api: String, replyTo: String, payload: ByteArray) {
        logger.finer { "NATS JetStream API [$api] from [${client.clientIdentifier}]" }
        when {
            api == "INFO" -> accountInfo(replyTo)
            api.startsWith("STREAM.INFO.") -> streamInfo(api.removePrefix("STREAM.INFO."), replyTo)
            api == "STREAM.NAMES" -> streamNames(parseBody(payload), replyTo)
            api == "STREAM.LIST" -> streamList(parseBody(payload), replyTo)
            api.startsWith("STREAM.MSG.GET.") -> msgGet(api.removePrefix("STREAM.MSG.GET."), parseBody(payload), replyTo)
            api.startsWith("CONSUMER.CREATE.") -> consumerCreate(api.removePrefix("CONSUMER.CREATE."), parseBody(payload), replyTo)
            api.startsWith("CONSUMER.DURABLE.CREATE.") -> consumerCreate(api.removePrefix("CONSUMER.DURABLE.CREATE."), parseBody(payload), replyTo)
            api.startsWith("CONSUMER.DELETE.") -> consumerDelete(api.removePrefix("CONSUMER.DELETE."), replyTo)
            api.startsWith("CONSUMER.INFO.") -> consumerInfo(api.removePrefix("CONSUMER.INFO."), replyTo)
            else -> reply(replyTo, error(null, 400, 10025, "JetStream API [$api] is not supported by MonsterMQ; only Key-Value buckets backed by archive groups are available"))
        }
    }

    private fun accountInfo(replyTo: String) {
        val limits = JsonObject()
            .put("max_memory", -1).put("max_storage", -1).put("max_streams", -1).put("max_consumers", -1)
            .put("max_ack_pending", -1).put("memory_max_stream_bytes", -1).put("storage_max_stream_bytes", -1)
            .put("max_bytes_required", false)
        reply(replyTo, JsonObject()
            .put("type", "io.nats.jetstream.api.v1.account_info_response")
            .put("memory", 0).put("storage", 0).put("reserved_memory", 0).put("reserved_storage", 0)
            .put("streams", buckets().size).put("consumers", consumers.size)
            .put("limits", limits)
            .put("api", JsonObject().put("total", 0).put("errors", 0)))
    }

    private fun streamConfig(bucket: String, group: ArchiveGroup): JsonObject = JsonObject()
        .put("name", "$STREAM_PREFIX$bucket")
        .put("description", "MonsterMQ archive group $bucket")
        .put("subjects", JsonArray().add("$KV_PREFIX$bucket.>"))
        .put("retention", "limits")
        .put("max_consumers", -1).put("max_msgs", -1).put("max_bytes", -1).put("max_age", 0)
        .put("max_msgs_per_subject", 1)
        .put("max_msg_size", -1)
        .put("discard", "new")
        .put("storage", if (group.getLastValType() == MessageStoreType.MEMORY) "memory" else "file")
        .put("num_replicas", 1)
        .put("duplicate_window", 120_000_000_000L)
        .put("compression", "none")
        .put("allow_direct", false)
        .put("mirror_direct", false)
        .put("sealed", group.lastValReadOnly)
        .put("deny_delete", true)
        .put("deny_purge", false)
        .put("allow_rollup_hdrs", true)

    /** Builds the stream info; scans the last-value store on a worker thread because it may be a database. */
    private fun buildStreamInfo(bucket: String, group: ArchiveGroup, done: (JsonObject) -> Unit) {
        data class Stats(var messages: Int = 0, var bytes: Long = 0, var last: Instant? = null)
        blocking({
            val stats = Stats()
            group.lastValStore?.findMatchingMessages("#") { message ->
                if (message.payload.isNotEmpty()) {
                    stats.messages++
                    stats.bytes += message.payload.size
                    if (stats.last == null || message.time.isAfter(stats.last)) stats.last = message.time
                }
                stats.messages < COUNT_LIMIT
            }
            stats
        }) { result, err ->
            if (err != null) logger.warning("Scanning bucket [$bucket] failed: ${err.message}")
            val stats = result ?: Stats()
            val zero = "0001-01-01T00:00:00Z"
            val lastSeq = stats.last?.toEpochMilli() ?: 0L
            val state = JsonObject()
                .put("messages", stats.messages).put("bytes", stats.bytes)
                .put("first_seq", if (stats.messages > 0) 1 else 0).put("first_ts", zero)
                .put("last_seq", lastSeq).put("last_ts", stats.last?.let { iso(it) } ?: zero)
                .put("num_subjects", stats.messages)
                .put("consumer_count", consumers.values.count { it.bucket == bucket })
            done(JsonObject()
                .put("config", streamConfig(bucket, group))
                .put("created", zero)
                .put("state", state)
                .put("ts", iso(Instant.now())))
        }
    }

    private fun streamInfo(stream: String, replyTo: String) {
        val type = "io.nats.jetstream.api.v1.stream_info_response"
        val bucket = bucketOfStream(stream)
        val group = bucket?.let { buckets()[it] }
        if (bucket == null || group == null) {
            reply(replyTo, streamNotFound(type))
            return
        }
        buildStreamInfo(bucket, group) { info ->
            reply(replyTo, info.put("type", type).put("total", 0).put("offset", 0).put("limit", 0))
        }
    }

    /** Buckets selected by an optional subject filter of a STREAM.NAMES/STREAM.LIST request. */
    private fun selectBuckets(request: JsonObject): Map<String, ArchiveGroup> {
        val all = buckets()
        val subject = request.getString("subject")
        if (subject.isNullOrEmpty() || subject == ">" || subject == "$KV_PREFIX*.>" || subject == "$KV_PREFIX>") return all
        if (!subject.startsWith(KV_PREFIX)) return emptyMap()
        val bucket = subject.removePrefix(KV_PREFIX).substringBefore('.')
        return all.filterKeys { it == bucket }
    }

    private fun streamNames(request: JsonObject, replyTo: String) {
        val names = selectBuckets(request).keys.sorted().map { "$STREAM_PREFIX$it" }
        reply(replyTo, JsonObject()
            .put("type", "io.nats.jetstream.api.v1.stream_names_response")
            .put("total", names.size).put("offset", 0).put("limit", 1024)
            .put("streams", JsonArray(names)))
    }

    private fun streamList(request: JsonObject, replyTo: String) {
        val selected = selectBuckets(request).toSortedMap().toList()
        val infos = arrayOfNulls<JsonObject>(selected.size)
        var remaining = selected.size
        val finish = {
            reply(replyTo, JsonObject()
                .put("type", "io.nats.jetstream.api.v1.stream_list_response")
                .put("total", selected.size).put("offset", 0).put("limit", 256)
                .put("streams", JsonArray(infos.filterNotNull()))
                .put("missing", JsonArray()))
        }
        if (remaining == 0) { finish(); return }
        selected.forEachIndexed { index, (bucket, group) ->
            buildStreamInfo(bucket, group) { info ->
                infos[index] = info
                remaining--
                if (remaining == 0) finish()
            }
        }
    }

    private fun msgGet(stream: String, request: JsonObject, replyTo: String) {
        val type = "io.nats.jetstream.api.v1.stream_msg_get_response"
        val bucket = bucketOfStream(stream)
        val group = bucket?.let { buckets()[it] }
        if (bucket == null || group == null) {
            reply(replyTo, streamNotFound(type))
            return
        }
        val subject = request.getString("last_by_subj")
        val prefix = "$KV_PREFIX$bucket."
        if (subject == null || !subject.startsWith(prefix)) {
            // Lookups by sequence are not possible, revisions are synthesized
            reply(replyTo, error(type, 404, 10037, "no message found"))
            return
        }
        val topic = keyToTopic(subject.removePrefix(prefix))
        if (topic == null) {
            reply(replyTo, error(type, 400, 10052, "invalid key"))
            return
        }
        if (!client.canSubscribe(topic)) {
            reply(replyTo, error(type, 403, 10101, "Permissions Violation for Subscription to \"$subject\""))
            return
        }
        blocking({ group.lastValStore?.get(topic) }) { message, err ->
            if (err != null) {
                reply(replyTo, error(type, 500, 10010, "last value store error: ${err.message}"))
            } else if (message == null || message.payload.isEmpty()) {
                reply(replyTo, error(type, 404, 10037, "no message found"))
            } else {
                reply(replyTo, JsonObject()
                    .put("type", type)
                    .put("message", JsonObject()
                        .put("subject", subject)
                        .put("seq", revision(message))
                        .put("data", Base64.getEncoder().encodeToString(message.payload))
                        .put("time", iso(message.time))))
            }
        }
    }

    // --- KV put / delete ---

    private fun handlePut(subject: String, replyTo: String?, headers: Map<String, String>, payload: ByteArray) {
        val ack: (JsonObject) -> Unit = { json -> if (replyTo != null) reply(replyTo, json) }
        val rest = subject.removePrefix(KV_PREFIX)
        val bucket = rest.substringBefore('.', "")
        val group = buckets()[bucket]
        if (group == null) {
            ack(streamNotFound("io.nats.jetstream.api.v1.pub_ack_response"))
            return
        }
        val topic = keyToTopic(rest.substringAfter('.', ""))
        if (topic == null) {
            ack(error(null, 400, 10052, "invalid key"))
            return
        }
        if (group.topicFilter.isNotEmpty() && !group.filterTree.isTopicNameMatching(topic)) {
            ack(error(null, 400, 10052, "key [$topic] does not match the topic filter of archive group [$bucket]"))
            return
        }
        if (group.lastValReadOnly) {
            ack(error(null, 400, 10039, "last value store of archive group [$bucket] is read-only"))
            return
        }
        if (!client.canPublish(topic)) {
            ack(error(null, 403, 10101, "Permissions Violation for Publish to \"$subject\""))
            return
        }

        val operation = headers["KV-Operation"]
        val isDelete = operation == "DEL" || operation == "PURGE"
        val value = if (isDelete) ByteArray(0) else payload

        val publish = {
            val message = BrokerMessage(
                messageId = 0,
                topicName = topic,
                payload = value,
                qosLevel = 0,
                isRetain = true,
                isDup = false,
                isQueued = false,
                clientId = client.clientIdentifier
            )
            client.sessionHandlerInstance.publishMessage(message)
            ack(JsonObject().put("stream", "$STREAM_PREFIX$bucket").put("seq", revision(message)))
        }

        val expected = headers["Nats-Expected-Last-Subject-Sequence"]?.toLongOrNull()
        if (expected == null) {
            publish()
            return
        }
        // Optimistic concurrency (kv create / kv update): compare with the current synthesized revision
        blocking({ group.lastValStore?.get(topic) }) { current, err ->
            val currentRevision = current?.takeIf { it.payload.isNotEmpty() }?.let { revision(it) } ?: 0L
            if (err != null) {
                ack(error(null, 500, 10010, "last value store error: ${err.message}"))
            } else if (currentRevision != expected) {
                ack(error(null, 400, 10071, "wrong last sequence: $currentRevision"))
            } else {
                publish()
            }
        }
    }

    // --- Consumers (watch / keys) ---

    private fun consumerCreate(path: String, request: JsonObject, replyTo: String) {
        val type = "io.nats.jetstream.api.v1.consumer_create_response"
        // path is <stream>[.<consumer name>[.<filter subject>]]
        val tokens = path.split('.', limit = 3)
        val stream = request.getString("stream_name") ?: tokens[0]
        val bucket = bucketOfStream(stream)
        val group = bucket?.let { buckets()[it] }
        if (bucket == null || group == null) {
            reply(replyTo, streamNotFound(type))
            return
        }
        val config = request.getJsonObject("config") ?: JsonObject()
        val deliverSubject = config.getString("deliver_subject")
        if (deliverSubject.isNullOrEmpty()) {
            reply(replyTo, error(type, 400, 10025, "pull consumers are not supported by MonsterMQ, use a push consumer"))
            return
        }

        val prefix = "$KV_PREFIX$bucket."
        val filterSubjects = mutableListOf<String>()
        config.getString("filter_subject")?.takeIf { it.isNotEmpty() }?.let { filterSubjects.add(it) }
        config.getJsonArray("filter_subjects")?.forEach { (it as? String)?.let { f -> filterSubjects.add(f) } }
        if (filterSubjects.isEmpty()) filterSubjects.add("$prefix>")
        val filters = mutableListOf<String>()
        for (filterSubject in filterSubjects) {
            if (!filterSubject.startsWith(prefix) || filterSubject.length == prefix.length) {
                reply(replyTo, error(type, 400, 10093, "filter subject [$filterSubject] is not part of bucket [$bucket]"))
                return
            }
            val filter = client.natsSubjectToMqttTopic(filterSubject.removePrefix(prefix))
            if (!client.canSubscribe(filter)) {
                reply(replyTo, error(type, 403, 10101, "Permissions Violation for Subscription to \"$filterSubject\""))
                return
            }
            filters.add(filter)
        }

        val name = config.getString("name") ?: config.getString("durable_name")
            ?: tokens.getOrNull(1)?.takeIf { it.isNotEmpty() } ?: UUID.randomUUID().toString().replace("-", "").take(8)
        consumers[name]?.let { removeConsumer(it) }

        val deliverPolicy = config.getString("deliver_policy", "all")
        val startSeq = config.getLong("opt_start_seq", 0L)
        val heartbeatMs = config.getLong("idle_heartbeat", 0L) / 1_000_000L
        val consumer = Consumer(name, bucket, group, deliverSubject, filters,
            config.getBoolean("headers_only", false), config.copy().put("name", name), Instant.now())
        consumer.heartbeatMs = heartbeatMs
        consumers[name] = consumer

        // Subscribe for live updates first; they are buffered until the snapshot has been delivered
        filters.forEach { client.addKvTopic(it) }

        blocking({
            val snapshot = linkedMapOf<String, BrokerMessage>()
            if (deliverPolicy != "new") {
                val store = group.lastValStore
                for (filter in filters) {
                    store?.findMatchingMessages(filter) { message ->
                        if (message.payload.isNotEmpty() && revision(message) >= startSeq) snapshot[message.topicName] = message
                        true
                    }
                }
            }
            snapshot.values.toList()
        }) { snapshot, err ->
            if (consumers[name] !== consumer) return@blocking // deleted or replaced meanwhile
            if (err != null) {
                removeConsumer(consumer)
                reply(replyTo, error(type, 500, 10010, "last value store error: ${err.message}"))
                return@blocking
            }
            val messages = snapshot ?: emptyList()
            consumer.snapshot = messages
            consumer.loaded = true
            reply(replyTo, consumerInfoJson(consumer, type, messages.size.toLong()))
            startDelivery(consumer)
            // Info requests that arrived while the snapshot was loading are answered now, with the real pending count
            consumer.waitingInfoReplies.forEach { consumerInfoReply(consumer, it) }
            consumer.waitingInfoReplies.clear()
        }
    }

    /**
     * Push the snapshot and then live updates, but only once the deliver subject has a subscriber:
     * clients may subscribe to it after the consumer was created, and like nats-server we wait for interest.
     */
    private fun startDelivery(consumer: Consumer) {
        val messages = consumer.snapshot ?: return // snapshot still loading
        if (consumer.ready || client.sidsForSubject(consumer.deliverSubject).isEmpty()) return
        consumer.bound = true
        consumer.snapshot = null
        messages.forEachIndexed { index, message -> deliver(consumer, message, (messages.size - index - 1).toLong()) }
        consumer.ready = true
        consumer.pendingLive.forEach { deliver(consumer, it, 0) }
        consumer.pendingLive.clear()
        if (consumer.heartbeatMs > 0) {
            consumer.timerId = client.vertxInstance.setPeriodic(consumer.heartbeatMs) { sendHeartbeat(consumer) }
        }
    }

    private fun consumerInfoJson(consumer: Consumer, type: String, pending: Long): JsonObject = JsonObject()
        .put("type", type)
        .put("stream_name", "$STREAM_PREFIX${consumer.bucket}")
        .put("name", consumer.name)
        .put("created", iso(consumer.created))
        .put("config", consumer.config)
        .put("delivered", JsonObject().put("consumer_seq", consumer.deliveredSeq).put("stream_seq", consumer.lastStreamSeq))
        .put("ack_floor", JsonObject().put("consumer_seq", 0).put("stream_seq", 0))
        .put("num_ack_pending", 0)
        .put("num_redelivered", 0)
        .put("num_waiting", 0)
        .put("num_pending", pending)
        .put("push_bound", true)
        .put("ts", iso(Instant.now()))

    private fun consumerDelete(path: String, replyTo: String) {
        val type = "io.nats.jetstream.api.v1.consumer_delete_response"
        val consumer = consumers[path.substringAfter('.', "")]
        if (consumer == null) {
            reply(replyTo, error(type, 404, 10014, "consumer not found"))
            return
        }
        removeConsumer(consumer)
        reply(replyTo, JsonObject().put("type", type).put("success", true))
    }

    private fun consumerInfo(path: String, replyTo: String) {
        val type = "io.nats.jetstream.api.v1.consumer_info_response"
        val consumer = consumers[path.substringAfter('.', "")]
        if (consumer == null) {
            reply(replyTo, error(type, 404, 10014, "consumer not found"))
            return
        }
        if (!consumer.loaded) consumer.waitingInfoReplies.add(replyTo) else consumerInfoReply(consumer, replyTo)
    }

    private fun consumerInfoReply(consumer: Consumer, replyTo: String) {
        reply(replyTo, consumerInfoJson(consumer, "io.nats.jetstream.api.v1.consumer_info_response", (consumer.snapshot?.size ?: 0).toLong()))
    }

    private fun removeConsumer(consumer: Consumer) {
        if (consumers[consumer.name] === consumer) consumers.remove(consumer.name)
        consumer.timerId?.let { client.vertxInstance.cancelTimer(it) }
        consumer.timerId = null
        consumer.filters.forEach { client.removeKvTopic(it) }
    }

    /** Push one entry to the consumer's deliver subject with JetStream ack metadata in the reply subject. */
    private fun deliver(consumer: Consumer, message: BrokerMessage, pending: Long) {
        consumer.deliveredSeq++
        val streamSeq = revision(message)
        consumer.lastStreamSeq = streamSeq
        val subject = kvSubject(consumer.bucket, message.topicName)
        val ackSubject = "\$JS.ACK.$STREAM_PREFIX${consumer.bucket}.${consumer.name}.1.$streamSeq.${consumer.deliveredSeq}.${nanos(message.time)}.$pending"
        when {
            message.payload.isEmpty() ->
                client.deliverVia(consumer.deliverSubject, subject, ackSubject, formatHeaders(null, mapOf("KV-Operation" to "DEL")), ByteArray(0))
            consumer.headersOnly ->
                client.deliverVia(consumer.deliverSubject, subject, ackSubject, formatHeaders(null, mapOf("Nats-Msg-Size" to message.payload.size.toString())), ByteArray(0))
            else ->
                client.deliverVia(consumer.deliverSubject, subject, ackSubject, null, message.payload)
        }
    }

    private fun sendHeartbeat(consumer: Consumer) {
        val headers = formatHeaders("100 Idle Heartbeat", mapOf(
            "Nats-Last-Consumer" to consumer.deliveredSeq.toString(),
            "Nats-Last-Stream" to consumer.lastStreamSeq.toString()
        ))
        client.deliverVia(consumer.deliverSubject, consumer.deliverSubject, null, headers, ByteArray(0))
    }
}
