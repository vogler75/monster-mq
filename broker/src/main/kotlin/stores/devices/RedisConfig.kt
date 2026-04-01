package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Redis Client address configuration for subscriptions, publications, and key-value sync.
 *  mode = SUBSCRIBE  -> Redis channel -> MQTT topic  (inbound)
 *  mode = PUBLISH    -> MQTT topic    -> Redis channel (outbound)
 *  mode = KV_SYNC    -> Bidirectional key-value sync
 */
data class RedisClientAddress(
    val mode: String,                    // "SUBSCRIBE", "PUBLISH", or "KV_SYNC"
    val redisChannel: String,            // Redis channel/pattern (Pub/Sub) or key pattern (KV_SYNC)
    val mqttTopic: String,               // Local MQTT topic (supports wildcards: + and #)
    val qos: Int = 0,
    val usePatternSubscribe: Boolean = false,  // Use PSUBSCRIBE for glob patterns (Pub/Sub only)
    val usePatternMatch: Boolean = false,      // Use SCAN MATCH for wildcard key discovery (KV_SYNC only)
    val kvPollIntervalMs: Long = 0,            // Polling interval for KV_SYNC reads (0 = disabled)
    val removePath: Boolean = true             // Strip base path before wildcard and prepend target prefix
) {
    companion object {
        const val MODE_SUBSCRIBE = "SUBSCRIBE"
        const val MODE_PUBLISH = "PUBLISH"
        const val MODE_KV_SYNC = "KV_SYNC"

        fun fromJsonObject(json: JsonObject) = RedisClientAddress(
            mode = json.getString("mode"),
            redisChannel = json.getString("redisChannel"),
            mqttTopic = json.getString("mqttTopic"),
            qos = json.getInteger("qos", 0),
            usePatternSubscribe = json.getBoolean("usePatternSubscribe", false),
            usePatternMatch = json.getBoolean("usePatternMatch", false),
            kvPollIntervalMs = json.getLong("kvPollIntervalMs", 0L),
            removePath = json.getBoolean("removePath", true)
        )
    }

    fun toJsonObject(): JsonObject = JsonObject()
        .put("mode", mode)
        .put("redisChannel", redisChannel)
        .put("mqttTopic", mqttTopic)
        .put("qos", qos)
        .put("usePatternSubscribe", usePatternSubscribe)
        .put("usePatternMatch", usePatternMatch)
        .put("kvPollIntervalMs", kvPollIntervalMs)
        .put("removePath", removePath)

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (mode != MODE_SUBSCRIBE && mode != MODE_PUBLISH && mode != MODE_KV_SYNC)
            errors.add("mode must be '$MODE_SUBSCRIBE', '$MODE_PUBLISH', or '$MODE_KV_SYNC'")
        if (redisChannel.isBlank()) errors.add("redisChannel cannot be blank")
        if (mqttTopic.isBlank()) errors.add("mqttTopic cannot be blank")
        if (qos !in 0..2) errors.add("qos must be 0, 1, or 2")
        if (kvPollIntervalMs < 0) errors.add("kvPollIntervalMs must be >= 0")
        return errors
    }

    fun isSubscribe() = mode == MODE_SUBSCRIBE
    fun isPublish() = mode == MODE_PUBLISH
    fun isKvSync() = mode == MODE_KV_SYNC

    // -- Topic/channel translation helpers --

    /**
     * Convert an incoming Redis channel name to an MQTT topic.
     *
     * When [removePath] is true and the configured [redisChannel] contains wildcards,
     * the base path is stripped and the configured [mqttTopic] prefix is prepended.
     *
     * Redis uses : as a conventional separator; MQTT uses /.
     * Glob wildcards: * matches any sequence, ? matches single char.
     *
     * Example: redisChannel="sensor:*", mqttTopic="devices/redis/sensor/#", removePath=true
     *   incoming "sensor:temp" -> "devices/redis/sensor/temp"
     */
    fun redisToMqttTopic(incomingChannel: String): String {
        val hasWildcard = redisChannel.contains('*') || redisChannel.contains('?')

        if (removePath && hasWildcard) {
            val wildcardIdx = redisChannel.indexOfFirst { it == '*' || it == '?' }
            val basePath = redisChannel.substring(0, wildcardIdx).trimEnd(':')
            val suffix = if (basePath.isNotEmpty() && incomingChannel.startsWith(basePath)) {
                incomingChannel.substring(basePath.length).trimStart(':')
            } else if (basePath.isEmpty()) {
                incomingChannel
            } else {
                incomingChannel
            }
            val mqttBase = mqttTopic.replace(Regex("[+#].*"), "").trimEnd('/')
            val mqttSuffix = suffix.replace(':', '/')
            return if (mqttBase.isNotEmpty() && mqttSuffix.isNotEmpty()) "$mqttBase/$mqttSuffix"
            else if (mqttBase.isNotEmpty()) mqttBase
            else mqttSuffix
        }

        // Simple conversion: replace : with /
        return incomingChannel.replace(':', '/')
    }

    /**
     * Convert an incoming MQTT topic to a Redis channel/key name.
     *
     * Example: mqttTopic="devices/redis/#", redisChannel="sensor:*", removePath=true
     *   incoming "devices/redis/temp" -> "sensor:temp"
     */
    fun mqttToRedisChannel(incomingMqttTopic: String): String {
        val mqttHasWildcard = mqttTopic.contains('#') || mqttTopic.contains('+')

        if (removePath && mqttHasWildcard) {
            val wildcardIdx = mqttTopic.indexOfFirst { it == '#' || it == '+' }
            val basePath = mqttTopic.substring(0, wildcardIdx).trimEnd('/')
            val suffix = if (basePath.isNotEmpty() && incomingMqttTopic.startsWith(basePath)) {
                incomingMqttTopic.substring(basePath.length).trimStart('/')
            } else if (basePath.isEmpty()) {
                incomingMqttTopic
            } else {
                incomingMqttTopic
            }
            val redisBase = redisChannel.replace(Regex("[*?].*"), "").trimEnd(':')
            val redisSuffix = suffix.replace('/', ':')
            return if (redisBase.isNotEmpty() && redisSuffix.isNotEmpty()) "$redisBase:$redisSuffix"
            else if (redisBase.isNotEmpty()) redisBase
            else redisSuffix
        }

        // Simple conversion: replace / with :
        return incomingMqttTopic.replace('/', ':')
    }
}

/**
 * Redis Client connection and data-flow configuration.
 */
data class RedisClientConfig(
    // -- Connection --
    val host: String = "localhost",
    val port: Int = 6379,
    val password: String? = null,
    val database: Int = 0,
    val useSsl: Boolean = false,
    val sslTrustAll: Boolean = false,
    val connectionString: String? = null,   // Optional full connection string override
    val maxPoolSize: Int = 6,
    val reconnectDelayMs: Long = 5000,
    val maxReconnectAttempts: Int = -1,      // -1 = unlimited
    val loopPrevention: Boolean = true,
    // -- Addresses --
    val addresses: List<RedisClientAddress> = emptyList()
) {
    companion object {
        fun fromJson(json: JsonObject): RedisClientConfig {
            val addresses = try {
                json.getJsonArray("addresses")?.map {
                    RedisClientAddress.fromJsonObject(it as JsonObject)
                } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }

            return RedisClientConfig(
                host = json.getString("host", "localhost"),
                port = json.getInteger("port", 6379),
                password = json.getString("password"),
                database = json.getInteger("database", 0),
                useSsl = json.getBoolean("useSsl", false),
                sslTrustAll = json.getBoolean("sslTrustAll", false),
                connectionString = json.getString("connectionString"),
                maxPoolSize = json.getInteger("maxPoolSize", 6),
                reconnectDelayMs = json.getLong("reconnectDelayMs", 5000L),
                maxReconnectAttempts = json.getInteger("maxReconnectAttempts", -1),
                loopPrevention = json.getBoolean("loopPrevention", true),
                addresses = addresses
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val addressesArray = JsonArray()
        addresses.forEach { addressesArray.add(it.toJsonObject()) }
        return JsonObject()
            .put("host", host)
            .put("port", port)
            .put("password", password)
            .put("database", database)
            .put("useSsl", useSsl)
            .put("sslTrustAll", sslTrustAll)
            .put("connectionString", connectionString)
            .put("maxPoolSize", maxPoolSize)
            .put("reconnectDelayMs", reconnectDelayMs)
            .put("maxReconnectAttempts", maxReconnectAttempts)
            .put("loopPrevention", loopPrevention)
            .put("addresses", addressesArray)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (connectionString == null) {
            if (host.isBlank()) errors.add("host cannot be blank")
            if (port !in 1..65535) errors.add("port must be between 1 and 65535")
        }
        if (database < 0) errors.add("database must be >= 0")
        if (maxPoolSize < 1) errors.add("maxPoolSize must be >= 1")
        addresses.forEachIndexed { i, a -> errors.addAll(a.validate().map { "address[$i]: $it" }) }
        return errors
    }

    /**
     * Build the Redis connection string from individual fields or return the override.
     */
    fun buildConnectionString(): String {
        if (!connectionString.isNullOrBlank()) return connectionString
        val scheme = if (useSsl) "rediss" else "redis"
        val authPart = if (!password.isNullOrBlank()) ":${password}@" else ""
        return "$scheme://${authPart}${host}:${port}/${database}"
    }
}
