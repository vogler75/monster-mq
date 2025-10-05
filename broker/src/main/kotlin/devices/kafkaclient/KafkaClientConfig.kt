package at.rocworks.devices.kafkaclient

import io.vertx.core.json.JsonObject

/**
 * Configuration classes for Kafka Subscriber device
 */
data class KafkaClientConfig(
    val bootstrapServers: String = "localhost:9092",
    val topic: String,
    val groupId: String = "monstermq-subscriber",
    val clientId: String = "",
    val qos: Int = 0,
    val retain: Boolean = false,
    val payloadFormat: String = PayloadFormat.DEFAULT,
    val keyTransform: KeyTransformConfig = KeyTransformConfig(),
    val keyFallbackTopic: String? = null,
    val extraConsumerConfig: Map<String, String> = emptyMap(),
    val pollIntervalMs: Long = 500,
    val maxPollRecords: Int = 100,
    val reconnectDelayMs: Long = 5000
) { 
    companion object {
        fun fromJson(obj: JsonObject): KafkaClientConfig {
            val keyTransform = obj.getJsonObject("keyTransform", JsonObject())
            return KafkaClientConfig(
                bootstrapServers = obj.getString("bootstrapServers", "localhost:9092"),
                topic = obj.getString("topic"),
                groupId = obj.getString("groupId", "monstermq-subscriber"),
                clientId = obj.getString("clientId", obj.getString("groupId", "monstermq-subscriber")), 
                qos = obj.getInteger("qos", 0),
                retain = obj.getBoolean("retain", false),
                payloadFormat = obj.getString("payloadFormat", PayloadFormat.DEFAULT) ?: PayloadFormat.DEFAULT,
                keyTransform = KeyTransformConfig.fromJson(keyTransform),
                keyFallbackTopic = obj.getString("keyFallbackTopic"),
                extraConsumerConfig = obj.getJsonObject("extraConsumerConfig", JsonObject()).let { ec -> ec.fieldNames().associateWith { k -> ec.getValue(k).toString() } },
                pollIntervalMs = obj.getLong("pollIntervalMs", 500),
                maxPollRecords = obj.getInteger("maxPollRecords", 100),
                reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000)
            )
        }
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (topic.isBlank()) errors.add("topic cannot be blank")
        if (qos !in 0..2) errors.add("qos must be 0,1,2")
        if (pollIntervalMs < 50) errors.add("pollIntervalMs should be >=50")
        if (maxPollRecords < 1) errors.add("maxPollRecords must be >=1")
        if (reconnectDelayMs < 500) errors.add("reconnectDelayMs should be >=500")
        errors.addAll(keyTransform.validate())
        return errors
    }
}

object PayloadFormat {
    const val DEFAULT = "DEFAULT" // internal encoded MqttMessage (binary form)
    const val JSON = "JSON"       // internal MqttMessage serialized as JSON text
    const val BINARY = "BINARY"   // plain Kafka record value bytes published as-is; topic = key (1:1), drop if key null
    const val TEXT = "TEXT"       // plain Kafka record value string (UTF-8) published; topic = key (1:1), drop if key null
}

data class KeyTransformConfig(
    val mode: String = Mode.NONE,
    val prefix: String = "",
    val regex: String = "",
    val replacement: String = ""
) {
    object Mode { const val NONE = "NONE"; const val KEY_PREFIX = "KEY_PREFIX"; const val REGEX = "REGEX" }

    companion object {
        fun fromJson(obj: JsonObject): KeyTransformConfig = KeyTransformConfig(
            mode = obj.getString("mode", Mode.NONE),
            prefix = obj.getString("prefix", ""),
            regex = obj.getString("regex", ""),
            replacement = obj.getString("replacement", "")
        )
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        when (mode) {
            Mode.KEY_PREFIX -> if (prefix.isBlank()) errors.add("prefix required for KEY_PREFIX mode")
            Mode.REGEX -> if (regex.isBlank()) errors.add("regex required for REGEX mode")
        }
        return errors
    }
}

object KafkaClientKeyTopicTransformer {
    fun transform(key: String?, cfg: KeyTransformConfig, fallback: String?): String? {
        if (key == null) return fallback
        return when (cfg.mode) {
            KeyTransformConfig.Mode.NONE -> key
            KeyTransformConfig.Mode.KEY_PREFIX -> cfg.prefix + key
            KeyTransformConfig.Mode.REGEX -> {
                val pattern = try { Regex(cfg.regex) } catch (e: Exception) { return null }
                val result = pattern.replace(key, cfg.replacement)
                if (result == key) null else result // if unchanged treat as no-match -> drop
            }
            else -> key
        }
    }
}