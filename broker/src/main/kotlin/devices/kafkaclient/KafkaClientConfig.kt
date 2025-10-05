package at.rocworks.devices.kafkaclient

import io.vertx.core.json.JsonObject

/**
 * Configuration classes for Kafka Subscriber device.
 *
 * NOTE: Explicit Kafka topic field removed. The Kafka topic to subscribe is now
 * derived from the owning DeviceConfig.namespace (see KafkaClientConnector).
 */
 data class KafkaClientConfig(
      val bootstrapServers: String = "localhost:9092",
      val groupId: String = "monstermq-subscriber",
      val payloadFormat: String = PayloadFormat.DEFAULT,
      val extraConsumerConfig: Map<String, String> = emptyMap(),
      val pollIntervalMs: Long = 500,
      val maxPollRecords: Int = 100,
       val reconnectDelayMs: Long = 5000,
       val destinationTopicPrefix: String? = null
  ) {
     companion object {
         fun fromJson(obj: JsonObject): KafkaClientConfig {
            // Legacy support: ignore any provided 'topic', 'keyTransform', or 'keyFallbackTopic' fields silently
               // destinationTopicPrefix normalization rules:
               // - Accept legacy 'keyTopicPrefix' as fallback if 'destinationTopicPrefix' absent
               // - Treat blank or whitespace-only value as disabled (null)
               // - Ensure trailing '/' when not null
               // - Forbid MQTT wildcards '+' and '#'
               val rawPrefix = obj.getString("destinationTopicPrefix") ?: obj.getString("keyTopicPrefix")
               val normalizedPrefix = rawPrefix?.trim()?.takeIf { it.isNotEmpty() }?.let { raw ->
                   if (raw.contains('+') || raw.contains('#')) {
                       throw IllegalArgumentException("destinationTopicPrefix must not contain MQTT wildcards + or #")
                   }
                   if (raw.endsWith('/')) raw else "$raw/"
               }
               return KafkaClientConfig(
                   bootstrapServers = obj.getString("bootstrapServers", "localhost:9092"),
                   groupId = obj.getString("groupId", "monstermq-subscriber"),
                   payloadFormat = obj.getString("payloadFormat", PayloadFormat.DEFAULT) ?: PayloadFormat.DEFAULT,
                   extraConsumerConfig = obj.getJsonObject("extraConsumerConfig", JsonObject()).let { ec -> ec.fieldNames().associateWith { k -> ec.getValue(k).toString() } },
                   pollIntervalMs = obj.getLong("pollIntervalMs", 500),
                   maxPollRecords = obj.getInteger("maxPollRecords", 100),
                    reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000),
                    destinationTopicPrefix = normalizedPrefix
               )
         }
     }

     fun validate(): List<String> {
         val errors = mutableListOf<String>()
         if (pollIntervalMs < 50) errors.add("pollIntervalMs should be >=50")
         if (maxPollRecords < 1) errors.add("maxPollRecords must be >=1")
          if (reconnectDelayMs < 500) errors.add("reconnectDelayMs should be >=500")
           if (destinationTopicPrefix != null) {
               if (destinationTopicPrefix.contains('+') || destinationTopicPrefix.contains('#')) {
                   errors.add("destinationTopicPrefix must not contain MQTT wildcards + or #")
               }
           }
           return errors
     }
 }

object PayloadFormat {
    const val DEFAULT = "DEFAULT" // internal encoded MqttMessage (binary form)
    const val JSON = "JSON"       // internal MqttMessage serialized as JSON text
    const val BINARY = "BINARY"   // plain Kafka record value bytes published as-is; topic = key (1:1), drop if key null
    const val TEXT = "TEXT"       // plain Kafka record value string (UTF-8) published; topic = key (1:1), drop if key null
}

