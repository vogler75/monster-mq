package at.rocworks.bus

import io.vertx.core.json.JsonObject

/**
 * Helper class to build Kafka producer and consumer configurations
 * by merging user-provided config with required defaults.
 */
object KafkaConfigBuilder {

    /**
     * Build Kafka producer configuration.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param userConfig User-provided configuration from YAML (Kafka.Config section)
     * @return Merged producer configuration
     */
    fun buildProducerConfig(bootstrapServers: String, userConfig: JsonObject?): Map<String, String> {
        val config = mutableMapOf<String, String>()

        // Required defaults that cannot be overridden
        config["bootstrap.servers"] = bootstrapServers
        config["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        config["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"

        // Apply user configuration (can override defaults)
        userConfig?.forEach { entry ->
            config[entry.key] = entry.value.toString()
        }

        // Set fallback defaults if not provided by user
        config.putIfAbsent("acks", "1")
        config.putIfAbsent("retries", "3")
        config.putIfAbsent("retry.backoff.ms", "1000")
        config.putIfAbsent("max.block.ms", "5000")

        return config
    }

    /**
     * Build Kafka consumer configuration.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId Consumer group ID
     * @param userConfig User-provided configuration from YAML (Kafka.Config section)
     * @return Merged consumer configuration
     */
    fun buildConsumerConfig(bootstrapServers: String, groupId: String, userConfig: JsonObject?): Map<String, String> {
        val config = mutableMapOf<String, String>()

        // Required defaults that cannot be overridden
        config["bootstrap.servers"] = bootstrapServers
        config["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        config["group.id"] = groupId

        // Apply user configuration (can override defaults)
        userConfig?.forEach { entry ->
            config[entry.key] = entry.value.toString()
        }

        // Set fallback defaults if not provided by user
        config.putIfAbsent("auto.offset.reset", "earliest")
        config.putIfAbsent("enable.auto.commit", "true")

        return config
    }
}