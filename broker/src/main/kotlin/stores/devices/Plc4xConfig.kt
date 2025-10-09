package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * Configuration for a PLC4X connection
 *
 * @property protocol The PLC4X protocol to use (e.g., S7, MODBUS_TCP, ADS)
 * @property connectionString The PLC4X connection string (e.g., "s7://192.168.1.10")
 * @property pollingInterval Interval in milliseconds for polling addresses (default: 1000ms)
 * @property reconnectDelay Delay in milliseconds before attempting reconnection (default: 5000ms)
 * @property addresses List of addresses to read from the device
 * @property enabled Whether polling is enabled for this connection
 */
data class Plc4xConnectionConfig(
    val protocol: String,
    val connectionString: String,
    val pollingInterval: Long = 1000,
    val reconnectDelay: Long = 5000,
    val addresses: List<Plc4xAddress> = emptyList(),
    val enabled: Boolean = true
) {
    companion object {
        fun fromJsonObject(json: JsonObject): Plc4xConnectionConfig {
            val addressesArray = json.getJsonArray("addresses", JsonArray())
            val addresses = addressesArray
                .map { it as JsonObject }
                .map { Plc4xAddress.fromJsonObject(it) }

            return Plc4xConnectionConfig(
                protocol = json.getString("protocol") ?: throw IllegalArgumentException("protocol is required"),
                connectionString = json.getString("connectionString") ?: throw IllegalArgumentException("connectionString is required"),
                pollingInterval = json.getLong("pollingInterval", 1000L),
                reconnectDelay = json.getLong("reconnectDelay", 5000L),
                addresses = addresses,
                enabled = json.getBoolean("enabled", true)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val addressesArray = JsonArray(addresses.map { it.toJsonObject() })

        return JsonObject()
            .put("protocol", protocol)
            .put("connectionString", connectionString)
            .put("pollingInterval", pollingInterval)
            .put("reconnectDelay", reconnectDelay)
            .put("addresses", addressesArray)
            .put("enabled", enabled)
    }

    /**
     * Validates the connection configuration
     * @return List of validation error messages (empty if valid)
     */
    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (protocol.isBlank()) {
            errors.add("Protocol cannot be blank")
        }

        if (connectionString.isBlank()) {
            errors.add("Connection string cannot be blank")
        }

        if (pollingInterval < 100) {
            errors.add("Polling interval must be at least 100ms")
        }

        if (reconnectDelay < 1000) {
            errors.add("Reconnect delay must be at least 1000ms")
        }

        // Validate each address
        addresses.forEachIndexed { index, address ->
            val addressErrors = address.validate()
            addressErrors.forEach { error ->
                errors.add("Address[$index]: $error")
            }
        }

        return errors
    }
}

/**
 * Configuration for a single PLC4X address/tag
 *
 * @property name Unique identifier for this address within the device
 * @property address The PLC4X address string (protocol-specific format, e.g., "%DB1.DBW0:INT")
 * @property topic The MQTT topic where values should be published
 * @property qos MQTT QoS level (0, 1, or 2)
 * @property retained Whether MQTT messages should be retained
 * @property scalingFactor Optional scaling factor applied to numeric values (value * scalingFactor)
 * @property offset Optional offset added to numeric values after scaling (value + offset)
 * @property deadband Optional deadband value - only publish if change exceeds this value
 * @property enabled Whether this address should be polled
 */
data class Plc4xAddress(
    val name: String,
    val address: String,
    val topic: String,
    val qos: Int = 0,
    val retained: Boolean = false,
    val scalingFactor: Double? = null,
    val offset: Double? = null,
    val deadband: Double? = null,
    val enabled: Boolean = true
) {
    companion object {
        fun fromJsonObject(json: JsonObject): Plc4xAddress {
            return Plc4xAddress(
                name = json.getString("name") ?: throw IllegalArgumentException("name is required"),
                address = json.getString("address") ?: throw IllegalArgumentException("address is required"),
                topic = json.getString("topic") ?: throw IllegalArgumentException("topic is required"),
                qos = json.getInteger("qos", 0),
                retained = json.getBoolean("retained", false),
                scalingFactor = json.getDouble("scalingFactor"),
                offset = json.getDouble("offset"),
                deadband = json.getDouble("deadband"),
                enabled = json.getBoolean("enabled", true)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val json = JsonObject()
            .put("name", name)
            .put("address", address)
            .put("topic", topic)
            .put("qos", qos)
            .put("retained", retained)
            .put("enabled", enabled)

        scalingFactor?.let { json.put("scalingFactor", it) }
        offset?.let { json.put("offset", it) }
        deadband?.let { json.put("deadband", it) }

        return json
    }

    /**
     * Validates the address configuration
     * @return List of validation error messages (empty if valid)
     */
    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (name.isBlank()) {
            errors.add("Name cannot be blank")
        }

        if (address.isBlank()) {
            errors.add("Address cannot be blank")
        }

        if (topic.isBlank()) {
            errors.add("Topic cannot be blank")
        }

        if (qos !in 0..2) {
            errors.add("QoS must be 0, 1, or 2")
        }

        deadband?.let {
            if (it < 0) {
                errors.add("Deadband must be non-negative")
            }
        }

        return errors
    }

    /**
     * Applies scaling and offset transformation to a numeric value
     * @param value The raw value from the PLC
     * @return The transformed value
     */
    fun transformValue(value: Number): Double {
        var result = value.toDouble()
        scalingFactor?.let { result *= it }
        offset?.let { result += it }
        return result
    }

    /**
     * Checks if the value change exceeds the deadband threshold
     * @param oldValue The previous value
     * @param newValue The new value
     * @return true if the change exceeds deadband, false otherwise
     */
    fun exceedsDeadband(oldValue: Number, newValue: Number): Boolean {
        if (deadband == null) return true
        return Math.abs(newValue.toDouble() - oldValue.toDouble()) > deadband
    }
}
