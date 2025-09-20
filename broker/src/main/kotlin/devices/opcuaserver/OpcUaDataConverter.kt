package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import io.vertx.core.json.JsonObject
import org.eclipse.milo.opcua.stack.core.Identifiers
import org.eclipse.milo.opcua.stack.core.types.builtin.*
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger
import java.time.Instant
import java.util.logging.Logger
import java.util.Base64

/**
 * Converter for data between MQTT payloads and OPC UA values
 */
class OpcUaDataConverter {
    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaDataConverter::class.java)

        /**
         * Convert MQTT payload to OPC UA DataValue
         */
        fun mqttToOpcUa(
            payload: ByteArray,
            dataType: OpcUaServerDataType,
            sourceTimestamp: Instant? = null
        ): DataValue {
            return try {
                val variant = when (dataType) {
                    OpcUaServerDataType.BINARY -> {
                        // Raw bytes as ByteString
                        Variant(ByteString.of(payload))
                    }

                    OpcUaServerDataType.TEXT -> {
                        // UTF-8 text
                        Variant(String(payload, Charsets.UTF_8))
                    }

                    OpcUaServerDataType.NUMERIC -> {
                        // Parse as double
                        val text = String(payload, Charsets.UTF_8).trim()
                        val number = text.toDoubleOrNull() ?: 0.0
                        Variant(number)
                    }

                    OpcUaServerDataType.BOOLEAN -> {
                        // Parse as boolean
                        val text = String(payload, Charsets.UTF_8).trim().lowercase()
                        val bool = text == "true" || text == "1" || text == "on"
                        Variant(bool)
                    }

                    OpcUaServerDataType.JSON -> {
                        // Parse JSON with value, timestamp, and status
                        try {
                            val json = JsonObject(String(payload, Charsets.UTF_8))
                            val value = json.getValue("value")
                            val timestamp = json.getString("timestamp")?.let {
                                DateTime(Instant.parse(it))
                            }
                            val status = json.getInteger("status", 0)

                            // Convert JSON value to appropriate variant
                            val opcValue = when (value) {
                                is Boolean -> Variant(value)
                                is Number -> Variant(value.toDouble())
                                is String -> Variant(value)
                                else -> Variant(value?.toString() ?: "")
                            }

                            // Return with custom timestamp and status if provided
                            return DataValue(
                                opcValue,
                                StatusCode(status.toLong()),
                                timestamp ?: DateTime.now(),
                                DateTime.now()
                            )
                        } catch (e: Exception) {
                            logger.warning("Failed to parse JSON payload: ${e.message}")
                            Variant(String(payload, Charsets.UTF_8))
                        }
                    }
                }

                DataValue(
                    variant,
                    StatusCode.GOOD,
                    sourceTimestamp?.let { DateTime(it) } ?: DateTime.now(),
                    DateTime.now()
                )

            } catch (e: Exception) {
                logger.severe("Error converting MQTT to OPC UA: ${e.message}")
                DataValue(
                    Variant.NULL_VALUE,
                    StatusCode.BAD,
                    DateTime.now(),
                    DateTime.now()
                )
            }
        }

        /**
         * Convert OPC UA DataValue to MQTT payload
         */
        fun opcUaToMqtt(dataValue: DataValue, dataType: OpcUaServerDataType): ByteArray {
            return try {
                when (dataType) {
                    OpcUaServerDataType.BINARY -> {
                        // Extract ByteString
                        val byteString = dataValue.value?.value as? ByteString
                        byteString?.bytes() ?: byteArrayOf()
                    }

                    OpcUaServerDataType.TEXT -> {
                        // Convert to string
                        val text = dataValue.value?.value?.toString() ?: ""
                        text.toByteArray(Charsets.UTF_8)
                    }

                    OpcUaServerDataType.NUMERIC -> {
                        // Convert number to string
                        val number = when (val value = dataValue.value?.value) {
                            is Number -> value.toString()
                            else -> "0"
                        }
                        number.toByteArray(Charsets.UTF_8)
                    }

                    OpcUaServerDataType.BOOLEAN -> {
                        // Convert boolean to string
                        val bool = when (val value = dataValue.value?.value) {
                            is Boolean -> value.toString()
                            else -> "false"
                        }
                        bool.toByteArray(Charsets.UTF_8)
                    }

                    OpcUaServerDataType.JSON -> {
                        // Create JSON with value, timestamp, and status
                        val json = JsonObject()

                        // Extract value
                        val value = dataValue.value?.value
                        when (value) {
                            is Boolean -> json.put("value", value)
                            is Number -> json.put("value", value.toDouble())
                            is String -> json.put("value", value)
                            is ByteString -> json.put("value", Base64.getEncoder().encodeToString(value.bytes()))
                            else -> json.put("value", value?.toString() ?: "")
                        }

                        // Add timestamp
                        dataValue.sourceTime?.javaInstant?.let {
                            json.put("timestamp", it.toString())
                        }

                        // Add status code
                        dataValue.statusCode?.let {
                            json.put("status", it.value.toInt())
                        }

                        json.encode().toByteArray(Charsets.UTF_8)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error converting OPC UA to MQTT: ${e.message}")
                byteArrayOf()
            }
        }

        /**
         * Determine the OPC UA DataType NodeId based on the conversion type
         */
        fun getOpcUaDataType(dataType: OpcUaServerDataType): NodeId {
            return when (dataType) {
                OpcUaServerDataType.BINARY -> Identifiers.ByteString
                OpcUaServerDataType.TEXT -> Identifiers.String
                OpcUaServerDataType.NUMERIC -> Identifiers.Double
                OpcUaServerDataType.BOOLEAN -> Identifiers.Boolean
                OpcUaServerDataType.JSON -> Identifiers.BaseDataType // Use generic type for JSON
            }
        }
    }
}