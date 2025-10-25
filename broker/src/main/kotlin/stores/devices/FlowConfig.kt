package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * Position of a node in the visual flow editor
 */
data class FlowPosition(
    val x: Double,
    val y: Double
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("x", x)
            .put("y", y)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowPosition {
            return FlowPosition(
                x = json.getDouble("x", 0.0),
                y = json.getDouble("y", 0.0)
            )
        }
    }
}

/**
 * A node in a flow with named inputs and outputs
 */
data class FlowNode(
    val id: String,
    val type: String,
    val name: String,
    val config: JsonObject,
    val inputs: List<String>,
    val outputs: List<String>,
    val language: String = "javascript",  // Script language: "javascript" or "python"
    val position: FlowPosition? = null
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("id", id)
            .put("type", type)
            .put("name", name)
            .put("config", config)
            .put("inputs", JsonArray(inputs))
            .put("outputs", JsonArray(outputs))
            .put("language", language)
            .apply {
                position?.let { put("position", it.toJsonObject()) }
            }
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowNode {
            return FlowNode(
                id = json.getString("id"),
                type = json.getString("type"),
                name = json.getString("name"),
                config = json.getJsonObject("config", JsonObject()),
                inputs = json.getJsonArray("inputs", JsonArray()).map { it.toString() },
                outputs = json.getJsonArray("outputs", JsonArray()).map { it.toString() },
                language = json.getString("language", "javascript"),
                position = json.getJsonObject("position")?.let { FlowPosition.fromJsonObject(it) }
            )
        }
    }
}

/**
 * Connection between nodes using named ports
 */
data class FlowConnection(
    val fromNode: String,
    val fromOutput: String,
    val toNode: String,
    val toInput: String
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("fromNode", fromNode)
            .put("fromOutput", fromOutput)
            .put("toNode", toNode)
            .put("toInput", toInput)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowConnection {
            return FlowConnection(
                fromNode = json.getString("fromNode"),
                fromOutput = json.getString("fromOutput"),
                toNode = json.getString("toNode"),
                toInput = json.getString("toInput")
            )
        }
    }
}

/**
 * Flow Class - Template/blueprint for flows
 * Stored as DeviceConfig with type="Flow-Class"
 */
data class FlowClass(
    val version: String,
    val description: String?,
    val nodes: List<FlowNode>,
    val connections: List<FlowConnection>
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("version", version)
            .put("description", description)
            .put("nodes", JsonArray(nodes.map { it.toJsonObject() }))
            .put("connections", JsonArray(connections.map { it.toJsonObject() }))
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowClass {
            return FlowClass(
                version = json.getString("version", "1.0"),
                description = json.getString("description"),
                nodes = json.getJsonArray("nodes", JsonArray()).map {
                    FlowNode.fromJsonObject(it as JsonObject)
                },
                connections = json.getJsonArray("connections", JsonArray()).map {
                    FlowConnection.fromJsonObject(it as JsonObject)
                }
            )
        }
    }
}

/**
 * Type of flow input
 */
enum class FlowInputType {
    TOPIC  // Dynamic input from MQTT topic (triggers flow)
}

/**
 * Input mapping for a flow instance
 */
data class FlowInputMapping(
    val nodeInput: String,      // "nodeId.inputName"
    val type: FlowInputType,
    val value: String
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("nodeInput", nodeInput)
            .put("type", type.name)
            .put("value", value)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowInputMapping {
            return FlowInputMapping(
                nodeInput = json.getString("nodeInput"),
                type = FlowInputType.valueOf(json.getString("type", "TOPIC")),
                value = json.getString("value")
            )
        }
    }
}

/**
 * Output mapping for a flow instance
 */
data class FlowOutputMapping(
    val nodeOutput: String,     // "nodeId.outputName"
    val topic: String
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("nodeOutput", nodeOutput)
            .put("topic", topic)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowOutputMapping {
            return FlowOutputMapping(
                nodeOutput = json.getString("nodeOutput"),
                topic = json.getString("topic")
            )
        }
    }
}

/**
 * Flow Instance - Concrete instance of a flow class with input/output mappings
 * Stored as DeviceConfig with type="Flow-Object"
 */
data class FlowInstance(
    val flowClassId: String,
    val inputMappings: List<FlowInputMapping>,
    val outputMappings: List<FlowOutputMapping>,
    val variables: JsonObject?
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("flowClassId", flowClassId)
            .put("inputMappings", JsonArray(inputMappings.map { it.toJsonObject() }))
            .put("outputMappings", JsonArray(outputMappings.map { it.toJsonObject() }))
            .put("variables", variables)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowInstance {
            return FlowInstance(
                flowClassId = json.getString("flowClassId"),
                inputMappings = json.getJsonArray("inputMappings", JsonArray()).map {
                    FlowInputMapping.fromJsonObject(it as JsonObject)
                },
                outputMappings = json.getJsonArray("outputMappings", JsonArray()).map {
                    FlowOutputMapping.fromJsonObject(it as JsonObject)
                },
                variables = json.getJsonObject("variables")
            )
        }
    }
}

/**
 * Runtime status of a flow instance
 */
data class FlowInstanceStatus(
    val running: Boolean,
    val lastExecution: Instant?,
    val executionCount: Long,
    val errorCount: Long,
    val lastError: String?,
    val subscribedTopics: List<String>
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("running", running)
            .put("lastExecution", lastExecution?.toString())
            .put("executionCount", executionCount)
            .put("errorCount", errorCount)
            .put("lastError", lastError)
            .put("subscribedTopics", JsonArray(subscribedTopics))
    }

    companion object {
        fun fromJsonObject(json: JsonObject): FlowInstanceStatus {
            return FlowInstanceStatus(
                running = json.getBoolean("running", false),
                lastExecution = json.getString("lastExecution")?.let { Instant.parse(it) },
                executionCount = json.getLong("executionCount", 0L),
                errorCount = json.getLong("errorCount", 0L),
                lastError = json.getString("lastError"),
                subscribedTopics = json.getJsonArray("subscribedTopics", JsonArray()).map { it.toString() }
            )
        }
    }
}

/**
 * Topic value stored in flow runtime
 */
data class TopicValue(
    val value: Any?,
    val timestamp: Long,
    val qos: Int
) {
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("value", value)
            .put("timestamp", timestamp)
            .put("qos", qos)
    }

    companion object {
        fun fromJsonObject(json: JsonObject): TopicValue {
            return TopicValue(
                value = json.getValue("value"),
                timestamp = json.getLong("timestamp", System.currentTimeMillis()),
                qos = json.getInteger("qos", 0)
            )
        }
    }
}
